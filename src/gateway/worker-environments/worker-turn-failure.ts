import { reportPlacementTransition } from "./placement-record.js";
import type {
  WorkerSessionPlacementRecord,
  WorkerSessionPlacementStore,
  WorkerSessionTurnClaim,
} from "./placement-store.js";
import type { WorkerEnvironmentService } from "./service.js";
import { boundedWorkerError } from "./worker-error.js";
import { releaseClaimIfOwned } from "./worker-turn-admission.js";
import { AcceptedWorkspacePublicationIndeterminateError } from "./workspace-accepted-publication.js";

export type WorkerTurnEnvironmentService = Pick<
  WorkerEnvironmentService,
  | "acknowledgeCredentialDelivery"
  | "acquireTurnCredential"
  | "destroy"
  | "get"
  | "startTunnel"
  | "stopTunnel"
> &
  Partial<
    Pick<
      WorkerEnvironmentService,
      | "resolveSshIdentity"
      | "supportsNodePortal"
      | "prepareComputer"
      | "readRuntimeRefresh"
      | "createGatewayTools"
    >
  >;

export type ActiveWorkerPlacement = Extract<WorkerSessionPlacementRecord, { state: "active" }>;

export class WorkerTurnExecutionError extends Error {}

export class WorkerWorkspaceReconciliationError extends Error {
  override name = "WorkerWorkspaceReconciliationError";
}

export async function failHandedOffTurn(params: {
  environments: WorkerTurnEnvironmentService;
  placements: WorkerSessionPlacementStore;
  placement: ActiveWorkerPlacement;
  turnClaim: WorkerSessionTurnClaim;
  error: unknown;
}): Promise<void> {
  const failures = [boundedWorkerError(params.error)];
  let drained: WorkerSessionPlacementRecord;
  try {
    drained = await params.placements.startDrain({
      sessionId: params.placement.sessionId,
      environmentId: params.placement.environmentId,
      ownerEpoch: params.placement.activeOwnerEpoch,
      expectedGeneration: params.placement.generation,
      expectedTurnClaim: params.turnClaim,
    });
  } catch (error) {
    if (error instanceof AcceptedWorkspacePublicationIndeterminateError) {
      throw error;
    }
    const current = params.placements.get(params.placement.sessionId);
    const exactDrainOwner =
      current?.state === "draining" &&
      current.generation === params.placement.generation + 1 &&
      current.environmentId === params.placement.environmentId &&
      current.activeOwnerEpoch === params.placement.activeOwnerEpoch &&
      params.placements.validateTurnClaim(params.turnClaim);
    if (exactDrainOwner) {
      // Another lifecycle owner already closed admission for this exact turn.
      // Release its claim without stealing that owner's reconciliation or teardown.
      await releaseClaimIfOwned(params.placements, params.turnClaim);
    }
    // A different drain owner may belong to a replacement placement. Never
    // tear down an environment after losing the exact source-generation CAS.
    return;
  }
  if (drained.state !== "draining") {
    return;
  }
  const draining = drained;
  await releaseClaimIfOwned(params.placements, params.turnClaim);
  const isCurrentDrain = () => {
    const current = params.placements.get(draining.sessionId);
    return (
      current?.state === "draining" &&
      current.generation === draining.generation &&
      current.environmentId === draining.environmentId &&
      current.activeOwnerEpoch === draining.activeOwnerEpoch &&
      current.turnClaim === null
    );
  };
  if (!isCurrentDrain()) {
    return;
  }
  try {
    await params.environments.stopTunnel(
      params.placement.environmentId,
      params.placement.activeOwnerEpoch,
    );
  } catch (error) {
    failures.push(`tunnel stop: ${boundedWorkerError(error)}`);
  }
  // A replacement may own the session after cleanup. Never destroy that placement.
  if (!isCurrentDrain()) {
    return;
  }
  try {
    await params.environments.destroy(params.placement.environmentId);
  } catch (error) {
    failures.push(`environment destroy: ${boundedWorkerError(error)}`);
  }
  // Publish failure only after raw teardown has settled for this exact drain.
  if (!isCurrentDrain()) {
    return;
  }
  try {
    const reconciling = await params.placements.startReconcile({
      sessionId: draining.sessionId,
      environmentId: draining.environmentId,
      ownerEpoch: draining.activeOwnerEpoch,
      expectedGeneration: draining.generation,
    });
    const recoveryError = failures.join("; ");
    const failed = await params.placements.fail({
      sessionId: reconciling.sessionId,
      expectedGeneration: reconciling.generation,
      recoveryError,
    });
    reportPlacementTransition(undefined, failed);
  } catch (error) {
    if (error instanceof AcceptedWorkspacePublicationIndeterminateError) {
      throw error;
    }
    // Leave the durable draining or reconciling row for startup reconciliation.
  }
}
