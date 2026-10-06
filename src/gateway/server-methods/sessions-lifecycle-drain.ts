import {
  ErrorCodes,
  GatewayErrorDetailCodes,
  errorShape,
  type ErrorShape,
  type SessionWorkspaceRecoveryRequiredErrorDetails,
} from "../../../packages/gateway-protocol/src/index.js";
import { createAgentRunDirectAbortError } from "../../agents/run-termination.js";
import { formatErrorMessage } from "../../infra/errors.js";
import { withTimeout } from "../../infra/fs-safe.js";
import {
  closeSessionControllerAdmission,
  captureSessionTarget,
  isCompetingSessionControllerWorkActive,
  hasSessionControllerQueuedWork,
  runSessionMutation,
  SESSION_CONTROLLER_DRAIN_TIMEOUT_MS,
} from "../../sessions/session-controller.lifecycle.js";
import { waitForChatAbortControllerRemoval } from "../chat-abort-lifecycle-internal.js";
import { createChatAbortOps } from "../chat-abort-ops.js";
import type { AgentTerminalSessionDrain } from "../terminal/session-manager.types.js";
import {
  getWorkerInferenceSessionControl,
  type AcceptedWorkerInferenceSessionDrain,
  type WorkerInferenceSessionDrain,
} from "../worker-environments/inference-control-internal.js";
import type { WorkerSessionPlacementStore } from "../worker-environments/placement-store.js";
import { isCurrentWorkerWorkspacePendingResultOwner } from "../worker-environments/placement-workspace-result.js";
import {
  prepareSessionWorkerPlacementArchiveCheck,
  prepareSessionWorkerPlacementMutationCheck,
  prepareSessionWorkerPlacementStop,
} from "../worker-environments/session-placement-lifecycle.js";
import { hasGatewaySessionAbortOwner } from "./chat-abort-authorization.js";
import { abortChatRunsForSessionKeyWithPartials } from "./chat-abort-runtime.js";
import type { GatewayRequestContext } from "./types.js";

type LifecyclePlacementService = NonNullable<
  GatewayRequestContext["workerSessionPlacementService"]
> &
  Partial<Pick<WorkerSessionPlacementStore, "waitForTurnClaimRelease">>;

type SessionLifecycleParams = {
  action: "archive" | "delete";
  authorize?: () => void;
  beforeCancel?: () => void;
  context: GatewayRequestContext;
  storePath: string;
  sessionKeys: string[];
  sessionId?: string;
  agentId: string;
  sessionKey: string;
  defaultAgentId?: string;
  lifecycleIdentities: string[];
};

export type SessionLifecycleDrain = {
  handoffToMutation(): void;
  release(): void;
  hasAuthoritativeWork(): boolean;
};

export class SessionLifecycleWorkspaceRecoveryError extends Error {
  constructor(readonly error: ErrorShape) {
    super(error.message);
  }
}

function hasAuthoritativeSessionWork(
  params: SessionLifecycleParams,
  workerDrain: WorkerInferenceSessionDrain | undefined,
  terminalDrain: AgentTerminalSessionDrain | undefined,
  workIdentities: string[],
): boolean {
  const sessionId = params.sessionId;
  return (
    isCompetingSessionControllerWorkActive(params.storePath, params.lifecycleIdentities) ||
    hasSessionControllerQueuedWork(params.storePath, workIdentities) ||
    hasGatewaySessionAbortOwner({
      sessionKeys: params.sessionKeys,
      sessionId,
      agentId: params.agentId,
      defaultAgentId: params.defaultAgentId,
    }) ||
    Boolean(
      sessionId &&
      params.context.workerSessionPlacementService?.getMany([sessionId]).get(sessionId)?.turnClaim,
    ) ||
    workerDrain?.hasWork() === true ||
    terminalDrain?.hasWork() === true
  );
}

/** Drain outside mutation locks; retain the closure until the final mutation owns ingress. */
export async function prepareSessionLifecycleDrain(
  params: SessionLifecycleParams,
): Promise<SessionLifecycleDrain> {
  const timeoutMs = SESSION_CONTROLLER_DRAIN_TIMEOUT_MS;
  const workIdentities = Array.from(
    new Set([...params.sessionKeys, ...(params.sessionId ? [params.sessionId] : [])]),
  );
  const target = captureSessionTarget({
    storeScope: params.storePath,
    sessionKey: params.sessionKey,
    aliases: params.lifecycleIdentities,
    agentId: params.agentId,
    incarnation: params.sessionId,
  });

  const workerService = params.context.workerEnvironmentService;
  let workerDrain: AcceptedWorkerInferenceSessionDrain | undefined;
  let workerDrained: Promise<void> | undefined;
  let terminalDrain: AgentTerminalSessionDrain | undefined;
  let reclaimed: Promise<void> | undefined;
  let releaseAdmissions = () => {};
  let released = false;
  const release = () => {
    if (released) {
      return;
    }
    released = true;
    try {
      terminalDrain?.release();
    } finally {
      try {
        workerDrain?.release();
      } finally {
        releaseAdmissions();
      }
    }
  };
  try {
    let preparedDrain:
      | {
          workerStop: ReturnType<typeof prepareSessionWorkerPlacementStop>;
          cancellation: ReturnType<typeof abortChatRunsForSessionKeyWithPartials>;
          controllerDrain: Promise<boolean>;
        }
      | undefined;
    const prepared = await runSessionMutation({
      target,
      kind: "delete",
      policy: "preempt",
      preempt: {
        activeRun: "abort",
        waitingInputs: "cancel",
        reason: createAgentRunDirectAbortError(),
      },
      prepare: async () => {
        // Settle preceding mutations before selecting owners, but never await their
        // cancellation completion before external placement drains have started.
        params.authorize?.();
        params.beforeCancel?.();
        const workerStop = prepareSessionWorkerPlacementStop(params);
        releaseAdmissions = closeSessionControllerAdmission({
          target,
          reason: createAgentRunDirectAbortError(),
        });
        if (params.sessionId) {
          const reservation = getWorkerInferenceSessionControl(workerService)?.reserveSessionDrain(
            params.sessionId,
          );
          try {
            workerDrain = reservation?.accept();
          } catch (error) {
            try {
              reservation?.release();
            } catch (releaseError) {
              if (releaseError !== error) {
                throw new AggregateError([error, releaseError], "Worker drain reservation failed", {
                  cause: releaseError,
                });
              }
            }
            throw error;
          }
          if (workerDrain) {
            workerDrained = workerDrain.drained;
            void workerDrained.catch(() => {});
            workerDrain.start();
          }
          terminalDrain = params.context.terminalSessions?.beginAgentSessionDrain({
            kind: "agent",
            agentSessionKey: params.sessionKey,
            agentSessionId: params.sessionId,
            agentId: params.agentId,
          });
        }

        // Capture dispatch custody before cancellation can settle its placement.
        if (workerStop.startBeforeDrain) {
          reclaimed = workerStop.stop();
          void reclaimed.catch(() => {});
        }
        let controllerDrain = Promise.resolve(true);
        const cancellation = abortChatRunsForSessionKeyWithPartials({
          context: params.context,
          ops: createChatAbortOps(params.context),
          sessionKey: params.sessionKeys[0]!,
          sessionKeyAliases: params.sessionKeys.slice(1),
          sessionId: params.sessionId,
          agentId: params.agentId,
          defaultAgentId: params.defaultAgentId,
          abortOrigin: "rpc",
          stopReason: params.action,
          stopSource: "mutation",
          requester: { isAdmin: true },
          includeProtectedRuns: true,
          onControllerTargets: (targets) => {
            controllerDrain = waitForChatAbortControllerRemoval({
              targets,
              timeoutMs,
            });
          },
        });
        // Observe failures immediately while the short mutation releases its queues.
        void cancellation.catch(() => {});
        preparedDrain = { workerStop, cancellation, controllerDrain };
      },
      run: async () => {
        if (!preparedDrain) {
          throw new Error("Session lifecycle drain was not prepared");
        }
        return preparedDrain;
      },
    });
    const abortResult = await prepared.cancellation;
    if (abortResult.unauthorized) {
      throw new Error("Session cancellation lost ownership");
    }

    params.authorize?.();
    if (params.sessionId) {
      const placements = params.context.workerSessionPlacementService;
      const pending = (await placements?.listPendingWorkspaceResultsAsync?.(params.sessionId))?.[0];
      params.authorize?.();
      const placement = placements?.getMany([params.sessionId]).get(params.sessionId);
      if (
        pending &&
        pending.workspaceAcceptedAtMs === null &&
        isCurrentWorkerWorkspacePendingResultOwner(placement, pending) &&
        params.context.workerPlacementRunnerAvailabilityReader?.read(placement)?.status ===
          "offline"
      ) {
        const details: SessionWorkspaceRecoveryRequiredErrorDetails = {
          code: GatewayErrorDetailCodes.SESSION_WORKSPACE_RECOVERY_REQUIRED,
          cause: "device_offline",
          recoveryAction: "continue_on_gateway",
          sessionId: params.sessionId,
          source: {
            generation: placement.generation,
            environmentId: placement.environmentId,
            ownerEpoch: placement.activeOwnerEpoch,
          },
        };
        throw new SessionLifecycleWorkspaceRecoveryError(
          errorShape(
            ErrorCodes.UNAVAILABLE,
            `Session ${params.sessionKey} has an unrecovered workspace result on an offline device. Reconnect the device to preserve its workspace, or use Continue on Gateway and accept that unsynced files may be lost.`,
            { details, retryable: false },
          ),
        );
      }
    }

    const placementService: LifecyclePlacementService | undefined =
      params.context.workerSessionPlacementService;
    const placement = params.sessionId
      ? placementService?.getMany([params.sessionId]).get(params.sessionId)
      : undefined;
    const placementWork = placement?.turnClaim
      ? placementService?.waitForTurnClaimRelease
        ? placementService
            .waitForTurnClaimRelease(params.sessionId!, { timeoutMs })
            .then(() => true)
        : Promise.resolve(false)
      : Promise.resolve(true);
    const workerWork = workerDrained
      ? withTimeout(workerDrained, timeoutMs, "worker inference lifecycle drain").then(() => true)
      : Promise.resolve(true);
    const terminalWork = terminalDrain
      ? withTimeout(terminalDrain.drained, timeoutMs, "agent terminal lifecycle drain").then(
          () => true,
        )
      : Promise.resolve(true);
    const drains = await Promise.all([
      prepared.controllerDrain,
      placementWork,
      workerWork,
      terminalWork,
    ]);
    if (!drains.every(Boolean)) {
      throw new Error("Session work is still active after the lifecycle drain");
    }
    // Failed placements keep cleanup custody without delaying archive visibility.
    // Other placements and destructive deletion still require safe reclaim.
    await (reclaimed ?? prepared.workerStop.stop());
    const placementTarget = { context: params.context, sessionId: params.sessionId };
    const assertPlacementCurrent =
      params.action === "archive"
        ? prepareSessionWorkerPlacementArchiveCheck(placementTarget).assertCurrent
        : prepareSessionWorkerPlacementMutationCheck(placementTarget);
    return {
      // Only the caller's active mutation may replace this mutex-free ingress lease.
      handoffToMutation: () => releaseAdmissions(),
      release,
      hasAuthoritativeWork: () => {
        try {
          assertPlacementCurrent();
        } catch {
          return true;
        }
        return hasAuthoritativeSessionWork(params, workerDrain, terminalDrain, workIdentities);
      },
    };
  } catch (error) {
    if (reclaimed || workerDrained || terminalDrain) {
      // Bound the failed caller's response without releasing accepted work.
      // Runtime custody retains the admission closures through real cleanup.
      void params.context
        .trackExecution(async () => {
          const failures = new Set([error]);
          const settlements = await Promise.allSettled([
            reclaimed,
            workerDrained,
            terminalDrain?.drained,
          ]);
          for (const settled of settlements) {
            if (settled.status === "rejected") {
              failures.add(settled.reason);
            }
          }
          try {
            release();
          } catch (releaseError) {
            failures.add(releaseError);
          }
          if (failures.size > 1) {
            throw new AggregateError(
              [...failures],
              "Session lifecycle and worker settlement failed",
              { cause: error },
            );
          }
        })
        .catch((cleanupError: unknown) => {
          params.context.logGateway.warn(
            `Session lifecycle cleanup failed: ${formatErrorMessage(cleanupError)}`,
          );
        });
      throw error;
    }
    release();
    throw error;
  }
}
