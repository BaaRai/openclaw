import { isSessionDeliveryGenerationRevokedError } from "../../../config/sessions/session-delivery-generation.js";
import { isAgentEventLifecycleGenerationCurrent } from "../../../infra/agent-events.js";
import { formatErrorMessage } from "../../../infra/errors.js";
import { hasSqliteWorkerOutcomeUnknown } from "../../../infra/sqlite-worker-contract.js";
import type { OpenClawStateWorkerContext } from "../../../state/openclaw-state-worker-context.types.js";
import type { SubagentKillSession } from "./subagent-control-session.js";
import {
  SUBAGENT_KILL_TASK_ERROR,
  type SubagentCancellationControl,
  type SubagentKillMutationResult,
  type SubagentKillTargetState,
} from "./subagent-control.types.js";
import { SUBAGENT_ENDED_REASON_KILLED } from "./subagent-lifecycle-events.js";
import { getCurrentSubagentRunOwner, subagentRuns } from "./subagent-registry-memory.js";
import {
  claimSubagentRunKill,
  markSubagentRunTerminated,
  retireSubagentObligations,
} from "./subagent-registry.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";
import { isSubagentObligationRetired } from "./subagent-requester-settle-identity.js";

type SubagentKillClaim = NonNullable<Awaited<ReturnType<typeof claimSubagentRunKill>>>;

type SubagentKillSettlementOwnerParams = {
  currentEntry: () => SubagentRunRecord | undefined;
  isCurrent: () => boolean;
  assertState: () => void;
  targetState: () => SubagentKillTargetState | undefined;
  getClaim: () => SubagentKillClaim | undefined;
  releaseClaim: (claim: SubagentKillClaim) => Promise<boolean>;
  session: SubagentKillSession;
  stateContext: OpenClawStateWorkerContext;
  cancellationControl: Pick<SubagentCancellationControl, "assertCurrent">;
  stopAcceptance: { accepted: boolean };
  suppressTaskDelivery?: boolean;
  requesterNotified?: boolean;
  withdrawQueuedReservation: () => void;
};

/** Identifies the terminal state produced by an accepted subagent kill. */
export function isKilledSubagentTarget(state: SubagentKillTargetState): boolean {
  return (
    state.state === "terminal" &&
    state.task.status === "cancelled" &&
    state.task.error === SUBAGENT_KILL_TASK_ERROR
  );
}

/** Owns durable kill-claim validation, release, and terminal settlement. */
export function createSubagentKillSettlementOwner(params: SubagentKillSettlementOwnerParams) {
  const ownsIntent = (current: SubagentRunRecord | undefined, claim: SubagentKillClaim) => {
    const intent = current?.killIntent;
    return (
      intent !== undefined &&
      intent.requestedAt === claim.requestedAt &&
      intent.reason === claim.reason &&
      intent.lifecycleGeneration === claim.lifecycleGeneration &&
      intent.sessionId === claim.sessionId &&
      intent.sessionLifecycleRevision === claim.sessionLifecycleRevision &&
      intent.suppressTaskDelivery === claim.suppressTaskDelivery
    );
  };

  const isCurrent = () => {
    const current = params.currentEntry();
    const claim = params.getClaim();
    return (
      current !== undefined &&
      params.isCurrent() &&
      (!claim ||
        ((ownsIntent(current, claim) ||
          (!current.killIntent &&
            current.endedReason === SUBAGENT_ENDED_REASON_KILLED &&
            current.killReconciliation?.killedAt === claim.requestedAt &&
            current.killReconciliation.taskCancellationAccepted === true &&
            current.execution.lifecycleGeneration === claim.lifecycleGeneration)) &&
          (claim.lifecycleGeneration === undefined ||
            isAgentEventLifecycleGenerationCurrent(claim.lifecycleGeneration))))
    );
  };

  const ownsSessionIncarnation = () => {
    try {
      params.assertState();
      return true;
    } catch (error) {
      if (isSessionDeliveryGenerationRevokedError(error)) {
        return false;
      }
      throw error;
    }
  };

  const assertCancellationCurrent = () => {
    params.cancellationControl.assertCurrent();
    if (!isCurrent() || !ownsSessionIncarnation()) {
      throw new Error("Subagent ownership changed before cancellation; retry.");
    }
  };

  const releaseChangedSession = async (
    claim: SubagentKillClaim,
  ): Promise<SubagentKillMutationResult> => {
    try {
      await params.releaseClaim(claim);
    } catch (error) {
      if (hasSqliteWorkerOutcomeUnknown(error)) {
        throw error;
      }
      return {
        killed: false,
        error: `Subagent session changed and its kill intent could not be released: ${formatErrorMessage(error)}`,
      };
    }
    return {
      killed: false,
      error: "Subagent session changed while the kill was pending; retry.",
    };
  };

  // A committed kill ends the row's completion obligation, including its owed inputs.
  const retireObligations = async (
    result: SubagentKillMutationResult,
  ): Promise<SubagentKillMutationResult> => {
    const current = params.currentEntry();
    if (!current || !isSubagentObligationRetired(current)) {
      return result;
    }
    try {
      // The killed row may already have a successor; retirement binds to this exact row.
      await retireSubagentObligations(
        current,
        () => {
          if (getCurrentSubagentRunOwner(subagentRuns, current) !== current) {
            throw new Error("Killed subagent row changed before obligation retirement.");
          }
        },
        { requesterNotified: params.requesterNotified },
      );
      return result;
    } catch (error) {
      if (hasSqliteWorkerOutcomeUnknown(error)) {
        throw error;
      }
      const failure = `Subagent obligation retirement failed: ${formatErrorMessage(error)}`;
      return { ...result, error: [result.error, failure].filter(Boolean).join(" ") };
    }
  };

  const settleKill = async (claim: SubagentKillClaim): Promise<SubagentKillMutationResult> => {
    if (!ownsSessionIncarnation()) {
      return releaseChangedSession(claim);
    }
    if (!isCurrent()) {
      return { killed: false, superseded: true };
    }
    const selected = params.currentEntry();
    if (!selected) {
      return { killed: false, superseded: true };
    }
    let marked = 0;
    try {
      marked = await markSubagentRunTerminated({
        runId: selected.runId,
        session: params.session,
        withdrawQueuedReservation: params.withdrawQueuedReservation,
        reason: "killed",
        suppressTaskDelivery: params.suppressTaskDelivery,
        context: params.stateContext,
        assertCurrent: () => {
          params.assertState();
          if (!params.stopAcceptance.accepted) {
            params.cancellationControl.assertCurrent();
          }
          if (!isCurrent()) {
            throw new Error("Subagent kill settlement lost its original claim.");
          }
        },
        assertPublicationCurrent: () => {
          params.assertState();
          if (!isCurrent()) {
            throw new Error("Subagent kill publication lost its original claim");
          }
        },
        onPublished: (count) => {
          marked = count;
        },
      });
    } catch (error) {
      if (hasSqliteWorkerOutcomeUnknown(error)) {
        throw error;
      }
      const action =
        marked > 0 ? "finish subagent kill cleanup" : "persist subagent kill tombstone";
      return {
        killed: marked > 0,
        error: `Failed to ${action}: ${formatErrorMessage(error)}`,
      };
    }
    if (marked === 0) {
      params.assertState();
      if (!params.isCurrent()) {
        return { killed: false, superseded: true };
      }
      return {
        killed: false,
        targetState: params.targetState(),
      };
    }
    return { killed: true };
  };
  const settle = async (claim: SubagentKillClaim) => retireObligations(await settleKill(claim));

  return {
    assertCancellationCurrent,
    isCurrent,
    ownsIntent,
    ownsSessionIncarnation,
    releaseChangedSession,
    retireObligations,
    settle,
  };
}
