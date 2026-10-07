import { isCronRunSessionKey } from "../../../sessions/session-key-utils.js";
import type { OpenClawStateWorkerContext } from "../../../state/openclaw-state-worker-context.types.js";
import type { SubagentAnnounceFlowOutcome } from "../announce/subagent-announce.js";
import {
  ensureCompletionState,
  ensureDeliveryState,
  clearSubagentPendingDelivery,
} from "./subagent-delivery-state.js";
import {
  SUBAGENT_ENDED_REASON_COMPLETE,
  type SubagentLifecycleEndedReason,
} from "./subagent-lifecycle-events.js";
import { retireSupersededCleanupIfNeeded } from "./subagent-registry-lifecycle-attempt.js";
import type { SubagentLifecycleAnnounceCleanupContext } from "./subagent-registry-lifecycle-context.js";
import {
  finalizeResumedAnnounceGiveUp,
  finishSubagentCleanup,
} from "./subagent-registry-lifecycle-give-up.js";
import { commitSubagentLifecycleMutation } from "./subagent-registry-lifecycle-persistence.js";
import { getCurrentSubagentRunOwner } from "./subagent-registry-memory.js";
import {
  assertSubagentRegistryWriteSourceCurrent,
  assertSubagentRegistryWriteOutcomeKnown,
} from "./subagent-registry-persistence.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";
import { getSubagentRunRuntimeKey } from "./subagent-run-generation.js";

export const finalizeSubagentCleanup = async (
  context: SubagentLifecycleAnnounceCleanupContext,
  observedEntry: SubagentRunRecord,
  cleanup: "delete" | "keep",
  announceOutcome: SubagentAnnounceFlowOutcome,
  cleanupGeneration: number,
  stateContext: OpenClawStateWorkerContext,
  options?: {
    skipAnnounce?: boolean;
    skipRequesterDelivery?: boolean;
  },
) => {
  const params = context.options;
  assertSubagentRegistryWriteSourceCurrent(stateContext);
  const publishedEntry = getCurrentSubagentRunOwner(params.runs, observedEntry);
  if (!publishedEntry) {
    return;
  }
  let entry = publishedEntry;
  let runId = entry.runId;
  const runtimeKey = getSubagentRunRuntimeKey(observedEntry);
  if (!context.isCleanupAttemptCurrent(runId, entry, cleanupGeneration)) {
    await retireSupersededCleanupIfNeeded(context, entry, cleanupGeneration);
    return;
  }
  const assertCurrent = () => {
    assertSubagentRegistryWriteSourceCurrent(stateContext);
    const current = getCurrentSubagentRunOwner(params.runs, entry);
    assertSubagentRegistryWriteOutcomeKnown([current?.runId ?? runId], stateContext.admission);
    if (!context.isCleanupGenerationCurrent(runId, entry, cleanupGeneration)) {
      throw new Error("Subagent cleanup generation changed before persistence.");
    }
    if (current) {
      entry = current;
      runId = current.runId;
    }
  };
  const isCurrent = () => {
    assertSubagentRegistryWriteSourceCurrent(stateContext);
    return context.isCleanupAttemptCurrent(runId, entry, cleanupGeneration);
  };
  const commit = async (
    mutate: (draft: SubagentRunRecord) => void | false,
    onPublished?: () => void,
  ) => {
    entry = await commitSubagentLifecycleMutation(context, {
      entry,
      stateContext,
      mutate,
      assertCurrent,
      onPublished,
    });
  };
  assertCurrent();
  const skipRequesterDelivery =
    options?.skipRequesterDelivery === true || entry.suppressCompletionDelivery === true;
  const finishCleanup = (
    skipRequesterSettleWake: boolean,
    completionReason?: SubagentLifecycleEndedReason,
  ) =>
    finishSubagentCleanup(context, {
      runId,
      entry,
      cleanup,
      cleanupGeneration,
      stateContext,
      isCurrent,
      skipRequesterSettleWake,
      completionReason,
    });
  if (entry.expectsCompletionMessage === false || skipRequesterDelivery) {
    await commit((draft) => {
      const intentionalNonDelivery = draft.delivery?.disposition === "intentional_non_delivery";
      clearSubagentPendingDelivery(draft);
      if (skipRequesterDelivery) {
        const delivery = ensureDeliveryState(draft);
        delivery.status = "not_required";
        // Preserve the lifecycle owner's terminal fact after cleanup clears retry state.
        delivery.disposition = intentionalNonDelivery ? "intentional_non_delivery" : undefined;
        draft.suppressCompletionDelivery = undefined;
      }
      draft.wakeOnDescendantSettle = undefined;
    });
    await finishCleanup(skipRequesterDelivery);
    return;
  }
  if (announceOutcome === "delivered" || announceOutcome === "intentional_non_delivery") {
    let terminalNonDelivery = false;
    await commit((draft) => {
      terminalNonDelivery =
        announceOutcome === "intentional_non_delivery" && draft.delivery?.status === "failed";
      const delivery = ensureDeliveryState(draft);
      const shouldCreditDelivery =
        announceOutcome === "delivered" || delivery.status === "delivered";
      if (shouldCreditDelivery) {
        const deliveredAt = delivery.deliveredAt ?? delivery.announcedAt ?? Date.now();
        delivery.status = "delivered";
        delivery.deliveredAt = deliveredAt;
        delivery.announcedAt = delivery.announcedAt ?? deliveredAt;
        if (!options?.skipAnnounce) {
          delivery.announcedAt = deliveredAt;
        }
        clearSubagentPendingDelivery(draft);
        delivery.lastDropReason = undefined;
      } else {
        // A handoff stays pending for requester-settle; explicit suppression is
        // terminal and must not start another turn that overrides the decision.
        // Nothing wakes a cron run requester: it reads this row itself.
        delivery.status = terminalNonDelivery
          ? "failed"
          : isCronRunSessionKey(draft.requesterSessionKey)
            ? "not_required"
            : "pending";
        delivery.disposition = "intentional_non_delivery";
        delivery.payload = undefined;
        delivery.createdAt = undefined;
      }
      draft.wakeOnDescendantSettle = undefined;
      const completion = ensureCompletionState(draft);
      completion.fallbackResultText = undefined;
      completion.fallbackCapturedAt = undefined;
    });
    await finishCleanup(terminalNonDelivery, entry.endedReason ?? SUBAGENT_ENDED_REASON_COMPLETE);
    return;
  }

  if (announceOutcome === "session_queued") {
    // The correlated queue owns transport now. Settlement, not admission,
    // decides delivered versus blocked and re-enters cleanup afterward.
    await commit(
      (draft) => {
        draft.cleanupHandled = false;
      },
      () => params.resumedRuns.delete(runtimeKey),
    );
    return;
  }

  // Live descendants re-enter this cleanup when they settle. Otherwise an undelivered
  // result is recorded once; nothing retries delivery on a timer.
  const activeDescendantRuns = await params.countPendingDescendantRuns(
    entry.childSessionKey,
    assertCurrent,
  );
  assertCurrent();
  let delivered = entry.delivery?.status === "delivered";
  if (!delivered && activeDescendantRuns > 0) {
    await commit(
      (draft) => {
        delivered = draft.delivery?.status === "delivered";
        if (delivered) {
          return false;
        }
        draft.wakeOnDescendantSettle = true;
        draft.cleanupHandled = false;
        return undefined;
      },
      () => params.resumedRuns.delete(runtimeKey),
    );
    if (!delivered) {
      return;
    }
  }
  if (delivered) {
    await finalizeSubagentCleanup(
      context,
      entry,
      cleanup,
      "delivered",
      cleanupGeneration,
      stateContext,
      options,
    );
    return;
  }
  await finalizeResumedAnnounceGiveUp(context, {
    runId,
    entry,
    reason: "permanent_failure",
    cleanup,
    cleanupGeneration,
    completedAt: Date.now(),
    stateContext,
  });
};
