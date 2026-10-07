import type {
  RequesterSettleWakeState,
  SubagentRunRecord,
} from "../registry/subagent-registry.types.js";
import { isSameSubagentRun, isSameSubagentRunOwner } from "../registry/subagent-run-generation.js";
import type { SubagentAnnounceDeliveryResult } from "./subagent-announce-dispatch.js";

export type RequesterSettleWakeBatchState = Omit<RequesterSettleWakeState, "retireAfterSettle">;

export type RequesterSettleWakeBatchCallbacks = {
  completeBatch: (
    batch: readonly SubagentRunRecord[],
    rearmGeneration?: number,
    delivery?: SubagentAnnounceDeliveryResult,
    onCommitted?: () => void,
  ) => void | Promise<void>;
};

export function retainedYieldIdentity(state: RequesterSettleWakeBatchState) {
  return {
    ...(state.pauseNotice ? { pauseNotice: state.pauseNotice } : {}),
    ...(state.requesterYieldBatch === true ? { requesterYieldBatch: true as const } : {}),
    ...(state.afterRequesterYield === true ? { afterRequesterYield: true as const } : {}),
    ...(state.yieldedFinalDeliverable === true ? { yieldedFinalDeliverable: true as const } : {}),
    ...(state.rearmGeneration !== undefined ? { rearmGeneration: state.rearmGeneration } : {}),
  };
}

export function readSharedBatchState(
  batch: readonly SubagentRunRecord[],
): RequesterSettleWakeBatchState {
  const states = batch
    .map((entry) => entry.requesterSettleWake)
    .filter((state): state is RequesterSettleWakeState => Boolean(state));
  const source = states[0];
  return {
    ...(source?.pauseNotice ? { pauseNotice: source.pauseNotice } : {}),
    ...(source?.batchRunIds ? { batchRunIds: [...source.batchRunIds] } : {}),
    ...(states.some((state) => state.requesterYieldBatch === true)
      ? { requesterYieldBatch: true }
      : {}),
    ...(states.some((state) => state.afterRequesterYield === true)
      ? { afterRequesterYield: true }
      : {}),
    ...(source?.yieldedFinalDeliverable === true ? { yieldedFinalDeliverable: true } : {}),
    ...(source?.rearmGeneration !== undefined ? { rearmGeneration: source.rearmGeneration } : {}),
  };
}

export function captureRequesterRunOwner(requesterRun: SubagentRunRecord | null | undefined) {
  const requesterTaskRunId = requesterRun?.taskRunId ?? requesterRun?.runId;
  return (currentRequester: SubagentRunRecord | null | undefined, continuationRunId: string) => {
    if (!currentRequester || !requesterRun) {
      return !currentRequester && !requesterRun;
    }
    if (isSameSubagentRun(currentRequester, requesterRun)) {
      return isSameSubagentRunOwner(currentRequester, requesterRun);
    }
    // Only the admitted continuation may replace its captured task owner.
    return (
      currentRequester.runId === continuationRunId &&
      currentRequester.taskRunId === requesterTaskRunId &&
      currentRequester.requesterSessionKey === requesterRun.requesterSessionKey &&
      currentRequester.requesterAgentId === requesterRun.requesterAgentId
    );
  };
}

/**
 * A yield hands continuation back to the requester, so its own final may reach the
 * conversation under its normal reply rules; private findings stay wake input. The
 * policy is fixed when the yield writes the batch: a batch without the marker came
 * from an earlier build and stays private, so an upgrade cannot republish its input.
 */
export function resolvePrivateSettlePolicy(
  completionRows: readonly SubagentRunRecord[],
  requesterYielded: boolean,
  state: RequesterSettleWakeBatchState,
  requester: { sessionId: string; lifecycleRevision?: string },
) {
  // One private result makes the aggregate private; public siblings keep their own route.
  const privateRows = completionRows.filter((entry) => entry.completionTarget === "parent");
  const hasPrivateRows = privateRows.length > 0;
  const yieldedFinalDeliverable =
    hasPrivateRows && requesterYielded && state.yieldedFinalDeliverable === true;
  const parentOnly = hasPrivateRows && !yieldedFinalDeliverable;
  // Private findings stay bound to the requester incarnation that produced them.
  const privateBinding = {
    ...(parentOnly ? { completionTarget: "parent" as const } : {}),
    ...(hasPrivateRows
      ? {
          completionRequesterSessionId: requester.sessionId,
          completionRequesterLifecycleRevision: requester.lifecycleRevision,
        }
      : {}),
  };
  const admissionMarker = yieldedFinalDeliverable ? { yieldedFinalDeliverable: true as const } : {};
  // A yield owes the conversation a visible final unless private findings let the
  // requester choose silence.
  const requireVisibleReply = requesterYielded && !hasPrivateRows;
  return { privateRows, requireVisibleReply, parentOnly, privateBinding, admissionMarker };
}
