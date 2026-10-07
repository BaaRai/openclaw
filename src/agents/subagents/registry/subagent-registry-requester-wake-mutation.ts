import { getGatewayContextResolver } from "../../../plugins/runtime/gateway-request-scope.js";
import type { OpenClawStateWorkerContext } from "../../../state/openclaw-state-worker-context.types.js";
import type { SubagentAnnounceDeliveryResult } from "../announce/subagent-announce-dispatch.js";
import { settleRequesterCompletionBatch } from "../completion/subagent-completion-admission.store.js";
import type { SubagentLifecycleWakeContext } from "./subagent-registry-lifecycle-context.js";
import { assertSubagentRegistryWriteSourceCurrent } from "./subagent-registry-persistence.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";
import { isSameSubagentRunOwner } from "./subagent-run-generation.js";

/** The batch still owns its rows; only a recorded visible final may outlive its Gateway owner. */
function assertRequesterSettleWakeBatchCurrent(
  context: SubagentLifecycleWakeContext,
  batch: readonly SubagentRunRecord[],
  rearmGeneration: number | undefined,
  visibleFinalDelivered: boolean,
): void {
  for (const entry of batch) {
    const current = context.options.runs.get(entry.runId);
    if (
      !isSameSubagentRunOwner(current, entry) ||
      !current?.requesterSettleWake ||
      current.requesterSettleWake.rearmGeneration !== rearmGeneration ||
      (current.requesterSettleWake.yieldedFinalDeliverable === true) !==
        (entry.requesterSettleWake?.yieldedFinalDeliverable === true)
    ) {
      throw new Error("Requester wake batch changed before its outcome was recorded");
    }
    const resolve = getGatewayContextResolver(entry);
    if (!visibleFinalDelivered && resolve && !resolve()) {
      throw new Error("Requester wake Gateway owner is closed");
    }
  }
}

/** Records the batch's one outcome; without an outcome the wake retires without delivery. */
export async function settleRequesterSettleWakeBatch(
  context: SubagentLifecycleWakeContext,
  batch: readonly SubagentRunRecord[],
  rearmGeneration: number | undefined,
  outcome: SubagentAnnounceDeliveryResult | undefined,
  stateContext: OpenClawStateWorkerContext,
): Promise<boolean> {
  const visibleFinalDelivered =
    outcome?.delivered === true && outcome.requesterVisibleFinalDelivered === true;
  const assertCurrent = () => {
    assertSubagentRegistryWriteSourceCurrent(stateContext);
    assertRequesterSettleWakeBatchCurrent(context, batch, rearmGeneration, visibleFinalDelivered);
  };
  try {
    assertCurrent();
  } catch {
    return false;
  }
  const result = await settleRequesterCompletionBatch({
    entries: batch,
    outcome,
    context: stateContext,
    assertCurrent,
  });
  return result.applied === true && result.publication === "published";
}
