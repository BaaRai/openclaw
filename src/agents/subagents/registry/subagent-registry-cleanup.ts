import { SUBAGENT_ENDED_REASON_COMPLETE } from "./subagent-lifecycle-events.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

export const shouldSuspendPendingFinalDelivery = (entry: SubagentRunRecord) =>
  entry.expectsCompletionMessage === true &&
  entry.endedReason === SUBAGENT_ENDED_REASON_COMPLETE &&
  entry.execution.outcome?.status === "ok";
