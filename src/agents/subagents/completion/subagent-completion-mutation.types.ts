import type { SubagentAnnounceDeliveryResult } from "../announce/subagent-announce-dispatch.js";
import type { SubagentRunSqliteRow } from "../registry/subagent-registry.store.row.js";
import type { SubagentRunRecord } from "../registry/subagent-registry.types.js";

export type BlockSubagentCompletionRequest = {
  subagent: SubagentRunRecord;
  reason: string;
  enqueuedAt?: number;
  suspendedReason?: "expiry" | "permanent_failure";
  storeReplaced?: true;
  lastDropReason?: NonNullable<SubagentRunRecord["delivery"]>["lastDropReason"];
  disposition?: NonNullable<SubagentRunRecord["delivery"]>["disposition"];
};

export type SubagentCompletionQueueReceipt =
  | { id: string; status: "pending"; enqueuedAt: number; payloadJson: string }
  | { id: string; status: "completed" | "failed" };

export type SubagentCompletionMutation =
  | { kind: "settle"; queueId: string; expected: SubagentRunRecord; subagent: SubagentRunRecord }
  | { kind: "block"; params: BlockSubagentCompletionRequest; now: number }
  | { kind: "reconcileCancelled"; expected: SubagentRunRecord; now: number }
  /** Retires a wake that owes no delivery; the outcome-bearing variant settles delivery. */
  | { kind: "requesterWake"; entries: readonly { subagent: SubagentRunRecord }[] }
  | {
      kind: "requesterBatch";
      entries: readonly { subagent: SubagentRunRecord }[];
      outcome: SubagentAnnounceDeliveryResult;
      now: number;
    };

export type SubagentCompletionMutationResult = {
  applied: boolean | null;
  records: Array<{ row: SubagentRunSqliteRow; cleanupHandled?: boolean }>;
  retiredRunIds: string[];
  queueIds: string[];
  queueReceipts?: SubagentCompletionQueueReceipt[];
};
