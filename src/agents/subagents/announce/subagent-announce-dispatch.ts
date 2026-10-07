type SubagentDeliveryPath = "steered" | "direct" | "queued" | "none";
type SubagentAnnounceDeliveryDisposition =
  | "delivered"
  | "session_queued"
  | "intentional_non_delivery"
  | "retryable"
  | "ambiguous"
  | "permanent_failure";
type SubagentAnnounceDeliveryFailureReason =
  | "completion_handoff_pending"
  | "completion_handoff_unavailable"
  | "delivery_suppressed"
  | "generated_media_missing"
  | "message_tool_delivery_missing"
  | "requester_abandoned"
  | "source_owner_changed"
  | "visible_reply_missing";

export type SubagentAnnounceDeliveryResult = {
  delivered: boolean;
  path: SubagentDeliveryPath;
  deliveredAt?: number;
  enqueuedAt?: number;
  /** Direct delivery that already committed the requester's visible final. */
  requesterVisibleFinalDelivered?: true;
  storeReplaced?: true;
  /** Bounded visible final returned by the direct requester synthesis turn. */
  finalAssistantVisibleText?: string;
  reason?: SubagentAnnounceDeliveryFailureReason;
  error?: string;
  // Stops fallback delivery when ownership changed or another terminal result
  // makes trying a second path unsafe.
  terminal?: boolean;
  disposition?: SubagentAnnounceDeliveryDisposition;
  missingMediaUrls?: string[];
};

export function sourceOwnerChangedResult(): SubagentAnnounceDeliveryResult {
  return {
    delivered: false,
    path: "none",
    reason: "source_owner_changed",
    error: "subagent source lifecycle changed before completion delivery",
    terminal: true,
    disposition: "intentional_non_delivery",
  };
}
