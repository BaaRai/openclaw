import { sessionDeliveryChannel } from "../../../utils/delivery-context.read.js";
import type { EmbeddedAgentQueueMessageOptions } from "../../embedded-agent-runner/run-state.js";
import {
  queueEmbeddedAgentMessageWithOutcomeAsync,
  queueGuardedEmbeddedAgentMessageWithOutcomeAsync,
  resolveEmbeddedRunAbandonment,
  type EmbeddedAgentQueueMessageOutcome,
} from "../../embedded-agent-runner/runs.js";
import type { CurrentInboundPromptContext } from "../../internal-runtime-context.js";
import {
  getSubagentRequesterSessionActivity as resolveRequesterSessionActivity,
  loadRequesterSessionEntry,
  resolveQueueSettings,
} from "./subagent-announce-delivery.runtime.js";

export const SOURCE_OWNER_CHANGED = Symbol("source_owner_changed");

export { resolveRequesterSessionActivity };

/**
 * One steer attempt into the active requester run. A refusal, including compaction,
 * leaves the completion to its queued requester turn; nothing re-steers on a timer.
 */
export async function resolveActiveWake(
  sessionId: string,
  message: string,
  wakeOptions: EmbeddedAgentQueueMessageOptions,
  isAttemptAllowed?: () => boolean,
  isSourceSessionAdmissionAllowed?: () => boolean,
): Promise<EmbeddedAgentQueueMessageOutcome | typeof SOURCE_OWNER_CHANGED> {
  if (isAttemptAllowed?.() === false || isSourceSessionAdmissionAllowed?.() === false) {
    return SOURCE_OWNER_CHANGED;
  }
  const result = isSourceSessionAdmissionAllowed
    ? await queueGuardedEmbeddedAgentMessageWithOutcomeAsync(
        sessionId,
        message,
        wakeOptions,
        () => isAttemptAllowed?.() !== false && isSourceSessionAdmissionAllowed(),
      )
    : await queueEmbeddedAgentMessageWithOutcomeAsync(sessionId, message, wakeOptions);
  if (
    isAttemptAllowed?.() === false ||
    (!result.queued && isSourceSessionAdmissionAllowed?.() === false)
  ) {
    return SOURCE_OWNER_CHANGED;
  }
  return result;
}

export async function maybeSteerSubagentAnnounce(params: {
  deliveryTimeoutMs?: number;
  requesterSessionKey: string;
  requesterAgentId?: string;
  steerMessage: string;
  currentInboundContext?: CurrentInboundPromptContext;
  signal?: AbortSignal;
  isSourceSessionEffectsAllowed?: () => boolean;
  isSourceSessionAdmissionAllowed?: () => boolean;
}): Promise<
  | { status: "steered"; deliveredAt?: number; enqueuedAt?: number }
  | { status: "none" | "dropped" | "source_owner_changed" }
> {
  if (params.signal?.aborted) {
    return { status: "none" };
  }
  const requester = loadRequesterSessionEntry(params.requesterSessionKey, params.requesterAgentId);
  const { cfg, entry, canonicalKey } = requester;
  const { sessionId, isActive } = resolveRequesterSessionActivity(
    params.requesterSessionKey,
    requester,
  );
  if (resolveEmbeddedRunAbandonment({ sessionKey: canonicalKey, sessionId })) {
    return { status: "none" };
  }
  if (!sessionId || !isActive) {
    return { status: "none" };
  }

  const queueSettings = resolveQueueSettings({
    cfg,
    channel: sessionDeliveryChannel(entry),
    sessionEntry: entry,
  });

  // Subagent announcements are internal handoffs into an active requester turn.
  // Queue modes such as followup/collect apply to user prompts, not this path.
  const queueOptions: EmbeddedAgentQueueMessageOptions = {
    deliveryTimeoutMs: params.deliveryTimeoutMs,
    steeringMode: "all",
    ...(queueSettings.debounceMs !== undefined ? { debounceMs: queueSettings.debounceMs } : {}),
    waitForTranscriptCommit: true,
    ...(params.currentInboundContext
      ? { currentInboundContext: params.currentInboundContext }
      : {}),
  };
  const queueOutcome = await resolveActiveWake(
    sessionId,
    params.steerMessage,
    queueOptions,
    params.isSourceSessionEffectsAllowed,
    params.isSourceSessionAdmissionAllowed,
  );
  if (queueOutcome === SOURCE_OWNER_CHANGED) {
    return { status: "source_owner_changed" };
  }
  if (queueOutcome.queued) {
    return {
      status: "steered",
      deliveredAt: queueOutcome.deliveredAtMs,
      enqueuedAt: queueOutcome.enqueuedAtMs,
    };
  }

  // A stale_run refusal means the requester run is evidence-dead: it will not
  // drain its steer queue, so "dropped" would discard the handoff. Report
  // not-active so dispatch takes the direct fallback instead.
  // Unguarded sinks likewise leave source-bound input to the direct Gateway path.
  if (
    queueOutcome.reason === "stale_run" ||
    queueOutcome.reason === "transcript_commit_wait_unsupported" ||
    (params.isSourceSessionAdmissionAllowed !== undefined &&
      queueOutcome.reason === "guarded_injection_unsupported")
  ) {
    return { status: "none" };
  }
  const currentActivity = resolveRequesterSessionActivity(
    params.requesterSessionKey,
    loadRequesterSessionEntry(params.requesterSessionKey, params.requesterAgentId),
  );
  return { status: currentActivity.isActive ? "dropped" : "none" };
}
