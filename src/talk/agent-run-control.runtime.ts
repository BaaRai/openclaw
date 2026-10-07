import { getActiveNativeAttempt } from "../agents/embedded-agent-runner/run-state.js";
import {
  queueEmbeddedAgentMessageWithOutcomeAsync,
  queueGuardedEmbeddedAgentMessageWithOutcomeAsync,
  resolveActiveEmbeddedRunOwnerByRunId,
} from "../agents/embedded-agent-runner/runs.js";
import { getRuntimeConfig } from "../config/config.js";
import { abortControlledSubagents } from "../gateway/server-methods/chat-abort-descendants.js";
import { getDiagnosticSessionActivitySnapshot } from "../logging/diagnostic-run-activity.js";
import { resolveActiveReplyRunOwnerForSignal } from "../sessions/session-controller.barriers.js";
import { resolveActiveSessionRunId } from "../sessions/session-controller.queries.js";
import { findSessionControllerEntry } from "../sessions/session-controller.state.js";
import { captureSessionControllerStop, stopSession } from "../sessions/session-controller.stop.js";

/**
 * Key-only Talk cancel: Stop source `talk` aborts the session's current turn and
 * that turn's subagents, and keeps the session's waiting inputs.
 */
async function stopRealtimeVoiceSessionRun(params: {
  sessionKey: string;
  sessionId: string;
}): Promise<boolean> {
  const entry = findSessionControllerEntry(params.sessionKey);
  const operation = entry?.active;
  if (!operation || operation.sessionId !== params.sessionId) {
    return false;
  }
  // Capture this exact turn and its claimed inputs before Stop can yield.
  const capture = captureSessionControllerStop({
    inputs: entry.mailbox?.entries.filter((input) => input.claim?.operation === operation),
    operations: [operation],
  });
  const turnRunId = getActiveNativeAttempt(params.sessionId)?.runId;
  const outcome = await stopSession({
    source: "talk",
    capture,
    hookContext: { ...params, commandSource: "talk" },
    stopChildren: async (applyParentStop) => {
      if (!turnRunId) {
        await applyParentStop();
        return { stopped: 0, failed: 0 };
      }
      const descendants = await abortControlledSubagents({
        cfg: getRuntimeConfig(),
        sessionKey: params.sessionKey,
        agentId: operation.agentId,
        requesterTurnRunId: turnRunId,
        beforeKill: applyParentStop,
      });
      return {
        stopped: descendants?.killed ?? 0,
        failed: descendants?.status === "error" ? descendants.failed : 0,
      };
    },
  }).completed;
  return outcome.aborted;
}

export const realtimeVoiceControlRuntime = {
  stopRealtimeVoiceSessionRun,
  queueEmbeddedAgentMessageWithOutcomeAsync,
  queueGuardedEmbeddedAgentMessageWithOutcomeAsync,
  resolveActiveEmbeddedRunOwnerByRunId,
  resolveActiveSessionRunId,
  resolveActiveReplyRunOwnerForSignal,
  getDiagnosticSessionActivitySnapshot,
};
