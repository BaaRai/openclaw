import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import {
  commitMainSessionRecovery,
  type MainSessionRecoveryPendingTarget,
} from "../../agents/main-session-recovery/main-session-recovery-store.js";
import { settleAcceptedRestartRecovery } from "../../agents/main-session-recovery/main-session-restart-dispatch-settlement.js";
import type { registerChatAbortController } from "../chat-abort.js";

/**
 * Bind durable recovery admission to the exact outcome recorded if execution never starts.
 * A failed start is restored as owed. A resend cancelled by Stop or an interrupt is
 * recorded as a stopped run instead, so it is neither replayed nor charged again.
 */
export async function admitAgentRestartRecovery(params: {
  lifecycleGeneration: string;
  runId: string;
  sessionId: string;
  sessionKey: string;
  storePath: string;
  activeRunAbort: ReturnType<typeof registerChatAbortController>;
}): Promise<() => Promise<MainSessionRecoveryPendingTarget | undefined>> {
  const admission = await commitMainSessionRecovery({
    command: {
      kind: "admit_recovery",
      lifecycleGeneration: params.lifecycleGeneration,
      now: Date.now(),
      runId: params.runId,
      sessionId: params.sessionId,
    },
    requireWriteSuccess: true,
    target: { sessionKey: params.sessionKey, storePath: params.storePath },
  });
  if (admission.transition.kind !== "admitted_recovery") {
    throw new Error(
      `Session "${params.sessionKey}" restart recovery reservation is stale; recovery was skipped.`,
    );
  }
  const sessionKey = admission.sessionKey ?? params.sessionKey;
  const admittedAttempt = admission.transition.admission;
  const sourceRunId = normalizeOptionalString(admission.entry?.restartRecoveryDeliverySourceRunId);
  let restored = false;
  return async () => {
    if (restored) {
      return undefined;
    }
    // Stop and interrupt cancel as "rpc"; restart and start timeouts leave the resend owed.
    const { controller, entry } = params.activeRunAbort;
    if (controller.signal.aborted && (entry?.adapter.abortStopReason?.trim() || "rpc") === "rpc") {
      // Matches the queue-abort result the dispatcher observes, which settles idempotently.
      await settleAcceptedRestartRecovery({
        expectedRecoveryRunId: params.runId,
        expectedRecoverySourceRunId: sourceRunId,
        expectedSessionId: params.sessionId,
        lifecycleGeneration: params.lifecycleGeneration,
        sessionKey,
        sessionKeys: [sessionKey],
        storePath: params.storePath,
        terminalStatus: "timeout",
      });
      restored = true;
      return undefined;
    }
    const recovery = await commitMainSessionRecovery({
      command: {
        kind: "mark_admitted_recovery_interrupted",
        ...admittedAttempt,
        now: Date.now(),
      },
      requireWriteSuccess: true,
      target: { sessionKey, storePath: params.storePath },
    });
    restored = true;
    return (recovery.transition.kind === "applied" || recovery.transition.kind === "no_change") &&
      recovery.entry?.sessionId === params.sessionId &&
      recovery.sessionKey
      ? {
          sessionId: recovery.entry.sessionId,
          sessionKey: recovery.sessionKey,
          storePath: params.storePath,
        }
      : undefined;
  };
}
