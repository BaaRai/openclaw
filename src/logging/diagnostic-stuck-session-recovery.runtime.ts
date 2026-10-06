import { diagnosticLogger as diag } from "./diagnostic-runtime.js";
import {
  formatRecoveryOutcome,
  type StuckSessionRecoveryOutcome,
  type StuckSessionRecoveryRequest,
} from "./diagnostic-session-recovery.js";

/** Diagnostics may request a tick, but never decide admission, cancel by ID or clear custody. */
export async function recoverStuckDiagnosticSession(
  params: StuckSessionRecoveryRequest,
): Promise<StuckSessionRecoveryOutcome> {
  const operation = params.operation;
  const base = { sessionId: params.sessionId, sessionKey: params.sessionKey };
  if (!operation) {
    return { ...base, status: "skipped", action: "observe_only", reason: "missing_session_ref" };
  }
  try {
    const decision = await operation.watchdog.tick();
    const state = operation.watchdog.snapshot();
    const outcome: StuckSessionRecoveryOutcome =
      state.recovery?.status === "settled" && decision.action === "stop"
        ? {
            ...base,
            status: "aborted",
            action: "abort_embedded_run",
            aborted: true,
            drained: true,
            forceCleared: false,
            released: 0,
          }
        : decision.action === "blocked" || state.recovery?.status === "blocked"
          ? {
              ...base,
              status: "failed",
              action: "none",
              reason: "exception",
              error:
                state.recovery?.error ??
                "Watchdog cleanup blocked: exact owner retains write-capable work",
            }
          : {
              ...base,
              status: "skipped",
              action: "observe_only",
              reason:
                state.recovery?.status === "settled" && decision.action === "expire_cleanup"
                  ? "terminal_outcome_committed"
                  : !state.current
                    ? "stale_session_state"
                    : "active_reply_work",
            };
    diag.warn("stuck session recovery outcome: " + formatRecoveryOutcome(outcome));
    return outcome;
  } catch (error) {
    return { ...base, status: "failed", action: "none", reason: "exception", error: String(error) };
  }
}
