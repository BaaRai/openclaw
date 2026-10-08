import { getSessionDiffBaselineCapture } from "../sessions/session-diff-capture.js";
import type { HookRunner } from "./hooks.js";

/** Synchronous transcript hooks need admission before callback invocation. */
export async function prepareSessionHookRunner(
  runner: HookRunner | null,
): Promise<HookRunner | null> {
  if (runner?.hasHooks("before_message_write")) {
    await getSessionDiffBaselineCapture();
  }
  return runner;
}
