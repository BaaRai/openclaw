import type { OpenClawConfig } from "../../config/types.openclaw.js";
import {
  getRpcSourceIdentity,
  type RpcSourceRef,
} from "../../sessions/session-controller.rpc-sources.js";
import {
  captureSessionControllerStop,
  stopSession,
  type SessionStopHookContext,
} from "../../sessions/session-controller.stop.js";
import {
  abortChatRunById,
  captureChatRunAbortPresentation,
  type ChatAbortOps,
} from "../chat-abort.js";
import { abortControlledSubagents } from "./chat-abort-descendants.js";

type ExactClientRunStopResult = {
  aborted: boolean;
  descendants?: Awaited<ReturnType<typeof abortControlledSubagents>>;
  failure?: { error: unknown };
};

/**
 * Stops one exact Gateway RPC run through Stop source `client-run`: its source
 * input, its chat presentation, and that turn's controlled subagents.
 */
export async function stopExactClientRun(params: {
  ops: ChatAbortOps;
  cfg: OpenClawConfig;
  runId: string;
  active: RpcSourceRef;
  assertCurrent?: () => void;
  hookContext: Omit<SessionStopHookContext, "sessionKey" | "sessionId">;
  /** Runs once the parent cancellation result is known. */
  afterParent?: () => void;
  /** Runs after the chat abort is prepared, before it commits. */
  onAbortPrepared?: () => (() => void) | void;
}): Promise<ExactClientRunStopResult> {
  const { ops, runId, active } = params;
  // Capture the input and presentation synchronously, before any await can select a successor.
  const capture = captureSessionControllerStop({ inputs: [active.input] });
  const presentation = captureChatRunAbortPresentation(ops, runId);
  const { sessionKey, sessionId, agentId } = getRpcSourceIdentity(active);
  const result: ExactClientRunStopResult = { aborted: false };
  try {
    await stopSession({
      source: "client-run",
      capture,
      assertCurrent: params.assertCurrent,
      reason: "rpc",
      hookContext: { ...params.hookContext, sessionKey, sessionId },
      afterParent: params.afterParent,
      onCancelled: (target) => {
        if (target === active.input) {
          result.aborted = true;
        }
      },
      cancelInput: (_input, cancel) =>
        abortChatRunById(ops, {
          runId,
          sessionKey,
          expectedEntry: active,
          presentation,
          cancel,
          assertCurrent: params.assertCurrent,
          stopReason: "rpc",
          onAbortPrepared: params.onAbortPrepared,
          onAbortCommitted: () => {
            result.aborted = true;
          },
        }).aborted,
      stopChildren: async (applyParentStop) => {
        result.descendants = await abortControlledSubagents({
          cfg: params.cfg,
          sessionKey,
          agentId,
          requesterTurnRunId: runId,
          beforeKill: applyParentStop,
        });
        return {
          stopped: result.descendants?.killed ?? 0,
          failed: result.descendants?.status === "error" ? result.descendants.failed : 0,
        };
      },
    }).completed;
  } catch (error) {
    result.failure = { error };
  }
  return result;
}
