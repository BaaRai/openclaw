// Key-only Talk cancel (Discord voice) goes through session Stop with source `talk`.
import { afterEach, expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import { testing as replyTesting } from "../auto-reply/reply/reply-run-registry.test-support.js";
import { registerInternalHook, unregisterInternalHook } from "../hooks/internal-hooks.js";
import { withSessionTurn } from "../sessions/session-controller.admission.js";
import { controlRealtimeVoiceAgentRun } from "./agent-run-control.js";

const sessionKey = "agent:main:discord-voice";
const turn = { sessionKey, sessionId: "voice-session", agentId: "main", storePath: "/tmp/talk" };

afterEach(() => {
  replyTesting.resetReplyRunRegistry();
});

it("aborts the active turn, keeps waiting inputs, and fires command:stop once", async () => {
  const hook = vi.fn();
  registerInternalHook("command:stop", hook);
  const started = createDeferred();
  const active = withSessionTurn(turn, async (_operation, signal) => {
    started.resolve();
    await new Promise((_resolve, reject) => {
      signal.addEventListener("abort", () => reject(new Error("turn aborted")), { once: true });
    });
  });
  void active.catch(() => {});
  await started.promise;
  const waitingRan = vi.fn();
  const waiting = withSessionTurn(turn, async () => waitingRan());

  try {
    const result = await controlRealtimeVoiceAgentRun({ sessionKey, text: "stop", mode: "cancel" });

    expect(result).toMatchObject({ ok: true, mode: "cancel", aborted: true });
    await expect(active).rejects.toBeDefined();
    await waiting;
    expect(waitingRan).toHaveBeenCalledOnce();
    expect(hook).toHaveBeenCalledOnce();
    expect(hook.mock.calls[0]?.[0]).toMatchObject({
      type: "command",
      action: "stop",
      sessionKey,
      context: { sessionId: "voice-session", commandSource: "talk" },
    });
  } finally {
    unregisterInternalHook("command:stop", hook);
  }
});
