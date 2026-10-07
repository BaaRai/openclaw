import { afterEach, describe, expect, it, vi } from "vitest";
import { testing as replyRunTesting } from "../../auto-reply/reply/reply-run-registry.test-support.js";
import { resetDiagnosticRunActivityForTest } from "../../logging/diagnostic-run-activity.js";
import { waitForSessionRunEnd as waitForEmbeddedAgentRunEnd } from "../../sessions/session-controller.native-runtime.js";
import { createDeferredCore } from "../../shared/deferred.js";
import {
  claimPendingEmbeddedAgentQuestionAnswer,
  preemptAndDrainEmbeddedHeartbeatRun,
  type EmbeddedAgentQueueHandle,
  type EmbeddedAgentQueueMessageOptions,
} from "./runs.js";
import {
  clearTestEmbeddedRun as clearActiveEmbeddedRun,
  registerTestEmbeddedRun as setActiveEmbeddedRun,
  createEmbeddedRunHandle,
  testing,
} from "./runs.test-support.js";

const sessionId = "session";
function start(overrides: Partial<EmbeddedAgentQueueHandle> = {}) {
  const handle = {
    ...createEmbeddedRunHandle(),
    queueMessage: vi.fn(async () => {}),
    ...overrides,
  };
  setActiveEmbeddedRun(sessionId, handle);
  return handle;
}
afterEach(() => {
  testing.resetActiveEmbeddedRuns();
  resetDiagnosticRunActivityForTest();
  replyRunTesting.resetReplyRunRegistry();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("embedded-agent active-run question answers and waits", () => {
  it.each([true, false])("claims pending answers without steering: %s", async (claimed) => {
    const queueMessage = vi.fn(async () => {});
    start({
      runId: "question-owner",
      queueMessage,
      messageInjectionV2: {
        version: 2,
        isAvailable: () => true,
        queueMessage,
        claimPendingUserInputAnswer: async (_text, options, assertCurrent) => {
          assertCurrent();
          return options?.isInboundUserMessage === true && claimed;
        },
      },
    });
    await expect(claimPendingEmbeddedAgentQuestionAnswer(sessionId, "Green")).resolves.toEqual(
      claimed ? { runId: "question-owner" } : null,
    );
    expect(queueMessage).not.toHaveBeenCalled();
  });

  it.each(["availability", "claim"] as const)(
    "rejects backend replacement during question %s",
    async (stage) => {
      const entered = createDeferredCore(),
        released = createDeferredCore();
      const queueMessage = vi.fn(async () => {}),
        replacement = createEmbeddedRunHandle({ runId: "replacement", queueMessage });
      const claim = vi.fn(
        async (
          _text: string,
          _options: EmbeddedAgentQueueMessageOptions | undefined,
          assertCurrent: () => void,
        ) => {
          entered.resolve();
          await released.promise;
          assertCurrent();
          return true;
        },
      );
      start({
        runId: "question-owner",
        queueMessage,
        messageInjectionV2: {
          version: 2,
          queueMessage,
          claimPendingUserInputAnswer: claim,
          isAvailable: () => {
            if (stage === "availability") {
              setActiveEmbeddedRun(sessionId, replacement);
            }
            return true;
          },
        },
      });
      const result = claimPendingEmbeddedAgentQuestionAnswer(sessionId, "Green");
      try {
        if (stage === "availability") {
          await expect(result).resolves.toBeNull();
          expect(claim).not.toHaveBeenCalled();
        } else {
          await entered.promise;
          setActiveEmbeddedRun(sessionId, replacement);
          released.resolve();
          await expect(result).rejects.toThrow("Message injection authority is no longer current");
        }
        expect(queueMessage).not.toHaveBeenCalled();
      } finally {
        released.resolve();
      }
    },
  );

  it("keeps handle and session waiters distinct through replacements", async () => {
    vi.useFakeTimers();
    const preempt = vi.fn(() => true),
      visibleAbort = vi.fn();
    const heartbeat = start({ isAbortable: () => false, preemptByVisibleTurn: preempt });
    const replacement = createEmbeddedRunHandle({ abort: visibleAbort });
    setActiveEmbeddedRun("visible", replacement);
    const heartbeatWait = preemptAndDrainEmbeddedHeartbeatRun(sessionId, 1_000);
    const sessionWait = waitForEmbeddedAgentRunEnd(sessionId, null);
    let heartbeatDrained = false,
      sessionDrained = false;
    void heartbeatWait.then(() => {
      heartbeatDrained = true;
    });
    void sessionWait.then(() => {
      sessionDrained = true;
    });
    await expect(preemptAndDrainEmbeddedHeartbeatRun("visible", 1_000)).resolves.toBe(
      "not-heartbeat",
    );
    setActiveEmbeddedRun(sessionId, replacement);
    await Promise.resolve();
    expect(heartbeatDrained).toBe(false);
    clearActiveEmbeddedRun(sessionId, heartbeat);
    await expect(heartbeatWait).resolves.toBe("drained");
    expect(sessionDrained).toBe(false);
    clearActiveEmbeddedRun(sessionId, replacement);
    const successor = createEmbeddedRunHandle();
    setActiveEmbeddedRun(sessionId, successor);
    await Promise.resolve();
    await Promise.resolve();
    expect(sessionDrained).toBe(false);
    clearActiveEmbeddedRun(sessionId, successor);
    await expect(sessionWait).resolves.toBe(true);
    expect(preempt).toHaveBeenCalledOnce();
    expect(visibleAbort).not.toHaveBeenCalled();
  });
});
