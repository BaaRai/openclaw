import "./subagent-announce.requester-settle-dispatch-mocks.test-support.js";
import { afterEach, describe, expect, it, onTestFinished, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../../../test/helpers/temp-dir.js";
import { createInternalAgentTurnFacade } from "../../../gateway/agent-turn/internal-facade.js";
import { registerChatAbortController } from "../../../gateway/chat-abort.js";
import { createGatewayMethodRegistry } from "../../../gateway/methods/registry.js";
import { createChatRunState } from "../../../gateway/server-chat-state.js";
import { waitForGatewayDispatch } from "../../../gateway/server-in-process-dispatch.js";
import type { GatewayRequestContext } from "../../../gateway/server-methods/types.js";
import { createSyntheticPluginRuntimeClient } from "../../../gateway/server-plugin-runtime-client.js";
import { getAgentEventLifecycleGeneration } from "../../../infra/agent-events.js";
import { withPluginRuntimeGatewayRequestScope } from "../../../plugins/runtime/gateway-request-scope.js";
import { withSessionTurn } from "../../../sessions/session-controller.admission.js";
import {
  captureSessionTarget,
  getCurrentSessionControllerClaim,
} from "../../../sessions/session-controller.lifecycle.js";
import { getExistingSessionControllerMailbox } from "../../../sessions/session-controller.mailbox.js";
import { markReplyOperationExecutionStarted } from "../../../sessions/session-controller.state.js";
import { rpcSourceTesting } from "../../../sessions/session-lifecycle-admission.test-support.js";
import { trackAsyncWork } from "../../../shared/async-work-scope.js";
import { createDeferredCore } from "../../../shared/deferred.js";
import { closeOpenClawAgentDatabasesAsync } from "../../../state/openclaw-agent-db.js";
import { prepareEmbeddedAttemptTimeout } from "../../embedded-agent-runner/run/attempt-timeout-prepare.js";
import { resolveAgentTimeoutMs } from "../../timeout.js";
import { consumeSubagentPauseNotice } from "../registry/subagent-delivery-state.js";
import type { SubagentRunRecord } from "../registry/subagent-registry.types.js";
import {
  registerRequesterFinalAttachment,
  promoteRequesterFinalAttachment,
} from "../requester-final-attachment.js";
import { sendSubagentAnnounceDirectly } from "./subagent-announce-direct-delivery.js";
import { setSubagentAnnounceDeliveryDepsForTest } from "./subagent-announce-overrides.test-support.js";
import {
  deliver,
  registryRead,
  startTurn,
  REQUESTER_KEY,
  settledChild,
  useRequesterSettleDispatchFixture,
} from "./subagent-announce.requester-settle-dispatch.test-support.js";
import { maybeWakeRequesterAfterAllChildrenSettled } from "./subagent-announce.requester-settle-wake.js";

const tempDirs = useAutoCleanupTempDirTracker((cleanup) =>
  afterEach(async () => {
    for (const dir of tempDirs.dirs) {
      await closeOpenClawAgentDatabasesAsync(dir);
    }
    cleanup();
  }),
);

const REQUESTER_TARGET = captureSessionTarget({
  storeScope: "/synthetic/requester-settle-dispatch/sessions.db",
  sessionKey: REQUESTER_KEY,
  incarnation: "requester-session",
  agentId: "main",
});
const REQUESTER_TURN = {
  target: REQUESTER_TARGET,
  sessionKey: REQUESTER_KEY,
  sessionId: "requester-session",
  agentId: "main",
};

function createContext(): GatewayRequestContext {
  const chatRunState = createChatRunState();
  const methodRegistry = createGatewayMethodRegistry([]);
  const context = Object.assign({} as GatewayRequestContext, {
    trackExecution: trackAsyncWork,
    agentRunSeq: new Map(),
    broadcast: vi.fn(),
    chatRunState,
    dedupe: new Map(),
    getRuntimeConfig: () => ({}),
    getGatewayMethodRegistry: () => methodRegistry,
    logGateway: { error: vi.fn(), warn: vi.fn() },
    nodeSendToSession: vi.fn(),
    removeChatRun: vi.fn(() => undefined),
  });
  context.createAgentTurnFacade = (principal) =>
    createInternalAgentTurnFacade({
      ...principal,
      getContext: () => context,
      getMethodRegistry: () => methodRegistry,
    });
  return context;
}

describe("requester settle dispatch deadline", () => {
  useRequesterSettleDispatchFixture();

  it.each([false, true])(
    "wakes a nested yielded requester once (child completed before yield=%s)",
    async (afterRequesterYield) => {
      const requesterSessionKey = "agent:main:subagent:middle";
      const requester: SubagentRunRecord = {
        ...settledChild(),
        runId: "yielded-requester",
        childSessionKey: requesterSessionKey,
        pauseReason: "sessions_yield",
        runTimeoutSeconds: 0,
      };
      registryRead.getLatestLiveSubagentRunByChildSessionKey.mockImplementation(
        (sessionKey, matches) =>
          sessionKey === requesterSessionKey && (!matches || matches(requester))
            ? requester
            : undefined,
      );
      const child = settledChild();
      child.requesterSessionKey = requesterSessionKey;
      child.requesterSettleWake = {
        batchRunIds: [child.runId],
        requesterYieldBatch: true,
        afterRequesterYield: afterRequesterYield ? true : undefined,
        rearmGeneration: 1,
      };
      registryRead.listSubagentRunsForRequester.mockReturnValue([child]);
      const append = vi.fn(() => true);
      const owner = {
        requesterAgentId: "main",
        requesterSessionKey,
        requesterSessionId: "requester-session",
        requesterTurnRunId: "yielded-requester",
      };
      const attachment = registerRequesterFinalAttachment({
        ...owner,
        lifecycleGeneration: getAgentEventLifecycleGeneration(),
        timeoutMs: 60_000,
        append,
      });
      onTestFinished(() => attachment.revoke());
      expect(
        promoteRequesterFinalAttachment({
          ...owner,
          batchRunIds: [child.runId],
          rearmGeneration: 1,
        }),
      ).toBe(true);
      const delivered = {
        delivered: true,
        path: "direct",
        finalAssistantVisibleText: "consolidated final",
      } as const;
      deliver.mockResolvedValue(delivered);
      const completeBatch = vi.fn<
        Parameters<typeof maybeWakeRequesterAfterAllChildrenSettled>[0]["completeBatch"]
      >((batch) => {
        for (const entry of batch) {
          entry.requesterSettleWake = undefined;
        }
      });
      const params = {
        isSourceCurrent: () => true,
        requesterSessionKey,
        settledEntry: child,
        completeBatch,
      };

      await expect(maybeWakeRequesterAfterAllChildrenSettled(params)).resolves.toBe(true);
      expect(deliver).toHaveBeenCalledWith(
        expect.objectContaining({
          targetRequesterSessionKey: requesterSessionKey,
          requesterIsSubagent: true,
          requireVisibleReply: true,
          sourceTool: "subagent_settle",
          triggerMessage: expect.stringContaining("child result"),
          directIdempotencyKey: `announce:requester-settle:main:${requesterSessionKey}:${child.runId}:yield-1`,
        }),
      );
      expect(completeBatch).toHaveBeenCalledWith([child], 1, delivered, expect.any(Function));
      expect(append).not.toHaveBeenCalled();
      const onCommitted = completeBatch.mock.calls[0]![3]!;
      onCommitted();
      onCommitted();
      expect(append).toHaveBeenCalledExactlyOnceWith(delivered.finalAssistantVisibleText);
      await expect(maybeWakeRequesterAfterAllChildrenSettled(params)).resolves.toBe(false);
      expect(deliver).toHaveBeenCalledOnce();
      expect(completeBatch).toHaveBeenCalledOnce();
    },
  );

  it("queues a pause notice behind the requester's current turn and delivers it once", async () => {
    vi.useFakeTimers();
    const context = createContext();
    const child = settledChild();
    child.pauseReason = "sessions_yield";
    child.execution.outcome = undefined;
    child.completion = { required: true };
    child.delivery = { status: "pending" };
    child.requesterSettleWake!.pauseNotice = { acknowledgment: "PAUSE-MARKER-mid-turn" };
    registryRead.listSubagentRunsForRequester.mockReturnValue([child]);
    const accepted = createDeferredCore();
    const releaseTurn = createDeferredCore();
    const currentTurnStarted = createDeferredCore();
    const received: string[] = [];
    const currentTurn = withSessionTurn(REQUESTER_TURN, async () => {
      currentTurnStarted.resolve();
      await releaseTurn.promise;
    });
    await currentTurnStarted.promise;
    startTurn.mockImplementation(async ({ controllerInput, preflight, io }) => {
      const request = preflight.request;
      io.emitAcceptance([true, { runId: request.idempotencyKey, status: "accepted" }], {
        runId: request.idempotencyKey,
      });
      const queued = withSessionTurn({ ...REQUESTER_TURN, controllerInput }, async () => {
        io.emitExecutionStarted?.();
        received.push(request.message);
      });
      accepted.resolve();
      await queued;
      io.emitFinal([
        true,
        { status: "ok", result: { payloads: [{ text: "Continuation needed." }] } },
      ]);
    });
    setSubagentAnnounceDeliveryDepsForTest({
      getRuntimeConfig: () => ({}),
      loadRequesterSessionEntry: () => ({
        cfg: {},
        canonicalKey: REQUESTER_KEY,
        agentId: "main",
        storePath: REQUESTER_TARGET.storeScope,
        entry: { sessionId: "requester-session", updatedAt: 1 },
      }),
      getRequesterSessionActivity: () => ({ sessionId: "requester-session", isActive: false }),
    });
    deliver.mockImplementation(sendSubagentAnnounceDirectly);
    const params = {
      requesterSessionKey: REQUESTER_KEY,
      isSourceCurrent: () => true,
      settledEntry: child,
      completeBatch: () => {
        consumeSubagentPauseNotice(child);
      },
    };
    const wake = withPluginRuntimeGatewayRequestScope(
      { context, client: createSyntheticPluginRuntimeClient(), isWebchatConnect: () => false },
      () => maybeWakeRequesterAfterAllChildrenSettled(params),
    );
    try {
      await accepted.promise;
      expect(received).toEqual([]);
      const mailbox = getExistingSessionControllerMailbox(REQUESTER_KEY, REQUESTER_TARGET);
      expect({
        activeCount: Number(Boolean(mailbox?.claim)),
        queuedCount: mailbox?.entries.filter((entry) => entry.phase !== "claimed").length ?? 0,
      }).toEqual({ activeCount: 1, queuedCount: 1 });
      releaseTurn.resolve();
      await currentTurn;
      await expect(wake).resolves.toBe(true);
      expect(received).toHaveLength(1);
      expect(received[0]).toContain("PAUSE-MARKER-mid-turn");
      expect(received[0]).toContain('"state":"paused"');
      expect(child.pauseReason).toBe("sessions_yield");
      await expect(maybeWakeRequesterAfterAllChildrenSettled(params)).resolves.toBe(false);
      expect(startTurn).toHaveBeenCalledOnce();
    } finally {
      releaseTurn.resolve();
      await currentTurn;
      await wake;
    }
  });

  it("rejects a replaced anchor after requester wake runtime loading", async () => {
    const retired = settledChild();
    const current = structuredClone(retired);
    registryRead.listSubagentRunsForRequester.mockReturnValue([current]);
    deliver.mockResolvedValue({ delivered: true, path: "direct" });
    const completeBatch = vi.fn();

    await expect(
      maybeWakeRequesterAfterAllChildrenSettled({
        isSourceCurrent: () => true,
        requesterSessionKey: REQUESTER_KEY,
        settledEntry: retired,
        completeBatch,
      }),
    ).resolves.toBe(false);
    expect(deliver).not.toHaveBeenCalled();
    expect(completeBatch).not.toHaveBeenCalled();
  });

  it.each(["final", "runtime timeout", "stop"] as const)(
    "keeps an executing completion under requester lifecycle ownership: %s",
    async (outcome) => {
      vi.useFakeTimers();
      vi.setSystemTime(10_000);
      const context = createContext();
      const child = settledChild();
      registryRead.listSubagentRunsForRequester.mockReturnValue([child]);
      const cfg = {
        agents: { defaults: { timeoutSeconds: 1, subagents: { announceTimeoutMs: 20 } } },
      };
      const executionStarted = createDeferredCore();
      const workDone = createDeferredCore();
      const stop = new AbortController();
      const timeoutMs = resolveAgentTimeoutMs({ cfg });
      const timedOut = vi.fn();
      const completeBatch = vi.fn();
      const finalReceipts: string[] = [];
      let acceptedSignal: AbortSignal | undefined;
      startTurn.mockImplementation(async ({ controllerInput, preflight, io }) => {
        const request = preflight.request as { idempotencyKey: string; sessionKey: string };
        const registration = registerChatAbortController({
          target: REQUESTER_TARGET,
          runId: request.idempotencyKey,
          sessionId: "requester-session",
          sessionKey: request.sessionKey,
          timeoutMs,
          kind: "agent",
          sourceInput: controllerInput,
        });
        acceptedSignal = registration.controller.signal;
        io.emitAcceptance([true, { runId: request.idempotencyKey, status: "accepted" }], {
          runId: request.idempotencyKey,
        });
        try {
          if (!registration.registered) {
            throw new Error("expected requester RPC source");
          }
          await withSessionTurn(
            {
              ...REQUESTER_TURN,
              controllerInput: registration.entry.input,
              abortSignal: registration.controller.signal,
            },
            async (operation) => {
              if (!operation) {
                throw new Error("expected requester operation");
              }
              markReplyOperationExecutionStarted(operation);
              operation.setPhase("running");
              io.emitExecutionStarted?.();
              const timeout = prepareEmbeddedAttemptTimeout({
                attempt: {
                  runId: request.idempotencyKey,
                  sessionId: "requester-session",
                  timeoutMs,
                },
                activeSession: { isCompacting: false, isStreaming: true },
                compactionState: { isCompacting: () => false },
                compactionTimeoutMs: 100,
                runAbortSignal: registration.controller.signal,
                isProbeSession: true,
                abortRun: () => registration.controller.abort(new Error("requester run timed out")),
                markTimedOutDuringCompaction: vi.fn(),
                markTimedOutByRunBudget: timedOut,
              });
              executionStarted.resolve();
              try {
                await waitForGatewayDispatch(
                  "synthetic requester work",
                  workDone.promise,
                  undefined,
                  registration.controller.signal,
                );
                finalReceipts.push("consolidated requester final");
              } finally {
                timeout.clearTimers();
              }
            },
          );
          io.emitFinal([
            true,
            { status: "ok", result: { payloads: [{ text: finalReceipts[0] }] } },
          ]);
        } finally {
          const claim = registration.entry?.input.claim;
          registration.cleanup();
          await registration.entry?.input.settlement.promise;
          await claim?.settlement.promise;
        }
      });
      setSubagentAnnounceDeliveryDepsForTest({
        getRuntimeConfig: () => cfg,
        loadRequesterSessionEntry: () => ({
          cfg,
          canonicalKey: REQUESTER_KEY,
          agentId: "main",
          storePath: REQUESTER_TARGET.storeScope,
          entry: { sessionId: "requester-session", updatedAt: 1 },
        }),
        getRequesterSessionActivity: () => ({ sessionId: "requester-session", isActive: false }),
      });
      deliver.mockImplementation(sendSubagentAnnounceDirectly);
      const wake = withPluginRuntimeGatewayRequestScope(
        {
          context,
          client: createSyntheticPluginRuntimeClient(),
          isWebchatConnect: () => false,
        },
        () =>
          maybeWakeRequesterAfterAllChildrenSettled({
            isSourceCurrent: () => true,
            requesterSessionKey: REQUESTER_KEY,
            settledEntry: child,
            signal: stop.signal,
            completeBatch,
          }),
      );
      try {
        await executionStarted.promise;
        await vi.advanceTimersByTimeAsync(21);
        expect(acceptedSignal?.aborted).toBe(false);
        expect(finalReceipts).toEqual([]);
        expect(completeBatch).not.toHaveBeenCalled();
        if (outcome === "final") {
          workDone.resolve();
        } else if (outcome === "runtime timeout") {
          await vi.advanceTimersByTimeAsync(timeoutMs);
        } else {
          stop.abort(new Error("requester stopped"));
        }
        await expect(wake).resolves.toBe(outcome === "final");
        expect(startTurn).toHaveBeenCalledOnce();
        expect(deliver).toHaveBeenCalledOnce();
        expect(timedOut).toHaveBeenCalledTimes(outcome === "runtime timeout" ? 1 : 0);
        expect(finalReceipts).toEqual(outcome === "final" ? ["consolidated requester final"] : []);
        if (outcome === "final") {
          expect(completeBatch).toHaveBeenCalledWith(
            [child],
            1,
            expect.objectContaining({ delivered: true, requesterVisibleFinalDelivered: true }),
            expect.any(Function),
          );
        } else {
          expect(acceptedSignal?.aborted).toBe(true);
          expect(completeBatch).not.toHaveBeenCalled();
          expect(child.requesterSettleWake).toMatchObject({ status: "pending", attemptCount: 1 });
        }
        const later = vi.fn();
        let laterClaim: ReturnType<typeof getCurrentSessionControllerClaim>;
        await withSessionTurn(REQUESTER_TURN, async () => {
          laterClaim = getCurrentSessionControllerClaim();
          later();
        });
        await laterClaim?.settlement.promise;
        expect(later).toHaveBeenCalledOnce();
        const mailbox = getExistingSessionControllerMailbox(REQUESTER_KEY, REQUESTER_TARGET);
        expect(mailbox?.claim).toBeUndefined();
        expect(mailbox?.entries ?? []).toEqual([]);
        expect(rpcSourceTesting.size).toBe(0);
        expect(child.execution.outcome).toEqual({ status: "ok" });
        expect(child.completion?.resultText).toBe("child result");
      } finally {
        workDone.resolve();
        stop.abort();
        await wake;
      }
    },
  );
});
