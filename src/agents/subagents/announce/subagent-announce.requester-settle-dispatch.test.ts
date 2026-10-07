import "./subagent-announce.requester-settle-dispatch-mocks.test-support.js";
import { describe, expect, it, onTestFinished, vi } from "vitest";
import { createInternalAgentTurnFacade } from "../../../gateway/agent-turn/internal-facade.js";
import { registerChatAbortController } from "../../../gateway/chat-abort.js";
import { createGatewayMethodRegistry } from "../../../gateway/methods/registry.js";
import { createChatRunState } from "../../../gateway/server-chat-state.js";
import { waitForGatewayDispatch } from "../../../gateway/server-in-process-dispatch.js";
import type { GatewayRequestContext } from "../../../gateway/server-methods/types.js";
import { dispatchGatewayMethodInProcess } from "../../../gateway/server-plugin-in-process-dispatch.js";
import { createSyntheticPluginRuntimeClient } from "../../../gateway/server-plugin-runtime-client.js";
import { getAgentEventLifecycleGeneration } from "../../../infra/agent-events.js";
import {
  bindGatewayContextResolver,
  withPluginRuntimeGatewayRequestScope,
} from "../../../plugins/runtime/gateway-request-scope.js";
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
import { createTestAdmittedRunContext } from "../../admitted-run-context.test-support.js";
import { prepareEmbeddedAttemptTimeout } from "../../embedded-agent-runner/run/attempt-timeout-prepare.js";
import { createEmbeddedRunLaneController } from "../../embedded-agent-runner/run/lane-controller.js";
import type { RunEmbeddedAgentParams } from "../../embedded-agent-runner/run/params.js";
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
  publishWakeTransition,
  useRequesterSettleDispatchFixture,
} from "./subagent-announce.requester-settle-dispatch.test-support.js";
import { maybeWakeRequesterAfterAllChildrenSettled } from "./subagent-announce.requester-settle-wake.js";

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
const GLOBAL_LANE = "subagent-settle-dispatch-proof";

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
        status: "pending",
        attemptCount: 0,
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
        transitionBatch: publishWakeTransition,
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
      transitionBatch: publishWakeTransition,
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
    const transitionBatch = vi.fn();
    const completeBatch = vi.fn();

    await expect(
      maybeWakeRequesterAfterAllChildrenSettled({
        isSourceCurrent: () => true,
        requesterSessionKey: REQUESTER_KEY,
        settledEntry: retired,
        transitionBatch,
        completeBatch,
      }),
    ).resolves.toBe(false);
    expect(deliver).not.toHaveBeenCalled();
    expect(transitionBatch).not.toHaveBeenCalled();
    expect(completeBatch).not.toHaveBeenCalled();
  });

  it("preserves the final attempt when its Gateway closes during runtime loading", async () => {
    const retired = settledChild();
    retired.requesterSettleWake!.attemptCount = 2;
    const pendingState = structuredClone(retired.requesterSettleWake);
    const firstContext = createContext();
    let firstOpen = true;
    bindGatewayContextResolver(retired, () => (firstOpen ? firstContext : undefined));
    registryRead.listSubagentRunsForRequester.mockReturnValue([retired]);
    deliver.mockResolvedValue({ delivered: true, path: "direct" });
    const transitionBatch = vi.fn(publishWakeTransition);
    const completeBatch = vi.fn();
    const loaded = createDeferredCore();
    const pending = loaded.promise.then(() =>
      maybeWakeRequesterAfterAllChildrenSettled({
        isSourceCurrent: () => true,
        requesterSessionKey: REQUESTER_KEY,
        settledEntry: retired,
        transitionBatch,
        completeBatch,
      }),
    );
    firstOpen = false;
    loaded.resolve();
    await expect(pending).resolves.toBe(false);
    expect(deliver).not.toHaveBeenCalled();
    expect(transitionBatch).not.toHaveBeenCalled();
    expect(completeBatch).not.toHaveBeenCalled();
    expect(retired.requesterSettleWake).toEqual(pendingState);

    const replacement = structuredClone(retired);
    const nextContext = createContext();
    bindGatewayContextResolver(replacement, () => nextContext);
    registryRead.listSubagentRunsForRequester.mockReturnValue([replacement]);
    await expect(
      maybeWakeRequesterAfterAllChildrenSettled({
        isSourceCurrent: () => true,
        requesterSessionKey: REQUESTER_KEY,
        settledEntry: replacement,
        transitionBatch,
        completeBatch,
      }),
    ).resolves.toBe(true);
    expect(deliver).toHaveBeenCalledOnce();
    expect(transitionBatch).toHaveBeenCalledWith(
      [replacement],
      expect.objectContaining({ attemptCount: 3 }),
      expect.any(Function),
    );
    expect(completeBatch).toHaveBeenCalledOnce();
  });

  it.each(["bound", "throwing", "incompatible", "unbound"] as const)(
    "replaces a %s batch only after its owner closes",
    async (binding) => {
      const retired = settledChild();
      const sibling = {
        ...structuredClone(retired),
        runId: "settled-sibling",
        childSessionKey: "agent:main:subagent:settled-sibling",
      };
      const retiredBatch = [retired, sibling];
      const firstContext = createContext();
      const replacementContext = createContext();
      let firstOpen = true;
      if (binding !== "unbound") {
        retiredBatch.forEach((entry, index) =>
          bindGatewayContextResolver(entry, () => {
            if (!firstOpen && binding === "throwing") {
              throw new Error("old Gateway resolver closed");
            }
            return firstOpen
              ? firstContext
              : binding === "incompatible"
                ? index === 0
                  ? firstContext
                  : replacementContext
                : undefined;
          }),
        );
      }
      registryRead.listSubagentRunsForRequester.mockReturnValue(retiredBatch);
      const oldDone = createDeferredCore<{ delivered: true; path: "direct" }>();
      const replacementDone = createDeferredCore<{ delivered: true; path: "direct" }>();
      deliver
        .mockImplementationOnce(async () => await oldDone.promise)
        .mockImplementationOnce(async () => await replacementDone.promise);
      const wake = (entry: SubagentRunRecord) =>
        maybeWakeRequesterAfterAllChildrenSettled({
          isSourceCurrent: () => true,
          requesterSessionKey: REQUESTER_KEY,
          settledEntry: entry,
          transitionBatch: publishWakeTransition,
          completeBatch: (batch) => {
            batch.forEach((member) => {
              member.requesterSettleWake = undefined;
            });
          },
        });
      const oldWake = wake(retired);
      let replacementWake: Promise<boolean> | undefined;
      try {
        await vi.waitFor(() => expect(deliver).toHaveBeenCalledOnce());
        await expect(wake(retired)).resolves.toBe(false);
        const replacementBatch = retiredBatch.map((entry) => structuredClone(entry));
        const replacement = replacementBatch[0]!;
        replacementBatch.forEach((entry) =>
          bindGatewayContextResolver(entry, () => replacementContext),
        );
        registryRead.listSubagentRunsForRequester.mockReturnValue(replacementBatch);
        // A fresh object or another open Gateway is not proof that the prior claim ended.
        await expect(wake(replacement)).resolves.toBe(false);
        expect(deliver).toHaveBeenCalledOnce();

        firstOpen = false;
        if (binding === "unbound") {
          await expect(wake(replacement)).resolves.toBe(false);
          expect(deliver).toHaveBeenCalledOnce();
          oldDone.resolve({ delivered: true, path: "direct" });
          await oldWake;
        }
        replacementWake = wake(replacement);
        void replacementWake.catch(() => {});
        await vi.waitFor(() => expect(deliver).toHaveBeenCalledTimes(2));
        expect(deliver.mock.calls[1]?.[0].directIdempotencyKey).toBe(
          deliver.mock.calls[0]?.[0].directIdempotencyKey,
        );
        expect(deliver.mock.calls[1]?.[0].resolveGatewayContext?.()).toBe(replacementContext);
        oldDone.resolve({ delivered: true, path: "direct" });
        await expect(oldWake).resolves.toBe(true);
        await expect(wake(replacement)).resolves.toBe(false);
        expect(deliver).toHaveBeenCalledTimes(2);
        replacementDone.resolve({ delivered: true, path: "direct" });
        await expect(replacementWake).resolves.toBe(true);
      } finally {
        oldDone.resolve({ delivered: true, path: "direct" });
        replacementDone.resolve({ delivered: true, path: "direct" });
        await oldWake;
        await replacementWake;
      }
    },
  );

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
            transitionBatch: publishWakeTransition,
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

  it("cancels timed-out wake runs before retry and later work enter the requester lane", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(10_000);
    const context = createContext();
    const child = settledChild();
    registryRead.listSubagentRunsForRequester.mockReturnValue([child]);
    const executions: string[] = [];
    const acceptedSignals: AbortSignal[] = [];
    let releaseGhost!: () => void;
    const ghostGate = new Promise<void>((resolve) => {
      releaseGhost = resolve;
    });

    startTurn.mockImplementation(async ({ preflight, io }) => {
      const request = preflight.request as { idempotencyKey: string; sessionKey: string };
      const registration = registerChatAbortController({
        target: REQUESTER_TARGET,
        runId: request.idempotencyKey,
        sessionId: "requester-session",
        sessionKey: request.sessionKey,
        timeoutMs: 60_000,
        kind: "agent",
      });
      let lifecycleGeneration = getAgentEventLifecycleGeneration();
      let params: RunEmbeddedAgentParams & { sessionFile: string } = {
        admittedRunContext: createTestAdmittedRunContext(request.idempotencyKey),
        abortSignal: registration.controller.signal,
        lifecycleGeneration,
        prompt: "requester settle wake",
        runId: request.idempotencyKey,
        sessionFile: "/tmp/requester-settle-proof.jsonl",
        sessionId: "requester-session",
        sessionKey: request.sessionKey,
        timeoutMs: 60_000,
        workspaceDir: "/tmp",
      };
      const lane = createEmbeddedRunLaneController({
        getLifecycleGeneration: () => lifecycleGeneration,
        getParams: () => params,
        globalLane: GLOBAL_LANE,
        initialQueuedLifecycleGeneration: lifecycleGeneration,
        setLifecycleGeneration: (value) => {
          lifecycleGeneration = value;
        },
        setParams: (value) => {
          params = value;
        },
      });
      acceptedSignals.push(registration.controller.signal);
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
            params = { ...params, replyOperation: operation };
            return await lane.enqueueSession(() =>
              lane.enqueueGlobal(async () => {
                executions.push(request.idempotencyKey);
                await ghostGate;
                return { meta: { durationMs: 1 } };
              }),
            );
          },
        );
        io.emitFinal([true, { runId: request.idempotencyKey, status: "ok" }]);
      } finally {
        registration.cleanup();
        await registration.entry?.input.settlement.promise;
      }
    });

    deliver.mockImplementation(async (params: { directIdempotencyKey: string }) => {
      try {
        await dispatchGatewayMethodInProcess(
          "agent",
          {
            idempotencyKey: params.directIdempotencyKey,
            message: "all children settled",
            sessionKey: REQUESTER_KEY,
          },
          {
            cancelOnDeadline: true,
            expectFinal: true,
            forceSyntheticClient: true,
            timeoutMs: 20,
          },
        );
        return { delivered: true, path: "direct" };
      } catch (error) {
        return {
          delivered: false,
          path: "direct",
          disposition: "retryable",
          error: error instanceof Error ? error.message : String(error),
        };
      }
    });

    let releaseBlocker!: () => void;
    const blockerGate = new Promise<void>((resolve) => {
      releaseBlocker = resolve;
    });
    const blocker = withSessionTurn(REQUESTER_TURN, async () => await blockerGate);
    await vi.advanceTimersByTimeAsync(0);
    expect(
      getExistingSessionControllerMailbox(REQUESTER_KEY, REQUESTER_TARGET)?.claim,
    ).toBeDefined();

    const wake = () =>
      withPluginRuntimeGatewayRequestScope(
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
            transitionBatch: publishWakeTransition,
            completeBatch: () => {},
          }),
      );

    let later: Promise<void> | undefined;
    try {
      const firstWake = wake();
      await vi.advanceTimersByTimeAsync(20);
      await expect(firstWake).resolves.toBe(false);
      expect(child.requesterSettleWake).toMatchObject({
        status: "pending",
        attemptCount: 1,
      });

      await vi.advanceTimersByTimeAsync(30_000);
      const replay = wake();
      await vi.advanceTimersByTimeAsync(20);
      await expect(replay).resolves.toBe(false);

      const deadlineCancelled = acceptedSignals.map((signal) => signal.aborted);
      releaseBlocker();
      await blocker;
      await vi.advanceTimersByTimeAsync(0);

      let laterRan = false;
      later = withSessionTurn(REQUESTER_TURN, async () => {
        laterRan = true;
      });
      await vi.advanceTimersByTimeAsync(0);
      const afterLaterDispatch = getExistingSessionControllerMailbox(
        REQUESTER_KEY,
        REQUESTER_TARGET,
      );

      expect({
        afterLaterDispatch: {
          activeCount: Number(Boolean(afterLaterDispatch?.claim)),
          queuedCount: afterLaterDispatch?.entries.length ?? 0,
        },
        deadlineCancelled,
        executions,
        laterRan,
      }).toEqual({
        afterLaterDispatch: { activeCount: 0, queuedCount: 0 },
        deadlineCancelled: [true, true],
        executions: [],
        laterRan: true,
      });
    } finally {
      releaseBlocker();
      releaseGhost();
      await Promise.allSettled([blocker, later]);
    }
  });
});
