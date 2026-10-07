import { MAX_TIMER_TIMEOUT_MS } from "@openclaw/normalization-core/number-coercion";
// Tests active reply run registry add, lookup, and cleanup behavior.
import { afterEach, describe, expect, it, vi, type MockInstance } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { QuestionAnswerUnconfirmedError } from "../../agents/harness/gateway-question-dispatch.js";
import { SessionPendingInputCustodyError } from "../../config/sessions/session-pending-input-custody-error.js";
import {
  getDiagnosticSessionActivitySnapshot,
  resetDiagnosticRunActivityForTest,
  RUN_STALE_TAKEOVER_MS,
} from "../../logging/diagnostic-run-activity.js";
import { diagnosticLogger } from "../../logging/diagnostic-runtime.js";
import { enqueueCommandInLane, setCommandLaneConcurrency } from "../../process/command-queue.js";
import { resetCommandQueueStateForTest } from "../../process/command-queue.test-support.js";
import {
  beginReplyMessageInjectionTarget,
  finalizeReplyMessageInjectionAttempt,
  hasCommittedReplyOperationOutcome,
  isSessionRunActive,
  interruptReplyRunTarget,
  REPLY_RUN_IDLE_SETTLE_TIMEOUT_MS,
  type ReplyBackendQueueMessageOptions,
  type ReplyOperation,
  markReplyOperationGlobalLaneWaitProgress,
  runAfterReplyOperationClear,
  resolveActiveSessionRunId,
  supersedeReplyRunByRunId,
  waitForReplyOperationOwnerSettlement,
  waitForReplyRunEndBySessionId,
  getSessionControllerOperation,
  isSessionRunActiveForKey,
  captureCurrentReplyMessageInjectionTarget,
  captureCurrentSessionRunInterruptTarget,
} from "../../sessions/session-controller.js";
import { isReplyRunEvidenceStale } from "../../sessions/session-controller.state.js";
import { captureSessionTarget } from "../../sessions/session-controller.target.js";
import { SESSION_WATCHDOG_CLEANUP_MS } from "../../sessions/session-controller.watchdog-state.js";
import { createUserTurnTranscriptRecorder } from "../../sessions/user-turn-transcript.js";
import { createTestUserTurnTranscriptTarget } from "../../sessions/user-turn-transcript.test-support.js";
import { beginReplyOperationFinalizationWork } from "./reply-run-finalization-lease.js";
import { registerReplyOperationRekeyCases } from "./reply-run-registry.rekey.cases.js";
import {
  createTestReplyOperation,
  queueCurrentReplyRunMessage,
  queueReplyMessageInjectionTarget,
} from "./reply-run-registry.test-helpers.js";
import { testing } from "./reply-run-registry.test-support.js";
import { admitReplyTurn } from "./reply-turn-admission.js";

async function withFakeReplyTimers<T>(run: () => Promise<T>): Promise<T> {
  vi.useFakeTimers();
  try {
    return await run();
  } finally {
    await vi.runOnlyPendingTimersAsync();
    vi.useRealTimers();
  }
}

describe("reply run registry", () => {
  afterEach(() => {
    testing.resetReplyRunRegistry();
    resetCommandQueueStateForTest();
    resetDiagnosticRunActivityForTest();
    vi.restoreAllMocks();
  });

  it("keeps ownership stable by sessionKey while sessionId rotates", async () => {
    await withFakeReplyTimers(async () => {
      const operation = createTestReplyOperation({
        sessionId: "session-old",
      });

      const oldWaitPromise = waitForReplyRunEndBySessionId("session-old", 1_000);

      operation.updateSessionId("session-new");

      expect(isSessionRunActiveForKey("agent:main:main")).toBe(true);
      expect(resolveActiveSessionRunId("agent:main:main")).toBe("session-new");
      expect(isSessionRunActive("session-new")).toBe(true);

      let settled = false;
      void oldWaitPromise.then(() => {
        settled = true;
      });
      await vi.advanceTimersByTimeAsync(100);
      expect(settled).toBe(false);

      operation.complete();

      await expect(oldWaitPromise).resolves.toBe(true);
    });
  });

  it("records reply-operation progress without claiming embedded-run activity", () => {
    const operation = createTestReplyOperation({
      sessionKey: "agent:main:telegram:direct:chat-1",
    });

    expect(
      getDiagnosticSessionActivitySnapshot({
        sessionId: "session-1",
        sessionKey: "agent:main:telegram:direct:chat-1",
      }),
    ).toMatchObject({
      activeWorkKind: undefined,
      lastProgressReason: "reply_operation:queued",
    });

    operation.updateSessionId("session-2");

    expect(
      getDiagnosticSessionActivitySnapshot({
        sessionId: "session-2",
        sessionKey: "agent:main:telegram:direct:chat-1",
      }),
    ).toMatchObject({
      activeWorkKind: undefined,
      lastProgressReason: "reply_operation:session_updated",
    });

    operation.complete();

    expect(
      getDiagnosticSessionActivitySnapshot({
        sessionId: "session-2",
        sessionKey: "agent:main:telegram:direct:chat-1",
      }),
    ).toMatchObject({
      activeWorkKind: undefined,
      lastProgressReason: "reply_operation:ended",
    });
  });

  it("keeps repeated request evidence across reply-operation progress", () => {
    const startedAt = Date.parse("2026-08-06T08:00:00Z");
    const now = vi.spyOn(Date, "now").mockReturnValue(startedAt);
    const ref = {
      sessionKey: "agent:main:telegram:direct:retry-bridge",
      sessionId: "session-retry-bridge",
    };
    const operation = createTestReplyOperation(ref);
    const attempt = operation.watchdog.attachAttempt({ assertCurrent: () => {} });
    const endRequest = attempt.beginRequest({});
    now.mockReturnValue(startedAt + 30_000);
    endRequest();
    attempt.beginRequest({});
    expect(getDiagnosticSessionActivitySnapshot(ref)).toMatchObject({
      lastProgressReason: "reply_operation:queued",
      lastProgressAgeMs: 30_000,
      activeWorkKind: "model_call",
    });
    operation.markWaitingForDeferredMaintenance();
    expect(getDiagnosticSessionActivitySnapshot(ref)).toMatchObject({
      lastProgressReason: "deferred_maintenance:waiting",
      lastProgressAgeMs: 30_000,
      activeWorkKind: "model_call",
    });
    expect(operation.watchdog.snapshot().semanticProgressAtMs).toBe(startedAt);
    attempt.close();
    operation.complete();
    expect(getDiagnosticSessionActivitySnapshot(ref)).toMatchObject({
      lastProgressReason: "reply_operation:ended",
      activeWorkKind: undefined,
    });
  });

  it("tracks deferred-maintenance wait as a reply-operation phase", () => {
    const operation = createTestReplyOperation({
      sessionKey: "agent:main:telegram:direct:chat-1",
      sessionId: "session-wait",
    });

    operation.markWaitingForDeferredMaintenance();

    expect(operation.phase).toBe("waiting_for_deferred_maintenance");
    expect(
      getDiagnosticSessionActivitySnapshot({
        sessionId: "session-wait",
        sessionKey: "agent:main:telegram:direct:chat-1",
      }),
    ).toMatchObject({
      activeWorkKind: undefined,
      lastProgressReason: "deferred_maintenance:waiting",
    });

    operation.markDeferredMaintenanceWaitEnded();

    expect(operation.phase).toBe("queued");
    expect(
      getDiagnosticSessionActivitySnapshot({
        sessionId: "session-wait",
        sessionKey: "agent:main:telegram:direct:chat-1",
      }),
    ).toMatchObject({
      activeWorkKind: undefined,
      lastProgressReason: "deferred_maintenance:wait_ended",
    });
  });

  it("keeps a reply alive while the saturated global lane waits past the stale threshold", async () => {
    vi.useFakeTimers();
    const operation = createTestReplyOperation({
      sessionKey: "agent:main:telegram:direct:lane-wait",
      sessionId: "session-global-lane-wait",
    });
    try {
      const lane = "test:reply-global-wait";
      setCommandLaneConcurrency(lane, 0);
      operation.setPhase("running");
      operation.markWaitingForGlobalLane();
      let ran = false;

      const queued = enqueueCommandInLane(
        lane,
        async () => {
          operation.markGlobalLaneWaitEnded();
          ran = true;
        },
        { onWait: () => markReplyOperationGlobalLaneWaitProgress(operation) },
      );

      await vi.advanceTimersByTimeAsync(RUN_STALE_TAKEOVER_MS + 1);
      expect(operation.phase).toBe("waiting_for_global_lane");
      expect(isReplyRunEvidenceStale(operation)).toBe(false);
      expect(ran).toBe(false);

      setCommandLaneConcurrency(lane, 1);
      await queued;

      expect(ran).toBe(true);
      expect(operation.phase).toBe("running");
      expect(
        getDiagnosticSessionActivitySnapshot({
          sessionId: operation.sessionId,
          sessionKey: operation.key,
        }).lastProgressReason,
      ).toBe("global_lane:wait_ended");
    } finally {
      operation.complete();
      vi.useRealTimers();
    }
  });

  it("runs completeThen callbacks after active state clears", () => {
    const operation = createTestReplyOperation({
      sessionId: "session-complete",
    });
    const afterClear = vi.fn(() => {
      expect(isSessionRunActiveForKey("agent:main:main")).toBe(false);
      expect(isSessionRunActive("session-complete")).toBe(false);
    });

    operation.completeThen(afterClear);

    expect(operation.result).toEqual({ kind: "completed" });
    expect(afterClear).toHaveBeenCalledTimes(1);
  });

  it("keeps owner settlement pending through its actual completion barrier", async () => {
    const operation = createTestReplyOperation({ sessionId: "session-stale-owner" });
    operation.setPhase("running");

    await expect(operation.watchdog.tick()).resolves.toMatchObject({ action: "observe" });
    expect(isSessionRunActiveForKey("agent:main:main")).toBe(true);

    const settlement = waitForReplyOperationOwnerSettlement(operation, 1_000);
    let settled = false;
    void settlement.then((value) => {
      settled = value;
    });
    await Promise.resolve();
    expect(settled).toBe(false);

    const { promise: completionBarrier, resolve: releaseCompletion } = createDeferred();
    operation.completeWithAfterClearBarrier(completionBarrier);
    await Promise.resolve();
    expect(settled).toBe(false);

    releaseCompletion();
    await expect(settlement).resolves.toBe(true);
  });
  it("keeps late delivery custody after finalization cleanup expires", async () => {
    await withFakeReplyTimers(async () => {
      const operation = createTestReplyOperation();
      const delivery = createDeferred();
      const settled = vi.fn();
      void operation.ownerSettlement.then(settled);
      operation.setPhase("running");
      operation.freezeAbort();
      try {
        await vi.advanceTimersByTimeAsync(SESSION_WATCHDOG_CLEANUP_MS);
        expect(operation.watchdog.snapshot().recovery?.status).toBe("blocked");
        expect(getSessionControllerOperation(operation.key)).toBe(operation);
        expect(() => createTestReplyOperation({ sessionId: "too-early" })).toThrow();
        expect(settled).not.toHaveBeenCalled();
        const ownerWait = waitForReplyOperationOwnerSettlement(operation, 100);
        await vi.advanceTimersByTimeAsync(100);
        await expect(ownerWait).resolves.toBe(false);
        expect(settled).not.toHaveBeenCalled();

        // Completion clears the slot, but raw delivery still fences its successor.
        operation.completeWithAfterClearBarrier(delivery.promise);
        expect(() => createTestReplyOperation({ sessionId: "successor" })).toThrow();
        operation.complete();
        await Promise.resolve();
        expect(settled).not.toHaveBeenCalled();
        expect(getSessionControllerOperation(operation.key)).toBeUndefined();
        expect(operation.result).toEqual({ kind: "completed" });
        delivery.resolve();
        await operation.ownerSettlement;
        const successor = createTestReplyOperation({ sessionId: "successor" });
        successor.complete();
      } finally {
        delivery.resolve();
        operation.completeWithAfterClearBarrier(delivery.promise);
        await operation.ownerSettlement;
      }
      expect(settled).toHaveBeenCalledOnce();
    });
  });

  it("interrupts only the captured operation when its abort admits a same-key successor", async () => {
    const operation = createTestReplyOperation({ sessionId: "session-interrupt-captured" });
    operation.setPhase("running");
    let successor: ReplyOperation | undefined;
    let successorAbortByUser: MockInstance<ReplyOperation["abortByUser"]> | undefined;
    operation.attachBackend({
      kind: "embedded",
      cancel: () => {
        operation.complete();
        successor = createTestReplyOperation({ sessionId: "session-interrupt-successor" });
        successor.setPhase("running");
        successorAbortByUser = vi.spyOn(successor, "abortByUser");
      },
    });
    const target = captureCurrentSessionRunInterruptTarget(operation.key);
    if (!target) {
      throw new Error("expected captured interrupt target");
    }

    await expect(interruptReplyRunTarget(target, 1_000)).resolves.toEqual({
      aborted: true,
      settled: true,
    });
    if (!successor || !successorAbortByUser) {
      throw new Error("expected same-key successor operation");
    }
    try {
      expect(successorAbortByUser).not.toHaveBeenCalled();
    } finally {
      successor.complete();
    }
  });

  it.each([false, true])(
    "keeps reentrant cancellation completion behind its delivery barrier (cancel throws=%s)",
    async (throwsAfterCompletion) => {
      const operation = createTestReplyOperation({ sessionId: "session-sync-cancel" });
      const delivery = createDeferred();
      const afterClear = vi.fn();
      const ownerSettled = vi.fn();
      void operation.ownerSettlement.then(ownerSettled);
      operation.setPhase("running");
      operation.attachBackend({
        kind: "embedded",
        cancel: () => {
          operation.completeWithAfterClearBarrier(delivery.promise);
          if (throwsAfterCompletion) {
            throw new Error("cancel failed after producer completion");
          }
        },
      });
      runAfterReplyOperationClear(operation, afterClear);
      try {
        await expect(
          operation.watchdog.tick(operation.watchdog.snapshot().semanticDeadlineAtMs),
        ).resolves.toMatchObject({ action: "blocked" });
        expect(operation.result).toEqual({ kind: "failed", code: "run_stalled" });
        expect(operation.abortSignal.aborted).toBe(true);
        expect(getSessionControllerOperation(operation.key)).toBeUndefined();
        expect(afterClear).not.toHaveBeenCalled();
        expect(ownerSettled).not.toHaveBeenCalled();
        const lateAfterClear = vi.fn();
        runAfterReplyOperationClear(operation, lateAfterClear);
        expect(lateAfterClear).not.toHaveBeenCalled();
        expect(() => createTestReplyOperation({ sessionId: "successor" })).toThrow();
        delivery.resolve();
        await operation.ownerSettlement;
        const successor = createTestReplyOperation({ sessionId: "successor" });
        await Promise.resolve();
        expect(afterClear).toHaveBeenCalledExactlyOnceWith("session-sync-cancel");
        expect(lateAfterClear).toHaveBeenCalledExactlyOnceWith("session-sync-cancel");
        expect(ownerSettled).toHaveBeenCalledOnce();
        expect(getSessionControllerOperation(operation.key)).toBe(successor);
        successor.complete();
      } finally {
        delivery.resolve();
        operation.complete();
        await operation.ownerSettlement;
      }
    },
  );

  it.each(["pending", "throws", "throws undefined", "pre-backend"] as const)(
    "retires exact stale custody at the cleanup deadline when cancellation is %s",
    async (cancellation) => {
      const operation = createTestReplyOperation({ sessionId: "session-cancel-pending" });
      const revokeWriter = vi.fn(async () => true);
      operation.registerTerminalProducerFence(revokeWriter);
      operation.setPhase("running");
      const cancel = vi.fn(() => {
        if (cancellation === "throws") {
          throw new Error("cancel failed");
        }
        if (cancellation === "throws undefined") {
          // oxlint-disable-next-line typescript/only-throw-error -- JavaScript cancellation can throw any value; custody must survive it.
          throw undefined;
        }
      });
      if (cancellation !== "pre-backend") {
        operation.attachBackend({ kind: "embedded", cancel });
      }
      const afterClear = vi.fn();
      const ownerSettled = vi.fn();
      void operation.ownerSettlement.then(ownerSettled);
      runAfterReplyOperationClear(operation, afterClear);
      const deadline = operation.watchdog.snapshot().semanticDeadlineAtMs;
      await expect(operation.watchdog.tick(deadline)).resolves.toMatchObject({ action: "blocked" });
      expect(operation.result).toEqual({ kind: "failed", code: "run_stalled" });
      expect(operation.abortSignal.aborted).toBe(true);
      expect(getSessionControllerOperation(operation.key)).toBe(operation);
      expect(cancel).toHaveBeenCalledTimes(cancellation === "pre-backend" ? 0 : 1);
      if (cancellation !== "pre-backend") {
        expect(cancel).toHaveBeenCalledWith("superseded");
      }
      const lateCancel = vi.fn();
      operation.attachBackend({ kind: "embedded", cancel: lateCancel });
      expect(lateCancel).toHaveBeenCalledWith("superseded");
      const lateAfterClear = vi.fn();
      runAfterReplyOperationClear(operation, lateAfterClear);
      await expect(
        operation.watchdog.tick(deadline + SESSION_WATCHDOG_CLEANUP_MS),
      ).resolves.toMatchObject({ action: "expire_cleanup" });
      expect(getSessionControllerOperation(operation.key)).toBeUndefined();
      expect(afterClear).toHaveBeenCalledExactlyOnceWith("session-cancel-pending");
      expect(lateAfterClear).toHaveBeenCalledExactlyOnceWith("session-cancel-pending");
      await operation.ownerSettlement;
      expect(revokeWriter).toHaveBeenCalledOnce();
      expect(ownerSettled).toHaveBeenCalledOnce();
      const successor = createTestReplyOperation({ sessionId: "successor" });
      operation.complete();
      expect(getSessionControllerOperation(operation.key)).toBe(successor);
      successor.complete();
    },
  );

  it.each([
    { firstStore: "store-a", laterStore: "store-a" },
    { firstStore: "store-a", laterStore: "store-b" },
    { firstStore: "store-a", laterStore: undefined },
    { firstStore: undefined, laterStore: "store-b" },
    { firstStore: undefined, laterStore: undefined },
  ])(
    "keeps after-clear custody scoped to its physical store ($firstStore -> $laterStore)",
    async ({ firstStore, laterStore }) => {
      const makeOperation = (storeScope: string | undefined) =>
        createTestReplyOperation({
          sessionKey: "global",
          sessionId: "first-session",
          target: storeScope
            ? captureSessionTarget({
                storeScope,
                sessionKey: "global",
                incarnation: "first-session",
              })
            : undefined,
        });
      const first = makeOperation(firstStore);
      const barrier = createDeferred();
      const afterClear = vi.fn();
      runAfterReplyOperationClear(first, afterClear);
      first.completeWithAfterClearBarrier(barrier.promise);
      try {
        if (firstStore && laterStore && firstStore !== laterStore) {
          const later = makeOperation(laterStore);
          later.updateSessionId("rotated-session");
          later.complete();
        } else {
          // Unbound metadata cannot manufacture a foreign physical session.
          expect(() => makeOperation(laterStore)).toThrow();
        }
        expect(afterClear).not.toHaveBeenCalled();
      } finally {
        barrier.resolve();
        await first.ownerSettlement;
      }
      expect(afterClear).toHaveBeenCalledExactlyOnceWith("first-session");
    },
  );

  it("keeps a late callback behind its own delivery when a foreign store has another barrier", async () => {
    const first = createTestReplyOperation({
      sessionKey: "global",
      sessionId: "first-session",
      target: captureSessionTarget({
        storeScope: "store-a",
        sessionKey: "global",
        incarnation: "first-session",
      }),
    });
    const firstBarrier = createDeferred();
    first.completeWithAfterClearBarrier(firstBarrier.promise);
    const later = createTestReplyOperation({
      sessionKey: "global",
      sessionId: "later-session",
      target: captureSessionTarget({
        storeScope: "store-b",
        sessionKey: "global",
        incarnation: "later-session",
      }),
    });
    const laterBarrier = createDeferred();
    later.completeWithAfterClearBarrier(laterBarrier.promise);
    const afterClear = vi.fn();
    const laterAfterClear = vi.fn();
    runAfterReplyOperationClear(first, afterClear);
    runAfterReplyOperationClear(later, laterAfterClear);
    try {
      expect(afterClear).not.toHaveBeenCalled();
      firstBarrier.resolve();
      await first.ownerSettlement;
      expect(afterClear).toHaveBeenCalledExactlyOnceWith("first-session");
      expect(laterAfterClear).not.toHaveBeenCalled();
    } finally {
      firstBarrier.resolve();
      laterBarrier.resolve();
      await Promise.all([first.ownerSettlement, later.ownerSettlement]);
    }
    expect(laterAfterClear).toHaveBeenCalledExactlyOnceWith("later-session");
  });

  it("keeps later after-clear work behind earlier delivery custody", async () => {
    const first = createTestReplyOperation({ sessionId: "first-session" });
    const firstBarrier = createDeferred();
    const firstAfterClear = vi.fn();
    runAfterReplyOperationClear(first, firstAfterClear);
    first.completeWithAfterClearBarrier(firstBarrier.promise);
    expect(() => createTestReplyOperation({ sessionId: "second-session" })).toThrow();
    firstBarrier.resolve();
    await first.ownerSettlement;
    expect(firstAfterClear).toHaveBeenCalledExactlyOnceWith("first-session");

    const second = createTestReplyOperation({ sessionId: "second-session" });
    const secondBarrier = createDeferred();
    const secondAfterClear = vi.fn();
    runAfterReplyOperationClear(second, secondAfterClear);
    second.completeWithAfterClearBarrier(secondBarrier.promise);
    expect(secondAfterClear).not.toHaveBeenCalled();
    secondBarrier.resolve();
    await second.ownerSettlement;
    expect(secondAfterClear).toHaveBeenCalledExactlyOnceWith("second-session");
  });

  it("keeps follow-up admission blocked until slow delivery settles", async () => {
    await withFakeReplyTimers(async () => {
      const operation = createTestReplyOperation({
        sessionId: "hung-session",
      });
      const { promise: barrier, resolve: releaseBarrier } = createDeferred();
      const afterClear = vi.fn();
      runAfterReplyOperationClear(operation, afterClear);
      operation.completeWithAfterClearBarrier(barrier, 35 * 60_000);

      await vi.advanceTimersByTimeAsync(REPLY_RUN_IDLE_SETTLE_TIMEOUT_MS);
      expect(afterClear).not.toHaveBeenCalled();
      expect(() =>
        createTestReplyOperation({
          sessionKey: "agent:main:main",
          sessionId: "blocked-session",
          resetTriggered: false,
          turnKind: "queued_followup",
        }),
      ).toThrow("Reply follow-up admission is blocked");

      releaseBarrier();
      await barrier;
      await vi.waitFor(() => {
        expect(afterClear).toHaveBeenCalledWith("hung-session");
      });
      const next = createTestReplyOperation({
        sessionId: "next-session",
        turnKind: "queued_followup",
      });
      next.complete();
    });
  });

  it("keeps follow-up admission blocked during an unsettled inter-block delay", async () => {
    await withFakeReplyTimers(async () => {
      const operation = createTestReplyOperation({
        sessionKey: "agent:main:mattermost:direct:user-1",
        sessionId: "mattermost-delivery-session",
      });
      let settledDeliveryCount = 1;
      const queuedDeliveryCount = 2;
      const afterClear = vi.fn();
      runAfterReplyOperationClear(operation, afterClear);
      const delivery = createDeferred();
      operation.completeWithAfterClearBarrier(delivery.promise, {
        maxTimeoutMs: REPLY_RUN_IDLE_SETTLE_TIMEOUT_MS * 3,
        shouldExtend: () => settledDeliveryCount < queuedDeliveryCount,
      });

      await vi.advanceTimersByTimeAsync(REPLY_RUN_IDLE_SETTLE_TIMEOUT_MS);
      expect(afterClear).not.toHaveBeenCalled();
      expect(() =>
        createTestReplyOperation({
          sessionKey: "agent:main:mattermost:direct:user-1",
          sessionId: "queued-followup",
          resetTriggered: false,
          turnKind: "queued_followup",
        }),
      ).toThrow();

      settledDeliveryCount = 2;
      await vi.advanceTimersByTimeAsync(REPLY_RUN_IDLE_SETTLE_TIMEOUT_MS);
      await vi.waitFor(() => {
        expect(afterClear).toHaveBeenCalledWith("mattermost-delivery-session");
      });

      expect(() =>
        createTestReplyOperation({
          sessionKey: "agent:main:mattermost:direct:user-1",
          turnKind: "queued_followup",
        }),
      ).toThrow();
      delivery.resolve();
      await operation.ownerSettlement;
      const followup = createTestReplyOperation({
        sessionKey: "agent:main:mattermost:direct:user-1",
        sessionId: "admitted-followup",
        turnKind: "queued_followup",
      });
      followup.complete();
    });
  });

  it("bounds after-clear notification without admitting a successor before raw delivery settles", async () => {
    await withFakeReplyTimers(async () => {
      const operation = createTestReplyOperation({
        sessionId: "hung-session",
      });
      const delivery = createDeferred();
      const ownerSettled = vi.fn();
      expect(operation.ownerSettlement).toBeDefined();
      void operation.ownerSettlement?.then(ownerSettled);
      const afterClear = vi.fn();
      runAfterReplyOperationClear(operation, afterClear);
      operation.completeWithAfterClearBarrier(delivery.promise);

      try {
        await vi.advanceTimersByTimeAsync(REPLY_RUN_IDLE_SETTLE_TIMEOUT_MS - 1);
        expect(afterClear).not.toHaveBeenCalled();

        await vi.advanceTimersByTimeAsync(1);
        expect(afterClear).toHaveBeenCalledWith("hung-session");
        expect(() =>
          createTestReplyOperation({
            sessionId: "next-session",
            turnKind: "queued_followup",
          }),
        ).toThrow();
        expect(ownerSettled).not.toHaveBeenCalled();

        const boundedWait = waitForReplyOperationOwnerSettlement(operation, 100);
        await vi.advanceTimersByTimeAsync(100);
        await expect(boundedWait).resolves.toBe(false);
        expect(ownerSettled).not.toHaveBeenCalled();
      } finally {
        delivery.resolve();
        await operation.ownerSettlement;
      }
      expect(ownerSettled).toHaveBeenCalledOnce();
      createTestReplyOperation({
        sessionId: "next-session",
        turnKind: "queued_followup",
      }).complete();
    });
  });

  it("retains failed operations until final delivery completes", () => {
    const operation = createTestReplyOperation({
      sessionId: "session-failed",
    });
    const afterClear = vi.fn();
    runAfterReplyOperationClear(operation, afterClear);

    operation.fail("run_failed", new Error("provider failed"));

    expect(operation.result).toMatchObject({ kind: "failed", code: "run_failed" });
    expect(getSessionControllerOperation("agent:main:main")).toBe(operation);
    expect(afterClear).not.toHaveBeenCalled();

    operation.complete();

    expect(isSessionRunActiveForKey("agent:main:main")).toBe(false);
    expect(afterClear).toHaveBeenCalledTimes(1);
  });

  it.each([
    {
      name: "user abort while queued",
      abort: (operation: ReturnType<typeof createTestReplyOperation>) => operation.abortByUser(),
      code: "aborted_by_user",
      reason: "user_abort",
      phase: "queued",
    },
    {
      name: "restart abort while running",
      abort: (operation: ReturnType<typeof createTestReplyOperation>) =>
        operation.abortForRestart(),
      code: "aborted_for_restart",
      reason: "restart",
      phase: "running",
    },
  ] as const)("preserves cleanup when backend cancellation throws: $name", async (testCase) => {
    await withFakeReplyTimers(async () => {
      const cancelError = new Error("cancel failed");
      const cancel = vi.fn(() => {
        throw cancelError;
      });
      const operation = createTestReplyOperation({
        sessionKey: `agent:main:${testCase.reason}-${testCase.phase}`,
        sessionId: `session-${testCase.reason}-${testCase.phase}`,
      });
      const revokeWriter = vi.fn(async () => true);
      operation.registerTerminalProducerFence(revokeWriter);
      operation.attachBackend({ kind: "embedded", cancel, isStreaming: () => true });
      operation.setPhase(testCase.phase);
      const afterClear = vi.fn();
      runAfterReplyOperationClear(operation, afterClear);

      expect(() => testCase.abort(operation)).toThrow(cancelError);
      expect(operation.result).toEqual({ kind: "aborted", code: testCase.code });
      expect(operation.phase).toBe("aborted");
      expect(operation.abortSignal.aborted).toBe(true);
      expect(cancel).toHaveBeenCalledOnce();
      expect(cancel).toHaveBeenCalledWith(testCase.reason);

      expect(getSessionControllerOperation(operation.key)).toBe(operation);
      expect(afterClear).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(SESSION_WATCHDOG_CLEANUP_MS);
      expect(operation.watchdog.snapshot().recovery?.status).toBe("settled");
      expect(getSessionControllerOperation(operation.key)).toBeUndefined();
      expect(afterClear).toHaveBeenCalledOnce();
      expect(cancel).toHaveBeenCalledTimes(2);
      expect(cancel).toHaveBeenLastCalledWith("superseded");
      expect(revokeWriter).toHaveBeenCalledOnce();
      operation.complete();
      expect(isSessionRunActiveForKey(operation.key)).toBe(false);
      expect(afterClear).toHaveBeenCalledOnce();
    });
  });

  it("admits a visible successor after an aborted producer misses its cleanup deadline", async () => {
    await withFakeReplyTimers(async () => {
      const cancel = vi.fn();
      const operation = createTestReplyOperation({
        sessionKey: "agent:main:hung-abort",
        sessionId: "session-hung-abort",
      });
      const revokeWriter = vi.fn(async () => true);
      operation.registerTerminalProducerFence(revokeWriter);
      operation.attachBackend({
        kind: "embedded",
        cancel,
        isStreaming: () => true,
      });
      operation.setPhase("running");
      const afterClear = vi.fn();
      runAfterReplyOperationClear(operation, afterClear);
      operation.abortByUser();
      let next: Awaited<ReturnType<typeof admitReplyTurn>> | undefined;
      void admitReplyTurn({
        sessionKey: "agent:main:hung-abort",
        sessionId: "session-after-hung-abort",
        kind: "visible",
        resetTriggered: false,
      }).then((result) => {
        next = result;
      });

      await vi.advanceTimersByTimeAsync(SESSION_WATCHDOG_CLEANUP_MS - 1);
      expect(getSessionControllerOperation("agent:main:hung-abort")).toBe(operation);
      expect(afterClear).not.toHaveBeenCalled();
      expect(next).toBeUndefined();

      await vi.advanceTimersByTimeAsync(1);

      expect(next?.status).toBe("owned");
      expect(getSessionControllerOperation("agent:main:hung-abort")).toBe(
        next?.status === "owned" ? next.operation : undefined,
      );
      expect(afterClear).toHaveBeenCalledTimes(1);
      expect(revokeWriter).toHaveBeenCalledOnce();
      operation.complete();
      if (next?.status === "owned") {
        expect(getSessionControllerOperation("agent:main:hung-abort")).toBe(next.operation);
        next.operation.complete();
      }
    });
  });

  it("keeps run_stalled attribution and ownership when cancel re-enters abortByUser", () => {
    const operation = createTestReplyOperation({
      sessionKey: "agent:main:reentrant-expire",
      sessionId: "reentrant-session",
    });
    operation.attachBackend({
      kind: "embedded",
      // Mirrors the run loop's abort handler: backend cancellation propagates
      // synchronously back into a user-shaped abort on the same operation.
      cancel: () => {
        operation.abortByUser();
      },
      isStreaming: () => true,
    });
    operation.setPhase("running");

    expect(operation.abortForStall()).toBe(true);
    expect(operation.result).toEqual({ kind: "failed", code: "run_stalled" });
    expect(getSessionControllerOperation("agent:main:reentrant-expire")).toBe(operation);
    operation.complete();
    expect(getSessionControllerOperation("agent:main:reentrant-expire")).toBeUndefined();
  });

  it("keeps supersession attribution when backend cancellation re-enters user abort", () => {
    const operation = createTestReplyOperation({
      sessionKey: "agent:main:heartbeat-preemption",
      sessionId: "heartbeat-preemption-session",
      turnKind: "heartbeat",
    });
    const order: string[] = [];
    const cancel = vi.fn((reason) => {
      order.push(`cancel:${reason}`);
      operation.abortByUser();
    });
    operation.attachBackend({
      kind: "embedded",
      runId: "heartbeat-preemption-run",
      cancel,
      isStreaming: () => true,
    });
    operation.setPhase("running");

    expect(supersedeReplyRunByRunId("heartbeat-preemption-run", () => order.push("record"))).toBe(
      true,
    );
    expect(cancel).toHaveBeenCalledWith("superseded");
    expect(order).toEqual(["record", "cancel:superseded"]);
    expect(operation.result).toEqual({
      kind: "aborted",
      code: "aborted_for_supersession",
    });
  });

  it("supersedes an abort-frozen heartbeat owner without cancelling its backend", () => {
    const beforeSupersede = vi.fn();
    const cancel = vi.fn();
    const operation = createTestReplyOperation({
      sessionKey: "agent:main:heartbeat-frozen",
      sessionId: "heartbeat-frozen-session",
      turnKind: "heartbeat",
    });
    operation.attachBackend({
      kind: "embedded",
      runId: "heartbeat-frozen-run",
      cancel,
      isStreaming: () => true,
    });
    operation.setPhase("running");
    operation.freezeAbort();

    expect(supersedeReplyRunByRunId("heartbeat-frozen-run", beforeSupersede)).toBe(true);
    expect(beforeSupersede).toHaveBeenCalledTimes(1);
    expect(cancel).not.toHaveBeenCalled();
    expect(operation.result).toEqual({
      kind: "aborted",
      code: "aborted_for_supersession",
    });
  });

  it("does not supersede a retained terminal reply owner", () => {
    const beforeSupersede = vi.fn();
    const cancel = vi.fn();
    const operation = createTestReplyOperation({
      sessionKey: "agent:main:terminal-reply",
      sessionId: "terminal-reply-session",
    });
    operation.attachBackend({
      kind: "cli",
      runId: "terminal-reply-run",
      cancel,
    });
    operation.setPhase("running");
    operation.fail("run_failed", new Error("delivery pending"));

    expect(supersedeReplyRunByRunId("terminal-reply-run", beforeSupersede)).toBe(false);
    expect(beforeSupersede).not.toHaveBeenCalled();
    expect(cancel).not.toHaveBeenCalled();
    expect(operation.result).toMatchObject({ kind: "failed", code: "run_failed" });
  });

  it("cancels terminal settle when the owner clears state first", async () => {
    await withFakeReplyTimers(async () => {
      const warnSpy = vi.spyOn(diagnosticLogger, "warn").mockImplementation(() => undefined);
      const operation = createTestReplyOperation({
        sessionKey: "agent:main:owner-clears",
        sessionId: "session-owner-clears",
      });
      operation.setPhase("running");

      operation.abortByUser();
      operation.complete();
      await vi.advanceTimersByTimeAsync(SESSION_WATCHDOG_CLEANUP_MS);

      expect(isSessionRunActiveForKey("agent:main:owner-clears")).toBe(false);
      expect(warnSpy).not.toHaveBeenCalled();
      expect(operation.watchdog.snapshot().current).toBe(false);
    });
  });

  it("does not let an old watchdog clear a replacement operation", async () => {
    const original = createTestReplyOperation({ sessionId: "session-reused" });
    original.complete();
    const replacement = createTestReplyOperation({ sessionId: "session-reused" });
    await expect(original.watchdog.tick(Number.MAX_SAFE_INTEGER)).resolves.toEqual({
      action: "observe",
      reason: "retired",
    });
    expect(replacement.result).toBeNull();
    expect(isSessionRunActive("session-reused")).toBe(true);
    replacement.complete();
  });

  it("reports a committed terminal outcome only while delivery is still finalizing", () => {
    const operation = createTestReplyOperation({
      sessionKey: "agent:main:committed-outcome",
      sessionId: "session-committed-outcome",
    });
    operation.setPhase("running");
    expect(hasCommittedReplyOperationOutcome(operation)).toBe(false);

    operation.freezeAbort();
    expect(hasCommittedReplyOperationOutcome(operation)).toBe(true);

    operation.complete();
    expect(hasCommittedReplyOperationOutcome(operation)).toBe(false);
  });

  it("cleans up stalled finalization without rewriting the committed result", async () => {
    await withFakeReplyTimers(async () => {
      const afterClear = vi.fn();
      const operation = createTestReplyOperation({
        sessionKey: "agent:main:hung-finalization",
        sessionId: "session-hung-finalization",
      });
      operation.setPhase("running");
      const cancel = vi.fn(() => operation.complete());
      operation.attachBackend({ kind: "embedded", cancel });
      runAfterReplyOperationClear(operation, afterClear);

      operation.freezeAbort();
      await vi.advanceTimersByTimeAsync(SESSION_WATCHDOG_CLEANUP_MS - 1);

      expect(getSessionControllerOperation("agent:main:hung-finalization")).toBe(operation);
      expect(operation.result).toBeNull();
      expect(operation.abortSignal.aborted).toBe(false);

      await vi.advanceTimersByTimeAsync(1);

      expect(getSessionControllerOperation("agent:main:hung-finalization")).toBeUndefined();
      expect(operation.result).toEqual({ kind: "completed" });
      expect(cancel).toHaveBeenCalledExactlyOnceWith("superseded");
      expect(operation.phase).toBe("completed");
      expect(operation.abortSignal.aborted).toBe(false);
      expect(afterClear).toHaveBeenCalledTimes(1);
    });
  });

  it("renews finalization from owner progress", async () => {
    await withFakeReplyTimers(async () => {
      const operation = createTestReplyOperation({
        sessionKey: "agent:main:progressing-finalization",
        sessionId: "session-progressing-finalization",
      });
      operation.setPhase("running");
      const cancel = vi.fn(() => operation.complete());
      operation.attachBackend({ kind: "embedded", cancel });
      operation.freezeAbort();

      await vi.advanceTimersByTimeAsync(SESSION_WATCHDOG_CLEANUP_MS - 15_000);
      operation.watchdog.progress("finalization");
      await vi.advanceTimersByTimeAsync(15_000);

      expect(getSessionControllerOperation("agent:main:progressing-finalization")).toBe(operation);
      expect(operation.result).toBeNull();

      await vi.advanceTimersByTimeAsync(SESSION_WATCHDOG_CLEANUP_MS - 15_000);

      expect(getSessionControllerOperation("agent:main:progressing-finalization")).toBeUndefined();
      expect(operation.result).toEqual({ kind: "completed" });
      expect(cancel).toHaveBeenCalledExactlyOnceWith("superseded");
    });
  });

  it("preserves bounded work that starts before finalization", async () => {
    await withFakeReplyTimers(async () => {
      const operation = createTestReplyOperation({
        sessionKey: "agent:main:pre-finalization-work",
        sessionId: "session-pre-finalization-work",
      });
      operation.setPhase("running");
      const cancel = vi.fn(() => operation.complete());
      operation.attachBackend({ kind: "embedded", cancel });
      beginReplyOperationFinalizationWork(operation, SESSION_WATCHDOG_CLEANUP_MS * 2);

      await vi.advanceTimersByTimeAsync(30_000);
      operation.freezeAbort();
      await vi.advanceTimersByTimeAsync(SESSION_WATCHDOG_CLEANUP_MS);

      expect(getSessionControllerOperation("agent:main:pre-finalization-work")).toBe(operation);

      await vi.advanceTimersByTimeAsync(30_000);
      expect(getSessionControllerOperation("agent:main:pre-finalization-work")).toBeUndefined();
      expect(operation.result).toEqual({ kind: "completed" });
      expect(cancel).toHaveBeenCalledExactlyOnceWith("superseded");
    });
  });

  it("does not shorten bounded work when finalization progress renews", async () => {
    await withFakeReplyTimers(async () => {
      const operation = createTestReplyOperation({
        sessionKey: "agent:main:overlapping-finalization-work",
        sessionId: "session-overlapping-finalization-work",
      });
      operation.setPhase("running");
      const cancel = vi.fn(() => operation.complete());
      operation.attachBackend({ kind: "embedded", cancel });
      operation.freezeAbort();
      beginReplyOperationFinalizationWork(operation, SESSION_WATCHDOG_CLEANUP_MS * 2);

      await vi.advanceTimersByTimeAsync(SESSION_WATCHDOG_CLEANUP_MS - 15_000);
      operation.watchdog.progress("finalization");
      await vi.advanceTimersByTimeAsync(SESSION_WATCHDOG_CLEANUP_MS);

      expect(getSessionControllerOperation("agent:main:overlapping-finalization-work")).toBe(
        operation,
      );

      await vi.advanceTimersByTimeAsync(15_000);
      expect(
        getSessionControllerOperation("agent:main:overlapping-finalization-work"),
      ).toBeUndefined();
      expect(operation.result).toEqual({ kind: "completed" });
      expect(cancel).toHaveBeenCalledExactlyOnceWith("superseded");
    });
  });

  it("clamps oversized wait timers instead of resolving idle waits immediately", async () => {
    await withFakeReplyTimers(async () => {
      const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");
      const operation = createTestReplyOperation({
        sessionId: "session-running",
      });

      const waitPromise = waitForReplyRunEndBySessionId(
        "session-running",
        MAX_TIMER_TIMEOUT_MS + 1,
      );

      expect(setTimeoutSpy).toHaveBeenCalledWith(expect.any(Function), MAX_TIMER_TIMEOUT_MS);
      operation.complete();
      await expect(waitPromise).resolves.toBe(true);
    });
  });

  it("waits for reply-run completion without a timer when requested", async () => {
    await withFakeReplyTimers(async () => {
      const operation = createTestReplyOperation({
        sessionKey: "agent:main:unbounded",
        sessionId: "session-unbounded",
      });
      const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");

      const waitPromise = waitForReplyRunEndBySessionId("session-unbounded", null);

      expect(setTimeoutSpy).not.toHaveBeenCalled();
      operation.complete();
      await expect(waitPromise).resolves.toBe(true);
    });
  });

  it("queues messages only through the active running backend", async () => {
    const queueMessage = vi.fn(async () => {});
    const operation = createTestReplyOperation({
      sessionId: "session-running",
    });

    operation.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      cancel: vi.fn(),
      isStreaming: () => true,
      queueMessage,
    });

    await expect(
      queueCurrentReplyRunMessage("session-running", "before running"),
    ).resolves.toMatchObject({ status: "rejected" });

    operation.setPhase("running");

    await expect(queueCurrentReplyRunMessage("session-running", "hello")).resolves.toEqual({
      status: "accepted",
    });
    expect(queueMessage).toHaveBeenCalledWith(
      "hello",
      expect.objectContaining({ onQueueAccepted: expect.any(Function) }),
    );
  });

  it("queues messages only when the task-suggestion tool surface matches", async () => {
    const queueMessage = vi.fn(async () => {});
    const operation = createTestReplyOperation({
      sessionId: "session-task-suggestions",
    });
    operation.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      taskSuggestionDeliveryMode: "gateway",
      cancel: vi.fn(),
      isStreaming: () => true,
      queueMessage,
    });
    operation.setPhase("running");

    await expect(
      queueCurrentReplyRunMessage("session-task-suggestions", "legacy client", {
        taskSuggestionDeliveryMode: undefined,
      }),
    ).resolves.toEqual({ status: "rejected", reason: "task_suggestion_delivery_mode_mismatch" });
    await expect(
      queueCurrentReplyRunMessage("session-task-suggestions", "capable client", {
        taskSuggestionDeliveryMode: "gateway",
      }),
    ).resolves.toEqual({ status: "accepted" });
    await expect(
      queueCurrentReplyRunMessage("session-task-suggestions", "internal completion"),
    ).resolves.toEqual({ status: "accepted" });
    expect(queueMessage).toHaveBeenCalledTimes(2);
    expect(queueMessage).toHaveBeenNthCalledWith(
      1,
      "capable client",
      expect.objectContaining({
        taskSuggestionDeliveryMode: "gateway",
        onQueueAccepted: expect.any(Function),
      }),
    );
    expect(queueMessage).toHaveBeenNthCalledWith(
      2,
      "internal completion",
      expect.objectContaining({ onQueueAccepted: expect.any(Function) }),
    );
  });

  it.each([
    { images: [{ type: "image" as const, data: "png", mimeType: "image/png" }] },
    { media: [{ path: "/tmp/stored.png", contentType: "image/png" }] },
    { imageOrder: ["offloaded" as const] },
  ])("queues image inputs only through backends that preserve them: %j", async (input) => {
    const queueMessage = vi.fn(async () => {});
    const operation = createTestReplyOperation({
      sessionId: "session-images",
    });
    operation.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      cancel: vi.fn(),
      isStreaming: () => true,
      queueMessage,
    });
    operation.setPhase("running");

    await expect(
      queueCurrentReplyRunMessage("session-images", "inspect", input),
    ).resolves.toMatchObject({ status: "rejected", reason: "image_input_unsupported" });
    expect(queueMessage).not.toHaveBeenCalled();

    operation.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      cancel: vi.fn(),
      isStreaming: () => true,
      queueMessage,
      supportsQueueMessageImages: true,
    });

    await expect(queueCurrentReplyRunMessage("session-images", "inspect", input)).resolves.toEqual({
      status: "accepted",
    });
    expect(queueMessage).toHaveBeenCalledWith(
      "inspect",
      expect.objectContaining({ ...input, onQueueAccepted: expect.any(Function) }),
    );
  });

  it("queues messages through queue-first legacy backends while token streaming is idle", async () => {
    const queueMessage = vi.fn(async () => {});
    const operation = createTestReplyOperation({
      sessionId: "session-running",
    });

    operation.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      cancel: vi.fn(),
      isStreaming: () => false,
      queueMessage,
    });
    operation.setPhase("running");

    await expect(queueCurrentReplyRunMessage("session-running", "hello")).resolves.toEqual({
      status: "accepted",
    });
    expect(queueMessage).toHaveBeenCalledWith(
      "hello",
      expect.objectContaining({ onQueueAccepted: expect.any(Function) }),
    );
  });

  it("refuses stale injectable owners for admission and delivery until activity resumes", async () => {
    vi.useFakeTimers();
    try {
      const queueMessage = vi.fn(async () => {});
      const operation = createTestReplyOperation({
        sessionId: "session-running",
        originatingLeafEntryId: "leaf-a",
      });
      operation.attachBackend({
        kind: "embedded",
        supportsTranscriptCommitWait: true,
        cancel: vi.fn(),
        isStreaming: () => false,
        isStopped: () => false,
        queueMessage,
      });
      operation.setPhase("running");

      const target = captureCurrentReplyMessageInjectionTarget("agent:main:main");
      expect(target).toBeDefined();

      vi.setSystemTime(operation.watchdog.snapshot().semanticDeadlineAtMs);

      expect(captureCurrentReplyMessageInjectionTarget("agent:main:main")).toBeUndefined();
      await expect(queueReplyMessageInjectionTarget(target!, "stale")).resolves.toMatchObject({
        status: "rejected",
        reason: "stale_run",
      });
      expect(queueMessage).not.toHaveBeenCalled();

      operation.watchdog.progress("semantic");

      expect(captureCurrentReplyMessageInjectionTarget("agent:main:main")).toBeDefined();
      await expect(queueReplyMessageInjectionTarget(target!, "fresh")).resolves.toEqual({
        status: "accepted",
      });
      expect(queueMessage).toHaveBeenCalledWith(
        "fresh",
        expect.objectContaining({ onQueueAccepted: expect.any(Function) }),
      );
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not queue messages through stopped backends", async () => {
    const queueMessage = vi.fn(async () => {});
    const operation = createTestReplyOperation({
      sessionId: "session-running",
    });

    operation.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      cancel: vi.fn(),
      isStreaming: () => true,
      isStopped: () => true,
      queueMessage,
    });
    operation.setPhase("running");

    await expect(queueCurrentReplyRunMessage("session-running", "hello")).resolves.toMatchObject({
      status: "rejected",
      reason: "injection_unavailable",
    });
    expect(queueMessage).not.toHaveBeenCalled();
  });

  it.each(["isStopped", "isCompacting"] as const)(
    "fails closed when backend %s checks throw",
    async (probe) => {
      const queueMessage = vi.fn(async () => {});
      const operation = createTestReplyOperation({
        sessionId: "session-running",
      });

      operation.attachBackend({
        kind: "embedded",
        supportsTranscriptCommitWait: true,
        cancel: vi.fn(),
        isStreaming: () => true,
        [probe]: () => {
          throw new Error("bad stopped state");
        },
        queueMessage,
      });
      operation.setPhase("running");

      await expect(queueCurrentReplyRunMessage("session-running", "hello")).resolves.toMatchObject({
        status: "rejected",
        reason: "injection_unavailable",
      });
      expect(queueMessage).not.toHaveBeenCalled();
    },
  );

  it("requires a real injection capability", () => {
    const operation = createTestReplyOperation({ originatingLeafEntryId: "leaf-a" });
    operation.setPhase("running");
    operation.attachBackend({ kind: "cli", runId: "run-a", cancel: vi.fn() });

    expect(captureCurrentReplyMessageInjectionTarget(operation.key)).toBeUndefined();
  });

  it.each([
    { source: "sync", unconfirmed: false },
    { source: "async", unconfirmed: true },
    { source: "mismatched-question", unconfirmed: false },
    { source: "mismatched-question", unconfirmed: true },
  ] as const)(
    "distinguishes rejection from non-replayable input: $source (unconfirmed=$unconfirmed)",
    async ({ source, unconfirmed }) => {
      const cause = new Error(`${source} rejection`);
      const error = unconfirmed ? new QuestionAnswerUnconfirmedError(cause) : cause;
      const queueMessage = vi.fn((): Promise<void> => {
        if (source === "sync") {
          throw error;
        }
        return Promise.reject(error);
      });
      const claimPendingUserInputAnswer = vi.fn(async () => {
        throw error;
      });
      const operation = createTestReplyOperation({ originatingLeafEntryId: "leaf-a" });
      operation.setPhase("running");
      operation.attachBackend({
        kind: "embedded",
        supportsTranscriptCommitWait: true,
        runId: "run-a",
        toolAuthorityFingerprint: "active-authority",
        cancel: vi.fn(),
        claimPendingUserInputAnswer,
        messageInjection: { isAvailable: () => true, queueMessage },
      });
      const target = captureCurrentReplyMessageInjectionTarget(operation.key)!;
      const confirmSteerTargetRunIdForPersistence = vi.fn(async () => {});
      const recorder = {
        ...createUserTurnTranscriptRecorder({
          input: { text: "answer" },
          target: createTestUserTurnTranscriptTarget(),
        }),
        confirmSteerTargetRunIdForPersistence,
      };
      const onQueueAccepted = vi.fn();
      const mismatch = source === "mismatched-question";
      const attempt = beginReplyMessageInjectionTarget(target, "answer", {
        isInboundUserMessage: true,
        toolAuthorityFingerprint: mismatch ? "incoming-authority" : "active-authority",
        pendingInputAuthorityFingerprint: "active-authority",
        waitForTranscriptCommit: true,
        userTurnTranscriptRecorder: recorder,
        onQueueAccepted,
      });

      await expect(attempt.acceptance).resolves.toBe(unconfirmed);
      if (unconfirmed) {
        await expect(attempt.outcome).resolves.toEqual({
          status: "indeterminate",
          errorMessage: error.message,
        });
        expect(onQueueAccepted).toHaveBeenCalledExactlyOnceWith(true);
      } else {
        await expect(attempt.outcome).resolves.toMatchObject({
          status: "rejected",
          reason: "runtime_rejected",
          errorMessage: String(error),
        });
      }
      expect(confirmSteerTargetRunIdForPersistence).not.toHaveBeenCalled();
      expect(queueMessage).toHaveBeenCalledTimes(mismatch ? 0 : 1);
      expect(claimPendingUserInputAnswer).toHaveBeenCalledTimes(mismatch ? 1 : 0);
    },
  );

  it("reports callback acceptance before outcome and composes the caller callback", async () => {
    const delivery = createDeferred();
    const callerOnQueueAccepted = vi.fn();
    let queueOptions: ReplyBackendQueueMessageOptions | undefined;
    const operation = createTestReplyOperation({ originatingLeafEntryId: "leaf-a" });
    operation.setPhase("running");
    operation.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      runId: "run-a",
      cancel: vi.fn(),
      messageInjection: {
        isAvailable: () => true,
        queueMessage: vi.fn((_text, options) => {
          queueOptions = options;
          return delivery.promise;
        }),
      },
    });
    const target = captureCurrentReplyMessageInjectionTarget(operation.key)!;
    const attempt = beginReplyMessageInjectionTarget(target, "accepted", {
      onQueueAccepted: callerOnQueueAccepted,
    });
    let outcomeSettled = false;
    void attempt.outcome.then(() => {
      outcomeSettled = true;
    });

    queueOptions?.onQueueAccepted?.(true);

    await expect(attempt.acceptance).resolves.toBe(true);
    expect(callerOnQueueAccepted).toHaveBeenCalledWith(true);
    expect(outcomeSettled).toBe(false);
    delivery.resolve();
    await expect(attempt.outcome).resolves.toEqual({ status: "accepted" });
  });

  it.each(
    (["direct", "wrapped", "unconfirmed"] as const).flatMap((failure) =>
      [false, true].map((bound) => ({ failure, bound })),
    ),
  )(
    "preserves accepted custody failure semantics ($failure, bound: $bound)",
    async ({ failure, bound }) => {
      const custodyError = new SessionPendingInputCustodyError(
        "Pending input ownership ended; submit a new turn to continue",
      );
      expect(custodyError.name).toBe("Error");
      expect(String(custodyError)).toBe(
        "Error: Pending input ownership ended; submit a new turn to continue",
      );
      const error =
        failure === "wrapped"
          ? new Error("Runtime persistence failed", { cause: custodyError })
          : failure === "unconfirmed"
            ? new QuestionAnswerUnconfirmedError(custodyError)
            : custodyError;
      const delivery = createDeferred();
      let sourceCurrent = true;
      const sourceAuthority = vi.fn(() => {
        if (!sourceCurrent) {
          throw new Error("Source authority closed after acceptance");
        }
      });
      const cancel = vi.fn();
      const operation = createTestReplyOperation({ originatingLeafEntryId: "leaf-a" });
      operation.setPhase("running");
      operation.attachBackend({
        kind: "embedded",
        supportsTranscriptCommitWait: true,
        runId: "run-a",
        cancel,
        messageInjectionV2: {
          version: 2,
          isAvailable: () => true,
          queueMessage: (_text, options, assertCurrent) => {
            assertCurrent();
            options?.onQueueAccepted?.(true);
            return delivery.promise;
          },
        },
      });
      const target = captureCurrentReplyMessageInjectionTarget(operation.key)!;
      const onQueueAccepted = vi.fn();
      const attempt = beginReplyMessageInjectionTarget(target, "accepted input", {
        ...(bound ? { assertCurrent: sourceAuthority } : {}),
        onQueueAccepted,
      });
      await expect(attempt.acceptance).resolves.toBe(true);
      expect(sourceAuthority).toHaveBeenCalledTimes(bound ? 1 : 0);
      sourceCurrent = false;
      delivery.reject(error);

      if (failure === "unconfirmed") {
        await expect(attempt.outcome).resolves.toEqual({
          status: "indeterminate",
          errorMessage: error.message,
        });
      } else if (bound) {
        await expect(attempt.outcome).resolves.toEqual({
          status: "failed",
          error: custodyError,
        });
        await expect(finalizeReplyMessageInjectionAttempt({ attempt, target })).rejects.toBe(
          custodyError,
        );
      } else {
        await expect(attempt.outcome).resolves.toEqual({
          status: "indeterminate",
          errorMessage: String(error),
        });
        await expect(
          finalizeReplyMessageInjectionAttempt({ attempt, target }),
        ).resolves.toMatchObject({
          status: "indeterminate",
        });
      }
      await expect(attempt.acceptance).resolves.toBe(true);
      expect(onQueueAccepted).toHaveBeenCalledExactlyOnceWith(true);
      expect(sourceAuthority).toHaveBeenCalledTimes(bound ? 1 : 0);
      expect(cancel).not.toHaveBeenCalled();
      expect(operation.result).toBeNull();
    },
  );

  it("falls back to queue settlement when the backend ignores acceptance callbacks", async () => {
    const operation = createTestReplyOperation({ originatingLeafEntryId: "leaf-a" });
    operation.setPhase("running");
    operation.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      runId: "run-a",
      cancel: vi.fn(),
      messageInjection: { isAvailable: () => true, queueMessage: vi.fn(async () => {}) },
    });
    const target = captureCurrentReplyMessageInjectionTarget(operation.key)!;
    const accepted = beginReplyMessageInjectionTarget(target, "accepted");
    await expect(accepted.acceptance).resolves.toBe(true);

    operation.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      runId: "run-a",
      cancel: vi.fn(),
      messageInjection: {
        isAvailable: () => true,
        queueMessage: vi.fn(async () => {
          throw new Error("rejected");
        }),
      },
    });
    const rejected = beginReplyMessageInjectionTarget(target, "rejected");
    await expect(rejected.acceptance).resolves.toBe(false);
  });

  it("keeps callback acceptance authoritative over later queue rejection", async () => {
    const delivery = createDeferred();
    let queueOptions: ReplyBackendQueueMessageOptions | undefined;
    const operation = createTestReplyOperation({ originatingLeafEntryId: "leaf-a" });
    operation.setPhase("running");
    operation.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      runId: "run-a",
      cancel: vi.fn(),
      messageInjection: {
        isAvailable: () => true,
        queueMessage: vi.fn((_text, options) => {
          queueOptions = options;
          return delivery.promise;
        }),
      },
    });
    const target = captureCurrentReplyMessageInjectionTarget(operation.key)!;
    const attempt = beginReplyMessageInjectionTarget(target, "uncertain");

    queueOptions?.onQueueAccepted?.(true);
    delivery.reject(new Error("transcript unconfirmed"));

    await expect(attempt.acceptance).resolves.toBe(true);
    await expect(attempt.outcome).resolves.toMatchObject({ status: "indeterminate" });
  });

  it("rejects an ABA successor even when key and leaf are reused", async () => {
    const first = createTestReplyOperation({ originatingLeafEntryId: "leaf-a" });
    first.setPhase("running");
    first.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      runId: "run-a",
      cancel: vi.fn(),
      messageInjection: { isAvailable: () => true, queueMessage: vi.fn(async () => {}) },
    });
    const target = captureCurrentReplyMessageInjectionTarget(first.key)!;
    first.complete();
    const successorQueue = vi.fn(async () => {});
    const successor = createTestReplyOperation({ originatingLeafEntryId: "leaf-a" });
    successor.setPhase("running");
    successor.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      runId: "run-a",
      cancel: vi.fn(),
      messageInjection: { isAvailable: () => true, queueMessage: successorQueue },
    });

    await expect(queueReplyMessageInjectionTarget(target, "must not move")).resolves.toMatchObject({
      status: "rejected",
      reason: "no_active_run",
    });
    expect(successorQueue).not.toHaveBeenCalled();
  });

  it("uses a replacement backend on the same operation", async () => {
    const operation = createTestReplyOperation({ originatingLeafEntryId: "leaf-a" });
    operation.setPhase("running");
    const firstQueue = vi.fn(async () => {});
    const first = {
      kind: "embedded" as const,
      runId: "run-a",
      cancel: vi.fn(),
      messageInjection: { isAvailable: () => true, queueMessage: firstQueue },
    };
    operation.attachBackend(first);
    const target = captureCurrentReplyMessageInjectionTarget(operation.key)!;
    const replacementQueue = vi.fn(async () => {});
    operation.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      runId: "run-a",
      cancel: vi.fn(),
      messageInjection: { isAvailable: () => true, queueMessage: replacementQueue },
    });

    await expect(queueReplyMessageInjectionTarget(target, "replacement")).resolves.toEqual({
      status: "accepted",
    });
    expect(firstQueue).not.toHaveBeenCalled();
    expect(replacementQueue).toHaveBeenCalledWith(
      "replacement",
      expect.objectContaining({ onQueueAccepted: expect.any(Function) }),
    );
  });

  it("keeps an invoked queue authoritative when the owner clears synchronously", async () => {
    const operation = createTestReplyOperation({ originatingLeafEntryId: "leaf-a" });
    operation.setPhase("running");
    const queueMessage = vi.fn(async () => {
      operation.complete();
    });
    operation.attachBackend({
      kind: "embedded",
      supportsTranscriptCommitWait: true,
      runId: "run-a",
      cancel: vi.fn(),
      messageInjection: { isAvailable: () => true, queueMessage },
    });
    const target = captureCurrentReplyMessageInjectionTarget(operation.key)!;

    await expect(queueReplyMessageInjectionTarget(target, "last input")).resolves.toEqual({
      status: "accepted",
    });
    expect(isSessionRunActiveForKey(operation.key)).toBe(false);
  });

  registerReplyOperationRekeyCases();
});
/* oxlint-disable max-lines -- TODO: split this grandfathered oversized file. */
