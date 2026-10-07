import { importFreshModule } from "openclaw/plugin-sdk/test-fixtures";
import { afterEach, describe, expect, it, vi } from "vitest";
import { testing as replyRunTesting } from "../../auto-reply/reply/reply-run-registry.test-support.js";
import { setDiagnosticsEnabledForProcess } from "../../infra/diagnostic-events.js";
import { resetDiagnosticRunActivityForTest } from "../../logging/diagnostic-run-activity.js";
import { resetDiagnosticSessionStateForTest } from "../../logging/diagnostic-session-state.js";
import { createReplyOperation, isSessionRunActive } from "../../sessions/session-controller.js";
import { isSessionRunCompactionBlocked } from "../../sessions/session-controller.queries.js";
import { createDeferredCore } from "../../shared/deferred.js";
import { withGatewayToolCallerIdentity } from "../tools/gateway-caller-context.js";
import { prepareEmbeddedRunPermissionChange } from "./run-permissions.js";
import { createEmbeddedRunPermissionChanges } from "./run/permission-change.js";
import {
  abortAndDrainEmbeddedAgentRun,
  abortEmbeddedAgentRun,
  isEmbeddedAgentRunHandleActive,
  markActiveEmbeddedRunAbandoned,
  prepareEmbeddedAgentRunCompletionClaim,
  resolveActiveEmbeddedRunOwner,
  resolveActiveEmbeddedRunOwnerByRunId,
  resolveEmbeddedRunAbandonment,
  setActiveEmbeddedRun as setDetachedEmbeddedRun,
} from "./runs.js";
import {
  clearTestEmbeddedRun as clearActiveEmbeddedRun,
  registerTestEmbeddedRun as setActiveEmbeddedRun,
  createEmbeddedRunHandle,
  testing,
} from "./runs.test-support.js";

const sessionId = "session";
const sessionKey = "agent:main:test";
const createRunHandle = createEmbeddedRunHandle;

function startReply(handle: ReturnType<typeof createRunHandle>) {
  const operation = createReplyOperation({ sessionId, sessionKey, resetTriggered: false });
  const backend = {
    kind: "embedded" as const,
    cancel: handle.abort,
    isStreaming: handle.isStreaming,
    isAbortable: handle.isAbortable,
    isCompacting: handle.isCompacting,
  };
  operation.setPhase("running");
  operation.attachBackend(backend);
  setActiveEmbeddedRun(sessionId, handle);
  return { operation, backend };
}

afterEach(() => {
  testing.resetActiveEmbeddedRuns();
  resetDiagnosticRunActivityForTest();
  replyRunTesting.resetReplyRunRegistry();
  resetDiagnosticSessionStateForTest();
  setDiagnosticsEnabledForProcess(false);
  vi.restoreAllMocks();
});

describe("embedded run ownership", () => {
  it.each([true, false])(
    "fences replacement permission acknowledgements: %s",
    async (sameOwner) => {
      const completed = createDeferredCore<boolean>();
      const owner = createEmbeddedRunPermissionChanges({});
      const other = createEmbeddedRunPermissionChanges({});
      setActiveEmbeddedRun(sessionId, {
        ...createRunHandle({ runId: "run" }),
        permissionChangeOwner: owner.forAttempt().owner,
        applyPermissionMode: () => completed.promise,
      });
      const change = prepareEmbeddedRunPermissionChange(sessionId);
      if (change.kind !== "active") {
        throw new Error("expected an active permission change");
      }
      const acknowledgement = change.apply("full", vi.fn());
      setActiveEmbeddedRun(sessionId, {
        ...createRunHandle({ runId: "run" }),
        permissionChangeOwner: (sameOwner ? owner : other).forAttempt().owner,
      });
      completed.resolve(true);
      await expect(acknowledgement).resolves.toBe(sameOwner);
      owner.close();
      other.close();
    },
  );

  it("rejects permissions captured by a replaced run", async () => {
    const applyPermissionMode = vi.fn(async () => true);
    setActiveEmbeddedRun(sessionId, { ...createRunHandle(), applyPermissionMode });
    const change = prepareEmbeddedRunPermissionChange(sessionId);
    if (change.kind !== "active") {
      throw new Error("expected an active permission change");
    }
    setActiveEmbeddedRun(sessionId, createRunHandle());
    await expect(change.apply("full", vi.fn())).resolves.toBe(false);
    expect(applyPermissionMode).not.toHaveBeenCalled();
  });

  it("skips failed compaction probes when aborting", () => {
    const unknown = vi.fn(),
      compacting = vi.fn(),
      normal = vi.fn();
    setActiveEmbeddedRun("unknown", {
      ...createRunHandle({ abort: unknown }),
      isCompacting: () => {
        throw new Error("compaction probe unavailable");
      },
    });
    setActiveEmbeddedRun("compacting", createRunHandle({ isCompacting: true, abort: compacting }));
    setActiveEmbeddedRun("normal", createRunHandle({ abort: normal }));
    expect(abortEmbeddedAgentRun(undefined, { mode: "compacting" })).toBe(true);
    expect(unknown).not.toHaveBeenCalled();
    expect(compacting).toHaveBeenCalledOnce();
    expect(normal).not.toHaveBeenCalled();
  });

  it("keeps queued reply operations out of compact abort checks", () => {
    const operation = createReplyOperation({
      sessionKey: "agent:main:main",
      sessionId: "session-reply-run",
      resetTriggered: false,
    });

    expect(isSessionRunCompactionBlocked("session-reply-run")).toBe(false);

    operation.setPhase("running");

    expect(isSessionRunCompactionBlocked("session-reply-run")).toBe(true);
  });

  it("aborts every active run in all mode", () => {
    const abortA = vi.fn();
    const abortB = vi.fn();

    setActiveEmbeddedRun(
      "session-a",
      createEmbeddedRunHandle({ isCompacting: true, abort: abortA }),
    );

    setActiveEmbeddedRun("session-b", createEmbeddedRunHandle({ abort: abortB }));

    const aborted = abortEmbeddedAgentRun(undefined, { mode: "all" });
    expect(aborted).toBe(true);
    expect(abortA).toHaveBeenCalledTimes(1);
    expect(abortB).toHaveBeenCalledTimes(1);
  });

  it("keeps finalizing runs active while rejecting abort requests", () => {
    const abort = vi.fn();
    const handle = createEmbeddedRunHandle({ abort, isAbortable: false });
    const { operation, backend } = startReply(handle);
    expect(abortEmbeddedAgentRun(sessionId)).toBe(false);
    expect(abortEmbeddedAgentRun(undefined, { mode: "all", reason: "restart" })).toBe(false);
    expect(isSessionRunCompactionBlocked(sessionId)).toBe(true);
    expect(isEmbeddedAgentRunHandleActive(sessionId)).toBe(true);
    expect(operation.result).toBeNull();
    expect(isSessionRunActive(sessionId)).toBe(true);
    expect(abort).not.toHaveBeenCalled();
    clearActiveEmbeddedRun(sessionId, handle);
    operation.detachBackend(backend);
    expect(abortEmbeddedAgentRun(undefined, { mode: "all" })).toBe(true);
    expect(operation.result).toEqual({ kind: "aborted", code: "aborted_for_restart" });
    operation.complete();
    expect(isEmbeddedAgentRunHandleActive(sessionId)).toBe(false);
    expect(isSessionRunActive(sessionId)).toBe(false);
  });

  it("keeps frozen run ownership through forced in-process restart", () => {
    const abort = vi.fn();
    const handle = createEmbeddedRunHandle({ abort, isAbortable: false });
    const operation = createReplyOperation({
      sessionKey: "agent:main:restart-finalizing",
      sessionId: "session-restart-finalizing",
      resetTriggered: false,
    });
    const replyBackend = {
      kind: "embedded" as const,
      cancel: handle.abort,
      isStreaming: handle.isStreaming,
      isAbortable: handle.isAbortable,
    };
    operation.setPhase("running");
    operation.attachBackend(replyBackend);
    setActiveEmbeddedRun("session-restart-finalizing", handle);

    expect(abortEmbeddedAgentRun(undefined, { mode: "all", reason: "restart" })).toBe(false);
    expect(isEmbeddedAgentRunHandleActive("session-restart-finalizing")).toBe(true);
    expect(isSessionRunActive("session-restart-finalizing")).toBe(true);
    expect(operation.result).toBeNull();
    expect(abort).not.toHaveBeenCalled();

    clearActiveEmbeddedRun("session-restart-finalizing", handle);
    operation.detachBackend(replyBackend);
    operation.complete();
    expect(isEmbeddedAgentRunHandleActive("session-restart-finalizing")).toBe(false);
    expect(isSessionRunActive("session-restart-finalizing")).toBe(false);
  });

  it("expires reply-owned stuck recovery as run_stalled instead of user abort", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    try {
      const cancel = vi.fn();
      const operation = createReplyOperation({
        sessionKey: "agent:main:reply-stuck",
        sessionId: "session-reply-stuck",
        resetTriggered: false,
      });
      cancel.mockImplementation(() => operation.complete());
      operation.attachBackend({
        kind: "embedded",
        cancel,
        isStreaming: () => true,
      });
      operation.setPhase("running");

      vi.setSystemTime(6 * 60_000);
      const result = await abortAndDrainEmbeddedAgentRun({
        sessionId: "session-reply-stuck",
        sessionKey: "agent:main:reply-stuck",
        reason: "stuck_recovery",
        forceClear: true,
      });

      expect(result).toEqual({ aborted: true, drained: true, forceCleared: false });
      expect(operation.result).toEqual({ kind: "failed", code: "run_stalled" });
      expect(cancel).toHaveBeenCalledWith("superseded");
    } finally {
      vi.useRealTimers();
    }
  });

  it("cancels a detached handle during stuck recovery", async () => {
    vi.useFakeTimers();
    try {
      const abort = vi.fn();
      const handle = createEmbeddedRunHandle({ abort, runId: "detached-stuck" });
      setDetachedEmbeddedRun("session-detached-stuck", handle);

      const pending = abortAndDrainEmbeddedAgentRun({
        sessionId: "session-detached-stuck",
        reason: "stuck_recovery",
        settleMs: 100,
      });

      expect(abort).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(100);
      await expect(pending).resolves.toEqual({
        aborted: true,
        drained: false,
        forceCleared: false,
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("expires stuck recovery as run_stalled even with a live embedded handle", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    try {
      // The live-handle path is the common field case: the wedged run still owns
      // a registered handle, and its abort handler re-enters abortByUser. The
      // expiry must win the attribution race (run_stalled, not aborted_by_user).
      const operation = createReplyOperation({
        sessionKey: "agent:main:reply-stuck-live",
        sessionId: "session-reply-stuck-live",
        resetTriggered: false,
      });
      const handle = createEmbeddedRunHandle({
        abort: () => {
          operation.abortByUser();
        },
      });
      operation.attachBackend({
        kind: "embedded",
        cancel: handle.abort,
        isStreaming: handle.isStreaming,
      });
      operation.setPhase("running");
      setActiveEmbeddedRun("session-reply-stuck-live", handle);

      vi.setSystemTime(6 * 60_000);
      const pending = abortAndDrainEmbeddedAgentRun({
        sessionId: "session-reply-stuck-live",
        sessionKey: "agent:main:reply-stuck-live",
        reason: "stuck_recovery",
        forceClear: true,
        settleMs: 50,
      });

      await vi.advanceTimersByTimeAsync(100);
      const result = await pending;
      expect(result.aborted).toBe(true);
      expect(operation.result).toEqual({ kind: "failed", code: "run_stalled" });
      clearActiveEmbeddedRun("session-reply-stuck-live", handle);
      operation.complete();
    } finally {
      vi.useRealTimers();
    }
  });

  it("preserves frozen ownership during compacting aborts", () => {
    const abort = vi.fn();
    const handle = createEmbeddedRunHandle({ abort });
    const operation = createReplyOperation({
      sessionKey: "agent:main:restart-owned",
      sessionId: "session-restart-owned",
      resetTriggered: false,
    });
    operation.setPhase("running");
    operation.attachBackend({
      kind: "embedded",
      cancel: handle.abort,
      isStreaming: handle.isStreaming,
      isAbortable: handle.isAbortable,
    });
    setActiveEmbeddedRun("session-restart-owned", handle);

    expect(abortEmbeddedAgentRun(undefined, { mode: "all", reason: "restart" })).toBe(true);
    expect(operation.result).toEqual({ kind: "aborted", code: "aborted_for_restart" });
    expect(abort).toHaveBeenCalledTimes(1);
    expect(abort).toHaveBeenCalledWith("restart");
  });

  it.each(["all", "compacting"] as const)(
    "does not bypass frozen shared ownership through %s handle aborts",
    (mode) => {
      const abort = vi.fn();
      const handle = createEmbeddedRunHandle({ abort, isCompacting: true });
      const caseSessionId = `session-restart-frozen-${mode}`;
      const operation = createReplyOperation({
        sessionKey: `agent:main:restart-frozen-${mode}`,
        sessionId: caseSessionId,
        resetTriggered: false,
      });
      operation.setPhase("running");
      operation.attachBackend({
        kind: "embedded",
        cancel: handle.abort,
        isStreaming: handle.isStreaming,
        isAbortable: handle.isAbortable,
        isCompacting: handle.isCompacting,
      });
      operation.freezeAbort();
      setActiveEmbeddedRun(caseSessionId, handle);

      expect(abortEmbeddedAgentRun(undefined, { mode, reason: "restart" })).toBe(false);
      expect(operation.result).toBeNull();
      expect(abort).not.toHaveBeenCalled();
    },
  );

  it("keeps shared restart ownership when the attached cancel callback throws", () => {
    const abort = vi.fn(() => {
      throw new Error("cancel failed");
    });
    const handle = createEmbeddedRunHandle({ abort });
    const operation = createReplyOperation({
      sessionKey: "agent:main:restart-throwing",
      sessionId: "session-restart-throwing",
      resetTriggered: false,
    });
    operation.setPhase("running");
    operation.attachBackend({
      kind: "embedded",
      cancel: handle.abort,
      isStreaming: handle.isStreaming,
      isAbortable: handle.isAbortable,
    });
    setActiveEmbeddedRun("session-restart-throwing", handle);

    expect(abortEmbeddedAgentRun(undefined, { mode: "all", reason: "restart" })).toBe(true);
    expect(operation.result).toEqual({ kind: "aborted", code: "aborted_for_restart" });
    expect(abort).toHaveBeenCalledTimes(1);
  });

  it("does not bypass retained terminal ownership through compacting handle aborts", () => {
    const abort = vi.fn();
    const handle = createEmbeddedRunHandle({ abort, isCompacting: true });
    const operation = createReplyOperation({
      sessionKey: "agent:main:restart-failed-compacting",
      sessionId: "session-restart-failed-compacting",
      resetTriggered: false,
    });
    operation.setPhase("running");
    operation.attachBackend({
      kind: "embedded",
      cancel: handle.abort,
      isStreaming: handle.isStreaming,
      isAbortable: handle.isAbortable,
      isCompacting: handle.isCompacting,
    });
    setActiveEmbeddedRun("session-restart-failed-compacting", handle);
    operation.fail("run_failed", new Error("terminal failure"));

    expect(abortEmbeddedAgentRun(undefined, { mode: "compacting", reason: "restart" })).toBe(false);
    expect(operation.result).toMatchObject({ kind: "failed", code: "run_failed" });
    expect(abort).not.toHaveBeenCalled();
  });

  it("preserves restart ownership when cancellation throws", () => {
    const abort = vi.fn(() => {
      throw new Error("cancel failed");
    });
    const { operation } = startReply(createRunHandle({ abort }));
    expect(abortEmbeddedAgentRun(undefined, { mode: "all", reason: "restart" })).toBe(true);
    expect(operation.result).toEqual({ kind: "aborted", code: "aborted_for_restart" });
    expect(abort).toHaveBeenCalledExactlyOnceWith("restart");
  });

  it("fences timeout recovery across module instances", async () => {
    const runsA = await importFreshModule<typeof import("./runs.js")>(
      import.meta.url,
      "./runs.js?scope=recovery-a",
    );
    const runsB = await importFreshModule<typeof import("./runs.js")>(
      import.meta.url,
      "./runs.js?scope=recovery-b",
    );
    const first = createRunHandle({ runId: "first" }),
      replacement = createRunHandle({ runId: "second" });
    const operation = createReplyOperation({ sessionId, sessionKey, resetTriggered: false });
    try {
      runsA.setActiveEmbeddedRun(sessionId, first, sessionKey, undefined, undefined, operation);
      expect(
        runsA.markActiveEmbeddedRunAbandoned({
          sessionId,
          sessionKey,
          handle: first,
          reason: "timeout",
        }),
      ).toBe(true);
      expect(runsA.markEmbeddedRunRecoveringTimeout({ sessionId, runId: "other" })).toBeUndefined();
      const stale = runsA.markEmbeddedRunRecoveringTimeout({ sessionId, runId: "first" });
      expect(stale).toBeDefined();
      runsB.setActiveEmbeddedRun(
        sessionId,
        replacement,
        sessionKey,
        undefined,
        undefined,
        operation,
      );
      expect(
        runsA.markActiveEmbeddedRunAbandoned({
          sessionId,
          sessionKey,
          handle: first,
          reason: "timeout",
        }),
      ).toBe(false);
      expect(
        runsB.markActiveEmbeddedRunAbandoned({
          sessionId,
          sessionKey,
          handle: replacement,
          reason: "timeout",
        }),
      ).toBe(true);
      const current = runsB.markEmbeddedRunRecoveringTimeout({ sessionId, runId: "second" });
      expect(current).toBeDefined();
      expect(runsA.restoreEmbeddedRunTimeoutAbandonment(stale!)).toBe(false);
      expect(runsB.resolveEmbeddedRunAbandonment({ sessionId })).toBe("recovering_timeout");
      expect(runsB.restoreEmbeddedRunTimeoutAbandonment(current!)).toBe(true);
      expect(runsB.resolveEmbeddedRunAbandonment({ sessionId })).toBe("timeout");
    } finally {
      runsB.clearActiveEmbeddedRun(sessionId, replacement, sessionKey);
      operation.complete();
    }
  });

  it("tracks timeout abandonment by session id, key, and file until a new run starts", () => {
    const sessionFile = "/tmp/abandoned-session.jsonl",
      handle = createRunHandle();
    const timeout = { sessionKey, reason: "timeout" } as const;
    setActiveEmbeddedRun(sessionId, handle, sessionKey, sessionFile);
    expect(markActiveEmbeddedRunAbandoned({ ...timeout, sessionId, handle, sessionFile })).toBe(
      true,
    );
    expect(resolveEmbeddedRunAbandonment({ sessionId })).toBe("timeout");
    expect(resolveEmbeddedRunAbandonment({ sessionKey })).toBe("timeout");
    expect(resolveEmbeddedRunAbandonment({ sessionFile })).toBe("timeout");
    const next = createRunHandle();
    clearActiveEmbeddedRun(sessionId, handle, sessionKey, sessionFile);
    setActiveEmbeddedRun("next", next, sessionKey, sessionFile);
    expect(resolveEmbeddedRunAbandonment({ sessionId })).toBeUndefined();
    expect(resolveEmbeddedRunAbandonment({ sessionKey })).toBeUndefined();
    expect(resolveEmbeddedRunAbandonment({ sessionFile })).toBeUndefined();
    expect(markActiveEmbeddedRunAbandoned({ ...timeout, sessionId: "next", handle: next })).toBe(
      true,
    );
    clearActiveEmbeddedRun("next", next, sessionKey, sessionFile);
    setActiveEmbeddedRun("third", createRunHandle(), sessionKey);
    expect(resolveEmbeddedRunAbandonment({ sessionKey })).toBeUndefined();
  });

  it("revokes prepared claims on abort", () => {
    const handle = createRunHandle({ runId: "run" });
    const { claimCompletion } = prepareEmbeddedAgentRunCompletionClaim(sessionId, "run");
    setActiveEmbeddedRun(sessionId, handle);
    expect(abortEmbeddedAgentRun(sessionId)).toBe(true);
    clearActiveEmbeddedRun(sessionId, handle);
    expect(claimCompletion()).toBe(false);
  });

  it("rejects Stop captured before owner replacement", () => {
    const firstAbort = vi.fn(),
      secondAbort = vi.fn();
    const first = { ...createRunHandle({ runId: "first", abort: firstAbort }), startedAtMs: 123 };
    setActiveEmbeddedRun(sessionId, first, sessionKey);
    const identity = resolveActiveEmbeddedRunOwnerByRunId("first");
    const expected = { runId: "first", sessionId, sessionKey, startedAtMs: 123 };
    expect(identity).toMatchObject(expected);
    expect(resolveActiveEmbeddedRunOwner(sessionId)).toMatchObject(expected);
    setActiveEmbeddedRun(
      sessionId,
      createRunHandle({ runId: "second", abort: secondAbort }),
      sessionKey,
    );
    expect(identity?.abort()).toBe(false);
    expect(firstAbort).not.toHaveBeenCalled();
    expect(secondAbort).not.toHaveBeenCalled();
  });

  it("publishes completion authority and fences later session owners", async () => {
    const first = createRunHandle({ runId: "first" });
    const oldClaim = prepareEmbeddedAgentRunCompletionClaim(sessionId, "first");
    let published = false;
    void oldClaim.registered.then(() => {
      published = true;
    });
    await Promise.resolve();
    expect(published).toBe(false);
    await withGatewayToolCallerIdentity(
      {
        agentId: "main",
        sessionKey,
        embeddedRunToolAuthorityBinding: () => ({
          source: "reply",
          project: () => "authority",
          assertActive: () => {},
        }),
      },
      () => setActiveEmbeddedRun(sessionId, first, sessionKey),
    );
    await expect(oldClaim.registered).resolves.toEqual({
      toolAuthority: expect.objectContaining({ source: "reply" }),
    });
    clearActiveEmbeddedRun(sessionId, first);
    const intervening = createRunHandle({ runId: "intervening" });
    setActiveEmbeddedRun(sessionId, intervening);
    clearActiveEmbeddedRun(sessionId, intervening);
    expect(oldClaim.claimCompletion()).toBe(false);
    const next = createRunHandle({ runId: "next" });
    const currentClaim = prepareEmbeddedAgentRunCompletionClaim(sessionId, "next");
    setActiveEmbeddedRun(sessionId, next);
    await expect(currentClaim.registered).resolves.toBeUndefined();
    clearActiveEmbeddedRun(sessionId, next);
    expect(currentClaim.claimCompletion()).toBe(true);
    expect(currentClaim.claimCompletion()).toBe(false);
  });
});
