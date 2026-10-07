import { describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { createGatewayActiveWorkSnapshot } from "../../infra/gateway-active-work.js";
import {
  claimSessionControllerTask,
  releaseSessionControllerClaim,
} from "../../sessions/session-controller.mailbox.js";
import { createReplyOperation } from "../../sessions/session-controller.operation.js";
import { rpcSourceTesting } from "../../sessions/session-lifecycle-admission.test-support.js";
import { registerWorkerInferenceSessionControl } from "../worker-environments/inference-control-internal.js";
import { createWorkerInferenceCancellationService } from "../worker-environments/inference-control.test-helpers.js";
import * as abortDescendants from "./chat-abort-descendants.js";
import { handleChatAbortRequestWithLifecycle } from "./chat-abort-handler.js";
import * as persistence from "./chat-transcript-persistence.js";
import {
  expectAbortPayload,
  invokeAbort,
  requireLastRespondCall,
} from "./chat.abort-authorization.test-helpers.js";
import {
  createActiveRun,
  createChatAbortContext,
  invokeChatAbortHandler,
} from "./chat.abort.test-helpers.js";

vi.mock("../session-utils.js", async () => {
  return {
    ...(await vi.importActual<typeof import("../session-utils.js")>("../session-utils.js")),
    loadSessionEntry: () => ({ entry: { sessionId: "main-session" } }),
  };
});

function createDeferredWorkerCancellation() {
  const cancelled = createDeferred();
  const workerPersistence = createDeferred<string[]>();
  const service = {};
  registerWorkerInferenceSessionControl(service, {
    hasSession: () => true,
    reserveSessionDrain: () => {
      throw new Error("unexpected drain reservation");
    },
    resolveSessionTargetForRunId: () => undefined,
    captureSessionCancellation: () => ({
      runIds: ["worker-run"],
      cancel: (control) => {
        control?.assertCurrent?.();
        control?.onCancelled?.("worker-run");
        cancelled.resolve();
        return workerPersistence.promise;
      },
    }),
  });
  return { cancelled, workerPersistence, service };
}

describe("chat.abort original authority and registration", () => {
  it("preserves exact-run descendant and partial persistence failures after parent Stop", async () => {
    const descendantFailure = new Error("descendant cancellation failed");
    const partialFailure = new Error("partial persistence failed");
    const context = createChatAbortContext();
    const run = createActiveRun("main", { sessionId: "main-session", agentId: "main" });
    rpcSourceTesting.set("parent-run", run);
    context.chatRunState.getOrCreate("parent-run").buffer = "captured parent output";
    const descendants = vi
      .spyOn(abortDescendants, "abortControlledSubagents")
      .mockImplementationOnce(async (params) => {
        await params.beforeKill?.();
        throw descendantFailure;
      });
    const persist = vi
      .spyOn(persistence, "persistAbortedPartials")
      .mockRejectedValueOnce(partialFailure);
    const respond = vi.fn();
    try {
      await expect(
        invokeChatAbortHandler({
          handler: handleChatAbortRequestWithLifecycle,
          context,
          request: { sessionKey: "main", runId: "parent-run" },
          client: { connect: { scopes: ["operator.admin"] } },
          respond,
        }),
      ).rejects.toMatchObject({ errors: [descendantFailure, partialFailure] });
      expect(run.input.abortSignal.aborted).toBe(true);
      expect(persist).toHaveBeenCalledOnce();
      expect(respond).not.toHaveBeenCalled();
    } finally {
      descendants.mockRestore();
      persist.mockRestore();
    }
  });

  it.each([undefined, "worker-run"])(
    "waits for worker cancellation persistence before responding to Stop with runId=%s",
    async (runId) => {
      const { cancelled, workerPersistence, service } = createDeferredWorkerCancellation();
      const respond = vi.fn();
      const stopping = invokeChatAbortHandler({
        handler: handleChatAbortRequestWithLifecycle,
        context: createChatAbortContext({ workerEnvironmentService: service }),
        request: { sessionKey: "main", ...(runId ? { runId } : {}) },
        client: { connect: { scopes: ["operator.admin"] } },
        respond,
      });
      try {
        await cancelled.promise;
        expect(respond).not.toHaveBeenCalled();
      } finally {
        workerPersistence.resolve(["worker-run"]);
        await stopping;
      }
      expectAbortPayload(requireLastRespondCall(respond)[1], {
        aborted: true,
        runIds: ["worker-run"],
      });
    },
  );

  it("preserves worker cancellation and partial persistence failures after synchronous Stop", async () => {
    const { cancelled, workerPersistence, service } = createDeferredWorkerCancellation();
    const workerFailure = new Error("worker cancellation write failed");
    const partialFailure = new Error("partial output write failed");
    const context = createChatAbortContext({ workerEnvironmentService: service });
    const run = createActiveRun("main", { sessionId: "main-session", agentId: "main" });
    rpcSourceTesting.set("worker-run", run);
    context.chatRunState.getOrCreate("worker-run").buffer = "captured output";
    const persist = vi
      .spyOn(persistence, "persistAbortedPartials")
      .mockRejectedValue(partialFailure);
    const respond = vi.fn();
    const stopping = invokeChatAbortHandler({
      handler: handleChatAbortRequestWithLifecycle,
      context,
      request: { sessionKey: "main" },
      client: { connect: { scopes: ["operator.admin"] } },
      respond,
    });
    const rejected = expect(stopping).rejects.toMatchObject({
      errors: [workerFailure, partialFailure],
    });
    try {
      await cancelled.promise;
      expect(run.input.abortSignal.aborted).toBe(true);
      expect(respond).not.toHaveBeenCalled();
      workerPersistence.reject(workerFailure);
      await rejected;
      expect(persist).toHaveBeenCalledOnce();
      expect(respond).not.toHaveBeenCalled();
    } finally {
      workerPersistence.resolve([]);
      await stopping.catch(() => undefined);
      persist.mockRestore();
    }
  });

  it.each(["queued", "active"] as const)(
    "stops subsequent effects after a synchronous %s cancellation revokes authority",
    async (firstEffect) => {
      let current = true;
      const cancelInferenceForSession = vi.fn(() => ["worker"]);
      const context = createChatAbortContext({
        workerEnvironmentService: createWorkerInferenceCancellationService(
          "main-session",
          ["worker"],
          cancelInferenceForSession,
        ),
      });
      const first = createActiveRun("main", {
        sessionId: "main-session",
        agentId: "main",
        queued: firstEffect === "queued",
      });
      const second = createActiveRun("main", {
        sessionId: "main-session",
        agentId: "main",
        queued: firstEffect === "queued",
      });
      if (firstEffect === "queued") {
        rpcSourceTesting.set("first", first);
        rpcSourceTesting.set("second", second);
      } else {
        rpcSourceTesting.set("first", first);
        rpcSourceTesting.set("second", second);
        context.chatRunState.getOrCreate("first").buffer = "committed partial";
        context.chatRunState.getOrCreate("second").buffer = "untouched partial";
      }
      first.input.abortSignal.addEventListener(
        "abort",
        () => {
          current = false;
        },
        { once: true },
      );

      const persist = vi.spyOn(persistence, "persistAbortedPartials").mockResolvedValue(undefined);
      try {
        await expect(
          invokeChatAbortHandler({
            handler: (options) =>
              handleChatAbortRequestWithLifecycle({
                ...options,
                hasCurrentClientAuthority: () => current,
              }),
            context,
            request: { sessionKey: "main" },
            client: { connect: { scopes: ["operator.admin"] } },
          }),
        ).rejects.toThrow("requester authority changed");
        expect(first.input.abortSignal.aborted).toBe(true);
        expect(second.input.abortSignal.aborted).toBe(false);
        expect(cancelInferenceForSession).not.toHaveBeenCalled();
        if (firstEffect === "active") {
          expect(persist).toHaveBeenCalledOnce();
          expect(persist.mock.calls[0]?.[0].snapshots.map((snapshot) => snapshot.runId)).toEqual([
            "first",
          ]);
          expect(context.chatRunState.resolveBuffer("second", { final: true }).text).toBe(
            "untouched partial",
          );
        } else {
          expect(persist.mock.calls.flatMap(([call]) => call.snapshots)).toEqual([]);
          if (firstEffect === "queued") {
            expect(persist).not.toHaveBeenCalled();
          } else {
            expect(context.chatRunState.resolveBuffer("first", { final: true }).text).toBe(
              "committed partial",
            );
            expect(context.chatRunState.resolveBuffer("second", { final: true }).text).toBe(
              "untouched partial",
            );
          }
        }
      } finally {
        persist.mockRestore();
      }
    },
  );

  it("does not adopt a replacement active registration during a session-wide Stop", async () => {
    const context = createChatAbortContext();
    const first = createActiveRun("main", { sessionId: "main-session", agentId: "main" });
    const stale = createActiveRun("main", { sessionId: "main-session", agentId: "main" });
    const replacement = createActiveRun("main", { sessionId: "main-session", agentId: "main" });
    rpcSourceTesting.set("first", first);
    rpcSourceTesting.set("reused", stale);
    first.input.abortSignal.addEventListener(
      "abort",
      () => {
        rpcSourceTesting.set("reused", replacement);
      },
      { once: true },
    );
    const response = await invokeAbort({
      context,
      sessionKey: "main",
      connId: "owner",
      deviceId: "device",
      scopes: ["operator.admin"],
    });
    expectAbortPayload(requireLastRespondCall(response)[1], {
      aborted: true,
      runIds: ["first"],
    });
    expect(stale.input.abortSignal.aborted).toBe(false);
    expect(replacement.input.abortSignal.aborted).toBe(false);
  });

  it.each(["active", "queued", "worker"] as const)(
    "retains the original source and target fence before explicit %s cancellation",
    async (kind) => {
      for (const changed of ["source", "target"] as const) {
        const cancelInferenceForSession = vi.fn(() => ["run-1"]);
        const run = createActiveRun("agent:main:main", {
          agentId: "main",
          queued: kind === "queued",
        });
        const context = createChatAbortContext({
          workerEnvironmentService: createWorkerInferenceCancellationService(
            "main-session",
            kind === "worker" ? ["run-1"] : [],
            cancelInferenceForSession,
          ),
        });
        if (kind === "active" || kind === "queued") {
          rpcSourceTesting.set("run-1", run);
        }
        const before = [...context.dedupe];
        await expect(
          invokeChatAbortHandler({
            handler: (options) =>
              handleChatAbortRequestWithLifecycle({
                ...options,
                hasCurrentClientAuthority: () => changed !== "source",
                sessionMutationAuthorization: {
                  assertCurrent: () => {
                    throw new Error("target changed");
                  },
                  assertTargetCurrent: () => {
                    throw new Error("target changed");
                  },
                },
              }),
            context,
            request: { sessionKey: "agent:main:main", runId: "run-1" },
            client: { connId: "owner", connect: { scopes: ["operator.admin"] } },
          }),
        ).rejects.toThrow(changed === "source" ? "requester authority changed" : "target changed");
        expect(run.input.abortSignal.aborted).toBe(false);
        expect(rpcSourceTesting.has("run-1")).toBe(kind === "active" || kind === "queued");
        expect([...context.dedupe]).toEqual(before);
        expect(cancelInferenceForSession).not.toHaveBeenCalled();
      }
    },
  );

  it("does not fall back to live worker queries without a registered capture owner", async () => {
    const captureSessionCancellation = vi.fn(() => ({
      runIds: ["worker-run"],
      cancel: async () => ["worker-run"],
    }));
    const context = createChatAbortContext({
      workerEnvironmentService: {
        captureSessionCancellation,
        hasSession: () => true,
      },
    });
    for (const runId of [undefined, "worker-run"]) {
      const response = await invokeAbort({
        context,
        runId,
        connId: "admin",
        deviceId: "admin",
        scopes: ["operator.admin"],
      });
      expectAbortPayload(requireLastRespondCall(response)[1], { aborted: false, runIds: [] });
    }
    expect(captureSessionCancellation).not.toHaveBeenCalled();
  });
});

it.each([
  { scope: "session-wide", runId: undefined, waitForCleanup: true },
  { scope: "exact-run", runId: "retained-run", waitForCleanup: false },
] as const)(
  "$scope Stop preserves raw source custody after its bounded acknowledgment",
  async ({ runId, waitForCleanup }) => {
    if (waitForCleanup) {
      vi.useFakeTimers();
    }
    const raw = createDeferred();
    const cancelled = createDeferred();
    const source = createActiveRun("main", {
      sessionId: "main-session",
      agentId: "main",
      queued: true,
    });
    const claim = await claimSessionControllerTask(source.input, (selected) => {
      createReplyOperation({
        sessionKey: "main",
        sessionId: "main-session",
        agentId: "main",
        resetTriggered: false,
        mailboxClaim: selected,
      });
    });
    const producer = (async () => {
      try {
        await raw.promise;
      } finally {
        claim.operation?.complete();
        releaseSessionControllerClaim(claim);
      }
    })();
    source.input.abortSignal.addEventListener("abort", () => cancelled.resolve(), { once: true });
    const context = createChatAbortContext({ sources: new Map([["retained-run", source]]) });
    let acknowledged = false;
    const stopping = invokeChatAbortHandler({
      handler: handleChatAbortRequestWithLifecycle,
      context,
      request: { sessionKey: "main", ...(runId ? { runId } : {}) },
      client: { connect: { scopes: ["operator.admin"] } },
    })
      .then(
        () => ({ error: undefined }),
        (error: unknown) => ({ error }),
      )
      .finally(() => {
        acknowledged = true;
      });
    try {
      await cancelled.promise;
      if (waitForCleanup) {
        await vi.advanceTimersByTimeAsync(14_999);
        expect(acknowledged).toBe(false);
        await vi.advanceTimersByTimeAsync(1);
      } else {
        await stopping;
      }
      expect(acknowledged).toBe(true);
      if (waitForCleanup) {
        expect((await stopping).error).toMatchObject({
          message: expect.stringContaining("cleanup is still pending"),
        });
      } else {
        expect((await stopping).error).toBeUndefined();
      }
      expect(claim.released).toBe(false);
      expect(source.input.phase).toBe("claimed");
      const snapshot = createGatewayActiveWorkSnapshot({
        getChatRuns: () => 0,
        getQueuedTurns: () => 0,
        getTerminalPersistence: () => 0,
      });
      expect(snapshot.idle).toBe(false);
      expect(snapshot.counts.sessionAdmissions).toBe(1);
      expect(() =>
        createReplyOperation({
          sessionKey: "main",
          sessionId: "successor",
          resetTriggered: false,
          target: source.input.target,
        }),
      ).toThrow("already active");
    } finally {
      raw.resolve();
      await producer;
      await claim.settlement.promise;
      await stopping;
      if (waitForCleanup) {
        vi.useRealTimers();
      }
    }
  },
);
