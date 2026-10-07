import { AsyncLocalStorage } from "node:async_hooks";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setImmediate as nextEventLoopTurn } from "node:timers/promises";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createDeferred, withinTest } from "../../test/helpers/promise.js";
import { emitTrustedDiagnosticEvent } from "../infra/diagnostic-events.js";
import { AsyncWorkScope } from "../shared/async-work-scope.js";
import { closeOpenClawAgentDatabasesAsync } from "../state/openclaw-agent-db-lifecycle.js";
import { resolveOpenClawAgentSqlitePath } from "../state/openclaw-agent-db.paths.js";
import { captureEnv, setTestEnvValue } from "../test-utils/env.js";
import { cleanupSessionStateForTest } from "../test-utils/session-state-cleanup.js";
import { prepareClientVoiceSessionClose } from "./client-voice-session-lifecycle.js";
import {
  completeRun,
  recordMutation,
  seedSession,
} from "./client-voice-session.fixture.test-support.js";
import {
  closeClientVoiceSession,
  closeStaleClientVoiceSessions,
  createOrResumeClientVoiceSession,
  registerClientVoiceConsultRun,
} from "./client-voice-session.js";
import { clientVoiceSessionTesting } from "./client-voice-session.test-support.js";

const { sendDurableMessageBatch } = vi.hoisted(() => ({
  sendDurableMessageBatch: vi.fn(async () => ({ status: "sent" })),
}));

vi.mock("../channels/message/runtime.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../channels/message/runtime.js")>();
  return { ...actual, sendDurableMessageBatchCore: sendDurableMessageBatch };
});

const envSnapshot = captureEnv(["OPENCLAW_STATE_DIR"]);
let tempDir: string;

describe("client voice session digest retry", () => {
  beforeEach(async () => {
    tempDir = await fs.realpath(
      await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-voice-digest-retry-")),
    );
    setTestEnvValue("OPENCLAW_STATE_DIR", tempDir);
    sendDurableMessageBatch.mockReset().mockResolvedValue({ status: "sent" });
  });

  afterEach(async () => {
    clientVoiceSessionTesting.reset();
    await cleanupSessionStateForTest({ stateDir: tempDir });
    envSnapshot.restore();
    await fs.rm(tempDir, { recursive: true, force: true });
  });

  it.for([false, true])(
    "preserves a displaced metadata file and confirmed send through marker retry (replacement=%s)",
    async (replacement, { signal }) => {
      const sessionKey = "agent:main:main";
      await seedSession(sessionKey, { channel: "discord", to: "channel:marker-retry" });
      const voiceSessionId = createOrResumeClientVoiceSession({
        agentId: "main",
        sessionKey,
        origin: "client",
      });
      recordMutation(voiceSessionId);
      await completeRun(`run-${voiceSessionId}`);
      const metadataPath = resolveOpenClawAgentSqlitePath({ agentId: "main" });
      const savedPath = `${metadataPath}.displaced`;
      const sending = createDeferred();
      const finishSend = createDeferred();
      sendDurableMessageBatch.mockImplementationOnce(async () => {
        sending.resolve();
        await finishSend.promise;
        return { status: "sent" };
      });
      vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
      try {
        await closeClientVoiceSession({ agentId: "main", sessionKey, voiceSessionId, config: {} });
        await withinTest(sending.promise, signal);
        // Context replacement while send is active must share its later committed delivery fact.
        await closeClientVoiceSession({ agentId: "main", sessionKey, voiceSessionId, config: {} });
        await closeOpenClawAgentDatabasesAsync(tempDir);
        await fs.rename(metadataPath, savedPath);
        if (replacement) {
          await fs.writeFile(metadataPath, new Uint8Array());
        }
        finishSend.resolve();
        await vi.advanceTimersByTimeAsync(0);
        const afterMarker = await fs.stat(metadataPath).catch((error: unknown) => {
          expect(error).toMatchObject({ code: "ENOENT" });
          return undefined;
        });
        const untouched = replacement ? afterMarker?.size === 0 : afterMarker === undefined;
        await closeOpenClawAgentDatabasesAsync(tempDir);
        await fs.rm(metadataPath, { force: true });
        await fs.rename(savedPath, metadataPath);
        await closeClientVoiceSession({ agentId: "main", sessionKey, voiceSessionId, config: {} });
        await vi.advanceTimersByTimeAsync(0);
        expect({ untouched, sends: sendDurableMessageBatch.mock.calls.length }).toEqual({
          untouched: true,
          sends: 1,
        });
        expect(
          clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.digestDeliveredAt,
        ).toEqual(expect.any(Number));
      } finally {
        finishSend.resolve();
        await vi.advanceTimersByTimeAsync(0);
        vi.useRealTimers();
      }
    },
  );

  it("does not replay or mark a partially delivered digest after expiry and re-record", async () => {
    const sessionKey = "agent:main:main";
    await seedSession(sessionKey, { channel: "discord", to: "channel:partial-digest" });
    const voiceSessionId = createOrResumeClientVoiceSession({
      agentId: "main",
      sessionKey,
      origin: "client",
    });
    recordMutation(voiceSessionId);
    await completeRun(`run-${voiceSessionId}`);
    sendDurableMessageBatch.mockImplementationOnce(async () => ({
      status: "partial_failed",
      results: [],
      sentBeforeError: true,
      receipt: { platformMessageIds: ["partial-message"], parts: [], sentAt: 123 },
      error: new Error("partial delivery"),
    }));
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      await closeClientVoiceSession({ agentId: "main", sessionKey, voiceSessionId, config: {} });
      await vi.advanceTimersByTimeAsync(0);
      await vi.advanceTimersByTimeAsync(
        clientVoiceSessionTesting.digestDeliveryPolicy.failureRetentionMs + 1,
      );
      await closeClientVoiceSession({ agentId: "main", sessionKey, voiceSessionId, config: {} });
      await closeStaleClientVoiceSessions({ agentId: "main", config: {} });
      await vi.advanceTimersByTimeAsync(0);
      expect(sendDurableMessageBatch).toHaveBeenCalledOnce();
      expect(
        clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.digestDeliveredAt,
      ).toBeUndefined();
      expect(clientVoiceSessionTesting.digestDeliverySnapshot()).toMatchObject({
        active: 0,
        retained: 1,
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("joins accepted digest delivery beyond both slots after the closing caller returns", async ({
    signal,
  }) => {
    await seedSession("agent:main:main", { channel: "discord", to: "channel:voice-updates" });
    const ids = Array.from({ length: 4 }, (_, index) =>
      createOrResumeClientVoiceSession({
        agentId: "main",
        sessionKey: "agent:main:main",
        origin: "client",
        voiceSessionId: `queued-digest-${index}`,
      }),
    );
    for (const id of ids) {
      recordMutation(id);
      await completeRun(`run-${id}`);
    }
    const close = prepareClientVoiceSessionClose();
    const caller = new AsyncWorkScope();
    const request = new AsyncLocalStorage<string>();
    const started = ids.map(() => createDeferred());
    const release = ids.map(() => createDeferred());
    const observedRequests: Array<string | undefined> = [];
    let calls = 0;
    const failed = createDeferred<never>();
    void failed.promise.catch(() => {});
    const warning = vi.spyOn(console, "warn").mockImplementation((message) => {
      failed.reject(new Error(String(message)));
    });
    sendDurableMessageBatch.mockImplementation(async () => {
      const index = calls++;
      observedRequests.push(request.getStore());
      started[index]!.resolve();
      await release[index]!.promise;
      return { status: "sent" };
    });
    let draining: Promise<void> | undefined;
    try {
      await request.run("closing-request", () =>
        caller.track(async () => {
          for (const voiceSessionId of ids) {
            await closeClientVoiceSession({
              agentId: "main",
              sessionKey: "agent:main:main",
              voiceSessionId,
              config: {},
            });
          }
        }),
      );
      await withinTest(Promise.race([started[1]!.promise, failed.promise]), signal);
      expect(calls).toBe(2);
      caller.beginClose();
      close.beginClose();
      let settled = false;
      draining = close.drain().then(() => {
        settled = true;
      });
      await nextEventLoopTurn();
      expect(settled, "accepted summaries must settle before voice persistence closes").toBe(false);
      release[0]!.resolve();
      release[1]!.resolve();
      await withinTest(Promise.race([started[3]!.promise, failed.promise]), signal);
      release[2]!.resolve();
      release[3]!.resolve();
      await withinTest(Promise.race([draining, failed.promise]), signal);
      expect(observedRequests).toEqual([undefined, undefined, undefined, undefined]);
      expect(sendDurableMessageBatch).toHaveBeenCalledTimes(4);
      expect(sendDurableMessageBatch).toHaveBeenCalledWith(
        expect.objectContaining({
          channel: "discord",
          to: "channel:voice-updates",
          payloads: [{ text: "Voice call changes\n- message: succeeded" }],
        }),
      );
      for (const id of ids) {
        expect(clientVoiceSessionTesting.readRecord("main", id)?.digestDeliveredAt).toEqual(
          expect.any(Number),
        );
      }
    } finally {
      for (const gate of release) {
        gate.resolve();
      }
      await draining;
      await nextEventLoopTurn();
      await caller.drain();
      warning.mockRestore();
    }
  });

  it("records post-close effects and defers the digest until the last consult completes", async () => {
    await seedSession("agent:main:main", {
      channel: "discord",
      to: "channel:voice-updates",
    });
    const voiceSessionId = createOrResumeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:main",
      origin: "client",
    });
    for (const runId of ["run-1", "run-2"]) {
      registerClientVoiceConsultRun({
        agentId: "main",
        sessionKey: "agent:main:main",
        voiceSessionId,
        runId,
      });
    }

    await closeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:main",
      voiceSessionId,
      config: {},
    });
    expect(sendDurableMessageBatch).not.toHaveBeenCalled();

    for (const runId of ["run-1", "run-2"]) {
      emitTrustedDiagnosticEvent({
        type: "tool.execution.started",
        runId,
        toolCallId: `call-${runId}`,
        toolName: "message",
        mutatingAction: true,
      });
      emitTrustedDiagnosticEvent({
        type: "tool.execution.completed",
        runId,
        toolCallId: `call-${runId}`,
        toolName: "message",
        durationMs: 5,
      });
    }
    expect(clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.effects).toEqual([
      expect.objectContaining({ runId: "run-1", status: "succeeded" }),
      expect.objectContaining({ runId: "run-2", status: "succeeded" }),
    ]);

    await completeRun("run-1");
    expect(sendDurableMessageBatch).not.toHaveBeenCalled();
    await completeRun("run-2");
    await vi.waitFor(() => expect(sendDurableMessageBatch).toHaveBeenCalledTimes(1));
    expect(sendDurableMessageBatch).toHaveBeenCalledWith(
      expect.objectContaining({
        payloads: [{ text: "Voice call changes\n- message: succeeded\n- message: succeeded" }],
      }),
    );
    expect(clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.digestDeliveredAt).toEqual(
      expect.any(Number),
    );

    await closeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:main",
      voiceSessionId,
      config: {},
    });
    expect(sendDurableMessageBatch).toHaveBeenCalledTimes(1);
  });

  it("retries a deferred digest on the next lifecycle trigger after run completion", async () => {
    await seedSession("agent:main:main", {
      channel: "discord",
      to: "channel:voice-updates",
    });
    const voiceSessionId = createOrResumeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:main",
      origin: "client",
    });
    registerClientVoiceConsultRun({
      agentId: "main",
      sessionKey: "agent:main:main",
      voiceSessionId,
      runId: "run-live",
    });
    emitTrustedDiagnosticEvent({
      type: "tool.execution.started",
      runId: "run-live",
      toolCallId: "call-run-live",
      toolName: "message",
      mutatingAction: true,
    });
    emitTrustedDiagnosticEvent({
      type: "tool.execution.completed",
      runId: "run-live",
      toolCallId: "call-run-live",
      toolName: "message",
      durationMs: 5,
    });
    // Call ends while the consult still runs, so the digest is deferred.
    await closeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:main",
      voiceSessionId,
      config: {},
    });
    sendDurableMessageBatch.mockRejectedValueOnce(new Error("channel offline"));
    await completeRun("run-live");
    await vi.waitFor(() =>
      expect(clientVoiceSessionTesting.digestDeliverySnapshot().active).toBe(0),
    );
    expect(
      clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.digestDeliveredAt,
    ).toBeUndefined();

    await closeStaleClientVoiceSessions({ agentId: "main", config: {} });
    await vi.waitFor(() =>
      expect(
        clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.digestDeliveredAt,
      ).toEqual(expect.any(Number)),
    );
    expect(sendDurableMessageBatch).toHaveBeenCalledTimes(2);
  });

  it("retries the mutation digest after a transient close-time send failure", async () => {
    await seedSession("agent:main:main", {
      channel: "discord",
      to: "channel:voice-updates",
    });
    const voiceSessionId = createOrResumeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:main",
      origin: "client",
    });
    recordMutation(voiceSessionId);
    await completeRun(`run-${voiceSessionId}`);
    sendDurableMessageBatch.mockRejectedValueOnce(new Error("channel offline"));

    await closeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:main",
      voiceSessionId,
      config: {},
    });
    await vi.waitFor(() =>
      expect(clientVoiceSessionTesting.digestDeliverySnapshot().active).toBe(0),
    );
    expect(
      clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.digestDeliveredAt,
    ).toBeUndefined();

    await closeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:main",
      voiceSessionId,
      config: {},
    });
    await vi.waitFor(() =>
      expect(
        clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.digestDeliveredAt,
      ).toEqual(expect.any(Number)),
    );
    expect(sendDurableMessageBatch).toHaveBeenCalledTimes(2);
  });

  it.each(["different path", "same path replacement"] as const)(
    "isolates retained digests while recovering another physical store (%s)",
    async (replacement) => {
      const sessionKey = "agent:main:main";
      await seedSession(sessionKey, { channel: "discord", to: "channel:original-store" });
      const voiceSessionId = createOrResumeClientVoiceSession({
        agentId: "main",
        sessionKey,
        origin: "client",
      });
      recordMutation(voiceSessionId);
      await completeRun(`run-${voiceSessionId}`);
      const originalPath = resolveOpenClawAgentSqlitePath({ agentId: "main" });
      const archivedPath = path.join(tempDir, "original-agent.sqlite");
      const otherState =
        replacement === "different path" ? path.join(tempDir, "other-state") : tempDir;
      const env = captureEnv(["OPENCLAW_STATE_DIR"]);
      sendDurableMessageBatch.mockRejectedValueOnce(new Error("original channel offline"));
      vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
      try {
        await closeClientVoiceSession({ agentId: "main", sessionKey, voiceSessionId, config: {} });
        await vi.advanceTimersByTimeAsync(0);
        expect(clientVoiceSessionTesting.digestDeliverySnapshot()).toMatchObject({
          active: 0,
          retained: 1,
        });
        if (replacement === "same path replacement") {
          await closeOpenClawAgentDatabasesAsync(tempDir);
          await fs.rename(originalPath, archivedPath);
        } else {
          setTestEnvValue("OPENCLAW_STATE_DIR", otherState);
        }
        await seedSession(sessionKey);
        const stale = createOrResumeClientVoiceSession({
          agentId: "main",
          sessionKey,
          origin: "client",
          now: 1,
        });
        expect(
          await closeStaleClientVoiceSessions({
            agentId: "main",
            config: {},
            now: 6 * 60 * 60_000 + 2,
          }),
        ).toBe(1);
        await vi.advanceTimersByTimeAsync(0);
        expect(clientVoiceSessionTesting.readRecord("main", stale)?.status).toBe("closed");
        expect(clientVoiceSessionTesting.digestDeliverySnapshot()).toMatchObject({
          active: 0,
          retained: 1,
        });
        expect(sendDurableMessageBatch).toHaveBeenCalledOnce();

        createOrResumeClientVoiceSession({
          agentId: "main",
          sessionKey,
          origin: "client",
          voiceSessionId,
        });
        await expect(
          closeClientVoiceSession({ agentId: "main", sessionKey, voiceSessionId, config: {} }),
        ).rejects.toThrow("physical source");
        expect(sendDurableMessageBatch).toHaveBeenCalledOnce();

        if (replacement === "same path replacement") {
          await closeOpenClawAgentDatabasesAsync(tempDir);
          await fs.rename(originalPath, path.join(tempDir, "replacement-agent.sqlite"));
          await fs.rename(archivedPath, originalPath);
        }
        env.restore();
        await closeStaleClientVoiceSessions({ agentId: "main", config: {} });
        await vi.advanceTimersByTimeAsync(0);
        expect(sendDurableMessageBatch).toHaveBeenCalledTimes(2);
        expect(sendDurableMessageBatch).toHaveBeenLastCalledWith(
          expect.objectContaining({ to: "channel:original-store" }),
        );
        expect(
          clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.digestDeliveredAt,
        ).toEqual(expect.any(Number));
      } finally {
        vi.useRealTimers();
        env.restore();
        if (otherState !== tempDir) {
          await cleanupSessionStateForTest({ stateDir: otherState });
        }
      }
    },
  );

  it("keeps a failed digest while a late consult owns the retry", async () => {
    await seedSession("agent:main:main", {
      channel: "discord",
      to: "channel:voice-updates",
    });
    const voiceSessionId = createOrResumeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:main",
      origin: "client",
    });
    recordMutation(voiceSessionId);
    await completeRun(`run-${voiceSessionId}`);
    sendDurableMessageBatch.mockRejectedValueOnce(new Error("channel offline"));

    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      await closeClientVoiceSession({
        agentId: "main",
        sessionKey: "agent:main:main",
        voiceSessionId,
        config: {},
      });
      await vi.advanceTimersByTimeAsync(0);
      expect(clientVoiceSessionTesting.digestDeliverySnapshot()).toMatchObject({
        active: 0,
        pending: 0,
        retained: 1,
      });

      // The run can register before config arrives; an identical replay must
      // still re-arm the closed session's digest, not return at binding reuse.
      registerClientVoiceConsultRun({
        agentId: "main",
        sessionKey: "agent:main:main",
        voiceSessionId,
        runId: "late-run",
      });
      registerClientVoiceConsultRun({
        agentId: "main",
        sessionKey: "agent:main:main",
        voiceSessionId,
        runId: "late-run",
        config: {},
      });
      await vi.advanceTimersByTimeAsync(0);
      await vi.advanceTimersByTimeAsync(
        clientVoiceSessionTesting.digestDeliveryPolicy.failureRetentionMs + 1,
      );
      expect(clientVoiceSessionTesting.digestDeliverySnapshot().retained).toBe(1);

      recordMutation(voiceSessionId, "late-run");
      await completeRun("late-run");
      await vi.advanceTimersByTimeAsync(0);
      expect(
        clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.digestDeliveredAt,
      ).toEqual(expect.any(Number));
      expect(sendDurableMessageBatch).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("delivers one mutation digest and skips webchat or missing targets", async () => {
    await seedSession("agent:main:main", {
      channel: "discord",
      to: "channel:voice-updates",
    });
    const delivered = createOrResumeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:main",
      origin: "client",
    });
    recordMutation(delivered);
    await completeRun(`run-${delivered}`);
    await closeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:main",
      voiceSessionId: delivered,
      config: {},
    });
    await closeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:main",
      voiceSessionId: delivered,
      config: {},
    });
    await vi.waitFor(() => expect(sendDurableMessageBatch).toHaveBeenCalledTimes(1));
    expect(sendDurableMessageBatch).toHaveBeenCalledWith(
      expect.objectContaining({
        durability: "required",
        requireUnknownSendReconciliation: true,
        payloads: [{ text: "Voice call changes\n- message: succeeded" }],
      }),
    );

    for (const [voiceSessionId, route] of [
      ["voice-webchat", { channel: "webchat", to: "browser" }],
      ["voice-no-target", {}],
    ] as const) {
      const sessionKey = `agent:main:${voiceSessionId}`;
      await seedSession(sessionKey, route);
      createOrResumeClientVoiceSession({
        agentId: "main",
        sessionKey,
        origin: "client",
        voiceSessionId,
      });
      registerClientVoiceConsultRun({
        agentId: "main",
        sessionKey,
        voiceSessionId,
        runId: `run-${voiceSessionId}`,
      });
      emitTrustedDiagnosticEvent({
        type: "tool.execution.started",
        runId: `run-${voiceSessionId}`,
        toolCallId: `call-${voiceSessionId}`,
        toolName: "message",
        mutatingAction: true,
      });
      await completeRun(`run-${voiceSessionId}`);
      await closeClientVoiceSession({
        agentId: "main",
        sessionKey,
        voiceSessionId,
        config: {},
      });
    }
    expect(sendDurableMessageBatch).toHaveBeenCalledTimes(1);
  });
});
