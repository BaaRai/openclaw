// Tests abort request handling, cutoff persistence, and active run cleanup.
// Preserve module setup before modules that consume it.
// oxfmt-ignore
import { useChatAbortRegistryFixture } from "../../gateway/server-methods/chat.abort-registry.test-support.js";

import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { resolveSessionAgentId } from "../../agents/agent-scope.js";
import { isSubagentRegistryWriteCommand } from "../../agents/subagent-test-fixtures.test-helpers.js";
import { rowToSubagentRunRecord } from "../../agents/subagents/registry/subagent-registry.store.codec.js";
import { getSubagentRunByChildSessionKey } from "../../agents/subagents/registry/subagent-registry.test-helpers.js";
import type { OpenClawConfig } from "../../config/config.js";
import {
  loadSessionEntry,
  markSessionAbortTarget,
  resolveSessionAbortTarget,
  type SessionAbortTargetResult,
} from "../../config/sessions/session-accessor.js";
import { registerInternalHook, unregisterInternalHook } from "../../hooks/internal-hooks.js";
import { getSessionBindingService } from "../../infra/outbound/session-binding-service.js";
import {
  createReplyOperation,
  isSessionRunActiveForKey,
} from "../../sessions/session-controller.js";
import { SESSION_CONTROLLER_DRAIN_TIMEOUT_MS } from "../../sessions/session-controller.lifecycle.js";
import { resetSessionControllerStateForTest } from "../../sessions/session-lifecycle-admission.test-support.js";
import * as stateWorker from "../../state/openclaw-state-worker-store.js";
import { stopSubagentsForRequester } from "./abort-operation.js";
import { getAbortMemory, setAbortMemory } from "./abort-primitives.js";
import { writeAbortSessionStore } from "./abort-queue.test-support.js";
import {
  addSubagentFixture,
  type SubagentRunFixture,
} from "./abort-subagent-registry.test-support.js";
import { registerAbortDetectionCases } from "./abort.detection.cases.js";
import { formatAbortReplyText, tryFastAbortFromMessage } from "./abort.js";
import { enqueueFollowupRun, getFollowupQueueDepth, type FollowupRun } from "./queue.js";
import { clearFollowupQueue } from "./queue/state.js";
import { buildTestCtx } from "./test-ctx.js";

type AbortEmbeddedAgentRunOptions = Parameters<
  typeof import("../../agents/embedded-agent-runner/runs.js").abortEmbeddedAgentRun
>[1];

vi.mock("../../agents/embedded-agent.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../agents/embedded-agent.js")>()),
  abortEmbeddedAgentRun: vi.fn().mockReturnValue(true),
}));

const acpManagerMocks = vi.hoisted(() => ({
  cancelSession: vi.fn(async (_params?: unknown) => {}),
}));

const runtimeAbortMocks = vi.hoisted(() => ({
  abortEmbeddedAgentRun: vi.fn<
    (sessionId: string | undefined, opts?: AbortEmbeddedAgentRunOptions) => boolean
  >(() => true),
  isSessionRunActive: vi.fn(() => false),
}));

vi.mock("../../agents/embedded-agent-runner/runs.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../agents/embedded-agent-runner/runs.js")>()),
  abortEmbeddedAgentRun: runtimeAbortMocks.abortEmbeddedAgentRun,
  isSessionRunActive: runtimeAbortMocks.isSessionRunActive,
}));
vi.mock("../../config/sessions/session-accessor.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../config/sessions/session-accessor.js")>();
  const exactRead = await import("../../config/sessions/session-accessor.sqlite-exact-read.js");
  return {
    ...actual,
    loadExactSessionEntryReadOnly: exactRead.loadExactSessionEntryReadOnly,
    markSessionAbortTarget: vi.fn(actual.markSessionAbortTarget),
    resolveSessionAbortTarget: vi.fn(actual.resolveSessionAbortTarget),
  };
});

vi.mock("../../acp/control-plane/manager.js", () => ({
  getAcpSessionManager: () => ({ cancelSession: acpManagerMocks.cancelSession }),
}));

const abortFixture = useChatAbortRegistryFixture();

describe("abort detection", () => {
  const trackedAbortMemoryKeys = new Set<string>();

  function setTrackedAbortMemory(key: string, value: boolean): void {
    trackedAbortMemoryKeys.add(key);
    setAbortMemory(key, value);
  }

  function readAbortSessionEntry(storePath: string, sessionKey: string) {
    return loadSessionEntry({ storePath, sessionKey });
  }

  async function createAbortConfig(params?: {
    commandsTextEnabled?: boolean;
    sessionIdsByKey?: Record<string, string>;
    nowMs?: number;
  }) {
    const root = abortFixture.stateDir;
    const storePath = path.join(root, "sessions.json");
    const cfg = {
      session: { store: storePath },
      ...(typeof params?.commandsTextEnabled === "boolean"
        ? { commands: { text: params.commandsTextEnabled } }
        : {}),
    } as OpenClawConfig;
    if (params?.sessionIdsByKey) {
      for (const sessionKey of Object.keys(params.sessionIdsByKey)) {
        trackedAbortMemoryKeys.add(sessionKey);
      }
      await writeAbortSessionStore(storePath, params.sessionIdsByKey, params.nowMs);
    }
    return { root, storePath, cfg };
  }

  async function runStopCommand(params: {
    cfg: OpenClawConfig;
    sessionKey?: string;
    parentSessionKey?: string;
    from: string;
    to: string;
    senderId?: string;
    commandSource?: "native" | "text";
    targetSessionKey?: string;
    messageSid?: string;
    timestamp?: number;
    body?: string;
  }) {
    for (const key of [
      params.sessionKey,
      params.parentSessionKey,
      params.targetSessionKey,
      params.from,
      params.to,
    ]) {
      if (key) {
        trackedAbortMemoryKeys.add(key);
      }
    }
    return tryFastAbortFromMessage({
      ctx: buildTestCtx({
        CommandBody: params.body ?? "/stop",
        RawBody: params.body ?? "/stop",
        CommandAuthorized: true,
        Provider: "telegram",
        Surface: "telegram",
        From: params.from,
        To: params.to,
        ...(params.sessionKey ? { SessionKey: params.sessionKey } : {}),
        ...(params.parentSessionKey ? { ParentSessionKey: params.parentSessionKey } : {}),
        ...(params.senderId ? { SenderId: params.senderId } : {}),
        ...(params.commandSource ? { CommandSource: params.commandSource } : {}),
        ...(params.targetSessionKey ? { CommandTargetSessionKey: params.targetSessionKey } : {}),
        ...(params.messageSid ? { MessageSid: params.messageSid } : {}),
        ...(typeof params.timestamp === "number" ? { Timestamp: params.timestamp } : {}),
      }),
      cfg: params.cfg,
    });
  }

  function enqueueQueuedFollowupRun(params: {
    root: string;
    cfg: OpenClawConfig;
    sessionId: string;
    sessionKey: string;
  }) {
    trackedAbortMemoryKeys.add(params.sessionKey);
    const followupRun: FollowupRun = {
      prompt: "queued",
      enqueuedAt: Date.now(),
      run: {
        agentId: resolveSessionAgentId({ config: params.cfg, sessionKey: params.sessionKey }),
        agentDir: path.join(params.root, "agent"),
        sessionId: params.sessionId,
        sessionKey: params.sessionKey,
        messageProvider: "telegram",
        agentAccountId: "acct",
        sessionFile: path.join(params.root, "session.jsonl"),
        workspaceDir: path.join(params.root, "workspace"),
        config: params.cfg,
        provider: "anthropic",
        model: "claude-opus-4-6",
        timeoutMs: 1000,
        blockReplyBreak: "text_end",
      },
    };
    enqueueFollowupRun(
      params.sessionKey,
      followupRun,
      { mode: "collect", debounceMs: 0, cap: 20, dropPolicy: "summarize" },
      "none",
    );
    return followupRun;
  }

  function createActiveAbortOperation(sessionKey: string, sessionId: string) {
    const operation = createReplyOperation({ sessionKey, sessionId, resetTriggered: false });
    const cancel = vi.fn(() => queueMicrotask(() => operation.complete()));
    operation.attachBackend({ kind: "embedded", cancel, isStreaming: () => true });
    return { operation, cancel };
  }

  function bindAcpSessionForTest(targetSessionKey: string) {
    vi.spyOn(getSessionBindingService(), "resolveByConversationAsync").mockImplementation(
      async (conversation) => ({
        bindingId: "test-acp-binding",
        targetKind: "session",
        targetSessionKey,
        conversation,
        status: "active",
        boundAt: 0,
      }),
    );
  }

  afterEach(() => {
    for (const key of trackedAbortMemoryKeys) {
      setAbortMemory(key, false);
      clearFollowupQueue(key);
    }
    trackedAbortMemoryKeys.clear();
    vi.restoreAllMocks();
    vi.mocked(markSessionAbortTarget).mockReset();
    vi.mocked(resolveSessionAbortTarget).mockReset();
    resetSessionControllerStateForTest();
    acpManagerMocks.cancelSession.mockReset().mockResolvedValue(undefined);
    runtimeAbortMocks.abortEmbeddedAgentRun.mockReset().mockReturnValue(true);
  });

  registerAbortDetectionCases(setTrackedAbortMemory);

  it("fast-aborts even when text commands are disabled", async () => {
    const { cfg } = await createAbortConfig({ commandsTextEnabled: false });

    const result = await runStopCommand({
      cfg,
      sessionKey: "telegram:123",
      from: "telegram:123",
      to: "telegram:123",
    });

    expect(result.handled).toBe(true);
  });

  it("resolves owner authorization after loading cancellation runtime", async () => {
    const sessionKey = "telegram:123";
    const sessionId = "session-123";
    const { root, cfg } = await createAbortConfig({
      sessionIdsByKey: { [sessionKey]: sessionId },
    });
    cfg.commands = { ownerAllowFrom: ["telegram:123"] };
    enqueueQueuedFollowupRun({ root, cfg, sessionId, sessionKey });
    const pending = runStopCommand({
      cfg,
      sessionKey,
      from: "telegram:123",
      to: "telegram:123",
      senderId: "123",
    });
    cfg.commands.ownerAllowFrom = ["telegram:other-owner"];

    await expect(pending).resolves.toEqual({ handled: false, aborted: false });
    expect(getFollowupQueueDepth(sessionKey)).toBe(1);
  });

  it("fast-aborts authorized text slash stop commands before they queue", async () => {
    const sessionKey = "telegram:123";
    const sessionId = "session-123";
    const activeSessionId = "session-active";
    const { root, cfg } = await createAbortConfig({
      sessionIdsByKey: { [sessionKey]: sessionId },
    });
    cfg.commands = {
      ...cfg.commands,
      ownerAllowFrom: ["telegram:123"],
    };
    const active = createActiveAbortOperation(sessionKey, activeSessionId);
    enqueueQueuedFollowupRun({ root, cfg, sessionId, sessionKey });

    expect(getFollowupQueueDepth(sessionKey)).toBe(1);

    const result = await runStopCommand({
      cfg,
      sessionKey,
      from: "telegram:123",
      to: "telegram:123",
      senderId: "123",
      commandSource: "text",
    });

    expect(result.handled).toBe(true);
    expect(active.cancel).toHaveBeenCalledOnce();

    expect(getFollowupQueueDepth(sessionKey)).toBe(0);
  });

  it("bounds Stop when an accepted backend cancellation never settles", async () => {
    vi.useFakeTimers();
    const sessionKey = "telegram:accepted-cancellation-never-settles";
    const sessionId = "accepted-cancellation-never-settles";
    const { cfg } = await createAbortConfig({ sessionIdsByKey: { [sessionKey]: sessionId } });
    const operation = createReplyOperation({ sessionKey, sessionId, resetTriggered: false });
    const cancel = vi.fn();
    operation.attachBackend({ kind: "embedded", cancel, isStreaming: () => true });
    const hookCompleted = createDeferred();
    const hook = vi.fn(() => hookCompleted.resolve());
    registerInternalHook("command:stop", hook);
    let observed:
      | { status: "fulfilled"; value: Awaited<ReturnType<typeof runStopCommand>> }
      | { status: "rejected"; reason: unknown }
      | undefined;
    const stopping = runStopCommand({
      cfg,
      sessionKey,
      from: "telegram:123",
      to: "telegram:123",
    });
    void stopping.then(
      (value) => {
        observed = { status: "fulfilled", value };
      },
      (reason: unknown) => {
        observed = { status: "rejected", reason };
      },
    );
    try {
      await hookCompleted.promise;
      expect(cancel).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(SESSION_CONTROLLER_DRAIN_TIMEOUT_MS + 1);
      await Promise.resolve();
      expect(observed).toMatchObject({
        status: "rejected",
        reason: expect.objectContaining({
          message: expect.stringContaining("cleanup is still pending"),
        }),
      });
    } finally {
      operation.complete();
      unregisterInternalHook("command:stop", hook);
      await stopping.catch(() => undefined);
      vi.useRealTimers();
    }
  });

  it("gives a bare stop word the channel-user queue, child, and hook policy", async () => {
    const sessionKey = "telegram:bare-stop";
    const sessionId = "session-bare-stop";
    const childKey = "agent:main:subagent:bare-stop-child";
    const childSessionId = "session-bare-stop-child";
    const { root, cfg } = await createAbortConfig({
      sessionIdsByKey: { [sessionKey]: sessionId, [childKey]: childSessionId },
    });
    cfg.commands = { ...cfg.commands, ownerAllowFrom: ["telegram:123"] };
    enqueueQueuedFollowupRun({ root, cfg, sessionId, sessionKey });
    await addSubagentFixture({
      runId: "bare-stop-child-run",
      childSessionKey: childKey,
      requesterSessionKey: sessionKey,
      requesterDisplayKey: sessionKey,
      task: "bare stop child",
      cleanup: "keep",
      createdAt: Date.now(),
    });
    const hook = vi.fn();
    registerInternalHook("command:stop", hook);

    try {
      const result = await runStopCommand({
        cfg,
        sessionKey,
        from: "telegram:123",
        to: "telegram:123",
        senderId: "123",
        commandSource: "text",
        body: "stop",
      });

      expect(result).toMatchObject({ handled: true, stoppedSubagents: 1 });
      expect(getFollowupQueueDepth(sessionKey)).toBe(0);
      expect(getSubagentRunByChildSessionKey(childKey)).toMatchObject({
        endedReason: "subagent-killed",
        killReconciliation: { suppressTaskDelivery: true },
      });
      expect(hook).toHaveBeenCalledOnce();
    } finally {
      unregisterInternalHook("command:stop", hook);
    }
  });

  it("fast-abort resolves canonical stored session identity before metadata persistence", async () => {
    const storeKey = "agent:main:telegram:group:-1001234567890:topic:99";
    const lookupKey = "Agent:Main:Telegram:Group:-1001234567890:Topic:99";
    const sessionId = "agent-topic-99";
    const { root, cfg } = await createAbortConfig({
      sessionIdsByKey: { [storeKey]: sessionId },
    });
    enqueueQueuedFollowupRun({ root, cfg, sessionId, sessionKey: storeKey });

    const result = await runStopCommand({
      cfg,
      sessionKey: lookupKey,
      from: "telegram:123",
      to: "telegram:123",
    });

    expect(result.handled).toBe(true);
    expect(getFollowupQueueDepth(storeKey)).toBe(0);
  });

  it("fast-abort still stops active runs when abort metadata persistence fails", async () => {
    const sessionKey = "telegram:persistence-failure";
    const sessionId = "session-persistence-failure";
    const activeSessionId = "active-persistence-failure";
    const { root, cfg } = await createAbortConfig({
      sessionIdsByKey: { [sessionKey]: sessionId },
    });
    const active = createActiveAbortOperation(sessionKey, activeSessionId);

    vi.mocked(markSessionAbortTarget).mockRejectedValueOnce(
      new Error("simulated persistence failure"),
    );
    enqueueQueuedFollowupRun({ root, cfg, sessionId: activeSessionId, sessionKey });

    const result = await runStopCommand({
      cfg,
      sessionKey,
      from: "telegram:123",
      to: "telegram:123",
    });

    expect(result.handled).toBe(true);
    expect(active.cancel).toHaveBeenCalledOnce();

    expect(getFollowupQueueDepth(sessionKey)).toBe(0);
    expect(getAbortMemory(sessionKey)).toBeUndefined();
  });

  it("fast-abort uses resolved target identity when abort metadata save fails", async () => {
    const requestedKey = "Agent:Main:Telegram:Group:-1001234567890:Topic:99";
    const canonicalKey = "agent:main:telegram:group:-1001234567890:topic:99";
    const sessionId = "resolved-persistence-failure";
    const { root, cfg } = await createAbortConfig();
    vi.mocked(markSessionAbortTarget).mockResolvedValueOnce({
      entry: {
        sessionId,
        updatedAt: 10,
      },
      persisted: false,
      persistenceError: "simulated persistence failure",
      sessionId,
      sessionKey: canonicalKey,
    });
    vi.mocked(resolveSessionAbortTarget).mockReturnValueOnce({
      entry: {
        sessionId,
        updatedAt: 10,
      },
      sessionId,
      sessionKey: canonicalKey,
    });
    enqueueQueuedFollowupRun({ root, cfg, sessionId, sessionKey: canonicalKey });

    const result = await runStopCommand({
      cfg,
      sessionKey: requestedKey,
      from: "telegram:123",
      to: "telegram:123",
    });

    expect(result.handled).toBe(true);
    expect(getFollowupQueueDepth(canonicalKey)).toBe(0);
    expect(getAbortMemory(canonicalKey)).toBeUndefined();
  });

  it("fast-abort uses abort memory when no persisted target entry exists", async () => {
    const sessionKey = "telegram:missing-persistence-target";
    const { cfg } = await createAbortConfig();
    vi.mocked(markSessionAbortTarget).mockResolvedValueOnce(null);
    vi.mocked(resolveSessionAbortTarget).mockReturnValueOnce(null);

    const result = await runStopCommand({
      cfg,
      sessionKey,
      from: "telegram:123",
      to: "telegram:123",
    });

    expect(result.handled).toBe(true);
    expect(getAbortMemory(sessionKey)).toBe(true);
  });

  it("fast-abort does not wait for abort metadata persistence before stopping runs", async () => {
    const sessionKey = "telegram:slow-persistence";
    const childKey = "agent:main:subagent:slow-persistence-child";
    const sessionId = "session-slow-persistence";
    const childSessionId = "session-slow-persistence-child";
    const { root, cfg } = await createAbortConfig({
      sessionIdsByKey: {
        [childKey]: childSessionId,
        [sessionKey]: sessionId,
      },
    });
    let finishPersistence: (() => void) | undefined;
    const persistenceStarted = new Promise<void>((resolveStarted) => {
      vi.mocked(markSessionAbortTarget).mockImplementationOnce(
        () =>
          new Promise<SessionAbortTargetResult | null>((resolvePersistence) => {
            resolveStarted();
            finishPersistence = () => {
              resolvePersistence({
                entry: {
                  sessionId,
                  updatedAt: 10,
                },
                persisted: true,
                sessionId,
                sessionKey,
              });
            };
          }),
      );
      vi.mocked(resolveSessionAbortTarget).mockReturnValueOnce({
        entry: {
          sessionId,
          updatedAt: 10,
        },
        sessionId,
        sessionKey,
      });
    });
    enqueueQueuedFollowupRun({ root, cfg, sessionId, sessionKey });
    await addSubagentFixture({
      runId: "slow-child-run",
      childSessionKey: childKey,
      requesterSessionKey: sessionKey,
      requesterDisplayKey: sessionKey,
      task: "slow child",
      cleanup: "keep",
      createdAt: Date.now(),
    });

    const resultPromise = runStopCommand({
      cfg,
      sessionKey,
      from: "telegram:123",
      to: "telegram:123",
    });
    await persistenceStarted;

    expect(getSubagentRunByChildSessionKey(childKey)).toMatchObject({
      endedReason: "subagent-killed",
      killReconciliation: { suppressTaskDelivery: true },
    });
    expect(getFollowupQueueDepth(sessionKey)).toBe(0);

    finishPersistence?.();
    await expect(resultPromise).resolves.toMatchObject({
      aborted: false,
      handled: true,
    });
  });

  it("plain-language stop leaves an idle ACP backend to /acp cancel", async () => {
    const sessionKey = "agent:codex:acp:test-1";
    const sessionId = "session-123";
    const { cfg } = await createAbortConfig({
      sessionIdsByKey: { [sessionKey]: sessionId },
    });

    const result = await runStopCommand({
      cfg,
      sessionKey,
      from: "telegram:123",
      to: "telegram:123",
      targetSessionKey: sessionKey,
    });

    expect(result).toMatchObject({ handled: true, aborted: false });
    expect(acpManagerMocks.cancelSession).not.toHaveBeenCalled();
  });

  it.each([false, true])(
    "binding resolution never recaptures a replacement native owner (explicit ACP target: %s)",
    async (explicitTarget) => {
      const sessionKey = "agent:main:telegram:direct:binding-wait";
      const acpKey = "agent:main:acp:binding-wait";
      const sessionId = "native-session";
      const { root, cfg } = await createAbortConfig({
        sessionIdsByKey: { [sessionKey]: sessionId, [acpKey]: "acp-session" },
      });
      const original = createActiveAbortOperation(sessionKey, sessionId);
      const entered = createDeferred();
      const release = createDeferred();
      vi.spyOn(getSessionBindingService(), "resolveByConversationAsync").mockImplementationOnce(
        async (conversation) => {
          entered.resolve();
          await release.promise;
          return {
            bindingId: "binding-wait",
            targetKind: "session",
            targetSessionKey: acpKey,
            conversation,
            status: "active",
            boundAt: 0,
          };
        },
      );
      const pending = runStopCommand({
        cfg,
        sessionKey,
        from: "telegram:binding-wait",
        to: "telegram:binding-wait",
        ...(explicitTarget ? { targetSessionKey: acpKey } : {}),
      });
      let replacement: ReturnType<typeof createActiveAbortOperation> | undefined;
      try {
        await entered.promise;
        original.operation.complete();
        replacement = createActiveAbortOperation(sessionKey, sessionId);
        enqueueQueuedFollowupRun({ root, cfg, sessionId, sessionKey });
        release.resolve();
        await pending;
        expect(replacement.cancel).not.toHaveBeenCalled();
        expect(replacement.operation.result).toBeNull();
        expect(getFollowupQueueDepth(sessionKey)).toBe(1);
      } finally {
        release.resolve();
        original.operation.complete();
        replacement?.operation.complete();
        await pending;
      }
    },
  );

  it.each([undefined, "agent:main:main"])(
    "propagates a zero-child callback failure for requester %s",
    async (requesterSessionKey) => {
      const beforeKill = vi.fn(() => {
        throw new Error("parent cancellation failed");
      });
      await expect(
        stopSubagentsForRequester({ cfg: {}, requesterSessionKey, beforeKill }),
      ).rejects.toThrow("parent cancellation failed");
      expect(beforeKill).toHaveBeenCalledOnce();
      expect(runtimeAbortMocks.abortEmbeddedAgentRun).not.toHaveBeenCalled();
    },
  );

  it("fast-abort of an ACP target also aborts the bound source dispatch lane", async () => {
    const sourceSessionKey = "agent:main:discord:channel:C1";
    const acpSessionKey = "agent:codex:acp:bound-session";
    const { root, cfg } = await createAbortConfig({
      sessionIdsByKey: {
        [sourceSessionKey]: "source-store-session",
        [acpSessionKey]: "acp-store-session",
      },
    });
    const { operation: sourceOperation } = createActiveAbortOperation(
      sourceSessionKey,
      "source-active-session",
    );
    enqueueQueuedFollowupRun({
      root,
      cfg,
      sessionId: "source-active-session",
      sessionKey: sourceSessionKey,
    });
    enqueueQueuedFollowupRun({
      root,
      cfg,
      sessionId: "acp-store-session",
      sessionKey: acpSessionKey,
    });
    bindAcpSessionForTest(acpSessionKey);

    const result = await runStopCommand({
      cfg,
      sessionKey: sourceSessionKey,
      from: "discord:C1",
      to: "discord:C1",
      targetSessionKey: acpSessionKey,
      commandSource: "native",
    });

    expect(result.handled).toBe(true);
    expect(sourceOperation.result).toEqual({ kind: "aborted", code: "aborted_by_user" });
    expect(isSessionRunActiveForKey(sourceSessionKey)).toBe(false);
    expect(getFollowupQueueDepth(sourceSessionKey)).toBe(0);
    expect(getFollowupQueueDepth(acpSessionKey)).toBe(0);
    expect(acpManagerMocks.cancelSession).not.toHaveBeenCalled();
  });

  it("does not report /stop success after the active backend freezes its outcome", async () => {
    const sessionKey = "agent:main:telegram:direct:finalizing";
    const sessionId = "session-finalizing";
    const { root, cfg } = await createAbortConfig({
      sessionIdsByKey: { [sessionKey]: sessionId },
    });
    const cancel = vi.fn();
    const operation = createReplyOperation({
      sessionKey,
      sessionId,
      resetTriggered: false,
    });
    operation.attachBackend({
      kind: "embedded",
      cancel,
      isStreaming: () => false,
      isAbortable: () => false,
    });
    operation.setPhase("running");
    enqueueQueuedFollowupRun({ root, cfg, sessionId, sessionKey });
    const hook = vi.fn();
    registerInternalHook("command:stop", hook);
    runtimeAbortMocks.abortEmbeddedAgentRun.mockReturnValue(false);
    vi.mocked(markSessionAbortTarget).mockClear();

    const result = await (async () => {
      try {
        return await runStopCommand({
          cfg,
          sessionKey,
          from: "telegram:finalizing",
          to: "telegram:finalizing",
        });
      } finally {
        unregisterInternalHook("command:stop", hook);
      }
    })();

    expect(result).toMatchObject({
      handled: true,
      aborted: false,
      rejectionReason: "finalizing",
    });
    expect(operation.result).toBeNull();
    expect(isSessionRunActiveForKey(sessionKey)).toBe(true);
    expect(cancel).not.toHaveBeenCalled();
    expect(getFollowupQueueDepth(sessionKey)).toBe(0);
    expect(hook).toHaveBeenCalledOnce();
    expect(markSessionAbortTarget).not.toHaveBeenCalled();
    expect(getAbortMemory(sessionKey)).toBeUndefined();
    expect(formatAbortReplyText(undefined, result.rejectionReason)).toBe(
      "Agent reply is already finalizing and can no longer be aborted.",
    );
    expect(formatAbortReplyText(0, undefined, 1)).toBe(
      "⚙️ Agent was aborted. Cancellation was incomplete for 1 sub-agent. Retry /stop.",
    );
    operation.complete();
  });

  it("fast-abort of a bound ACP target clears captured source input without inventing an active run", async () => {
    const sourceSessionKey = "agent:main:discord:channel:C2";
    const acpSessionKey = "agent:codex:acp:bound-session-stored-source";
    const { root, cfg } = await createAbortConfig({
      sessionIdsByKey: {
        [sourceSessionKey]: "source-store-session",
        [acpSessionKey]: "acp-store-session",
      },
    });
    enqueueQueuedFollowupRun({
      root,
      cfg,
      sessionId: "source-store-session",
      sessionKey: sourceSessionKey,
    });
    enqueueQueuedFollowupRun({
      root,
      cfg,
      sessionId: "acp-store-session",
      sessionKey: acpSessionKey,
    });
    bindAcpSessionForTest(acpSessionKey);

    const result = await runStopCommand({
      cfg,
      sessionKey: sourceSessionKey,
      from: "discord:C2",
      to: "discord:C2",
      targetSessionKey: acpSessionKey,
      commandSource: "native",
    });

    expect(result.handled).toBe(true);
    expect(getFollowupQueueDepth(sourceSessionKey)).toBe(0);
    expect(getFollowupQueueDepth(acpSessionKey)).toBe(0);
  });

  it("does not abort the caller source lane for an unbound explicit ACP target", async () => {
    const sourceSessionKey = "agent:main:discord:channel:C3";
    const acpSessionKey = "agent:codex:acp:unbound-explicit-target";
    const { cfg } = await createAbortConfig({
      sessionIdsByKey: {
        [sourceSessionKey]: "source-store-session",
        [acpSessionKey]: "acp-store-session",
      },
    });
    const { operation: sourceOperation } = createActiveAbortOperation(
      sourceSessionKey,
      "source-active-session",
    );

    const result = await runStopCommand({
      cfg,
      sessionKey: sourceSessionKey,
      from: "discord:C3",
      to: "discord:C3",
      targetSessionKey: acpSessionKey,
      commandSource: "native",
    });

    expect(result.handled).toBe(true);
    expect(sourceOperation.result).toBeNull();
    expect(isSessionRunActiveForKey(sourceSessionKey)).toBe(true);
    expect(acpManagerMocks.cancelSession).not.toHaveBeenCalled();
    sourceOperation.complete();
  });

  it("uses ParentSessionKey as the source lane for a bound explicit ACP target", async () => {
    const sourceSessionKey = "agent:main:discord:channel:C4";
    const acpSessionKey = "agent:codex:acp:bound-parent-source";
    const { cfg } = await createAbortConfig({
      sessionIdsByKey: {
        [sourceSessionKey]: "source-store-session",
        [acpSessionKey]: "acp-store-session",
      },
    });
    const { operation: sourceOperation } = createActiveAbortOperation(
      sourceSessionKey,
      "source-active-session",
    );
    bindAcpSessionForTest(acpSessionKey);

    const result = await runStopCommand({
      cfg,
      parentSessionKey: sourceSessionKey,
      from: "discord:C4",
      to: "discord:C4",
      targetSessionKey: acpSessionKey,
      commandSource: "native",
    });

    expect(result.handled).toBe(true);
    expect(sourceOperation.result).toEqual({ kind: "aborted", code: "aborted_by_user" });
    expect(isSessionRunActiveForKey(sourceSessionKey)).toBe(false);
  });

  it("fast-abort from an ACP-bound source conversation aborts source and bound ACP lanes", async () => {
    const sourceSessionKey = "agent:main:telegram:direct:source-1";
    const acpSessionKey = "agent:codex:acp:bound-source-stop";
    const { root, storePath, cfg } = await createAbortConfig({
      sessionIdsByKey: {
        [sourceSessionKey]: "source-store-session",
        [acpSessionKey]: "acp-store-session",
      },
    });
    const { operation: sourceOperation } = createActiveAbortOperation(
      sourceSessionKey,
      "source-active-session",
    );
    const { operation: acpOperation } = createActiveAbortOperation(
      acpSessionKey,
      "acp-active-session",
    );
    enqueueQueuedFollowupRun({
      root,
      cfg,
      sessionId: "source-active-session",
      sessionKey: sourceSessionKey,
    });
    enqueueQueuedFollowupRun({
      root,
      cfg,
      sessionId: "acp-active-session",
      sessionKey: acpSessionKey,
    });
    bindAcpSessionForTest(acpSessionKey);

    const result = await runStopCommand({
      cfg,
      sessionKey: sourceSessionKey,
      from: "telegram:source-1",
      to: "telegram:source-1",
      messageSid: "77",
      timestamp: 1234567890000,
    });

    expect(result.handled).toBe(true);
    expect(sourceOperation.result).toEqual({ kind: "aborted", code: "aborted_by_user" });
    expect(acpOperation.result).toEqual({ kind: "aborted", code: "aborted_by_user" });
    expect(isSessionRunActiveForKey(sourceSessionKey)).toBe(false);
    expect(isSessionRunActiveForKey(acpSessionKey)).toBe(false);
    expect(getFollowupQueueDepth(sourceSessionKey)).toBe(0);
    expect(getFollowupQueueDepth(acpSessionKey)).toBe(0);
    expect(acpManagerMocks.cancelSession).not.toHaveBeenCalled();
    const sourceEntry = readAbortSessionEntry(storePath, sourceSessionKey);
    const acpEntry = readAbortSessionEntry(storePath, acpSessionKey);
    expect(sourceEntry?.abortCutoffMessageSid).toBe("77");
    expect(sourceEntry?.abortCutoffTimestamp).toBe(1234567890000);
    expect(acpEntry?.abortCutoffMessageSid).toBeUndefined();
    expect(acpEntry?.abortCutoffTimestamp).toBeUndefined();
  });

  it("persists abort cutoff metadata when only ParentSessionKey identifies the command session", async () => {
    const sessionKey = "telegram:parent-only";
    const sessionId = "session-parent-only";
    const { storePath, cfg } = await createAbortConfig({
      sessionIdsByKey: { [sessionKey]: sessionId },
    });

    const result = await runStopCommand({
      cfg,
      parentSessionKey: sessionKey,
      from: "telegram:parent-only",
      to: "telegram:parent-only",
      messageSid: "56",
      timestamp: 1234567890001,
    });

    expect(result.handled).toBe(true);
    const entry = readAbortSessionEntry(storePath, sessionKey);
    expect(entry?.abortedLastRun).toBe(true);
    expect(entry?.abortCutoffMessageSid).toBe("56");
    expect(entry?.abortCutoffTimestamp).toBe(1234567890001);
  });

  it("does not persist cutoff metadata when native /stop targets a different session", async () => {
    const slashSessionKey = "telegram:slash:123";
    const targetSessionKey = "agent:main:telegram:group:123";
    const targetSessionId = "session-target";
    const { storePath, cfg } = await createAbortConfig({
      sessionIdsByKey: { [targetSessionKey]: targetSessionId },
    });

    const result = await runStopCommand({
      cfg,
      sessionKey: slashSessionKey,
      from: "telegram:123",
      to: "telegram:123",
      targetSessionKey,
      messageSid: "999",
      timestamp: 1234567890000,
    });

    expect(result.handled).toBe(true);
    const entry = readAbortSessionEntry(storePath, targetSessionKey);
    expect(entry?.abortedLastRun).toBe(true);
    expect(entry?.abortCutoffMessageSid).toBeUndefined();
    expect(entry?.abortCutoffTimestamp).toBeUndefined();
  });

  it("continues stopping siblings when one termination persistence write fails", async () => {
    const sessionKey = "telegram:persistence-failure-parent";
    const firstChildKey = "agent:main:subagent:persistence-failure-first";
    const secondChildKey = "agent:main:subagent:persistence-failure-second";
    const run = (runId: string, childSessionKey: string): SubagentRunFixture => ({
      runId,
      childSessionKey,
      requesterSessionKey: sessionKey,
      requesterDisplayKey: sessionKey,
      task: "stop despite persistence failure",
      cleanup: "keep",
      createdAt: Date.now(),
    });
    for (const fixture of [
      run("run-persistence-failure-first", firstChildKey),
      run("run-persistence-failure-second", secondChildKey),
    ]) {
      await addSubagentFixture(fixture);
    }
    let failedTombstone = false;
    const execute = stateWorker.runOpenClawStateWorkerOperation;
    vi.spyOn(stateWorker, "runOpenClawStateWorkerOperation").mockImplementation(
      (context, operation, options) =>
        execute(
          context,
          (scope) =>
            operation({
              execute: async (command, executeOptions) => {
                if (isSubagentRegistryWriteCommand(command) && !failedTombstone) {
                  const firstRow = command.input.values.find(
                    (row) => row.run_id === "run-persistence-failure-first",
                  );
                  const first = firstRow && rowToSubagentRunRecord(firstRow);
                  if (
                    first?.execution.status === "terminal" &&
                    first.endedReason === "subagent-killed"
                  ) {
                    failedTombstone = true;
                    throw new Error("sqlite busy");
                  }
                }
                return scope.execute(command, executeOptions);
              },
            }),
          options,
        ),
    );

    await expect(
      stopSubagentsForRequester({
        cfg: {} as OpenClawConfig,
        requesterSessionKey: sessionKey,
      }),
    ).resolves.toEqual({ stopped: 1, failed: 1 });
    expect(failedTombstone).toBe(true);
    expect(getSubagentRunByChildSessionKey(firstChildKey)?.killIntent).toBeDefined();
    expect(getSubagentRunByChildSessionKey(secondChildKey)?.endedReason).toBe("subagent-killed");
  });

  it("cascade stop kills depth-2 children when stopping depth-1 agent", async () => {
    const sessionKey = "telegram:parent";
    const depth1Key = "agent:main:subagent:child-1";
    const depth2Key = "agent:main:subagent:child-1:subagent:grandchild-1";
    const sessionId = "session-parent";
    const depth1SessionId = "session-child";
    const depth2SessionId = "session-grandchild";
    const { cfg } = await createAbortConfig({
      sessionIdsByKey: {
        [sessionKey]: sessionId,
        [depth1Key]: depth1SessionId,
        [depth2Key]: depth2SessionId,
      },
    });

    await addSubagentFixture({
      runId: "run-1",
      childSessionKey: depth1Key,
      requesterSessionKey: sessionKey,
      requesterDisplayKey: "telegram:parent",
      task: "orchestrator",
      cleanup: "keep",
      createdAt: Date.now(),
    });
    await addSubagentFixture({
      runId: "run-2",
      childSessionKey: depth2Key,
      requesterSessionKey: depth1Key,
      requesterDisplayKey: depth1Key,
      task: "leaf worker",
      cleanup: "keep",
      createdAt: Date.now(),
    });

    const result = await runStopCommand({
      cfg,
      sessionKey,
      from: "telegram:parent",
      to: "telegram:parent",
    });

    // Should stop both depth-1 and depth-2 agents (cascade)
    expect(result.stoppedSubagents).toBe(2);
  });

  it("stops a subagent that is paused after yielding", async () => {
    const sessionKey = "telegram:yield-parent";
    const childKey = "agent:main:subagent:yield-child";
    const now = Date.now();
    await addSubagentFixture({
      runId: "run-yield-child",
      childSessionKey: childKey,
      requesterSessionKey: sessionKey,
      requesterDisplayKey: sessionKey,
      task: "paused worker",
      cleanup: "keep",
      createdAt: now - 1_000,
      endedAt: now - 500,
      pauseReason: "sessions_yield",
    });

    const result = await stopSubagentsForRequester({
      cfg: {} as OpenClawConfig,
      requesterSessionKey: sessionKey,
    });

    expect(result).toEqual({ stopped: 1, failed: 0 });
    expect(getSubagentRunByChildSessionKey(childKey)).toMatchObject({
      endedReason: "subagent-killed",
      killReconciliation: { suppressTaskDelivery: true },
    });
  });

  it("cascade stop still traverses an ended current parent when a stale older active row exists", async () => {
    const sessionKey = "telegram:parent";
    const depth1Key = "agent:main:subagent:child-ended-stale";
    const depth2Key = "agent:main:subagent:child-ended-stale:subagent:grandchild-active";
    const now = Date.now();
    const { cfg } = await createAbortConfig({
      nowMs: now,
      sessionIdsByKey: {
        [sessionKey]: "session-parent",
        [depth1Key]: "session-child-ended-stale",
        [depth2Key]: "session-grandchild-active",
      },
    });

    for (const fixture of [
      {
        runId: "run-stale-parent",
        childSessionKey: depth1Key,
        requesterSessionKey: sessionKey,
        requesterDisplayKey: "telegram:parent",
        task: "stale orchestrator",
        cleanup: "keep",
        createdAt: now - 2_000,
        startedAt: now - 1_900,
      },
      {
        runId: "run-current-parent",
        childSessionKey: depth1Key,
        requesterSessionKey: sessionKey,
        requesterDisplayKey: "telegram:parent",
        task: "current orchestrator",
        cleanup: "keep",
        createdAt: now - 1_000,
        startedAt: now - 900,
        endedAt: now - 500,
        outcome: { status: "ok" },
      },
    ] satisfies SubagentRunFixture[]) {
      await addSubagentFixture(fixture);
    }
    await addSubagentFixture({
      runId: "run-active-child",
      childSessionKey: depth2Key,
      requesterSessionKey: depth1Key,
      requesterDisplayKey: depth1Key,
      task: "leaf worker",
      cleanup: "keep",
      createdAt: now - 400,
    });

    const result = await runStopCommand({
      cfg,
      sessionKey,
      from: "telegram:parent",
      to: "telegram:parent",
    });

    expect(result.stoppedSubagents).toBe(1);
    expect(getSubagentRunByChildSessionKey(depth1Key)?.endedReason).not.toBe("subagent-killed");
    expect(getSubagentRunByChildSessionKey(depth2Key)?.endedReason).toBe("subagent-killed");
  });

  it("stopSubagentsForRequester does not traverse a child that moved to a newer parent", async () => {
    const oldParentKey = "agent:main:subagent:old-parent";
    const newParentKey = "agent:main:subagent:new-parent";
    const childKey = "agent:main:subagent:shared-child";
    const leafKey = `${childKey}:subagent:leaf`;
    const now = Date.now();

    await addSubagentFixture({
      runId: "run-shared-child-stale-parent",
      childSessionKey: childKey,
      requesterSessionKey: oldParentKey,
      controllerSessionKey: oldParentKey,
      requesterDisplayKey: oldParentKey,
      task: "shared child stale parent",
      cleanup: "keep",
      createdAt: now - 2_000,
      endedAt: now - 1_000,
      outcome: { status: "ok" },
    });
    await addSubagentFixture({
      runId: "run-leaf-active",
      childSessionKey: leafKey,
      requesterSessionKey: childKey,
      controllerSessionKey: childKey,
      requesterDisplayKey: childKey,
      task: "leaf worker",
      cleanup: "keep",
      createdAt: now - 500,
    });
    await addSubagentFixture({
      runId: "run-shared-child-current-parent",
      childSessionKey: childKey,
      requesterSessionKey: newParentKey,
      controllerSessionKey: newParentKey,
      requesterDisplayKey: newParentKey,
      task: "shared child current parent",
      cleanup: "keep",
      createdAt: now - 250,
    });

    const result = await stopSubagentsForRequester({
      cfg: {} as OpenClawConfig,
      requesterSessionKey: oldParentKey,
    });

    expect(result).toEqual({ stopped: 0, failed: 0 });
    expect(getSubagentRunByChildSessionKey(childKey)?.execution.endedAt).toBeUndefined();
    expect(getSubagentRunByChildSessionKey(leafKey)?.execution.endedAt).toBeUndefined();
  });
});
/* oxlint-disable max-lines -- TODO: split this grandfathered oversized file. */
