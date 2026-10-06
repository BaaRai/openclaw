// Preserve module setup before modules that consume it.
// oxfmt-ignore
import { mockSessionReplacementForStore } from "./subagent-control.leaf-mocks.test-support.js";
// oxfmt-ignore
import { runSubagentStateWorkerOperation, useSubagentControlFixture } from "./subagent-control.test-support.js";
import path from "node:path";
import { afterAll, beforeEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../../test/helpers/promise.js";
import { useAutoCleanupTempDirTracker } from "../../../../test/helpers/temp-dir.js";
import { stopSubagentsForRequester } from "../../../auto-reply/reply/abort-operation.js";
import { cleanupBrowserSessionsForLifecycleEnd } from "../../../browser-lifecycle-cleanup.js";
import {
  loadSessionEntry,
  replaceSessionEntry,
  replaceSessionEntrySync,
} from "../../../config/sessions/session-accessor.js";
import { applySessionEntryExactReplacements } from "../../../config/sessions/session-accessor.sqlite-replacement-projection.js";
import type { SessionEntry } from "../../../config/sessions/types.js";
import type { OpenClawConfig } from "../../../config/types.openclaw.js";
import { ensureContextEnginesInitialized } from "../../../context-engine/init.js";
import { resolveContextEngine } from "../../../context-engine/registry.js";
import { rotateAgentEventLifecycleGeneration } from "../../../infra/agent-events.js";
import { createReplyOperation } from "../../../sessions/session-controller.js";
import {
  beginSessionEffect,
  captureSessionTarget,
  consumeSessionEffectHandoff,
  getSessionMutationCount,
  SESSION_CONTROLLER_DRAIN_TIMEOUT_MS,
} from "../../../sessions/session-controller.lifecycle.js";
import { closeOpenClawAgentDatabasesAsync } from "../../../state/openclaw-agent-db.js";
import { loadAgentRuntimePluginRegistryHandle } from "../../runtime-plugins.js";
import { createSubagentRunRecord } from "../../subagent-test-fixtures.test-helpers.js";
import { enqueueSwarmRun, holdQueuedSwarmRun, releaseSwarmRun } from "../swarm/swarm-scheduler.js";
import { testing as swarmSchedulerTesting } from "../swarm/swarm-scheduler.test-support.js";
import * as killSession from "./subagent-control-session.js";
import {
  buildControlledSubagentRunsReadContext,
  killAllControlledSubagentRuns,
  killSubagentRunAdmin,
} from "./subagent-control.js";
import { registerLateDescendantControlTests } from "./subagent-control.late-registration.test-support.js";
import { registerQueueStopControlTests } from "./subagent-control.queue-stop.test-support.js";
import {
  SUBAGENT_ENDED_REASON_COMPLETE,
  SUBAGENT_ENDED_REASON_KILLED,
} from "./subagent-lifecycle-events.js";
import { subagentRuns } from "./subagent-registry-memory.js";
import { mutateSubagentRuns } from "./subagent-registry-persistence.js";
import {
  markSubagentRunTerminated,
  replaceSubagentRunAfterSteerCore,
  startQueuedSubagentRun,
} from "./subagent-registry.js";
import {
  addSubagentRunForTests,
  getSubagentRunByChildSessionKey,
} from "./subagent-registry.test-helpers.js";

const fixture = useSubagentControlFixture();

type ControlRuntime = typeof import("./subagent-control.runtime.js");

const controlRuntimeMocks = vi.hoisted(() => ({
  clearSessionQueues: vi.fn<ControlRuntime["clearSessionQueues"]>(() => ({
    followupCleared: 0,
    keys: [],
  })),
}));

vi.mock("./subagent-control.runtime.js", () => controlRuntimeMocks);

function setSubagentControlDepsForTest(overrides: Partial<ControlRuntime> = {}) {
  controlRuntimeMocks.clearSessionQueues.mockReset();
  // Default to the canonical store; individual race tests replace only their fault boundary.
  vi.mocked(applySessionEntryExactReplacements).mockReset();
  if (overrides.clearSessionQueues) {
    controlRuntimeMocks.clearSessionQueues.mockImplementation(overrides.clearSessionQueues);
  }
}

const tempDirs = useAutoCleanupTempDirTracker((cleanup) =>
  afterAll(async () => {
    await closeOpenClawAgentDatabasesAsync(tempRoot);
    cleanup();
  }),
);
const tempRoot = tempDirs.make("openclaw-subagent-control-");
let tempStoreIndex = 0;

function nextSessionStorePath(label: string) {
  tempStoreIndex += 1;
  return path.join(tempRoot, `${tempStoreIndex}-${label}.json`);
}

function cfgWithSessionStore(storePath = nextSessionStorePath("sessions")): OpenClawConfig {
  return {
    session: { store: storePath },
  } as OpenClawConfig;
}

function controllerFor(controllerSessionKey = "agent:main:main") {
  return {
    controllerSessionKey,
    callerSessionKey: controllerSessionKey,
    callerIsSubagent: false,
    controlScope: "children" as const,
  };
}

async function writeSessionStoreFixture(label: string, store: Record<string, unknown>) {
  const storePath = nextSessionStorePath(label);
  for (const [sessionKey, entry] of Object.entries(store)) {
    const record = entry && typeof entry === "object" ? (entry as Record<string, unknown>) : {};
    const sessionId =
      typeof record.sessionId === "string" && record.sessionId.trim()
        ? record.sessionId
        : `sess-${sessionKey.replaceAll(/[^a-z0-9]+/gi, "-").replace(/^-|-$/g, "")}`;
    await replaceSessionEntry({ storePath, sessionKey }, {
      ...record,
      sessionId,
    } as SessionEntry);
  }
  return storePath;
}

function resetRegistryLeafMocks() {
  vi.mocked(cleanupBrowserSessionsForLifecycleEnd).mockReset();
  vi.mocked(ensureContextEnginesInitialized).mockReset();
  vi.mocked(loadAgentRuntimePluginRegistryHandle).mockReset();
  fixture.worker.mockReset().mockImplementation(runSubagentStateWorkerOperation);
  vi.mocked(resolveContextEngine).mockReset();
}

beforeEach(() => {
  setSubagentControlDepsForTest();
  resetRegistryLeafMocks();
  vi.mocked(cleanupBrowserSessionsForLifecycleEnd).mockResolvedValue(undefined);
  vi.mocked(ensureContextEnginesInitialized).mockResolvedValue(undefined);
  vi.mocked(resolveContextEngine).mockImplementation(async () => ({
    info: { id: "test", name: "Test" },
    assemble: async ({ messages }) => ({ messages, estimatedTokens: 0 }),
    compact: async () => ({ ok: true, compacted: false }),
    ingest: async () => ({ ingested: false }),
  }));
});

describe("killSubagentRunAdmin", () => {
  it("kills a subagent by session key without requester ownership checks", async () => {
    const childSessionKey = "agent:main:subagent:worker";
    const storePath = await writeSessionStoreFixture("admin-kill", {
      [childSessionKey]: {
        sessionId: "sess-worker",
        updatedAt: Date.now(),
      },
    });

    await addSubagentRunForTests({
      runId: "run-worker",
      childSessionKey,
      controllerSessionKey: "agent:main:other-controller",
      requesterSessionKey: "agent:main:other-requester",
      requesterDisplayKey: "other-requester",
      task: "do the work",
      createdAt: Date.now() - 5_000,
      startedAt: Date.now() - 4_000,
    });

    const cfg = cfgWithSessionStore(storePath);

    const result = await killSubagentRunAdmin({
      cfg,
      sessionKey: childSessionKey,
    });

    expect(result.found).toBe(true);
    expect(result.killed).toBe(true);
    if (!result.found) {
      throw new Error("expected tracked subagent run");
    }
    expect(result.runId).toBe("run-worker");
    expect(result.sessionKey).toBe(childSessionKey);
    expect(loadSessionEntry({ storePath, sessionKey: childSessionKey })?.abortedLastRun).toBe(true);
    expect(getSubagentRunByChildSessionKey(childSessionKey)?.execution.endedAt).toBeTypeOf(
      "number",
    );
  });

  it("returns found=false when the session key is not tracked as a subagent run", async () => {
    const result = await killSubagentRunAdmin({
      cfg: cfgWithSessionStore(),
      sessionKey: "agent:main:subagent:missing",
    });

    expect(result).toEqual({ found: false, killed: false });
  });

  it("does not kill a replacement run when an exact run id is required", async () => {
    const childSessionKey = "agent:main:subagent:replacement";
    await addSubagentRunForTests({
      runId: "run-current",
      childSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "replacement work",
      createdAt: Date.now() - 1_000,
      startedAt: Date.now() - 900,
    });

    const result = await killSubagentRunAdmin({
      cfg: cfgWithSessionStore(),
      sessionKey: childSessionKey,
      expectedRunId: "run-stale",
    });

    expect(result).toEqual({ found: false, killed: false });
    expect(getSubagentRunByChildSessionKey(childSessionKey)?.execution.endedAt).toBeUndefined();
  });

  it("does not kill a same-id replacement generation", async () => {
    const childSessionKey = "agent:main:subagent:same-id-replacement";
    await addSubagentRunForTests({
      runId: "run-reused",
      childSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "replacement work",
      generation: 2,
      createdAt: Date.now() - 1_000,
      startedAt: Date.now() - 900,
    });

    const result = await killSubagentRunAdmin({
      cfg: cfgWithSessionStore(),
      sessionKey: childSessionKey,
      expectedRunId: "run-reused",
      expectedGeneration: 1,
      expectedOwnerKey: "agent:main:main",
    });
    const foreignOwner = await killSubagentRunAdmin({
      cfg: cfgWithSessionStore(),
      sessionKey: childSessionKey,
      expectedRunId: "run-reused",
      expectedGeneration: 2,
      expectedOwnerKey: "agent:main:other",
    });

    expect(result).toEqual({ found: false, killed: false });
    expect(foreignOwner).toEqual({ found: false, killed: false });
    expect(getSubagentRunByChildSessionKey(childSessionKey)?.execution.endedAt).toBeUndefined();
  });

  it("does not adopt a restart-recovery successor when an exact run id is required", async () => {
    const childSessionKey = "agent:main:subagent:fenced-recovery-successor";
    const sessionId = "sess-fenced-recovery-successor";
    const recoveryRunId = "run-fenced-recovery-successor";
    const receipt = {
      sessionId,
      sessionMarker: `${sessionId}:1`,
      idempotencyKey: recoveryRunId,
      phase: "accepted" as const,
    };
    let source = createSubagentRunRecord({
      runId: "run-fenced-recovery-source",
      childSessionKey,
      controllerSessionKey: "agent:main:controller",
      requesterSessionKey: "agent:main:requester",
      requesterDisplayKey: "requester",
      task: "source recovery task",
      completion: { required: false },
      delivery: { status: "pending" },
      generation: 1,
      createdAt: Date.now() - 2_000,
      execution: {
        status: "interrupted",
        startedAt: Date.now() - 1_000,
        restartRecovery: receipt,
      },
    });
    await addSubagentRunForTests(source);
    source = subagentRuns.get(source.runId)!;
    const storePath = await writeSessionStoreFixture("fenced-recovery-successor", {
      [childSessionKey]: { sessionId, updatedAt: Date.now(), abortedLastRun: true },
    });
    const interrupted = createDeferred();
    const admission = await beginSessionEffect({
      scope: storePath,
      identities: [childSessionKey, sessionId],
      assertAllowed: () => {},
      onInterrupt: () => interrupted.resolve(),
    });
    const handoffId = admission.createHandoff();
    setSubagentControlDepsForTest({
      clearSessionQueues: () => ({ followupCleared: 0, keys: [] }),
    });

    const pendingKill = killSubagentRunAdmin({
      cfg: cfgWithSessionStore(storePath),
      sessionKey: childSessionKey,
      expectedRunId: source.runId,
    });
    let adopted: ReturnType<typeof consumeSessionEffectHandoff>;
    try {
      await interrupted.promise;
      expect(getSessionMutationCount()).toBeGreaterThan(0);
      adopted = consumeSessionEffectHandoff({
        handoffId,
        scope: storePath,
        identities: [childSessionKey, sessionId],
        onInterrupt: () => undefined,
      });
      expect(
        await replaceSubagentRunAfterSteerCore({
          previousRunId: source.runId,
          nextRunId: recoveryRunId,
          expected: source,
        }),
      ).toBe(true);
      expect(adopted).toBeDefined();
      adopted?.release();

      await expect(pendingKill).resolves.toMatchObject({
        found: true,
        killed: false,
        runId: source.runId,
      });
      expect(getSubagentRunByChildSessionKey(childSessionKey)).toMatchObject({
        runId: recoveryRunId,
        execution: { status: "running" },
      });
      expect(getSubagentRunByChildSessionKey(childSessionKey)?.execution.endedAt).toBeUndefined();
    } finally {
      adopted?.release();
      admission.release();
      await pendingKill;
    }
  });

  it("does not adopt a same-id successor when an exact run id is required", async () => {
    const childSessionKey = "agent:main:subagent:fenced-same-id-successor";
    const runId = "run-fenced-same-id-successor";
    let source = createSubagentRunRecord({
      runId,
      childSessionKey,
      controllerSessionKey: "agent:main:controller",
      requesterSessionKey: "agent:main:requester",
      requesterDisplayKey: "requester",
      task: "same-id recovery source",
      generation: 1,
      createdAt: Date.now() - 2_000,
      execution: {
        status: "interrupted",
        startedAt: Date.now() - 1_000,
        restartRecovery: {
          sessionId: "sess-fenced-same-id-successor",
          sessionMarker: "sess-fenced-same-id-successor:1",
          idempotencyKey: runId,
          phase: "accepted",
        },
      },
    });
    await addSubagentRunForTests(source);
    source = subagentRuns.get(source.runId)!;
    setSubagentControlDepsForTest({
      clearSessionQueues: () => ({ followupCleared: 0, keys: [] }),
    });

    const replacementReady = createDeferred();
    let replacementPending = true;
    const pendingKill = killSubagentRunAdmin(
      {
        cfg: cfgWithSessionStore(),
        sessionKey: childSessionKey,
        expectedRunId: runId,
      },
      {
        assertCurrent: () => {},
        prepareRead: () => (replacementPending ? replacementReady.promise : undefined),
      },
    );
    await addSubagentRunForTests({
      ...source,
      task: "same-id recovery successor",
      generation: 2,
      createdAt: Date.now(),
      execution: { status: "running", startedAt: Date.now() },
    });

    replacementPending = false;
    replacementReady.resolve();
    await expect(pendingKill).resolves.toMatchObject({
      found: true,
      killed: false,
      runId,
    });
    expect(getSubagentRunByChildSessionKey(childSessionKey)).toMatchObject({
      runId,
      generation: 2,
      execution: { status: "running" },
    });
    expect(getSubagentRunByChildSessionKey(childSessionKey)?.execution.endedAt).toBeUndefined();
  });

  it("keeps a killed steer-restart run on its failed projection", async () => {
    const childSessionKey = "agent:main:subagent:steer-restart";
    const endedAt = Date.now() - 1_000;
    await addSubagentRunForTests({
      runId: "run-steer-restart",
      childSessionKey,
      controllerSessionKey: "agent:main:controller",
      requesterSessionKey: "agent:main:requester",
      requesterDisplayKey: "requester",
      task: "replace active run",
      createdAt: endedAt - 4_000,
      startedAt: endedAt - 3_000,
      endedAt,
      endedReason: SUBAGENT_ENDED_REASON_KILLED,
      suppressAnnounceReason: "steer-restart",
      outcome: { status: "error", error: "agent run aborted" },
      completion: { required: false, resultText: null, capturedAt: endedAt },
    });

    const result = await killSubagentRunAdmin({ cfg: {}, sessionKey: childSessionKey });

    expect(result).toMatchObject({
      found: true,
      killed: false,
      targetState: {
        state: "terminal",
        task: {
          status: "failed",
          endedAt,
          error: "agent run aborted",
        },
      },
    });
  });

  it("refreshes target completion after descendant cancellation settles", async () => {
    const childSessionKey = "agent:main:subagent:cascade-completion-race";
    const descendantSessionKey = "agent:main:subagent:cascade-completion-child";
    const storePath = await writeSessionStoreFixture("admin-kill-cascade-completion-race", {
      [childSessionKey]: {
        sessionId: "sess-cascade-completion-race",
        updatedAt: Date.now(),
      },
      [descendantSessionKey]: {
        sessionId: "sess-cascade-completion-child",
        updatedAt: Date.now(),
      },
    });
    let run = createSubagentRunRecord({
      runId: "run-cascade-completion-race",
      childSessionKey,
      controllerSessionKey: "agent:main:controller",
      requesterSessionKey: "agent:main:requester",
      requesterDisplayKey: "requester",
      task: "finish while descendant cancellation settles",
      createdAt: Date.now() - 5_000,
      startedAt: Date.now() - 4_000,
    });
    const abortedLastRunWrites: boolean[] = [];
    await addSubagentRunForTests(run);
    run = subagentRuns.get(run.runId)!;
    await addSubagentRunForTests({
      runId: "run-cascade-completion-child",
      childSessionKey: descendantSessionKey,
      controllerSessionKey: childSessionKey,
      requesterSessionKey: childSessionKey,
      requesterDisplayKey: "parent",
      task: "descendant",
      createdAt: Date.now() - 3_000,
      startedAt: Date.now() - 2_000,
    });
    let terminalPublication = Promise.resolve();
    setSubagentControlDepsForTest({});
    mockSessionReplacementForStore(storePath, async (params) => {
      const sessionKey = params.activeSessionKey!;
      const current = loadSessionEntry({ storePath: params.storePath, sessionKey, clone: false });
      const operation = await params.update(current ? [{ sessionKey, entry: current }] : []);
      params.assertCommitAllowed?.();
      const replacement = [...(operation.replacements ?? [])][0]?.entry;
      if (
        sessionKey === childSessionKey &&
        replacement &&
        replacement.abortedLastRun !== current?.abortedLastRun
      ) {
        abortedLastRunWrites.push(replacement.abortedLastRun === true);
      }
      if (sessionKey === descendantSessionKey) {
        const endedAt = Date.now();
        terminalPublication = mutateSubagentRuns([run.runId], (rows) => {
          const currentRun = rows.get(run.runId)!;
          return {
            value: undefined,
            postimages: new Map([
              [
                run.runId,
                {
                  ...currentRun,
                  endedReason: SUBAGENT_ENDED_REASON_COMPLETE,
                  completion: { required: false, resultText: "done", capturedAt: endedAt },
                  execution: {
                    ...currentRun.execution,
                    status: "terminal" as const,
                    endedAt,
                    outcome: { status: "ok" as const },
                  },
                },
              ],
            ]),
          };
        });
      }
      await terminalPublication;
      return operation.result;
    });

    const result = await killSubagentRunAdmin({
      cfg: cfgWithSessionStore(storePath),
      sessionKey: childSessionKey,
    });

    await terminalPublication;
    expect(result).toMatchObject({
      found: true,
      killed: true,
      targetState: {
        state: "terminal",
        task: {
          status: "succeeded",
        },
      },
    });
    expect(abortedLastRunWrites).toEqual([true, false]);
  });

  it("does not kill a newest finalizing run when only a stale older row is still active", async () => {
    const childSessionKey = "agent:main:subagent:worker-stale-admin";

    await addSubagentRunForTests({
      runId: "run-stale-admin",
      childSessionKey,
      controllerSessionKey: "agent:main:other-controller",
      requesterSessionKey: "agent:main:other-requester",
      requesterDisplayKey: "other-requester",
      task: "stale admin task",
      createdAt: Date.now() - 9_000,
      startedAt: Date.now() - 8_000,
    });
    await addSubagentRunForTests({
      runId: "run-current-admin",
      childSessionKey,
      controllerSessionKey: "agent:main:other-controller",
      requesterSessionKey: "agent:main:other-requester",
      requesterDisplayKey: "other-requester",
      task: "current admin task",
      createdAt: Date.now() - 5_000,
      startedAt: Date.now() - 4_000,
      endedAt: Date.now() - 1_000,
      outcome: { status: "ok" },
    });

    const result = await killSubagentRunAdmin({
      cfg: cfgWithSessionStore(),
      sessionKey: childSessionKey,
    });

    expect(result.found).toBe(true);
    expect(result.killed).toBe(false);
    if (!result.found) {
      throw new Error("expected finalizing subagent run");
    }
    if (!("targetState" in result)) {
      throw new Error("expected finalizing target state");
    }
    expect(result.targetState).toEqual({ state: "finalizing" });
    expect(result.runId).toBe("run-current-admin");
    expect(result.sessionKey).toBe(childSessionKey);
  });

  it("does not retarget an ordinary same-id successor in the admin path", async () => {
    const childSessionKey = "agent:main:subagent:admin-same-id-successor";
    let source = createSubagentRunRecord({
      runId: "run-admin-same-id",
      childSessionKey,
      controllerSessionKey: "agent:main:controller",
      requesterSessionKey: "agent:main:requester",
      requesterDisplayKey: "requester",
      task: "admin source",
      generation: 1,
      createdAt: Date.now() - 5_000,
      startedAt: Date.now() - 4_000,
    });
    await addSubagentRunForTests(source);
    source = subagentRuns.get(source.runId)!;
    setSubagentControlDepsForTest({
      clearSessionQueues: () => ({ followupCleared: 0, keys: [] }),
    });

    const replacementReady = createDeferred();
    let replacementPending = true;
    const pendingKill = killSubagentRunAdmin(
      {
        cfg: cfgWithSessionStore(),
        sessionKey: childSessionKey,
      },
      {
        assertCurrent: () => {},
        prepareRead: () => (replacementPending ? replacementReady.promise : undefined),
      },
    );
    await addSubagentRunForTests({
      ...source,
      task: "admin successor",
      generation: 2,
      createdAt: Date.now(),
      execution: { status: "running", startedAt: Date.now() },
    });

    replacementPending = false;
    replacementReady.resolve();
    await expect(pendingKill).resolves.toMatchObject({
      found: true,
      killed: false,
      runId: source.runId,
    });
    expect(getSubagentRunByChildSessionKey(childSessionKey)).toMatchObject({
      runId: source.runId,
      generation: 2,
      execution: { status: "running" },
    });
  });

  it("does not mutate the run when the durable kill intent cannot persist", async () => {
    const childSessionKey = "agent:main:subagent:worker-store-fail";
    const storePath = await writeSessionStoreFixture("admin-kill-store-fail", {
      [childSessionKey]: {
        sessionId: "sess-worker-store-fail",
        updatedAt: Date.now(),
      },
    });

    await addSubagentRunForTests({
      runId: "run-worker-store-fail",
      childSessionKey,
      controllerSessionKey: "agent:main:other-controller",
      requesterSessionKey: "agent:main:other-requester",
      requesterDisplayKey: "other-requester",
      task: "do the work",
      createdAt: Date.now() - 5_000,
      startedAt: Date.now() - 4_000,
    });

    resetRegistryLeafMocks();
    fixture.worker.mockImplementation((context, operation, options) =>
      runSubagentStateWorkerOperation(
        context,
        (scope) =>
          operation({
            ...scope,
            execute: async (command) => {
              if (command.type === "subagents.persistChanges") {
                throw new Error("session store unavailable");
              }
              return scope.execute(command);
            },
          }),
        options,
      ),
    );

    const result = await killSubagentRunAdmin({
      cfg: cfgWithSessionStore(storePath),
      sessionKey: childSessionKey,
    });

    expect(result).toMatchObject({
      found: true,
      killed: false,
      runId: "run-worker-store-fail",
      sessionKey: childSessionKey,
      error: expect.stringContaining("Failed to persist subagent kill intent"),
    });
    expect(getSubagentRunByChildSessionKey(childSessionKey)).toMatchObject({
      runId: "run-worker-store-fail",
      execution: { status: "running" },
    });
    expect(getSubagentRunByChildSessionKey(childSessionKey)?.execution.endedAt).toBeUndefined();
  });
});

describe("controlled subagent cancellation races", () => {
  it("does not mutate the live session when the caller passes a stale run entry", async () => {
    const childSessionKey = "agent:main:subagent:stale-kill-worker";
    const storePath = await writeSessionStoreFixture("stale-kill", {
      [childSessionKey]: {
        updatedAt: Date.now(),
      },
    });

    await addSubagentRunForTests({
      runId: "run-stale",
      childSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "stale task",
      createdAt: Date.now() - 9_000,
      startedAt: Date.now() - 8_000,
    });
    const stale = subagentRuns.get("run-stale")!;
    await addSubagentRunForTests({
      runId: "run-current",
      childSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "current task",
      createdAt: Date.now() - 4_000,
      startedAt: Date.now() - 3_000,
    });

    const result = await killAllControlledSubagentRuns({
      cfg: cfgWithSessionStore(storePath),
      controller: controllerFor(),
      runs: [stale],
    });

    expect(result).toEqual({
      status: "ok",
      killed: 0,
      labels: [],
    });
    const persisted = loadSessionEntry({ storePath, sessionKey: childSessionKey });
    expect(persisted?.abortedLastRun).toBeUndefined();
    expect(getSubagentRunByChildSessionKey(childSessionKey)?.runId).toBe("run-current");
  });

  it("does not let 24 in-flight kills cross into same-id successor generations", async () => {
    const count = 24;
    const controllerSessionKey = "agent:main:main";
    const oldRuns = Array.from({ length: count }, (_, index) =>
      createSubagentRunRecord({
        runId: `run-old-${index}`,
        childSessionKey: `agent:main:subagent:generation-race-${index}`,
        controllerSessionKey,
        requesterSessionKey: controllerSessionKey,
        task: `old task ${index}`,
        generation: 1,
        createdAt: Date.now() - 5_000,
        startedAt: Date.now() - 4_000,
      }),
    );
    const storePath = await writeSessionStoreFixture(
      "generation-race",
      Object.fromEntries(
        oldRuns.map((entry, index) => [
          entry.childSessionKey,
          { sessionId: `sess-generation-race-${index}`, updatedAt: Date.now() },
        ]),
      ),
    );
    for (const [index, entry] of oldRuns.entries()) {
      await addSubagentRunForTests(entry);
      oldRuns[index] = subagentRuns.get(entry.runId)!;
    }

    const clearQueues = vi.fn(() => ({ followupCleared: 0, keys: [] }));
    setSubagentControlDepsForTest({
      clearSessionQueues: clearQueues,
    });

    const replacementsReady = createDeferred();
    const killsEntered = oldRuns.map(() => createDeferred());
    const pendingKills = oldRuns.map((entry, index) =>
      killAllControlledSubagentRuns({
        cfg: cfgWithSessionStore(storePath),
        controller: controllerFor(controllerSessionKey),
        runs: [entry],
        beforeKill: async () => {
          killsEntered[index]!.resolve();
          await replacementsReady.promise;
          return true;
        },
      }),
    );

    await Promise.all(killsEntered.map((entered) => entered.promise));
    const successorKeys: string[] = [];
    const descendantKeys: string[] = [];
    for (const [index, entry] of oldRuns.entries()) {
      successorKeys.push(entry.childSessionKey);
      descendantKeys.push(`${entry.childSessionKey}:subagent:leaf`);
      await addSubagentRunForTests({
        ...entry,
        runId: entry.runId,
        task: `successor task ${index}`,
        generation: 2,
        createdAt: Date.now(),
        execution: { status: "running", startedAt: Date.now() },
      });
      await addSubagentRunForTests({
        ...entry,
        runId: `run-successor-leaf-${index}`,
        childSessionKey: descendantKeys[index]!,
        controllerSessionKey: entry.childSessionKey,
        requesterSessionKey: entry.childSessionKey,
        requesterDisplayKey: entry.childSessionKey,
        task: `successor leaf ${index}`,
        generation: 1,
        createdAt: Date.now(),
        execution: { status: "running", startedAt: Date.now() },
      });
    }

    replacementsReady.resolve();
    const results = await Promise.all(pendingKills);

    expect(results.every((result) => result.status === "ok" && result.killed === 0)).toBe(true);
    expect(clearQueues).not.toHaveBeenCalled();
    for (const [index, childSessionKey] of successorKeys.entries()) {
      expect(getSubagentRunByChildSessionKey(childSessionKey)).toMatchObject({
        runId: `run-old-${index}`,
        controllerSessionKey,
        generation: 2,
        execution: { status: "running" },
      });
      expect(getSubagentRunByChildSessionKey(childSessionKey)?.execution.endedAt).toBeUndefined();
      expect(getSubagentRunByChildSessionKey(descendantKeys[index]!)).toMatchObject({
        runId: `run-successor-leaf-${index}`,
        execution: { status: "running" },
      });
      expect(
        getSubagentRunByChildSessionKey(descendantKeys[index]!)?.execution.endedAt,
      ).toBeUndefined();
    }
  });

  it("fences a successor that appears while kill persistence is pending", async () => {
    const childSessionKey = "agent:main:subagent:persist-generation-race";
    const descendantSessionKey = `${childSessionKey}:subagent:leaf`;
    const controllerSessionKey = "agent:main:main";
    let oldRun = createSubagentRunRecord({
      runId: "run-persist-old",
      childSessionKey,
      controllerSessionKey,
      requesterSessionKey: controllerSessionKey,
      task: "old persisted task",
      generation: 1,
      createdAt: Date.now() - 5_000,
      startedAt: Date.now() - 4_000,
    });
    const storePath = await writeSessionStoreFixture("persist-generation-race", {
      [childSessionKey]: {
        sessionId: "sess-persist-generation-race",
        updatedAt: Date.now(),
      },
    });
    const oldRunFixture = oldRun;
    await addSubagentRunForTests(oldRun);
    oldRun = subagentRuns.get(oldRun.runId)!;

    const persistenceStarted = createDeferred();
    const persistenceRelease = createDeferred();
    const clearQueues = vi.fn(() => ({ followupCleared: 0, keys: [] }));
    setSubagentControlDepsForTest({
      clearSessionQueues: clearQueues,
    });
    const persistMarker = killSession.persistSubagentAbortedLastRun;
    using markerSpy = vi.spyOn(killSession, "persistSubagentAbortedLastRun");
    markerSpy.mockImplementation(async (params) => {
      if (params.childSessionKey === childSessionKey && params.abortedLastRun) {
        persistenceStarted.resolve();
        await persistenceRelease.promise;
      }
      return persistMarker(params);
    });

    const pendingKill = killAllControlledSubagentRuns({
      cfg: cfgWithSessionStore(storePath),
      controller: controllerFor(controllerSessionKey),
      runs: [oldRun],
    });
    try {
      await persistenceStarted.promise;

      await addSubagentRunForTests({
        ...oldRunFixture,
        runId: "run-persist-successor",
        controllerSessionKey: "agent:foreign:controller",
        requesterSessionKey: "agent:foreign:controller",
        requesterDisplayKey: "agent:foreign:controller",
        task: "successor persisted task",
        generation: 2,
        createdAt: Date.now(),
        execution: { status: "running", startedAt: Date.now() },
      });
      await addSubagentRunForTests({
        ...oldRunFixture,
        runId: "run-persist-successor-leaf",
        childSessionKey: descendantSessionKey,
        controllerSessionKey: childSessionKey,
        requesterSessionKey: childSessionKey,
        requesterDisplayKey: childSessionKey,
        task: "successor persisted leaf",
        createdAt: Date.now(),
        execution: { status: "running", startedAt: Date.now() },
      });
    } finally {
      persistenceRelease.resolve();
      await pendingKill;
    }

    await expect(pendingKill).resolves.toMatchObject({
      status: "ok",
      killed: 1,
      labels: ["old persisted task"],
    });
    expect(clearQueues).toHaveBeenCalledOnce();
    for (const [sessionKey, runId] of [
      [childSessionKey, "run-persist-successor"],
      [descendantSessionKey, "run-persist-successor-leaf"],
    ] as const) {
      const successor = getSubagentRunByChildSessionKey(sessionKey);
      expect(successor).toMatchObject({ runId, execution: { status: "running" } });
      expect(successor?.execution.endedAt).toBeUndefined();
    }
  });

  it("does not patch the replacement session after the killed row commits", async () => {
    const childSessionKey = "agent:main:subagent:kill-session-patch-reset";
    const storePath = await writeSessionStoreFixture("kill-session-patch-reset", {
      [childSessionKey]: {
        sessionId: "sess-kill-session-patch-reset",
        lifecycleRevision: "revision-before-reset",
        updatedAt: Date.now(),
      },
    });
    let entry = createSubagentRunRecord({
      runId: "run-kill-session-patch-reset",
      childSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "do not patch successor",
      createdAt: Date.now() - 5_000,
      startedAt: Date.now() - 4_000,
    });
    await addSubagentRunForTests(entry);
    entry = subagentRuns.get(entry.runId)!;
    const patches: Array<Partial<SessionEntry> | null> = [];
    mockSessionReplacementForStore(storePath, async (params) => {
      const replacement: SessionEntry = {
        sessionId: "sess-kill-session-patch-reset",
        lifecycleRevision: "revision-after-reset",
        updatedAt: Date.now(),
      };
      const operation = await params.update([{ sessionKey: childSessionKey, entry: replacement }]);
      params.assertCommitAllowed?.();
      patches.push([...(operation.replacements ?? [])][0]?.entry ?? null);
      return operation.result;
    });

    await expect(
      killAllControlledSubagentRuns({
        cfg: cfgWithSessionStore(storePath),
        controller: controllerFor(),
        runs: [entry],
      }),
    ).resolves.toMatchObject({ status: "ok", killed: 1 });

    expect(patches).toEqual([null, null]);
    expect(getSubagentRunByChildSessionKey(childSessionKey)).toMatchObject({
      endedReason: SUBAGENT_ENDED_REASON_KILLED,
      execution: { status: "terminal" },
    });
  });

  it("kills a yielded descendant without reviving a stale child row", async () => {
    const parentSessionKey = "agent:main:subagent:kill-parent";
    const childSessionKey = `${parentSessionKey}:subagent:child`;
    const leafSessionKey = `${childSessionKey}:subagent:leaf`;

    let parentRun = createSubagentRunRecord({
      runId: "run-parent-current",
      childSessionKey: parentSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "current parent task",
      createdAt: Date.now() - 8_000,
      startedAt: Date.now() - 7_000,
      endedAt: Date.now() - 6_000,
      outcome: { status: "ok" },
    });
    await addSubagentRunForTests(parentRun);
    parentRun = subagentRuns.get(parentRun.runId)!;
    await addSubagentRunForTests({
      runId: "run-child-stale",
      childSessionKey,
      controllerSessionKey: parentSessionKey,
      requesterSessionKey: parentSessionKey,
      requesterDisplayKey: parentSessionKey,
      task: "stale child task",
      createdAt: Date.now() - 5_000,
      startedAt: Date.now() - 4_000,
    });
    await addSubagentRunForTests({
      runId: "run-child-current",
      childSessionKey,
      controllerSessionKey: parentSessionKey,
      requesterSessionKey: parentSessionKey,
      requesterDisplayKey: parentSessionKey,
      task: "current child task",
      createdAt: Date.now() - 3_000,
      startedAt: Date.now() - 2_000,
      endedAt: Date.now() - 1_500,
      outcome: { status: "ok" },
    });
    await addSubagentRunForTests({
      runId: "run-leaf-active",
      childSessionKey: leafSessionKey,
      controllerSessionKey: childSessionKey,
      requesterSessionKey: childSessionKey,
      requesterDisplayKey: childSessionKey,
      task: "leaf task",
      createdAt: Date.now() - 1_000,
      startedAt: Date.now() - 900,
      endedAt: Date.now() - 800,
      pauseReason: "sessions_yield",
    });

    const result = await killAllControlledSubagentRuns({
      cfg: cfgWithSessionStore(),
      controller: controllerFor(),
      runs: [parentRun],
    });

    expect(result).toEqual({
      status: "ok",
      killed: 1,
      labels: ["leaf task"],
    });
    expect(getSubagentRunByChildSessionKey(leafSessionKey)?.execution.endedAt).toBeTypeOf("number");
  });

  it("does not cascade through a child session that moved to a newer parent", async () => {
    const oldParentSessionKey = "agent:main:subagent:old-parent";
    const newParentSessionKey = "agent:main:subagent:new-parent";
    const childSessionKey = "agent:main:subagent:shared-child";
    const leafSessionKey = `${childSessionKey}:subagent:leaf`;

    let oldParentRun = createSubagentRunRecord({
      runId: "run-old-parent-current",
      childSessionKey: oldParentSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "old parent task",
      createdAt: Date.now() - 8_000,
      startedAt: Date.now() - 7_000,
      endedAt: Date.now() - 6_000,
      outcome: { status: "ok" },
    });
    await addSubagentRunForTests(oldParentRun);
    oldParentRun = subagentRuns.get(oldParentRun.runId)!;
    await addSubagentRunForTests({
      runId: "run-new-parent-current",
      childSessionKey: newParentSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "new parent task",
      createdAt: Date.now() - 5_000,
      startedAt: Date.now() - 4_000,
    });
    await addSubagentRunForTests({
      runId: "run-child-stale-old-parent",
      childSessionKey,
      controllerSessionKey: oldParentSessionKey,
      requesterSessionKey: oldParentSessionKey,
      requesterDisplayKey: oldParentSessionKey,
      task: "stale shared child task",
      createdAt: Date.now() - 4_000,
      startedAt: Date.now() - 3_500,
      endedAt: Date.now() - 3_000,
      outcome: { status: "ok" },
    });
    await addSubagentRunForTests({
      runId: "run-child-current-new-parent",
      childSessionKey,
      controllerSessionKey: newParentSessionKey,
      requesterSessionKey: newParentSessionKey,
      requesterDisplayKey: newParentSessionKey,
      task: "current shared child task",
      createdAt: Date.now() - 2_000,
      startedAt: Date.now() - 1_500,
    });
    await addSubagentRunForTests({
      runId: "run-leaf-active",
      childSessionKey: leafSessionKey,
      controllerSessionKey: childSessionKey,
      requesterSessionKey: childSessionKey,
      requesterDisplayKey: childSessionKey,
      task: "leaf task",
      createdAt: Date.now() - 1_000,
      startedAt: Date.now() - 900,
    });

    const result = await killAllControlledSubagentRuns({
      cfg: cfgWithSessionStore(),
      controller: controllerFor(),
      runs: [oldParentRun],
    });

    expect(result).toEqual({
      status: "ok",
      killed: 0,
      labels: [],
    });
    expect(getSubagentRunByChildSessionKey(leafSessionKey)?.execution.endedAt).toBeUndefined();
  });

  it("interrupts a pending recovery admission before deciding the kill target is inactive", async () => {
    const controllerSessionKey = "agent:main:main";
    const childSessionKey = "agent:main:subagent:kill-recovery-admission";
    const sessionId = "sess-kill-recovery-admission";
    let entry = createSubagentRunRecord({
      runId: "run-kill-recovery-admission",
      childSessionKey,
      controllerSessionKey,
      requesterSessionKey: controllerSessionKey,
      task: "kill recovery admission",
      createdAt: Date.now() - 2_000,
      execution: { status: "running", startedAt: Date.now() - 1_000 },
    });
    await addSubagentRunForTests(entry);
    entry = subagentRuns.get(entry.runId)!;
    const storePath = await writeSessionStoreFixture("kill-recovery-admission", {
      [childSessionKey]: { sessionId, updatedAt: Date.now(), abortedLastRun: true },
    });
    const interrupted = createDeferred();
    const admission = await beginSessionEffect({
      scope: storePath,
      identities: [childSessionKey, sessionId],
      assertAllowed: () => {},
      onInterrupt: () => interrupted.resolve(),
    });
    const handoffId = admission.createHandoff();
    let recoveryActive = false;
    setSubagentControlDepsForTest({
      clearSessionQueues: () => ({ followupCleared: 0, keys: [] }),
    });

    const pendingKill = killAllControlledSubagentRuns({
      cfg: cfgWithSessionStore(storePath),
      controller: controllerFor(controllerSessionKey),
      runs: [entry],
    });
    let adopted: ReturnType<typeof consumeSessionEffectHandoff>;
    try {
      await interrupted.promise;
      expect(getSessionMutationCount()).toBeGreaterThan(0);
      adopted = consumeSessionEffectHandoff({
        handoffId,
        scope: storePath,
        identities: [childSessionKey, sessionId],
        onInterrupt: () => {
          recoveryActive = true;
        },
      });
      expect(adopted).toBeDefined();
      expect(recoveryActive).toBe(true);
      adopted?.release();

      await expect(pendingKill).resolves.toMatchObject({ status: "ok" });
      expect(getSubagentRunByChildSessionKey(childSessionKey)).toMatchObject({
        endedReason: SUBAGENT_ENDED_REASON_KILLED,
        execution: { status: "terminal" },
      });
    } finally {
      adopted?.release();
      admission.release();
      await pendingKill;
    }
  });

  it.each([false, true])(
    "releases queued=%s work when interrupted admission does not drain",
    async (queued) => {
      const controllerSessionKey = "agent:main:main";
      const childSessionKey = "agent:main:subagent:kill-admission-timeout";
      const sessionId = "sess-kill-admission-timeout";
      let entry = createSubagentRunRecord({
        runId: "run-kill-admission-timeout",
        childSessionKey,
        controllerSessionKey,
        requesterSessionKey: controllerSessionKey,
        task: "hold admission during kill",
        createdAt: Date.now() - 2_000,
        collect: queued,
        execution: queued
          ? { status: "queued" }
          : { status: "running", startedAt: Date.now() - 1_000 },
      });
      await addSubagentRunForTests(entry);
      entry = subagentRuns.get(entry.runId)!;
      const storePath = await writeSessionStoreFixture("kill-admission-timeout", {
        [childSessionKey]: { sessionId, updatedAt: Date.now() },
      });
      const interrupted = createDeferred();
      const admission = await beginSessionEffect({
        scope: storePath,
        identities: [childSessionKey, sessionId],
        assertAllowed: () => {},
        onInterrupt: () => interrupted.resolve(),
      });
      setSubagentControlDepsForTest({
        clearSessionQueues: () => ({ followupCleared: 0, keys: [] }),
      });

      const dispatch = vi.fn(async () => {});
      if (queued) {
        enqueueSwarmRun({
          groupId: "drain",
          runId: entry.runId,
          maxConcurrent: 1,
          activeRunIds: ["holder"],
          start: dispatch,
          onStartFailure: () => true,
        });
      }
      vi.useFakeTimers();
      const pendingKill = killAllControlledSubagentRuns({
        cfg: cfgWithSessionStore(storePath),
        controller: controllerFor(controllerSessionKey),
        runs: [entry],
      });
      let killSettled = false;
      void pendingKill.then(
        () => {
          killSettled = true;
        },
        () => {
          killSettled = true;
        },
      );
      try {
        await Promise.race([
          interrupted.promise,
          pendingKill.then(() => {
            throw new Error("Cancellation settled before interrupting the held admission");
          }),
        ]);
        await Promise.resolve();
        expect(killSettled).toBe(false);
        if (queued) {
          releaseSwarmRun("holder");
        }
        await Promise.resolve();
        expect(dispatch).not.toHaveBeenCalled();
        await vi.advanceTimersByTimeAsync(SESSION_CONTROLLER_DRAIN_TIMEOUT_MS);

        await expect(pendingKill).resolves.toMatchObject({
          status: "error",
          error:
            "hold admission during kill: Subagent is still active; try the kill again in a moment.",
        });
        expect(getSubagentRunByChildSessionKey(childSessionKey)?.execution.endedAt).toBeUndefined();
        expect(getSubagentRunByChildSessionKey(childSessionKey)?.killIntent).toBeUndefined();
        if (queued) {
          await vi.waitFor(() => expect(dispatch).toHaveBeenCalledOnce());
        }
      } finally {
        admission.release();
        swarmSchedulerTesting.reset();
        vi.useRealTimers();
        await pendingKill;
      }
    },
  );

  it("finishes the kill when a child accepts cancellation but never settles", async () => {
    const controllerSessionKey = "agent:main:main";
    const childSessionKey = "agent:main:subagent:kill-unsettled-producer";
    const sessionId = "sess-kill-unsettled-producer";
    let entry = createSubagentRunRecord({
      runId: "run-kill-unsettled-producer",
      childSessionKey,
      controllerSessionKey,
      requesterSessionKey: controllerSessionKey,
      task: "ignore cancellation",
      createdAt: Date.now() - 2_000,
      execution: { status: "running", startedAt: Date.now() - 1_000 },
    });
    await addSubagentRunForTests(entry);
    entry = subagentRuns.get(entry.runId)!;
    const storePath = await writeSessionStoreFixture("kill-unsettled-producer", {
      [childSessionKey]: { sessionId, updatedAt: Date.now() },
    });
    setSubagentControlDepsForTest({
      clearSessionQueues: () => ({ followupCleared: 0, keys: [] }),
    });
    const operation = createReplyOperation({
      sessionKey: childSessionKey,
      sessionId,
      resetTriggered: false,
      target: captureSessionTarget({
        storeScope: storePath,
        sessionKey: childSessionKey,
        incarnation: sessionId,
      }),
    });
    operation.attachBackend({ kind: "embedded", isStreaming: () => true, cancel: () => {} });
    vi.useFakeTimers();
    const pendingKill = killAllControlledSubagentRuns({
      cfg: cfgWithSessionStore(storePath),
      controller: controllerFor(controllerSessionKey),
      runs: [entry],
    });
    try {
      await vi.waitFor(() => expect(operation.abortSignal.aborted).toBe(true));
      await vi.advanceTimersByTimeAsync(SESSION_CONTROLLER_DRAIN_TIMEOUT_MS);

      await expect(pendingKill).resolves.toMatchObject({ status: "ok", killed: 1 });
      expect(getSubagentRunByChildSessionKey(childSessionKey)).toMatchObject({
        endedReason: SUBAGENT_ENDED_REASON_KILLED,
      });
    } finally {
      operation.complete();
      vi.useRealTimers();
      await pendingKill;
    }
  });

  it("leaves restart recovery disabled when the kill tombstone cannot persist", async () => {
    const controllerSessionKey = "agent:main:main";
    const childSessionKey = "agent:main:subagent:kill-tombstone-failure";
    const sessionId = "sess-kill-tombstone-failure";
    let entry = createSubagentRunRecord({
      runId: "run-kill-tombstone-failure",
      childSessionKey,
      controllerSessionKey,
      requesterSessionKey: controllerSessionKey,
      task: "kill tombstone failure",
      createdAt: Date.now() - 2_000,
      execution: {
        status: "interrupted",
        startedAt: Date.now() - 1_000,
        restartRecovery: {
          sessionId,
          sessionMarker: `${sessionId}:1`,
          idempotencyKey: "recovery-kill-tombstone-failure",
          phase: "reserved",
        },
      },
    });
    await addSubagentRunForTests(entry);
    entry = subagentRuns.get(entry.runId)!;
    const storePath = await writeSessionStoreFixture("kill-tombstone-failure", {
      [childSessionKey]: { sessionId, updatedAt: 1, abortedLastRun: true },
    });
    const abortedLastRunWrites: boolean[] = [];
    let persistenceWrites = 0;
    mockSessionReplacementForStore(storePath, async (params) => {
      const current = { sessionId, updatedAt: 1, abortedLastRun: true };
      const operation = await params.update([{ sessionKey: childSessionKey, entry: current }]);
      for (const { entry: replacement } of operation.replacements ?? []) {
        abortedLastRunWrites.push(replacement.abortedLastRun === true);
      }
      return operation.result;
    });
    resetRegistryLeafMocks();
    fixture.worker.mockImplementation((context, operation, options) =>
      runSubagentStateWorkerOperation(
        context,
        (scope) =>
          operation({
            ...scope,
            execute: async (command) => {
              if (command.type === "subagents.persistChanges" && ++persistenceWrites === 2) {
                throw new Error("sqlite busy");
              }
              return scope.execute(command);
            },
          }),
        options,
      ),
    );

    await expect(
      killAllControlledSubagentRuns({
        cfg: cfgWithSessionStore(storePath),
        controller: controllerFor(controllerSessionKey),
        runs: [entry],
      }),
    ).resolves.toMatchObject({
      status: "error",
      error: expect.stringContaining("Failed to persist subagent kill tombstone"),
    });

    expect(abortedLastRunWrites).toEqual([]);
    expect(getSubagentRunByChildSessionKey(childSessionKey)).toMatchObject({
      runId: entry.runId,
      killIntent: { reason: "killed", sessionId },
      execution: {
        status: "interrupted",
        restartRecovery: { phase: "reserved" },
      },
    });
    expect(getSubagentRunByChildSessionKey(childSessionKey)?.execution.endedAt).toBeUndefined();
  });
});

describe("killAllControlledSubagentRuns", () => {
  registerLateDescendantControlTests({
    cfgWithSessionStore,
    setSubagentControlDepsForTest,
    writeSessionStoreFixture,
  });

  registerQueueStopControlTests({
    cfgWithSessionStore,
    setSubagentControlDepsForTest,
    writeSessionStoreFixture,
  });

  it.each([
    "intent write",
    "tombstone write",
    "session replacement at intent",
    "session replacement release",
    "row replacement",
    "lifecycle rotation",
    "parent persistence",
  ])("releases or withdraws the exact queued reservation after %s failure", async (failure) => {
    const controllerSessionKey = "agent:main:main";
    let entry = createSubagentRunRecord({
      runId: "failure-queued",
      childSessionKey: "agent:main:subagent:failure-queued",
      controllerSessionKey,
      requesterSessionKey: controllerSessionKey,
      task: "queued failure",
      createdAt: 1,
      generation: 1,
      collect: true,
      swarmLaunchPending: true,
      execution: { status: "queued" },
    });
    await addSubagentRunForTests(entry);
    entry = subagentRuns.get(entry.runId)!;
    const storePath = await writeSessionStoreFixture("queue-failure", {
      [entry.childSessionKey]: { sessionId: "queued-session", updatedAt: 1 },
    });
    const dispatch = vi.fn(async () => {});
    const reserve = () =>
      enqueueSwarmRun({
        groupId: "failure-lane",
        runId: entry.runId,
        maxConcurrent: 1,
        activeRunIds: [],
        start: dispatch,
        onStartFailure: () => true,
      });
    reserve();
    let writes = 0;
    let rejectedWrite: number | undefined;
    resetRegistryLeafMocks();
    fixture.worker.mockImplementation((context, operation, options) =>
      runSubagentStateWorkerOperation(
        context,
        (scope) =>
          operation({
            ...scope,
            execute: async (command, commandOptions) => {
              const writeNumber =
                command.type === "subagents.persistChanges" ? ++writes : undefined;
              if (writeNumber !== undefined) {
                if (failure === "session replacement at intent" && writeNumber === 1) {
                  replaceSessionEntrySync(
                    { storePath, sessionKey: entry.childSessionKey },
                    { sessionId: "new-session", updatedAt: 2 },
                  );
                }
                if (
                  (failure === "intent write" && writeNumber === 1) ||
                  (["tombstone write", "session replacement release"].includes(failure) &&
                    writeNumber === 2)
                ) {
                  rejectedWrite = writeNumber;
                  throw new Error("sqlite busy");
                }
              }
              const receipt = await scope.execute(command, commandOptions);
              if (failure === "session replacement release" && writeNumber === 1) {
                replaceSessionEntrySync(
                  { storePath, sessionKey: entry.childSessionKey },
                  { sessionId: "new-session", updatedAt: 2 },
                );
              }
              return receipt;
            },
          }),
        options,
      ),
    );
    setSubagentControlDepsForTest({
      clearSessionQueues: () => ({ followupCleared: 0, keys: [] }),
    });
    const reservationReleases: Promise<void>[] = [];
    try {
      const pending = killAllControlledSubagentRuns({
        cfg: cfgWithSessionStore(storePath),
        controller: {
          controllerSessionKey,
          controllerAgentId: "main",
          callerSessionKey: controllerSessionKey,
          callerIsSubagent: false,
          controlScope: "children",
        },
        runs: [entry],
        beforeKill: async () => {
          await Promise.resolve();
          expect(
            dispatch,
            "scheduled pump cannot dispatch while cancellation owns the reservation",
          ).not.toHaveBeenCalled();
          if (failure === "row replacement") {
            const reservation = holdQueuedSwarmRun(entry.runId);
            const withdrawn = reservation?.withdraw();
            if (reservation) {
              reservationReleases.push(reservation.release());
            }
            expect(withdrawn).toBe(true);
            await addSubagentRunForTests({ ...entry, generation: 2, createdAt: 2 });
            reserve();
          }
          if (failure === "lifecycle rotation") {
            rotateAgentEventLifecycleGeneration();
          }
          if (failure === "parent persistence") {
            throw new Error("partial persistence failed");
          }
          return true;
        },
      });
      if (failure === "parent persistence") {
        await expect(pending).rejects.toThrow("partial persistence failed");
      } else {
        const result = await pending;
        expect(result.killed).toBe(0);
        expect(result.status).toBe(
          ["row replacement", "lifecycle rotation"].includes(failure) ? "ok" : "error",
        );
        if (failure === "session replacement release") {
          expect(rejectedWrite).toBe(2);
          expect(result).toMatchObject({
            error: expect.stringContaining("kill intent could not be released"),
          });
        }
      }
      if (["tombstone write", "session replacement release"].includes(failure)) {
        expect(subagentRuns.get(entry.runId)?.killIntent).toMatchObject({ reason: "killed" });
        const survivor = vi.fn(async () => {});
        enqueueSwarmRun({
          groupId: "failure-lane",
          runId: "survivor",
          maxConcurrent: 1,
          activeRunIds: [],
          start: survivor,
          onStartFailure: () => true,
        });
        await vi.waitFor(() => expect(survivor).toHaveBeenCalledOnce());
        expect(dispatch).not.toHaveBeenCalled();
        expect(await markSubagentRunTerminated({ runId: entry.runId })).toBe(1);
        expect(subagentRuns.get(entry.runId)?.collectorCompletion).toMatchObject({
          status: "killed",
        });
        expect(dispatch).not.toHaveBeenCalled();
      } else {
        expect(subagentRuns.get(entry.runId)?.killIntent).toBeUndefined();
        await vi.waitFor(() => expect(dispatch).toHaveBeenCalledOnce());
        expect(
          getSubagentRunByChildSessionKey(entry.childSessionKey)?.execution.endedAt,
        ).toBeUndefined();
      }
    } finally {
      await Promise.all(reservationReleases);
      swarmSchedulerTesting.reset();
    }
  });

  it.each([false, true])(
    "preserves exactRunId=%s authority when an in-flight launch remaps the same row",
    async (exactRunId) => {
      const runId = "launch-before-admission";
      const childSessionKey = "agent:main:subagent:launch-remap";
      const controllerSessionKey = "agent:main:main";
      let entry = createSubagentRunRecord({
        runId,
        childSessionKey,
        controllerSessionKey,
        requesterSessionKey: controllerSessionKey,
        task: "launch remap",
        createdAt: 1,
        collect: true,
        swarmLaunchPending: true,
        schedulerSlotId: runId,
        execution: { status: "queued" },
      });
      await addSubagentRunForTests(entry);
      entry = subagentRuns.get(entry.runId)!;
      const sessionId = "launch-remap-session";
      const storePath = await writeSessionStoreFixture("launch-remap", {
        [childSessionKey]: { sessionId, updatedAt: 1 },
      });
      const admission = await beginSessionEffect({
        scope: storePath,
        identities: [childSessionKey, sessionId],
        assertAllowed: () => {},
      });
      const response = createDeferred();
      const started = createDeferred();
      const launchDone = createDeferred();
      const lease = consumeSessionEffectHandoff({
        handoffId: admission.createHandoff(),
        scope: storePath,
        identities: [childSessionKey, sessionId],
        onInterrupt: () => response.resolve(),
      });
      enqueueSwarmRun({
        groupId: "remapping",
        runId,
        maxConcurrent: 1,
        activeRunIds: [],
        start: async () => {
          started.resolve();
          try {
            await response.promise;
            expect(await startQueuedSubagentRun(runId, "accepted-launch")).toBe(true);
          } finally {
            lease?.release();
            launchDone.resolve();
          }
        },
        onStartFailure: () => true,
      });
      setSubagentControlDepsForTest({
        clearSessionQueues: () => ({ followupCleared: 0, keys: [] }),
      });
      try {
        await started.promise;
        const cfg = cfgWithSessionStore(storePath);
        if (exactRunId) {
          expect(
            await killSubagentRunAdmin({ cfg, sessionKey: childSessionKey, expectedRunId: runId }),
          ).toMatchObject({ killed: true });
        } else {
          expect(
            await killAllControlledSubagentRuns({
              cfg,
              runs: [entry],
              controller: {
                controllerSessionKey,
                controllerAgentId: "main",
                callerSessionKey: controllerSessionKey,
                callerIsSubagent: false,
                controlScope: "children",
              },
            }),
          ).toMatchObject({ killed: 1 });
        }
        expect(getSubagentRunByChildSessionKey(childSessionKey)?.runId).toBe("accepted-launch");
      } finally {
        response.resolve();
        await launchDone.promise;
        lease?.release();
        swarmSchedulerTesting.reset();
      }
    },
  );

  it("checks controller agent identity before holding or cancelling bare-session children", async () => {
    const entries = ["main", "work"].map((requesterAgentId) =>
      createSubagentRunRecord({
        runId: `agent-owned-${requesterAgentId}`,
        childSessionKey: `agent:${requesterAgentId}:subagent:worker`,
        controllerSessionKey: "global",
        requesterSessionKey: "global",
        requesterAgentId,
        requesterDisplayKey: "global",
        task: requesterAgentId,
        createdAt: 1,
        collect: true,
        execution: { status: "queued" },
      }),
    );
    const started: string[] = [];
    for (const [index, entry] of entries.entries()) {
      await addSubagentRunForTests(entry);
      entries[index] = subagentRuns.get(entry.runId)!;
    }
    for (const entry of entries) {
      enqueueSwarmRun({
        groupId: entry.runId,
        runId: entry.runId,
        maxConcurrent: 1,
        activeRunIds: [],
        start: async () => {
          started.push(entry.requesterAgentId!);
        },
        onStartFailure: () => true,
      });
    }
    try {
      const result = await killAllControlledSubagentRuns({
        cfg: cfgWithSessionStore(),
        controller: {
          controllerSessionKey: "global",
          controllerAgentId: "main",
          callerSessionKey: "global",
          callerIsSubagent: false,
          controlScope: "children",
        },
        runs: entries,
        beforeKill: async () => {
          await Promise.resolve();
          expect(started).toEqual(["work"]);
          return true;
        },
      });
      expect(result).toMatchObject({ killed: 1, labels: ["main"] });
      expect(started).toEqual(["work"]);
    } finally {
      swarmSchedulerTesting.reset();
    }
  });

  it.each(["bulk", "channel stop"])(
    "continues %s cancellation after one registry persistence failure",
    async (kind) => {
      let failNextPersistence = true;
      let persistedAfterFailure = false;
      let first = createSubagentRunRecord({
        runId: "run-bulk-persistence-failure-first",
        childSessionKey: "agent:main:subagent:bulk-persistence-failure-first",
        controllerSessionKey: "agent:main:main",
        task: "first bulk task",
        createdAt: Date.now() - 2_000,
        startedAt: Date.now() - 1_900,
      });
      let second = createSubagentRunRecord({
        ...first,
        runId: "run-bulk-persistence-failure-second",
        childSessionKey: "agent:main:subagent:bulk-persistence-failure-second",
        task: "second bulk task",
        createdAt: Date.now() - 1_000,
        execution: { status: "running", startedAt: Date.now() - 900 },
      });
      await addSubagentRunForTests(first);
      first = subagentRuns.get(first.runId)!;
      await addSubagentRunForTests(second);
      second = subagentRuns.get(second.runId)!;
      fixture.worker.mockImplementation((context, operation, options) =>
        runSubagentStateWorkerOperation(
          context,
          (scope) =>
            operation({
              ...scope,
              execute: async (command) => {
                if (command.type === "subagents.persistChanges") {
                  if (failNextPersistence) {
                    failNextPersistence = false;
                    throw new Error("sqlite busy");
                  }
                  persistedAfterFailure = true;
                }
                return scope.execute(command);
              },
            }),
          options,
        ),
      );

      if (kind === "channel stop") {
        expect(
          await stopSubagentsForRequester({
            cfg: cfgWithSessionStore(),
            requesterSessionKey: "agent:main:main",
          }),
        ).toEqual({ stopped: 1, failed: 1 });
      } else {
        const result = await killAllControlledSubagentRuns({
          cfg: cfgWithSessionStore(),
          controller: controllerFor(),
          runs: [first, second],
        });

        expect(result).toEqual({
          status: "error",
          error: "first bulk task: Failed to persist subagent kill intent: sqlite busy",
          failed: 1,
          killed: 1,
          labels: ["second bulk task"],
        });
      }
      expect(persistedAfterFailure).toBe(true);
      expect(
        getSubagentRunByChildSessionKey(first.childSessionKey)?.execution.endedAt,
      ).toBeUndefined();
      expect(getSubagentRunByChildSessionKey(second.childSessionKey)?.execution.endedAt).toBeTypeOf(
        "number",
      );
    },
  );

  it("ignores stale same-id generations in bulk kill requests", async () => {
    const childSessionKey = "agent:main:subagent:stale-kill-all-worker";
    const storePath = await writeSessionStoreFixture("stale-kill-all", {
      [childSessionKey]: {
        updatedAt: Date.now(),
      },
    });

    await addSubagentRunForTests({
      runId: "run-same-bulk",
      childSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "stale bulk task",
      generation: 1,
      createdAt: Date.now() - 9_000,
      startedAt: Date.now() - 8_000,
    });
    const stale = subagentRuns.get("run-same-bulk")!;
    await addSubagentRunForTests({
      runId: "run-same-bulk",
      childSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "current bulk task",
      generation: 2,
      createdAt: Date.now() - 4_000,
      startedAt: Date.now() - 3_000,
    });

    const result = await killAllControlledSubagentRuns({
      cfg: cfgWithSessionStore(storePath),
      controller: controllerFor(),
      runs: [stale],
    });

    expect(result).toEqual({
      status: "ok",
      killed: 0,
      labels: [],
    });
    const persisted = loadSessionEntry({ storePath, sessionKey: childSessionKey });
    expect(persisted?.abortedLastRun).toBeUndefined();
    expect(getSubagentRunByChildSessionKey(childSessionKey)).toMatchObject({
      runId: "run-same-bulk",
      generation: 2,
    });
  });

  it("does not let a stale bulk entry suppress the current yielded entry", async () => {
    const childSessionKey = "agent:main:subagent:stale-kill-all-shadow-worker";
    const storePath = await writeSessionStoreFixture("stale-kill-all-shadow", {
      [childSessionKey]: {
        updatedAt: Date.now(),
      },
    });

    await addSubagentRunForTests({
      runId: "run-stale-shadow",
      childSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "stale shadow task",
      createdAt: Date.now() - 9_000,
      startedAt: Date.now() - 8_000,
    });
    const stale = subagentRuns.get("run-stale-shadow")!;
    let currentShadowRun = createSubagentRunRecord({
      runId: "run-current-shadow",
      childSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "current shadow task",
      createdAt: Date.now() - 4_000,
      startedAt: Date.now() - 3_000,
      endedAt: Date.now() - 2_000,
      pauseReason: "sessions_yield",
    });
    await addSubagentRunForTests(currentShadowRun);
    currentShadowRun = subagentRuns.get(currentShadowRun.runId)!;

    const result = await killAllControlledSubagentRuns({
      cfg: cfgWithSessionStore(storePath),
      controller: controllerFor(),
      runs: [stale, currentShadowRun],
    });

    expect(result).toEqual({
      status: "ok",
      killed: 1,
      labels: ["current shadow task"],
    });
    expect(subagentRuns.get(currentShadowRun.runId)).toMatchObject({
      endedReason: SUBAGENT_ENDED_REASON_KILLED,
      execution: { status: "terminal", endedAt: expect.any(Number) },
    });
    expect(subagentRuns.get(stale.runId)).toEqual(stale);
  });

  it("does not kill a newest finished bulk target when only a stale older row is still active", async () => {
    const childSessionKey = "agent:main:subagent:stale-bulk-finished-worker";

    await addSubagentRunForTests({
      runId: "run-stale-bulk-finished",
      childSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "stale bulk finished task",
      createdAt: Date.now() - 9_000,
      startedAt: Date.now() - 8_000,
    });
    let currentBulkFinishedRun = createSubagentRunRecord({
      runId: "run-current-bulk-finished",
      childSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "current bulk finished task",
      createdAt: Date.now() - 5_000,
      startedAt: Date.now() - 4_000,
      endedAt: Date.now() - 1_000,
      outcome: { status: "ok" },
    });
    await addSubagentRunForTests(currentBulkFinishedRun);
    currentBulkFinishedRun = subagentRuns.get(currentBulkFinishedRun.runId)!;

    const result = await killAllControlledSubagentRuns({
      cfg: cfgWithSessionStore(),
      controller: controllerFor(),
      runs: [currentBulkFinishedRun],
    });

    expect(result).toEqual({
      status: "ok",
      killed: 0,
      labels: [],
    });
  });

  it("cascades through descendants for an ended current bulk target even when a stale older row is still active", async () => {
    const parentSessionKey = "agent:main:subagent:stale-bulk-desc-parent";
    const childSessionKey = `${parentSessionKey}:subagent:leaf`;

    await addSubagentRunForTests({
      runId: "run-stale-bulk-desc-parent",
      childSessionKey: parentSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "stale bulk parent task",
      createdAt: Date.now() - 9_000,
      startedAt: Date.now() - 8_000,
    });
    let currentBulkParentRun = createSubagentRunRecord({
      runId: "run-current-bulk-desc-parent",
      childSessionKey: parentSessionKey,
      controllerSessionKey: "agent:main:main",
      task: "current bulk parent task",
      createdAt: Date.now() - 5_000,
      startedAt: Date.now() - 4_000,
      endedAt: Date.now() - 1_000,
      outcome: { status: "ok" },
    });
    await addSubagentRunForTests(currentBulkParentRun);
    currentBulkParentRun = subagentRuns.get(currentBulkParentRun.runId)!;
    await addSubagentRunForTests({
      runId: "run-active-bulk-desc-child",
      childSessionKey,
      controllerSessionKey: parentSessionKey,
      requesterSessionKey: parentSessionKey,
      requesterDisplayKey: parentSessionKey,
      task: "active bulk child task",
      createdAt: Date.now() - 3_000,
      startedAt: Date.now() - 2_000,
    });

    const result = await killAllControlledSubagentRuns({
      cfg: cfgWithSessionStore(),
      controller: controllerFor(),
      runs: [currentBulkParentRun],
    });

    expect(result).toEqual({
      status: "ok",
      killed: 1,
      labels: ["active bulk child task"],
    });
    expect(getSubagentRunByChildSessionKey(childSessionKey)?.execution.endedAt).toBeTypeOf(
      "number",
    );
  });
});

describe("controlled subagent reads", () => {
  it.each([
    {
      name: "control owner",
      controllerSessionKey: "agent:main:main",
      requesterSessionKey: "agent:main:telegram:direct:abc123",
      expectedCount: 1,
    },
    {
      name: "completion owner",
      controllerSessionKey: "agent:main:telegram:direct:abc123",
      requesterSessionKey: "agent:main:main",
      expectedCount: 1,
    },
    {
      name: "unrelated session",
      controllerSessionKey: "agent:other:discord:direct:xyz",
      requesterSessionKey: "agent:other:main",
      expectedCount: 0,
    },
  ])(
    "applies read visibility for the $name",
    async ({ controllerSessionKey, requesterSessionKey, expectedCount }) => {
      const childSessionKey = "agent:main:subagent:list-visibility";
      await addSubagentRunForTests({
        runId: "run-list-visibility",
        childSessionKey,
        controllerSessionKey,
        requesterSessionKey,
        requesterDisplayKey: requesterSessionKey,
        task: "visibility test",
        createdAt: Date.now(),
        startedAt: Date.now(),
      });

      const { runs: results } = await buildControlledSubagentRunsReadContext("agent:main:main");
      expect(results).toHaveLength(expectedCount);
      if (expectedCount === 1) {
        expect(results[0]?.childSessionKey).toBe(childSessionKey);
      }
    },
  );

  it("uses one stable snapshot for listing and descendant counts", async () => {
    const now = Date.now();
    const rootSessionKey = "agent:main:main";
    const parentSessionKey = "agent:main:subagent:status-parent";
    await addSubagentRunForTests({
      runId: "run-status-parent",
      childSessionKey: parentSessionKey,
      controllerSessionKey: rootSessionKey,
      requesterSessionKey: rootSessionKey,
      requesterDisplayKey: rootSessionKey,
      task: "status parent",
      createdAt: now - 4_000,
      startedAt: now - 3_500,
      endedAt: now - 3_000,
    });
    await addSubagentRunForTests({
      runId: "run-status-child-1",
      childSessionKey: `${parentSessionKey}:subagent:child-1`,
      controllerSessionKey: parentSessionKey,
      requesterSessionKey: parentSessionKey,
      requesterDisplayKey: parentSessionKey,
      task: "status child 1",
      createdAt: now - 2_000,
      startedAt: now - 1_500,
    });

    const context = await buildControlledSubagentRunsReadContext(rootSessionKey);

    await addSubagentRunForTests({
      runId: "run-status-child-2",
      childSessionKey: `${parentSessionKey}:subagent:child-2`,
      controllerSessionKey: parentSessionKey,
      requesterSessionKey: parentSessionKey,
      requesterDisplayKey: parentSessionKey,
      task: "status child 2",
      createdAt: now - 1_000,
      startedAt: now - 500,
    });

    expect(context.runs.map((run) => run.runId)).toEqual(["run-status-parent"]);
    expect(context.list.pendingDescendants.get(parentSessionKey)).toBe(1);
    expect(
      (await buildControlledSubagentRunsReadContext(rootSessionKey)).list.pendingDescendants.get(
        parentSessionKey,
      ),
    ).toBe(2);
  });

  it("partitions duplicate bare controller keys by owning agent", async () => {
    const now = Date.now();
    for (const agentId of ["research", "ops"]) {
      await addSubagentRunForTests({
        runId: `run-${agentId}`,
        childSessionKey: `agent:${agentId}:subagent:child`,
        controllerSessionKey: "global",
        requesterSessionKey: "global",
        requesterAgentId: agentId,
        requesterDisplayKey: "global",
        task: `${agentId} task`,
        createdAt: now,
        startedAt: now,
      });
    }

    const cfg = {
      agents: {
        ownership: "explicit",
        entries: { research: {}, ops: {} },
      },
    } as OpenClawConfig;
    const context = await buildControlledSubagentRunsReadContext("global", "research", cfg);
    expect(context.runs.map((run) => run.runId)).toEqual(["run-research"]);
  });
});
/* oxlint-disable max-lines -- TODO: split this grandfathered oversized file. */
