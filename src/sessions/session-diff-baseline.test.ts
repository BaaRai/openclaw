import path from "node:path";
import { afterAll, beforeEach, describe, expect, it, vi } from "vitest";
import {
  isSessionEntryDataSql,
  observeHostDataSql,
} from "../../test/helpers/sqlite-statement-execution-counter.js";
import { hasBeforeToolCallPolicy } from "../agents/agent-tools.before-tool-call.policy.js";
import { wrapToolWithBeforeToolCallHook } from "../agents/agent-tools.before-tool-call.wrapper.js";
import { createAdmittedHostCapabilityTestFixture } from "../agents/harness/host-capability.test-support.js";
import {
  nativeHookRelayEventHasLocalWork,
  nativeHookRelayEventToolMatcher,
} from "../agents/harness/native-hook-relay-events.js";
import { SessionWorkStartInvalidatedError } from "../config/sessions/lifecycle.js";
import {
  deleteSessionEntryLifecycle,
  loadSessionEntry,
  replaceSessionEntry,
} from "../config/sessions/session-accessor.js";
import { createSessionDiffBaselineCaptureClaim } from "../config/sessions/session-diff-baseline-capture.js";
import { projectionLane } from "../config/sessions/session-transcript-worker-resources.js";
import type { InternalSessionEntry, SessionDiffBaseline } from "../config/sessions/types.js";
import { createDeferredCore } from "../shared/deferred.js";
import { useSessionStoreTempDirs } from "../test-utils/session-state-cleanup.js";

type CaptureSessionDiffBaseline =
  (typeof import("./session-diff.js"))["captureSessionDiffBaseline"];
type PatchSessionEntryCore =
  (typeof import("../config/sessions/session-accessor.js"))["patchSessionEntryCore"];
const captureMocks = vi.hoisted(() => ({
  capture: vi.fn<CaptureSessionDiffBaseline>(),
}));
const persistenceMocks = vi.hoisted(() => ({
  actualPatch: undefined as PatchSessionEntryCore | undefined,
  patch: vi.fn<PatchSessionEntryCore>(),
}));

vi.mock("../config/sessions/session-accessor.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../config/sessions/session-accessor.js")>();
  persistenceMocks.actualPatch = actual.patchSessionEntryCore;
  return {
    ...actual,
    patchSessionEntryCore: persistenceMocks.patch,
  };
});

vi.mock("./session-diff.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./session-diff.js")>()),
  captureSessionDiffBaseline: captureMocks.capture,
}));

import {
  ensureSessionDiffBaseline,
  withSessionDiffBaselineCapture,
} from "./session-diff-baseline.js";

const sessionDirs = useSessionStoreTempDirs(afterAll, "openclaw-session-diff-owner-");

function baseline(sessionId: string): SessionDiffBaseline {
  return {
    version: 1,
    sessionId,
    root: "/workspace",
    files: [],
  };
}

function makeEntry(
  sessionId: string,
  fields: Partial<InternalSessionEntry> = {},
): InternalSessionEntry {
  return { createdVia: "operator", sessionId, updatedAt: Date.now(), ...fields };
}

async function seedEntry(params: {
  entry: InternalSessionEntry;
  sessionKey?: string;
  agentId?: string;
}): Promise<{
  agentId: string;
  entry: InternalSessionEntry;
  sessionKey: string;
  storePath: string;
}> {
  const dir = sessionDirs.make();
  const storePath = path.join(dir, "sessions.json");
  const agentId = params.agentId ?? "main";
  const sessionKey = params.sessionKey ?? "agent:main:diff-owner";
  await replaceSessionEntry({ agentId, sessionKey, storePath }, params.entry);
  return { agentId, entry: params.entry, sessionKey, storePath };
}

function ensure(target: Awaited<ReturnType<typeof seedEntry>>, isNewSession = false) {
  return ensureSessionDiffBaseline({ ...target, cwd: "/workspace", isNewSession });
}

function loadInternal(sessionKey: string, storePath: string): InternalSessionEntry | undefined {
  return loadSessionEntry({ sessionKey, storePath }) as InternalSessionEntry | undefined;
}

function expectWorkStartError(
  result: PromiseSettledResult<unknown>,
  message: RegExp,
  code: "SESSION_WORK_START_CHANGED" | "SESSION_WORK_START_INVALIDATED",
): void {
  expect(result.status).toBe("rejected");
  if (result.status === "rejected") {
    expect(result.reason).toMatchObject({ code });
    expect(String(result.reason)).toMatch(message);
  }
}

function deferCapture() {
  const started = createDeferredCore();
  const capture = createDeferredCore<SessionDiffBaseline>();
  captureMocks.capture.mockImplementation(() => {
    started.resolve();
    return capture.promise;
  });
  return { started: started.promise, resolve: capture.resolve };
}

describe("ensureSessionDiffBaseline", () => {
  beforeEach(() => {
    captureMocks.capture.mockReset();
    persistenceMocks.patch.mockReset();
    persistenceMocks.patch.mockImplementation((...args) => {
      if (!persistenceMocks.actualPatch) {
        throw new Error("missing actual session entry patcher");
      }
      return persistenceMocks.actualPatch(...args);
    });
  });

  it.each(["embedded", "native", "host-tool"] as const)(
    "keeps a captured %s tool blocked when the baseline fails after model preparation",
    async (runtime) => {
      const entry = makeEntry(`deferred-${runtime}`, {
        sessionDiffBaselineCapture: createSessionDiffBaselineCaptureClaim(),
      });
      const target = await seedEntry({ entry });
      const capture = createDeferredCore<SessionDiffBaseline>();
      const capturing = createDeferredCore();
      captureMocks.capture.mockImplementationOnce(() => {
        capturing.resolve();
        return capture.promise;
      });
      const prepared = createDeferredCore<() => Promise<unknown>>();
      const finished = createDeferredCore();
      const mutate = vi.fn().mockResolvedValue({ content: [], details: {} });
      const scoped = withSessionDiffBaselineCapture(async () => {
        await ensureSessionDiffBaseline({
          ...target,
          cwd: "/workspace",
          isNewSession: false,
          deferCapture: true,
        });
        expect(hasBeforeToolCallPolicy()).toBe(true);
        const policy = {
          sessionKey: target.sessionKey,
          agentId: target.agentId,
          preToolUseLoopDetection: false,
        };
        expect(nativeHookRelayEventHasLocalWork(policy, "pre_tool_use")).toBe(true);
        expect(nativeHookRelayEventToolMatcher(policy, "pre_tool_use")).toBeUndefined();
        const host =
          runtime !== "embedded"
            ? await createAdmittedHostCapabilityTestFixture({
                runId: `baseline-${runtime}`,
                sessionId: entry.sessionId,
                sessionKey: target.sessionKey,
                agentId: target.agentId,
              })
            : undefined;
        const sourceTool = {
          name: "write",
          label: "Write",
          description: "Write a workspace file",
          parameters: { type: "object", properties: {} },
          execute: mutate,
        } satisfies Parameters<typeof wrapToolWithBeforeToolCallHook>[0];
        const tool = wrapToolWithBeforeToolCallHook(sourceTool, undefined, {
          emitDiagnostics: false,
        });
        prepared.resolve(
          host && runtime === "native"
            ? async () => {
                await host.hostCapabilities.runBeforeToolCall({ toolName: "write", params: {} });
                return mutate();
              }
            : () =>
                (host ? host.hostCapabilities.bindToolSurface([sourceTool])[0]! : tool).execute(
                  "write-1",
                  {},
                ),
        );
        try {
          await finished.promise;
        } finally {
          host?.closeHost();
          host?.closeAdmission();
        }
      });
      const settlement = Promise.allSettled([scoped]);
      try {
        const invoke = await prepared.promise;
        await capturing.promise;
        // Native HTTP callbacks and retained tools execute outside the preparation ALS scope.
        expect(hasBeforeToolCallPolicy()).toBe(false);
        const execution = invoke();
        const refused = expect(execution).rejects.toBeInstanceOf(SessionWorkStartInvalidatedError);
        capture.reject(new SessionWorkStartInvalidatedError("baseline generation changed"));
        await refused;
        expect(mutate).not.toHaveBeenCalled();
      } finally {
        capture.resolve(baseline(entry.sessionId));
        finished.resolve();
        await settlement;
      }
    },
  );

  it.each([false, true])(
    "keeps a global session baseline in its selected agent's custom store (new=%s)",
    async (isNewSession) => {
      const entry: InternalSessionEntry = {
        createdVia: "operator",
        sessionId: "work-global-session",
        sessionDiffBaselineCapture: isNewSession
          ? undefined
          : createSessionDiffBaselineCaptureClaim(),
        updatedAt: 2,
      };
      const target = await seedEntry({ agentId: "work", sessionKey: "global", entry });
      const mainScope = { agentId: "main", sessionKey: "global", storePath: target.storePath };
      await replaceSessionEntry(mainScope, { sessionId: "main-global-session", updatedAt: 1 });
      const mainBefore = loadSessionEntry(mainScope);
      captureMocks.capture.mockResolvedValue(baseline(entry.sessionId));

      const sql = observeHostDataSql();
      const settled = await ensureSessionDiffBaseline({
        ...target,
        cwd: "/workspace",
        isNewSession,
      }).finally(sql.restore);

      expect(sql.queries.filter(isSessionEntryDataSql)).toEqual([]);
      expect(settled.sessionDiffBaseline).toEqual(baseline(entry.sessionId));
      const persisted = loadSessionEntry(target);
      expect(persisted).toMatchObject({
        sessionId: entry.sessionId,
        sessionDiffBaseline: baseline(entry.sessionId),
      });
      expect(persisted?.sessionDiffBaselineCapture).toBeUndefined();
      expect(loadSessionEntry(mainScope)).toEqual(mainBefore);
    },
  );

  it("shares one capture across concurrent first-turn ensures", async () => {
    const sessionId = "concurrent-session";
    const entry = makeEntry(sessionId);
    const target = await seedEntry({ entry });
    const capture = deferCapture();

    const first = ensure(target, true);
    const second = ensure(target, true);
    try {
      await capture.started;
      expect(captureMocks.capture).toHaveBeenCalledTimes(1);
      capture.resolve(baseline(sessionId));

      const [firstResult, secondResult] = await Promise.all([first, second]);
      expect(captureMocks.capture).toHaveBeenCalledTimes(1);
      expect(firstResult.sessionDiffBaseline).toEqual(baseline(sessionId));
      expect(secondResult.sessionDiffBaseline).toEqual(baseline(sessionId));
    } finally {
      capture.resolve(baseline(sessionId));
      await Promise.allSettled([first, second]);
    }
  });

  it("rejects a stale cached baseline after the authoritative generation rotates", async () => {
    const sessionId = "stale-cached-settled";
    const cachedEntry = makeEntry(sessionId, {
      lifecycleRevision: "cached-generation",
      sessionDiffBaseline: baseline(sessionId),
    });
    const target = await seedEntry({ entry: cachedEntry });
    const freshClaim = createSessionDiffBaselineCaptureClaim();
    await replaceSessionEntry(
      { sessionKey: target.sessionKey, storePath: target.storePath },
      {
        ...cachedEntry,
        lifecycleRevision: "fresh-generation",
        sessionDiffBaseline: undefined,
        sessionDiffBaselineCapture: freshClaim,
      },
    );

    await expect(ensure(target)).rejects.toMatchObject({ code: "SESSION_WORK_START_CHANGED" });
    expect(captureMocks.capture).not.toHaveBeenCalled();
    expect(loadInternal(target.sessionKey, target.storePath)).toMatchObject({
      lifecycleRevision: "fresh-generation",
      sessionDiffBaselineCapture: freshClaim,
    });
  });

  it("settles an authoritative pending claim instead of returning a stale cached baseline", async () => {
    const sessionId = "same-generation-stale-settled";
    const cachedEntry = makeEntry(sessionId, {
      lifecycleRevision: "shared-generation",
      sessionDiffBaseline: baseline(sessionId),
    });
    const target = await seedEntry({ entry: cachedEntry });
    const pendingClaim = createSessionDiffBaselineCaptureClaim();
    await replaceSessionEntry(
      { sessionKey: target.sessionKey, storePath: target.storePath },
      {
        ...cachedEntry,
        sessionDiffBaseline: undefined,
        sessionDiffBaselineCapture: pendingClaim,
      },
    );
    const authoritativeBaseline = { ...baseline(sessionId), root: "/authoritative" };
    captureMocks.capture.mockResolvedValue(authoritativeBaseline);

    const settled = await ensure(target);
    expect(settled.sessionDiffBaseline).toEqual(authoritativeBaseline);
    expect(settled.sessionDiffBaselineCapture).toBeUndefined();
    expect(captureMocks.capture).toHaveBeenCalledOnce();
    expect(loadInternal(target.sessionKey, target.storePath)).toMatchObject({
      lifecycleRevision: "shared-generation",
      sessionDiffBaseline: authoritativeBaseline,
    });
    expect(
      loadInternal(target.sessionKey, target.storePath)?.sessionDiffBaselineCapture,
    ).toBeUndefined();
  });

  it("fails closed when the authoritative generation read fails", async () => {
    const sessionId = "settled-read-failure";
    const entry = makeEntry(sessionId, {
      lifecycleRevision: "read-failure-generation",
      sessionDiffBaseline: baseline(sessionId),
    });
    const target = await seedEntry({ entry });
    const read = vi
      .spyOn(projectionLane.pool, "run")
      .mockRejectedValueOnce(new Error("authoritative read failed"));
    try {
      await expect(ensure(target)).rejects.toBeInstanceOf(SessionWorkStartInvalidatedError);
      expect(captureMocks.capture).not.toHaveBeenCalled();
    } finally {
      read.mockRestore();
    }
  });

  it("returns a terminal unavailable entry after capture failure and never retries it", async () => {
    const sessionId = "failed-session";
    const entry = makeEntry(sessionId, {
      sessionDiffBaselineCapture: createSessionDiffBaselineCaptureClaim(),
    });
    const target = await seedEntry({ entry });
    captureMocks.capture.mockRejectedValue(new Error("capture failed"));

    const settled = await ensure(target);
    expect(settled.sessionDiffBaselineCapture).toMatchObject({ status: "unavailable" });
    const unavailable = loadInternal(target.sessionKey, target.storePath);
    expect(unavailable?.sessionDiffBaselineCapture).toMatchObject({
      status: "unavailable",
    });
    if (!unavailable) {
      throw new Error("expected unavailable capture marker");
    }

    await expect(ensure({ ...target, entry: unavailable })).resolves.toEqual(unavailable);
    expect(captureMocks.capture).toHaveBeenCalledTimes(1);
  });

  it.each([
    ["captured baseline", false],
    ["terminal unavailable", true],
  ] as const)("fails closed when persisting %s fails", async (_label, captureFails) => {
    const sessionId = `settlement-failure-${captureFails ? "unavailable" : "baseline"}`;
    const claim = createSessionDiffBaselineCaptureClaim();
    const entry = makeEntry(sessionId, {
      sessionDiffBaselineCapture: claim,
    });
    const target = await seedEntry({ entry });
    if (captureFails) {
      captureMocks.capture.mockRejectedValueOnce(new Error("capture failed"));
    } else {
      captureMocks.capture.mockResolvedValueOnce(baseline(sessionId));
    }
    persistenceMocks.patch.mockRejectedValueOnce(new Error("settlement write failed"));

    const [settled] = await Promise.allSettled([ensure(target)]);
    if (!settled) {
      throw new Error("expected capture settlement");
    }
    expectWorkStartError(
      settled,
      /could not persist its diff baseline/i,
      "SESSION_WORK_START_INVALIDATED",
    );
    expect(loadInternal(target.sessionKey, target.storePath)).toMatchObject({
      sessionDiffBaselineCapture: claim,
    });
  });

  it("preserves an existing work-start invalidation from settlement persistence", async () => {
    const sessionId = "settlement-invalidation";
    const entry = makeEntry(sessionId, {
      sessionDiffBaselineCapture: createSessionDiffBaselineCaptureClaim(),
    });
    const target = await seedEntry({ entry });
    const invalidation = new SessionWorkStartInvalidatedError(
      "session reset while persisting baseline",
    );
    captureMocks.capture.mockResolvedValueOnce(baseline(sessionId));
    persistenceMocks.patch.mockRejectedValueOnce(invalidation);

    await expect(ensure(target)).rejects.toBe(invalidation);
    expect(loadInternal(target.sessionKey, target.storePath)).toMatchObject({
      sessionDiffBaselineCapture: entry.sessionDiffBaselineCapture,
    });
  });

  it("does not retroactively capture a legacy existing session", async () => {
    const entry = makeEntry("legacy-session");
    const target = await seedEntry({ entry });

    const authoritative = loadInternal(target.sessionKey, target.storePath);
    await expect(ensure(target)).resolves.toEqual(authoritative);
    expect(captureMocks.capture).not.toHaveBeenCalled();
    expect(loadInternal(target.sessionKey, target.storePath)).toMatchObject(entry);
    expect(loadInternal(target.sessionKey, target.storePath)).not.toHaveProperty(
      "sessionDiffBaselineCapture",
    );
  });

  it("rejects claim arming before mutating a replacement lifecycle generation", async () => {
    const sessionId = "replacement-before-arm";
    const entry = makeEntry(sessionId, {
      lifecycleRevision: "old-generation",
    });
    const target = await seedEntry({ entry });
    persistenceMocks.patch.mockImplementationOnce(async (...args) => {
      await replaceSessionEntry(
        { sessionKey: target.sessionKey, storePath: target.storePath },
        { ...entry, lifecycleRevision: "replacement-generation" },
      );
      if (!persistenceMocks.actualPatch) {
        throw new Error("missing actual session entry patcher");
      }
      return await persistenceMocks.actualPatch(...args);
    });

    await expect(ensure(target, true)).rejects.toMatchObject({
      code: "SESSION_WORK_START_CHANGED",
    });
    expect(loadInternal(target.sessionKey, target.storePath)).toMatchObject({
      lifecycleRevision: "replacement-generation",
      sessionId,
    });
    expect(loadInternal(target.sessionKey, target.storePath)?.sessionDiffBaselineCapture).toBe(
      undefined,
    );
    expect(captureMocks.capture).not.toHaveBeenCalled();
  });

  it("invalidates claim arming when the authoritative row is missing", async () => {
    const entry = makeEntry("deleted-before-arm");
    const storePath = path.join(sessionDirs.make(), "sessions.json");

    const result = await Promise.allSettled([
      ensure(
        { agentId: "main", entry, sessionKey: "agent:main:missing-before-arm", storePath },
        true,
      ),
    ]);

    const [settled] = result;
    if (!settled) {
      throw new Error("expected claim-arm settlement");
    }
    expectWorkStartError(settled, /was deleted while starting work/i, "SESSION_WORK_START_CHANGED");
    expect(captureMocks.capture).not.toHaveBeenCalled();
  });

  it("invalidates capture completion after the authoritative row is deleted", async () => {
    const sessionId = "deleted-during-capture";
    const entry = makeEntry(sessionId, {
      sessionDiffBaselineCapture: createSessionDiffBaselineCaptureClaim(),
    });
    const target = await seedEntry({ entry });
    const capture = deferCapture();
    const completion = ensure(target);
    const outcome = Promise.allSettled([completion]);
    await capture.started;
    expect(captureMocks.capture).toHaveBeenCalledOnce();
    await deleteSessionEntryLifecycle({
      archiveTranscript: false,
      storePath: target.storePath,
      target: { canonicalKey: target.sessionKey, storeKeys: [target.sessionKey] },
    });
    capture.resolve(baseline(sessionId));

    const [settled] = await outcome;
    if (!settled) {
      throw new Error("expected capture settlement");
    }
    expectWorkStartError(settled, /was deleted while starting work/i, "SESSION_WORK_START_CHANGED");
    expect(loadInternal(target.sessionKey, target.storePath)).toBeUndefined();
  });

  it("rejects an old completion after the same session id receives a fresh claim", async () => {
    const sessionId = "same-session-id";
    const oldClaim = createSessionDiffBaselineCaptureClaim();
    const entry = makeEntry(sessionId, {
      sessionDiffBaselineCapture: oldClaim,
    });
    const target = await seedEntry({ entry });
    const capture = deferCapture();
    const oldCompletions = [ensure(target), ensure(target)];
    const outcomes = Promise.allSettled(oldCompletions);
    await capture.started;
    expect(captureMocks.capture).toHaveBeenCalledTimes(1);

    const freshClaim = createSessionDiffBaselineCaptureClaim();
    await replaceSessionEntry(
      { sessionKey: target.sessionKey, storePath: target.storePath },
      { ...entry, lifecycleRevision: "fresh-generation", sessionDiffBaselineCapture: freshClaim },
    );
    capture.resolve(baseline(sessionId));
    for (const result of await outcomes) {
      expectWorkStartError(result, /changed while starting work/i, "SESSION_WORK_START_CHANGED");
    }

    expect(loadInternal(target.sessionKey, target.storePath)).toMatchObject({
      lifecycleRevision: "fresh-generation",
      sessionDiffBaselineCapture: freshClaim,
    });
    expect(loadInternal(target.sessionKey, target.storePath)?.sessionDiffBaseline).toBeUndefined();
  });

  it("rejects an old completion before mutating a same-claim replacement generation", async () => {
    const sessionId = "same-claim-replacement";
    const claim = createSessionDiffBaselineCaptureClaim();
    const entry = makeEntry(sessionId, {
      lifecycleRevision: "old-generation",
      sessionDiffBaselineCapture: claim,
    });
    const target = await seedEntry({ entry });
    const capture = deferCapture();
    const completion = ensure(target);
    const outcome = Promise.allSettled([completion]);
    await capture.started;
    expect(captureMocks.capture).toHaveBeenCalledOnce();

    await replaceSessionEntry(
      { sessionKey: target.sessionKey, storePath: target.storePath },
      { ...entry, lifecycleRevision: "replacement-generation" },
    );
    capture.resolve(baseline(sessionId));

    const [settled] = await outcome;
    if (!settled) {
      throw new Error("expected capture settlement");
    }
    expectWorkStartError(settled, /changed while starting work/i, "SESSION_WORK_START_CHANGED");
    expect(loadInternal(target.sessionKey, target.storePath)).toMatchObject({
      lifecycleRevision: "replacement-generation",
      sessionDiffBaselineCapture: claim,
    });
    expect(loadInternal(target.sessionKey, target.storePath)?.sessionDiffBaseline).toBeUndefined();
  });
});
