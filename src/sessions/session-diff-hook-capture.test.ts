import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { withinTest } from "../../test/helpers/promise.js";
import { prepareCommandSessionDiffBaseline } from "../agents/command/session-preparation.js";
import { resolveHookModelSelection } from "../agents/embedded-agent-runner/run/setup.js";
import { runAgentHarnessBeforeMessageWriteHook } from "../agents/harness/hook-helpers.js";
import { loadSessionEntry, replaceSessionEntry } from "../config/sessions/session-accessor.js";
import { createSessionDiffBaselineCaptureClaim } from "../config/sessions/session-diff-baseline-capture.js";
import type { InternalSessionEntry } from "../config/sessions/types.js";
import {
  getGlobalHookRunner,
  initializeGlobalHookRunner,
  resetGlobalHookRunner,
} from "../plugins/hook-runner-global.js";
import { prepareSessionHookRunner } from "../plugins/hook-runner-session.js";
import { createMockPluginRegistry } from "../plugins/hooks.test-helpers.js";
import { createPluginMetadataSnapshotFixture } from "../plugins/plugin-metadata.test-support.js";
import { withPluginRuntimeGenerationScope } from "../plugins/runtime/generation-scope.js";
import { createDeferredCore } from "../shared/deferred.js";
import { useSessionStoreTempDirs } from "../test-utils/session-state-cleanup.js";
import { ensureSessionDiffBaseline } from "./session-diff-baseline.js";
import { withSessionDiffBaselineCapture } from "./session-diff-capture.js";
import { collectCheckoutDiff } from "./session-diff.runtime.js";

const capture = vi.hoisted(() => ({
  hold: undefined as Promise<void> | undefined,
}));
vi.mock("./session-diff.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./session-diff.js")>();
  return {
    ...actual,
    captureSessionDiffBaseline: async (
      params: Parameters<typeof actual.captureSessionDiffBaseline>[0],
    ) => {
      await capture.hold;
      return actual.captureSessionDiffBaseline(params);
    },
  };
});

const dirs = useSessionStoreTempDirs(afterAll, "session-diff-hook-");
afterEach(() => {
  capture.hold = undefined;
  vi.restoreAllMocks();
  resetGlobalHookRunner();
});

async function fixture() {
  const cwd = dirs.make();
  execFileSync("git", ["init", "-q", "-b", "main", cwd]);
  const entry: InternalSessionEntry = {
    sessionId: "hook-capture",
    createdVia: "operator",
    updatedAt: 1,
    sessionDiffBaselineCapture: createSessionDiffBaselineCaptureClaim(),
  };
  const target = {
    agentId: "main",
    sessionKey: "agent:main:hook-capture",
    storePath: path.join(dirs.make(), "sessions.json"),
  };
  await replaceSessionEntry(target, entry);
  return { ...target, cwd, entry, isNewSession: false };
}

function resolveModel() {
  return resolveHookModelSelection({
    prompt: "test",
    provider: "test",
    modelId: "test-model",
    hookRunner: getGlobalHookRunner(),
    hookContext: { runId: "hook-run" },
  });
}

describe("session diff capture before plugin hooks", () => {
  it.for([
    "before_model_resolve",
    "before_message_write",
    "before_agent_reply",
    "before_compaction",
    "prepared_before_message_write",
  ] as const)(
    "attributes writes by a registered %s hook after held capture",
    async (kind, { signal }) => {
      const preparedRegistry = kind === "prepared_before_message_write";
      const hookName = preparedRegistry ? "before_message_write" : kind;
      const target = await fixture();
      const file = path.join(target.cwd, "hook-write.txt");
      const release = createDeferredCore();
      capture.hold = release.promise;
      const registry = createMockPluginRegistry([
        { hookName, handler: () => fs.writeFileSync(file, "hook write\n") },
      ]);
      initializeGlobalHookRunner(preparedRegistry ? createMockPluginRegistry([]) : registry);
      const runner = getGlobalHookRunner()!;
      const selected = createDeferredCore();
      const hasHooks = runner.hasHooks;
      vi.spyOn(runner, "hasHooks").mockImplementation((name, context) => {
        const registered = hasHooks(name, context);
        if (name === hookName && registered) {
          selected.resolve();
        }
        return registered;
      });
      const operation = withSessionDiffBaselineCapture(async () => {
        if (preparedRegistry) {
          await ensureSessionDiffBaseline({ ...target, deferCapture: true });
          return withPluginRuntimeGenerationScope(
            { metadataSnapshot: createPluginMetadataSnapshotFixture(), pluginRegistry: registry },
            async () => {
              await prepareSessionHookRunner(getGlobalHookRunner());
              runAgentHarnessBeforeMessageWriteHook({
                message: { role: "user", content: "test", timestamp: 1 },
              });
            },
          );
        } else if (hookName === "before_message_write") {
          await prepareCommandSessionDiffBaseline(target);
        } else {
          await ensureSessionDiffBaseline({ ...target, deferCapture: true });
        }
        await resolveModel();
        if (hookName === "before_message_write") {
          runAgentHarnessBeforeMessageWriteHook({
            message: { role: "user", content: "test", timestamp: 1 },
          });
        } else if (hookName === "before_agent_reply" && runner.hasHooks(hookName)) {
          await runner.runBeforeAgentReply({ cleanedBody: "test" }, { runId: "hook-run" });
        } else if (hookName === "before_compaction" && runner.hasHooks(hookName)) {
          await runner.runBeforeCompaction({ messageCount: 1 }, { runId: "hook-run" });
        }
      });
      const settlement = Promise.allSettled([operation]);
      try {
        await withinTest(selected.promise, signal);
        expect(fs.existsSync(file)).toBe(false);
        release.resolve();
        await operation;
        expect(fs.readFileSync(file, "utf8")).toBe("hook write\n");
        const entry = loadSessionEntry(target);
        expect(entry?.sessionDiffBaseline).toBeDefined();
        const diff = await collectCheckoutDiff({
          cwd: target.cwd,
          sessionId: target.entry.sessionId,
          baseline: entry?.sessionDiffBaseline,
        });
        expect(diff.files.map((changed) => changed.path)).toEqual(["hook-write.txt"]);
      } finally {
        release.resolve();
        await settlement;
      }
    },
  );

  it.for([false, true])(
    "prepares the model during held capture with only unrelated hooks=%s",
    async (unrelatedHook, { signal }) => {
      const target = await fixture();
      const release = createDeferredCore();
      capture.hold = release.promise;
      initializeGlobalHookRunner(
        createMockPluginRegistry(
          unrelatedHook ? [{ hookName: "gateway_stop", handler: vi.fn() }] : [],
        ),
      );
      const prepared = createDeferredCore();
      const operation = withSessionDiffBaselineCapture(async () => {
        await prepareCommandSessionDiffBaseline(target);
        expect(await resolveModel()).toEqual({ provider: "test", modelId: "test-model" });
        prepared.resolve();
      });
      const settlement = Promise.allSettled([operation]);
      try {
        await withinTest(prepared.promise, signal);
        expect(loadSessionEntry(target)?.sessionDiffBaselineCapture?.status).toBe("pending");
      } finally {
        release.resolve();
        await settlement;
      }
      await operation;
    },
  );
});
