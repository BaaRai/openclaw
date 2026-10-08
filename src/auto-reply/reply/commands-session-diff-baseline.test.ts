import { execFileSync } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { awaitGateBeforeSettlement, createDeferred } from "../../../test/helpers/promise.js";
import { useAutoCleanupTempDirTracker } from "../../../test/helpers/temp-dir.js";
import type { InternalSessionEntry } from "../../config/sessions/types.js";
import { registerPluginCommandInRegistry } from "../../plugins/command-registration.js";
import { createEmptyPluginRegistry } from "../../plugins/registry-empty.js";
import { resetPluginRuntimeStateForTest, setActivePluginRegistry } from "../../plugins/runtime.js";
import * as captureOwner from "../../sessions/session-diff-capture.js";
import {
  collectCheckoutDiff,
  collectCheckoutDiffBaseline,
} from "../../sessions/session-diff.runtime.js";
import { handleCommands } from "./commands-core.js";
import type { CommandDispatchParams } from "./commands-types.js";
import { buildCommandTestParams } from "./commands.test-harness.js";

const mocks = vi.hoisted(() => ({
  execute:
    vi.fn<ReturnType<typeof import("../../agents/bash-tools.js").createExecTool>["execute"]>(),
}));

// mock-isolation: Keep subprocess execution outside the command ordering fixture.
vi.mock("../../agents/bash-tools.js", () => ({
  createExecTool: () => ({ execute: mocks.execute }),
}));

// mock-isolation: These command turns never reset or compact a session.
vi.mock("./commands-reset.js", () => ({ maybeHandleResetCommand: async () => null }));
// mock-isolation: Plugin compaction is unrelated to the registered file-writing command.
vi.mock("./commands-compact.js", () => ({ handleCompactCommand: vi.fn() }));

// mock-isolation: Exercise real selected handlers without loading every command runtime.
vi.mock("./commands-handlers.runtime.js", async () => {
  const [{ handleBashCommand }, { handlePluginCommand }] = await Promise.all([
    import("./commands-bash.js"),
    import("./commands-plugin.js"),
  ]);
  return { loadCommandHandlers: () => [handlePluginCommand, handleBashCommand] };
});

const tempDirs = useAutoCleanupTempDirTracker(afterEach);
let registry = createEmptyPluginRegistry();

beforeEach(() => {
  mocks.execute.mockReset();
  resetPluginRuntimeStateForTest();
  registry = createEmptyPluginRegistry();
  setActivePluginRegistry(registry);
});
afterEach(() => {
  vi.restoreAllMocks();
  resetPluginRuntimeStateForTest();
});

function commandParams(body: string, workspaceDir: string): CommandDispatchParams {
  return {
    ...buildCommandTestParams(
      body,
      {
        agents: { entries: { main: {} } },
        commands: { text: true, bash: true },
        channels: { whatsapp: { allowFrom: ["*"] } },
      },
      { commandText: body, SenderId: "owner", From: "user", To: "bot" },
      { workspaceDir },
    ),
    resolveModelLevels: async () => ({
      resolvedThinkLevel: undefined,
      resolvedReasoningLevel: "off",
    }),
  };
}

describe("command session diff capture", () => {
  it.each([
    { body: "/bash printf written", authorized: true },
    { body: "! printf written", authorized: true },
    { body: "/write-note", authorized: false },
  ])(
    "attributes the first-turn $body write after capture settles",
    async ({ body, authorized }) => {
      const workspaceDir = tempDirs.make("command-diff-baseline-");
      execFileSync("git", ["-C", workspaceDir, "init", "-q", "-b", "main"], { stdio: "pipe" });
      await fs.writeFile(path.join(workspaceDir, "before.txt"), "preexisting\n");
      const file = path.join(workspaceDir, "session.txt");
      const write = vi.fn(async () => fs.writeFile(file, "written\n"));
      mocks.execute.mockImplementation(async () => {
        await write();
        return {
          content: [{ type: "text", text: "written" }],
          details: { status: "completed", exitCode: 0, durationMs: 0, aggregated: "written" },
        };
      });
      expect(
        registerPluginCommandInRegistry(registry, "capture-test", {
          name: "write-note",
          description: "Write a test note",
          requireAuth: false,
          handler: async () => {
            await write();
            return { text: "written" };
          },
        }),
      ).toEqual({ ok: true });

      const release = createDeferred();
      const joined = createDeferred();
      const sessionId = "first-command";
      const ready = release.promise.then(async (): Promise<InternalSessionEntry> => {
        const baseline = await collectCheckoutDiffBaseline({ cwd: workspaceDir });
        if (!baseline) {
          throw new Error("test checkout baseline was unavailable");
        }
        return {
          sessionId,
          updatedAt: 1,
          sessionDiffBaseline: { ...baseline, sessionId },
        };
      });
      vi.spyOn(captureOwner, "getSessionDiffBaselineCapture").mockImplementation(() => {
        joined.resolve();
        return ready;
      });
      const params = commandParams(body, workspaceDir);
      params.command.isAuthorizedSender = authorized;
      params.command.senderIsOwner = authorized;
      const command = handleCommands(params);
      try {
        await awaitGateBeforeSettlement(
          joined.promise,
          command,
          "command executed before joining the pending baseline capture",
        );
        expect(write).not.toHaveBeenCalled();
        await expect(fs.readFile(file, "utf8")).rejects.toMatchObject({ code: "ENOENT" });
        release.resolve();
        const result = await command;
        expect(result.shouldContinue).toBe(false);
        expect(result.reply?.text).toContain("written");
        expect(write).toHaveBeenCalledOnce();
        const entry = await ready;
        const diff = await collectCheckoutDiff({
          cwd: workspaceDir,
          scope: "uncommitted",
          sessionId,
          baseline: entry.sessionDiffBaseline,
        });
        expect(diff.files.map((changed) => changed.path)).toEqual(["session.txt"]);
      } finally {
        release.resolve();
        await Promise.allSettled([command, ready]);
      }
    },
  );

  it.each(["hello", "/unknown-command hello"])(
    "continues model input %s while capture is held",
    async (body) => {
      const capture = createDeferred<InternalSessionEntry>();
      const joined = createDeferred();
      vi.spyOn(captureOwner, "getSessionDiffBaselineCapture").mockImplementation(() => {
        joined.resolve();
        return capture.promise;
      });
      const command = handleCommands(commandParams(body, "/unused-workspace"));
      try {
        const result = await awaitGateBeforeSettlement(
          command,
          joined.promise,
          "model input unnecessarily joined the pending baseline capture",
        );
        expect(result).toEqual({ shouldContinue: true });
        expect(mocks.execute).not.toHaveBeenCalled();
      } finally {
        capture.resolve({ sessionId: "model-input", updatedAt: 1 });
        await command;
      }
    },
  );
});
