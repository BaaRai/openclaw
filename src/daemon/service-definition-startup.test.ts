import fs from "node:fs/promises";
import path from "node:path";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import * as nativeExec from "./exec-file.js";
import * as schtasks from "./schtasks-exec.js";
import * as inspection from "./schtasks-inspection-deadline.js";
import * as layout from "./schtasks-layout.js";
import * as probe from "./schtasks-state-probe.js";
import { readScheduledTaskDefinitionMutationCapability } from "./service-audit-schtasks.js";
import {
  captureGatewayServiceDefinitionBackup,
  restoreGatewayServiceDefinitionBackup,
} from "./service-definition-backup.js";
import {
  GatewayServiceDefinitionBackupReceiptSchema,
  publishServiceFile,
  readServiceFileState,
} from "./service-stage.js";
import type { GatewayServiceCommandConfig, GatewayServiceEnv } from "./service-types.js";

const native = vi.hoisted(() => ({
  readCommand: vi.fn<typeof import("./schtasks-layout.js").readScheduledTaskCommand>(),
}));
// mock-isolation: Exercise definition custody without loading native service dispatchers.
vi.mock("./service.js", () => ({
  resolveGatewayService: () => ({ readCommand: native.readCommand }),
}));

const dirs = useAutoCleanupTempDirTracker(afterEach);
afterEach(() => vi.restoreAllMocks());
beforeEach(() => {
  native.readCommand.mockReset();
});

async function startupFixture(extension: "cmd" | "vbs" = "cmd") {
  const root = await fs.realpath(dirs.make("service-definition-startup-"));
  const env: GatewayServiceEnv = {
    HOME: root,
    USERPROFILE: root,
    APPDATA: path.join(root, "AppData", "Roaming"),
    OPENCLAW_STATE_DIR: path.join(root, "state"),
    OPENCLAW_WINDOWS_TASK_HIDDEN_LAUNCHER: extension === "vbs" ? "1" : "0",
    USERNAME: "operator",
  };
  vi.spyOn(process, "platform", "get").mockReturnValue("win32");
  const task = vi
    .spyOn(schtasks, "execSchtasks")
    .mockRejectedValue(new Error("Unexpected Task access"));
  vi.spyOn(nativeExec, "execFileUtf8").mockRejectedValue(new Error("Unexpected native execution"));
  const taskState = vi
    .spyOn(probe, "probeScheduledTaskState")
    .mockReturnValue({ status: "missing" });
  const sourcePath = layout.resolveTaskScriptPath(env);
  const companionPath = layout.resolveTaskLauncherScriptPath(
    { OPENCLAW_WINDOWS_TASK_HIDDEN_LAUNCHER: "1" },
    sourcePath,
  );
  const launcherPath = layout.resolveStartupEntryPath(env, extension);
  const aliasPath = layout.resolveStartupEntryPath(env, extension === "cmd" ? "vbs" : "cmd");
  const virtualEnv = {
    ...env,
    APPDATA: "C:/Startup Fixture/roaming",
    OPENCLAW_TASK_SCRIPT: "C:/Startup Fixture/state/gateway.cmd",
  };
  const virtualScript = layout.resolveTaskScriptPath(virtualEnv);
  const virtualPaths = new Map([
    [virtualScript, sourcePath],
    [layout.resolveStartupEntryPath(virtualEnv, extension), launcherPath],
    [layout.resolveStartupEntryPath(virtualEnv, extension === "cmd" ? "vbs" : "cmd"), aliasPath],
  ]);
  const command: GatewayServiceCommandConfig = {
    programArguments: ["C:\\Node A\\node.exe", "C:\\OpenClaw\\index.js", "gateway"],
    sourcePath,
    startupEntryPaths: [launcherPath],
    environment: { OPENCLAW_STATE_DIR: env.OPENCLAW_STATE_DIR! },
  };
  // Translate filesystem locations at inspection's I/O boundary so the real
  // Windows parser and registration rechecks run on every host.
  const readTaskFile = inspection.readTaskFile;
  vi.spyOn(inspection, "readTaskFile").mockImplementation((file, deadline) =>
    readTaskFile(virtualPaths.get(file) ?? file, deadline),
  );
  const readCommand = layout.readScheduledTaskCommand;
  native.readCommand.mockImplementation(async (_env, options) => {
    const observed = await readCommand(virtualEnv, options);
    return (
      observed && {
        ...observed,
        sourcePath:
          observed.sourcePath && (virtualPaths.get(observed.sourcePath) ?? observed.sourcePath),
        ...(observed.startupEntryPaths && {
          startupEntryPaths: observed.startupEntryPaths.map(
            (file) => virtualPaths.get(file) ?? file,
          ),
        }),
      }
    );
  });
  vi.spyOn(layout, "readScheduledTaskCommand").mockImplementation(native.readCommand);
  const original = Buffer.from(layout.buildTaskScript(command));
  const launcher =
    extension === "cmd"
      ? Buffer.from(layout.buildStartupLauncherScript({ scriptPath: virtualScript }))
      : layout.encodeWindowsLauncherScript({
          format: "vbs",
          content: layout.buildHiddenLauncherScript({ scriptPath: virtualScript }),
        });
  await fs.mkdir(path.dirname(sourcePath), { recursive: true });
  await fs.mkdir(path.dirname(launcherPath), { recursive: true });
  await fs.writeFile(sourcePath, original);
  await fs.writeFile(launcherPath, launcher);
  const registration = () => Promise.all([launcherPath, aliasPath].map(readServiceFileState));
  const originalRegistration = await registration();
  const context = { env, command, assertCurrent: () => {} };
  return {
    ...context,
    sourcePath,
    companionPath,
    launcherPath,
    aliasPath,
    original,
    launcher,
    registration,
    originalRegistration,
    task,
    taskState,
    virtualScript,
    registerTask: () =>
      taskState.mockReturnValue({
        status: "found",
        state: 3,
        taskPath: layout.resolveTaskName(env),
        actions: [{ type: 0, path: virtualScript, arguments: "", workingDirectory: "" }],
      }),
    capture: () => captureGatewayServiceDefinitionBackup(context),
  };
}

it.each(["cmd", "vbs"] as const)(
  "admits known Startup %s registration without changing it or accessing Task XML",
  async (extension) => {
    const f = await startupFixture(extension);
    expect(await readScheduledTaskDefinitionMutationCapability(f.env)).toEqual({
      kind: "writable",
    });
    expect(await f.registration()).toEqual(f.originalRegistration);
    expect(await fs.readFile(f.sourcePath)).toEqual(f.original);
    expect(f.task).not.toHaveBeenCalled();
  },
);

it.each([
  "custom command",
  "custom Startup launcher",
  "conflicting aliases",
  "edited launcher",
  "replaced launcher",
  "appearing Task",
] as const)("refuses Startup mutation capability after %s", async (change) => {
  const f = await startupFixture();
  if (change === "custom command") {
    await fs.appendFile(f.sourcePath, "\r\noperator-command\r\n");
  } else if (change === "custom Startup launcher") {
    await fs.appendFile(f.launcherPath, "\r\noperator-command\r\n");
  } else if (change === "conflicting aliases") {
    await fs.writeFile(
      f.aliasPath,
      layout.buildHiddenLauncherScript({ scriptPath: "C:/Other/gateway.cmd" }),
    );
  } else {
    const readCommand = native.readCommand.getMockImplementation()!;
    let reads = 0;
    native.readCommand.mockImplementation(async (...args) => {
      reads += 1;
      if (reads === 2 && change === "edited launcher") {
        await fs.appendFile(f.launcherPath, "\r\noperator-command\r\n");
      } else if (reads === 2 && change === "replaced launcher") {
        const replacement = `${f.launcherPath}.replacement`;
        await fs.writeFile(replacement, f.launcher);
        await fs.rename(replacement, f.launcherPath);
      }
      const observed = await readCommand(...args);
      if (reads === 3 && change === "appearing Task") {
        f.registerTask();
      }
      return observed;
    });
  }
  expect(await readScheduledTaskDefinitionMutationCapability(f.env)).toEqual({
    kind: "unknown",
    reason: "inspection-failed",
  });
  expect(f.task).not.toHaveBeenCalled();
});

it.each(["compensation", "serialized receipt"] as const)(
  "restores changed command files through %s while retaining Startup registration",
  async (recovery) => {
    const f = await startupFixture("vbs");
    const capture = await f.capture();
    const candidate = Buffer.from(
      layout.buildTaskScript({
        ...f.command,
        programArguments: ["C:\\Node B\\node.exe", "C:\\OpenClaw\\index.js", "gateway"],
      }),
    );
    await publishServiceFile({
      filePath: f.sourcePath,
      contents: candidate,
      mode: 0o600,
      definitionTransaction: capture.hooks,
    });
    await publishServiceFile({
      filePath: f.companionPath,
      contents: layout.buildHiddenLauncherScript({ scriptPath: f.sourcePath }),
      mode: 0o600,
      definitionTransaction: capture.hooks,
    });
    const receipt = await capture.finish();
    expect(receipt.task).toBeUndefined();
    expect(receipt.guards).toEqual(
      expect.arrayContaining([
        { sourcePath: f.launcherPath, after: f.originalRegistration[0] },
        { sourcePath: f.aliasPath, after: null },
      ]),
    );
    expect(await fs.readFile(f.sourcePath)).toEqual(candidate);
    expect(await readServiceFileState(f.companionPath)).not.toBeNull();
    expect(await f.registration()).toEqual(f.originalRegistration);
    if (recovery === "compensation") {
      expect(await capture.compensate()).toBe(true);
      const restored = await Promise.all([f.sourcePath, f.companionPath].map(readServiceFileState));
      await capture.compensate();
      expect(await Promise.all([f.sourcePath, f.companionPath].map(readServiceFileState))).toEqual(
        restored,
      );
    } else {
      const checkpoint = capture.backupPaths.find((file) => file.endsWith(".receipt.bak"))!;
      const retained = GatewayServiceDefinitionBackupReceiptSchema.parse(
        JSON.parse(await fs.readFile(checkpoint, "utf8")),
      );
      expect(retained).toEqual(receipt);
      await restoreGatewayServiceDefinitionBackup({
        env: f.env,
        command: f.command,
        assertCurrent: f.assertCurrent,
        receipt: retained,
      });
    }
    expect(await fs.readFile(f.sourcePath)).toEqual(f.original);
    expect(await readServiceFileState(f.companionPath)).toBeNull();
    expect(await f.registration()).toEqual(f.originalRegistration);
    expect(f.task).not.toHaveBeenCalled();
  },
);

it.each(["appeared", "deleted", "edited", "replaced", "became a Task"] as const)(
  "preserves the current definition when Startup registration %s before publication",
  async (change) => {
    const f = await startupFixture();
    const capture = await f.capture();
    let changedRegistration = f.originalRegistration;
    await expect(
      publishServiceFile({
        filePath: f.sourcePath,
        contents: "candidate command\r\n",
        mode: 0o600,
        definitionTransaction: capture.hooks,
        beforeRename: async () => {
          if (change === "appeared") {
            await fs.writeFile(f.aliasPath, "operator launcher\r\n");
          } else if (change === "deleted") {
            await fs.unlink(f.launcherPath);
          } else if (change === "edited") {
            await fs.appendFile(f.launcherPath, "operator command\r\n");
          } else if (change === "replaced") {
            const replacement = `${f.launcherPath}.replacement`;
            await fs.writeFile(replacement, f.launcher);
            await fs.rename(replacement, f.launcherPath);
          } else {
            f.registerTask();
          }
          changedRegistration = await f.registration();
        },
      }),
    ).rejects.toThrow(
      /SERVICE_DEFINITION_UNKNOWN|Effective Scheduled Task service command could not be inspected/,
    );
    expect(await fs.readFile(f.sourcePath)).toEqual(f.original);
    expect(await f.registration()).toEqual(changedRegistration);
    expect(f.task).not.toHaveBeenCalled();
  },
);
