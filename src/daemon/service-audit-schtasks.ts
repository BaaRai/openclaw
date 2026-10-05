import fs from "node:fs/promises";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import { DOMParser } from "linkedom";
import { hasErrnoCode } from "../infra/errno.js";
import {
  getWindowsPowerShellExePath,
  getWindowsSystem32ExePath,
} from "../infra/windows-install-roots.js";
import { decodeWindowsLauncherScript } from "../infra/windows-launcher-encoding.js";
import { hasCommandProcessCleanupError } from "../process/exec-result.js";
import { execFileUtf8 } from "./exec-file.js";
import { execSchtasks } from "./schtasks-exec.js";
import {
  buildTaskScript,
  buildHiddenLauncherScript,
  readScheduledTaskCommand,
  resolveTaskName,
  resolveTaskScriptPath,
  resolveTaskLauncherScriptPath,
} from "./schtasks-layout.js";
import { probeScheduledTaskState } from "./schtasks-state-probe.js";
import { buildScheduledTaskXml } from "./schtasks-xml.js";
import {
  isInstallerServiceDescription,
  serviceDefinitionPreserved,
} from "./service-audit-preservation.js";
import type {
  GatewayServiceExpectedCommand,
  ServiceDefinitionDrift,
} from "./service-audit-types.js";
import { resolveTaskUser } from "./service-process-env.js";
import { readServiceFileState } from "./service-stage.js";
import type {
  GatewayServiceEnv,
  GatewayServiceReadOptions,
  ServiceDefinitionMutationCapability,
} from "./service-types.js";

function elementKey(node: ReturnType<DOMParser["parseFromString"]>["documentElement"]): string {
  return !node.parentElement || node.parentElement.tagName === "Task"
    ? node.tagName
    : `${elementKey(node.parentElement)}.${node.tagName}`;
}

export async function auditScheduledTaskDefinition(
  env: GatewayServiceEnv,
  findings: ServiceDefinitionDrift[],
  timeoutMs?: number,
  expectedCommand?: GatewayServiceExpectedCommand,
  expectedXml?: string,
): Promise<string> {
  const sourcePath = resolveTaskScriptPath(env);
  const hiddenPath = resolveTaskLauncherScriptPath(
    { OPENCLAW_WINDOWS_TASK_HIDDEN_LAUNCHER: "1" },
    sourcePath,
  );
  const query = await execSchtasks(["/Query", "/TN", resolveTaskName(env), "/XML"]);
  if (query.code !== 0) {
    throw new Error("Scheduled Task definition could not be read.");
  }
  const parser = new DOMParser();
  const xml = query.stdout.replace(/^\uFEFF/u, "").replaceAll(String.fromCharCode(0), "");
  const installed = parser.parseFromString(xml, "text/xml");
  if (installed.documentElement?.tagName !== "Task" || installed.doctype) {
    throw new Error("Scheduled Task definition could not be decoded.");
  }
  const expected = parser.parseFromString(
    expectedXml ??
      buildScheduledTaskXml({
        taskDescription: "",
        taskUser: resolveTaskUser(env),
        launchPath: sourcePath,
        interactive: env.OPENCLAW_SERVICE_KIND === "node",
      }),
    "text/xml",
  );
  if (expected.documentElement?.tagName !== "Task" || expected.doctype) {
    throw new Error("Expected Scheduled Task definition could not be decoded.");
  }
  const taskUser = expected.querySelector("Principals > Principal > UserId")?.textContent;
  const samePath = (left: string, right: string) =>
    path.win32.normalize(left).toLowerCase() === path.win32.normalize(right).toLowerCase();
  const unknown = (key: string, reason: string) =>
    findings.push({
      kind: "unknown-edit",
      key,
      reason,
      sourcePath,
      message: `Scheduled Task ${key} contains an unrecognized setting.`,
    });
  const outdated = (key: string, current: string | null, value: string) =>
    findings.push({
      kind: "outdated",
      key,
      current,
      expected: value,
      sourcePath,
      message: `Scheduled Task ${key} differs from the installer value ${value}.`,
    });
  // Scheduler exports names and SIDs independently; a WORKGROUP caller may use
  // the short name while a retained trigger contains its machine-qualified alias.
  const accounts = [
    ...installed.querySelectorAll(
      "Principal > UserId, LogonTrigger > UserId, RegistrationInfo > Author",
    ),
  ].map((node) => node.textContent);
  const sidPattern = /^S-1-[\d-]+$/u;
  const resolvedSids = new Map<string, string>();
  const sid = (name: string) =>
    sidPattern.test(name) ? name : resolvedSids.get(name.toLowerCase());
  const sameUser = (name: string) =>
    Boolean(
      taskUser &&
      (name.toLowerCase() === taskUser.toLowerCase() ||
        (sid(name) !== undefined && sid(name) === sid(taskUser))),
    );
  const names = [
    ...new Set(
      [...(taskUser ? [taskUser] : []), ...accounts].filter((name) => !sidPattern.test(name)),
    ),
  ];
  if (names.length > 0 && taskUser && accounts.some((name) => !sameUser(name))) {
    const encoded = Buffer.from(JSON.stringify(names)).toString("base64");
    // PowerShell 5 emits a JSON array as one object; @() would nest the account list.
    const identity = await execFileUtf8(
      getWindowsPowerShellExePath(),
      [
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        `$ErrorActionPreference='Stop'; $names=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('${encoded}')) | ConvertFrom-Json; foreach($name in $names) { try { ([Security.Principal.NTAccount]$name).Translate([Security.Principal.SecurityIdentifier]).Value } catch { Write-Output '-' } }`,
      ],
      { timeout: timeoutMs ?? 15_000 },
    );
    const values = identity.stdout.trim().split(/\r?\n/u);
    if (identity.code === 0 && values.length === names.length) {
      names.forEach((name, index) => {
        if (sidPattern.test(values[index]!)) {
          resolvedSids.set(name.toLowerCase(), values[index]!);
        }
      });
    }
  }
  const nativeDefaults: Record<string, string> = {
    // https://learn.microsoft.com/en-us/windows/win32/taskschd/task-scheduler-schema
    // DeleteExpiredTaskAfter is excluded: omission disables deletion, unlike explicit PT0S.
    "Principals.Principal.RunLevel": "LeastPrivilege",
    "Triggers.BootTrigger.Enabled": "true",
    "Triggers.BootTrigger.ExecutionTimeLimit": "PT72H",
    "Triggers.BootTrigger.Delay": "PT0M",
    "Triggers.LogonTrigger.Enabled": "true",
    "Triggers.LogonTrigger.ExecutionTimeLimit": "PT72H",
    "Triggers.LogonTrigger.Delay": "PT0M",
    "Settings.AllowStartOnDemand": "true",
    "Settings.MultipleInstancesPolicy": "IgnoreNew",
    "Settings.DisallowStartIfOnBatteries": "true",
    "Settings.StopIfGoingOnBatteries": "true",
    "Settings.AllowHardTerminate": "true",
    "Settings.StartWhenAvailable": "false",
    "Settings.RunOnlyIfNetworkAvailable": "false",
    "Settings.WakeToRun": "false",
    "Settings.Enabled": "true",
    "Settings.Hidden": "false",
    "Settings.ExecutionTimeLimit": "PT72H",
    "Settings.Priority": "7",
    "Settings.RunOnlyIfIdle": "false",
    "Settings.IdleSettings.Duration": "PT10M",
    "Settings.IdleSettings.WaitTimeout": "PT1H",
    "Settings.IdleSettings.StopOnIdleEnd": "true",
    "Settings.IdleSettings.RestartOnIdle": "false",
    "Settings.UseUnifiedSchedulingEngine": "false",
    "Settings.DisallowStartOnRemoteAppSession": "false",
    "Settings.Volatile": "false",
  };
  const released: Record<string, string> = {
    "Settings.DisallowStartIfOnBatteries": "true",
    "Settings.StopIfGoingOnBatteries": "true",
    // Pre-XML installers used /Create defaults for these settings.
    "Settings.ExecutionTimeLimit": "PT72H",
    "Settings.IdleSettings.StopOnIdleEnd": "true",
    "Principals.Principal.LogonType": "InteractiveToken",
    "Settings.RestartOnFailure.Count": "0",
    "Settings.RestartOnFailure.Interval": "PT0S",
  };
  const preserved =
    /^(?:RegistrationInfo\.(?:Description|Date|Author|URI)|Actions\.Exec\.(?:Command|Arguments|WorkingDirectory))$/u;
  const seen = new Set<string>();
  for (const node of [installed.documentElement, ...installed.querySelectorAll("Task *")]) {
    const key = elementKey(node);
    const canonical =
      key === "Task"
        ? expected.documentElement
        : expected.querySelector(key.replaceAll(".", " > "));
    if (seen.has(key)) {
      unknown(key, "Duplicate native definition field.");
    }
    seen.add(key);
    for (const attribute of new Set([
      ...node.getAttributeNames(),
      ...(canonical?.getAttributeNames() ?? []),
    ])) {
      if (
        !(key === "Task" && attribute === "version") &&
        node.getAttribute(attribute) !== canonical?.getAttribute(attribute)
      ) {
        unknown(`${key}.@${attribute}`, "Native definition attribute differs from the installer.");
      }
    }
    const current = node.textContent;
    let nativeRegistration = false;
    if (
      (expectedCommand || expectedXml) &&
      key.startsWith("RegistrationInfo.") &&
      preserved.test(key)
    ) {
      const recognized = key.endsWith("Description")
        ? isInstallerServiceDescription(current, env)
        : key.endsWith("Date")
          ? /^\d{4}-\d\d-\d\dT[\d:.+-]+Z?$/u.test(current)
          : key.endsWith("Author")
            ? sameUser(current)
            : current === `\\${resolveTaskName(env)}`;
      nativeRegistration = key !== "RegistrationInfo.Description" && recognized;
      if (!expectedXml && !recognized) {
        unknown(key, "The installer would replace custom service metadata.");
      }
    }
    if (
      (!expectedXml && preserved.test(key)) ||
      (expectedXml &&
        (nativeRegistration ||
          key === "Settings.Enabled" ||
          (key === "Actions.Exec.Command" &&
            canonical &&
            samePath(current, canonical.textContent)))) ||
      (node.tagName === "UserId" && canonical && sameUser(current))
    ) {
      continue;
    }
    if (
      (!canonical && nativeDefaults[key] === current) ||
      (canonical &&
        (node.children.length || canonical.children.length || current === canonical.textContent))
    ) {
      continue;
    }
    if (canonical && released[key] === current) {
      outdated(key, current, canonical.textContent);
    } else if (
      !expectedXml &&
      canonical &&
      ((key.startsWith("Settings.") && key !== "Settings.Enabled") ||
        key === "Triggers.LogonTrigger.Enabled" ||
        key === "Triggers.BootTrigger.Enabled")
    ) {
      findings.push(serviceDefinitionPreserved(key, sourcePath));
    } else {
      unknown(key, "The key or value is not a recognized installer setting.");
    }
  }
  for (const node of expected.querySelectorAll("Task *")) {
    const key = elementKey(node);
    if (
      seen.has(key) ||
      node.children.length ||
      (!expectedXml && preserved.test(key)) ||
      (expectedXml && key === "Settings.Enabled") ||
      // Default leaf values do not imply that a missing trigger or principal exists.
      (nativeDefaults[key] === node.textContent &&
        (key.startsWith("Settings.") ||
          installed.querySelector(elementKey(node.parentElement!).replaceAll(".", " > "))))
    ) {
      continue;
    }
    if (key.startsWith("Principals.") || key.endsWith("UserId")) {
      unknown(key, "Installer identity field is missing.");
    } else {
      outdated(key, null, node.textContent);
    }
  }
  const launcher = installed.querySelector("Actions > Exec > Command")?.textContent;
  const launcherArguments = installed.querySelector("Actions > Exec > Arguments")?.textContent;
  const hiddenSelected = Boolean(
    launcher &&
    ((samePath(launcher, hiddenPath) && !launcherArguments) ||
      (["wscript.exe", getWindowsSystem32ExePath("wscript.exe")].some((candidate) =>
        samePath(candidate, launcher),
      ) &&
        launcherArguments === `"${hiddenPath}"`)),
  );
  if (!expectedXml) {
    const canonicalLauncher = expected.querySelector("Actions > Exec > Command")?.textContent;
    const canonicalArguments = expected.querySelector("Actions > Exec > Arguments")?.textContent;
    const workingDirectory = installed.querySelector(
      "Actions > Exec > WorkingDirectory",
    )?.textContent;
    const canonicalDirectory = expected.querySelector(
      "Actions > Exec > WorkingDirectory",
    )?.textContent;
    const sameDirectory =
      workingDirectory === canonicalDirectory ||
      Boolean(
        workingDirectory && canonicalDirectory && samePath(workingDirectory, canonicalDirectory),
      );
    const currentAction = Boolean(
      launcher &&
      canonicalLauncher &&
      samePath(launcher, canonicalLauncher) &&
      launcherArguments === canonicalArguments,
    );
    const legacyAction = Boolean(
      hiddenSelected || (launcher && samePath(launcher, sourcePath) && !launcherArguments),
    );
    if (!currentAction && !legacyAction) {
      unknown(
        "Actions.Exec.Command",
        "Native task points at an unrecognized launcher or arguments.",
      );
    } else if (!currentAction) {
      outdated("Actions.Exec.Command", launcher ?? null, canonicalLauncher ?? sourcePath);
    }
    if (!sameDirectory) {
      if (workingDirectory === undefined && legacyAction) {
        if (canonicalDirectory !== undefined) {
          outdated("Actions.Exec.WorkingDirectory", null, canonicalDirectory);
        }
      } else {
        unknown(
          "Actions.Exec.WorkingDirectory",
          "Native task uses an operator-owned working directory.",
        );
      }
    }
  }
  if (expectedCommand) {
    const command = await readScheduledTaskCommand(env, { requireEffective: true, timeoutMs });
    const normalize = (text: string) =>
      text
        .split(/\r?\n/u)
        .map((line) => line.trim())
        .filter((line) => {
          const comment = /^(?:rem |')(.+)$/iu.exec(line)?.[1];
          return (
            line &&
            !(comment && isInstallerServiceDescription(comment.trim(), env)) &&
            line !== 'set "OPENCLAW_WINDOWS_TASK_HIDDEN_LAUNCHER=1"' &&
            line !==
              'if not defined OPENCLAW_WINDOWS_TASK_HIDDEN_LAUNCHER set "OPENCLAW_WINDOWS_TASK_HIDDEN_LAUNCHER=cmd"'
          );
        })
        .join("\n")
        .replace(/(?: --task-supervisor)?(?:\s*<\s*NUL)?$/iu, "");
    const read = async (file: string) =>
      decodeWindowsLauncherScript({ buffer: await fs.readFile(file) });
    if (!command || normalize(await read(sourcePath)) !== normalize(buildTaskScript(command))) {
      unknown("TaskScript", "The generated task script contains unrecognized behavior.");
    }
    if (
      hiddenSelected ||
      ((!taskUser || env.OPENCLAW_SERVICE_KIND === "node") &&
        resolveTaskLauncherScriptPath({ ...env, ...expectedCommand.environment }, sourcePath) !==
          sourcePath)
    ) {
      const legacy = `CreateObject("WScript.Shell").Run """${sourcePath.replaceAll('"', '""')}""", 0, False`;
      // 2026.9.3 emitted this waiting launcher before the supervisor environment marker.
      const releasedWaiting = `WScript.Quit CreateObject("WScript.Shell").Run("""${sourcePath.replaceAll('"', '""')}""", 0, True)`;
      const generated = buildHiddenLauncherScript({
        scriptPath: sourcePath,
        taskSupervisor: command?.environment?.OPENCLAW_SERVICE_KIND === "gateway",
      });
      const installedLauncher = await read(hiddenPath).catch((error: unknown) => {
        if (!hiddenSelected && hasErrnoCode(error, "ENOENT")) {
          return undefined;
        }
        throw error;
      });
      if (
        installedLauncher !== undefined &&
        ![legacy, releasedWaiting, generated].some(
          (candidate) => normalize(candidate) === normalize(installedLauncher),
        )
      ) {
        unknown("TaskLauncher", "The generated task launcher contains unrecognized behavior.");
      }
    }
  }
  return xml;
}

/** Read-only admission; native publication still owns permissions, locking and CAS. */
export async function readScheduledTaskDefinitionMutationCapability(
  env: GatewayServiceEnv,
  options: Pick<GatewayServiceReadOptions, "timeoutMs"> & { environment?: GatewayServiceEnv } = {},
): Promise<ServiceDefinitionMutationCapability> {
  const unknown = { kind: "unknown", reason: "inspection-failed" } as const;
  const deadline =
    options.timeoutMs === undefined ? undefined : performance.now() + options.timeoutMs;
  const remaining = () => {
    const timeoutMs = deadline === undefined ? undefined : deadline - performance.now();
    if (timeoutMs !== undefined && (!Number.isFinite(timeoutMs) || timeoutMs <= 0)) {
      throw new Error("Scheduled Task definition inspection deadline expired.");
    }
    return timeoutMs;
  };
  try {
    if (!resolveTaskUser(env)) {
      return unknown;
    }
    const taskName = resolveTaskName(env);
    const observed = probeScheduledTaskState(taskName, remaining());
    if (observed.status === "unknown") {
      return unknown;
    }
    const readCommand = () =>
      readScheduledTaskCommand(env, {
        requireEffective: true,
        requireLoaded: true,
        timeoutMs: remaining(),
      });
    const command = await readCommand();
    if ((command === null) !== (observed.status === "missing")) {
      return unknown;
    }
    const scriptPath = resolveTaskScriptPath(env);
    if (
      command?.sourcePath &&
      path.win32.normalize(command.sourcePath).toLowerCase() !==
        path.win32.normalize(scriptPath).toLowerCase()
    ) {
      return unknown;
    }
    const paths = [
      ...new Set(
        [env, { ...env, ...options.environment }].flatMap((target) => {
          const script = resolveTaskScriptPath(target);
          return [
            script,
            resolveTaskLauncherScriptPath({ OPENCLAW_WINDOWS_TASK_HIDDEN_LAUNCHER: "1" }, script),
          ];
        }),
      ),
    ];
    for (const file of paths) {
      for (let parent = path.dirname(file); ; parent = path.dirname(parent)) {
        remaining();
        const entry = await fs.lstat(parent).catch((error: unknown) => {
          if (hasErrnoCode(error, "ENOENT")) {
            return null;
          }
          throw error;
        });
        if (entry && (!entry.isDirectory() || entry.isSymbolicLink())) {
          return unknown;
        }
        if (parent === path.dirname(parent)) {
          break;
        }
      }
    }
    const files = await Promise.all(paths.map(readServiceFileState));
    if (!command && files.some(Boolean)) {
      return unknown;
    }
    let xml: string | undefined;
    if (command) {
      const findings: ServiceDefinitionDrift[] = [];
      xml = await auditScheduledTaskDefinition(env, findings, remaining(), command);
      if (findings.some((finding) => finding.kind === "unknown-edit")) {
        return unknown;
      }
    }
    if (
      !isDeepStrictEqual(files, await Promise.all(paths.map(readServiceFileState))) ||
      !isDeepStrictEqual(command, await readCommand())
    ) {
      return unknown;
    }
    if (xml !== undefined) {
      const current = await execSchtasks(["/Query", "/TN", taskName, "/XML"]);
      if (
        current.code !== 0 ||
        current.stdout.replace(/^\uFEFF/u, "").replaceAll(String.fromCharCode(0), "") !== xml
      ) {
        return unknown;
      }
    } else if (probeScheduledTaskState(taskName, remaining()).status !== "missing") {
      return unknown;
    }
    remaining();
    return { kind: "writable" };
  } catch (error) {
    if (hasCommandProcessCleanupError(error)) {
      throw error;
    }
    return unknown;
  }
}
