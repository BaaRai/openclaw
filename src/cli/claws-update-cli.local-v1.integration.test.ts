import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { stableStringify } from "@openclaw/normalization-core";
import { afterEach, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import { readClawStatus } from "../claws/lifecycle-state.js";
import { buildClawAddPlan } from "../claws/lifecycle.js";
import { persistClawInstallRecord, readClawInstallRecord } from "../claws/provenance.js";
import { readClawManifestFile } from "../claws/reader.js";
import type { ClawUpdatePlan } from "../claws/update-plan.js";
import { clearRuntimeConfigSnapshot, setRuntimeConfigSnapshot } from "../config/config.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import {
  closeOpenClawStateDatabaseAsync,
  closeOpenClawStateDatabaseForTest,
  openOpenClawStateDatabase,
} from "../state/openclaw-state-db.js";
import { runClawsUpdateCommand } from "./claws-update-cli.runtime.js";

// Lease-broker admission is covered separately; retain real CLI planning, apply, and SQLite.
vi.mock("../state/openclaw-state-lease.js", async () => ({
  ...(await vi.importActual<typeof import("../state/openclaw-state-lease.js")>(
    "../state/openclaw-state-lease.js",
  )),
  withOpenClawStateLease: async (
    _options: unknown,
    run: (lease: { assertOwned: () => void }) => Promise<unknown>,
  ) => await run({ assertOwned: () => undefined }),
}));

const tempDirs = useAutoCleanupTempDirTracker((cleanup) =>
  afterEach(async () => {
    clearRuntimeConfigSnapshot();
    vi.unstubAllEnvs();
    await closeOpenClawStateDatabaseAsync();
    closeOpenClawStateDatabaseForTest();
    cleanup();
  }),
);

it("keeps old Add consent during preview and host settings during local Update", async () => {
  const root = tempDirs.make("openclaw-claw-v1-local-update-");
  const stateDir = join(root, "state");
  const configPath = join(root, "openclaw.json");
  const workspace = join(root, "workspace");
  const packageRoot = join(root, "package");
  const profilePath = join(packageRoot, "profiles", "openclaw.yml");
  const manifestPath = join(packageRoot, "openclaw.claw.json");
  const env = { OPENCLAW_STATE_DIR: stateDir };
  for (const [key, value] of Object.entries({
    HOME: root,
    OPENCLAW_HOME: root,
    OPENCLAW_STATE_DIR: stateDir,
    OPENCLAW_CONFIG_PATH: configPath,
  })) {
    vi.stubEnv(key, value);
  }
  await mkdir(join(packageRoot, "profiles"), { recursive: true });
  await writeFile(manifestPath, JSON.stringify({ schemaVersion: 1, agent: { id: "worker" } }));
  const profileBytes = Buffer.from(
    [
      "schemaVersion: 1",
      "agent:",
      "  model: { primary: acme/packaged }",
      "  subagents: { allowAgents: [researcher], delegationMode: prefer }",
      "  tools: { allow: [read] }",
      "",
    ].join("\n"),
  );
  await writeFile(profilePath, profileBytes);

  const fresh = await readClawManifestFile(manifestPath);
  expect(fresh).toMatchObject({
    ok: false,
    diagnostics: [expect.objectContaining({ code: "legacy_openclaw_profile_requires_conversion" })],
  });
  const released = await readClawManifestFile(manifestPath, {
    authorizeLegacyLocalUpdateHostSettings: () => true,
  });
  if (!released.ok) {
    throw new Error("expected released local package to parse through the compatibility reader");
  }
  const addPlan = await buildClawAddPlan({
    manifest: released.manifest,
    openClawProfile: released.openClawProfile,
    source: released.source,
    context: { workspace },
  });
  expect(addPlan.blockers).toEqual([]);
  await mkdir(workspace);
  persistClawInstallRecord(addPlan, { env, nowMs: 1 });

  const operatorModel = { primary: "acme/operator" };
  const operatorSubagents = { allowAgents: ["researcher"], delegationMode: "suggest" as const };
  const installedAgent = {
    ...addPlan.agent.config,
    model: operatorModel,
    subagents: operatorSubagents,
  };
  const { id: _id, ...agentEntry } = installedAgent;
  const config: OpenClawConfig = {
    gateway: { controlUi: { experimental: { claws: true } } },
    agents: { ownership: "explicit", entries: { worker: agentEntry, researcher: {} } },
  };
  await writeFile(configPath, JSON.stringify(config));
  setRuntimeConfigSnapshot(config);

  // The stable release recorded the complete agent and its original Add consent.
  const oldDigest = `sha256:${createHash("sha256").update(stableStringify(installedAgent)).digest("hex")}`;
  const oldConsent = `sha256:${"a".repeat(64)}`;
  openOpenClawStateDatabase({ env })
    .db /* sqlite-allow-raw: test-only released-record fixture. */
    .prepare(
      "UPDATE claw_installs SET agent_config_digest = ?, plan_integrity = ? WHERE agent_id = ?",
    )
    .run(oldDigest, oldConsent, "worker");
  expect(readClawInstallRecord("worker", { env })).toMatchObject({
    claw: { integrityKind: "development-snapshot", integrity: released.source.integrity },
    agentConfigDigest: oldDigest,
    planIntegrity: oldConsent,
  });
  await expect(
    readClawStatus("worker", { env, config, readOnly: true, sourceMcpServers: {} }),
  ).resolves.toMatchObject({ records: [{ agentState: "present" }] });

  const runtime = { log: vi.fn(), error: vi.fn(), exit: vi.fn() };
  await runClawsUpdateCommand("worker", { dryRun: true, json: true }, runtime);
  const preview = JSON.parse(String(runtime.log.mock.calls.at(-1)?.[0])) as ClawUpdatePlan;
  expect(preview).toMatchObject({ blockers: [] });
  expect(runtime.exit).not.toHaveBeenCalled();
  expect(preview.diagnostics).toEqual(
    expect.arrayContaining([
      expect.objectContaining({ code: "legacy_openclaw_model_ignored" }),
      expect.objectContaining({ code: "legacy_openclaw_subagents_ignored" }),
    ]),
  );
  expect(preview.actions).toContainEqual(
    expect.objectContaining({ kind: "agent", action: "change", blocked: false }),
  );
  expect(readClawInstallRecord("worker", { env })).toMatchObject({
    agentConfigDigest: oldDigest,
    planIntegrity: oldConsent,
  });
  expect(await readFile(profilePath)).toEqual(profileBytes);

  runtime.log.mockClear();
  await runClawsUpdateCommand(
    "worker",
    { yes: true, planIntegrity: preview.planIntegrity, json: true },
    runtime,
  );
  const applied = JSON.parse(String(runtime.log.mock.calls.at(-1)?.[0]));
  expect(applied.error).toBeUndefined();
  expect(applied).toMatchObject({ status: "complete", agentId: "worker" });
  expect(runtime.exit).not.toHaveBeenCalled();
  expect(JSON.parse(await readFile(configPath, "utf8"))).toMatchObject({
    agents: { entries: { worker: { model: operatorModel, subagents: operatorSubagents } } },
  });
  expect(readClawInstallRecord("worker", { env })).toMatchObject({
    claw: { integrity: released.source.integrity },
    status: "complete",
    planIntegrity: expect.not.stringMatching(oldConsent),
  });
  expect(await readFile(profilePath)).toEqual(profileBytes);

  runtime.log.mockClear();
  await runClawsUpdateCommand("worker", { from: manifestPath, dryRun: true, json: true }, runtime);
  expect(runtime.exit).toHaveBeenCalledWith(1);
  expect(JSON.parse(String(runtime.log.mock.calls.at(-1)?.[0]))).toMatchObject({
    valid: false,
    diagnostics: [expect.objectContaining({ code: "legacy_openclaw_profile_requires_conversion" })],
  });
});
