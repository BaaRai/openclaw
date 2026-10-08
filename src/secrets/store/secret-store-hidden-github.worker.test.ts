import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { requireNodeSqlite } from "../../infra/node-sqlite.js";
import { isSecretValueRegisteredForRedaction } from "../../logging/secret-redaction-registry.js";
import {
  closeOpenClawStateDatabaseAsync,
  openOpenClawStateDatabase,
} from "../../state/openclaw-state-db.js";
import {
  createOpenClawTestState,
  type OpenClawTestState,
} from "../../test-utils/openclaw-test-state.js";
import {
  consumeGitHubSetupHandoff,
  deleteHiddenGitHubSecretRecord,
  listHiddenGitHubSecretRecords,
  readHiddenGitHubSecretRecord,
  writeHiddenGitHubSecretRecord,
} from "./secret-store-hidden-github.js";
import { writeSecretStoreEntry } from "./secret-store-mutations.js";

let state: OpenClawTestState;
beforeAll(async () => {
  state = await createOpenClawTestState({ prefix: "github-hidden-worker-", applyEnv: true });
  // Same-version databases may legitimately predate the additive secret table.
  openOpenClawStateDatabase().db.exec("DROP TABLE secret_store_entries");
  await closeOpenClawStateDatabaseAsync();
});
afterAll(async () => {
  vi.restoreAllMocks();
  await closeOpenClawStateDatabaseAsync();
  await state.cleanup();
});

it("settles hidden records and single-use handoffs without caller-thread SQL", async () => {
  const { DatabaseSync, StatementSync } = requireNodeSqlite();
  const spies = [
    vi.spyOn(DatabaseSync.prototype, "exec"),
    vi.spyOn(DatabaseSync.prototype, "prepare"),
    vi.spyOn(StatementSync.prototype, "get"),
    vi.spyOn(StatementSync.prototype, "all"),
    vi.spyOn(StatementSync.prototype, "run"),
    vi.spyOn(StatementSync.prototype, "iterate"),
  ];
  try {
    const name = `github-device-${"1".repeat(32)}`;
    const setup = `github-setup-${"1".repeat(32)}`;
    await writeHiddenGitHubSecretRecord({ name, value: "synthetic-device", expectedValue: null });
    expect(await readHiddenGitHubSecretRecord({ name })).toBe("synthetic-device");
    expect(await listHiddenGitHubSecretRecords({ prefix: "github-device" })).toEqual([
      { name, value: "synthetic-device" },
    ]);
    expect(isSecretValueRegisteredForRedaction("synthetic-device")).toBe(true);
    await writeSecretStoreEntry({
      scope: { kind: "team" },
      name: setup,
      value: "synthetic-setup",
      kind: "secret",
      updatedBy: "fixture",
    });
    const consumed = await Promise.all([
      consumeGitHubSetupHandoff({ name: setup }),
      consumeGitHubSetupHandoff({ name: setup }),
    ]);
    expect(consumed.filter((value) => value === "synthetic-setup")).toHaveLength(1);
    expect(consumed.filter((value) => value === undefined)).toHaveLength(1);
    expect(await deleteHiddenGitHubSecretRecord({ name, expectedValue: "synthetic-device" })).toBe(
      true,
    );
    for (const spy of spies) {
      expect(spy).not.toHaveBeenCalled();
    }
  } finally {
    for (const spy of spies) {
      spy.mockRestore();
    }
  }
});

it("observes foreign commits on the next read and rejects stale writes and cleanup", async () => {
  const name = `github-oauth-${"2".repeat(32)}`;
  await writeHiddenGitHubSecretRecord({ name, value: "synthetic-before", expectedValue: null });
  expect(await listHiddenGitHubSecretRecords({ prefix: "github-oauth" })).toEqual([
    { name, value: "synthetic-before" },
  ]);
  const { DatabaseSync } = requireNodeSqlite();
  const foreign = new DatabaseSync(openOpenClawStateDatabase().path);
  try {
    foreign
      .prepare("UPDATE secret_store_entries SET value = ? WHERE name = ?")
      .run("synthetic-foreign", name);
    expect(await readHiddenGitHubSecretRecord({ name })).toBe("synthetic-foreign");
    expect(await listHiddenGitHubSecretRecords({ prefix: "github-oauth" })).toEqual([
      { name, value: "synthetic-foreign" },
    ]);
    expect(
      await writeHiddenGitHubSecretRecord({
        name,
        value: "synthetic-stale",
        expectedValue: "synthetic-before",
      }),
    ).toBe(false);
    expect(await deleteHiddenGitHubSecretRecord({ name, expectedValue: "synthetic-before" })).toBe(
      false,
    );
    expect(await readHiddenGitHubSecretRecord({ name })).toBe("synthetic-foreign");
  } finally {
    foreign.close();
  }
});

it("refuses a handoff when host authority is withdrawn before write admission", async () => {
  const name = `github-setup-${"3".repeat(32)}`;
  await writeSecretStoreEntry({
    scope: { kind: "team" },
    name,
    value: "synthetic-retained",
    kind: "secret",
    updatedBy: "fixture",
  });
  let current = true;
  const consuming = consumeGitHubSetupHandoff({
    name,
    assertCurrent: () => {
      if (!current) {
        throw new Error("Synthetic caller revoked");
      }
    },
  });
  current = false;
  await expect(consuming).rejects.toThrow("Synthetic caller revoked");
  expect(await consumeGitHubSetupHandoff({ name })).toBe("synthetic-retained");
});
