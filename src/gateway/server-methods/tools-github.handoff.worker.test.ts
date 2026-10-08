import { afterAll, beforeAll, expect, it, vi } from "vitest";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { writeSecretStoreEntries } from "../../secrets/store/secret-store-mutations.js";
import {
  closeOpenClawStateDatabaseAsync,
  openOpenClawStateDatabase,
} from "../../state/openclaw-state-db.js";
import {
  createOpenClawTestState,
  type OpenClawTestState,
} from "../../test-utils/openclaw-test-state.js";

// mock-isolation: Keep external GitHub effects outside the real setup-handoff transaction fixture.
vi.mock("../../agents/github-tool-identity.js", () => ({
  createManagedGitHubProfileId: () => `ghp_${"1".repeat(32)}`,
  resolveConfiguredGitHubToolIdentity: () => undefined,
  resolveManagedGitHubProfileDir: () => "/synthetic-profile",
  resolveGitHubToolIdentityStatus: async () => ({ state: "synthetic-configured" }),
  installManagedGitHubProfile: async (input: {
    assertCurrent: () => void;
    commitConfig: (account: { accountId: number; login: string }) => Promise<void>;
  }) => {
    input.assertCurrent();
    await input.commitConfig({ accountId: 1, login: "synthetic-user" });
  },
}));
// mock-isolation: The fixture tests secret consumption and opaque guards, not config persistence.
vi.mock("../github-tool-identity-config.js", () => ({
  updateGitHubToolIdentityConfig: async () => ({ agents: { entries: { main: {} } } }),
}));

import { createGatewayRequestContext } from "../server-request-context.js";
import { makeContextParams } from "../server-request-context.test-support.js";
import { toolsGitHubHandlers } from "./tools-github.js";

let state: OpenClawTestState;
beforeAll(async () => {
  state = await createOpenClawTestState({ prefix: "github-sdk-handoff-", applyEnv: true });
});
afterAll(async () => {
  await closeOpenClawStateDatabaseAsync();
  await state.cleanup();
});

it("consumes a setup handoff beside an opaque SDK guard that writes to the same store", async () => {
  const name = `github-setup-${"8".repeat(32)}`;
  await writeSecretStoreEntries({
    scope: { kind: "team" },
    updatedBy: "fixture",
    entries: [
      { name, kind: "secret", value: "synthetic-opaque-sdk-token" },
      { name: "OPAQUE_GUARD_MARKER", kind: "env", value: "0" },
    ],
  });
  const { db } = openOpenClawStateDatabase();
  // An incorrectly worker-held transaction must fail immediately, not wait on its own host guard.
  db.exec("PRAGMA busy_timeout = 0");
  const config: OpenClawConfig = { agents: { entries: { main: {} } } };
  const context = createGatewayRequestContext(makeContextParams());
  context.getRuntimeConfig = () => config;
  const respond = vi.fn();
  const handler = toolsGitHubHandlers["tools.github.configure"];
  if (!handler) {
    throw new Error("Missing GitHub configure handler");
  }
  await handler({
    params: { scope: "system", agentId: "main", mode: "managed", secretName: name },
    respond,
    context,
    client: null,
    req: { type: "req", id: "opaque-github", method: "tools.github.configure" },
    isWebchatConnect: () => false,
    sessionMutationCommitGuard: () => {
      db.prepare(
        "UPDATE secret_store_entries SET value = CAST(value AS INTEGER) + 1 WHERE scope_kind = 'team' AND scope_id = '' AND name = 'OPAQUE_GUARD_MARKER'",
      ).run();
    },
  });
  expect(respond).toHaveBeenCalledExactlyOnceWith(true, { state: "synthetic-configured" });
  expect(
    db.prepare("SELECT name FROM secret_store_entries WHERE name = ?").get(name),
  ).toBeUndefined();
  const marker = db
    .prepare(
      "SELECT CAST(value AS INTEGER) AS count FROM secret_store_entries WHERE name = 'OPAQUE_GUARD_MARKER'",
    )
    .get();
  expect(marker?.count).toBeGreaterThan(0);
});
