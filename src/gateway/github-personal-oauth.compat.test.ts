import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { expect, expectTypeOf, it, vi } from "vitest";
import { awaitGateBeforeSettlement } from "../../test/helpers/promise.js";
import * as githubOAuthClient from "../agents/github-oauth-client.js";
import {
  resolveManagedGitHubProfileDir,
  resolveManagedGitHubProfileRoot,
} from "../agents/github-tool-identity.js";
import * as managedProfiles from "../agents/github-tool-identity.js";
import { createDeferredCore } from "../shared/deferred.js";
import { openOpenClawStateDatabase } from "../state/openclaw-state-db.js";
import {
  disconnectedUserGitHubConnection,
  readUserGitHubConnection,
  replaceUserGitHubConnection,
  updateUserGitHubRefresh,
  type UserGitHubConnection,
} from "../state/user-github-connections.js";
import { ensureProfileForEmail } from "../state/user-profiles.js";
import { setTestEnvValue } from "../test-utils/env.js";
import { createOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import * as githubCli from "./github-cli-preflight.js";
import * as deviceFlow from "./github-oauth-device-flow.js";
import { createPersonalGitHubOAuthLifecycle } from "./github-personal-oauth.js";
type ReleasedAction = { owner: string; assertCurrent: () => void };

it("preserves the released plain-action SDK and synchronous cancel/disconnect results", async () => {
  const state = await createOpenClawTestState({ scenario: "minimal", applyEnv: true });
  const service = createPersonalGitHubOAuthLifecycle();
  try {
    const owner = ensureProfileForEmail("github-sdk@example.test").id;
    let live = true;
    const action: ReleasedAction = {
      owner,
      assertCurrent() {
        if (!live) {
          throw new Error("Synthetic SDK authority closed");
        }
      },
    };
    expectTypeOf<typeof service.startAuthorization>().parameter(0).toEqualTypeOf<ReleasedAction>();
    expectTypeOf<typeof service.pollAuthorization>().parameter(0).toEqualTypeOf<ReleasedAction>();
    const requestId = randomUUID();
    const now = Date.now();
    await replaceUserGitHubConnection(
      owner,
      {
        expected: undefined,
        next: {
          ...disconnectedUserGitHubConnection(),
          pending: { kind: "starting", requestId, createdAtMs: now, expiresAtMs: now + 60_000 },
        },
      },
      action.assertCurrent,
    );
    expect(service.cancelAuthorization(action, requestId)).toBe(true);
    expect(service.cancelAuthorization(action, requestId)).toBe(false);
    const generation = readUserGitHubConnection(owner)?.generation;
    expect(service.disconnect(action)).toBeUndefined();
    expect(readUserGitHubConnection(owner)?.generation).not.toBe(generation);
    live = false;
    expect(() => service.disconnect(action)).toThrow("Synthetic SDK authority closed");
  } finally {
    await service.stop();
    await state.cleanup();
  }
});

it("keeps orphan retirement on its originating store when the environment changes during the scan", async () => {
  const state = await createOpenClawTestState({ scenario: "minimal", applyEnv: true });
  const service = createPersonalGitHubOAuthLifecycle();
  const scanned = createDeferredCore();
  const release = createDeferredCore();
  let maintenance: Promise<void> | undefined;
  const readdir = fs.readdir;
  let restoreScan: (() => void) | undefined;
  try {
    openOpenClawStateDatabase();
    const replacement = state.path("replacement");
    const replacementEnv = { ...state.env, OPENCLAW_STATE_DIR: replacement };
    openOpenClawStateDatabase({ env: replacementEnv });
    const profileId = `ghp_${"b".repeat(32)}`;
    const sourceRoot = resolveManagedGitHubProfileRoot({
      agentId: "",
      scope: "personal",
      env: state.env,
    });
    const sourceProfile = resolveManagedGitHubProfileDir({
      agentId: "",
      scope: "personal",
      profileId,
      env: state.env,
    });
    const replacementProfile = resolveManagedGitHubProfileDir({
      agentId: "",
      scope: "personal",
      profileId,
      env: replacementEnv,
    });
    await fs.mkdir(sourceProfile, { recursive: true });
    await fs.writeFile(path.join(sourceProfile, "marker"), "source");
    await fs.cp(sourceProfile, replacementProfile, { recursive: true });
    const scan = vi.spyOn(fs, "readdir").mockImplementation(async (...args) => {
      const entries = await readdir(...args);
      if (String(args[0]) === sourceRoot) {
        scanned.resolve();
        await release.promise;
      }
      return entries;
    });
    restoreScan = () => scan.mockRestore();
    maintenance = service.maintain();
    await awaitGateBeforeSettlement(
      scanned.promise,
      maintenance,
      "Maintenance did not enter its profile scan",
    );
    setTestEnvValue("OPENCLAW_STATE_DIR", replacement);
    release.resolve();
    await maintenance;
    await expect(fs.stat(sourceProfile)).rejects.toMatchObject({ code: "ENOENT" });
    expect(await fs.readFile(path.join(replacementProfile, "marker"), "utf8")).toBe("source");
  } finally {
    release.resolve();
    await maintenance?.catch(() => {});
    restoreScan?.();
    setTestEnvValue("OPENCLAW_STATE_DIR", state.stateDir);
    await service.stop();
    await state.cleanup();
  }
});

it.each(["start", "poll"] as const)(
  "preserves a concurrent token refresh while personal authorization %s awaits GitHub",
  async (phase) => {
    const state = await createOpenClawTestState({ scenario: "minimal", applyEnv: true });
    const service = createPersonalGitHubOAuthLifecycle();
    const entered = createDeferredCore();
    const release = createDeferredCore();
    const now = Date.now();
    const device = {
      deviceCode: "d".repeat(40),
      userCode: "ABCD-1234",
      verificationUri: "https://github.com/login/device" as const,
      createdAtMs: now,
      expiresAtMs: now + 900_000,
      pollIntervalMs: 1_000,
      nextPollAtMs: now,
    };
    const tokens = {
      accessToken: "synthetic-new-authorization",
      refreshToken: "synthetic-new-refresh",
      tokenType: "bearer" as const,
      scopes: ["repo"],
      expiresInSeconds: 3_600,
      refreshTokenExpiresInSeconds: 7_200,
    };
    const cli = vi.spyOn(githubCli, "assertGitHubCliAvailable").mockReturnValue(undefined);
    const start = vi.spyOn(deviceFlow, "startGitHubDeviceFlow").mockImplementation(async () => {
      if (phase === "start") {
        entered.resolve();
        await release.promise;
      }
      return device;
    });
    const poll = vi.spyOn(deviceFlow, "pollGitHubDeviceFlow").mockImplementation(async () => {
      entered.resolve();
      await release.promise;
      return { kind: "authorized", tokens };
    });
    let pending: Promise<unknown> | undefined;
    let selectionBeforeInstall: UserGitHubConnection["selection"] | undefined;
    const owner = ensureProfileForEmail("personal-concurrent-refresh@example.test").id;
    const install = vi
      .spyOn(managedProfiles, "installManagedGitHubProfile")
      .mockImplementation(async (input) => {
        input.assertCurrent?.();
        selectionBeforeInstall = readUserGitHubConnection(owner)?.selection;
        const account = { accountId: 102, login: "synthetic-next-user", avatarUrl: null };
        await input.commitConfig(account);
        return account;
      });
    try {
      const profileId = `ghp_${"c".repeat(32)}`;
      const operationId = randomUUID();
      await replaceUserGitHubConnection(
        owner,
        {
          expected: undefined,
          next: {
            ...disconnectedUserGitHubConnection(),
            selection: {
              kind: "connected",
              profileId,
              accountId: 101,
              login: "synthetic-current-user",
              refreshToken: "synthetic-original-refresh",
              scopes: ["repo"],
              accessExpiresAtMs: now + 60_000,
              refreshExpiresAtMs: now + 120_000,
              refresh: { operationId },
            },
          },
        },
        () => {},
      );
      const action = { owner, assertCurrent() {}, assertMutationCurrent() {} };
      if (phase === "start") {
        pending = service.startAuthorizationAsync(action);
      } else {
        const { requestId } = await service.startAuthorizationAsync(action);
        pending = service.pollAuthorizationAsync(action, requestId);
      }
      await awaitGateBeforeSettlement(
        entered.promise,
        pending,
        "Personal authorization did not enter its provider exchange",
      );
      await expect(
        updateUserGitHubRefresh({
          owner,
          profileId,
          operationId,
          update: {
            kind: "rotated",
            receivedAtMs: now,
            tokens: { ...tokens, refreshToken: "synthetic-rotated-existing-refresh" },
          },
        }),
      ).resolves.toBe(true);
      release.resolve();
      if (phase === "start") {
        expect(await pending).toMatchObject({ userCode: "ABCD-1234" });
        expect(readUserGitHubConnection(owner)?.selection).toMatchObject({
          refreshToken: "synthetic-rotated-existing-refresh",
        });
      } else {
        expect(await pending).toMatchObject({ status: "success" });
        expect(selectionBeforeInstall).toMatchObject({
          refreshToken: "synthetic-rotated-existing-refresh",
        });
        expect(readUserGitHubConnection(owner)?.selection).toMatchObject({
          accountId: 102,
          refreshToken: tokens.refreshToken,
        });
      }
    } finally {
      release.resolve();
      await pending?.catch(() => {});
      await service.stop();
      install.mockRestore();
      poll.mockRestore();
      start.mockRestore();
      cli.mockRestore();
      await state.cleanup();
    }
  },
);

it("continues maintenance after a preceding refresh changes an expired pending connection", async () => {
  const state = await createOpenClawTestState({ scenario: "minimal", applyEnv: true });
  const service = createPersonalGitHubOAuthLifecycle();
  const entered = createDeferredCore();
  const release = createDeferredCore();
  let maintenance: Promise<void> | undefined;
  const now = Date.now();
  const tokens = {
    accessToken: "synthetic-maintenance-access",
    refreshToken: "synthetic-maintenance-rotated",
    tokenType: "bearer" as const,
    scopes: ["repo"],
    expiresInSeconds: 3_600,
    refreshTokenExpiresInSeconds: 7_200,
  };
  const refresh = vi
    .spyOn(githubOAuthClient, "refreshGitHubOAuthToken")
    .mockImplementation(async ({ refreshToken }) => {
      if (refreshToken === "synthetic-first-refresh") {
        entered.resolve();
        await release.promise;
      }
      return { status: "refreshed", tokens };
    });
  const materialize = vi
    .spyOn(managedProfiles, "refreshManagedGitHubProfile")
    .mockImplementation(async (input) => {
      input.assertCurrent?.();
      return { accountId: input.expectedAccountId, login: "synthetic-maintained", avatarUrl: null };
    });
  try {
    const owners = ["first", "second", "third"]
      .map((name) => ensureProfileForEmail(`maintenance-${name}@example.test`).id)
      .toSorted();
    const first = owners[0];
    const second = owners[1];
    const third = owners[2];
    if (!first || !second || !third) {
      throw new Error("Expected three maintenance owners");
    }
    const connected = (id: string, refreshToken: string): UserGitHubConnection => ({
      ...disconnectedUserGitHubConnection(),
      selection: {
        kind: "connected",
        profileId: `ghp_${id.repeat(32)}`,
        accountId: 101,
        login: "synthetic-user",
        refreshToken,
        scopes: ["repo"],
        accessExpiresAtMs: now - 1,
        refreshExpiresAtMs: now + 7_200_000,
      },
    });
    const expired: UserGitHubConnection = {
      ...disconnectedUserGitHubConnection(),
      pending: {
        kind: "starting",
        requestId: randomUUID(),
        createdAtMs: now - 60_000,
        expiresAtMs: now - 1,
      },
    };
    for (const [owner, next] of [
      [first, connected("e", "synthetic-first-refresh")],
      [second, expired],
      [third, connected("f", "synthetic-third-refresh")],
    ] as const) {
      await replaceUserGitHubConnection(owner, { expected: undefined, next }, () => {});
    }
    maintenance = service.maintain();
    await awaitGateBeforeSettlement(
      entered.promise,
      maintenance,
      "Maintenance did not enter the first refresh",
    );
    const currentSelection = {
      ...connected("a", "synthetic-second-fresh").selection,
      accessExpiresAtMs: now + 3_600_000,
    };
    await replaceUserGitHubConnection(
      second,
      { expected: expired, next: { ...expired, selection: currentSelection } },
      () => {},
    );
    release.resolve();
    await expect(maintenance).resolves.toBeUndefined();
    expect(readUserGitHubConnection(second)).toMatchObject({ selection: currentSelection });
    expect(readUserGitHubConnection(second)?.pending).toBeUndefined();
    expect(refresh.mock.calls.map(([input]) => input.refreshToken)).toEqual([
      "synthetic-first-refresh",
      "synthetic-third-refresh",
    ]);
    expect(readUserGitHubConnection(third)?.selection).toMatchObject({
      refreshToken: tokens.refreshToken,
    });
  } finally {
    release.resolve();
    await maintenance?.catch(() => {});
    await service.stop();
    materialize.mockRestore();
    refresh.mockRestore();
    await state.cleanup();
  }
});
