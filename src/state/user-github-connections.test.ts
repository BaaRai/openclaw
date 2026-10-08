import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import {
  closeOpenClawStateDatabaseAsync,
  runOpenClawStateWriteTransaction,
} from "./openclaw-state-db.js";
import {
  disconnectedUserGitHubConnection,
  disconnectUserGitHubConnection,
  listUserGitHubConnections,
  observeUserGitHubProfileRetirement,
  readUserGitHubConnectionAsync,
  readUserGitHubConnection,
  replaceUserGitHubConnection,
  type UserGitHubConnection,
} from "./user-github-connections.js";
import {
  mutateUserGitHubConnectionInDatabase,
  mutateUserGitHubConnectionSync,
} from "./user-github-connections.kernel.js";
import type { UserGitHubMutation } from "./user-github-connections.worker-contract.js";
import { ensureProfileForEmail } from "./user-profiles.js";

const tempDirs = useAutoCleanupTempDirTracker((cleanup) => {
  afterAll(async () => {
    await closeOpenClawStateDatabaseAsync();
    cleanup();
  });
});
let options: { path: string };

beforeAll(() => {
  options = { path: join(tempDirs.make("user-github-worker-"), "openclaw.sqlite") };
});

describe("personal GitHub worker ownership", () => {
  it("observes foreign connections and refuses a replacement prepared before that commit", async () => {
    const owner = ensureProfileForEmail("freshness@example.test", options).id;
    const before = disconnectedUserGitHubConnection();
    await replaceUserGitHubConnection(
      owner,
      { expected: undefined, next: before },
      () => {},
      options,
    );
    expect(await readUserGitHubConnectionAsync(owner, options)).toEqual(before);
    expect(await listUserGitHubConnections(options)).toContainEqual({ owner, connection: before });
    const after = disconnectedUserGitHubConnection();
    const external = new DatabaseSync(options.path);
    try {
      external
        .prepare(
          "UPDATE secret_store_entries SET value = ? WHERE scope_kind = 'identity' AND scope_id = ? AND name = 'github-connection'",
        )
        .run(JSON.stringify(after), owner);
      expect(await readUserGitHubConnectionAsync(owner, options)).toEqual(after);
      expect(await listUserGitHubConnections(options)).toContainEqual({ owner, connection: after });
      await expect(
        replaceUserGitHubConnection(owner, { expected: before, next: before }, () => {}, options),
      ).rejects.toThrow("authorization changed");
      expect(await readUserGitHubConnectionAsync(owner, options)).toEqual(after);
    } finally {
      external.close();
    }
  });

  it("distinguishes absent and deleted credentials from corrupt credentials and noncanonical owners", async () => {
    const owner = ensureProfileForEmail("joined-read@example.test", options).id;
    const target = ensureProfileForEmail("joined-target@example.test", options).id;
    expect(readUserGitHubConnection(owner, options)).toBeUndefined();
    expect(() => readUserGitHubConnection(randomUUID(), options)).toThrow("owner changed");
    const connection = disconnectedUserGitHubConnection();
    await replaceUserGitHubConnection(
      owner,
      { expected: undefined, next: connection },
      () => {},
      options,
    );
    const external = new DatabaseSync(options.path);
    try {
      external
        .prepare(
          "UPDATE secret_store_entries SET allowed_hosts = '[]' WHERE scope_kind = 'identity' AND scope_id = ? AND name = 'github-connection'",
        )
        .run(owner);
      expect(() => readUserGitHubConnection(owner, options)).toThrow(
        "Personal GitHub state is invalid",
      );
      external
        .prepare(
          "UPDATE secret_store_entries SET deleted_at_ms = 1 WHERE scope_kind = 'identity' AND scope_id = ? AND name = 'github-connection'",
        )
        .run(owner);
      expect(readUserGitHubConnection(owner, options)).toBeUndefined();
      external.prepare("UPDATE user_profiles SET merged_into = ? WHERE id = ?").run(target, owner);
      expect(() => readUserGitHubConnection(owner, options)).toThrow("owner changed");
      expect(readUserGitHubConnection(target, options)).toBeUndefined();
    } finally {
      external.close();
    }
  });

  it("rolls back a disconnect whose live authority is lost at commit and publishes only settled retirement", async () => {
    const owner = ensureProfileForEmail("authority@example.test", options).id;
    const connected: UserGitHubConnection = {
      version: 1,
      generation: randomUUID(),
      selection: {
        kind: "connected",
        profileId: `ghp_${"a".repeat(32)}`,
        accountId: 101,
        login: "synthetic-user",
        refreshToken: "synthetic-refresh-token",
        scopes: ["repo"],
        accessExpiresAtMs: Date.now() + 60_000,
        refreshExpiresAtMs: Date.now() + 120_000,
      },
    };
    await replaceUserGitHubConnection(
      owner,
      { expected: undefined, next: connected },
      () => {},
      options,
    );
    const retired = vi.fn();
    const unobserve = observeUserGitHubProfileRetirement(retired);
    let guarded = 0;
    try {
      await expect(
        disconnectUserGitHubConnection(
          owner,
          (role) => {
            if (role && ++guarded === 2) {
              throw new Error("Synthetic request closed before commit");
            }
          },
          options,
        ),
      ).rejects.toThrow("Synthetic request closed before commit");
      expect(await readUserGitHubConnectionAsync(owner, options)).toEqual(connected);
      expect(retired).not.toHaveBeenCalled();
      await disconnectUserGitHubConnection(owner, () => {}, options);
      expect(retired).toHaveBeenCalledExactlyOnceWith({
        profileIds: [`ghp_${"a".repeat(32)}`],
        context: expect.objectContaining({
          admission: expect.objectContaining({ databasePath: options.path }),
        }),
      });
      expect((await readUserGitHubConnectionAsync(owner, options))?.selection.kind).toBe(
        "disconnected",
      );
    } finally {
      unobserve();
    }
  });
  it.each([
    { kind: "start", stage: "transaction" },
    { kind: "start", stage: "commit" },
    { kind: "pending", stage: "transaction" },
    { kind: "pending", stage: "commit" },
    { kind: "install", stage: "transaction" },
    { kind: "install", stage: "commit" },
  ] as const)(
    "rolls back $kind when authorization expires during the $stage grant",
    ({ kind, stage }) => {
      const now = Date.now();
      const expiry = now + 60_000;
      const owner = ensureProfileForEmail(`expiry-${kind}-${stage}@example.test`, options).id;
      const profileId = `ghp_${"d".repeat(32)}`;
      const authorization = {
        kind: "device" as const,
        requestId: randomUUID(),
        deviceCode: "d".repeat(40),
        userCode: "ABCD-1234",
        verificationUri: "https://github.com/login/device" as const,
        createdAtMs: now,
        expiresAtMs: expiry,
        pollIntervalMs: 1_000,
        nextPollAtMs: now,
        candidate: {
          profileId,
          receivedAtMs: now,
          tokens: {
            accessToken: "synthetic-expiry-access",
            refreshToken: "synthetic-expiry-refresh",
            tokenType: "bearer" as const,
            scopes: ["repo"],
            expiresInSeconds: 3_600,
            refreshTokenExpiresInSeconds: 7_200,
          },
        },
      };
      const record: UserGitHubConnection = {
        ...disconnectedUserGitHubConnection(),
        pending: authorization,
      };
      mutateUserGitHubConnectionSync(
        { kind: "replace", owner, expected: undefined, next: record },
        () => {},
        options,
      );
      const mutation: UserGitHubMutation =
        kind === "start"
          ? {
              kind,
              owner,
              pending: {
                kind: "starting",
                requestId: randomUUID(),
                createdAtMs: now,
                expiresAtMs: expiry,
              },
            }
          : kind === "pending"
            ? {
                kind,
                owner,
                generation: record.generation,
                expectedPending: authorization,
                pending: { ...authorization, nextPollAtMs: now + 1_000 },
              }
            : {
                kind,
                owner,
                generation: record.generation,
                requestId: authorization.requestId,
                profileId,
                account: { accountId: 101, login: "synthetic-user" },
              };
      vi.useFakeTimers({ toFake: ["Date"] });
      vi.setSystemTime(now);
      try {
        expect(() =>
          runOpenClawStateWriteTransaction(
            ({ db }) =>
              mutateUserGitHubConnectionInDatabase(
                db,
                mutation,
                (grant) => {
                  if (grant === stage) {
                    vi.setSystemTime(expiry);
                  }
                },
                "worker",
              ),
            options,
          ),
        ).toThrow("authorization changed or expired");
        expect(readUserGitHubConnection(owner, options)).toEqual(record);
      } finally {
        vi.useRealTimers();
      }
    },
  );
});
