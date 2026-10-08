import { randomUUID } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";
import { isDeepStrictEqual } from "node:util";
import { safeParseJson } from "@openclaw/normalization-core/json-coercion";
import { z } from "zod";
import { cloneEnvWithPlatformSemantics } from "../config/config-env-vars.js";
import {
  executeSqliteQuerySync,
  executeSqliteQueryTakeFirstSync,
  getNodeSqliteKysely,
} from "../infra/kysely-sync.js";
import { deferSqlitePostCommitPublication } from "../infra/sqlite-post-commit.js";
import { registerSecretValueForRedaction } from "../logging/secret-redaction-registry.js";
import {
  PersonalGitHubStateError,
  decodePersonalGitHubSecret,
  readPersonalGitHubSecret,
  writePersonalGitHubSecret,
} from "../secrets/store/secret-store-hidden-github.kernel.js";
import { isMissingSecretStoreTableError } from "../secrets/store/secret-store-sqlite.js";
import {
  githubOAuthTimestamp as timestamp,
  githubOAuthSecret as secret,
  githubOAuthProfileId as profileId,
  githubOAuthScopes as scopes,
  githubOAuthRefreshFields,
  githubOAuthDeviceFields,
  validGitHubDeviceTiming,
} from "../shared/github-oauth-values.js";
import { registerListener } from "../shared/listeners.js";
import { readTrackedStateDatabaseIdentity } from "./openclaw-state-db-handle.js";
import { ensureSecretStoreSchema } from "./openclaw-state-db-schema-additive.js";
import { tableExists } from "./openclaw-state-db-schema-helpers.js";
import type { DB } from "./openclaw-state-db.generated.js";
import {
  openOpenClawStateDatabase,
  runOpenClawStateWriteTransaction,
  type OpenClawStateDatabaseOptions,
} from "./openclaw-state-db.js";
import { resolveOpenClawStateDirForDatabasePath } from "./openclaw-state-db.paths.js";
import { readOpenClawStateLeaseExpiry } from "./openclaw-state-lease-store.js";
import { captureOpenClawStateWorkerContext } from "./openclaw-state-worker-context.js";
import type { OpenClawStateWorkerContext } from "./openclaw-state-worker-context.types.js";
import type {
  UserGitHubCommit,
  UserGitHubMutation,
  UserGitHubRole,
} from "./user-github-connections.worker-contract.js";
import { selectUserProfileGitHubIdentities } from "./user-profile-github-identity.js";
import { selectResolvedUserProfileMetadataById } from "./user-profiles-internal.js";
import type { UserProfilesDatabase } from "./user-profiles.types.js";

const tokenPair = z.strictObject({
  accessToken: secret,
  refreshToken: secret,
  tokenType: z.literal("bearer"),
  scopes,
  expiresInSeconds: z
    .number()
    .int()
    .positive()
    .max(366 * 86400),
  refreshTokenExpiresInSeconds: z
    .number()
    .int()
    .positive()
    .max(366 * 86400),
});
const deviceFields = {
  requestId: z.string().uuid(),
  createdAtMs: timestamp,
  expiresAtMs: timestamp,
};
const device = z.strictObject({
  ...deviceFields,
  kind: z.literal("device"),
  ...githubOAuthDeviceFields,
  candidate: z.strictObject({ profileId, tokens: tokenPair, receivedAtMs: timestamp }).optional(),
});
const connected = z.strictObject({
  kind: z.literal("connected"),
  profileId,
  ...githubOAuthRefreshFields,
  refreshFailure: z.enum(["expired", "failed"]).optional(),
  refresh: z
    .strictObject({
      operationId: z.string().uuid(),
      tokens: tokenPair.optional(),
      receivedAtMs: timestamp.optional(),
    })
    .optional(),
});
const connectionSchema = z
  .strictObject({
    version: z.literal(1),
    generation: z.string().uuid(),
    selection: z.discriminatedUnion("kind", [
      z.strictObject({ kind: z.literal("disconnected") }),
      connected,
    ]),
    pending: z
      .discriminatedUnion("kind", [
        z.strictObject({ ...deviceFields, kind: z.literal("starting") }),
        device,
      ])
      .optional(),
  })
  .superRefine((record, ctx) => {
    const pending = record.pending;
    if (pending && !validGitHubDeviceTiming(pending)) {
      ctx.addIssue({ code: "custom", message: "Invalid device timing" });
    }
    const selection = record.selection;
    if (
      selection.kind === "connected" &&
      (selection.refreshExpiresAtMs <= selection.accessExpiresAtMs ||
        Boolean(selection.refresh?.tokens) !== (selection.refresh?.receivedAtMs !== undefined))
    ) {
      ctx.addIssue({ code: "custom", message: "Invalid refresh state" });
    }
  });

export type UserGitHubConnection = z.infer<typeof connectionSchema>;
export type UserGitHubConnected = z.infer<typeof connected>;
export type UserGitHubDevice = z.infer<typeof device>;

type RetirementObserver = (retirement: {
  profileIds: readonly string[];
  context: OpenClawStateWorkerContext;
}) => void;
const retirementObservers = new Set<RetirementObserver>();
export function observeUserGitHubProfileRetirement(observer: RetirementObserver): () => void {
  return registerListener(retirementObservers, observer);
}

export function publishUserGitHubProfileRetirement(
  ids: readonly string[],
  context: OpenClawStateWorkerContext,
): void {
  if (ids.length) {
    for (const observer of retirementObservers) {
      observer({ profileIds: ids, context });
    }
  }
}

function retireAfterCommit(db: DatabaseSync, ids: string[], env?: NodeJS.ProcessEnv): void {
  if (!ids.length || !retirementObservers.size) {
    return;
  }
  const identity = readTrackedStateDatabaseIdentity(db);
  if (!identity) {
    // Offline, untracked handles do not own a runtime credentials directory.
    return;
  }
  const context = captureOpenClawStateWorkerContext({
    path: identity.canonicalPath,
    env: env ?? {
      OPENCLAW_STATE_DIR: resolveOpenClawStateDirForDatabasePath(identity.canonicalPath),
    },
  });
  if (
    context.admission.identity.key !== identity.key ||
    context.admission.identity.birthtime !== identity.birthtime
  ) {
    throw new Error("Personal GitHub retirement source changed before publication.");
  }
  deferSqlitePostCommitPublication(db, () => {
    publishUserGitHubProfileRetirement(ids, context);
  });
}

export function parseConnection(raw: string): UserGitHubConnection {
  const result = connectionSchema.safeParse(safeParseJson(raw));
  if (!result.success) {
    throw new PersonalGitHubStateError();
  }
  const record = result.data;
  if (record.pending?.kind === "device") {
    registerSecretValueForRedaction(record.pending.deviceCode);
    if (record.pending.candidate) {
      registerTokens(record.pending.candidate.tokens);
    }
  }
  if (record.selection.kind === "connected") {
    registerSecretValueForRedaction(record.selection.refreshToken);
    if (record.selection.refresh?.tokens) {
      registerTokens(record.selection.refresh.tokens);
    }
  }
  return record;
}

function registerTokens(tokens: z.infer<typeof tokenPair>): void {
  registerSecretValueForRedaction(tokens.accessToken);
  registerSecretValueForRedaction(tokens.refreshToken);
}

/** Display fallback to a tombstone is never credential ownership. */
export function resolvePersonalGitHubOwner(
  profile: string,
  db = openOpenClawStateDatabase().db,
): string | undefined {
  if (!tableExists(db, "user_profiles")) {
    return undefined;
  }
  const resolved = selectResolvedUserProfileMetadataById(db, profile);
  return resolved && !resolved.merged_into ? resolved.id : undefined;
}

export function requireOwner(db: DatabaseSync, owner: string): void {
  if (resolvePersonalGitHubOwner(owner, db) !== owner) {
    throw new Error("Personal GitHub owner changed; reconnect and try again.");
  }
}

export function readConnection(db: DatabaseSync, owner: string): UserGitHubConnection | undefined {
  const raw = readPersonalGitHubSecret(db, owner);
  return raw === undefined ? undefined : parseConnection(raw);
}

export function readUserGitHubConnection(
  owner: string,
  database?: OpenClawStateDatabaseOptions,
): UserGitHubConnection | undefined {
  const db = openOpenClawStateDatabase(database).db;
  if (!tableExists(db, "user_profiles")) {
    throw new Error("Personal GitHub owner changed; reconnect and try again.");
  }
  try {
    const row = executeSqliteQueryTakeFirstSync(
      db,
      getNodeSqliteKysely<Pick<DB, "secret_store_entries"> & UserProfilesDatabase>(db)
        .selectFrom("user_profiles")
        .leftJoin("secret_store_entries", (join) =>
          join
            .onRef("secret_store_entries.scope_id", "=", "user_profiles.id")
            .on("secret_store_entries.scope_kind", "=", "identity")
            .on("secret_store_entries.name", "=", "github-connection")
            .on("secret_store_entries.deleted_at_ms", "is", null),
        )
        .select((eb) => [
          "user_profiles.id",
          // Keep the profile reader's native conversion failures without loading avatar blobs.
          eb
            .case()
            .when(eb.fn<string>("typeof", ["user_profiles.avatar"]), "=", "blob")
            .then(null)
            .else(eb.ref("user_profiles.avatar"))
            .end()
            .as("avatar"),
          "user_profiles.created_at",
          "user_profiles.updated_at",
          "secret_store_entries.name as secret_name",
          "secret_store_entries.value",
          "secret_store_entries.kind",
          "secret_store_entries.allowed_hosts",
        ])
        .where("user_profiles.id", "=", owner)
        .where("user_profiles.merged_into", "is", null),
    );
    if (!row) {
      throw new Error("Personal GitHub owner changed; reconnect and try again.");
    }
    const raw = decodePersonalGitHubSecret(row.secret_name === null ? undefined : row);
    return raw === undefined ? undefined : parseConnection(raw);
  } catch (error) {
    if (!isMissingSecretStoreTableError(error)) {
      throw error;
    }
    requireOwner(db, owner);
    return undefined;
  }
}

export function disconnectedUserGitHubConnection(): UserGitHubConnection {
  return { version: 1, generation: randomUUID(), selection: { kind: "disconnected" } };
}

export function connectionProfiles(record: UserGitHubConnection | undefined): string[] {
  return [
    ...(record?.selection.kind === "connected" ? [record.selection.profileId] : []),
    ...(record?.pending?.kind === "device" && record.pending.candidate
      ? [record.pending.candidate.profileId]
      : []),
  ];
}

// Only explicit replacement may repair corruption. A broken merge target must
// count as disconnected state so it never adopts the source's credentials.
export function readConnectionForReplacement(db: DatabaseSync, owner: string) {
  try {
    return readConnection(db, owner);
  } catch (error) {
    if (!(error instanceof PersonalGitHubStateError)) {
      throw error;
    }
    return disconnectedUserGitHubConnection();
  }
}

/** Transfer only this live source, never credentials stranded on historical aliases. */
export function mergeUserGitHubConnection(db: DatabaseSync, source: string, target: string): void {
  requireOwner(db, source);
  requireOwner(db, target);
  const sourceRecord = readConnectionForReplacement(db, source);
  const targetRecord = readConnectionForReplacement(db, target);
  const selected = targetRecord ?? sourceRecord;
  if (!selected) {
    return;
  }
  const next: UserGitHubConnection = { ...selected, generation: randomUUID(), pending: undefined };
  ensureSecretStoreSchema(db);
  writePersonalGitHubSecret(db, target, JSON.stringify(next));
  if (sourceRecord) {
    writePersonalGitHubSecret(db, source, null);
  }
  const retained = new Set(connectionProfiles(next));
  retireAfterCommit(
    db,
    [...connectionProfiles(sourceRecord), ...connectionProfiles(targetRecord)].filter(
      (id) => !retained.has(id),
    ),
  );
}

export function listUserGitHubConnectionsInDatabase(db: DatabaseSync): Array<{
  owner: string;
  connection: UserGitHubConnection;
}> {
  if (!tableExists(db, "secret_store_entries") || !tableExists(db, "user_profiles")) {
    return [];
  }
  const query = getNodeSqliteKysely<
    Pick<DB, "secret_store_entries"> & Pick<UserProfilesDatabase, "user_profiles">
  >(db);
  return executeSqliteQuerySync(
    db,
    query
      .selectFrom("secret_store_entries")
      .innerJoin("user_profiles", "user_profiles.id", "secret_store_entries.scope_id")
      .select(["scope_id", "value"])
      .where("scope_kind", "=", "identity")
      .where("name", "=", "github-connection")
      .where("kind", "=", "secret")
      .where("allowed_hosts", "is", null)
      .where("deleted_at_ms", "is", null)
      .where("merged_into", "is", null)
      .orderBy("scope_id"),
  ).rows.flatMap((row) => {
    try {
      return [{ owner: row.scope_id, connection: parseConnection(row.value) }];
    } catch {
      return [];
    }
  });
}

export function mutateUserGitHubConnectionInDatabase(
  db: DatabaseSync,
  input: UserGitHubMutation,
  admit: (
    stage: "transaction" | "commit",
    facts: {
      kind: string;
      owner: string;
      role: UserGitHubRole | undefined;
      result: UserGitHubCommit;
      identity?: UserGitHubMutation["lease"];
      expiresAt?: number;
    },
  ) => void,
  family: "worker" | "native-compatibility",
): UserGitHubCommit {
  const profile = tableExists(db, "user_profiles")
    ? selectResolvedUserProfileMetadataById(db, input.owner)
    : undefined;
  const owner = profile && !profile.merged_into ? profile.id : undefined;
  const empty: UserGitHubCommit = {
    kind: "user-github",
    owner: input.owner,
    changed: false,
    connection: undefined,
    retired: [],
  };
  if (!owner || (input.kind !== "refresh" && owner !== input.owner)) {
    if (input.kind === "refresh" || input.kind === "expire-pending") {
      return empty;
    }
    throw new Error("Personal GitHub owner changed; reconnect and try again.");
  }
  const current =
    input.kind === "disconnect"
      ? readConnectionForReplacement(db, owner)
      : readConnection(db, owner);
  let next: UserGitHubConnection;
  if (input.kind === "replace") {
    if (!isDeepStrictEqual(current, input.expected)) {
      throw new Error("My GitHub authorization changed; try again.");
    }
    next = input.next;
  } else if (input.kind === "pending") {
    if (
      !current?.pending ||
      current.generation !== input.generation ||
      !isDeepStrictEqual(current.pending, input.expectedPending) ||
      current.pending.expiresAtMs <= Date.now() ||
      (input.pending && input.pending.requestId !== current.pending.requestId)
    ) {
      throw new Error("My GitHub authorization changed or expired; start again.");
    }
    next = { ...current, pending: input.pending };
  } else if (input.kind === "disconnect") {
    next = disconnectedUserGitHubConnection();
  } else if (input.kind === "cancel") {
    if (!current || current.pending?.requestId !== input.requestId) {
      return empty;
    }
    next = { ...current, pending: undefined };
  } else if (input.kind === "expire-pending") {
    if (
      !current?.pending ||
      current.generation !== input.generation ||
      current.pending.requestId !== input.requestId ||
      current.pending.expiresAtMs > Date.now()
    ) {
      return empty;
    }
    next = { ...current, pending: undefined };
  } else if (input.kind === "start") {
    next = { ...(current ?? disconnectedUserGitHubConnection()), pending: input.pending };
  } else if (input.kind === "install") {
    const pending = current?.pending;
    const candidate = pending?.kind === "device" ? pending.candidate : undefined;
    if (
      !current ||
      current.generation !== input.generation ||
      !pending ||
      pending.requestId !== input.requestId ||
      pending.expiresAtMs <= Date.now() ||
      !candidate ||
      candidate.profileId !== input.profileId
    ) {
      throw new Error("My GitHub authorization changed or expired; start again.");
    }
    next = {
      ...current,
      generation: randomUUID(),
      pending: undefined,
      selection: {
        kind: "connected",
        profileId: candidate.profileId,
        accountId: input.account.accountId,
        login: input.account.login,
        refreshToken: candidate.tokens.refreshToken,
        scopes: candidate.tokens.scopes,
        accessExpiresAtMs: candidate.receivedAtMs + candidate.tokens.expiresInSeconds * 1000,
        refreshExpiresAtMs:
          candidate.receivedAtMs + candidate.tokens.refreshTokenExpiresInSeconds * 1000,
      },
    };
  } else {
    const selection = current?.selection;
    if (
      !current ||
      selection?.kind !== "connected" ||
      selection.profileId !== input.profileId ||
      selection.refresh?.operationId !== input.operationId
    ) {
      return empty;
    }
    const update = input.update;
    next = {
      ...current,
      selection:
        update.kind === "rotated"
          ? {
              ...selection,
              refreshToken: update.tokens.refreshToken,
              scopes: update.tokens.scopes,
              accessExpiresAtMs: update.receivedAtMs + update.tokens.expiresInSeconds * 1000,
              refreshExpiresAtMs:
                update.receivedAtMs + update.tokens.refreshTokenExpiresInSeconds * 1000,
              refreshFailure: undefined,
              refresh: {
                operationId: input.operationId,
                tokens: update.tokens,
                receivedAtMs: update.receivedAtMs,
              },
            }
          : update.kind === "failed"
            ? { ...selection, refresh: undefined, refreshFailure: update.failure }
            : {
                ...selection,
                login: update.login,
                refresh: undefined,
                refreshFailure: undefined,
              },
    };
  }
  const pendingExpiry =
    input.kind === "start"
      ? input.pending.expiresAtMs
      : input.kind === "pending" || input.kind === "install"
        ? current?.pending?.expiresAtMs
        : undefined;
  const assertPendingUnexpired = () => {
    if (pendingExpiry !== undefined && pendingExpiry <= Date.now()) {
      throw new Error("My GitHub authorization changed or expired; start again.");
    }
  };
  assertPendingUnexpired();
  next = parseConnection(JSON.stringify(next));
  const retained = new Set(connectionProfiles(next));
  const result: UserGitHubCommit = {
    kind: "user-github",
    owner,
    changed: true,
    connection: next,
    retired: connectionProfiles(current).filter((id) => !retained.has(id)),
  };
  const role: UserGitHubRole | undefined =
    input.kind === "refresh" || family === "native-compatibility"
      ? undefined
      : {
          profileId: owner,
          role: profile?.role ?? null,
          githubLogin: selectUserProfileGitHubIdentities(db, [owner]).get(owner)?.login ?? null,
        };
  const expiresAt = input.lease ? readOpenClawStateLeaseExpiry(db, input.lease) : undefined;
  if (input.lease && (expiresAt === undefined || expiresAt <= Date.now())) {
    throw new Error("Personal GitHub profile lease changed.");
  }
  const facts = {
    ...(input.lease
      ? { kind: "state-lease", identity: input.lease, expiresAt }
      : { kind: "user-github" }),
    owner: input.owner,
    role,
    result,
  };
  admit("transaction", facts);
  assertPendingUnexpired();
  if (family === "native-compatibility") {
    ensureSecretStoreSchema(db);
  }
  writePersonalGitHubSecret(db, owner, JSON.stringify(next));
  admit("commit", facts);
  assertPendingUnexpired();
  return result;
}

/** v2026.9.8 gateway-runtime exposes synchronous personal OAuth mutation callbacks. */
export function mutateUserGitHubConnectionSync(
  input: UserGitHubMutation,
  assertCurrent: () => void,
  options?: OpenClawStateDatabaseOptions,
): UserGitHubCommit {
  const environment = cloneEnvWithPlatformSemantics(options?.env ?? process.env);
  return runOpenClawStateWriteTransaction(
    ({ db }) => {
      const result = mutateUserGitHubConnectionInDatabase(
        db,
        input,
        (stage) => {
          if (stage === "transaction") {
            assertCurrent();
          }
        },
        "native-compatibility",
      );
      retireAfterCommit(db, result.retired, environment);
      return result;
    },
    options,
    { operationLabel: "users.github.sdk-mutate" },
  );
}
