import type { DatabaseSync } from "node:sqlite";
import type { Selectable } from "kysely";
import {
  executeSqliteQuerySync,
  executeSqliteQueryTakeFirstSync,
  getNodeSqliteKysely,
} from "../../infra/kysely-sync.js";
import { normalizeSqliteNumber } from "../../infra/sqlite-number.js";
import { registerSecretValueForRedaction } from "../../logging/secret-redaction-registry.js";
import type { DB as OpenClawStateKyselyDatabase } from "../../state/openclaw-state-db.generated.js";
import type { HiddenGitHubDelete } from "./secret-store-hidden-github.types.js";
import { isMissingSecretStoreTableError } from "./secret-store-sqlite.js";
import {
  SECRET_STORE_VALUE_MAX_BYTES,
  SecretStoreValidationError,
} from "./secret-store-validation-error.js";

type HiddenGitHubStoreDatabase = Pick<OpenClawStateKyselyDatabase, "secret_store_entries">;
type HiddenGitHubStoreRow = Selectable<OpenClawStateKyselyDatabase["secret_store_entries"]>;
type HiddenGitHubStoreKind = "device" | "oauth";
type HiddenGitHubStoreNameKind = "setup" | HiddenGitHubStoreKind;
type HiddenGitHubStorePrefix = "github-device" | "github-oauth";

export const GITHUB_SETUP_HANDOFF_MAX_AGE_MS = 10 * 60_000;
export const GITHUB_DEVICE_STORE_MAX_AGE_MS = 15 * 60_000;
const HIDDEN_GITHUB_STORE_NAME_PATTERN = /^github-(setup|device|oauth)-[a-f0-9]{32}$/u;

export function classifyHiddenGitHubStoreName(name: string): HiddenGitHubStoreNameKind | undefined {
  const kind = HIDDEN_GITHUB_STORE_NAME_PATTERN.exec(name)?.[1];
  return kind === "setup" || kind === "device" || kind === "oauth" ? kind : undefined;
}

export function assertHiddenGitHubSecretRecordName(name: string): HiddenGitHubStoreKind {
  const kind = classifyHiddenGitHubStoreName(name);
  if (kind !== "device" && kind !== "oauth") {
    throw new SecretStoreValidationError(
      "SECRET_STORE_INVALID_NAME",
      "Hidden GitHub secret record name must match github-device-<32 lowercase hex characters> or github-oauth-<32 lowercase hex characters>.",
    );
  }
  return kind;
}

function hiddenGitHubStoreKindFromPrefix(prefix: HiddenGitHubStorePrefix): HiddenGitHubStoreKind {
  if (prefix === "github-device") {
    return "device";
  }
  if (prefix === "github-oauth") {
    return "oauth";
  }
  throw new SecretStoreValidationError(
    "SECRET_STORE_INVALID_NAME",
    'Hidden GitHub secret record prefix must be "github-device" or "github-oauth".',
  );
}

export function validateHiddenGitHubSecretValue(value: string): void {
  const bytes = Buffer.byteLength(value, "utf8");
  if (bytes > SECRET_STORE_VALUE_MAX_BYTES) {
    throw new SecretStoreValidationError(
      "SECRET_STORE_VALUE_TOO_LARGE",
      `Secret store value exceeds ${SECRET_STORE_VALUE_MAX_BYTES} UTF-8 bytes.`,
    );
  }
  if (value.length === 0) {
    throw new SecretStoreValidationError(
      "SECRET_STORE_VALUE_EMPTY",
      "Secret store value is empty. Secret entries require a value; check the command that produced it.",
    );
  }
}

export class PersonalGitHubStateError extends Error {
  constructor() {
    super("Personal GitHub state is invalid; disconnect and reconnect My GitHub.");
  }
}

/** Private GitHub aggregate only; identity secrets have no generic reader or projection. */
export function readPersonalGitHubSecret(db: DatabaseSync, profileId: string): string | undefined {
  try {
    const row = executeSqliteQueryTakeFirstSync(
      db,
      getNodeSqliteKysely<HiddenGitHubStoreDatabase>(db)
        .selectFrom("secret_store_entries")
        .select(["value", "kind", "allowed_hosts"])
        .where("scope_kind", "=", "identity")
        .where("scope_id", "=", profileId)
        .where("name", "=", "github-connection")
        .where("deleted_at_ms", "is", null),
    );
    return decodePersonalGitHubSecret(row);
  } catch (error) {
    if (isMissingSecretStoreTableError(error)) {
      return undefined;
    }
    throw error;
  }
}

/** Shared decoding for exact secret readers and the live-owner credential join. */
export function decodePersonalGitHubSecret(
  row: { value: string | null; kind: string | null; allowed_hosts: string | null } | undefined,
): string | undefined {
  if (!row) {
    return undefined;
  }
  if (row.kind !== "secret" || row.allowed_hosts !== null || row.value === null) {
    throw new PersonalGitHubStateError();
  }
  try {
    validateHiddenGitHubSecretValue(row.value);
  } catch (error) {
    if (error instanceof SecretStoreValidationError) {
      throw new PersonalGitHubStateError();
    }
    throw error;
  }
  registerSecretValueForRedaction(row.value);
  return row.value;
}

/** The caller owns the synchronous profile/connection transaction and its preconditions. */
export function writePersonalGitHubSecret(
  db: DatabaseSync,
  profileId: string,
  value: string | null,
): void {
  const query = getNodeSqliteKysely<HiddenGitHubStoreDatabase>(db);
  if (value === null) {
    executeSqliteQuerySync(
      db,
      query
        .deleteFrom("secret_store_entries")
        .where("scope_kind", "=", "identity")
        .where("scope_id", "=", profileId)
        .where("name", "=", "github-connection"),
    );
    return;
  }
  validateHiddenGitHubSecretValue(value);
  const now = Date.now();
  upsertHiddenGitHubSecret(
    db,
    {
      scope_kind: "identity",
      scope_id: profileId,
      name: "github-connection",
      value,
      updated_by: null,
    },
    now,
  );
  registerSecretValueForRedaction(value);
}

export function upsertHiddenGitHubSecret(
  db: DatabaseSync,
  entry: Pick<HiddenGitHubStoreRow, "scope_kind" | "scope_id" | "name" | "value" | "updated_by">,
  now: number,
  expectedValue?: string | null,
): boolean {
  const values = {
    value: entry.value,
    updated_by: entry.updated_by,
    kind: "secret",
    allowed_hosts: null,
    deleted_at_ms: null,
    updated_at_ms: now,
  };
  const query = getNodeSqliteKysely<HiddenGitHubStoreDatabase>(db);
  if (typeof expectedValue === "string") {
    const currentTime = Date.now();
    let update = query
      .updateTable("secret_store_entries")
      .set(values)
      .where("scope_kind", "=", entry.scope_kind)
      .where("scope_id", "=", entry.scope_id)
      .where("name", "=", entry.name)
      .where("value", "=", expectedValue)
      .where("kind", "=", "secret")
      .where("allowed_hosts", "is", null)
      .where("deleted_at_ms", "is", null)
      .where("created_at_ms", "<=", currentTime);
    if (classifyHiddenGitHubStoreName(entry.name) === "device") {
      update = update.where("created_at_ms", ">", currentTime - GITHUB_DEVICE_STORE_MAX_AGE_MS);
    }
    const updated = executeSqliteQuerySync(db, update);
    return Number(updated.numAffectedRows ?? 0n) === 1;
  }
  const result = executeSqliteQuerySync(
    db,
    query
      .insertInto("secret_store_entries")
      .values({ ...entry, ...values, created_at_ms: now })
      .onConflict((conflict) =>
        expectedValue === null
          ? conflict.columns(["scope_kind", "scope_id", "name"]).doNothing()
          : conflict.columns(["scope_kind", "scope_id", "name"]).doUpdateSet(values),
      ),
  );
  return Number(result.numAffectedRows ?? 0n) === 1;
}

export function isLiveHiddenGitHubStoreRow(
  row: Pick<HiddenGitHubStoreRow, "created_at_ms" | "updated_at_ms">,
  kind: HiddenGitHubStoreKind,
  now: number,
): boolean {
  const createdAtMs = normalizeSqliteNumber(row.created_at_ms);
  const updatedAtMs = normalizeSqliteNumber(row.updated_at_ms);
  return (
    createdAtMs !== undefined &&
    updatedAtMs !== undefined &&
    createdAtMs <= now &&
    (kind !== "device" || createdAtMs > now - GITHUB_DEVICE_STORE_MAX_AGE_MS)
  );
}

export function readHiddenGitHubRow(db: DatabaseSync, name: string) {
  assertHiddenGitHubSecretRecordName(name);
  try {
    return executeSqliteQueryTakeFirstSync(
      db,
      getNodeSqliteKysely<HiddenGitHubStoreDatabase>(db)
        .selectFrom("secret_store_entries")
        .select(["name", "value", "created_at_ms", "updated_at_ms"])
        .where("scope_kind", "=", "team")
        .where("scope_id", "=", "")
        .where("name", "=", name)
        .where("kind", "=", "secret")
        .where("allowed_hosts", "is", null)
        .where("deleted_at_ms", "is", null),
    );
  } catch (error) {
    if (isMissingSecretStoreTableError(error)) {
      return undefined;
    }
    throw error;
  }
}

export function listHiddenGitHubRows(db: DatabaseSync, prefix: HiddenGitHubStorePrefix) {
  const kind = hiddenGitHubStoreKindFromPrefix(prefix);
  try {
    const rows = executeSqliteQuerySync(
      db,
      getNodeSqliteKysely<HiddenGitHubStoreDatabase>(db)
        .selectFrom("secret_store_entries")
        .select(["name", "value", "created_at_ms", "updated_at_ms"])
        .where("scope_kind", "=", "team")
        .where("scope_id", "=", "")
        .where("name", ">=", `${prefix}-`)
        .where("name", "<", `${prefix}.`)
        .where("kind", "=", "secret")
        .where("allowed_hosts", "is", null)
        .where("deleted_at_ms", "is", null)
        .orderBy("name", "asc"),
    ).rows;
    return rows.filter((row) => classifyHiddenGitHubStoreName(row.name) === kind);
  } catch (error) {
    if (isMissingSecretStoreTableError(error)) {
      return [];
    }
    throw error;
  }
}

/** The exact row predicate and returned value share the consuming DELETE snapshot. */
export function consumeGitHubSetupHandoffInDatabase(
  db: DatabaseSync,
  input: { name: string; nowMs: number },
): string | undefined {
  if (classifyHiddenGitHubStoreName(input.name) !== "setup") {
    return undefined;
  }
  try {
    return executeSqliteQueryTakeFirstSync(
      db,
      getNodeSqliteKysely<HiddenGitHubStoreDatabase>(db)
        .deleteFrom("secret_store_entries")
        .where("scope_kind", "=", "team")
        .where("scope_id", "=", "")
        .where("name", "=", input.name)
        .where("kind", "=", "secret")
        .where("allowed_hosts", "is", null)
        .where("created_at_ms", ">=", input.nowMs - GITHUB_SETUP_HANDOFF_MAX_AGE_MS)
        .where("created_at_ms", "<=", input.nowMs)
        .where("deleted_at_ms", "is", null)
        .returning("value"),
    )?.value;
  } catch (error) {
    if (isMissingSecretStoreTableError(error)) {
      return undefined;
    }
    throw error;
  }
}

export function deleteHiddenGitHubSecretRecordInDatabase(
  db: DatabaseSync,
  input: HiddenGitHubDelete,
): boolean {
  assertHiddenGitHubSecretRecordName(input.name);
  try {
    let query = getNodeSqliteKysely<HiddenGitHubStoreDatabase>(db)
      .deleteFrom("secret_store_entries")
      .where("scope_kind", "=", "team")
      .where("scope_id", "=", "")
      .where("name", "=", input.name);
    if (input.expectedValue !== undefined) {
      query = query.where("value", "=", input.expectedValue);
    }
    return Number(executeSqliteQuerySync(db, query).numAffectedRows ?? 0n) > 0;
  } catch (error) {
    if (isMissingSecretStoreTableError(error)) {
      return false;
    }
    throw error;
  }
}
