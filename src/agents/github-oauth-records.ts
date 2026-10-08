import { z } from "zod";
import { isManagedGitHubProfileId } from "../config/github-identity-profile-id.js";
import { normalizeAgentId } from "../routing/session-key.js";
import {
  deleteHiddenGitHubSecretRecordInDatabase,
  isLiveHiddenGitHubStoreRow,
  readHiddenGitHubRow,
} from "../secrets/store/secret-store-hidden-github.kernel.js";
import {
  deleteHiddenGitHubSecretRecord,
  listHiddenGitHubSecretRecords,
  readHiddenGitHubSecretRecord,
  writeHiddenGitHubSecretRecord,
} from "../secrets/store/secret-store.js";
import {
  githubOAuthTimestamp as timestamp,
  githubOAuthProfileId,
  githubOAuthScopes,
  githubOAuthRefreshFields,
  githubOAuthDeviceFields,
  validGitHubDeviceTiming,
} from "../shared/github-oauth-values.js";
import { withExistingOpenClawStateDatabaseCurrentReadOnly } from "../state/openclaw-state-db-readonly.js";
import { runOpenClawStateWriteTransaction } from "../state/openclaw-state-db.js";
import type { OpenClawStateWorkerContext } from "../state/openclaw-state-worker-context.types.js";
import type { GitHubOAuthTokenPair } from "./github-oauth-client.js";
import type { GitHubToolAccount } from "./github-tool-account.js";

const OAUTH_RECORD_PREFIX = "github-oauth-";
const OPAQUE_ID_PATTERN = /^[a-f0-9]{32}$/u;
const DEVICE_REQUEST_ID_PATTERN = /^github-device-[a-f0-9]{32}$/u;
const requestIdSchema = z.string().regex(DEVICE_REQUEST_ID_PATTERN);
const scope = z.enum(["system", "agent"]);
const agentId = z
  .string()
  .min(1)
  .max(128)
  .refine((value) => normalizeAgentId(value) === value);
const authorValue = z.string().refine((value) => value.trim().length > 0);
// Zod drops __proto__ before strict-object checks; shared records reject that extra key.
function strictRecord<T extends z.ZodRawShape>(shape: T) {
  return z
    .unknown()
    .refine(
      (value) => value === null || typeof value !== "object" || !Object.hasOwn(value, "__proto__"),
    )
    .pipe(z.strictObject(shape));
}
const identityConfig = strictRecord({
  profileId: githubOAuthProfileId,
  kind: z.literal("oauth").optional(),
  allowInSandbox: z.boolean().optional(),
  gitAuthor: strictRecord({ name: authorValue.optional(), email: authorValue.optional() })
    .refine((author) => Object.keys(author).length > 0)
    .optional(),
}).nullable();
const agentLifecycleBinding = strictRecord({
  agentId,
  provenance: strictRecord({
    agentId,
    createdVia: z.enum(["operator", "agent", "claw"]),
    creatorAgentId: agentId.nullable(),
    createdAtMs: timestamp,
  }).nullable(),
}).refine(
  (binding) => binding.provenance === null || binding.provenance.agentId === binding.agentId,
);
const authorizationFields = {
  agentId,
  scope,
  expectedIdentity: identityConfig,
  // Invalid bindings were historically discarded for System records, but never for agents.
  agentLifecycleBinding: agentLifecycleBinding.optional().catch(undefined),
};
function validAgentBinding(record: z.infer<z.ZodObject<typeof authorizationFields>>): boolean {
  return record.scope === "agent"
    ? record.agentLifecycleBinding?.agentId === record.agentId
    : record.agentLifecycleBinding === undefined;
}
function omitMissingAgentBinding<T extends { agentLifecycleBinding?: unknown }>(record: T): T {
  if (record.agentLifecycleBinding === undefined) {
    delete record.agentLifecycleBinding;
  }
  return record;
}
const deviceRecordSchema = strictRecord({
  version: z.literal(1),
  requestId: requestIdSchema,
  ...githubOAuthDeviceFields,
  ...authorizationFields,
})
  .refine(validAgentBinding)
  .refine(validGitHubDeviceTiming)
  .transform(omitMissingAgentBinding);
const pendingInitialSchema = strictRecord({
  requestId: requestIdSchema,
  scope,
  agentId,
  expectedIdentity: identityConfig,
  agentLifecycleBinding: authorizationFields.agentLifecycleBinding,
})
  .refine(validAgentBinding)
  .transform(omitMissingAgentBinding);
const oauthRecordSchema = strictRecord({
  version: z.literal(1),
  profileId: githubOAuthProfileId,
  agentId,
  scope,
  ...githubOAuthRefreshFields,
  scopes: githubOAuthScopes.refine((values) => {
    // Shared records require canonical ordering; personal records preserve their scope order.
    const canonical = [...new Set(values)].toSorted((left, right) => left.localeCompare(right));
    return (
      canonical.length === values.length &&
      canonical.every((value, index) => value === values[index])
    );
  }),
  createdAtMs: timestamp,
  pendingInitial: pendingInitialSchema.optional(),
  pendingRefresh: z.literal(true).optional(),
  refreshFailure: z.enum(["expired", "failed"]).optional(),
}).refine(
  (record) =>
    record.accessExpiresAtMs > record.createdAtMs &&
    record.refreshExpiresAtMs > record.accessExpiresAtMs &&
    (!record.pendingInitial ||
      (record.pendingInitial.scope === record.scope &&
        record.pendingInitial.agentId === record.agentId)) &&
    !(record.pendingInitial && record.pendingRefresh) &&
    !(record.pendingRefresh && record.refreshFailure),
);

export type GitHubIdentityScope = z.infer<typeof scope>;
export type GitHubDeviceAuthorizationRecord = Readonly<z.infer<typeof deviceRecordSchema>>;
type GitHubOAuthPendingInitial = Readonly<z.infer<typeof pendingInitialSchema>>;
export type GitHubOAuthRecord = Readonly<Omit<z.infer<typeof oauthRecordSchema>, "scopes">> & {
  readonly scopes: readonly string[];
};

export function createGitHubOAuthRecord(params: {
  profileId: string;
  scope: GitHubIdentityScope;
  agentId: string;
  account: GitHubToolAccount;
  tokens: GitHubOAuthTokenPair;
  now: number;
  pendingInitial?: GitHubOAuthPendingInitial;
  pendingRefresh?: true;
}): GitHubOAuthRecord {
  return {
    version: 1,
    profileId: params.profileId,
    scope: params.scope,
    agentId: params.agentId,
    accountId: params.account.accountId,
    login: params.account.login,
    refreshToken: params.tokens.refreshToken,
    accessExpiresAtMs: params.now + params.tokens.expiresInSeconds * 1_000,
    refreshExpiresAtMs: params.now + params.tokens.refreshTokenExpiresInSeconds * 1_000,
    scopes: params.tokens.scopes,
    createdAtMs: params.now,
    ...(params.pendingInitial ? { pendingInitial: params.pendingInitial } : {}),
    ...(params.pendingRefresh ? { pendingRefresh: true } : {}),
  };
}

function githubDeviceRecordName(requestId: string): string {
  if (!DEVICE_REQUEST_ID_PATTERN.test(requestId)) {
    throw new Error("GitHub device authorization request id is invalid.");
  }
  return requestId;
}

function githubOAuthRecordName(profileId: string): string {
  if (!isManagedGitHubProfileId(profileId)) {
    throw new Error("Managed GitHub profile id is invalid.");
  }
  return `${OAUTH_RECORD_PREFIX}${profileId.slice("ghp_".length)}`;
}

function parseGitHubOAuthProfileId(name: string): string | undefined {
  const opaqueId = name.startsWith(OAUTH_RECORD_PREFIX)
    ? name.slice(OAUTH_RECORD_PREFIX.length)
    : "";
  return OPAQUE_ID_PATTERN.test(opaqueId) ? `ghp_${opaqueId}` : undefined;
}

function parseGitHubRecord<T>(raw: string, schema: z.ZodType<T>): T | undefined {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return undefined;
  }
  const result = schema.safeParse(value);
  return result.success ? result.data : undefined;
}

export type GitHubOAuthRecordAccess = {
  context?: OpenClawStateWorkerContext;
  assertCurrent?: () => void;
};
type GitHubOAuthRecordWrite = GitHubOAuthRecordAccess & {
  expected?: GitHubDeviceAuthorizationRecord | GitHubOAuthRecord | null;
};
type GitHubOAuthRecordDelete = GitHubOAuthRecordAccess & {
  expected?: GitHubDeviceAuthorizationRecord | GitHubOAuthRecord;
  expectedValue?: string;
};
const storedValues = new WeakMap<object, string>();

export class GitHubOAuthRecordChangedError extends Error {
  constructor() {
    super("GitHub authorization changed before persistence.");
  }
}

function expectedValue(options: GitHubOAuthRecordWrite): { expectedValue?: string | null } {
  if (options.expected === undefined) {
    return {};
  }
  if (options.expected === null) {
    return { expectedValue: null };
  }
  const value = storedValues.get(options.expected);
  if (value === undefined) {
    throw new Error("GitHub authorization comparison requires a stored record.");
  }
  return { expectedValue: value };
}

function remember<T extends object>(record: T | undefined, value: string): T | undefined {
  if (record) {
    storedValues.set(record, value);
  }
  return record;
}

async function writeRecord(name: string, record: object, options: GitHubOAuthRecordWrite) {
  const value = JSON.stringify(record);
  const written = await writeHiddenGitHubSecretRecord({
    name,
    value,
    context: options.context,
    assertCurrent: options.assertCurrent,
    ...expectedValue(options),
  });
  if (!written) {
    throw new GitHubOAuthRecordChangedError();
  }
  storedValues.set(record, value);
}

export function writeGitHubDeviceAuthorizationRecord(
  record: GitHubDeviceAuthorizationRecord,
  options: GitHubOAuthRecordWrite = {},
): Promise<void> {
  const parsed = parseGitHubRecord(JSON.stringify(record), deviceRecordSchema);
  if (!parsed || parsed.requestId !== record.requestId) {
    throw new Error("GitHub device authorization record is invalid.");
  }
  return writeRecord(githubDeviceRecordName(record.requestId), parsed, options).then(() => {
    storedValues.set(record, JSON.stringify(parsed));
  });
}

export async function readGitHubDeviceAuthorizationRecord(
  requestId: string,
  options: GitHubOAuthRecordAccess = {},
): Promise<GitHubDeviceAuthorizationRecord | undefined> {
  const raw = await readHiddenGitHubSecretRecord({
    name: githubDeviceRecordName(requestId),
    ...options,
  });
  const record =
    raw === undefined ? undefined : remember(parseGitHubRecord(raw, deviceRecordSchema), raw);
  return record?.requestId === requestId ? record : undefined;
}

export function deleteGitHubDeviceAuthorizationRecord(
  requestId: string,
  options: GitHubOAuthRecordDelete = {},
): Promise<boolean> {
  const expected = options.expectedValue ?? expectedValue(options).expectedValue;
  return deleteHiddenGitHubSecretRecord({
    name: githubDeviceRecordName(requestId),
    context: options.context,
    assertCurrent: options.assertCurrent,
    expectedValue: expected ?? undefined,
  });
}

export async function listGitHubDeviceAuthorizationRecords(
  options: GitHubOAuthRecordAccess = {},
): Promise<
  Array<{
    requestId: string;
    record: GitHubDeviceAuthorizationRecord | undefined;
    expectedValue: string;
  }>
> {
  return (await listHiddenGitHubSecretRecords({ prefix: "github-device", ...options })).flatMap(
    ({ name, value }) => {
      const requestId = name;
      if (!DEVICE_REQUEST_ID_PATTERN.test(requestId)) {
        return [];
      }
      const record = remember(parseGitHubRecord(value, deviceRecordSchema), value);
      return [
        {
          requestId,
          record: record?.requestId === requestId ? record : undefined,
          expectedValue: value,
        },
      ];
    },
  );
}

export function writeGitHubOAuthRecord(
  record: GitHubOAuthRecord,
  options: GitHubOAuthRecordWrite = {},
): Promise<void> {
  const parsed = parseGitHubRecord(JSON.stringify(record), oauthRecordSchema);
  if (!parsed || parsed.profileId !== record.profileId) {
    throw new Error("GitHub OAuth record is invalid.");
  }
  return writeRecord(githubOAuthRecordName(record.profileId), parsed, options).then(() => {
    storedValues.set(record, JSON.stringify(parsed));
  });
}

export async function inspectGitHubOAuthRecord(
  profileId: string,
  options: GitHubOAuthRecordAccess = {},
): Promise<
  { state: "missing" } | { state: "invalid" } | { state: "valid"; record: GitHubOAuthRecord }
> {
  const raw = await readHiddenGitHubSecretRecord({
    name: githubOAuthRecordName(profileId),
    ...options,
  });
  if (raw === undefined) {
    return { state: "missing" };
  }
  const record = remember(parseGitHubRecord(raw, oauthRecordSchema), raw);
  return record?.profileId === profileId ? { state: "valid", record } : { state: "invalid" };
}

export function deleteGitHubOAuthRecord(
  profileId: string,
  options: GitHubOAuthRecordDelete = {},
): Promise<boolean> {
  const expected = options.expectedValue ?? expectedValue(options).expectedValue;
  return deleteHiddenGitHubSecretRecord({
    name: githubOAuthRecordName(profileId),
    context: options.context,
    assertCurrent: options.assertCurrent,
    expectedValue: expected ?? undefined,
  });
}

export async function listGitHubOAuthRecords(options: GitHubOAuthRecordAccess = {}): Promise<
  Array<{
    profileId: string;
    record: GitHubOAuthRecord | undefined;
    expectedValue: string;
  }>
> {
  return (await listHiddenGitHubSecretRecords({ prefix: "github-oauth", ...options })).flatMap(
    ({ name, value }) => {
      const profileId = parseGitHubOAuthProfileId(name);
      if (!profileId) {
        return [];
      }
      const record = remember(parseGitHubRecord(value, oauthRecordSchema), value);
      return [
        {
          profileId,
          record: record?.profileId === profileId ? record : undefined,
          expectedValue: value,
        },
      ];
    },
  );
}

/** Released Gateway SDK cancellation keeps its synchronous completion boundary. */
export function cancelGitHubDeviceAuthorizationRecordForSdk(
  requestId: string,
  context: OpenClawStateWorkerContext,
  onCleanupFailure: () => void,
): boolean {
  const name = githubDeviceRecordName(requestId);
  context.admission.assertCurrent();
  const options = { path: context.admission.databasePath, env: context.environment };
  const row = withExistingOpenClawStateDatabaseCurrentReadOnly(
    ({ db }) => readHiddenGitHubRow(db, name),
    options,
  );
  const existed = Boolean(
    row &&
    isLiveHiddenGitHubStoreRow(row, "device", Date.now()) &&
    parseGitHubRecord(row.value, deviceRecordSchema)?.requestId === requestId,
  );
  try {
    runOpenClawStateWriteTransaction(
      ({ db }) => {
        context.admission.assertCurrent();
        deleteHiddenGitHubSecretRecordInDatabase(db, { name });
      },
      options,
      { operationLabel: "github-oauth.cancel-sdk" },
    );
  } catch {
    onCleanupFailure();
  }
  return existed;
}

/** Released Gateway SDK retirement retains immediate metadata removal. */
export function retireGitHubOAuthRecordForSdk(
  profileId: string,
  context: OpenClawStateWorkerContext,
): void {
  const name = githubOAuthRecordName(profileId);
  context.admission.assertCurrent();
  runOpenClawStateWriteTransaction(
    ({ db }) => {
      context.admission.assertCurrent();
      deleteHiddenGitHubSecretRecordInDatabase(db, { name });
    },
    { path: context.admission.databasePath, env: context.environment },
    { operationLabel: "github-oauth.retire-sdk" },
  );
}
