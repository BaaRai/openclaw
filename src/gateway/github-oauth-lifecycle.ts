import { randomBytes } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import type {
  ToolsGitHubAuthorizePollResult,
  ToolsGitHubAuthorizeStartResult,
} from "../../packages/gateway-protocol/src/index.js";
import { captureAgentLifecycleBinding } from "../agents/agent-lifecycle-registry.js";
import { resolveAgentConfig } from "../agents/agent-scope.js";
import {
  clearGitHubCredentialVerificationCache,
  type GitHubOAuthTokenPair,
} from "../agents/github-oauth-client.js";
import {
  createGitHubOAuthRecord,
  cancelGitHubDeviceAuthorizationRecordForSdk,
  deleteGitHubDeviceAuthorizationRecord as deleteDeviceRecord,
  deleteGitHubOAuthRecord as deleteOAuthRecord,
  inspectGitHubOAuthRecord as inspectOAuthRecord,
  listGitHubDeviceAuthorizationRecords as listDeviceRecords,
  listGitHubOAuthRecords as listOAuthRecords,
  readGitHubDeviceAuthorizationRecord as readDeviceRecord,
  retireGitHubOAuthRecordForSdk,
  type GitHubDeviceAuthorizationRecord,
  type GitHubIdentityScope,
  type GitHubOAuthRecord,
  writeGitHubDeviceAuthorizationRecord as writeDeviceRecord,
  writeGitHubOAuthRecord as writeOAuthRecord,
} from "../agents/github-oauth-records.js";
import { clearNativeGitHubTokenCache } from "../agents/github-read-identity.js";
import {
  createManagedGitHubProfileId,
  installManagedGitHubProfile,
  removeManagedGitHubProfile,
  resolveConfiguredGitHubToolIdentity,
  resolveGitHubToolIdentityStatus,
  resolveManagedGitHubProfileDir,
} from "../agents/github-tool-identity.js";
import { resolveConfigPath } from "../config/paths.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import type { GitHubToolIdentityConfig } from "../config/types.tools.js";
import { formatErrorMessage } from "../infra/errors.js";
import type { GatewayScheduler, GatewaySchedulerScope } from "../infra/gateway-scheduler.js";
import { warnGitHubOAuthDeprecation } from "../plugins/compat/github-oauth-deprecation.js";
import { getOrCreatePromise } from "../shared/lazy-promise.js";
import { settlesWithin } from "../shared/settle-within.js";
import { captureOpenClawStateWorkerContext } from "../state/openclaw-state-worker-context.js";
import { assertGitHubCliAvailable } from "./github-cli-preflight.js";
import { pollGitHubDeviceFlow, startGitHubDeviceFlow } from "./github-oauth-device-flow.js";
import {
  authorizationStillOwned,
  createGitHubOAuthRecordAuthority,
  configuredOAuthIdentities,
  MAINTENANCE_INTERVAL_MS,
  SHUTDOWN_DRAIN_TIMEOUT_MS,
} from "./github-oauth-lifecycle-helpers.js";
import { createGitHubOAuthRefresh } from "./github-oauth-refresh.js";
import { createPersonalGitHubOAuthLifecycle } from "./github-personal-oauth.js";
import { updateGitHubToolIdentityConfig } from "./github-tool-identity-config.js";

type GitHubOAuthLifecycle = ReturnType<typeof createGitHubOAuthLifecycle>;

let activeLifecycle: GitHubOAuthLifecycle | undefined;

export function installActiveGitHubOAuthLifecycle(lifecycle: GitHubOAuthLifecycle): () => void {
  activeLifecycle = lifecycle;
  return () => {
    if (activeLifecycle === lifecycle) {
      activeLifecycle = undefined;
    }
  };
}

export async function requestCurrentGitHubOAuthRefresh(agentId: string): Promise<void> {
  await activeLifecycle?.refreshEffectiveIdentity(agentId);
}

export async function requestCurrentPersonalGitHubRefresh(owner: string): Promise<void> {
  if (!activeLifecycle) {
    throw new Error("My GitHub lifecycle is unavailable; retry after Gateway startup.");
  }
  await activeLifecycle.personal.refresh(owner);
}

export function createGitHubOAuthLifecycle(params: {
  getConfig: () => OpenClawConfig;
  getPersistedConfig?: () => OpenClawConfig;
  warn: (message: string) => void;
  scheduler: GatewayScheduler;
}) {
  const storage = { context: captureOpenClawStateWorkerContext() };
  const stateDatabase = {
    path: storage.context.admission.databasePath,
    env: storage.context.environment,
  };
  const expectedConfigPath = resolveConfigPath();
  const readGitHubDeviceAuthorizationRecord = (id: string) => readDeviceRecord(id, storage);
  const inspectGitHubOAuthRecord = (id: string) => inspectOAuthRecord(id, storage);
  const listGitHubDeviceAuthorizationRecords = () => listDeviceRecords(storage);
  const listGitHubOAuthRecords = () => listOAuthRecords(storage);
  const writeGitHubDeviceAuthorizationRecord = (
    record: GitHubDeviceAuthorizationRecord,
    expected: GitHubDeviceAuthorizationRecord | null,
    assertCurrent?: () => void,
  ) => writeDeviceRecord(record, { ...storage, expected, assertCurrent });
  const writeGitHubOAuthRecord = (
    record: GitHubOAuthRecord,
    expected: GitHubOAuthRecord | null,
    assertCurrent?: () => void,
  ) => writeOAuthRecord(record, { ...storage, expected, assertCurrent });
  const personal = createPersonalGitHubOAuthLifecycle();
  const deviceController = new AbortController();
  const devicePolls = new Map<string, Promise<ToolsGitHubAuthorizePollResult>>();
  const committingRequests = new Set<string>();
  const cancellingRequests = new Map<string, object>();
  const pendingCleanup = new Map<
    string,
    { expected?: GitHubDeviceAuthorizationRecord; expectedValue?: string }
  >();
  const authorizationWrites = new Map<string, Promise<unknown>>();
  let maintenance: Promise<void> | undefined;
  let maintenanceScope: GatewaySchedulerScope | undefined;
  let stopping = false;
  const refresh = createGitHubOAuthRefresh({
    context: storage.context,
    getConfig: params.getConfig,
    warn: params.warn,
    isStopping: () => stopping,
  });
  const assertAuthorizationCurrent = (record: GitHubDeviceAuthorizationRecord) => {
    storage.context.admission.assertCurrent();
    if (
      stopping ||
      cancellingRequests.has(record.requestId) ||
      !authorizationStillOwned(params.getConfig(), record, stateDatabase)
    ) {
      throw new Error("GitHub authorization is no longer current.");
    }
  };
  const warnMaintenanceError = (error: unknown) => {
    params.warn(`GitHub OAuth maintenance failed; will retry: ${formatErrorMessage(error)}`);
  };

  const queueDeviceCleanup = async (
    requestId: string,
    expected?: GitHubDeviceAuthorizationRecord,
    expectedValue?: string,
  ) => {
    try {
      await deleteDeviceRecord(requestId, { ...storage, expected, expectedValue });
      pendingCleanup.delete(requestId);
    } catch {
      pendingCleanup.set(requestId, { expected, expectedValue });
    }
  };

  const queueOAuthCleanup = async (
    profileId: string,
    expected?: GitHubOAuthRecord,
    expectedValue?: string,
    assertCurrent?: () => void,
  ) => {
    clearNativeGitHubTokenCache();
    try {
      return await deleteOAuthRecord(profileId, {
        ...storage,
        expected,
        expectedValue,
        assertCurrent,
      });
    } catch {
      // Orphan cleanup scans every minute and after restart.
      return false;
    }
  };

  const status = async (agentId: string, selectedScope: GitHubIdentityScope) => {
    storage.context.admission.assertCurrent();
    const result = await resolveGitHubToolIdentityStatus({
      config: params.getConfig(),
      agentId,
      selectedScope,
      env: storage.context.environment,
    });
    storage.context.admission.assertCurrent();
    return result;
  };

  const reserveCancellation = (requestId: string) => {
    const token = {};
    cancellingRequests.set(requestId, token);
    return () => {
      const clear = () => {
        if (cancellingRequests.get(requestId) === token) {
          cancellingRequests.delete(requestId);
        }
      };
      const pending = devicePolls.get(requestId);
      if (pending) {
        void pending.then(clear, clear);
      } else {
        clear();
      }
    };
  };

  const installDeviceTokens = async (
    record: GitHubDeviceAuthorizationRecord,
    tokens: GitHubOAuthTokenPair,
  ): Promise<ToolsGitHubAuthorizePollResult> => {
    const current = params.getConfig();
    if (!authorizationStillOwned(current, record, stateDatabase)) {
      await queueDeviceCleanup(record.requestId, record);
      return { status: "failed", reason: "identity_changed" };
    }
    const profileId = createManagedGitHubProfileId();
    const profileDir = resolveManagedGitHubProfileDir({
      agentId: record.agentId,
      scope: record.scope,
      profileId,
      env: storage.context.environment,
    });
    let nextConfig = current;
    let metadataWritten = false;
    const completeAuthorization = async (
      config: OpenClawConfig,
    ): Promise<ToolsGitHubAuthorizePollResult> => {
      await queueDeviceCleanup(record.requestId, record);
      if (record.expectedIdentity?.kind === "oauth") {
        await queueOAuthCleanup(record.expectedIdentity.profileId);
      }
      const githubStatus = await resolveGitHubToolIdentityStatus({
        config,
        agentId: record.agentId,
        selectedScope: record.scope,
        env: storage.context.environment,
      });
      storage.context.admission.assertCurrent();
      return { status: "success", githubStatus };
    };
    try {
      await installManagedGitHubProfile({
        profileDir,
        token: tokens.accessToken,
        retainProfileOnCommitFailure: true,
        assertCurrent: () => assertAuthorizationCurrent(record),
        commitConfig: async (account) => {
          const pending = await readGitHubDeviceAuthorizationRecord(record.requestId);
          if (
            !pending ||
            pending.createdAtMs !== record.createdAtMs ||
            pending.deviceCode !== record.deviceCode ||
            !isDeepStrictEqual(pending.expectedIdentity, record.expectedIdentity) ||
            stopping ||
            cancellingRequests.has(record.requestId) ||
            !authorizationStillOwned(params.getConfig(), record, stateDatabase)
          ) {
            throw new Error("GitHub authorization is no longer pending.");
          }
          committingRequests.add(record.requestId);
          const pendingInitial = {
            requestId: record.requestId,
            scope: record.scope,
            agentId: record.agentId,
            expectedIdentity: record.expectedIdentity,
            ...(record.agentLifecycleBinding
              ? { agentLifecycleBinding: record.agentLifecycleBinding }
              : {}),
          } as const;
          await writeGitHubOAuthRecord(
            createGitHubOAuthRecord({
              profileId,
              scope: record.scope,
              agentId: record.agentId,
              account,
              tokens,
              now: Date.now(),
              pendingInitial,
            }),
            null,
          );
          metadataWritten = true;
          assertAuthorizationCurrent(record);
          const identity: GitHubToolIdentityConfig = {
            profileId,
            kind: "oauth",
            gitAuthor: record.expectedIdentity?.gitAuthor
              ? structuredClone(record.expectedIdentity.gitAuthor)
              : {
                  name: account.login,
                  email: `${account.accountId}+${account.login}@users.noreply.github.com`,
                },
          };
          nextConfig = await updateGitHubToolIdentityConfig({
            scope: record.scope,
            agentId: record.agentId,
            identity,
            expectedIdentity: record.expectedIdentity,
            expectedConfigPath,
            stateDatabase,
            ...(record.agentLifecycleBinding
              ? { agentLifecycleBinding: record.agentLifecycleBinding }
              : {}),
          });
          const inspected = await inspectGitHubOAuthRecord(profileId);
          if (inspected.state !== "valid" || !inspected.record.pendingInitial) {
            throw new Error("GitHub OAuth initial record is unavailable.");
          }
          await writeGitHubOAuthRecord(
            { ...inspected.record, pendingInitial: undefined },
            inspected.record,
          );
        },
      });
    } catch {
      if (metadataWritten) {
        try {
          const persistedConfig = params.getPersistedConfig?.();
          if (!persistedConfig) {
            throw new Error("Authoritative persisted config is unavailable.");
          }
          const persistedIdentity = resolveConfiguredGitHubToolIdentity({
            config: persistedConfig,
            scope: record.scope,
            agentId: record.agentId,
          });
          if (persistedIdentity?.profileId === profileId && persistedIdentity.kind === "oauth") {
            const inspected = await inspectGitHubOAuthRecord(profileId);
            if (inspected.state === "valid" && inspected.record.pendingInitial) {
              await writeGitHubOAuthRecord(
                { ...inspected.record, pendingInitial: undefined },
                inspected.record,
              );
            }
            return await completeAuthorization(persistedConfig);
          }
        } catch {
          // The commit outcome is unknown. Preserve the profile and refresh
          // record so lifecycle reconciliation can decide from durable config.
          await queueDeviceCleanup(record.requestId, record);
          return { status: "failed", reason: "setup_failed" };
        }
      }
      if (metadataWritten) {
        await queueOAuthCleanup(profileId);
      }
      await removeManagedGitHubProfile(profileDir).catch(() => undefined);
      await queueDeviceCleanup(record.requestId, record);
      return { status: "failed", reason: "setup_failed" };
    } finally {
      committingRequests.delete(record.requestId);
    }
    return await completeAuthorization(nextConfig);
  };

  const pollOnce = async (requestId: string): Promise<ToolsGitHubAuthorizePollResult> => {
    if (cancellingRequests.has(requestId)) {
      return { status: "expired" };
    }
    const record = await readGitHubDeviceAuthorizationRecord(requestId);
    const now = Date.now();
    if (cancellingRequests.has(requestId)) {
      return { status: "expired" };
    }
    if (!record || record.expiresAtMs <= now) {
      await queueDeviceCleanup(requestId, record);
      return { status: "expired" };
    }
    if (stopping || !authorizationStillOwned(params.getConfig(), record, stateDatabase)) {
      await queueDeviceCleanup(requestId, record);
      return { status: "failed", reason: "identity_changed" };
    }
    const result = await pollGitHubDeviceFlow(record, deviceController.signal);
    const currentRecord = await readGitHubDeviceAuthorizationRecord(requestId);
    if (!currentRecord || cancellingRequests.has(requestId)) {
      return { status: "expired" };
    }
    if (
      currentRecord.deviceCode !== record.deviceCode ||
      currentRecord.createdAtMs !== record.createdAtMs ||
      !isDeepStrictEqual(currentRecord.expectedIdentity, record.expectedIdentity) ||
      stopping ||
      !authorizationStillOwned(params.getConfig(), currentRecord, stateDatabase)
    ) {
      await queueDeviceCleanup(requestId, currentRecord);
      return { status: "failed", reason: "identity_changed" };
    }
    if (result.kind === "authorized") {
      return await installDeviceTokens(currentRecord, result.tokens);
    }
    if (result.kind === "waiting") {
      await writeGitHubDeviceAuthorizationRecord(
        {
          ...currentRecord,
          pollIntervalMs: result.pollIntervalMs,
          nextPollAtMs: result.nextPollAtMs,
        },
        currentRecord,
        () => assertAuthorizationCurrent(currentRecord),
      );
    } else {
      await queueDeviceCleanup(requestId, currentRecord);
    }
    return result.result;
  };

  const reconcileRecords = async (): Promise<void> => {
    for (const {
      requestId,
      record,
      expectedValue,
    } of await listGitHubDeviceAuthorizationRecords()) {
      if (!record || record.expiresAtMs <= Date.now()) {
        await queueDeviceCleanup(requestId, record, expectedValue);
      }
    }
    for (const { profileId, record, expectedValue } of await listGitHubOAuthRecords()) {
      if (!record) {
        await queueOAuthCleanup(profileId, undefined, expectedValue);
        continue;
      }
      const authority = createGitHubOAuthRecordAuthority({
        record,
        database: stateDatabase,
        getConfig: params.getConfig,
        getPersistedConfig: params.getPersistedConfig,
        isCommitting: (requestId) => committingRequests.has(requestId),
        assertCurrent: () => storage.context.admission.assertCurrent(),
      });
      let ownership: ReturnType<typeof authority.read>;
      try {
        ownership = authority.read();
      } catch {
        continue;
      }
      if (record.pendingInitial) {
        if (ownership === "selected") {
          await writeGitHubOAuthRecord(
            { ...record, pendingInitial: undefined },
            record,
            authority.assertSelected,
          );
          if (record.pendingInitial.expectedIdentity?.kind === "oauth") {
            await queueOAuthCleanup(record.pendingInitial.expectedIdentity.profileId);
          }
          continue;
        }
        if (
          ownership !== "unselected" ||
          !(await queueOAuthCleanup(profileId, record, undefined, authority.assertUnselected))
        ) {
          continue;
        }
        try {
          authority.assertUnselected();
          await removeManagedGitHubProfile(
            resolveManagedGitHubProfileDir({
              agentId: record.agentId,
              scope: record.scope,
              profileId,
              env: storage.context.environment,
            }),
          );
        } catch {
          // A newly selected profile remains intact; failed removals are retried by startup cleanup.
        }
        continue;
      }
      const current = resolveConfiguredGitHubToolIdentity({
        config: params.getConfig(),
        ...record,
      });
      if (
        ownership !== "selected" ||
        current?.profileId !== profileId ||
        current.kind !== "oauth"
      ) {
        await queueOAuthCleanup(profileId, record, undefined, authority.assertUnselected);
        continue;
      }
      if (record.pendingRefresh && !stopping) {
        await refresh.request({
          scope: record.scope,
          agentId: record.agentId,
          identity: { ...current, kind: "oauth" },
        });
      }
    }
  };

  const runMaintenance = async (): Promise<void> => {
    for (const [requestId, { expected, expectedValue }] of pendingCleanup) {
      await queueDeviceCleanup(requestId, expected, expectedValue);
    }
    if (refresh.hasPending()) {
      await refresh.retryPending();
    }
    await reconcileRecords();
    for (const configured of configuredOAuthIdentities(params.getConfig())) {
      if (stopping) {
        break;
      }
      await refresh.request(configured);
    }
  };

  const maintain = (): Promise<void> => {
    if (stopping && !maintenance) {
      return Promise.resolve();
    }
    maintenance ??= runMaintenance()
      .catch(warnMaintenanceError)
      .finally(() => {
        maintenance = undefined;
      });
    return maintenance;
  };

  return {
    personal,
    startAuthorization: async (input: {
      scope: GitHubIdentityScope;
      agentId: string;
    }): Promise<ToolsGitHubAuthorizeStartResult> => {
      if (stopping) {
        throw new Error("GitHub authorization lifecycle is stopping.");
      }
      assertGitHubCliAvailable();
      const expectedIdentity = structuredClone(
        resolveConfiguredGitHubToolIdentity({ config: params.getConfig(), ...input }) ?? null,
      );
      const agentLifecycleBinding =
        input.scope === "agent"
          ? captureAgentLifecycleBinding(params.getConfig(), input.agentId, stateDatabase)
          : undefined;
      if (input.scope === "agent" && !agentLifecycleBinding) {
        throw new Error("GitHub authorization requires an active agent.");
      }
      const authorization = await startGitHubDeviceFlow(deviceController.signal);
      const requestId = `github-device-${randomBytes(16).toString("hex")}`;
      const record: GitHubDeviceAuthorizationRecord = {
        version: 1,
        requestId,
        ...authorization,
        agentId: input.agentId,
        scope: input.scope,
        expectedIdentity,
        ...(agentLifecycleBinding ? { agentLifecycleBinding } : {}),
      };
      const assertCurrent = () => assertAuthorizationCurrent(record);
      assertCurrent();
      const key = `${input.scope}:${input.scope === "system" ? "" : input.agentId}`;
      const write = (authorizationWrites.get(key) ?? Promise.resolve()).then(async () => {
        assertCurrent();
        const previous = await listGitHubDeviceAuthorizationRecords();
        assertCurrent();
        for (const existing of previous) {
          if (existing.record?.scope === input.scope && existing.record.agentId === input.agentId) {
            await queueDeviceCleanup(existing.requestId, existing.record);
            assertCurrent();
          }
        }
        await writeGitHubDeviceAuthorizationRecord(record, null, assertCurrent);
        assertCurrent();
      });
      const settled = write.catch(() => undefined);
      authorizationWrites.set(key, settled);
      try {
        await write;
      } finally {
        if (authorizationWrites.get(key) === settled) {
          authorizationWrites.delete(key);
        }
      }
      return {
        requestId,
        userCode: authorization.userCode,
        verificationUri: authorization.verificationUri,
        expiresInMs: Math.max(0, authorization.expiresAtMs - Date.now()),
        pollAfterMs: authorization.pollIntervalMs,
      };
    },
    pollAuthorization: (requestId: string): Promise<ToolsGitHubAuthorizePollResult> =>
      getOrCreatePromise(devicePolls, requestId, () => pollOnce(requestId), {
        evictOnSettled: true,
      }),
    /** @deprecated Await cancelAuthorizationAsync; removed at the next Plugin SDK major. */
    cancelAuthorization: (requestId: string): boolean => {
      warnGitHubOAuthDeprecation("cancelAuthorization");
      if (committingRequests.has(requestId)) {
        return false;
      }
      const release = reserveCancellation(requestId);
      try {
        return cancelGitHubDeviceAuthorizationRecordForSdk(requestId, storage.context, () =>
          pendingCleanup.set(requestId, {}),
        );
      } finally {
        release();
      }
    },
    cancelAuthorizationAsync: async (requestId: string): Promise<boolean> => {
      if (committingRequests.has(requestId)) {
        return false;
      }
      const release = reserveCancellation(requestId);
      try {
        const record = await readGitHubDeviceAuthorizationRecord(requestId);
        if (!record || committingRequests.has(requestId)) {
          return false;
        }
        return await deleteDeviceRecord(requestId, {
          ...storage,
          expected: record,
          assertCurrent: () => {
            if (committingRequests.has(requestId)) {
              throw new Error("GitHub authorization is already committing.");
            }
          },
        });
      } catch (error) {
        if (committingRequests.has(requestId)) {
          return false;
        }
        throw error;
      } finally {
        release();
      }
    },
    status,
    /** @deprecated Await retireProfileAsync; removed at the next Plugin SDK major. */
    retireProfile: (profileId: string): void => {
      warnGitHubOAuthDeprecation("retireProfile");
      clearNativeGitHubTokenCache();
      try {
        retireGitHubOAuthRecordForSdk(profileId, storage.context);
      } catch {
        // Orphan cleanup retries through the asynchronous maintenance owner.
      }
    },
    retireProfileAsync: async (profileId: string): Promise<void> => {
      await queueOAuthCleanup(profileId);
    },
    refreshEffectiveIdentity: async (agentId: string): Promise<void> => {
      if (stopping) {
        return;
      }
      const config = params.getConfig();
      const agent = resolveAgentConfig(config, agentId)?.tools?.github;
      const identity = agent ?? config.tools?.github;
      if (identity?.kind !== "oauth") {
        return;
      }
      await refresh.request({
        scope: agent ? "agent" : "system",
        agentId,
        identity: { ...identity, kind: "oauth" },
      });
    },
    maintain: async () => {
      await Promise.all([maintain(), personal.maintain()]).catch(warnMaintenanceError);
    },
    start: () => {
      if (stopping || maintenanceScope) {
        return;
      }
      const scheduler = params.scheduler.scope();
      maintenanceScope = scheduler;
      // Personal file cleanup must not delay System/agent refresh.
      scheduler.schedule({
        id: "maintenance:github-oauth",
        delayMs: 0,
        everyMs: MAINTENANCE_INTERVAL_MS,
        run: maintain,
      });
      scheduler.schedule({
        id: "maintenance:github-personal-oauth",
        delayMs: 0,
        everyMs: MAINTENANCE_INTERVAL_MS,
        run: () => personal.maintain().catch(warnMaintenanceError),
      });
    },
    stop: async () => {
      clearGitHubCredentialVerificationCache();
      stopping = true;
      maintenanceScope?.beginClose();
      deviceController.abort();
      const drain = (async () => {
        await Promise.allSettled([
          personal.stop(),
          maintenanceScope?.stop(),
          maintenance,
          ...devicePolls.values(),
          refresh.settlement(),
          ...authorizationWrites.values(),
        ]);
        if (refresh.hasPending()) {
          await runMaintenance();
        }
      })();
      await settlesWithin(drain, SHUTDOWN_DRAIN_TIMEOUT_MS);
    },
  };
}
