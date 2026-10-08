import { isDeepStrictEqual } from "node:util";
import { refreshGitHubOAuthToken } from "../agents/github-oauth-client.js";
import {
  GitHubOAuthRecordChangedError,
  inspectGitHubOAuthRecord as inspectOAuthRecord,
  type GitHubOAuthRecord,
  writeGitHubOAuthRecord as writeOAuthRecord,
} from "../agents/github-oauth-records.js";
import type { GitHubToolAccount } from "../agents/github-tool-account.js";
import {
  GitHubAccountMismatchError,
  refreshManagedGitHubProfile,
  resolveConfiguredGitHubToolIdentity,
  resolveManagedGitHubProfileDir,
} from "../agents/github-tool-identity.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { formatErrorMessage } from "../infra/errors.js";
import { getOrCreatePromise } from "../shared/lazy-promise.js";
import type { OpenClawStateWorkerContext } from "../state/openclaw-state-worker-context.types.js";
import { REFRESH_SKEW_MS, type ConfiguredOAuthIdentity } from "./github-oauth-lifecycle-helpers.js";

/** Accepted token rotations retain their physical store and settle independently of new admission. */
export function createGitHubOAuthRefresh(params: {
  context: OpenClawStateWorkerContext;
  getConfig: () => OpenClawConfig;
  isStopping: () => boolean;
  warn: (message: string) => void;
}) {
  const storage = { context: params.context };
  const inspectGitHubOAuthRecord = (id: string) => inspectOAuthRecord(id, storage);
  const writeGitHubOAuthRecord = (record: GitHubOAuthRecord, expected: GitHubOAuthRecord | null) =>
    writeOAuthRecord(record, { ...storage, expected });
  const refreshes = new Map<string, Promise<void>>();
  const pendingRefreshes = new Map<
    string,
    {
      record: GitHubOAuthRecord & { pendingRefresh: true };
      previous: GitHubOAuthRecord;
      accessToken: string;
    }
  >();
  const applyPendingRefresh = async (
    record: GitHubOAuthRecord,
    accessToken: string,
  ): Promise<void> => {
    const profileDir = resolveManagedGitHubProfileDir({
      agentId: record.agentId,
      scope: record.scope,
      profileId: record.profileId,
      env: storage.context.environment,
    });
    let account: GitHubToolAccount;
    try {
      account = await refreshManagedGitHubProfile({
        profileDir,
        token: accessToken,
        expectedAccountId: record.accountId,
        assertCurrent: () => storage.context.admission.assertCurrent(),
      });
    } catch (error) {
      if (error instanceof GitHubAccountMismatchError) {
        await writeGitHubOAuthRecord(
          {
            ...record,
            pendingRefresh: undefined,
            refreshFailure: "expired",
          },
          record,
        );
      }
      return;
    }
    await writeGitHubOAuthRecord(
      {
        ...record,
        login: account.login,
        pendingRefresh: undefined,
        refreshFailure: undefined,
      },
      record,
    );
  };

  const persistRotation = async (
    profileId: string,
    pending: NonNullable<ReturnType<typeof pendingRefreshes.get>>,
  ) => {
    try {
      await writeGitHubOAuthRecord(pending.record, pending.previous);
      pendingRefreshes.delete(profileId);
      return pending.record;
    } catch (error) {
      if (!(error instanceof GitHubOAuthRecordChangedError)) {
        return undefined;
      }
      const current = await inspectGitHubOAuthRecord(profileId);
      // A lost reply can leave our exact postimage committed; settle it without replaying rotation.
      if (
        current.state === "valid" &&
        current.record.pendingRefresh === true &&
        isDeepStrictEqual(current.record, pending.record)
      ) {
        pendingRefreshes.delete(profileId);
        return current.record;
      }
      pendingRefreshes.delete(profileId);
      return undefined;
    }
  };

  const refreshOne = async (configured: ConfiguredOAuthIdentity): Promise<void> => {
    const profileId = configured.identity.profileId;
    const inspected = await inspectGitHubOAuthRecord(profileId);
    if (params.isStopping()) {
      return;
    }
    if (inspected.state !== "valid") {
      return;
    }
    const currentRecord = inspected.record;
    if (currentRecord.pendingInitial) {
      return;
    }
    const now = Date.now();
    if (currentRecord.refreshFailure === "expired" || currentRecord.refreshExpiresAtMs <= now) {
      return;
    }
    if (!currentRecord.pendingRefresh && currentRecord.accessExpiresAtMs > now + REFRESH_SKEW_MS) {
      return;
    }
    const currentIdentity = resolveConfiguredGitHubToolIdentity({
      config: params.getConfig(),
      ...currentRecord,
    });
    if (currentIdentity?.kind !== "oauth" || currentIdentity.profileId !== profileId) {
      return;
    }
    let refreshed;
    try {
      refreshed = await refreshGitHubOAuthToken({
        refreshToken: currentRecord.refreshToken,
      });
    } catch {
      if (!currentRecord.pendingRefresh) {
        await writeGitHubOAuthRecord({ ...currentRecord, refreshFailure: "failed" }, currentRecord);
      }
      return;
    }
    if (refreshed.status === "error") {
      const refreshFailure = refreshed.code === "bad_refresh_token" ? "expired" : "failed";
      await writeGitHubOAuthRecord(
        {
          ...currentRecord,
          pendingRefresh: undefined,
          refreshFailure,
        },
        currentRecord,
      );
      return;
    }
    const rotatedRecord: GitHubOAuthRecord & { pendingRefresh: true } = {
      ...currentRecord,
      refreshToken: refreshed.tokens.refreshToken,
      accessExpiresAtMs: now + refreshed.tokens.expiresInSeconds * 1_000,
      refreshExpiresAtMs: now + refreshed.tokens.refreshTokenExpiresInSeconds * 1_000,
      scopes: refreshed.tokens.scopes,
      pendingRefresh: true,
      pendingInitial: undefined,
      refreshFailure: undefined,
    };
    const pending = {
      record: rotatedRecord,
      previous: currentRecord,
      accessToken: refreshed.tokens.accessToken,
    };
    pendingRefreshes.set(profileId, pending);
    const persisted = await persistRotation(profileId, pending);
    if (persisted) {
      await applyPendingRefresh(persisted, refreshed.tokens.accessToken);
    }
  };

  const settlePending = async (profileId: string): Promise<boolean> => {
    const pending = pendingRefreshes.get(profileId);
    if (!pending) {
      return false;
    }
    const persisted = await persistRotation(profileId, pending);
    if (persisted) {
      await applyPendingRefresh(persisted, pending.accessToken);
    }
    return true;
  };
  const withRefresh = (profileId: string, run: () => Promise<void>): Promise<void> =>
    getOrCreatePromise(refreshes, profileId, run, { evictOnSettled: true });
  const requestRefresh = (configured: ConfiguredOAuthIdentity): Promise<void> =>
    withRefresh(configured.identity.profileId, async () => {
      try {
        if (!(await settlePending(configured.identity.profileId))) {
          await refreshOne(configured);
        }
      } catch (error) {
        params.warn(`GitHub OAuth refresh failed; will retry: ${formatErrorMessage(error)}`);
      }
    });

  return {
    request: requestRefresh,
    async retryPending(): Promise<void> {
      for (const profileId of [...pendingRefreshes.keys()].toSorted()) {
        try {
          await withRefresh(profileId, async () => {
            await settlePending(profileId);
          });
        } catch {
          // Retain the rotated refresh token in memory for the next maintenance pass.
        }
      }
    },
    async settlement(): Promise<void> {
      await Promise.allSettled(refreshes.values());
    },
    hasPending: () => pendingRefreshes.size > 0,
  };
}
