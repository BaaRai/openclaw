import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import type {
  UsersGitHubAuthorizePollResult,
  UsersGitHubAuthorizeStartResult,
} from "../../packages/gateway-protocol/src/schema/users.js";
import {
  refreshGitHubOAuthToken,
  type GitHubOAuthTokenPair,
} from "../agents/github-oauth-client.js";
import { clearNativeGitHubTokenCache } from "../agents/github-read-identity.js";
import {
  createManagedGitHubProfileId,
  installManagedGitHubProfile,
  refreshManagedGitHubProfile,
  removeManagedGitHubProfile,
  resolveManagedGitHubProfileDir,
  resolveManagedGitHubProfileRoot,
} from "../agents/github-tool-identity.js";
import { hasErrnoCode } from "../infra/errno.js";
import { getOrCreatePromise } from "../shared/lazy-promise.js";
import { openOpenClawStateDatabase } from "../state/openclaw-state-db.js";
import {
  type OpenClawStateLeaseContext,
  withOpenClawStateLease,
} from "../state/openclaw-state-lease.js";
import { captureOpenClawStateWorkerContext } from "../state/openclaw-state-worker-context.js";
import type { OpenClawStateWorkerContext } from "../state/openclaw-state-worker-context.types.js";
import {
  listUserGitHubConnections,
  disconnectUserGitHubConnection,
  observeUserGitHubProfileRetirement,
  readUserGitHubConnection,
  readUserGitHubConnectionAsync,
  resolvePersonalGitHubOwner,
  replaceUserGitHubConnection,
  mutateUserGitHubConnectionAsync,
  updateUserGitHubRefresh,
  type UserGitHubConnection,
  type UserGitHubConnected,
  type UserGitHubDevice,
} from "../state/user-github-connections.js";
import {
  connectionProfiles,
  listUserGitHubConnectionsInDatabase,
} from "../state/user-github-connections.kernel.js";
import type { UserGitHubRole } from "../state/user-github-connections.worker-contract.js";
import { assertGitHubCliAvailable } from "./github-cli-preflight.js";
import { pollGitHubDeviceFlow, startGitHubDeviceFlow } from "./github-oauth-device-flow.js";
import {
  createPersonalGitHubSdkAdapters,
  type PersonalGitHubOperationAction,
} from "./github-personal-oauth.compat.js";
import {
  personalGitHubStatus,
  projectPending,
  revalidatePersonalGitHubStatus,
  resolvePersonalGitHubStatus,
  type PersonalGitHubAction,
} from "./github-personal-status.js";

export type PersonalGitHubActionV2 = PersonalGitHubAction & {
  assertMutationCurrent: (role?: UserGitHubRole) => void;
};
const profileDir = (profileId: string, context: OpenClawStateWorkerContext) =>
  resolveManagedGitHubProfileDir({
    agentId: "",
    scope: "personal",
    profileId,
    env: context.environment,
  });
const withProfileLease = <T>(
  profileId: string,
  context: OpenClawStateWorkerContext,
  run: (assertOwned: () => void, lease: OpenClawStateLeaseContext) => Promise<T>,
) =>
  withOpenClawStateLease(
    {
      scope: "personal-github-profile",
      key: profileId,
      database: {
        scope: "shared",
        options: { path: context.admission.databasePath, env: context.environment },
      },
      leaseMs: 60000,
      waitMs: 30000,
    },
    async (lease) => await run(() => lease.assertOwned(), lease),
  );

function requirePending(
  record: UserGitHubConnection | undefined,
  generation: string,
  requestId: string,
): UserGitHubConnection & { pending: NonNullable<UserGitHubConnection["pending"]> } {
  if (
    !record?.pending ||
    record.generation !== generation ||
    record.pending.requestId !== requestId ||
    record.pending.expiresAtMs <= Date.now()
  ) {
    throw new Error("My GitHub authorization changed or expired; start again.");
  }
  return { ...record, pending: record.pending };
}

function needsRefresh(selection: UserGitHubConnected): boolean {
  return (
    Boolean(selection.refresh?.tokens) ||
    (selection.refreshFailure !== "expired" &&
      selection.refreshExpiresAtMs > Date.now() &&
      (Boolean(selection.refresh) || selection.accessExpiresAtMs <= Date.now() + 600000))
  );
}

/** Personal adapters share device transport and profile materialization with System/agent OAuth. */
export function createPersonalGitHubOAuthLifecycle() {
  const abort = new AbortController();
  const polls = new Map<string, Promise<UsersGitHubAuthorizePollResult>>();
  const refreshes = new Map<string, Promise<void>>();
  const rotated = new Map<
    string,
    {
      owner: string;
      context: OpenClawStateWorkerContext;
      profileId: string;
      operationId: string;
      tokens: GitHubOAuthTokenPair;
      receivedAtMs: number;
    }
  >();
  type Retirement = { key: string; id: string; context: OpenClawStateWorkerContext };
  const retirements = new Map<string, Retirement>();
  const queueRetirement = (id: string, context: OpenClawStateWorkerContext): Retirement => {
    const key = JSON.stringify([context.admission.identity.key, profileDir(id, context)]);
    const retirement = retirements.get(key) ?? { key, id, context };
    retirements.set(key, retirement);
    return retirement;
  };
  const cleanups = new Map<string, Promise<void>>();
  let stopped = false;
  let inspectedProfiles = false;
  const profileIsReferenced = (id: string, context: OpenClawStateWorkerContext) =>
    listUserGitHubConnectionsInDatabase(
      openOpenClawStateDatabase({
        path: context.admission.databasePath,
        env: context.environment,
      }).db,
    ).some(
      ({ connection }) =>
        (connection.selection.kind === "connected" && connection.selection.profileId === id) ||
        (connection.pending?.kind === "device" && connection.pending.candidate?.profileId === id),
    );
  const assertRunning = () => {
    if (stopped) {
      throw new Error("GitHub authorization is stopping.");
    }
  };
  const retire = async ({ key, id, context }: Retirement) => {
    await getOrCreatePromise(
      cleanups,
      key,
      () =>
        withProfileLease(id, context, async (assertOwned) => {
          context.admission.assertCurrent();
          assertOwned();
          if (profileIsReferenced(id, context)) {
            return;
          }
          await removeManagedGitHubProfile(profileDir(id, context));
        }).then(
          () => {
            retirements.delete(key);
          },
          () => {
            /* Retain the original source for maintenance retry. */
          },
        ),
      { evictOnSettled: true },
    );
  };
  const unobserve = observeUserGitHubProfileRetirement(({ profileIds, context }) => {
    for (const id of profileIds) {
      void retire(queueRetirement(id, context));
    }
  });
  const guard = (action: PersonalGitHubAction) => {
    assertRunning();
    action.assertCurrent();
  };

  const workerAction = (action: PersonalGitHubActionV2): PersonalGitHubOperationAction => ({
    ...action,
    mutate: (input, context, lease) =>
      mutateUserGitHubConnectionAsync(
        input,
        (role) => {
          assertRunning();
          action.assertMutationCurrent(role);
        },
        { context, lease },
      ),
  });

  const install = async (
    action: PersonalGitHubOperationAction,
    generation: string,
    pending: UserGitHubDevice,
    context: OpenClawStateWorkerContext,
  ): Promise<UsersGitHubAuthorizePollResult> => {
    const candidate = pending.candidate;
    if (!candidate) {
      throw new Error("My GitHub authorization has no candidate.");
    }
    const assertCurrent = () => {
      guard(action);
      const record = requirePending(
        readUserGitHubConnection(action.owner, {
          path: context.admission.databasePath,
          env: context.environment,
        }),
        generation,
        pending.requestId,
      );
      if (
        record.pending.kind !== "device" ||
        record.pending.candidate?.profileId !== candidate.profileId
      ) {
        throw new Error("My GitHub authorization changed.");
      }
    };
    try {
      await withProfileLease(candidate.profileId, context, async (assertOwned, lease) => {
        const assertInstall = () => {
          assertOwned();
          assertCurrent();
        };
        assertInstall();
        // A prior attempt may have materialized the inactive candidate before losing its response.
        await removeManagedGitHubProfile(profileDir(candidate.profileId, context));
        assertInstall();
        await installManagedGitHubProfile({
          profileDir: profileDir(candidate.profileId, context),
          token: candidate.tokens.accessToken,
          assertCurrent: assertInstall,
          commitConfig: async (account) => {
            await action.mutate(
              {
                kind: "install",
                owner: action.owner,
                generation,
                requestId: pending.requestId,
                profileId: candidate.profileId,
                account,
              },
              context,
              lease,
            );
          },
        });
      });
      guard(action);
      return { status: "success", personal: personalGitHubStatus(action) };
    } catch {
      guard(action);
      return { status: "failed", reason: "setup_failed" };
    }
  };

  const pollOnce = async (
    action: PersonalGitHubOperationAction,
    initial: UserGitHubConnection,
    requestId: string,
    context: OpenClawStateWorkerContext,
  ): Promise<UsersGitHubAuthorizePollResult> => {
    const pending = initial.pending;
    if (!pending || pending.requestId !== requestId || pending.expiresAtMs <= Date.now()) {
      return { status: "expired" };
    }
    if (pending.kind === "starting") {
      return { status: "pending", retryAfterMs: 1000 };
    }
    if (pending.candidate) {
      return await install(action, initial.generation, pending, context);
    }
    const polled = await pollGitHubDeviceFlow(pending, abort.signal);
    guard(action);
    let next: UserGitHubConnection;
    try {
      const owned = requirePending(initial, initial.generation, requestId);
      if (owned.pending.kind !== "device" || owned.pending.deviceCode !== pending.deviceCode) {
        throw new Error("My GitHub authorization changed.");
      }
      const changed = await action.mutate(
        {
          kind: "pending",
          owner: action.owner,
          generation: initial.generation,
          expectedPending: owned.pending,
          pending:
            polled.kind === "terminal"
              ? undefined
              : {
                  ...owned.pending,
                  ...(polled.kind === "authorized"
                    ? {
                        candidate: {
                          receivedAtMs: Date.now(),
                          profileId: createManagedGitHubProfileId(),
                          tokens: polled.tokens,
                        },
                      }
                    : {
                        pollIntervalMs: polled.pollIntervalMs,
                        nextPollAtMs: polled.nextPollAtMs,
                      }),
                },
        },
        context,
      );
      if (!changed.connection) {
        throw new Error("My GitHub authorization returned no connection.");
      }
      next = changed.connection;
    } catch {
      guard(action);
      return { status: "failed", reason: "identity_changed" };
    }
    if (polled.kind !== "authorized") {
      return polled.result;
    }
    if (next.pending?.kind !== "device") {
      throw new Error("My GitHub authorization changed.");
    }
    return await install(action, next.generation, next.pending, context);
  };

  const persistRotation = (
    pending: NonNullable<ReturnType<typeof rotated.get>>,
  ): Promise<boolean> =>
    updateUserGitHubRefresh(
      {
        owner: pending.owner,
        profileId: pending.profileId,
        operationId: pending.operationId,
        update: { kind: "rotated", tokens: pending.tokens, receivedAtMs: pending.receivedAtMs },
      },
      { context: pending.context },
    );
  const materializeRefresh = async (
    owner: string,
    id: string,
    operationId: string,
    assertOwned: () => void,
    lease: OpenClawStateLeaseContext,
    context: OpenClawStateWorkerContext,
  ): Promise<void> => {
    const readExact = () => {
      assertOwned();
      context.admission.assertCurrent();
      const database = { path: context.admission.databasePath, env: context.environment };
      const canonical = resolvePersonalGitHubOwner(owner, openOpenClawStateDatabase(database).db);
      const selection = canonical
        ? readUserGitHubConnection(canonical, database)?.selection
        : undefined;
      if (
        selection?.kind !== "connected" ||
        selection.profileId !== id ||
        selection.refresh?.operationId !== operationId ||
        !selection.refresh.tokens
      ) {
        throw new Error("My GitHub refresh ownership changed.");
      }
      return selection;
    };
    const current = readExact();
    const account = await refreshManagedGitHubProfile({
      profileDir: profileDir(id, context),
      token: current.refresh!.tokens!.accessToken,
      expectedAccountId: current.accountId,
      assertCurrent: () => {
        readExact();
      },
    });
    assertOwned();
    await updateUserGitHubRefresh(
      {
        owner,
        profileId: id,
        operationId,
        update: { kind: "materialized", login: account.login },
      },
      { lease, context },
    );
  };

  const refresh = async (
    owner: string,
    context = captureOpenClawStateWorkerContext(),
  ): Promise<void> => {
    assertRunning();
    const initial = (await readUserGitHubConnectionAsync(owner, { context }))?.selection;
    assertRunning();
    if (initial?.kind !== "connected") {
      return;
    }
    const id = initial.profileId;
    if (!rotated.has(id) && !needsRefresh(initial)) {
      return;
    }
    await getOrCreatePromise(
      refreshes,
      id,
      () =>
        withProfileLease(id, context, async (assertOwned, lease) => {
          const memory = rotated.get(id);
          if (memory) {
            if (!(await persistRotation(memory))) {
              rotated.delete(id);
              return;
            }
            rotated.delete(id);
          }
          const record = await readUserGitHubConnectionAsync(owner, { context });
          assertOwned();
          const selection = record?.selection;
          if (
            !record ||
            selection?.kind !== "connected" ||
            selection.profileId !== id ||
            !needsRefresh(selection)
          ) {
            return;
          }
          if (selection.refresh?.tokens) {
            await materializeRefresh(
              owner,
              id,
              selection.refresh.operationId,
              assertOwned,
              lease,
              context,
            );
            return;
          }
          const operationId = selection.refresh?.operationId ?? randomUUID();
          await replaceUserGitHubConnection(
            owner,
            {
              expected: record,
              next: { ...record, selection: { ...selection, refresh: { operationId } } },
            },
            assertRunning,
            { lease, context },
          );
          assertOwned();
          let result;
          try {
            // Refresh rotates remote credentials: shutdown drains this bounded exchange, never aborts it.
            result = await refreshGitHubOAuthToken({
              refreshToken: selection.refreshToken,
            });
          } catch {
            await updateUserGitHubRefresh(
              {
                owner,
                profileId: id,
                operationId,
                update: { kind: "failed", failure: "failed" },
              },
              { context },
            );
            return;
          }
          if (result.status === "error") {
            await updateUserGitHubRefresh(
              {
                owner,
                profileId: id,
                operationId,
                update: {
                  kind: "failed",
                  failure: result.code === "bad_refresh_token" ? "expired" : "failed",
                },
              },
              { context },
            );
            return;
          }
          // Persist remote rotation even if the initiating request closed or its profile merged.
          // The exact operation CAS fences disconnect/replacement; memory retries use that same CAS.
          const pending = {
            owner,
            context,
            profileId: id,
            operationId,
            tokens: result.tokens,
            receivedAtMs: Date.now(),
          };
          rotated.set(id, pending);
          if (!(await persistRotation(pending))) {
            rotated.delete(id);
            return;
          }
          rotated.delete(id);
          await materializeRefresh(owner, id, operationId, assertOwned, lease, context);
        }),
      { evictOnSettled: true },
    );
  };

  let maintenance: Promise<void> | undefined;
  const runMaintenance = async (): Promise<void> => {
    const context = captureOpenClawStateWorkerContext();
    if (!inspectedProfiles) {
      const root = resolveManagedGitHubProfileRoot({
        agentId: "",
        scope: "personal",
        env: context.environment,
      });
      const entries = await fs.readdir(root, { withFileTypes: true }).catch((error: unknown) => {
        if (hasErrnoCode(error, "ENOENT")) {
          return [];
        }
        throw error;
      });
      const candidates = entries.filter(
        (entry) => entry.isDirectory() && /^ghp_[a-f0-9]{32}$/u.test(entry.name),
      );
      const referenced = new Set(
        (candidates.length ? await listUserGitHubConnections({ context }) : []).flatMap(
          ({ connection }) => connectionProfiles(connection),
        ),
      );
      for (const entry of candidates) {
        if (!referenced.has(entry.name)) {
          queueRetirement(entry.name, context);
        }
      }
      inspectedProfiles = true;
    }
    for (const pending of rotated.values()) {
      try {
        await persistRotation(pending);
        rotated.delete(pending.profileId);
      } catch {
        /* Keep the exact rotated pair for the next durable write. */
      }
    }
    for (const retirement of retirements.values()) {
      await retire(retirement);
    }
    for (const { owner, connection } of await listUserGitHubConnections({ context })) {
      if (stopped) {
        break;
      }
      if (connection.pending && connection.pending.expiresAtMs <= Date.now()) {
        await mutateUserGitHubConnectionAsync(
          {
            kind: "expire-pending",
            owner,
            generation: connection.generation,
            requestId: connection.pending.requestId,
          },
          assertRunning,
          { context },
        );
      }
      try {
        await refresh(owner, context);
      } catch {
        /* Exact pending recovery remains durable for retry. */
      }
    }
  };

  const startAuthorization = async (
    action: PersonalGitHubOperationAction,
  ): Promise<UsersGitHubAuthorizeStartResult> => {
    guard(action);
    assertGitHubCliAvailable();
    const context = captureOpenClawStateWorkerContext();
    const requestId = randomUUID();
    const createdAtMs = Date.now();
    const started = await action.mutate(
      {
        kind: "start",
        owner: action.owner,
        pending: { kind: "starting", requestId, createdAtMs, expiresAtMs: createdAtMs + 900000 },
      },
      context,
    );
    const initial = started.connection;
    if (!initial) {
      throw new Error("My GitHub authorization returned no connection.");
    }
    guard(action);
    const authorization = await startGitHubDeviceFlow(abort.signal);
    guard(action);
    if (authorization.expiresAtMs <= Date.now()) {
      throw new Error("My GitHub authorization expired while starting; start again.");
    }
    const changed = await action.mutate(
      {
        kind: "pending",
        owner: action.owner,
        generation: initial.generation,
        expectedPending: requirePending(initial, initial.generation, requestId).pending,
        pending: { ...authorization, kind: "device", requestId },
      },
      context,
    );
    guard(action);
    const next = changed.connection;
    if (next?.pending?.kind !== "device") {
      throw new Error("My GitHub authorization changed.");
    }
    return projectPending(next.pending);
  };

  const pollAuthorization = async (
    action: PersonalGitHubOperationAction,
    requestId: string,
  ): Promise<UsersGitHubAuthorizePollResult> => {
    guard(action);
    const context = captureOpenClawStateWorkerContext();
    const current = await readUserGitHubConnectionAsync(action.owner, { context });
    guard(action);
    if (current?.pending?.requestId !== requestId) {
      return { status: "expired" };
    }
    const key = `${action.owner}\0${requestId}`;
    const result = await getOrCreatePromise(
      polls,
      key,
      () => pollOnce(action, current, requestId, context),
      { evictOnSettled: true },
    );
    guard(action);
    return result;
  };

  return {
    status: resolvePersonalGitHubStatus,
    revalidateStatus: revalidatePersonalGitHubStatus,
    ...createPersonalGitHubSdkAdapters({
      assertCurrent: guard,
      startAuthorization,
      pollAuthorization,
    }),
    startAuthorizationAsync(action: PersonalGitHubActionV2) {
      return startAuthorization(workerAction(action));
    },
    pollAuthorizationAsync(action: PersonalGitHubActionV2, requestId: string) {
      return pollAuthorization(workerAction(action), requestId);
    },
    async cancelAuthorizationAsync(
      action: PersonalGitHubActionV2,
      requestId: string,
    ): Promise<boolean> {
      guard(action);
      const context = captureOpenClawStateWorkerContext();
      const result = await workerAction(action).mutate(
        { kind: "cancel", owner: action.owner, requestId },
        context,
      );
      guard(action);
      return result.changed;
    },
    async disconnectAsync(action: PersonalGitHubActionV2): Promise<void> {
      guard(action);
      const context = captureOpenClawStateWorkerContext();
      await disconnectUserGitHubConnection(
        action.owner,
        (role) => {
          assertRunning();
          action.assertMutationCurrent(role);
        },
        { context },
      );
      guard(action);
      clearNativeGitHubTokenCache();
    },
    refresh,
    maintain(): Promise<void> {
      if (stopped) {
        return Promise.resolve();
      }
      maintenance ??= runMaintenance().finally(() => {
        maintenance = undefined;
      });
      return maintenance;
    },
    async stop(): Promise<void> {
      stopped = true;
      abort.abort();
      unobserve();
      await Promise.allSettled([
        ...(maintenance ? [maintenance] : []),
        ...polls.values(),
        ...refreshes.values(),
        ...cleanups.values(),
      ]);
      for (const pending of rotated.values()) {
        try {
          await persistRotation(pending);
        } catch {
          /* In-memory rotation remains owned until process exit. */
        }
      }
    },
  };
}
