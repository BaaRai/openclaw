import { isDeepStrictEqual } from "node:util";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import {
  createSqliteWorkerOperationAdmission,
  observeSqliteWorkerCommittedFacts,
  type SqliteWorkerAdmissionFactory,
  type SqliteWorkerOperationAdmission,
  type SqliteWorkerAdmissionRequest,
} from "../infra/sqlite-worker-operation-admission.js";
import type { OpenClawStateDatabaseOptions } from "./openclaw-state-db.js";
import type { OpenClawStateLeaseContext } from "./openclaw-state-lease-context.js";
import {
  withOpenClawStateLeaseWorkerAdmission,
  type WorkerLeaseScope,
} from "./openclaw-state-lease-worker-owner.js";
import { captureOpenClawStateWorkerContext } from "./openclaw-state-worker-context.js";
import type { OpenClawStateWorkerContext } from "./openclaw-state-worker-context.types.js";
import { runOpenClawStateWorkerOperation } from "./openclaw-state-worker-store.js";
import {
  parseConnection,
  publishUserGitHubProfileRetirement,
} from "./user-github-connections.kernel.js";
import type { UserGitHubConnection } from "./user-github-connections.schema.js";
import type {
  UserGitHubCommit,
  UserGitHubMutation,
  UserGitHubRefreshUpdate,
  UserGitHubReplacement,
  UserGitHubRole,
} from "./user-github-connections.worker-contract.js";

export {
  disconnectedUserGitHubConnection,
  observeUserGitHubProfileRetirement,
  readUserGitHubConnection,
  resolvePersonalGitHubOwner,
} from "./user-github-connections.kernel.js";
export type {
  UserGitHubConnection,
  UserGitHubConnected,
  UserGitHubDevice,
} from "./user-github-connections.schema.js";
export type { UserGitHubRole } from "./user-github-connections.worker-contract.js";

type Options = Pick<OpenClawStateDatabaseOptions, "path" | "env"> & {
  context?: OpenClawStateWorkerContext;
  lease?: OpenClawStateLeaseContext;
};

function parseCommit(value: unknown): UserGitHubCommit {
  if (
    !isRecord(value) ||
    value.kind !== "user-github" ||
    typeof value.owner !== "string" ||
    typeof value.changed !== "boolean" ||
    !Array.isArray(value.retired) ||
    !value.retired.every((id): id is string => typeof id === "string")
  ) {
    throw new Error("Personal GitHub mutation returned invalid committed facts.");
  }
  return {
    kind: "user-github",
    owner: value.owner,
    changed: value.changed,
    connection:
      value.connection === undefined
        ? undefined
        : parseConnection(JSON.stringify(value.connection)),
    retired: value.retired,
  };
}

function parseRole(value: unknown): UserGitHubRole {
  if (
    !isRecord(value) ||
    typeof value.profileId !== "string" ||
    (value.role !== null && typeof value.role !== "string") ||
    (value.githubLogin !== null && typeof value.githubLogin !== "string")
  ) {
    throw new Error("Personal GitHub mutation has no current profile authority.");
  }
  return { profileId: value.profileId, role: value.role, githubLogin: value.githubLogin };
}

export async function mutateUserGitHubConnectionAsync(
  mutation: UserGitHubMutation,
  assertCurrent: (role?: UserGitHubRole) => void,
  options: Options,
): Promise<UserGitHubCommit> {
  const context = options.context ?? captureOpenClawStateWorkerContext(options);
  const captured = structuredClone(mutation);
  assertCurrent();
  let committed: UserGitHubCommit | undefined;
  const run = async (lease?: WorkerLeaseScope): Promise<UserGitHubCommit> => {
    let admission: SqliteWorkerOperationAdmission | undefined;
    let prepared: UserGitHubCommit | undefined;
    const createAdmission: SqliteWorkerAdmissionFactory = (operation) => {
      let stage: "transaction" | "commit" | "complete" = "transaction";
      const inspect = (request: SqliteWorkerAdmissionRequest) => {
        if (
          request.stage === "prepare" &&
          isRecord(request.facts) &&
          request.facts.kind === "schema-maintenance"
        ) {
          return;
        }
        context.admission.assertCurrent();
        if (
          request.stage !== stage ||
          !isRecord(request.facts) ||
          request.facts.kind !== (lease ? "state-lease" : "user-github") ||
          request.facts.owner !== captured.owner
        ) {
          throw new Error("Personal GitHub mutation requires its exact transaction admission.");
        }
        const result = parseCommit(request.facts.result);
        const role = captured.kind === "refresh" ? undefined : parseRole(request.facts.role);
        if (role && role.profileId !== captured.owner) {
          throw new Error("Personal GitHub owner changed.");
        }
        if (prepared && !isDeepStrictEqual(prepared, result)) {
          throw new Error("Personal GitHub mutation changed during commit admission.");
        }
        prepared = result;
        assertCurrent(role);
        stage = stage === "transaction" ? "commit" : "complete";
      };
      const source = lease?.createAdmission(operation);
      admission =
        source?.admission ??
        createSqliteWorkerOperationAdmission((request, grant) => {
          inspect(request);
          grant();
        });
      if (source) {
        admission.observeRequests(inspect);
      }
      observeSqliteWorkerCommittedFacts(admission, (receipt) => {
        const result = parseCommit(receipt.facts);
        if (!prepared || !isDeepStrictEqual(result, prepared)) {
          throw new Error("Personal GitHub commit receipt does not match its admitted mutation.");
        }
        committed = result;
        publishUserGitHubProfileRetirement(result.retired, context);
      });
      return {
        admission,
        nativeLocations: source?.nativeLocations ?? [context.admission.databasePath],
      };
    };
    return runOpenClawStateWorkerOperation(
      context,
      (scope) =>
        scope.execute({
          type: "userGitHub.mutate",
          input: { ...captured, ...(lease ? { lease: lease.identity } : {}) },
        }),
      { createAdmission },
    );
  };
  try {
    return await (options.lease
      ? withOpenClawStateLeaseWorkerAdmission(options.lease, context.admission.databasePath, run)
      : run());
  } catch (error) {
    // Accepted remote rotations and disconnects retain their native committed result.
    if (committed) {
      return committed;
    }
    throw error;
  }
}

export async function replaceUserGitHubConnection(
  owner: string,
  replacement: UserGitHubReplacement,
  assertCurrent: (role?: UserGitHubRole) => void,
  options: Options = {},
): Promise<UserGitHubConnection> {
  const result = await mutateUserGitHubConnectionAsync(
    { kind: "replace", owner, ...replacement },
    assertCurrent,
    options,
  );
  if (!result.connection) {
    throw new Error("Personal GitHub replacement returned no connection.");
  }
  return result.connection;
}

export async function disconnectUserGitHubConnection(
  owner: string,
  assertCurrent: (role?: UserGitHubRole) => void,
  options: Options = {},
): Promise<void> {
  await mutateUserGitHubConnectionAsync({ kind: "disconnect", owner }, assertCurrent, options);
}

/** Settles an exact remote rotation even if its original request closed or owner merged. */
export async function updateUserGitHubRefresh(
  params: {
    owner: string;
    profileId: string;
    operationId: string;
    update: UserGitHubRefreshUpdate;
  },
  options: Options = {},
): Promise<boolean> {
  return (await mutateUserGitHubConnectionAsync({ kind: "refresh", ...params }, () => {}, options))
    .changed;
}

export async function readUserGitHubConnectionAsync(
  owner: string,
  options: Options = {},
): Promise<UserGitHubConnection | undefined> {
  const context = options.context ?? captureOpenClawStateWorkerContext(options);
  const result = await runOpenClawStateWorkerOperation(context, (scope) =>
    scope.execute({ type: "userGitHub.read", input: { owner } }),
  );
  context.admission.assertCurrent();
  return result && parseConnection(JSON.stringify(result));
}

export async function listUserGitHubConnections(
  options: Options = {},
): Promise<Array<{ owner: string; connection: UserGitHubConnection }>> {
  const context = options.context ?? captureOpenClawStateWorkerContext(options);
  const result = await runOpenClawStateWorkerOperation(context, (scope) =>
    scope.execute({ type: "userGitHub.list", input: undefined }),
  );
  context.admission.assertCurrent();
  return result.map(({ owner, connection }) => ({
    owner,
    connection: parseConnection(JSON.stringify(connection)),
  }));
}
