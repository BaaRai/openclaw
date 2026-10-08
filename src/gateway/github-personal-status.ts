import type {
  PersonalGitHubStatus,
  UsersGitHubAuthorizeStartResult,
} from "../../packages/gateway-protocol/src/schema/users.js";
import { preparePersonalGitHubPublicationIdentity } from "../agents/github-tool-identity.js";
import type { OpenClawStateDatabaseOptions } from "../state/openclaw-state-db.js";
import { captureOpenClawStateWorkerContext } from "../state/openclaw-state-worker-context.js";
import {
  readUserGitHubConnection,
  readUserGitHubConnectionAsync,
  type UserGitHubConnection,
  type UserGitHubDevice,
} from "../state/user-github-connections.js";

export type PersonalGitHubAction = { owner: string; assertCurrent: () => void };

export function projectPending(pending: UserGitHubDevice): UsersGitHubAuthorizeStartResult {
  return {
    requestId: pending.requestId,
    userCode: pending.userCode,
    verificationUri: pending.verificationUri,
    expiresInMs: Math.max(0, pending.expiresAtMs - Date.now()),
    pollAfterMs: Math.max(1, Math.min(60000, pending.nextPollAtMs - Date.now())),
  };
}

function unavailablePersonalGitHubStatus(): PersonalGitHubStatus {
  return {
    state: "unavailable",
    generation: null,
    account: null,
    accessExpiresAtMs: null,
    refreshState: "failed",
    pending: null,
  };
}

function projectPersonalGitHubStatus(
  record: UserGitHubConnection | undefined,
): PersonalGitHubStatus {
  const selection = record?.selection;
  const connected = selection?.kind === "connected" ? selection : undefined;
  return {
    state: connected ? "connected" : "disconnected",
    generation: record?.generation ?? null,
    account: connected ? { accountId: connected.accountId, login: connected.login } : null,
    accessExpiresAtMs: connected?.accessExpiresAtMs ?? null,
    refreshState: !connected
      ? "not_applicable"
      : connected.refresh
        ? "refreshing"
        : (connected.refreshFailure ??
          (connected.refreshExpiresAtMs <= Date.now() ? "expired" : "available")),
    pending:
      record?.pending?.kind === "device" && record.pending.expiresAtMs > Date.now()
        ? projectPending(record.pending)
        : null,
  };
}

export function personalGitHubStatus(
  action: PersonalGitHubAction,
  database?: OpenClawStateDatabaseOptions,
): PersonalGitHubStatus {
  action.assertCurrent();
  try {
    return projectPersonalGitHubStatus(readUserGitHubConnection(action.owner, database));
  } catch {
    action.assertCurrent();
    return unavailablePersonalGitHubStatus();
  }
}

function validatePersonalGitHubStatus(
  current: PersonalGitHubStatus,
  prepared: PersonalGitHubStatus,
): PersonalGitHubStatus {
  if (
    current.generation !== prepared.generation ||
    current.account?.accountId !== prepared.account?.accountId ||
    current.account?.login.toLowerCase() !== prepared.account?.login.toLowerCase()
  ) {
    throw new Error("My GitHub connection changed; reload its status.");
  }
  return prepared.state === "unavailable" ? { ...current, state: "unavailable" } : current;
}

export function revalidatePersonalGitHubStatus(
  action: PersonalGitHubAction,
  prepared: PersonalGitHubStatus,
): PersonalGitHubStatus {
  return validatePersonalGitHubStatus(personalGitHubStatus(action), prepared);
}

export async function resolvePersonalGitHubStatus(
  action: PersonalGitHubAction,
): Promise<PersonalGitHubStatus> {
  action.assertCurrent();
  const context = captureOpenClawStateWorkerContext();
  const database = { path: context.admission.databasePath, env: context.environment };
  let record: UserGitHubConnection | undefined;
  try {
    record = await readUserGitHubConnectionAsync(action.owner, { context });
  } catch {
    action.assertCurrent();
    return unavailablePersonalGitHubStatus();
  }
  action.assertCurrent();
  const status = projectPersonalGitHubStatus(record);
  if (record?.selection.kind !== "connected") {
    return status;
  }
  const revalidate = () => {
    context.admission.assertCurrent();
    return validatePersonalGitHubStatus(personalGitHubStatus(action, database), status);
  };
  try {
    // Receipts use the durable selection above; live status must additionally
    // prove the selected profile can authenticate without borrowing native auth.
    await preparePersonalGitHubPublicationIdentity({
      profileId: record.selection.profileId,
      accountId: record.selection.accountId,
      assertCurrent: revalidate,
      env: context.environment,
    });
    return revalidate();
  } catch {
    return { ...revalidate(), state: "unavailable" };
  }
}
