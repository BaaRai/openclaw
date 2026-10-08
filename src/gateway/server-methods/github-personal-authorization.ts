import {
  intersectOperatorScopes,
  operatorScopeSatisfied,
  roleScopesAllow,
} from "../../shared/operator-scope-compat.js";
import { openOpenClawStateDatabase } from "../../state/openclaw-state-db.js";
import { resolveOpenClawStateSqlitePath } from "../../state/openclaw-state-db.paths.js";
import { captureOpenClawStateWorkerContext } from "../../state/openclaw-state-worker-context.js";
import { prepareUserProfileRoleAuthority } from "../../state/user-channel-identity-operations.js";
import { resolvePersonalGitHubOwner } from "../../state/user-github-connections.js";
import type { PersonalGitHubActionV2 } from "../github-personal-oauth.js";
import type { PersonalGitHubAction } from "../github-personal-status.js";
import { GitHubPublicationSessionChangedError } from "../github-publication-failure.js";
import { hasCurrentGatewayOperatorAccess } from "../operator-access-policy.js";
import {
  resolveOperatorRolePolicy,
  resolveOperatorRolePolicyForProfile,
  resolveOperatorRolePolicyForAssignment,
} from "../operator-role-policy.js";
import type { SessionMutationTarget } from "../session-mutation-authorization-error.js";
import {
  createSessionListEntryFilter,
  resolveSessionMutationAuthorization,
} from "../session-sharing.js";
import type { GatewaySessionStoreDiscoveryCache } from "../session-utils-store-candidates.js";
import { loadGatewaySessionEntryReadOnly } from "../session-utils.js";
import { isGatewayClientProfilePending } from "./gateway-client-identity.js";
import {
  isIneligiblePersonalGatewayCaller,
  isSyntheticGatewayCaller,
} from "./gateway-personal-caller.js";
import { readGatewayRequestMutationAuthority } from "./session-mutation-guards.js";
import type { GatewayRequestHandlerOptions } from "./types.js";

type Request = Pick<GatewayRequestHandlerOptions, "client" | "context" | "signal">;

/** Intersect the live role ceiling with the socket grant, preserving scope implications. */
function currentGitHubClient(
  options: Request,
  scope: "operator.read" | "operator.write" | "operator.sessions.read",
  owner?: string | { profileId: string; role: string | null; githubLogin?: string | null },
) {
  const { client, context } = options;
  if (
    options.signal?.aborted ||
    client?.invalidated ||
    client?.connectionSignal?.aborted ||
    !hasCurrentGatewayOperatorAccess(client?.internal?.operatorAccessAuthority) ||
    (client?.connId &&
      !isSyntheticGatewayCaller(client) &&
      !context.getClientConnIds?.((current) => current === client).has(client.connId))
  ) {
    throw new Error("GitHub request connection is no longer current; reconnect and try again.");
  }
  if (!client) {
    return null;
  }
  if (isGatewayClientProfilePending(client)) {
    throw new Error("Authenticated profile verification is unavailable; retry the request.");
  }
  const cfg = context.getRuntimeConfig();
  const profileId = typeof owner === "string" ? owner : owner?.profileId;
  const policy =
    typeof owner === "object"
      ? resolveOperatorRolePolicyForAssignment(
          owner.profileId,
          owner.role,
          cfg,
          owner.githubLogin ?? null,
        )
      : profileId
        ? resolveOperatorRolePolicyForProfile(profileId, cfg)
        : resolveOperatorRolePolicy(client, cfg);
  const granted = client.connect.scopes ?? [];
  const scopes = policy ? intersectOperatorScopes(granted, policy.scopes) : granted;
  if (
    client.connect.role !== "operator" ||
    !roleScopesAllow({
      role: "operator",
      requestedScopes: [scope],
      allowedScopes: scopes,
    })
  ) {
    throw new Error(`GitHub requires current ${scope} permission.`);
  }
  return {
    ...client,
    connect: { ...client.connect, scopes },
    ...(profileId && client.authenticatedUserProfile
      ? { authenticatedUserProfile: { ...client.authenticatedUserProfile, profileId } }
      : {}),
  };
}

type PersonalEligibility =
  | { kind: "eligible"; action: PersonalGitHubAction }
  | { kind: "absent" | "ineligible" };

/** Shared reads do not require a person; absence never substitutes for failed authentication. */
export async function prepareGitHubPublicationOptionsRead(
  options: Request &
    Pick<
      GatewayRequestHandlerOptions,
      "req" | "hasCurrentClientAuthority" | "sessionMutationCommitGuard"
    >,
  { sessionKey, agentId: requestedAgentId }: SessionMutationTarget,
  signal?: AbortSignal,
) {
  // Store discovery is stable within this request; session rows remain live reads.
  const targetDiscoveryCache: GatewaySessionStoreDiscoveryCache = new Map();
  const authority = readGatewayRequestMutationAuthority(options);
  const client = options.client;
  const profileReference = client?.authenticatedUserProfile?.profileId;
  const userId = client?.authenticatedUserId;
  const access = client?.internal?.operatorAccessAuthority;
  const assertConnection = () => {
    signal?.throwIfAborted();
    authority.assertCurrent();
    if (
      client?.authenticatedUserProfile?.profileId !== profileReference ||
      client?.authenticatedUserId !== userId ||
      client?.internal?.operatorAccessAuthority !== access
    ) {
      throw new Error("GitHub profile changed; retry publication options.");
    }
  };
  assertConnection();
  currentGitHubClient(options, "operator.sessions.read");
  const profile = profileReference
    ? await prepareUserProfileRoleAuthority(profileReference)
    : undefined;
  assertConnection();
  if (profileReference && !profile) {
    throw new Error("Authenticated profile verification is unavailable; retry the request.");
  }
  const currentClient = () => {
    assertConnection();
    if (profile && !profile.isCurrent()) {
      throw new Error("GitHub profile changed; retry publication options.");
    }
    return currentGitHubClient(options, "operator.sessions.read", profile);
  };
  const eligibleClient = currentClient();
  const personal: PersonalEligibility =
    !eligibleClient?.connId ||
    isIneligiblePersonalGatewayCaller(eligibleClient) ||
    !operatorScopeSatisfied("operator.read", eligibleClient.connect.scopes ?? [])
      ? { kind: "ineligible" }
      : !profile
        ? { kind: "absent" }
        : {
            kind: "eligible",
            action: preparePersonalGitHubAction(options, "operator.read", signal),
          };
  const readSession = (key: string, agentId?: string) => {
    const loaded = loadGatewaySessionEntryReadOnly(key, { agentId, targetDiscoveryCache });
    const filter = createSessionListEntryFilter({
      cfg: options.context.getRuntimeConfig(),
      client: currentClient(),
    });
    return loaded.entry && filter?.(loaded.canonicalKey, loaded.entry) !== false
      ? {
          sessionId: loaded.entry.sessionId,
          sessionKey: loaded.canonicalKey,
          agentId: loaded.agentId,
          lifecycleRevision: loaded.entry.lifecycleRevision ?? null,
          archivedAt: loaded.entry.archivedAt ?? null,
        }
      : null;
  };
  const session = readSession(sessionKey, requestedAgentId);
  if (!session) {
    throw new Error("GitHub publication session was not found.");
  }
  // sessionId/lifecycleRevision pin the incarnation; archivedAt is re-read below because
  // archiving flips it without touching either identity field.
  const readCurrent = () => {
    const current = readSession(session.sessionKey, session.agentId);
    if (
      !current ||
      current.sessionId !== session.sessionId ||
      current.lifecycleRevision !== session.lifecycleRevision
    ) {
      throw new Error("GitHub publication session access changed; select the session again.");
    }
    return current;
  };
  return {
    personal,
    session,
    sessionScoped: authority.sessionScope === "operator.sessions.read",
    currentSession: readCurrent,
    // Callbacks may refresh live facts, but must not replace the response's archive snapshot.
    assertSessionUnchanged: (snapshot: ReturnType<typeof readCurrent>): void => {
      const current = readCurrent();
      if (current.archivedAt !== snapshot.archivedAt) {
        throw new Error("GitHub publication session access changed; select the session again.");
      }
    },
  };
}

/** Authority stays in this direct connection closure; a profile or request id alone grants nothing. */
function capturePersonalGitHubAction(
  options: Request,
  scope: "operator.read" | "operator.write",
  signal?: AbortSignal,
) {
  const { client, context } = options;
  const source = captureOpenClawStateWorkerContext();
  const database = { path: source.admission.databasePath, env: source.environment };
  const resolveOwner = () => {
    signal?.throwIfAborted();
    if (
      !client?.connId ||
      client.connect?.role !== "operator" ||
      isIneligiblePersonalGatewayCaller(client) ||
      options.signal?.aborted ||
      !context.getClientConnIds?.((current) => current === client).has(client.connId)
    ) {
      throw new Error("My GitHub requires a current authenticated human Gateway connection.");
    }
    source.admission.assertCurrent();
    // Role-policy helpers also use the ambient store; reject retargeting before either lookup.
    if (resolveOpenClawStateSqlitePath() !== database.path) {
      throw new Error("My GitHub state store changed; retry from your current profile.");
    }
    const profile = client.authenticatedUserProfile?.profileId;
    const owner = profile
      ? resolvePersonalGitHubOwner(profile, openOpenClawStateDatabase(database).db)
      : undefined;
    if (!owner) {
      throw new Error("My GitHub requires a verified durable user profile; sign in and try again.");
    }
    currentGitHubClient(options, scope, owner);
    return owner;
  };
  const owner = resolveOwner();
  return {
    database,
    action: {
      owner,
      assertCurrent: () => {
        if (resolveOwner() !== owner) {
          throw new Error("My GitHub owner changed; retry from your current profile.");
        }
      },
    },
  };
}

export function preparePersonalGitHubAction(
  options: Request,
  scope: "operator.read" | "operator.write" = "operator.read",
  signal?: AbortSignal,
): PersonalGitHubAction {
  return capturePersonalGitHubAction(options, scope, signal).action;
}

/** Personal writes consume transaction-local role facts while the host retains the live socket. */
export async function preparePersonalGitHubActionV2(
  options: Request,
): Promise<PersonalGitHubActionV2> {
  const { action, database } = capturePersonalGitHubAction(options, "operator.read");
  const { client } = options;
  const profileReference = client?.authenticatedUserProfile?.profileId;
  const userReference = client?.authenticatedUserId;
  const access = client?.internal?.operatorAccessAuthority;
  const prepared = await prepareUserProfileRoleAuthority(action.owner, database);
  action.assertCurrent();
  if (!prepared || prepared.profileId !== action.owner) {
    throw new Error("My GitHub owner changed; retry from your current profile.");
  }
  return {
    ...action,
    assertMutationCurrent(role) {
      if (
        !prepared.isCurrent() ||
        client?.authenticatedUserProfile?.profileId !== profileReference ||
        client?.authenticatedUserId !== userReference ||
        client?.internal?.operatorAccessAuthority !== access ||
        (role && role.profileId !== action.owner)
      ) {
        throw new Error("My GitHub owner changed; retry from your current profile.");
      }
      currentGitHubClient(options, "operator.read", role ?? prepared);
    },
  };
}

export function preparePersonalGitHubSessionAction(
  options: Request,
  { sessionKey, agentId }: SessionMutationTarget,
): PersonalGitHubAction & {
  sessionId: string;
  sessionKey: string;
  agentId: string;
  lifecycleRevision: string | null;
} {
  const action = preparePersonalGitHubAction(options, "operator.write");
  const targetDiscoveryCache: GatewaySessionStoreDiscoveryCache = new Map();
  const initial = loadGatewaySessionEntryReadOnly(sessionKey, { agentId, targetDiscoveryCache });
  if (!initial.entry?.sessionId) {
    throw new Error("GitHub publication session was not found.");
  }
  const sessionId = initial.entry.sessionId;
  const lifecycleRevision = initial.entry.lifecycleRevision ?? null;
  const assertCurrent = () => {
    action.assertCurrent();
    const current = loadGatewaySessionEntryReadOnly(initial.canonicalKey, {
      agentId: initial.agentId,
      targetDiscoveryCache,
    });
    if (
      current.entry?.sessionId !== sessionId ||
      (current.entry.lifecycleRevision ?? null) !== lifecycleRevision ||
      current.entry.archivedAt !== undefined ||
      current.canonicalKey !== initial.canonicalKey
    ) {
      throw new GitHubPublicationSessionChangedError();
    }
    // This is a session mutation, not a run start. Preserve current admin rights without
    // retaining an admin grant that the person's live role no longer permits.
    const { error } = resolveSessionMutationAuthorization({
      client: currentGitHubClient(options, "operator.write", action.owner),
      method: "sessions.github.publish",
      requestParams: { sessionKey: initial.canonicalKey, agentId: initial.agentId },
      context: options.context,
    });
    if (error) {
      throw new Error(error.message);
    }
  };
  assertCurrent();
  return {
    ...action,
    assertCurrent,
    sessionId,
    lifecycleRevision,
    sessionKey: initial.canonicalKey,
    agentId: initial.agentId,
  };
}
