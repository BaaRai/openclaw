import { isDeepStrictEqual } from "node:util";
import { matchesAgentLifecycleBinding } from "../agents/agent-lifecycle-registry.js";
import { withAgentRosterFactsBatch } from "../agents/agent-scope-config.js";
import { listAgentIds, resolveAgentConfig } from "../agents/agent-scope.js";
import type {
  GitHubDeviceAuthorizationRecord,
  GitHubIdentityScope,
  GitHubOAuthRecord,
} from "../agents/github-oauth-records.js";
import { resolveConfiguredGitHubToolIdentity } from "../agents/github-tool-identity.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import type { GitHubToolIdentityConfig } from "../config/types.tools.js";
import type { OpenClawStateDatabaseOptions } from "../state/openclaw-state-db.js";

export const REFRESH_SKEW_MS = 10 * 60_000;
export const MAINTENANCE_INTERVAL_MS = 60_000;
export const SHUTDOWN_DRAIN_TIMEOUT_MS = 31_000;

export type ConfiguredOAuthIdentity = {
  scope: GitHubIdentityScope;
  agentId: string;
  identity: GitHubToolIdentityConfig & { kind: "oauth" };
};

export function identityStillSelected(
  config: OpenClawConfig,
  location: { scope: GitHubIdentityScope; agentId: string },
  expected: GitHubToolIdentityConfig | null,
): boolean {
  const current = resolveConfiguredGitHubToolIdentity({ config, ...location });
  return isDeepStrictEqual(current ?? null, expected);
}

export function authorizationStillOwned(
  config: OpenClawConfig,
  record: GitHubDeviceAuthorizationRecord,
  database?: Pick<OpenClawStateDatabaseOptions, "path" | "env">,
): boolean {
  return (
    identityStillSelected(config, record, record.expectedIdentity) &&
    (record.scope === "system" ||
      (record.agentLifecycleBinding !== undefined &&
        matchesAgentLifecycleBinding(config, record.agentLifecycleBinding, database)))
  );
}

/** Config ownership stays live across metadata admission and profile filesystem effects. */
export function createGitHubOAuthRecordAuthority(params: {
  record: GitHubOAuthRecord;
  database: Pick<OpenClawStateDatabaseOptions, "path" | "env">;
  getConfig: () => OpenClawConfig;
  getPersistedConfig?: () => OpenClawConfig;
  isCommitting: (requestId: string) => boolean;
  assertCurrent: () => void;
}) {
  const { record } = params;
  const read = (): "selected" | "unselected" | "unproven" => {
    params.assertCurrent();
    const pending = record.pendingInitial;
    if (pending && params.isCommitting(pending.requestId)) {
      throw new Error("GitHub authorization is still committing.");
    }
    const config = pending ? params.getPersistedConfig?.() : params.getConfig();
    if (!config) {
      throw new Error("Authoritative persisted config is unavailable.");
    }
    const identity = resolveConfiguredGitHubToolIdentity({ config, ...record });
    if (identity?.profileId !== record.profileId) {
      return "unselected";
    }
    if (
      identity.kind !== "oauth" ||
      (pending &&
        record.scope === "agent" &&
        (!pending.agentLifecycleBinding ||
          !matchesAgentLifecycleBinding(config, pending.agentLifecycleBinding, params.database)))
    ) {
      return "unproven";
    }
    return "selected";
  };
  return {
    read,
    assertSelected() {
      if (read() !== "selected") {
        throw new Error("GitHub authorization ownership changed before completion.");
      }
    },
    assertUnselected() {
      if (
        read() !== "unselected" ||
        resolveConfiguredGitHubToolIdentity({
          config: params.getConfig(),
          ...record,
        })?.profileId === record.profileId
      ) {
        throw new Error("GitHub profile became selected before cleanup.");
      }
    },
  };
}

export function configuredOAuthIdentities(config: OpenClawConfig): ConfiguredOAuthIdentity[] {
  return withAgentRosterFactsBatch(config, () => {
    const identities: ConfiguredOAuthIdentity[] = [];
    const system = config.tools?.github;
    if (system?.kind === "oauth") {
      identities.push({
        scope: "system",
        agentId: "system",
        identity: { ...system, kind: "oauth" },
      });
    }
    for (const agentId of listAgentIds(config).toSorted()) {
      const identity = resolveAgentConfig(config, agentId)?.tools?.github;
      if (identity?.kind === "oauth") {
        identities.push({ scope: "agent", agentId, identity: { ...identity, kind: "oauth" } });
      }
    }
    return identities;
  });
}
