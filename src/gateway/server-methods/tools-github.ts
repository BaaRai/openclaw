import {
  ErrorCodes,
  errorShape,
  validateToolsGitHubAuthorizeCancelParams,
  validateToolsGitHubAuthorizePollParams,
  validateToolsGitHubAuthorizeStartParams,
  validateToolsGitHubConfigureParams,
  validateToolsGitHubStatusParams,
} from "../../../packages/gateway-protocol/src/index.js";
import {
  captureAgentLifecycleBinding,
  matchesAgentLifecycleBinding,
} from "../../agents/agent-lifecycle-registry.js";
import {
  createManagedGitHubProfileId,
  installManagedGitHubProfile,
  resolveConfiguredGitHubToolIdentity,
  resolveGitHubToolIdentityStatus,
  resolveManagedGitHubProfileDir,
} from "../../agents/github-tool-identity.js";
import { resolveConfigPath } from "../../config/paths.js";
import { getActiveSecretsRuntimeConfigSnapshot } from "../../secrets/runtime-state.js";
import {
  consumeGitHubSetupHandoff,
  consumeGitHubSetupHandoffWithNativeGuard,
} from "../../secrets/store/secret-store.js";
import { captureOpenClawStateWorkerContext } from "../../state/openclaw-state-worker-context.js";
import { GitHubCliUnavailableError } from "../github-cli-preflight.js";
import { identityStillSelected } from "../github-oauth-lifecycle-helpers.js";
import { updateGitHubToolIdentityConfig } from "../github-tool-identity-config.js";
import { resolveAgentIdOrRespondError } from "./agent-id-shared.js";
import { readGatewayRequestMutationAuthority } from "./session-mutation-guards.js";
import type { GatewayRequestHandlers } from "./types.js";
import { defineValidatedGatewayHandler } from "./validation.js";

export const toolsGitHubHandlers: GatewayRequestHandlers = {
  "tools.github.status": defineValidatedGatewayHandler(
    "tools.github.status",
    validateToolsGitHubStatusParams,
    async ({ params, respond, context }) => {
      const resolved = resolveAgentIdOrRespondError({
        rawAgentId: params.agentId,
        respond,
        cfg: context.getRuntimeConfig(),
      });
      if (!resolved) {
        return;
      }
      respond(
        true,
        await resolveGitHubToolIdentityStatus({
          config: resolved.cfg,
          sourceConfig: getActiveSecretsRuntimeConfigSnapshot()?.sourceConfig ?? resolved.cfg,
          agentId: resolved.agentId,
          selectedScope: params.selectedScope,
        }),
      );
    },
  ),
  "tools.github.configure": defineValidatedGatewayHandler(
    "tools.github.configure",
    validateToolsGitHubConfigureParams,
    async (options) => {
      const { params, respond, context } = options;
      const authority = readGatewayRequestMutationAuthority(options);
      const resolved = resolveAgentIdOrRespondError({
        rawAgentId: params.agentId,
        respond,
        cfg: context.getRuntimeConfig(),
      });
      if (!resolved) {
        return;
      }
      try {
        const storage = captureOpenClawStateWorkerContext();
        const stateDatabase = { path: storage.admission.databasePath, env: storage.environment };
        const expectedConfigPath = resolveConfigPath();
        const previousIdentity = resolveConfiguredGitHubToolIdentity({
          config: resolved.cfg,
          agentId: resolved.agentId,
          scope: params.scope,
        });
        const agentLifecycleBinding =
          params.scope === "agent"
            ? captureAgentLifecycleBinding(resolved.cfg, resolved.agentId, stateDatabase)
            : undefined;
        const assertCurrent = () => {
          storage.admission.assertCurrent();
          authority.assertCurrent();
          const config = context.getRuntimeConfig();
          if (
            !identityStillSelected(
              config,
              { scope: params.scope, agentId: resolved.agentId },
              previousIdentity ?? null,
            ) ||
            (params.scope === "agent" &&
              (!agentLifecycleBinding ||
                !matchesAgentLifecycleBinding(config, agentLifecycleBinding, stateDatabase)))
          ) {
            throw new Error("GitHub identity changed during setup.");
          }
        };
        let nextConfig = resolved.cfg;
        if (params.mode === "inherit") {
          nextConfig = await updateGitHubToolIdentityConfig({
            scope: params.scope,
            agentId: resolved.agentId,
            expectedIdentity: previousIdentity ?? null,
            expectedConfigPath,
            stateDatabase,
          });
        } else {
          const gitAuthor = params.gitAuthor
            ? {
                ...(params.gitAuthor.name !== undefined
                  ? { name: params.gitAuthor.name.trim() }
                  : {}),
                ...(params.gitAuthor.email !== undefined
                  ? { email: params.gitAuthor.email.trim() }
                  : {}),
              }
            : undefined;
          const consume =
            authority.family === "worker"
              ? consumeGitHubSetupHandoff
              : consumeGitHubSetupHandoffWithNativeGuard;
          const token = await consume({
            name: params.secretName,
            context: storage,
            assertCurrent,
          });
          assertCurrent();
          if (!token) {
            throw new Error("temporary GitHub credential is unavailable");
          }
          const profileId = createManagedGitHubProfileId();
          const profileDir = resolveManagedGitHubProfileDir({
            agentId: resolved.agentId,
            scope: params.scope,
            profileId,
            env: storage.environment,
          });
          await installManagedGitHubProfile({
            profileDir,
            token,
            assertCurrent,
            commitConfig: async (account) => {
              const identity = {
                profileId,
                gitAuthor: gitAuthor ?? {
                  name: account.login,
                  email: `${account.accountId}+${account.login}@users.noreply.github.com`,
                },
              };
              nextConfig = await updateGitHubToolIdentityConfig({
                scope: params.scope,
                agentId: resolved.agentId,
                identity,
                expectedIdentity: previousIdentity ?? null,
                expectedConfigPath,
                stateDatabase,
                ...(agentLifecycleBinding ? { agentLifecycleBinding } : {}),
              });
            },
          });
        }
        if (previousIdentity?.kind === "oauth") {
          const service = context.githubOAuthService;
          if (service?.retireProfileAsync) {
            await service.retireProfileAsync(previousIdentity.profileId);
          } else {
            service?.retireProfile(previousIdentity.profileId);
          }
        }
        respond(
          true,
          await resolveGitHubToolIdentityStatus({
            config: nextConfig,
            agentId: resolved.agentId,
            selectedScope: params.scope,
            env: storage.environment,
          }),
        );
      } catch {
        respond(
          false,
          undefined,
          errorShape(ErrorCodes.UNAVAILABLE, "GitHub identity setup failed"),
        );
      }
    },
  ),
  "tools.github.authorize.start": defineValidatedGatewayHandler(
    "tools.github.authorize.start",
    validateToolsGitHubAuthorizeStartParams,
    async ({ params, respond, context }) => {
      const resolved = resolveAgentIdOrRespondError({
        rawAgentId: params.agentId,
        respond,
        cfg: context.getRuntimeConfig(),
      });
      if (!resolved) {
        return;
      }
      try {
        const service = context.githubOAuthService;
        if (!service) {
          throw new Error("GitHub authorization lifecycle is unavailable.");
        }
        respond(
          true,
          await service.startAuthorization({ scope: params.scope, agentId: resolved.agentId }),
        );
      } catch (error) {
        respond(
          false,
          undefined,
          errorShape(
            ErrorCodes.UNAVAILABLE,
            error instanceof GitHubCliUnavailableError
              ? error.message
              : "GitHub authorization could not start",
          ),
        );
      }
    },
  ),
  "tools.github.authorize.poll": defineValidatedGatewayHandler(
    "tools.github.authorize.poll",
    validateToolsGitHubAuthorizePollParams,
    async ({ params, respond, context }) => {
      const service = context.githubOAuthService;
      if (!service) {
        throw new Error("GitHub authorization lifecycle is unavailable.");
      }
      respond(true, await service.pollAuthorization(params.requestId));
    },
    () => errorShape(ErrorCodes.UNAVAILABLE, "GitHub authorization polling failed"),
  ),
  "tools.github.authorize.cancel": defineValidatedGatewayHandler(
    "tools.github.authorize.cancel",
    validateToolsGitHubAuthorizeCancelParams,
    async ({ params, respond, context }) => {
      const service = context.githubOAuthService;
      if (!service) {
        respond(
          false,
          undefined,
          errorShape(ErrorCodes.UNAVAILABLE, "GitHub authorization lifecycle is unavailable"),
        );
        return;
      }
      const cancelled = service.cancelAuthorizationAsync
        ? await service.cancelAuthorizationAsync(params.requestId)
        : service.cancelAuthorization(params.requestId);
      respond(true, { cancelled });
    },
  ),
};
