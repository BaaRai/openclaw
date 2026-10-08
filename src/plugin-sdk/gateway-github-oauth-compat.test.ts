import type { GatewayRequestHandlerOptions as CoreHandler } from "openclaw/plugin-sdk/core";
import type { GatewayRequestHandlerOptions as RuntimeHandler } from "openclaw/plugin-sdk/gateway-runtime";
import { expectTypeOf, it } from "vitest";
import type {
  PersonalGitHubStatus,
  ToolsGitHubAuthorizePollResult,
  ToolsGitHubAuthorizeStartResult,
  ToolsGitHubStatusResult,
  UsersGitHubAuthorizePollResult,
  UsersGitHubAuthorizeStartResult,
} from "../../packages/gateway-protocol/src/index.js";
import type { createGitHubOAuthLifecycle } from "../gateway/github-oauth-lifecycle.js";

it("accepts a v2026.9.8 GitHub service without requiring the new awaited companions", () => {
  type ReleasedAction = { owner: string; assertCurrent: () => void };
  type ReleasedService = {
    personal: {
      status: (action: ReleasedAction) => Promise<PersonalGitHubStatus>;
      revalidateStatus: (
        action: ReleasedAction,
        prepared: PersonalGitHubStatus,
      ) => PersonalGitHubStatus;
      startAuthorization: (action: ReleasedAction) => Promise<UsersGitHubAuthorizeStartResult>;
      pollAuthorization: (
        action: ReleasedAction,
        requestId: string,
      ) => Promise<UsersGitHubAuthorizePollResult>;
      cancelAuthorization: (action: ReleasedAction, requestId: string) => boolean;
      disconnect: (action: ReleasedAction) => void;
      refresh: (owner: string) => Promise<void>;
      maintain: () => Promise<void>;
      stop: () => Promise<void>;
    };
    startAuthorization: (input: {
      scope: "system" | "agent";
      agentId: string;
    }) => Promise<ToolsGitHubAuthorizeStartResult>;
    pollAuthorization: (requestId: string) => Promise<ToolsGitHubAuthorizePollResult>;
    cancelAuthorization: (requestId: string) => boolean;
    status: (
      agentId: string,
      selectedScope: "system" | "agent",
    ) => Promise<ToolsGitHubStatusResult>;
    retireProfile: (profileId: string) => void;
    refreshEffectiveIdentity: (agentId: string) => Promise<void>;
    maintain: () => Promise<void>;
    start: () => void;
    stop: () => Promise<void>;
  };
  type PublicService = NonNullable<RuntimeHandler["context"]["githubOAuthService"]>;
  type ConcreteService = ReturnType<typeof createGitHubOAuthLifecycle>;
  expectTypeOf<CoreHandler["context"]>().toEqualTypeOf<RuntimeHandler["context"]>();
  expectTypeOf<ReleasedService>().toExtend<PublicService>();
  expectTypeOf<PublicService>().toExtend<ReleasedService>();
  expectTypeOf<ConcreteService>().toExtend<PublicService>();
  expectTypeOf<ConcreteService>().toExtend<
    Required<Pick<PublicService, "cancelAuthorizationAsync" | "retireProfileAsync">>
  >();
  expectTypeOf<ConcreteService["personal"]>().toExtend<
    Required<
      Pick<
        PublicService["personal"],
        | "startAuthorizationAsync"
        | "pollAuthorizationAsync"
        | "cancelAuthorizationAsync"
        | "disconnectAsync"
      >
    >
  >();
});
