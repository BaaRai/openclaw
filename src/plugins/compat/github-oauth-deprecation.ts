import { warnSessionPersistenceDeprecation } from "../../agents/sessions/session-persistence-deprecation.js";
import { pluginInstanceInvocation } from "../plugin-instance-invocation.js";

export function warnGitHubOAuthDeprecation(method: string, replacement = `${method}Async`): void {
  const pluginId = pluginInstanceInvocation.getStore()?.instance.pluginId;
  warnSessionPersistenceDeprecation(
    `GatewayRequestHandlerOptions.context.githubOAuthService.${method}`,
    `GatewayRequestHandlerOptions.context.githubOAuthService.${replacement}`,
    pluginId === undefined ? undefined : { pluginId },
  );
}
