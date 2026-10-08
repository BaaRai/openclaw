import type {
  UsersGitHubAuthorizePollResult,
  UsersGitHubAuthorizeStartResult,
} from "../../packages/gateway-protocol/src/schema/users.js";
import { clearNativeGitHubTokenCache } from "../agents/github-read-identity.js";
import { warnGitHubOAuthDeprecation } from "../plugins/compat/github-oauth-deprecation.js";
import type { OpenClawStateLeaseContext } from "../state/openclaw-state-lease.js";
import type { OpenClawStateWorkerContext } from "../state/openclaw-state-worker-context.types.js";
import { mutateUserGitHubConnectionSync } from "../state/user-github-connections.kernel.js";
import type {
  UserGitHubCommit,
  UserGitHubMutation,
} from "../state/user-github-connections.worker-contract.js";
import type { PersonalGitHubAction } from "./github-personal-status.js";

export type PersonalGitHubOperationAction = PersonalGitHubAction & {
  mutate: (
    input: UserGitHubMutation,
    context: OpenClawStateWorkerContext,
    lease?: OpenClawStateLeaseContext,
  ) => Promise<UserGitHubCommit>;
};

/** v2026.9.8 gateway-runtime callbacks stay native until the next SDK major. */
export function createPersonalGitHubSdkAdapters(flow: {
  assertCurrent: (action: PersonalGitHubAction) => void;
  startAuthorization: (
    action: PersonalGitHubOperationAction,
  ) => Promise<UsersGitHubAuthorizeStartResult>;
  pollAuthorization: (
    action: PersonalGitHubOperationAction,
    requestId: string,
  ) => Promise<UsersGitHubAuthorizePollResult>;
}) {
  const nativeAction = (action: PersonalGitHubAction): PersonalGitHubOperationAction => ({
    ...action,
    async mutate(input, context, lease) {
      context.admission.assertCurrent();
      return mutateUserGitHubConnectionSync(
        input,
        () => {
          flow.assertCurrent(action);
          lease?.assertOwned();
        },
        { path: context.admission.databasePath, env: context.environment },
      );
    },
  });
  return {
    startAuthorization(action: PersonalGitHubAction) {
      warnGitHubOAuthDeprecation("personal.startAuthorization");
      return flow.startAuthorization(nativeAction(action));
    },
    pollAuthorization(action: PersonalGitHubAction, requestId: string) {
      warnGitHubOAuthDeprecation("personal.pollAuthorization");
      return flow.pollAuthorization(nativeAction(action), requestId);
    },
    cancelAuthorization(action: PersonalGitHubAction, requestId: string): boolean {
      warnGitHubOAuthDeprecation("personal.cancelAuthorization");
      flow.assertCurrent(action);
      return mutateUserGitHubConnectionSync(
        { kind: "cancel", owner: action.owner, requestId },
        () => flow.assertCurrent(action),
      ).changed;
    },
    disconnect(action: PersonalGitHubAction): void {
      warnGitHubOAuthDeprecation("personal.disconnect");
      flow.assertCurrent(action);
      mutateUserGitHubConnectionSync({ kind: "disconnect", owner: action.owner }, () =>
        flow.assertCurrent(action),
      );
      clearNativeGitHubTokenCache();
    },
  };
}
