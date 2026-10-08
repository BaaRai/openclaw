import type { createGitHubOAuthLifecycle } from "./github-oauth-lifecycle.js";

type Lifecycle = ReturnType<typeof createGitHubOAuthLifecycle>;
type SharedAsyncMethod = "cancelAuthorizationAsync" | "retireProfileAsync";
type PersonalAsyncMethod =
  | "startAuthorizationAsync"
  | "pollAuthorizationAsync"
  | "cancelAuthorizationAsync"
  | "disconnectAsync";

/** Released plugin-supplied services may implement only the v2026.9.8 methods. */
export type GitHubOAuthServiceContract = Omit<Lifecycle, "personal" | SharedAsyncMethod> &
  Partial<Pick<Lifecycle, SharedAsyncMethod>> & {
    personal: Omit<Lifecycle["personal"], PersonalAsyncMethod> &
      Partial<Pick<Lifecycle["personal"], PersonalAsyncMethod>>;
  };
