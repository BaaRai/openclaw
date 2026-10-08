import type { GitHubOAuthTokenPair } from "../agents/github-oauth-client.js";
import type { OpenClawStateLeaseIdentity } from "./openclaw-state-lease.types.js";
import type { UserGitHubConnection } from "./user-github-connections.schema.js";

export type UserGitHubRole = {
  profileId: string;
  role: string | null;
  githubLogin: string | null;
};
export type UserGitHubReplacement = {
  expected: UserGitHubConnection | undefined;
  next: UserGitHubConnection;
};
export type UserGitHubRefreshUpdate =
  | { kind: "rotated"; tokens: GitHubOAuthTokenPair; receivedAtMs: number }
  | { kind: "failed"; failure: "expired" | "failed" }
  | { kind: "materialized"; login: string };
export type UserGitHubMutation = (
  | ({ kind: "replace" } & UserGitHubReplacement)
  | {
      kind: "pending";
      generation: string;
      expectedPending: NonNullable<UserGitHubConnection["pending"]>;
      pending: UserGitHubConnection["pending"];
    }
  | { kind: "disconnect" }
  | { kind: "cancel"; requestId: string }
  | { kind: "expire-pending"; generation: string; requestId: string }
  | {
      kind: "start";
      pending: { kind: "starting"; requestId: string; createdAtMs: number; expiresAtMs: number };
    }
  | {
      kind: "install";
      generation: string;
      requestId: string;
      profileId: string;
      account: { accountId: number; login: string };
    }
  | {
      kind: "refresh";
      profileId: string;
      operationId: string;
      update: UserGitHubRefreshUpdate;
    }
) & { owner: string; lease?: OpenClawStateLeaseIdentity };
export type UserGitHubCommit = {
  kind: "user-github";
  owner: string;
  changed: boolean;
  connection: UserGitHubConnection | undefined;
  retired: string[];
};
export type UserGitHubWorkerOperations = {
  "userGitHub.read": {
    input: { owner: string };
    output: UserGitHubConnection | undefined;
  };
  "userGitHub.list": {
    input: undefined;
    output: Array<{ owner: string; connection: UserGitHubConnection }>;
  };
  "userGitHub.mutate": {
    input: UserGitHubMutation;
    output: UserGitHubCommit;
  };
};
