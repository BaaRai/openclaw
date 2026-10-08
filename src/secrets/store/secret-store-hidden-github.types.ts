import type { SecretStoreRow } from "./secret-store.types.js";

export type HiddenGitHubRecord = { name: string; value: string };
export type HiddenGitHubRow = Pick<
  SecretStoreRow,
  "name" | "value" | "created_at_ms" | "updated_at_ms"
>;
export type HiddenGitHubWrite = HiddenGitHubRecord & {
  updatedBy?: string | null;
  expectedValue?: string | null;
};
export type HiddenGitHubDelete = { name: string; expectedValue?: string };
export type HiddenGitHubPrefix = "github-device" | "github-oauth";
export type HiddenGitHubReadOperations = {
  "githubSecrets.read": {
    input: { name: string };
    output: { type: "githubSecrets.read"; row: HiddenGitHubRow | undefined };
  };
  "githubSecrets.list": {
    input: { prefix: HiddenGitHubPrefix };
    output: { type: "githubSecrets.list"; rows: HiddenGitHubRow[] };
  };
};
