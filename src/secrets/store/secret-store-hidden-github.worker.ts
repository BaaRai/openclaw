import type { DatabaseSync } from "node:sqlite";
import { requestSqliteWorkerOperationAdmission } from "../../infra/sqlite-worker-operation-admission.js";
import type {
  WorkerOperationHandlers,
  WorkerOperations,
  WorkerWriteOperationContext,
} from "../../state/worker-operation-registry.js";
import {
  assertHiddenGitHubSecretRecordName,
  consumeGitHubSetupHandoffInDatabase,
  deleteHiddenGitHubSecretRecordInDatabase,
  listHiddenGitHubRows,
  readHiddenGitHubRow,
  upsertHiddenGitHubSecret,
  validateHiddenGitHubSecretValue,
} from "./secret-store-hidden-github.kernel.js";
import type {
  HiddenGitHubDelete,
  HiddenGitHubReadOperations,
  HiddenGitHubWrite,
} from "./secret-store-hidden-github.types.js";

function admit(stage: "transaction" | "commit") {
  requestSqliteWorkerOperationAdmission({ stage, facts: undefined });
}

export const hiddenGitHubOperations = {
  "githubSecrets.write": (input: HiddenGitHubWrite & { nowMs: number }, { write }) =>
    write(
      ({ db }) => {
        admit("transaction");
        assertHiddenGitHubSecretRecordName(input.name);
        validateHiddenGitHubSecretValue(input.value);
        const changed = upsertHiddenGitHubSecret(
          db,
          {
            scope_kind: "team",
            scope_id: "",
            name: input.name,
            value: input.value,
            updated_by: input.updatedBy ?? null,
          },
          input.nowMs,
          input.expectedValue,
        );
        admit("commit");
        return changed;
      },
      { operationLabel: "secrets.store.write-hidden-github" },
    ),
  "githubSecrets.delete": (input: HiddenGitHubDelete, { write }) =>
    write(
      ({ db }) => {
        admit("transaction");
        const changed = deleteHiddenGitHubSecretRecordInDatabase(db, input);
        admit("commit");
        return changed;
      },
      { operationLabel: "secrets.store.delete-hidden-github" },
    ),
  "githubSecrets.consumeSetup": (input: { name: string; nowMs?: number }, { write }) =>
    write(
      ({ db }) => {
        admit("transaction");
        const value = consumeGitHubSetupHandoffInDatabase(db, {
          name: input.name,
          nowMs: input.nowMs ?? Date.now(),
        });
        admit("commit");
        return value;
      },
      { operationLabel: "secrets.store.consume-github-setup-handoff" },
    ),
} satisfies WorkerOperationHandlers<WorkerWriteOperationContext>;

export type HiddenGitHubWorkerOperations = WorkerOperations<typeof hiddenGitHubOperations>;

export const hiddenGitHubReadOperations = {
  "githubSecrets.read": (input: HiddenGitHubReadOperations["githubSecrets.read"]["input"], db) => ({
    type: "githubSecrets.read" as const,
    row: readHiddenGitHubRow(db, input.name),
  }),
  "githubSecrets.list": (input: HiddenGitHubReadOperations["githubSecrets.list"]["input"], db) => ({
    type: "githubSecrets.list" as const,
    rows: listHiddenGitHubRows(db, input.prefix),
  }),
} satisfies WorkerOperationHandlers<DatabaseSync>;
