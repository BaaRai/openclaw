import {
  deferSqliteWorkerCommitReceipt,
  requestSqliteWorkerOperationAdmission,
} from "../infra/sqlite-worker-operation-admission.js";
import { runOpenClawStateWriteTransaction } from "./openclaw-state-db.js";
import {
  listUserGitHubConnectionsInDatabase,
  mutateUserGitHubConnectionInDatabase,
  readUserGitHubConnection,
} from "./user-github-connections.kernel.js";
import type { UserGitHubMutation } from "./user-github-connections.worker-contract.js";
import type { WorkerOperationHandlers } from "./worker-operation-registry.js";

export const userGitHubOperations = {
  "userGitHub.read": (input: { owner: string }, { open }) =>
    readUserGitHubConnection(input.owner, { database: open() }),
  "userGitHub.list": (_input: undefined, { open }) =>
    listUserGitHubConnectionsInDatabase(open().db),
  "userGitHub.mutate": (input: UserGitHubMutation, { open }) =>
    runOpenClawStateWriteTransaction(
      ({ db }) => {
        const result = mutateUserGitHubConnectionInDatabase(
          db,
          input,
          (stage, facts) => {
            requestSqliteWorkerOperationAdmission({ stage, facts });
            if (stage === "commit") {
              deferSqliteWorkerCommitReceipt(db, facts.result);
            }
          },
          "worker",
        );
        return result;
      },
      { database: open() },
      { operationLabel: "users.github.mutate" },
    ),
} satisfies WorkerOperationHandlers;
