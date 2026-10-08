import {
  deferSqliteWorkerCommitReceipt,
  requestSqliteWorkerOperationAdmission,
} from "../../infra/sqlite-worker-operation-admission.js";
import {
  runOpenClawStateWriteTransaction,
  type OpenClawStateDatabase,
} from "../../state/openclaw-state-db.js";
import type {
  WorkerOperationContext,
  WorkerOperationHandlers,
} from "../../state/worker-operation-registry.js";
import { createWorkerEnvironmentCommitAdmission } from "./store-commit-authority.js";
import { reconcileAttachedSessionOwners } from "./store-mutations.js";
import { readWorkerEnvironmentFacts } from "./store-row-codec.js";
import { readTotalChanges } from "./store-write.js";
import { createWorkerEnvironmentStoreKernel } from "./store.kernel.js";
import type {
  WorkerEnvironmentMutationInput,
  WorkerEnvironmentMutationMethods,
} from "./store.types.js";
import { pruneObservedTerminalWorkerEnvironments } from "./terminal-environment-retention.js";

type Method = keyof WorkerEnvironmentMutationMethods | "initialize";
type Input<Name extends Method> = {
  nowMs?: number;
} & (Name extends keyof WorkerEnvironmentMutationMethods
  ? { input: WorkerEnvironmentMutationInput<Name> }
  : unknown);
type MutationContext = {
  db: OpenClawStateDatabase["db"];
  store: ReturnType<typeof createWorkerEnvironmentStoreKernel>;
  now: () => number;
  touch: (id: string) => void;
};

function mutation<Name extends Method, Result>(
  name: Name,
  execute: (input: Input<Name>, context: MutationContext) => Result,
) {
  return (input: Input<Name>, { open }: WorkerOperationContext) => {
    const database = open();
    return runOpenClawStateWriteTransaction(
      (transactionDatabase) => {
        const { db } = transactionDatabase;
        requestSqliteWorkerOperationAdmission({ stage: "transaction", facts: undefined });
        const now = () => input.nowMs ?? Date.now();
        const store = createWorkerEnvironmentStoreKernel(transactionDatabase, now);
        const changesBefore = readTotalChanges(db);
        const touched = new Set<string>();
        const result = execute(input, { db, store, now, touch: (id) => touched.add(id.trim()) });
        const receipt = {
          result,
          changed: readTotalChanges(db) !== changesBefore,
          facts: readWorkerEnvironmentFacts(db, [...touched]),
        };
        deferSqliteWorkerCommitReceipt(db, receipt);
        requestSqliteWorkerOperationAdmission({
          stage: "commit",
          facts: createWorkerEnvironmentCommitAdmission(receipt.facts),
        });
        return receipt;
      },
      { database },
      { operationLabel: `workerEnvironments.${name}` },
    );
  };
}

type EnvironmentMutation = {
  [Name in keyof WorkerEnvironmentMutationMethods]: WorkerEnvironmentMutationInput<Name> extends {
    environmentId: string;
  }
    ? Name
    : never;
}[keyof WorkerEnvironmentMutationMethods];

type EnvironmentMutationMethods = {
  [Name in EnvironmentMutation]: (
    input: Input<Name>["input"],
  ) => ReturnType<WorkerEnvironmentMutationMethods[Name]>;
};

function environmentMutation<Name extends EnvironmentMutation>(name: Name) {
  return mutation(name, ({ input }, { store, touch }) => {
    touch(input.environmentId);
    const methods: EnvironmentMutationMethods = store;
    return methods[name](input);
  });
}

export const workerEnvironmentOperations = {
  "workerEnvironments.initialize": mutation("initialize", (_input, { db, now, touch }) => {
    for (const id of reconcileAttachedSessionOwners(db, now())) {
      touch(id);
    }
    return undefined;
  }),
  "workerEnvironments.createIntent": environmentMutation("createIntent"),
  "workerEnvironments.ensureNodeEnrollment": mutation(
    "ensureNodeEnrollment",
    ({ input }, { store, touch }) => {
      touch(input);
      return store.ensureNodeEnrollment(input);
    },
  ),
  "workerEnvironments.revokeEnvironmentCredential": environmentMutation(
    "revokeEnvironmentCredential",
  ),
  "workerEnvironments.reconcileSharedHost": environmentMutation("reconcileSharedHost"),
  "workerEnvironments.adoptProvisionCleanupFailure": environmentMutation(
    "adoptProvisionCleanupFailure",
  ),
  "workerEnvironments.requestDestroy": environmentMutation("requestDestroy"),
  "workerEnvironments.refreshBootstrapReceipt": environmentMutation("refreshBootstrapReceipt"),
  "workerEnvironments.transition": environmentMutation("transition"),
  "workerEnvironments.renewCredential": environmentMutation("renewCredential"),
  "workerEnvironments.markCredentialDelivered": environmentMutation("markCredentialDelivered"),
  "workerEnvironments.recordError": environmentMutation("recordError"),
  "workerEnvironments.ensurePreparedIntent": mutation(
    "ensurePreparedIntent",
    ({ input }, { store, touch }) => {
      const value = store.ensurePreparedIntent(input);
      touch(input.intent.environmentId);
      if (value) {
        touch(value.environmentId);
      }
      return value;
    },
  ),
  "workerEnvironments.requestPreparedDestroy": environmentMutation("requestPreparedDestroy"),
  "workerEnvironments.createSessionAttachmentIntent": mutation(
    "createSessionAttachmentIntent",
    ({ input }, { store, touch }) => {
      const previous = store.getSessionAttachmentRecord(input.sessionId);
      if (previous) {
        touch(previous.environmentId);
      }
      touch(input.environmentId);
      return store.createSessionAttachmentIntent(input);
    },
  ),
  "workerEnvironments.closeSessionAttachment": mutation(
    "closeSessionAttachment",
    ({ input }, { store, touch }) => {
      const value = store.closeSessionAttachment(input);
      if (value) {
        touch(value.environmentId);
      }
      return value;
    },
  ),
  "workerEnvironments.cancelSessionAttachmentReservation": environmentMutation(
    "cancelSessionAttachmentReservation",
  ),
  "workerEnvironments.touchSessionAttachment": environmentMutation("touchSessionAttachment"),
  "workerEnvironments.pruneTerminalEnvironments": mutation(
    "pruneTerminalEnvironments",
    ({ input: { approved } }, { db, touch }) => {
      for (const row of approved) {
        touch(row.environment_id);
      }
      return pruneObservedTerminalWorkerEnvironments(db, approved);
    },
  ),
} satisfies WorkerOperationHandlers;
