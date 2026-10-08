import { serialize } from "node:v8";
import { expectDefined } from "@openclaw/normalization-core";
import { runWithSqliteWorkerStateContext } from "../../infra/sqlite-worker-state-context.js";
import {
  createSqliteWorkerWriteAdmission,
  reserveSqliteWorkerInputPreparation,
} from "../../infra/sqlite-worker-store.js";
import { registerSecretValueForRedaction } from "../../logging/secret-redaction-registry.js";
import { createKeyedFifoLeaseRegistry } from "../../shared/keyed-fifo-lease.js";
import { executeExistingOpenClawStateRead } from "../../state/openclaw-state-db-readonly.js";
import {
  runOpenClawStateWriteTransaction,
  type OpenClawStateDatabaseOptions,
} from "../../state/openclaw-state-db.js";
import {
  captureOpenClawStateReadWorkerContext,
  captureOpenClawStateWorkerContext,
} from "../../state/openclaw-state-worker-context.js";
import type { OpenClawStateWorkerContext } from "../../state/openclaw-state-worker-context.types.js";
import { runOpenClawStateWorkerOperation } from "../../state/openclaw-state-worker-store.js";
import type { DomainScope } from "../../state/openclaw-state-worker-store.types.js";
import {
  assertHiddenGitHubSecretRecordName,
  classifyHiddenGitHubStoreName,
  consumeGitHubSetupHandoffInDatabase,
  isLiveHiddenGitHubStoreRow,
  validateHiddenGitHubSecretValue,
} from "./secret-store-hidden-github.kernel.js";
import type {
  HiddenGitHubDelete,
  HiddenGitHubPrefix,
  HiddenGitHubRecord,
  HiddenGitHubWrite,
} from "./secret-store-hidden-github.types.js";

type Options = {
  database?: Pick<OpenClawStateDatabaseOptions, "path" | "env">;
  context?: OpenClawStateWorkerContext;
  assertCurrent?: () => void;
};

const writes = createKeyedFifoLeaseRegistry(Symbol.for("openclaw.hiddenGitHubWrites"));
type Mutation<T> =
  | { kind: "worker"; run: (scope: DomainScope) => Promise<T> }
  | {
      kind: "native-compatibility";
      run: (context: OpenClawStateWorkerContext, assertCurrent: () => void) => T;
    };

async function mutate<T>(params: Options, input: unknown, operation: Mutation<T>): Promise<T> {
  const context = params.context ?? captureOpenClawStateWorkerContext(params.database);
  const assertCallerCurrent = params.assertCurrent;
  const assertCurrent = () => {
    context.admission.assertCurrent();
    assertCallerCurrent?.();
  };
  const preparation = reserveSqliteWorkerInputPreparation(serialize(input).byteLength);
  const lease = expectDefined(
    writes.reserve([
      context.admission.identity.key,
      `path:${context.admission.identity.canonicalPath}`,
    ]),
    "GitHub secret writer lease",
  );
  try {
    await lease.wait();
    preparation.assertCurrent();
    assertCurrent();
    return await runOpenClawStateWorkerOperation(
      context,
      async (scope) => {
        if (operation.kind === "native-compatibility") {
          preparation.release();
          assertCurrent();
          return runWithSqliteWorkerStateContext(context, () =>
            operation.run(context, assertCurrent),
          );
        }
        return await preparation.handoff(() => operation.run(scope));
      },
      operation.kind === "native-compatibility"
        ? {}
        : {
            assertCurrent,
            createAdmission: createSqliteWorkerWriteAdmission(assertCurrent, [
              context.admission.databasePath,
            ]),
          },
    );
  } finally {
    preparation.release();
    lease.release();
  }
}

export async function writeHiddenGitHubSecretRecord(
  params: HiddenGitHubWrite & Options,
): Promise<boolean> {
  assertHiddenGitHubSecretRecordName(params.name);
  validateHiddenGitHubSecretValue(params.value);
  registerSecretValueForRedaction(params.value);
  if (typeof params.expectedValue === "string") {
    registerSecretValueForRedaction(params.expectedValue);
  }
  const input = {
    name: params.name,
    value: params.value,
    updatedBy: params.updatedBy,
    expectedValue: params.expectedValue,
    nowMs: Date.now(),
  };
  return await mutate(params, input, {
    kind: "worker",
    run: (scope) => scope.execute({ type: "githubSecrets.write", input }),
  });
}

export async function readHiddenGitHubSecretRecord(
  params: { name: string } & Options,
): Promise<string | undefined> {
  const kind = assertHiddenGitHubSecretRecordName(params.name);
  const context = params.context ?? captureOpenClawStateReadWorkerContext(params.database);
  const assertCurrent = params.assertCurrent;
  assertCurrent?.();
  const reply = await executeExistingOpenClawStateRead(
    { path: context.admission.databasePath, env: context.environment },
    { type: "githubSecrets.read", input: { name: params.name } },
    { context, current: true },
  );
  if (reply && (!reply.ok || reply.type !== "githubSecrets.read")) {
    throw new Error("Unexpected hidden GitHub record result.");
  }
  const candidate = reply?.row;
  const row =
    candidate && isLiveHiddenGitHubStoreRow(candidate, kind, Date.now()) ? candidate : undefined;
  if (row) {
    registerSecretValueForRedaction(row.value);
  }
  context.admission.assertCurrent();
  assertCurrent?.();
  return row?.value;
}

/** The owner returns one coherent snapshot instead of a name scan followed by N reads. */
export async function listHiddenGitHubSecretRecords(
  params: { prefix: HiddenGitHubPrefix } & Options,
): Promise<HiddenGitHubRecord[]> {
  const context = params.context ?? captureOpenClawStateReadWorkerContext(params.database);
  const { prefix, assertCurrent } = params;
  assertCurrent?.();
  const reply = await executeExistingOpenClawStateRead(
    { path: context.admission.databasePath, env: context.environment },
    { type: "githubSecrets.list", input: { prefix } },
    { context, current: true },
  );
  if (reply && (!reply.ok || reply.type !== "githubSecrets.list")) {
    throw new Error("Unexpected hidden GitHub record list result.");
  }
  const kind = prefix === "github-device" ? "device" : "oauth";
  const now = Date.now();
  const rows = (reply?.rows ?? []).filter((row) => isLiveHiddenGitHubStoreRow(row, kind, now));
  for (const row of rows) {
    registerSecretValueForRedaction(row.value);
  }
  context.admission.assertCurrent();
  assertCurrent?.();
  return rows.map(({ name, value }) => ({ name, value }));
}

export async function deleteHiddenGitHubSecretRecord(
  params: HiddenGitHubDelete & Options,
): Promise<boolean> {
  assertHiddenGitHubSecretRecordName(params.name);
  const input = { name: params.name, expectedValue: params.expectedValue };
  return await mutate(params, input, {
    kind: "worker",
    run: (scope) => scope.execute({ type: "githubSecrets.delete", input }),
  });
}

/** Atomically returns and hard-deletes one exact fresh, non-egress setup handoff. */
export async function consumeGitHubSetupHandoff(
  params: { name: string; nowMs?: number } & Options,
): Promise<string | undefined> {
  if (classifyHiddenGitHubStoreName(params.name) !== "setup") {
    return undefined;
  }
  const input = { name: params.name, nowMs: params.nowMs };
  const value = await mutate(params, input, {
    kind: "worker",
    run: (scope) => scope.execute({ type: "githubSecrets.consumeSetup", input }),
  });
  if (value !== undefined) {
    registerSecretValueForRedaction(value);
  }
  return value;
}

/** v2026.9.4 opaque SDK commit guards stay beside the native transaction until the next SDK major. */
export async function consumeGitHubSetupHandoffWithNativeGuard(
  params: { name: string } & Options,
): Promise<string | undefined> {
  if (classifyHiddenGitHubStoreName(params.name) !== "setup") {
    return undefined;
  }
  const input = { name: params.name };
  const value = await mutate(params, input, {
    kind: "native-compatibility",
    run: (context, assertCurrent) =>
      runOpenClawStateWriteTransaction(
        ({ db }) => {
          assertCurrent();
          const consumed = consumeGitHubSetupHandoffInDatabase(db, { ...input, nowMs: Date.now() });
          assertCurrent();
          return consumed;
        },
        { path: context.admission.databasePath, env: context.environment },
        {
          operationLabel: "secrets.store.consume-github-setup-sdk",
        },
      ),
  });
  if (value !== undefined) {
    registerSecretValueForRedaction(value);
  }
  return value;
}
