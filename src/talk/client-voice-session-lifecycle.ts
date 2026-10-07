import { AsyncLocalStorage } from "node:async_hooks";
import {
  captureSqliteWorkerStateContext,
  runWithSqliteWorkerStateContext,
} from "../infra/sqlite-worker-state-context.js";
import { AsyncWorkScope } from "../shared/async-work-scope.js";
import { createDeferredCore } from "../shared/deferred.js";
import { registerOpenClawStateDatabaseAsyncResource } from "../state/openclaw-state-db-cache.js";
import { captureOpenClawStateWorkerContext } from "../state/openclaw-state-worker-context.js";
import type { OpenClawStateWorkerContext } from "../state/openclaw-state-worker-context.types.js";

const current = new AsyncLocalStorage<{
  context: OpenClawStateWorkerContext;
  scope: AsyncWorkScope;
  active: boolean;
}>();
const lifetimes = new Map<string, ReturnType<typeof createLifetime>>();

function createLifetime({ admission }: OpenClawStateWorkerContext) {
  let work = new AsyncWorkScope();
  const scopes = new Set([work]);
  let gateways = 0;
  let closing = false;
  const drain = (scope: AsyncWorkScope) =>
    AsyncWorkScope.runWhenAllIdle(
      () => [scope],
      async () => {
        await scope.drain();
        scopes.delete(scope);
      },
    );
  const owner = {
    retainGateway() {
      if (closing) {
        work = new AsyncWorkScope();
        scopes.add(work);
        closing = false;
      }
      gateways += 1;
      let released = false;
      let pending: Promise<void> | undefined;
      const beginClose = () => {
        if (!released) {
          released = true;
          if (--gateways === 0) {
            closing = true;
            pending = drain(work);
          }
        }
      };
      return {
        beginClose,
        drain: () => {
          beginClose();
          return pending ?? Promise.resolve();
        },
      };
    },
    assertOpen(inherited: boolean) {
      if (closing && !inherited) {
        throw new Error("Voice session persistence admission is closed");
      }
    },
    scope: () => work,
  };
  const unregister = registerOpenClawStateDatabaseAsyncResource({
    async close(identity) {
      if (
        !identity ||
        identity.key === admission.identity.key ||
        identity.canonicalPath === admission.identity.canonicalPath
      ) {
        closing = true;
        await Promise.all([...scopes].map(drain));
        lifetimes.delete(admission.coordinationKey);
        unregister();
      }
    },
  });
  return owner;
}

function lifetime(context: OpenClawStateWorkerContext) {
  let owner = lifetimes.get(context.admission.coordinationKey);
  if (!owner) {
    owner = createLifetime(context);
    lifetimes.set(context.admission.coordinationKey, owner);
  }
  return owner;
}

/** Retain accepted work before a bounded queue or provider can yield. */
export function captureClientVoiceSessionSettlement() {
  const inherited = current.getStore();
  if (inherited && !inherited.active) {
    throw new Error("Voice session persistence lost its accepted owner");
  }
  const selected = inherited?.context ?? captureOpenClawStateWorkerContext();
  const context = inherited?.context ?? {
    ...captureSqliteWorkerStateContext(selected),
    admission: selected.admission,
  };
  const owner = lifetime(context);
  owner.assertOpen(Boolean(inherited));
  const scope = inherited?.scope ?? owner.scope();
  const settled = createDeferredCore();
  void scope.track(() => settled.promise);
  const operation = { context, scope, active: true };
  return {
    run<T>(run: () => T): T {
      if (!operation.active) {
        throw new Error("Voice session persistence lost its accepted owner");
      }
      try {
        context.admission.assertCurrent();
        return scope.run(() =>
          current.run(operation, () => runWithSqliteWorkerStateContext(context, run)),
        );
      } catch (error) {
        operation.active = false;
        settled.resolve();
        throw error;
      }
    },
    release() {
      operation.active = false;
      settled.resolve();
    },
  };
}

export async function withClientVoiceSessionSettlement<T>(
  run: () => Promise<T>,
  onAdmissionFailure?: (error: unknown) => Promise<T>,
): Promise<T> {
  let accepted: ReturnType<typeof captureClientVoiceSessionSettlement> | undefined;
  let entered = false;
  try {
    accepted = captureClientVoiceSessionSettlement();
    return await accepted.run(() => {
      entered = true;
      return run();
    });
  } catch (error) {
    // Close still owns provider teardown after refusal, but may not replay entered work.
    if (!entered && onAdmissionFailure) {
      return await onAdmissionFailure(error);
    }
    throw error;
  } finally {
    accepted?.release();
  }
}

export function assertClientVoiceSessionAdmission(): void {
  const accepted = captureClientVoiceSessionSettlement();
  accepted.release();
}

export function assertClientVoiceSessionSettlementCurrent(): void {
  const accepted = current.getStore();
  if (accepted) {
    if (!accepted.active) {
      throw new Error("Voice session persistence lost its accepted owner");
    }
    accepted.context.admission.assertCurrent();
  }
}

export function prepareClientVoiceSessionClose() {
  return lifetime(captureOpenClawStateWorkerContext()).retainGateway();
}
