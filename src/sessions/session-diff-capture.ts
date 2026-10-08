import { AsyncLocalStorage } from "node:async_hooks";
import type { InternalSessionEntry } from "../config/sessions/types.js";
import { resolveGlobalSingleton } from "../shared/global-singleton.js";

const captureScope = resolveGlobalSingleton(
  Symbol.for("openclaw.sessionDiffBaselineCaptureScope"),
  () => new AsyncLocalStorage<{ ready?: Promise<InternalSessionEntry>; settled?: boolean }>(),
);

/** Host-gated inference can overlap capture; reply settlement retains capture failures. */
export async function withSessionDiffBaselineCapture<T>(run: () => Promise<T>): Promise<T> {
  const scope: { ready?: Promise<InternalSessionEntry>; settled?: boolean } = {};
  return captureScope.run(scope, async () => {
    try {
      return await run();
    } finally {
      await scope.ready;
    }
  });
}

export function getSessionDiffBaselineCapture(): Promise<InternalSessionEntry> | undefined {
  return captureScope.getStore()?.ready;
}

export function bindSessionDiffBaselineCaptureAssertion(): () => void {
  const scope = captureScope.getStore();
  return () => {
    if (scope?.ready && !scope.settled) {
      throw new Error("Session diff baseline capture must settle before native tool execution");
    }
  };
}

export function deferSessionDiffBaselineCapture(ready: Promise<InternalSessionEntry>): void {
  const scope = captureScope.getStore();
  if (!scope) {
    throw new Error("Deferred session diff capture requires a reply scope");
  }
  scope.ready = ready;
  scope.settled = false;
  // Failed capture admission remains closed even for tools bound after settlement.
  void ready.then(
    () => {
      scope.settled = true;
    },
    () => undefined,
  );
}
