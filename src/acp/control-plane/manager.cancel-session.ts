import type { OpenClawConfig } from "../../config/types.openclaw.js";
import {
  AcpRuntimeError,
  toAcpRuntimeError,
  withAcpRuntimeErrorBoundary,
} from "../runtime/errors.js";
import {
  matchesAcpSessionRuntimeLocator,
  resolveAcpSessionControlOwner,
} from "../runtime/session-control-owner.js";
import type { AcpSessionRuntimeLocator } from "../runtime/session-meta-control.types.js";
import type { ManagerRuntimeHandleCache } from "./manager.runtime-handle-cache.js";
import type {
  ActiveTurnState,
  AcpSessionManagerDeps,
  EnsureManagerRuntimeHandle,
  ResolveManagerSessionAsync,
  SetManagerSessionState,
  WithManagerSessionActor,
} from "./manager.types.js";
import { requireReadySessionMeta } from "./manager.utils.js";

/** Sends ACP cancel to a backend that has no OpenClaw turn, such as a persistent runtime. */
export async function runManagerCancelSession(params: {
  assertActive?: () => void;
  cfg: OpenClawConfig;
  sessionKey: string;
  agentId: string;
  reason?: string;
  withSessionActor: WithManagerSessionActor;
  resolveSession: ResolveManagerSessionAsync;
  prepareSessionControlRead: AcpSessionManagerDeps["prepareSessionControlRead"];
  ensureRuntimeHandle: EnsureManagerRuntimeHandle;
  runtimeHandles: Pick<ManagerRuntimeHandleCache, "get" | "clearIfHandleMatches">;
  setSessionState: SetManagerSessionState;
}): Promise<void> {
  params.assertActive?.();
  const target = { cfg: params.cfg, sessionKey: params.sessionKey, agentId: params.agentId };
  type ControlRead = Awaited<ReturnType<AcpSessionManagerDeps["prepareSessionControlRead"]>>;
  const readCurrentOwner = async (read: ControlRead) => {
    const current = await read.readCurrent(params.cfg);
    read.assertCurrent(params.cfg);
    if (!current.session?.acp) {
      throw new AcpRuntimeError("ACP_TURN_FAILED", "ACP task owner could not be verified.");
    }
    return current;
  };
  await params.withSessionActor(params, async (isCurrentActor) => {
    const control = await params.prepareSessionControlRead(target);
    let runtimeLocator: AcpSessionRuntimeLocator | undefined;
    const assertTargetCurrent = () => {
      control.assertCurrent(params.cfg);
      control.assertNativeAcpCurrent?.(params.cfg, runtimeLocator);
      if (!isCurrentActor()) {
        throw new AcpRuntimeError("ACP_TURN_FAILED", "ACP session actor was replaced.");
      }
    };
    const assertAdmission = () => {
      assertTargetCurrent();
      params.assertActive?.();
    };
    try {
      assertAdmission();
      const resolution = await params.resolveSession({ ...target, assertCurrent: assertAdmission });
      assertAdmission();
      const resolvedMeta = requireReadySessionMeta(resolution);
      const { entry, constraint } = await readCurrentOwner(control);
      assertAdmission();
      const ownerKey = resolveAcpSessionControlOwner(entry);
      const { runtime, handle } = await params.ensureRuntimeHandle({
        ...target,
        assertActive: assertAdmission,
        meta: resolvedMeta,
        isCurrentActor,
        readAcpControl: () => constraint,
        assertMetadataCommitAllowed: (locator) => {
          control.assertNativeAcpCurrent?.(params.cfg, locator);
        },
        expectedControlBinding:
          entry && ownerKey
            ? {
                sessionId: entry.sessionId,
                lifecycleRevision: entry.lifecycleRevision,
                sessionStartedAt: entry.sessionStartedAt,
                ownerKey,
              }
            : undefined,
      });
      const ensuredLocator: AcpSessionRuntimeLocator = {
        backend: handle.backend,
        runtimeSessionName: handle.runtimeSessionName,
      };
      runtimeLocator = ensuredLocator;
      const readCurrentRuntime = async () => {
        const current = await readCurrentOwner(control);
        if (!matchesAcpSessionRuntimeLocator(current.session.acp, ensuredLocator)) {
          throw new AcpRuntimeError(
            "ACP_TURN_FAILED",
            "ACP runtime locator changed before cancellation.",
          );
        }
        return current.constraint
          ? { ...current.constraint, runtimeLocator: ensuredLocator }
          : undefined;
      };
      let failure: AcpRuntimeError | undefined;
      try {
        await readCurrentRuntime();
        assertAdmission();
        try {
          await runtime.cancel({ handle, reason: params.reason });
        } catch (error) {
          failure = toAcpRuntimeError({
            error,
            fallbackCode: "ACP_TURN_FAILED",
            fallbackMessage: "ACP cancel failed before completion.",
          });
        }
        const acpControl = await readCurrentRuntime();
        assertTargetCurrent();
        await params.setSessionState({
          ...target,
          state: failure ? "error" : "idle",
          lastError: failure?.message,
          clearLastError: !failure,
          isCurrentActor,
          assertCurrent: assertTargetCurrent,
          acpControl,
        });
      } catch (error) {
        // Retire only this cache entry; persistent backend state may have another owner.
        if (isCurrentActor() && params.runtimeHandles.get(target)?.handle === handle) {
          params.runtimeHandles.clearIfHandleMatches({ ...target, handle });
        }
        throw error;
      }
      if (failure) {
        throw failure;
      }
    } finally {
      control.release();
    }
  });
}

/** Aborts and deduplicates runtime cancellation for one active manager turn. */
export async function cancelManagerActiveTurn(params: {
  activeTurn: ActiveTurnState;
  reason?: string;
  assertCurrent?: () => void;
}): Promise<void> {
  if (!params.activeTurn.cancelPromise) {
    let runtimeCancelStarted = false;
    // The stream abort and its caller join the same runtime effect.
    const cancellation = withAcpRuntimeErrorBoundary({
      run: async () => {
        // Yield so reentrant cancellation from the abort below sees the installed promise.
        await Promise.resolve();
        params.assertCurrent?.();
        params.activeTurn.abortController.abort();
        runtimeCancelStarted = true;
        await params.activeTurn.runtime.cancel({
          handle: params.activeTurn.handle,
          reason: params.reason,
        });
      },
      fallbackCode: "ACP_TURN_FAILED",
      fallbackMessage: "ACP cancel failed before completion.",
    });
    params.activeTurn.cancelPromise = cancellation;
    void cancellation.catch(() => {
      // A refused admission can retry; an attempted RPC must retain its possibly applied outcome.
      if (!runtimeCancelStarted && params.activeTurn.cancelPromise === cancellation) {
        params.activeTurn.cancelPromise = undefined;
      }
    });
  }
  await params.activeTurn.cancelPromise;
}
