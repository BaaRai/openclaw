import { scheduleMainSessionRecoveryPendingTarget } from "../../agents/main-session-recovery/main-session-recovery-owner-release.js";
import { isMainRestartRecoveryCandidate } from "../../agents/main-session-recovery/main-session-recovery-state.js";
import {
  claimMainSessionRecoveryOwner,
  type MainSessionRecoveryOwnerLease,
} from "../../agents/main-session-recovery/main-session-recovery-store.js";
import { beginForegroundSessionMaintenance } from "../../agents/session-maintenance/coordinator.js";
import {
  isRestartRecoveryTombstone,
  SessionWorkStartChangedError,
  resolveSessionWorkStartError,
} from "../../config/sessions/lifecycle.js";
import type { SessionAdmissionDatabaseClaim } from "../../config/sessions/session-accessor.sqlite-entry-admission.js";
import { loadSessionEntryForAdmission } from "../../config/sessions/session-accessor.sqlite-entry.js";
import { normalizeStoreSessionKey } from "../../config/sessions/store-entry.js";
import type { InternalSessionEntry, SessionEntry } from "../../config/sessions/types.js";
import type { GatewayRecoveryRuntime } from "../../gateway/server-instance-runtime.types.js";
import type { GatewayContextResolver } from "../../gateway/server-methods/types.js";
import { getAgentEventLifecycleGeneration } from "../../infra/agent-events.js";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import {
  bindGatewayContextResolver,
  getGatewayContextResolver,
  getPluginRuntimeGatewayRequestScope,
} from "../../plugins/runtime/gateway-request-scope.js";
import { parseAgentSessionKey } from "../../routing/session-key.js";
import { evaluateTurnAdmission } from "../../sessions/session-controller.admission-rule.js";
import { createSessionControllerPhaseLogger } from "../../sessions/session-controller.diagnostics.js";
import {
  createReplyOperation,
  isReplyRunSuccessorAdmissionBlocked,
  REPLY_RUN_IDLE_SETTLE_TIMEOUT_MS,
  ReplyRunAlreadyActiveError,
  ReplyRunFollowupAdmissionBlockedError,
  ReplyRunSuccessorAdmissionBlockedError,
  runAfterReplyOperationClear,
  type ReplyOperation,
  type ReplyTurnKind,
  waitForReplyRunFollowupAdmission,
  waitForReplyRunSuccessorAdmission,
  waitForSessionRunIdle,
} from "../../sessions/session-controller.js";
import {
  beginSessionEffect,
  captureSessionTarget,
  type SessionEffectRef,
} from "../../sessions/session-controller.lifecycle.js";
import type { SessionControllerMailboxClaim } from "../../sessions/session-controller.mailbox.js";
import {
  getSessionControllerEntry,
  findSessionControllerEntry,
  bindSessionControllerEntryTarget,
  expireVisibleStaleOperation,
  lifecycleAdmissionByOperation,
  resolveVisibleActiveWaitMs,
  sessionControllers,
} from "../../sessions/session-controller.state.js";
import {
  QueuedFollowupLifecycleInvalidatedError,
  rejectLifecycleInvalidatedWork,
} from "./reply-turn-admission-errors.js";
import {
  bindReplyAdmissionRelease,
  releaseReplyRecoveryOwner,
} from "./reply-turn-admission-lifecycle.js";
import { retryRestartRecoveryBeforeSelectedClaim } from "./reply-turn-recovery-predecessor.js";
import { waitForRestartRecoveryProgress } from "./reply-turn-recovery-wait.js";
import { createReplyTurnRotationEvidence } from "./reply-turn-rotation.js";

export { runWithReplyOperationLifecycleAdmission } from "./reply-turn-admission-lifecycle.js";

/** Admission result for a reply turn attempting to own the session run slot. */
type ReplyTurnAdmission =
  | {
      status: "owned";
      operation: ReplyOperation;
      sessionEntry?: SessionEntry;
      databaseClaim?: SessionAdmissionDatabaseClaim;
    }
  | {
      status: "skipped";
      reason: "active-run" | "aborted" | "lifecycle-invalidated";
      activeOperation?: ReplyOperation;
      sessionEntry?: SessionEntry;
      lifecycleAdmission?: SessionEffectRef;
    };

class ReplyOperationChangedDuringAdmissionError extends Error {}

const log = createSubsystemLogger("auto-reply/reply-turn-admission");

function isAbortSignalAborted(signal: AbortSignal | undefined): boolean {
  return signal?.aborted === true;
}

type ReplyTurnAdmissionParams = {
  mailboxClaim?: SessionControllerMailboxClaim;
  rotationEvidence?: ReturnType<typeof createReplyTurnRotationEvidence>;
  runId?: string;
  assertRequestCurrent?: () => void;
  providerReviewAcknowledgment?: import("../../sessions/provider-review.js").ProviderReviewAcknowledgment;
  agentId?: string;
  sessionKey: string;
  sessionId: string;
  expectedSessionId?: string;
  /** Observed predecessors, from oldest to newest. */
  expectedActiveOperations?: readonly ReplyOperation[];
  storePath?: string;
  kind: ReplyTurnKind;
  resetTriggered: boolean;
  allowRestartTombstoneParentFork?: boolean;
  allowRestartTombstoneReset?: boolean;
  routeThreadId?: string | number;
  originatingLeafEntryId?: string | null;
  /**
   * Move this already-held operation into sessionKey's run slot instead of
   * creating a new one. Used when a native command turn (admitted under its
   * slash source key) continues into a full agent turn on the target session.
   */
  adoptOperation?: ReplyOperation;
  upstreamAbortSignal?: AbortSignal;
  resolveGatewayContext?: GatewayContextResolver;
  waitTimeoutMs?: number;
  waitForActive?: boolean;
  retainLifecycleAdmissionOnActive?: boolean;
  onLifecycleInterrupt?: () => void;
};

/** Waits for or claims the per-session reply run slot. */
export async function admitReplyTurn(
  params: ReplyTurnAdmissionParams,
): Promise<ReplyTurnAdmission> {
  const target = params.storePath
    ? captureSessionTarget({
        storeScope: params.storePath,
        sessionKey: params.sessionKey,
        agentId: params.agentId,
      })
    : undefined;
  const controller =
    params.mailboxClaim?.mailbox.owner ??
    findSessionControllerEntry(params.sessionKey, target) ??
    getSessionControllerEntry(params.sessionKey, target);
  if (target && !controller.target) {
    bindSessionControllerEntryTarget(controller, target);
  }
  if (target && controller.target?.storeScope !== target.storeScope) {
    throw new Error("Reply claim belongs to a different physical store");
  }
  // Private registry identity is only a read/wait key. Logical keys still own
  // persistence, operation identity, and external routing.
  const controllerKey = controller.id;
  const activeAtAdmission = controller.active;
  const releaseForeground =
    params.kind === "visible"
      ? await beginForegroundSessionMaintenance(params.sessionKey)
      : undefined;
  let foregroundTransferred = false;
  // Maintenance may finish after the observed reply rotates and clears its slot.
  let sessionId = activeAtAdmission?.result ? activeAtAdmission.sessionId : params.sessionId;
  const resolveGatewayContext = params.adoptOperation
    ? getGatewayContextResolver(params.adoptOperation)
    : Object.hasOwn(params, "resolveGatewayContext")
      ? params.resolveGatewayContext
      : getPluginRuntimeGatewayRequestScope()?.resolveGatewayContext;
  let expectedSessionId = params.expectedSessionId;
  const lifecycleGeneration = getAgentEventLifecycleGeneration();
  let recoveryDispatchOutcome: "deferred" | "failed" | undefined;
  const rotations =
    params.rotationEvidence ??
    createReplyTurnRotationEvidence({
      sessionKey: params.sessionKey,
      controller,
      expectedActiveOperations: params.expectedActiveOperations,
      activeAtAdmission,
    });

  const waitTimeoutMs =
    params.waitTimeoutMs ??
    (params.kind === "queued_followup" ? REPLY_RUN_IDLE_SETTLE_TIMEOUT_MS : undefined);
  let admittedDatabaseClaim: SessionAdmissionDatabaseClaim | undefined;
  let discardedDatabaseClaimRelease: Promise<void> | undefined;
  let owned = false;
  let admitting = true;
  function rejectSessionChange(
    message = `Session "${params.sessionKey}" changed while starting work. Retry.`,
  ): never {
    rejectLifecycleInvalidatedWork({ kind: params.kind, message, transientSessionChange: true });
  }
  const assertDatabaseOwnerCurrent = (nextClaim?: SessionAdmissionDatabaseClaim) => {
    if (
      admittedDatabaseClaim &&
      (!admittedDatabaseClaim.isCurrent() ||
        (nextClaim && nextClaim.incarnation !== admittedDatabaseClaim.incarnation))
    ) {
      const release = nextClaim?.release();
      if (release) {
        discardedDatabaseClaimRelease = release;
        // The synchronous assertion revokes now; the outer finally joins release.
        void release.catch(() => {});
      }
      rejectSessionChange(
        `Session store for "${params.sessionKey}" changed while starting work. Retry.`,
      );
    }
  };
  const assertRecoveryOwnerCurrent = (
    recoveryRuntime: GatewayRecoveryRuntime | undefined,
    action: "starting" | "waiting for",
  ) => {
    assertDatabaseOwnerCurrent();
    if (
      lifecycleGeneration !== getAgentEventLifecycleGeneration() ||
      resolveGatewayContext?.()?.recoveryRuntime !== recoveryRuntime
    ) {
      rejectSessionChange(
        `Session "${params.sessionKey}" changed while ${action} recovery. Retry.`,
      );
    }
  };
  const waitForRecovery = async () => {
    const recoveryRuntime = resolveGatewayContext?.()?.recoveryRuntime;
    await waitForRestartRecoveryProgress({
      agentId: params.agentId,
      sessionKey: params.sessionKey,
      signal: params.upstreamAbortSignal,
    });
    assertRecoveryOwnerCurrent(recoveryRuntime, "waiting for");
  };
  // Retries may release a lifecycle lease, but cannot replace the first physical
  // database owner after waiting for an active turn, delivery, or writer.
  try {
    while (true) {
      if (isAbortSignalAborted(params.upstreamAbortSignal)) {
        return { status: "skipped", reason: "aborted" };
      }
      const storelessRotation = !params.storePath ? rotations.takeStorelessRotation() : undefined;
      if (storelessRotation) {
        if (expectedSessionId && !storelessRotation.sessionIds.has(expectedSessionId)) {
          return { status: "skipped", reason: "lifecycle-invalidated" };
        }
        sessionId = storelessRotation.sessionId;
        expectedSessionId = expectedSessionId ? storelessRotation.sessionId : undefined;
      }
      if (isReplyRunSuccessorAdmissionBlocked(controllerKey)) {
        if (params.kind === "heartbeat") {
          return { status: "skipped", reason: "active-run" };
        }
        const logBarrier = createSessionControllerPhaseLogger("successor-barrier", {
          sessionKey: params.sessionKey,
          sessionId,
          sourceId: params.runId,
        });
        logBarrier("waiting");
        const successorAdmission = await waitForReplyRunSuccessorAdmission(
          controllerKey,
          params.kind === "visible" ? null : waitTimeoutMs,
          { signal: params.upstreamAbortSignal },
        );
        logBarrier(
          successorAdmission.settled ? "settled" : "failed",
          successorAdmission.settled ? undefined : "aborted-or-deadline",
        );
        if (!successorAdmission.settled) {
          return {
            status: "skipped",
            reason: isAbortSignalAborted(params.upstreamAbortSignal) ? "aborted" : "active-run",
          };
        }
        rotations.recordBarrierSources(successorAdmission.sources);
        continue;
      }
      const turnAdmission = evaluateTurnAdmission(controller, {
        kind: params.kind,
        sessionKey: params.sessionKey,
        registeredEntry: sessionControllers.get(controller.id),
        claim: params.mailboxClaim,
      });
      if (!turnAdmission.admitted && turnAdmission.reason === "followup-barrier") {
        if (params.kind === "heartbeat") {
          return { status: "skipped", reason: "active-run" };
        }
        // Pin the physical database before waiting on retained delivery custody.
        // This is a read claim, not turn admission or permission to write.
        if (params.storePath && !admittedDatabaseClaim) {
          const current = await loadSessionEntryForAdmission(
            {
              agentId: params.agentId,
              storePath: params.storePath,
              sessionKey: params.sessionKey,
              readConsistency: "latest",
            },
            {
              signal: params.upstreamAbortSignal,
              assertCurrent: () => params.assertRequestCurrent?.(),
            },
          );
          admittedDatabaseClaim = current.databaseClaim;
        }
        const logBarrier = createSessionControllerPhaseLogger("followup-barrier", {
          sessionKey: params.sessionKey,
          sessionId,
          sourceId: params.runId,
        });
        logBarrier("waiting");
        const settlement = await waitForReplyRunFollowupAdmission(
          controllerKey,
          waitTimeoutMs ?? REPLY_RUN_IDLE_SETTLE_TIMEOUT_MS,
          { signal: params.upstreamAbortSignal },
        );
        logBarrier(
          settlement.settled ? "settled" : "failed",
          settlement.settled ? undefined : "aborted-or-deadline",
        );
        if (!settlement.settled) {
          return {
            status: "skipped",
            reason: isAbortSignalAborted(params.upstreamAbortSignal) ? "aborted" : "active-run",
          };
        }
        if (admittedDatabaseClaim && !admittedDatabaseClaim.isCurrent()) {
          return { status: "skipped", reason: "lifecycle-invalidated" };
        }
        rotations.recordBarrierSources(settlement.sources);
        continue;
      }
      const rotationObservation = params.storePath ? rotations.observeAdmission() : undefined;
      try {
        const storePath = params.storePath;
        let operation: ReplyOperation | undefined;
        let admittedSessionEntry: InternalSessionEntry | undefined;
        let recoveryOwnerLease: MainSessionRecoveryOwnerLease | undefined;
        let interruptedBeforeOperation = false;
        let recoveryClaimStarted = false;
        const admission = storePath
          ? await beginSessionEffect({
              scope: storePath,
              resolveGatewayContext,
              identities: [params.sessionKey],
              storeWriterIdentities:
                parseAgentSessionKey(params.sessionKey) &&
                normalizeStoreSessionKey(params.sessionKey) === params.sessionKey
                  ? [params.sessionKey]
                  : undefined,
              signal: params.upstreamAbortSignal,
              onInterrupt: () => {
                interruptedBeforeOperation = true;
                operation?.abortForRestart();
                params.onLifecycleInterrupt?.();
              },
              assertAllowed: async (signal) => {
                assertDatabaseOwnerCurrent();
                const assertCurrent = () => {
                  params.assertRequestCurrent?.();
                  assertDatabaseOwnerCurrent();
                  if (
                    !admitting ||
                    interruptedBeforeOperation ||
                    lifecycleGeneration !== getAgentEventLifecycleGeneration()
                  ) {
                    throw new SessionWorkStartChangedError(
                      "Session changed while waiting for state admission.",
                    );
                  }
                };
                const current = await loadSessionEntryForAdmission(
                  {
                    agentId: params.agentId,
                    storePath,
                    sessionKey: params.sessionKey,
                    readConsistency: "latest",
                  },
                  {
                    signal,
                    assertCurrent,
                  },
                );
                if (
                  !admitting ||
                  interruptedBeforeOperation ||
                  params.upstreamAbortSignal?.aborted
                ) {
                  await current.databaseClaim.release();
                  throw new SessionWorkStartChangedError("Session changed during state admission.");
                }
                try {
                  params.assertRequestCurrent?.();
                } catch (error) {
                  await current.databaseClaim.release();
                  throw error;
                }
                assertDatabaseOwnerCurrent(current.databaseClaim);
                const previousDatabaseClaim = admittedDatabaseClaim;
                admittedDatabaseClaim = current.databaseClaim;
                await previousDatabaseClaim?.release();
                signal.throwIfAborted();
                assertCurrent();
                const currentEntry = current.entry;
                admittedSessionEntry = currentEntry;
                if (expectedSessionId && !currentEntry) {
                  rejectSessionChange(
                    `Session "${params.sessionKey}" was deleted while starting work. Retry.`,
                  );
                }
                rotationObservation?.recordCompletions();
                const activeOperationRotatedExpectedSession = rotations.hasExpectedSessionRotation({
                  expectedSessionId,
                  sessionId: currentEntry?.sessionId,
                  databaseIdentity: admittedDatabaseClaim?.identity,
                });
                if (
                  expectedSessionId &&
                  currentEntry?.sessionId !== expectedSessionId &&
                  !activeOperationRotatedExpectedSession
                ) {
                  rejectSessionChange();
                }
                if (activeOperationRotatedExpectedSession) {
                  expectedSessionId = currentEntry?.sessionId;
                }
                const archivedSessionError = resolveSessionWorkStartError(
                  params.sessionKey || sessionId,
                  currentEntry,
                  {
                    providerReviewAcknowledgment: params.providerReviewAcknowledgment,
                    allowRestartTombstoneReplacement:
                      (params.resetTriggered && params.allowRestartTombstoneReset === true) ||
                      params.allowRestartTombstoneParentFork === true,
                  },
                );
                if (archivedSessionError) {
                  const tombstone = currentEntry?.mainRestartRecovery?.tombstone;
                  if (params.kind === "visible" && tombstone) {
                    log.warn(`${archivedSessionError} Recovery reason: ${tombstone.reason}`);
                  }
                  rejectLifecycleInvalidatedWork({
                    kind: params.kind,
                    message: archivedSessionError,
                    restartRecoveryTombstone: isRestartRecoveryTombstone(currentEntry),
                  });
                }
                sessionId = currentEntry?.sessionId ?? sessionId;
              },
            })
          : undefined;
        try {
          if (isReplyRunSuccessorAdmissionBlocked(controllerKey)) {
            throw new ReplyRunSuccessorAdmissionBlockedError(params.sessionKey);
          }
          const shouldClaimRecoveryOwner =
            storePath &&
            !params.resetTriggered &&
            params.allowRestartTombstoneParentFork !== true &&
            admittedSessionEntry &&
            ((admittedSessionEntry.status === "running" &&
              (admittedSessionEntry.abortedLastRun === true ||
                (params.kind !== "heartbeat" &&
                  admittedSessionEntry.restartRecoveryRuns !== undefined))) ||
              admittedSessionEntry.mainRestartRecovery?.tombstone !== undefined) &&
            isMainRestartRecoveryCandidate(admittedSessionEntry, params.sessionKey);
          const gatewayContext = resolveGatewayContext?.();
          const recoveryRuntime = gatewayContext?.recoveryRuntime;
          if (
            shouldClaimRecoveryOwner &&
            admittedSessionEntry?.abortedLastRun === true &&
            !admittedSessionEntry.mainRestartRecovery?.tombstone &&
            params.kind !== "heartbeat" &&
            gatewayContext &&
            recoveryRuntime
          ) {
            // The interrupted turn owns its delivery claim. Resume it before the
            // new input enters ordinary queue selection; a foreground claim would
            // instead block recovery while this input rejects the old delivery claim.
            admission?.release();
            if (recoveryDispatchOutcome) {
              if (params.kind === "queued_followup") {
                return { status: "skipped", reason: "active-run" };
              }
              if (recoveryDispatchOutcome === "failed") {
                throw new Error(`Restart recovery failed: ${params.sessionKey}. See Gateway logs.`);
              }
              await waitForRecovery();
              recoveryDispatchOutcome = undefined;
              continue;
            }
            assertRecoveryOwnerCurrent(recoveryRuntime, "starting");
            params.upstreamAbortSignal?.throwIfAborted();
            const recovery = await retryRestartRecoveryBeforeSelectedClaim({
              agentId: params.agentId,
              cfg: gatewayContext.getRuntimeConfig(),
              claim: params.mailboxClaim,
              expectedRecoveryRunId: admittedSessionEntry.restartRecoveryDeliveryRunId,
              expectedRecoverySourceRunId: admittedSessionEntry.restartRecoveryDeliverySourceRunId,
              gatewayRuntime: recoveryRuntime,
              sessionId,
              sessionKey: params.sessionKey,
              storePath,
              upstreamAbortSignal: params.upstreamAbortSignal,
            });
            if (!recovery) {
              continue;
            }
            assertRecoveryOwnerCurrent(recoveryRuntime, "starting");
            recoveryDispatchOutcome = recovery.failed > 0 ? "failed" : "deferred";
            // Recovery may have completed or another owner may have won. Reload
            // the exact session and its live owner instead of using this snapshot.
            continue;
          }
          if (shouldClaimRecoveryOwner) {
            // A claim can durably clear recovery state. Once it starts, a later
            // preparation change must fail this admission instead of replaying it.
            recoveryClaimStarted = true;
            const ownerClaim = await claimMainSessionRecoveryOwner({
              lifecycleGeneration: getAgentEventLifecycleGeneration(),
              sessionId,
              target: { agentId: params.agentId, sessionKey: params.sessionKey, storePath },
            });
            if (ownerClaim.kind === "invalidated") {
              rejectSessionChange();
            }
            recoveryOwnerLease = ownerClaim.kind === "claimed" ? ownerClaim.lease : undefined;
            admittedSessionEntry = ownerClaim.entry;
          }
          if (interruptedBeforeOperation || isAbortSignalAborted(params.upstreamAbortSignal)) {
            rejectSessionChange();
          }
          assertDatabaseOwnerCurrent();
          if (rotationObservation?.changed()) {
            if (recoveryClaimStarted) {
              rejectSessionChange();
            }
            // A predecessor can rotate after the final row read but before this handoff.
            // Reacquire the full admission; its session ID alone grants no authority.
            throw new ReplyOperationChangedDuringAdmissionError();
          }
          if (params.adoptOperation) {
            // The dispatch closures own this object's abort/delivery lifecycle,
            // so the reservation must move rather than be recreated. Throws
            // ReplyRunAlreadyActiveError into the shared busy handling below.
            params.adoptOperation.updateSessionKey(
              params.sessionKey,
              params.agentId,
              params.mailboxClaim,
            );
            operation = params.adoptOperation;
          } else {
            operation = createReplyOperation({
              mailboxClaim: params.mailboxClaim,
              target: params.storePath
                ? captureSessionTarget({
                    storeScope: params.storePath,
                    sessionKey: params.sessionKey,
                    incarnation: sessionId,
                    agentId: params.agentId,
                  })
                : undefined,
              sessionKey: params.sessionKey,
              sessionId,
              agentId: params.agentId,
              turnKind: params.kind,
              resetTriggered: params.resetTriggered,
              routeThreadId: params.routeThreadId,
              originatingLeafEntryId: params.originatingLeafEntryId,
              upstreamAbortSignal: params.upstreamAbortSignal,
            });
            bindGatewayContextResolver(operation, resolveGatewayContext);
          }
        } catch (error) {
          const pendingRecovery = recoveryOwnerLease
            ? await releaseReplyRecoveryOwner(recoveryOwnerLease)
            : undefined;
          if (
            error instanceof ReplyRunAlreadyActiveError &&
            admission &&
            params.retainLifecycleAdmissionOnActive
          ) {
            void admission.released.then(() => {
              scheduleMainSessionRecoveryPendingTarget(pendingRecovery);
            });
            return {
              status: "skipped",
              reason: "active-run",
              activeOperation: controller.active,
              ...(admittedSessionEntry ? { sessionEntry: admittedSessionEntry } : {}),
              lifecycleAdmission: admission,
            };
          }
          admission?.release();
          scheduleMainSessionRecoveryPendingTarget(pendingRecovery);
          throw error;
        }
        const databaseClaim = admittedDatabaseClaim;
        bindReplyAdmissionRelease({
          operation,
          admission,
          databaseClaim,
          recoveryOwnerLease,
          sessionId,
          sessionKey: params.sessionKey,
        });
        if (releaseForeground) {
          foregroundTransferred = true;
          // Priority follows admission; optional jobs separately wait for real delivery.
          runAfterReplyOperationClear(operation, releaseForeground);
        }
        owned = true;
        return {
          status: "owned",
          operation,
          databaseClaim,
          ...(admittedSessionEntry ? { sessionEntry: admittedSessionEntry } : {}),
        };
      } catch (error) {
        if (isAbortSignalAborted(params.upstreamAbortSignal)) {
          return { status: "skipped", reason: "aborted" };
        }
        if (error instanceof QueuedFollowupLifecycleInvalidatedError) {
          return { status: "skipped", reason: "lifecycle-invalidated" };
        }
        if (error instanceof ReplyOperationChangedDuringAdmissionError) {
          if (!rotations.hasCurrentRotationEvidence()) {
            rejectLifecycleInvalidatedWork({
              kind: params.kind,
              message: `Session "${params.sessionKey}" changed while starting work. Retry.`,
              transientSessionChange: true,
            });
          }
          continue;
        }
        if (error instanceof ReplyRunSuccessorAdmissionBlockedError) {
          if (params.kind === "heartbeat") {
            return { status: "skipped", reason: "active-run" };
          }
          continue;
        }
        if (error instanceof ReplyRunFollowupAdmissionBlockedError) {
          if (params.kind === "heartbeat") {
            return { status: "skipped", reason: "active-run" };
          }
          const followupAdmission = await waitForReplyRunFollowupAdmission(
            controllerKey,
            waitTimeoutMs ?? REPLY_RUN_IDLE_SETTLE_TIMEOUT_MS,
            { signal: params.upstreamAbortSignal },
          );
          if (!followupAdmission.settled) {
            return {
              status: "skipped",
              reason: isAbortSignalAborted(params.upstreamAbortSignal) ? "aborted" : "active-run",
            };
          }
          rotations.recordBarrierSources(followupAdmission.sources);
          continue;
        }
        if (!(error instanceof ReplyRunAlreadyActiveError)) {
          throw error;
        }
        const activeOperation = controller.active;
        if (params.kind === "visible" && activeOperation?.turnKind === "heartbeat") {
          // Background heartbeats must yield before queue policy can steer this
          // user turn into the heartbeat's model run and lose its visible reply.
          activeOperation.supersede();
        }
        if (params.kind === "visible" && expireVisibleStaleOperation(activeOperation)) {
          continue;
        }
        // Visible and queued turns may wait for active runs when waitForActive is set.
        if (params.kind === "heartbeat" || params.waitForActive === false) {
          return { status: "skipped", reason: "active-run", activeOperation };
        }
        const activeWaitTimeoutMs =
          params.kind === "visible" ? resolveVisibleActiveWaitMs(activeOperation) : waitTimeoutMs;
        const activeDatabaseIdentity = activeOperation
          ? lifecycleAdmissionByOperation.get(activeOperation)?.databaseIdentity
          : undefined;
        const ended = await waitForSessionRunIdle(controllerKey, activeWaitTimeoutMs, {
          signal: params.upstreamAbortSignal,
        });
        if (!ended) {
          if (params.kind === "visible" && !isAbortSignalAborted(params.upstreamAbortSignal)) {
            // Visible turns block on active work like before, but in bounded wait
            // slices: each wake reclaims the owner once it is provably stale,
            // otherwise loops back to keep waiting.
            const latestActiveOperation = controller.active;
            expireVisibleStaleOperation(latestActiveOperation ?? activeOperation);
            continue;
          }
          return {
            status: "skipped",
            reason: isAbortSignalAborted(params.upstreamAbortSignal) ? "aborted" : "active-run",
            activeOperation,
          };
        }
        if (activeOperation) {
          rotations.recordCompletedOperation(activeOperation, activeDatabaseIdentity);
        }
      } finally {
        rotationObservation?.dispose();
      }
    }
  } finally {
    admitting = false;
    if (!foregroundTransferred) {
      releaseForeground?.();
    }
    if (!owned) {
      try {
        await admittedDatabaseClaim?.release();
      } finally {
        await discardedDatabaseClaimRelease;
      }
    }
  }
}

/** Resolves the default turn kind from reply options. */
export function resolveReplyTurnKind(opts?: { isHeartbeat?: boolean }): ReplyTurnKind {
  return opts?.isHeartbeat === true ? "heartbeat" : "visible";
}
