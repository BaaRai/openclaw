import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import {
  createAgentRunRestartAbortError,
  createAgentRunSupersededAbortError as createSupersededError,
  isAgentRunRestartAbortReason,
  isAgentRunSupersededAbortReason,
} from "../agents/run-termination.js";
import type { ReplyFollowupAdmissionBarrierTimeoutPolicy } from "../auto-reply/reply/reply-dispatcher.types.js";
import type * as replyRunSettle from "../auto-reply/reply/reply-run-finalization-lease.js";
import { createAbortError } from "../infra/abort-signal.js";
import { getAgentEventLifecycleGeneration } from "../infra/agent-events.js";
import { markDiagnosticRunProgress } from "../logging/diagnostic-run-activity.js";
import { diagnosticLogger as diag } from "../logging/diagnostic-runtime.js";
import { createDeferredCore } from "../shared/deferred.js";
import {
  initialReplyOperationState,
  transitionReplyOperation,
  type ReplyOperationEvent,
} from "./reply-operation-state.js";
import {
  flushReplyOperationAfterClear,
  registerFollowupAdmissionBarrier,
  startReplyOperationSuccessorBarriers,
  updateFollowupAdmissionSessionId,
  updateSuccessorAdmissionSessionId,
} from "./session-controller.barriers.js";
import type { ReplyBackendCancelReason, ReplyOperation } from "./session-controller.contracts.js";
import {
  releaseSessionControllerOperation,
  bindSessionControllerTarget,
  captureSessionTarget,
} from "./session-controller.lifecycle.js";
import {
  prepareReplyOperationAdmission,
  installReplyOperationAdmission,
  type CreateReplyOperationParams,
} from "./session-controller.operation-admission.js";
import {
  clearReplyRunState,
  evictReplyOperationByOperation,
  getAttachedBackend,
  isReplyOperationAbortable,
  notifyReplyRunEnded,
  operationsByUpstreamAbortSignal,
  producerCompletionByOperation,
  prepareReplyRunKeyUpdate,
  recordOperationBackendRunId,
  getSessionControllerEntry,
  addSessionControllerEntryAlias,
  controllerEntryByOperation,
  resolveReplyOperationAgentId,
  runAfterReplyOperationClear,
} from "./session-controller.state.js";
import { captureSessionControllerStop, stopSession } from "./session-controller.stop.js";
import { createTerminalProducerFenceRegistry } from "./session-controller.terminal-producer-fences.js";
import { createReplyOperationToolAuthority } from "./session-controller.tool-authority.js";
import {
  createSessionControllerWatchdog,
  type SessionWatchdogEffect,
  type SessionWatchdogWait,
} from "./session-controller.watchdog.js";

type ReplyOperationResult = NonNullable<ReplyOperation["result"]>;
type ReplyOperationAbortCode = Extract<ReplyOperationResult, { kind: "aborted" }>["code"];

export function createReplyOperation(params: CreateReplyOperationParams): ReplyOperation {
  const admitted = prepareReplyOperationAdmission(params);
  const { sessionKey, sessionId } = admitted;
  let owner = admitted.owner;
  const controller = new AbortController();
  // Mutable for rekey adoption; closures must read this, never params.sessionKey.
  let currentSessionKey = sessionKey;
  let currentSessionId = sessionId;
  let currentAgentId = resolveReplyOperationAgentId(sessionKey, params.agentId);
  let state = initialReplyOperationState();
  let staleExpiryReason: replyRunSettle.ReplyOperationStaleReason | undefined;
  let terminalRecovery = false;
  let acceptedSteeredInboundAudio = false;
  let sourceReplyDelivered = false;
  // A recorded yield ends the turn's work before settlement records it as the result.
  const ended = () => state.result !== null || state.phase === "yielded";
  const settledResult = (): ReplyOperationResult =>
    state.phase === "yielded" ? { kind: "yielded" } : { kind: "completed" };
  const toolAuthority = createReplyOperationToolAuthority({
    isOpen: () => !ended(),
    ownsRunSlot: () => owner.active === operation,
  });
  const ownerSettlement = createDeferredCore();
  const producerCompletion = createDeferredCore();
  let ownerCompletionBarrier: Promise<void> | undefined;
  let ownerSettled = false;
  let installed = false;
  let cleanupPreservesOutcome = false;
  const executionCleanups = new Set<() => Promise<void>>();
  const terminalProducerFences = createTerminalProducerFenceRegistry(() => ownerSettled);
  let phaseWait: SessionWatchdogWait | undefined;
  const finishOwner = () => {
    if (ownerSettled) {
      return;
    }
    ownerSettled = true;
    watchdog.close();
    ownerSettlement.resolve(undefined);
    releaseSessionControllerOperation(operation);
  };
  const settleOwner = (): void => {
    const pending = ownerCompletionBarrier;
    if (!pending) {
      finishOwner();
      return;
    }
    void pending.then(() => {
      if (pending !== ownerCompletionBarrier) {
        settleOwner();
        return;
      }
      finishOwner();
    });
  };
  const startedAtMs = Date.now();
  const lifecycleGeneration = getAgentEventLifecycleGeneration();
  let lastActivityAtMs = startedAtMs;
  const upstreamAbortSignal = params.upstreamAbortSignal;
  let upstreamAbortHandler: (() => void) | undefined;
  const detachUpstreamAbort = () => {
    if (!upstreamAbortHandler) {
      return;
    }
    upstreamAbortSignal?.removeEventListener("abort", upstreamAbortHandler);
    upstreamAbortHandler = undefined;
  };
  const ownedSessionIds = new Set([sessionId]);
  const recordActivity = () => {
    lastActivityAtMs = Date.now();
    watchdog.progress("transport");
  };

  const markProgress = (reason: string) => {
    // Phase observations must not renew the semantic-stall deadline.
    watchdog.progress("transport", reason);
    markDiagnosticRunProgress({
      sessionId: currentSessionId,
      sessionKey: currentSessionKey,
      reason,
    });
  };

  const apply = (event: ReplyOperationEvent) => {
    const transition = transitionReplyOperation(state, event);
    state = transition.state;
    for (const effect of transition.effects) {
      if (effect === "activity") {
        recordActivity();
      } else {
        const reasons = {
          "maintenance-wait": "deferred_maintenance:waiting",
          "maintenance-ready": "deferred_maintenance:wait_ended",
          "lane-wait": "global_lane:waiting",
          "lane-ready": "global_lane:wait_ended",
        };
        phaseWait?.close();
        phaseWait = undefined;
        if (effect === "maintenance-wait" || effect === "lane-wait") {
          const phase = state.phase;
          phaseWait = watchdog.beginWait({
            kind: effect === "maintenance-wait" ? "deferred_maintenance" : "global_capacity",
            isCurrent: () => owner.active === operation && !state.result && state.phase === phase,
          });
        } else {
          watchdog.progress("semantic", reasons[effect]);
        }
        markProgress(reasons[effect]);
      }
    }
  };
  const setResult = (result: ReplyOperationResult) => {
    apply({ type: "result", result });
    toolAuthority.close();
    phaseWait?.close();
    watchdog.beginTerminal();
  };

  const clearState = (
    afterClearBarrier?: PromiseLike<unknown>,
    followupAdmissionBarrierTimeout?: number | ReplyFollowupAdmissionBarrierTimeoutPolicy,
  ) => {
    if (state.cleared) {
      return;
    }
    apply({ type: "clear" });
    toolAuthority.close();
    phaseWait?.close();
    evictReplyOperationByOperation.delete(operation);
    detachUpstreamAbort();
    const registeredBarrier = afterClearBarrier
      ? registerFollowupAdmissionBarrier(
          operation,
          afterClearBarrier,
          followupAdmissionBarrierTimeout,
        )
      : undefined;
    updateFollowupAdmissionSessionId(operation);
    // Start owner handoff before waking a successor that could snapshot state it mutates.
    startReplyOperationSuccessorBarriers(operation);
    markProgress("reply_operation:ended");
    clearReplyRunState({
      sessionKey: currentSessionKey,
      sessionId: currentSessionId,
      operation,
    });
    if (!registeredBarrier) {
      flushReplyOperationAfterClear(operation, currentSessionId);
      return;
    }
    void registeredBarrier.settled.then(() =>
      flushReplyOperationAfterClear(operation, registeredBarrier.source.sessionId),
    );
  };

  const abortInternally = (reason?: unknown) => {
    if (!controller.signal.aborted) {
      controller.abort(reason);
    }
  };

  const expireOwner = async (
    effect: SessionWatchdogEffect,
    cleanup: boolean,
  ): Promise<"settled" | "blocked"> => {
    if (!effect.isCurrent()) {
      return ownerSettled ? "settled" : "blocked";
    }
    const backend = getAttachedBackend(operation);
    const cleanups = [...executionCleanups];
    if (!effect.isCurrent()) {
      return ownerSettled ? "settled" : "blocked";
    }
    const failures: unknown[] = [];
    try {
      if (cleanup) {
        // After committed output, only this captured owner may retire native resources.
        cleanupPreservesOutcome ||= state.abortFrozen && !state.result;
        detachUpstreamAbort();
        backend?.cancel("superseded");
      } else {
        stopSession({
          capture: captureSessionControllerStop({ operations: [operation] }),
          source: "watchdog",
          assertCurrent: () => {
            if (!effect.isCurrent()) {
              throw new Error("Watchdog owner retired before Stop");
            }
          },
        });
      }
    } catch (error) {
      failures.push(error);
    }
    for (const cleanupOwner of cleanups) {
      // Stop may close the attempt synchronously; captured cleanup custody survives it.
      if (ownerSettled) {
        break;
      }
      try {
        await cleanupOwner();
      } catch (error) {
        failures.push(error);
      }
    }
    if (cleanup && ended() && owner.active === operation) {
      const { failures: fenceFailures, fenced } = await terminalProducerFences.revokeAll();
      failures.push(...fenceFailures);
      if (fenced) {
        // Durable revocation rejects late writers before this exact operation releases its slot.
        if (!state.result) {
          setResult(settledResult());
        }
        clearState();
        settleOwner();
      }
    }
    if (failures.length) {
      throw new AggregateError(failures, "Watchdog cleanup remains blocked");
    }
    return ownerSettled ? "settled" : "blocked";
  };
  const watchdog = createSessionControllerWatchdog({
    startedAtMs,
    // Exact custody survives rekey and rotation; never rediscover the owner through its slot.
    isCurrent: () => installed && !ownerSettled,
    readPhase: () =>
      ownerSettled ? "settled" : ended() ? "terminal" : state.abortFrozen ? "finishing" : "active",
    requestStop: (effect) => expireOwner(effect, false),
    expireCleanup: (effect) => expireOwner(effect, true),
    onWarning: (decision) =>
      diag.warn(
        `reply watchdog: sessionKey=${currentSessionKey} action=${decision.action} reason=${decision.reason}`,
      ),
  });

  const abortOperation = (
    reason: ReplyBackendCancelReason,
    abortReason: unknown,
    abortedCode: ReplyOperationAbortCode,
  ) => {
    const backend = getAttachedBackend(operation);
    if (!state.result) {
      setResult({ kind: "aborted", code: abortedCode });
      detachUpstreamAbort();
    }
    abortInternally(abortReason);
    // Cancellation may throw. Only actual producer completion releases custody.
    try {
      backend?.cancel(reason);
    } finally {
      watchdog.beginTerminal();
    }
  };
  // Agent-run abort codes on the reason select the cancel reason and result code.
  const abortWithReason = (reason: unknown) => {
    const restart = isAgentRunRestartAbortReason(reason);
    const superseded = isAgentRunSupersededAbortReason(reason);
    abortOperation(
      restart ? "restart" : superseded ? "superseded" : "user_abort",
      reason,
      restart ? "aborted_for_restart" : superseded ? "aborted_for_supersession" : "aborted_by_user",
    );
  };
  const abortIfAbortable = (reason: () => unknown) => {
    if (!isReplyOperationAbortable(operation)) {
      return false;
    }
    abortWithReason(reason());
    return true;
  };

  const operation: ReplyOperation = {
    get key() {
      return currentSessionKey;
    },
    get sessionId() {
      return currentSessionId;
    },
    get agentId() {
      return currentAgentId;
    },
    turnKind: params.turnKind ?? "visible",
    lifecycleGeneration,
    get routeThreadId() {
      return params.routeThreadId;
    },
    get originatingLeafEntryId() {
      return params.originatingLeafEntryId;
    },
    abortSignal: controller.signal,
    watchdog,
    get resetTriggered() {
      return params.resetTriggered;
    },
    get terminalRecovery() {
      return terminalRecovery;
    },
    get acceptedSteeredInboundAudio() {
      return acceptedSteeredInboundAudio;
    },
    get sourceReplyDelivered() {
      return sourceReplyDelivered;
    },
    get toolAuthorityFingerprint() {
      return toolAuthority.toolAuthorityFingerprint;
    },
    get personalToolParticipants() {
      return toolAuthority.personalToolParticipants;
    },
    get toolAuthorityRoute() {
      return toolAuthority.toolAuthorityRoute;
    },
    get requestedToolAuthorityRoute() {
      return toolAuthority.requestedToolAuthorityRoute;
    },
    get automaticFallbackRoute() {
      return toolAuthority.automaticFallbackRoute;
    },
    setAutomaticFallbackRoute: toolAuthority.setAutomaticFallbackRoute,
    get phase() {
      return state.phase;
    },
    get result() {
      return state.result;
    },
    get abortFrozen() {
      return state.abortFrozen;
    },
    registerExecutionCleanup(cleanup) {
      if (ownerSettled) {
        throw new Error("Operation already settled");
      }
      executionCleanups.add(cleanup);
      return () => {
        executionCleanups.delete(cleanup);
      };
    },
    registerTerminalProducerFence: (fence) => terminalProducerFences.register(fence),
    get terminalProducerBlocked() {
      return terminalProducerFences.blocked;
    },
    get staleExpiryReason() {
      return staleExpiryReason;
    },
    get startedAtMs() {
      return startedAtMs;
    },
    get lastActivityAtMs() {
      return lastActivityAtMs;
    },
    hasOwnedSessionId(candidateSessionId) {
      const normalizedSessionId = normalizeOptionalString(candidateSessionId);
      return normalizedSessionId ? ownedSessionIds.has(normalizedSessionId) : false;
    },
    captureOwnedSessionIds() {
      return new Set(ownedSessionIds);
    },
    recordActivity,
    setPhase(phase) {
      apply({ type: "phase", phase });
    },
    markWaitingForDeferredMaintenance() {
      apply({ type: "maintenance-wait" });
    },
    markDeferredMaintenanceWaitEnded() {
      apply({ type: "maintenance-ready" });
    },
    markWaitingForGlobalLane() {
      apply({ type: "lane-wait" });
    },
    markGlobalLaneWaitEnded() {
      apply({ type: "lane-ready" });
    },
    markTerminalRecovery() {
      terminalRecovery = true;
    },
    markSteeredInputAccepted({ inboundAudio }) {
      acceptedSteeredInboundAudio ||= inboundAudio;
      sourceReplyDelivered = false;
    },
    markSourceReplyDelivered() {
      sourceReplyDelivered = true;
    },
    bindToolAuthoritySnapshot: toolAuthority.bindToolAuthoritySnapshot,
    projectToolAuthorityFingerprint: toolAuthority.projectToolAuthorityFingerprint,
    bindToolAuthorityRoute: toolAuthority.bindToolAuthorityRoute,
    updateSessionId(nextSessionId) {
      if (state.result) {
        return;
      }
      const normalizedNextSessionId = normalizeOptionalString(nextSessionId);
      if (!normalizedNextSessionId || normalizedNextSessionId === currentSessionId) {
        return;
      }
      recordActivity();
      currentSessionId = normalizedNextSessionId;
      ownedSessionIds.add(currentSessionId);
      addSessionControllerEntryAlias(owner, currentSessionId);
      if (owner.target) {
        bindSessionControllerTarget(
          operation,
          captureSessionTarget({
            ...owner.target,
            aliases: [...owner.logicalAliases],
            incarnation: currentSessionId,
          }),
        );
      }
      updateFollowupAdmissionSessionId(operation);
      updateSuccessorAdmissionSessionId(operation, currentSessionId);
      markProgress("reply_operation:session_updated");
    },
    updateSessionKey(nextSessionKey, agentId, mailboxClaim) {
      const update = prepareReplyRunKeyUpdate(
        operation,
        nextSessionKey,
        agentId,
        state.cleared,
        mailboxClaim,
      );
      if (!update) {
        return;
      }
      recordActivity();
      currentAgentId = update.agentId;
      if (update.sessionKey === currentSessionKey) {
        return;
      }
      const previousOwner = owner;
      const capturedTarget = mailboxClaim?.mailbox.owner.target ?? owner.target;
      const target = capturedTarget
        ? captureSessionTarget({
            ...capturedTarget,
            sessionKey: update.sessionKey,
            aliases: mailboxClaim ? capturedTarget.aliases : [update.sessionKey],
            // A moved command selects the destination identity, not the source incarnation.
            incarnation: mailboxClaim ? capturedTarget.incarnation : undefined,
          })
        : undefined;
      const nextOwner =
        mailboxClaim?.mailbox.owner ?? getSessionControllerEntry(update.sessionKey, target);
      if (nextOwner !== owner) {
        previousOwner.active = undefined;
      }
      currentSessionKey = update.sessionKey;
      owner = nextOwner;
      owner.active = operation;
      addSessionControllerEntryAlias(owner, currentSessionId);
      controllerEntryByOperation.set(operation, owner);
      if (mailboxClaim) {
        mailboxClaim.operation = operation;
      }
      if (target) {
        bindSessionControllerTarget(operation, target);
      }
      if (previousOwner !== owner) {
        notifyReplyRunEnded(previousOwner);
      }
      markProgress("reply_operation:session_key_adopted");
    },
    attachBackend(handle) {
      if (state.result || state.cleared || owner.active !== operation) {
        handle.cancel(
          state.result?.kind === "aborted"
            ? state.result.code === "aborted_for_restart"
              ? "restart"
              : state.result.code === "aborted_for_supersession"
                ? "superseded"
                : "user_abort"
            : "superseded",
        );
        return;
      }
      recordActivity();
      toolAuthority.bindBackendFingerprint(handle.toolAuthorityFingerprint);
      recordOperationBackendRunId(operation, handle.runId);
      owner.attachment = {
        operation,
        backend: handle,
        projectSessionActive:
          owner.attachment?.operation === operation
            ? owner.attachment.projectSessionActive
            : undefined,
      };
      if (controller.signal.aborted) {
        handle.cancel("superseded");
      }
    },
    detachBackend(handle) {
      if (owner.active === operation && owner.attachment?.backend === handle) {
        owner.attachment.backend = undefined;
        if (
          !("handle" in owner.attachment) &&
          owner.attachment.projectSessionActive === undefined
        ) {
          owner.attachment = undefined;
        }
      }
    },
    freezeAbort() {
      apply({ type: "freeze" });
      detachUpstreamAbort();
      watchdog.beginFinalization();
    },
    yield() {
      const before = state;
      apply({ type: "yield" });
      if (state === before) {
        return false;
      }
      toolAuthority.close();
      phaseWait?.close();
      watchdog.beginTerminal();
      return true;
    },
    ownerSettlement: ownerSettlement.promise,
    complete() {
      producerCompletion.resolve();
      if (!state.result) {
        setResult(settledResult());
      }
      clearState();
      settleOwner();
    },
    completeThen(afterClear) {
      runAfterReplyOperationClear(operation, afterClear);
      operation.complete();
    },
    completeWithAfterClearBarrier(barrier, timeoutMs) {
      // Producer work is done; delivery may still need a successor operation.
      producerCompletion.resolve();
      // Admission may time out, but the writer settles only after its actual delivery barrier.
      const completed = Promise.resolve(barrier).then(
        () => {},
        () => {},
      );
      ownerCompletionBarrier = ownerCompletionBarrier
        ? Promise.all([ownerCompletionBarrier, completed]).then(() => {})
        : completed;
      if (!state.result) {
        setResult(settledResult());
      }
      clearState(barrier, timeoutMs);
      settleOwner();
    },
    fail(code, cause) {
      if (cleanupPreservesOutcome && !state.result) {
        return;
      }
      apply({ type: "freeze" });
      detachUpstreamAbort();
      watchdog.beginTerminal();
      if (!state.result) {
        setResult({ kind: "failed", code, cause });
      }
      watchdog.beginTerminal();
    },
    abort: (reason) =>
      abortIfAbortable(() => reason ?? createAbortError("Reply operation aborted by user")),
    abortByUser: () => abortIfAbortable(() => createAbortError("Reply operation aborted by user")),
    abortForRestart: () => abortIfAbortable(createAgentRunRestartAbortError),
    abortForStall() {
      if (!isReplyOperationAbortable(operation)) {
        return false;
      }
      const backend = getAttachedBackend(operation);
      staleExpiryReason ??= "no_activity";
      apply({ type: "freeze" });
      setResult({ kind: "failed", code: "run_stalled" });
      detachUpstreamAbort();
      abortInternally(createAbortError("Reply operation stalled"));
      try {
        backend?.cancel("superseded");
      } finally {
        watchdog.beginTerminal();
      }
      return true;
    },
    supersede(beforeSupersede) {
      const abortFrozen = state.abortFrozen;
      if (
        state.result ||
        cleanupPreservesOutcome ||
        state.cleared ||
        (!abortFrozen && !isReplyOperationAbortable(operation))
      ) {
        return false;
      }
      beforeSupersede?.();
      if (abortFrozen) {
        setResult({ kind: "aborted", code: "aborted_for_supersession" });
        watchdog.beginTerminal();
        return true;
      }
      abortOperation("superseded", createSupersededError(), "aborted_for_supersession");
      return true;
    },
  };

  producerCompletionByOperation.set(operation, producerCompletion.promise);
  operationsByUpstreamAbortSignal.set(operation.abortSignal, operation);
  evictReplyOperationByOperation.set(operation, () => {
    if (state.cleared) {
      return;
    }
    if (!state.result) {
      setResult({ kind: "aborted", code: "aborted_for_restart" });
    }
    abortInternally(createAgentRunRestartAbortError());
    try {
      getAttachedBackend(operation)?.cancel("restart");
    } catch (error) {
      diag.warn(
        `reply run lifecycle eviction cancel failed: sessionKey=${currentSessionKey} error=${String(error)}`,
      );
      throw error;
    } finally {
      watchdog.beginTerminal();
    }
  });

  installReplyOperationAdmission(operation, admitted, params.mailboxClaim);
  installed = true;
  watchdog.start();
  markProgress("reply_operation:queued");
  if (upstreamAbortSignal) {
    operationsByUpstreamAbortSignal.set(upstreamAbortSignal, operation);
    const abortFromUpstream = () => {
      if (!state.result) {
        abortWithReason(upstreamAbortSignal.reason);
      }
    };
    if (upstreamAbortSignal.aborted) {
      abortFromUpstream();
    } else {
      upstreamAbortSignal.addEventListener("abort", abortFromUpstream, { once: true });
      upstreamAbortHandler = abortFromUpstream;
    }
  }

  return operation;
}
