import crypto from "node:crypto";
import { readChannelContextGatewayContextResolver } from "../../channels/message-access/admission-evidence.js";
import {
  isRestartRecoveryTombstone,
  isSessionWorkStartInvalidatedError,
} from "../../config/sessions/lifecycle.js";
import {
  loadSessionEntryReadOnly,
  patchSessionEntryCore,
} from "../../config/sessions/session-accessor.js";
import { isRecoverableTerminalSessionStatus } from "../../config/sessions/terminal-status.js";
import type { SessionEntry } from "../../config/sessions/types.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import {
  prepareSessionWorkerPlacementMutationCheck,
  resolveWorkerPlacementArchiveRestoreError,
  type SessionWorkerPlacementContext,
} from "../../gateway/worker-environments/session-placement-lifecycle.js";
import { logVerbose } from "../../globals.js";
import { getPluginRuntimeGatewayRequestScope } from "../../plugins/runtime/gateway-request-scope.js";
import {
  type ReplyOperation,
  waitForReplyBarrierSettlement,
  getSessionControllerOperation,
} from "../../sessions/session-controller.js";
import {
  runSessionMutation,
  type SessionEffectRef,
} from "../../sessions/session-controller.lifecycle.js";
import {
  claimSessionControllerTask,
  releaseSessionControllerClaim,
  tryClaimSessionControllerTask,
  trackSessionControllerSourceWork,
} from "../../sessions/session-controller.mailbox.js";
import { classifySessionStateActor } from "../../sessions/session-state-events.js";
import {
  isNativeCommandTurn,
  resolveCommandTurnContext,
  resolveCommandTurnTargetSessionKey,
} from "../command-turn-context.js";
import { isExplicitCommandTurnContext } from "../command-turn-detection.js";
import { isActiveRunSafeCommandTurn } from "../commands-registry.js";
import type { FinalizedMsgContext } from "../templating.js";
import {
  createAbortAwareDispatcher,
  DispatchReplyOperationAbortedError,
} from "./dispatch-from-config.abort.js";
import type { InboundMessageAuditTerminalRecorder } from "./dispatch-from-config.audit.js";
import {
  resolveDispatchResetAdmission,
  shouldLetSlackRoutedThreadBypassBusyReplyOperation,
} from "./dispatch-from-config.context.js";
import { createReplyTurnLedger } from "./dispatch-from-config.turn-ledger.js";
import type { DispatchFromConfigParams } from "./dispatch-from-config.types.js";
import { DispatchSessionRefreshRequiredError } from "./dispatch-session-refresh-error.js";
import { REPLY_ADMISSION_TICKET } from "./reply-admission-ticket.js";
import { waitForReplyDispatcherIdle } from "./reply-dispatcher.js";
import type { ReplyDispatcher } from "./reply-dispatcher.types.js";
import { resolveReplyOperationRunState } from "./reply-operation-run-state.js";
import { readReplySourceInput } from "./reply-source-binding.js";
import {
  admitReplyTurn,
  resolveReplyTurnKind,
  runWithReplyOperationLifecycleAdmission,
} from "./reply-turn-admission.js";
import { isUnauthorizedTextSlashCommand } from "./source-reply-delivery-mode.js";

type DispatchReplyOperationAcquisition =
  | { status: "ready" }
  | { status: "busy" }
  | { status: "aborted" };

async function restoreArchivedDispatchSession(params: {
  ctx: FinalizedMsgContext;
  entry?: SessionEntry;
  hasPluginOwnedBinding: boolean;
  placementContext?: SessionWorkerPlacementContext;
  sessionKey?: string;
  storePath?: string;
}): Promise<SessionEntry | undefined> {
  const { ctx, entry, hasPluginOwnedBinding, sessionKey, storePath } = params;
  if (
    !entry ||
    !sessionKey ||
    !storePath ||
    entry.archivedAt === undefined ||
    isRestartRecoveryTombstone(entry) ||
    hasPluginOwnedBinding ||
    ctx.InboundAccessAuthorized !== true ||
    ctx.InboundEventKind === "room_event" ||
    isNativeCommandTurn(ctx.CommandTurn) ||
    classifySessionStateActor({ inputProvenance: ctx.InputProvenance }).actorType !== "human"
  ) {
    return entry;
  }
  let placementContext = params.placementContext;
  if (!placementContext) {
    try {
      placementContext = (
        await import("../../gateway/session-worker-placement-context.js")
      ).resolveSessionWorkerPlacementContext();
    } catch {
      return entry;
    }
  }
  const snapshotSessionId = entry.sessionId;
  const snapshotArchivedAt = entry.archivedAt;
  const canRestore = (currentEntry: SessionEntry) => {
    if (
      currentEntry.sessionId !== snapshotSessionId ||
      currentEntry.archivedAt !== snapshotArchivedAt ||
      isRestartRecoveryTombstone(currentEntry)
    ) {
      return false;
    }
    try {
      const placement = currentEntry.sessionId
        ? placementContext.workerSessionPlacementService
            ?.getMany([currentEntry.sessionId])
            .get(currentEntry.sessionId)
        : undefined;
      return !resolveWorkerPlacementArchiveRestoreError({
        context: placementContext,
        key: sessionKey,
        placement,
      });
    } catch {
      return false;
    }
  };
  return await runSessionMutation({
    scope: storePath,
    identities: [sessionKey, snapshotSessionId],
    run: async () => {
      const scope = { sessionKey, storePath };
      const currentEntry = loadSessionEntryReadOnly(scope);
      if (!currentEntry || !canRestore(currentEntry)) {
        return currentEntry;
      }
      let assertCommitAllowed: (() => void) | undefined;
      if (currentEntry.worktree) {
        const { synchronizeSessionWorktreeArchive } =
          await import("../../sessions/session-worktree-lifecycle.js");
        // Keep the target fenced through Git/allocation waits without retaining the agent writer.
        assertCommitAllowed = await synchronizeSessionWorktreeArchive({
          archived: false,
          entry: currentEntry,
          scope,
          commitGuard: prepareSessionWorkerPlacementMutationCheck({
            context: placementContext,
            sessionId: currentEntry.sessionId,
          }),
        });
      }
      const updatedEntry = await patchSessionEntryCore(
        scope,
        (current) =>
          canRestore(current)
            ? { archivedAt: undefined, archivedBy: undefined, archiveReason: undefined }
            : null,
        // The writer may have waited; revalidate the prepared binding at the actual commit edge.
        { assertCommitAllowed },
      );
      return updatedEntry ?? undefined;
    },
  });
}

export function createDispatchReplyOperationCoordinator(params: {
  allowActiveQueueResolution?: boolean;
  agentId: string;
  cfg: OpenClawConfig;
  ctx: FinalizedMsgContext;
  dispatcher: ReplyDispatcher;
  dispatchOperationSessionKey?: string;
  initialDispatchReplyOperation?: ReplyOperation;
  messageAuditTerminal?: InboundMessageAuditTerminalRecorder;
  operationSessionStoreEntry: {
    entry?: SessionEntry;
    storePath?: string;
  };
  replyOptions?: DispatchFromConfigParams["replyOptions"];
  sessionWorkerPlacementContext?: SessionWorkerPlacementContext;
  resolveOperationExpectedSessionId: () => string | undefined;
  routeThreadId?: string | number;
}) {
  let dispatchReplyOperation: ReplyOperation | undefined;
  let admittedExpectedSessionId: string | undefined;
  let dispatchAbortOperation: ReplyOperation | undefined;
  let preDispatchAbortOperation: ReplyOperation | undefined;
  let preDispatchLifecycleAdmission: SessionEffectRef | undefined;
  let removePreDispatchLifecycleAbortListener: (() => void) | undefined;
  let preDispatchLifecycleAbortController: AbortController | undefined;
  let dispatchLifecycleAbortController: AbortController | undefined;
  let preDispatchLifecycleInterrupted = false;
  let dispatchResetTriggered = false;
  let allowRestartTombstoneParentFork = false;
  let allowRestartTombstoneReset = false;
  const dispatchLifecycleWork = {
    owner: new Set<Promise<void>>(),
    delivery: new Set<Promise<void>>(),
  };

  const trackDispatchLifecycleWork = (
    work: Promise<unknown>,
    phase: "owner" | "delivery" = "owner",
  ) => {
    const input = readReplySourceInput(params.replyOptions);
    if (input) {
      trackSessionControllerSourceWork(input, work);
    }
    if (!dispatchReplyOperation && !preDispatchLifecycleAdmission) {
      return;
    }
    const pending = dispatchLifecycleWork[phase];
    const settled = work.then(
      () => {},
      () => {},
    );
    pending.add(settled);
    void settled.then(() => {
      pending.delete(settled);
    });
  };

  const waitForDispatchDelivery = async (): Promise<void> => {
    await Promise.allSettled(Array.from(dispatchLifecycleWork.delivery));
    await waitForReplyDispatcherIdle(params.dispatcher);
  };

  const releasePreDispatchLifecycleAdmission = async (
    afterWorkBarrier?: () => PromiseLike<unknown>,
  ): Promise<void> => {
    removePreDispatchLifecycleAbortListener?.();
    removePreDispatchLifecycleAbortListener = undefined;
    const admission = preDispatchLifecycleAdmission;
    const preDispatchAbortController = preDispatchLifecycleAbortController;
    const dispatchAbortController = dispatchLifecycleAbortController;
    preDispatchLifecycleAdmission = undefined;
    if (!admission) {
      return;
    }
    const pendingWork = [...dispatchLifecycleWork.owner, ...dispatchLifecycleWork.delivery];
    const clearAbortControllers = () => {
      if (preDispatchLifecycleAbortController === preDispatchAbortController) {
        preDispatchLifecycleAbortController = undefined;
      }
      if (dispatchLifecycleAbortController === dispatchAbortController) {
        dispatchLifecycleAbortController = undefined;
      }
    };
    if (!afterWorkBarrier && pendingWork.length === 0) {
      clearAbortControllers();
      admission.release();
      return;
    }
    try {
      await Promise.allSettled(pendingWork);
      if (afterWorkBarrier) {
        await waitForReplyBarrierSettlement(
          afterWorkBarrier(),
          params.dispatcher.resolveFollowupAdmissionBarrierTimeoutPolicy?.(),
        );
      }
    } finally {
      clearAbortControllers();
      admission.release();
    }
  };

  const armPreDispatchLifecycleAbortRelease = () => {
    const abortSignal =
      params.replyOptions?.turnAdoptionLifecycle?.abortSignal ?? params.replyOptions?.abortSignal;
    if (!abortSignal || !preDispatchLifecycleAdmission) {
      return;
    }
    removePreDispatchLifecycleAbortListener?.();
    const onAbort = () => {
      void releasePreDispatchLifecycleAdmission(() =>
        waitForReplyDispatcherIdle(params.dispatcher),
      );
    };
    abortSignal.addEventListener("abort", onAbort, { once: true });
    removePreDispatchLifecycleAbortListener = () =>
      abortSignal.removeEventListener("abort", onAbort);
    if (abortSignal.aborted) {
      onAbort();
    }
  };

  const runWithDispatchLifecycleAdmission = async <T>(run: () => Promise<T>): Promise<T> => {
    if (dispatchReplyOperation) {
      return await runWithReplyOperationLifecycleAdmission(dispatchReplyOperation, run);
    }
    const work = preDispatchLifecycleAdmission
      ? preDispatchLifecycleAdmission.run(run)
      : Promise.resolve().then(run);
    const input = readReplySourceInput(params.replyOptions);
    if (input) {
      trackSessionControllerSourceWork(input, work);
    }
    return await work;
  };

  const ensureDispatchReplyOperation = async (
    phase: "pre_dispatch" | "command_resolution" | "dispatch",
    hasPluginOwnedBinding = false,
  ): Promise<DispatchReplyOperationAcquisition> => {
    // Native controls must never acquire a target turn or wait for its capacity.
    // A command which continues into model execution retargets its unclaimed
    // input there and uses the ordinary mailbox selector.
    if (
      isNativeCommandTurn(resolveCommandTurnContext(params.ctx)) ||
      (resolveCommandTurnTargetSessionKey(params.ctx) &&
        resolveCommandTurnTargetSessionKey(params.ctx) !== params.dispatchOperationSessionKey)
    ) {
      return { status: "ready" };
    }
    // Archive restoration belongs to pre-dispatch ownership resolution. Later calls only upgrade admission.
    if (phase === "pre_dispatch") {
      params.operationSessionStoreEntry.entry = await restoreArchivedDispatchSession({
        ctx: params.ctx,
        entry: params.operationSessionStoreEntry.entry,
        hasPluginOwnedBinding,
        placementContext: params.sessionWorkerPlacementContext,
        sessionKey: params.dispatchOperationSessionKey,
        storePath: params.operationSessionStoreEntry.storePath,
      });
      ({
        resetTriggered: dispatchResetTriggered,
        allowRestartTombstoneParentFork,
        allowRestartTombstoneReset,
      } = resolveDispatchResetAdmission({
        agentId: params.agentId,
        cfg: params.cfg,
        ctx: params.ctx,
        entry: params.operationSessionStoreEntry.entry,
        hasPluginOwnedBinding,
        sessionKey: params.dispatchOperationSessionKey,
        storePath: params.operationSessionStoreEntry.storePath,
      }));
    }
    if (phase !== "pre_dispatch") {
      // The next full reply operation revalidates the persisted session. Drop
      // the hook-only lease after its queued delivery settles so a waiting
      // lifecycle mutation cannot commit while that delivery is still active.
      await releasePreDispatchLifecycleAdmission(() =>
        waitForReplyDispatcherIdle(params.dispatcher),
      );
      if (preDispatchLifecycleInterrupted) {
        return { status: dispatchReplyOperation ? "aborted" : "busy" };
      }
    }
    if (dispatchReplyOperation) {
      return { status: "ready" };
    }
    if (dispatchAbortOperation && !dispatchAbortOperation.result) {
      return { status: "busy" };
    }
    if (
      phase !== "pre_dispatch" &&
      preDispatchAbortOperation?.result &&
      preDispatchAbortOperation.result.kind !== "completed" &&
      preDispatchAbortOperation.result.kind !== "yielded" &&
      // Low-level queue resolution can abort the old owner before final delivery acquires its
      // successor operation. The old result belongs to that owner, not to this inbound turn.
      params.allowActiveQueueResolution !== true
    ) {
      dispatchAbortOperation = preDispatchAbortOperation;
      return { status: "busy" };
    }
    const dispatchOperationSessionKey = params.dispatchOperationSessionKey;
    if (!dispatchOperationSessionKey) {
      return { status: "ready" };
    }
    const operationSessionId =
      dispatchAbortOperation?.sessionId ??
      params.operationSessionStoreEntry.entry?.sessionId ??
      crypto.randomUUID();
    const replyTurnKind = resolveReplyTurnKind(params.replyOptions);
    const input = readReplySourceInput(params.replyOptions);
    const activeReplyOperation = input
      ? input.mailbox.owner.active
      : getSessionControllerOperation(dispatchOperationSessionKey);
    const commandRequiresTurn =
      (isExplicitCommandTurnContext(params.ctx, params.cfg) ||
        isUnauthorizedTextSlashCommand(params.ctx)) &&
      !isActiveRunSafeCommandTurn({
        commandTurn: resolveCommandTurnContext(params.ctx),
        cfg: params.cfg,
        provider: params.ctx.Provider ?? params.ctx.Surface,
      });
    const allowQueuePreparation = replyTurnKind === "visible" && !commandRequiresTurn;
    const allowActiveResolution =
      replyTurnKind === "visible" && (phase === "pre_dispatch" || phase === "command_resolution");
    const allowGatewayQueueResolution =
      phase !== "pre_dispatch" &&
      allowQueuePreparation &&
      (input !== undefined || params.allowActiveQueueResolution === true) &&
      activeReplyOperation !== undefined &&
      activeReplyOperation.turnKind !== "heartbeat";
    if (allowGatewayQueueResolution) {
      // Gateway and low-level plugin turns must reach getReplyFromConfig while the owner is active;
      // that layer applies the session's steer/followup/collect/drop policy without concurrent runs.
      return { status: "ready" };
    }
    const allowSlackRoutedThreadBypass =
      phase !== "pre_dispatch" &&
      shouldLetSlackRoutedThreadBypassBusyReplyOperation({
        activeOperation: activeReplyOperation,
        ctx: params.ctx,
        routeThreadId: params.routeThreadId,
      });
    const lifecycleOnlyAbortController =
      allowActiveResolution || allowSlackRoutedThreadBypass ? new AbortController() : undefined;
    const onLifecycleInterrupt = () => {
      preDispatchLifecycleInterrupted = true;
      lifecycleOnlyAbortController?.abort();
    };
    // Queue/steer/question completion may already have handed this exact input
    // off without a dispatch operation. Final delivery must not reacquire it.
    if (input && (input.custody.enqueued || input.phase === "consumed" || input.injection)) {
      return { status: "ready" };
    }
    // Fast control events run before this gate. Ordinary pre-dispatch preparation
    // may proceed while occupied, but only the canonical selector grants a turn.
    let mailboxClaim = input ? tryClaimSessionControllerTask(input, replyTurnKind) : undefined;
    if (input && !mailboxClaim) {
      if (input.abortSignal.aborted || input.retirementRequested) {
        return { status: "aborted" };
      }
      const predecessor = input.mailbox.owner.active;
      if (
        predecessor &&
        isRecoverableTerminalSessionStatus(params.operationSessionStoreEntry.entry?.status)
      ) {
        // Persisted terminal state is not proof that raw producer work settled.
        // Recovery acts only on this captured owner, never a by-ID successor.
        await predecessor.watchdog.tick();
        mailboxClaim = tryClaimSessionControllerTask(input, replyTurnKind);
      }
      if (
        !mailboxClaim &&
        (allowActiveResolution ||
          allowQueuePreparation ||
          allowSlackRoutedThreadBypass ||
          allowGatewayQueueResolution)
      ) {
        return { status: "ready" };
      }
      if (!mailboxClaim) {
        if (replyTurnKind === "heartbeat") {
          return { status: "busy" };
        }
        const selected = claimSessionControllerTask(input, () => {}, replyTurnKind);
        params.replyOptions?.[REPLY_ADMISSION_TICKET]?.release();
        try {
          mailboxClaim = await selected;
        } catch (error) {
          if (input.abortSignal.aborted) {
            return { status: "aborted" };
          }
          throw error;
        }
      }
    }
    let admission: Awaited<ReturnType<typeof admitReplyTurn>>;
    try {
      admission = await admitReplyTurn({
        mailboxClaim,
        runId: params.replyOptions?.runId,
        assertRequestCurrent: () => params.replyOptions?.operatorAuthority?.assertCurrent(),
        providerReviewAcknowledgment: params.replyOptions?.providerReviewAcknowledgment,
        agentId: params.agentId,
        sessionKey: dispatchOperationSessionKey,
        resolveGatewayContext:
          readChannelContextGatewayContextResolver(params.ctx) ??
          getPluginRuntimeGatewayRequestScope()?.resolveGatewayContext,
        sessionId: operationSessionId,
        expectedSessionId:
          params.replyOptions?.expectedExistingSessionId ??
          params.resolveOperationExpectedSessionId(),
        expectedActiveOperations: [
          params.replyOptions?.expectedActiveReplyOperation,
          params.initialDispatchReplyOperation,
        ].filter((operation): operation is ReplyOperation => operation !== undefined),
        storePath: params.operationSessionStoreEntry.storePath,
        kind: replyTurnKind,
        resetTriggered: dispatchResetTriggered,
        allowRestartTombstoneParentFork,
        allowRestartTombstoneReset,
        routeThreadId: params.routeThreadId,
        originatingLeafEntryId: params.replyOptions?.turnAdoptionLifecycle?.originatingLeafEntryId,
        upstreamAbortSignal: input?.abortSignal ?? params.replyOptions?.abortSignal,
        waitForActive: !allowActiveResolution && !allowSlackRoutedThreadBypass,
        retainLifecycleAdmissionOnActive: allowActiveResolution || allowSlackRoutedThreadBypass,
        onLifecycleInterrupt,
      });
      if (mailboxClaim) {
        const claim = mailboxClaim;
        if (admission.status === "owned") {
          // Keep adoption open while preparation binds the actual FollowupRun.
          // Only raw settlement releases the selected claim, not callbacks.
          const releaseClaim = () => releaseSessionControllerClaim(claim);
          void admission.operation.ownerSettlement.then(releaseClaim, releaseClaim);
        } else {
          releaseSessionControllerClaim(claim);
        }
      }
    } catch (error) {
      if (mailboxClaim) {
        releaseSessionControllerClaim(mailboxClaim);
      }
      if (
        phase === "pre_dispatch" &&
        replyTurnKind === "visible" &&
        isSessionWorkStartInvalidatedError(error)
      ) {
        throw new DispatchSessionRefreshRequiredError(error);
      }
      throw error;
    }
    // Admission has verified the predecessor's lineage in this physical store.
    // Carry that identity through initialization even when the active run still owns the slot.
    admittedExpectedSessionId =
      admission.status === "owned"
        ? admission.operation.sessionId
        : admission.sessionEntry?.sessionId;
    const runState = resolveReplyOperationRunState(params.replyOptions);
    // A turn already accepted into the queue or active run keeps that custody when
    // dispatch later finds the session idle; the queued turn owns its answer.
    if (runState && runState.admission?.status !== "accepted") {
      runState.admission =
        admission.status === "owned"
          ? { status: "owned" }
          : { status: "skipped", reason: admission.reason };
    }
    if (admission.status === "skipped") {
      if (allowActiveResolution && admission.reason === "active-run") {
        preDispatchAbortOperation = admission.activeOperation;
        preDispatchLifecycleAdmission = admission.lifecycleAdmission;
        if (phase === "pre_dispatch") {
          preDispatchLifecycleAbortController = lifecycleOnlyAbortController;
        } else {
          dispatchLifecycleAbortController = lifecycleOnlyAbortController;
        }
        armPreDispatchLifecycleAbortRelease();
        return { status: "ready" };
      }
      if (
        admission.reason === "active-run" &&
        shouldLetSlackRoutedThreadBypassBusyReplyOperation({
          activeOperation: admission.activeOperation,
          ctx: params.ctx,
          routeThreadId: params.routeThreadId,
        })
      ) {
        preDispatchLifecycleAdmission = admission.lifecycleAdmission;
        dispatchLifecycleAbortController = lifecycleOnlyAbortController;
        armPreDispatchLifecycleAbortRelease();
        logVerbose(
          `dispatch-from-config: allowing Slack routed thread ${params.routeThreadId} while ${dispatchOperationSessionKey} has an active reply operation in another Slack thread`,
        );
        return { status: "ready" };
      }
      admission.lifecycleAdmission?.release();
      dispatchAbortOperation = admission.activeOperation;
      logVerbose(
        `dispatch-from-config: skipped reply operation admission for ${dispatchOperationSessionKey}; reason=${admission.reason}`,
      );
      return { status: "busy" };
    }
    // Mark both initial and replacement admissions before a sibling can mistake
    // this recovery for the terminal predecessor it observed (#86827).
    if (
      replyTurnKind === "visible" &&
      isRecoverableTerminalSessionStatus(params.operationSessionStoreEntry.entry?.status) &&
      operationSessionId === params.operationSessionStoreEntry.entry?.sessionId
    ) {
      admission.operation.markTerminalRecovery();
    }
    dispatchReplyOperation = admission.operation;
    dispatchAbortOperation = admission.operation;
    return { status: "ready" };
  };

  let cachedPreDispatchAbortSignal:
    | {
        operationSignal: AbortSignal | undefined;
        lifecycleSignal: AbortSignal | undefined;
        upstreamSignal: AbortSignal | undefined;
        signal: AbortSignal | undefined;
      }
    | undefined;
  const getPreDispatchAbortSignal = () => {
    const operationSignal = (dispatchAbortOperation ?? preDispatchAbortOperation)?.abortSignal;
    const lifecycleSignal = preDispatchLifecycleAbortController?.signal;
    const upstreamSignal = params.replyOptions?.abortSignal;
    if (
      cachedPreDispatchAbortSignal &&
      cachedPreDispatchAbortSignal.operationSignal === operationSignal &&
      cachedPreDispatchAbortSignal.lifecycleSignal === lifecycleSignal &&
      cachedPreDispatchAbortSignal.upstreamSignal === upstreamSignal
    ) {
      return cachedPreDispatchAbortSignal.signal;
    }
    const abortSignals = [operationSignal, lifecycleSignal, upstreamSignal].filter(
      (signal): signal is AbortSignal => Boolean(signal),
    );
    const signal = abortSignals.length > 1 ? AbortSignal.any(abortSignals) : abortSignals[0];
    cachedPreDispatchAbortSignal = { operationSignal, lifecycleSignal, upstreamSignal, signal };
    return signal;
  };

  const getDispatchAbortSignal = () => {
    const operationSignal =
      dispatchReplyOperation?.abortSignal ?? dispatchLifecycleAbortController?.signal;
    // The operation mirrors upstream aborts until the backend commits its
    // terminal outcome, then keeps delivery alive during bounded finalization.
    return operationSignal ?? params.replyOptions?.abortSignal;
  };

  const getQueuedFollowupAbortSignal = () =>
    params.replyOptions?.turnAdoptionLifecycle?.abortSignal ??
    dispatchReplyOperation?.abortSignal ??
    params.replyOptions?.abortSignal;
  let observedReplyDelivery = false;
  let agentRunTerminalOutcome: "completed" | "failed" | undefined;
  let agentRunId = params.replyOptions?.runId;
  const markObservedReplyDelivery = async () => {
    if (observedReplyDelivery) {
      return;
    }
    observedReplyDelivery = true;
    dispatchReplyOperation?.watchdog.progress("finalization", "reply:delivery_observed");
    await params.replyOptions?.onObservedReplyDelivery?.();
  };
  const getReplyOptions = (): DispatchFromConfigParams["replyOptions"] => {
    const abortSignal = getDispatchAbortSignal();
    const expectedExistingSessionId = params.replyOptions?.expectedExistingSessionId
      ? (dispatchReplyOperation?.sessionId ??
        params.replyOptions.expectedActiveReplyOperation?.sessionId ??
        preDispatchAbortOperation?.sessionId ??
        admittedExpectedSessionId)
      : undefined;
    return {
      ...params.replyOptions,
      ...(expectedExistingSessionId ? { expectedExistingSessionId } : {}),
      ...(abortSignal
        ? {
            abortSignal,
            queuedFollowupAbortSignal: getQueuedFollowupAbortSignal(),
          }
        : {}),
      onAgentRunStart: (...args) => {
        agentRunTerminalOutcome = "completed";
        // Execution may generate its ID in copied options; finalization needs the observed run.
        agentRunId = args[0];
        params.messageAuditTerminal?.observeRunId(args[0]);
        return params.replyOptions?.onAgentRunStart?.(...args);
      },
      onAgentRunTerminalOutcome: (outcome) => {
        if (outcome === "failed" || agentRunTerminalOutcome === undefined) {
          agentRunTerminalOutcome = outcome;
        }
        params.replyOptions?.onAgentRunTerminalOutcome?.(outcome);
      },
      ...(dispatchReplyOperation ? { replyOperation: dispatchReplyOperation } : {}),
    };
  };

  const completeDispatchReplyOperation = () => {
    void releasePreDispatchLifecycleAdmission(() => waitForReplyDispatcherIdle(params.dispatcher));
    const operation = dispatchReplyOperation;
    if (!operation) {
      return;
    }
    const timeoutPolicy = params.dispatcher.resolveFollowupAdmissionBarrierTimeoutPolicy?.();
    const complete = () =>
      operation.completeWithAfterClearBarrier(waitForDispatchDelivery(), timeoutPolicy);
    // Abort races the resolver, not its bookkeeping. Retain this exact owner
    // until that work exits; delivery must remain after-clear to avoid queue cycles.
    if (dispatchLifecycleWork.owner.size > 0) {
      void Promise.allSettled(Array.from(dispatchLifecycleWork.owner)).then(complete);
    } else {
      complete();
    }
  };

  const failDispatchReplyOperation = (error: unknown, terminalOutcome?: "failed") => {
    if (terminalOutcome === "failed") {
      agentRunTerminalOutcome = "failed";
    }
    dispatchReplyOperation?.freezeAbort();
    if (dispatchReplyOperation && !dispatchReplyOperation.result) {
      dispatchReplyOperation.fail("run_failed", error);
    }
    completeDispatchReplyOperation();
  };

  const isDispatchOperationAborted = () => getDispatchAbortSignal()?.aborted === true;
  const isPreDispatchOperationAborted = () => getPreDispatchAbortSignal()?.aborted === true;
  const throwIfDispatchOperationAborted = () => {
    if (isDispatchOperationAborted()) {
      throw new DispatchReplyOperationAbortedError();
    }
  };

  const turnLedger = createReplyTurnLedger(params.dispatcher);
  return {
    completeDispatchReplyOperation,
    // Hook-queued payloads must settle through the turn ledger too, or a
    // hook-delivered visible reply could trigger the no-visible-reply fallback.
    dispatchHookDispatcher: createAbortAwareDispatcher({
      dispatcher: {
        ...params.dispatcher,
        sendToolResult: (payload) => turnLedger.sendQueued("tool", payload).queued,
        sendBlockReply: (payload) => turnLedger.sendQueued("block", payload).queued,
        sendFinalReply: (payload) => turnLedger.sendQueued("final", payload).queued,
        ...(params.dispatcher.sendPreparedReply
          ? {
              sendPreparedReply: (kind, plan) => turnLedger.sendPreparedQueued(kind, plan).queued,
            }
          : {}),
      },
      isAborted: isPreDispatchOperationAborted,
    }),
    turnLedger,
    ensureDispatchReplyOperation,
    failDispatchReplyOperation,
    getAgentRunId: () => agentRunId,
    getAgentRunTerminalOutcome: () => agentRunTerminalOutcome,
    getDispatchAbortOperation: () => dispatchAbortOperation,
    getDispatchAbortSignal,
    getDispatchReplyOperation: () => dispatchReplyOperation,
    getReplyOptions,
    getObservedReplyDelivery: () => observedReplyDelivery,
    getPreDispatchAbortSignal,
    isDispatchOperationAborted,
    isPreDispatchOperationAborted,
    markObservedReplyDelivery,
    releasePreDispatchLifecycleAdmission,
    runWithDispatchLifecycleAdmission,
    throwIfDispatchOperationAborted,
    trackDispatchLifecycleWork,
  };
}
