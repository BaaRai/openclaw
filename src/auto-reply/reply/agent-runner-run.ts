import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { resolveDefaultAgentId } from "../../agents/agent-scope-config.js";
import { resolveReplyCompletion } from "../../agents/reply-completion.js";
import { readChannelContextGatewayContextResolver } from "../../channels/message-access/admission-evidence.js";
import { settleProgressVisibilityCallbackResult } from "../../channels/progress-visibility.js";
import { resolveRestartRecoverySteeringBlockReason } from "../../config/sessions/restart-recovery-receipt.js";
import { hasRestartRecoverySourceClaim } from "../../config/sessions/restart-recovery-state.js";
import { updateSessionEntry } from "../../config/sessions/session-accessor.js";
import { readSessionEntryInWorker } from "../../config/sessions/session-entry-read-runtime.js";
import { logVerbose } from "../../globals.js";
import {
  assertAgentRunLifecycleGenerationCurrent,
  getAgentEventLifecycleGeneration,
} from "../../infra/agent-events.js";
import { measureDiagnosticsTimelineSpan } from "../../infra/diagnostics-timeline.js";
import { hasOutboundReplyContent } from "../../plugin-sdk/reply-payload.js";
import {
  getGatewayContextResolver,
  getPluginRuntimeGatewayRequestScope,
} from "../../plugins/runtime/gateway-request-scope.js";
import {
  submitSessionControllerInput,
  claimSessionControllerInput,
  tryClaimSessionControllerTask,
  releaseSessionControllerClaim,
  retireSessionControllerInput,
} from "../../sessions/session-controller.mailbox.js";
import { markReplyPayloadForSourceSuppressionDelivery } from "../reply-payload.js";
import type { OriginatingChannelType } from "../templating.js";
import type { ReplyPayload } from "../types.js";
import {
  BLOCK_REPLY_SEND_TIMEOUT_MS,
  cleanupReplyAgentRun,
  handleReplyAgentRunError,
  type RunReplyAgentParams,
  scheduleFollowupDrainAfterReplyOperationClear,
} from "./agent-runner-core.js";
import {
  continueStalledReplyTurn,
  createReplyAgentRestartRecoveryController,
  executePreparedReplyAgentRun,
} from "./agent-runner-execute.js";
import { resolveReplySteeringAuthority } from "./agent-runner-fallback-authority.js";
import { createShouldEmitToolOutput, createShouldEmitToolResult } from "./agent-runner-helpers.js";
import { runReplyQuestionInput } from "./agent-runner-question-input.js";
import { runActiveReplySteer } from "./agent-runner-steer-adoption.js";
import { prepareReplyStreamingDelivery } from "./agent-runner-streaming-delivery.js";
import { createFollowupRunner } from "./followup-runner.js";
import { REPLY_RUN_STILL_SHUTTING_DOWN_TEXT } from "./get-reply-run-queue.js";
import { resolveOriginMessageProvider } from "./origin-routing.js";
import { resolveActiveRunQueueAction } from "./queue-policy.js";
import {
  enqueueFollowupRun,
  scheduleFollowupDrain,
  completeFollowupRunLifecycle,
} from "./queue.js";
import { resolveFollowupAbortSignal } from "./queue/types.js";
import { REPLY_ADMISSION_TICKET } from "./reply-admission-ticket.js";
import { prepareReplyAgentTurn } from "./reply-agent-turn-preparation.js";
import { createReplyMediaContext } from "./reply-media-paths.js";
import * as replyRunState from "./reply-operation-run-state.js";
import { bindReplyOperationTyping } from "./reply-run-typing.js";
import { bindReplySourceToFollowup, retireUnadoptedReplySource } from "./reply-source-binding.js";
import { createReplyToModeFilterForChannel, resolveReplyToMode } from "./reply-threading.js";
import { prepareReplyToolAuthority } from "./reply-tool-authority.js";
import { resolveReplyTurnKind } from "./reply-turn-admission.js";
import { createReplyTurnRotationEvidence } from "./reply-turn-rotation.js";
import {
  isDuplicateRestartRecoverySource,
  retireTerminalRestartRecoverySourceClaim,
} from "./restart-recovery-claim.js";
import { resolveRoutedDeliveryThreadId } from "./routed-delivery-thread.js";
import { resolveSourceReplyExpectation } from "./source-reply-delivery-mode.js";
import { readChannelSourceTurnId } from "./source-turn-id.js";
import { createTypingSignaler } from "./typing-mode.js";
export async function runReplyAgent(
  input: RunReplyAgentParams,
): Promise<ReplyPayload | ReplyPayload[] | undefined> {
  const params = { ...input };
  const {
    followupRun,
    queueKey,
    resolvedQueue,
    shouldSteer,
    shouldFollowup,
    hasQueuedFollowups = false,
    isActive,
    isRunActive,
    opts,
    typing,
    sessionEntry,
    sessionStore,
    sessionKey,
    runtimePolicySessionKey,
    storePath,
    defaultModel,
    resolvedVerboseLevel,
    toolProgressDetail,
    isNewSession,
    blockStreamingEnabled,
    blockReplyChunking,
    sessionCtx,
    typingMode,
    resetTriggered,
    replyOperation: providedReplyOperation,
  } = params;
  // Physical admission needs the same default identity execution has always
  // resolved; capture it before submitting the source, not after preparation.
  followupRun.run.agentId ??= resolveDefaultAgentId(followupRun.run.config);
  followupRun.turnAdoptionLifecycle ??= opts?.turnAdoptionLifecycle;
  bindReplySourceToFollowup(opts, followupRun);
  const controllerInput = submitSessionControllerInput(
    queueKey,
    followupRun,
    resolvedQueue,
    opts?.runId,
  );
  try {
    followupRun.abortSignal = followupRun.abortSignal
      ? AbortSignal.any([controllerInput.abortSignal, followupRun.abortSignal])
      : controllerInput.abortSignal;
    followupRun.operatorAuthority?.assertCurrent();
    const resolveGatewayContext = providedReplyOperation
      ? getGatewayContextResolver(providedReplyOperation)
      : (readChannelContextGatewayContextResolver(sessionCtx) ??
        getPluginRuntimeGatewayRequestScope()?.resolveGatewayContext);
    // One lifecycle for all adoption sites in this run.
    const turnAdoptionLifecycle = opts?.turnAdoptionLifecycle;
    const releaseAdmissionTicket = () => opts?.[REPLY_ADMISSION_TICKET]?.release();
    let activeSessionEntry = sessionEntry;
    let activeSessionStore = sessionStore;
    const effectiveResetTriggered = resetTriggered === true;

    const isHeartbeat = opts?.isHeartbeat === true;
    const replyExpectation = (followupRun.run.terminalReplyExpectation ??=
      resolveSourceReplyExpectation({
        ctx: {
          ...sessionCtx,
          InboundEventKind: followupRun.currentInboundEventKind ?? sessionCtx.InboundEventKind,
          InputProvenance: followupRun.run.inputProvenance ?? sessionCtx.InputProvenance,
        },
        cfg: followupRun.run.config,
        isHeartbeat,
      }));
    let didDeliverVisiblePartialReply = false;
    const onPartialReply = opts?.onPartialReply;
    const runOpts = onPartialReply
      ? {
          ...opts,
          onPartialReply: async (
            payload: Parameters<NonNullable<typeof opts.onPartialReply>>[0],
          ) => {
            const operation = controllerInput.claim?.operation;
            const observed = await settleProgressVisibilityCallbackResult(onPartialReply(payload));
            if (observed.visible && hasOutboundReplyContent(payload, { trimText: true })) {
              didDeliverVisiblePartialReply = true;
              operation?.watchdog.progress("finalization", "reply:partial_delivered");
            }
            return observed.result;
          },
        }
      : opts;
    const replyOperationRunState = replyRunState.resolveReplyOperationRunState(opts);
    if (replyOperationRunState && !isHeartbeat && replyExpectation === "required") {
      // Dispatch may observe a preflight stall before controller admission finishes.
      // Until this run owns the queue facts, the continuation must decline explicitly.
      replyOperationRunState.continueStalledTurn = () => false;
    }
    if (replyOperationRunState) {
      replyOperationRunState.replyCompletion = resolveReplyCompletion(
        followupRun.run.terminalReplyExpectation,
        "empty",
      );
    }
    followupRun.replyOperationRunStates = replyOperationRunState
      ? [replyOperationRunState]
      : undefined;
    const traceAttributes = {
      provider: followupRun.run.provider,
      hasSessionKey: Boolean(sessionKey ?? followupRun.run.sessionKey),
      isHeartbeat,
      queueMode: resolvedQueue.mode,
      isActive,
      blockStreamingEnabled,
    };
    const traceAgentPhase = <T>(name: string, run: () => Promise<T> | T): Promise<T> =>
      measureDiagnosticsTimelineSpan(name, run, {
        phase: "agent-turn",
        config: followupRun.run.config,
        attributes: traceAttributes,
      });
    const readGeneration = getAgentEventLifecycleGeneration();
    const assertReadCurrent = () => {
      assertAgentRunLifecycleGenerationCurrent(readGeneration);
      followupRun.operatorAuthority?.assertCurrent();
    };
    const restartRecoverySourceTurnId = readChannelSourceTurnId(sessionCtx);
    let restartRecoveryEntry: typeof activeSessionEntry;
    try {
      restartRecoveryEntry =
        sessionKey && storePath
          ? ((await readSessionEntryInWorker(
              { agentId: followupRun.run.agentId, storePath, sessionKey },
              assertReadCurrent,
            )) ?? activeSessionEntry)
          : activeSessionEntry;
      assertReadCurrent();
    } catch (error) {
      releaseAdmissionTicket();
      typing.cleanup();
      retireSessionControllerInput(controllerInput);
      throw error;
    }
    if (
      restartRecoverySourceTurnId &&
      isDuplicateRestartRecoverySource(restartRecoveryEntry, restartRecoverySourceTurnId)
    ) {
      // Durable source ownership identifies provider redelivery even if the run
      // became terminal before its claim cleanup committed.
      if (
        restartRecoveryEntry?.status !== "running" &&
        sessionKey &&
        storePath &&
        hasRestartRecoverySourceClaim(restartRecoveryEntry, restartRecoverySourceTurnId)
      ) {
        const retired = await retireTerminalRestartRecoverySourceClaim({
          agentId: followupRun.run.agentId,
          sessionId: restartRecoveryEntry.sessionId,
          sessionKey,
          sourceTurnId: restartRecoverySourceTurnId,
          storePath,
        });
        if (retired) {
          activeSessionEntry = retired;
          if (activeSessionStore) {
            activeSessionStore[sessionKey] = retired;
          }
        }
      }
      releaseAdmissionTicket();
      typing.cleanup();
      retireSessionControllerInput(controllerInput);
      return undefined;
    }

    const effectiveShouldSteer = !isHeartbeat && !effectiveResetTriggered && shouldSteer;
    const effectiveShouldFollowup = !effectiveResetTriggered && shouldFollowup;
    const messageInjectionDisposition = opts?.messageInjectionDisposition ?? "none";
    const activeReplyOperation = controllerInput.mailbox.owner.active ?? providedReplyOperation;
    const steeringAuthority = resolveReplySteeringAuthority(followupRun, activeReplyOperation);
    const shouldQueueAuthorityMismatch =
      effectiveShouldSteer && isActive && steeringAuthority.shouldQueueAuthorityMismatch;
    if (shouldQueueAuthorityMismatch) {
      logVerbose(
        `queue: active session ${activeReplyOperation?.sessionId ?? followupRun.run.sessionId} has different or unknown tool authority; queuing instead of steering`,
      );
    }
    const typingSignals = createTypingSignaler({
      typing,
      mode: typingMode,
      isHeartbeat,
    });
    // New steering must not reuse a terminal source claim. Compare the active
    // source identity so unrelated retained tombstones still permit steering.
    // The parked admission owner rechecks after any predecessor wait.
    const activeSourceTurnId =
      controllerInput.mailbox.owner.sourceTurnId ??
      normalizeOptionalString(restartRecoveryEntry?.restartRecoveryDeliverySourceRunId) ??
      "";
    const terminalDeliveryBlockReason = resolveRestartRecoverySteeringBlockReason(
      restartRecoveryEntry,
      activeReplyOperation?.sessionId ?? followupRun.run.sessionId,
      activeSourceTurnId,
    );
    const shouldQueueTerminalReceiptSteer =
      effectiveShouldSteer &&
      isActive &&
      !shouldQueueAuthorityMismatch &&
      messageInjectionDisposition === "none" &&
      terminalDeliveryBlockReason !== undefined;
    if (shouldQueueTerminalReceiptSteer) {
      logVerbose(
        `queue: active session ${activeReplyOperation?.sessionId ?? followupRun.run.sessionId} cannot accept steering (${terminalDeliveryBlockReason}); queuing instead`,
      );
    }

    const questionInput = await runReplyQuestionInput(input);
    if (questionInput.handled) {
      releaseAdmissionTicket();
      typing.cleanup();
      retireSessionControllerInput(controllerInput);
      return questionInput.payload;
    }

    const baseShouldEmitToolResult = createShouldEmitToolResult({
      sessionKey,
      storePath,
      resolvedVerboseLevel,
      verboseLevelOverride: followupRun.run.verboseLevelOverride,
    });
    const channelProgressCanConsumeToolResults =
      Boolean(opts?.forceToolResultProgress) && Boolean(opts?.onToolResult);
    const shouldEmitToolResult = () =>
      channelProgressCanConsumeToolResults || baseShouldEmitToolResult();
    const shouldEmitToolOutput = createShouldEmitToolOutput({
      sessionKey,
      storePath,
      resolvedVerboseLevel,
      verboseLevelOverride: followupRun.run.verboseLevelOverride,
    });

    const pendingToolTasks = new Set<Promise<void>>();
    const blockReplyTimeoutMs = opts?.blockReplyTimeoutMs ?? BLOCK_REPLY_SEND_TIMEOUT_MS;
    const touchActiveSessionEntry = async () => {
      if (!activeSessionEntry || !activeSessionStore || !sessionKey) {
        return;
      }
      // Keep the in-memory snapshot aligned with the pending-reset write boundary.
      const updatedAt = activeSessionEntry.updatedAt === 0 ? 0 : Date.now();
      activeSessionEntry.updatedAt = updatedAt;
      activeSessionStore[sessionKey] = activeSessionEntry;
      if (storePath) {
        await updateSessionEntry(
          { agentId: followupRun.run.agentId, storePath, sessionKey },
          () => ({ updatedAt }),
          { skipMaintenance: true, takeCacheOwnership: true },
        );
      }
    };

    const queuedRunFollowupTurn = createFollowupRunner({
      resolveGatewayContext,
      opts,
      typing,
      typingMode,
      sessionEntry: activeSessionEntry,
      sessionStore: activeSessionStore,
      sessionKey,
      storePath,
      defaultModel,
      toolProgressDetail,
    });

    if (messageInjectionDisposition === "accepted") {
      if (replyOperationRunState) {
        replyOperationRunState.admission = { status: "accepted", mode: "steer" };
      }
      releaseAdmissionTicket();
      typing.cleanup();
      retireSessionControllerInput(controllerInput);
      return undefined;
    }

    const bindQueueDisposition = () => {
      const observe = followupRun.onQueueDisposition;
      followupRun.onQueueDisposition = (disposition) => {
        observe?.(disposition);
        if (
          replyOperationRunState &&
          (disposition !== "queue-cap-old" ||
            replyOperationRunState.admission?.status !== "accepted")
        ) {
          replyOperationRunState.admission = { status: "skipped", reason: "queue-cap" };
        }
      };
    };

    // Steering skips the question creator's caller policy, so refused input queues.
    if (
      !questionInput.refusedNotice &&
      effectiveShouldSteer &&
      isActive &&
      !shouldQueueAuthorityMismatch &&
      !shouldQueueTerminalReceiptSteer &&
      messageInjectionDisposition === "none"
    ) {
      bindQueueDisposition();
      const result = await runActiveReplySteer({
        followupRun,
        opts,
        providedReplyOperation: activeReplyOperation,
        queueKey,
        releaseAdmissionTicket,
        replyOperationRunState,
        resolvedQueue,
        restartRecoverySourceTurnId,
        runFollowup: queuedRunFollowupTurn,
        sessionCtx,
        sessionKey,
        sessionEntry: activeSessionEntry,
        storePath,
        touchActiveSessionEntry,
        typing,
        typingSignals,
        toolAuthorityFingerprint: steeringAuthority.toolAuthorityFingerprint,
        automaticFallbackRoute: steeringAuthority.automaticFallbackRoute,
        pendingInputAuthorityFingerprint: steeringAuthority.pendingInputAuthorityFingerprint,
      });
      return result === "handled" ? undefined : result;
    }

    const activeRunQueueAction = questionInput.refusedNotice
      ? "enqueue-followup"
      : resolveActiveRunQueueAction({
          hasQueuedFollowups,
          interrupt: resolvedQueue.mode === "interrupt",
          isActive,
          isHeartbeat,
          shouldFollowup: effectiveShouldFollowup || shouldQueueAuthorityMismatch,
          resetTriggered: effectiveResetTriggered,
        });
    if (activeRunQueueAction === "drop") {
      retireSessionControllerInput(controllerInput);
      if (replyOperationRunState) {
        replyOperationRunState.admission = { status: "skipped", reason: "active-run" };
      }
      releaseAdmissionTicket();
      typing.cleanup();
      return undefined;
    }

    if (activeRunQueueAction === "enqueue-followup") {
      bindQueueDisposition();
      const enqueued = enqueueFollowupRun(
        queueKey,
        followupRun,
        resolvedQueue,
        "message-id",
        queuedRunFollowupTurn,
        false,
      );
      if (!enqueued) {
        releaseAdmissionTicket();
        typing.cleanup();
        retireSessionControllerInput(controllerInput);
        return undefined;
      }
      if (replyOperationRunState) {
        replyOperationRunState.admission = { status: "accepted", mode: "followup" };
      }
      // The queue must stay dormant while the active owner can still collect
      // messages. Registering after enqueue closes the owner-clear race.
      const queuedOperationOwner = controllerInput.mailbox.owner.active ?? activeReplyOperation;
      if (queuedOperationOwner) {
        scheduleFollowupDrainAfterReplyOperationClear({
          operation: queuedOperationOwner,
          queueKey,
          runFollowup: queuedRunFollowupTurn,
        });
      } else {
        scheduleFollowupDrain(queueKey, queuedRunFollowupTurn);
      }
      releaseAdmissionTicket();
      const queuedBehindActiveRun = isRunActive?.() === true;
      await touchActiveSessionEntry();
      if (queuedBehindActiveRun) {
        await typingSignals.signalToolStart();
      } else {
        typing.cleanup();
      }
      return questionInput.refusedNotice;
    }

    const replySessionKey = sessionKey ?? followupRun.run.sessionKey;
    const replyRouteThreadId = resolveRoutedDeliveryThreadId({
      ctx: sessionCtx,
      sessionKey: replySessionKey,
    });
    const replyTurnKind = resolveReplyTurnKind(opts);
    const rotationEvidence = providedReplyOperation
      ? undefined
      : createReplyTurnRotationEvidence({
          sessionKey: replySessionKey ?? "",
          controller: controllerInput.mailbox.owner,
        });
    let directCompactionNotice:
      | ((phase: import("./compaction-notice.js").CompactionNoticePhase) => Promise<void>)
      | undefined;
    const preparation = await prepareReplyAgentTurn({
      queued: followupRun,
      defaults: {
        resolveGatewayContext,
        opts: runOpts,
        typing,
        typingMode,
        sessionEntry: activeSessionEntry,
        sessionStore: activeSessionStore,
        sessionKey,
        storePath,
        defaultModel,
        toolProgressDetail,
      },
      kind: replyTurnKind,
      resetTriggered: effectiveResetTriggered,
      routeThreadId: replyRouteThreadId,
      providedReplyOperation,
      rotationEvidence,
      claimSource: providedReplyOperation
        ? undefined
        : async () => {
            const observation = rotationEvidence?.observeAdmission();
            try {
              return await (isHeartbeat
                ? tryClaimSessionControllerTask(controllerInput, "heartbeat")
                : claimSessionControllerInput(followupRun));
            } catch (error) {
              if (!resolveFollowupAbortSignal(followupRun)?.aborted) {
                throw error;
              }
              return undefined;
            } finally {
              observation?.dispose();
            }
          },
      onBeforeClaimWait: releaseAdmissionTicket,
      configure: (cfg) => {
        const replyToChannel = resolveOriginMessageProvider({
          originatingChannel: sessionCtx.OriginatingChannel,
          provider: sessionCtx.Surface ?? sessionCtx.Provider,
        }) as OriginatingChannelType | undefined;
        const replyToMode =
          followupRun.originatingReplyToMode ??
          resolveReplyToMode(cfg, replyToChannel, sessionCtx.AccountId, sessionCtx.ChatType);
        const applyReplyToMode = createReplyToModeFilterForChannel(replyToMode, replyToChannel);
        const replyMediaContext = createReplyMediaContext({
          cfg,
          agentId: followupRun.run.agentId,
          sessionKey,
          workspaceDir: followupRun.run.workspaceDir,
          mediaNormalizationOwner: followupRun.run.mediaNormalizationOwner,
          messageProvider: followupRun.run.messageProvider,
          accountId: followupRun.originatingAccountId ?? followupRun.run.agentAccountId,
          groupId: followupRun.run.groupId,
          groupChannel: followupRun.run.groupChannel,
          groupSpace: followupRun.run.groupSpace,
          requesterSenderId: followupRun.run.senderId,
          requesterSenderName: followupRun.run.senderName,
          requesterSenderUsername: followupRun.run.senderUsername,
          requesterSenderE164: followupRun.run.senderE164,
        });
        const streaming = prepareReplyStreamingDelivery({
          opts,
          sessionCtx,
          cfg,
          applyReplyToMode,
          blockStreamingEnabled,
          blockReplyChunking,
          blockReplyTimeoutMs,
        });
        directCompactionNotice = streaming.sendDirectCompactionNotice;
        return {
          applyReplyToMode,
          cfg,
          replyMediaContext,
          replyToChannel,
          replyToMode,
          ...streaming,
        };
      },
      signalRunStart: typingSignals.signalRunStart,
      trace: traceAgentPhase,
      onCompactionNotice: async (phase) => await directCompactionNotice?.(phase),
    });
    if (preparation.kind === "skipped") {
      preparation.operation?.complete();
      if (replyOperationRunState) {
        replyOperationRunState.admission = {
          status: "skipped",
          reason: preparation.reason === "active-run" ? "active-run" : "aborted",
        };
      }
      typing.cleanup();
      if (preparation.reason !== "active-run" || replyTurnKind !== "visible") {
        return undefined;
      }
      return markReplyPayloadForSourceSuppressionDelivery({
        text: REPLY_RUN_STILL_SHUTTING_DOWN_TEXT,
      });
    }
    if (replyOperationRunState) {
      replyOperationRunState.admission = { status: "owned" };
    }
    const turn = preparation.turn;
    Object.assign(followupRun, turn.queued);
    activeSessionEntry = turn.session.current();
    activeSessionStore = turn.sessionStore;
    const replyOperation = turn.operation;
    const {
      applyReplyToMode,
      blockReplyPipeline,
      cfg,
      replyMediaContext,
      replyToChannel,
      replyToMode,
    } = preparation.configured;
    const resolveVisibleReplyDelivery = async () => {
      try {
        await blockReplyPipeline?.flush({ force: true });
      } catch (flushError) {
        logVerbose(
          `failed to flush streamed reply blocks before surfacing run failure: ${String(flushError)}`,
        );
      }
      return didDeliverVisiblePartialReply || blockReplyPipeline?.didStream() === true;
    };
    let runFollowupTurn = queuedRunFollowupTurn;
    let shouldDrainQueuedFollowupsAfterClear = false;
    const returnWithQueuedFollowupDrain = <T>(value: T): T => {
      shouldDrainQueuedFollowupsAfterClear = true;
      return value;
    };
    let restartRecovery: ReturnType<typeof createReplyAgentRestartRecoveryController> | undefined;
    try {
      replyOperation.bindToolAuthoritySnapshot(prepareReplyToolAuthority(followupRun));
      bindReplyOperationTyping(replyOperation, typing);
      if (replyOperationRunState && !isHeartbeat && replyExpectation === "required") {
        // Dispatch owns the stall notice; this owner holds the queue facts needed to answer
        // instead. The same sender's next queued request inherits the guidance; otherwise one
        // recovery run bound to this turn's route and authority is queued.
        replyOperationRunState.continueStalledTurn = () =>
          continueStalledReplyTurn({
            followupRun,
            queueKey,
            resolvedQueue,
            replyOperation,
            runFollowupTurn,
          });
      }
      restartRecovery = createReplyAgentRestartRecoveryController({
        activeSessionStore,
        cfg,
        followupRun,
        getActiveSessionEntry: () => activeSessionEntry,
        opts,
        replyOperation,
        restartRecoverySourceTurnId,
        runtimePolicySessionKey,
        sessionCtx,
        sessionKey,
        setActiveSessionEntry: (entry) => {
          activeSessionEntry = entry;
        },
        storePath,
      });
      if (turn.preflightError) {
        throw turn.preflightError;
      }
      return await executePreparedReplyAgentRun({
        ...params,
        activeSessionStore,
        admitUserTurn: restartRecovery.admitUserTurn,
        applyReplyToMode,
        beginBeforeAgentReply: restartRecovery.beginBeforeAgentReply,
        blockReplyPipeline,
        cfg,
        checkpointBeforeAgentReply: restartRecovery.checkpointBeforeAgentReply,
        resolveVisibleReplyDelivery,
        activeIsNewSession: isNewSession,
        getActiveSessionEntry: () => activeSessionEntry,
        isHeartbeat,
        isRestartRecoveryArmed: restartRecovery.isArmed,
        opts: runOpts,
        pendingToolTasks,
        replyMediaContext,
        replyOperation,
        replyRouteThreadId,
        replyToChannel,
        replyToMode,
        preflightCompactionApplied: turn.preflightCompactionApplied,
        returnWithQueuedFollowupDrain,
        runFollowupTurn,
        setRunFollowupTurn: (runner) => {
          runFollowupTurn = runner;
        },
        shouldEmitToolOutput,
        shouldEmitToolResult,
        traceAgentPhase,
        turnAdoptionLifecycle,
        typingSignals,
      });
    } catch (error) {
      replyRunState.recordReplyOperationAgentTurn(
        followupRun.replyOperationRunStates,
        replyOperation,
      );
      return await handleReplyAgentRunError(error, {
        resolveVisibleReplyDelivery,
        isHeartbeat,
        replyExpectation,
        isRestartRecoveryArmed: restartRecovery?.isArmed ?? (async () => false),
        replyOperation,
        resolvedVerboseLevel,
        returnWithQueuedFollowupDrain,
        sessionCtx,
      });
    } finally {
      await cleanupReplyAgentRun({
        blockReplyPipeline,
        clearRestartRecoveryDeliveryClaim: restartRecovery?.clear ?? (async () => {}),
        providedReplyOperation,
        queueKey,
        replyOperation,
        runFollowupTurn,
        sessionKey,
        shouldDrainQueuedFollowupsAfterClear,
        typing,
      });
      completeFollowupRunLifecycle(followupRun);
      if (controllerInput.claim) {
        releaseSessionControllerClaim(controllerInput.claim);
      }
    }
  } finally {
    retireUnadoptedReplySource(controllerInput);
  }
}
