import { randomUUID } from "node:crypto";
import { isFutureDateTimestampMs } from "@openclaw/normalization-core/number-coercion";
import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { readAcpSessionMetaForEntries } from "../../acp/runtime/session-meta-readonly.js";
import {
  createAgentRunRestartAbortError,
  isAgentRunDirectAbortReason,
} from "../../agents/run-termination.js";
import { resolveQueueSettings } from "../../auto-reply/reply/queue/settings-runtime.js";
import { resolveSessionWorkStartError } from "../../config/sessions.js";
import { hasRestartRecoveryTerminalRun } from "../../config/sessions/restart-recovery-state.js";
import type { SessionEntry } from "../../config/sessions/types.js";
import { getAgentEventLifecycleGeneration } from "../../infra/agent-events.js";
import { claimAgentRunContext, clearAgentRunContext } from "../../infra/agent-run-registry.js";
import {
  isProgressCardRefreshInputProvenance,
  progressCardRefreshRunProjection,
} from "../../sessions/input-provenance.js";
import { resolveActiveReplyRunOwnerForSignal } from "../../sessions/session-controller.barriers.js";
import {
  type ReplyMessageInjectionTarget,
  type ReplyOperation,
  captureCurrentSessionRunInterruptTarget,
} from "../../sessions/session-controller.js";
import {
  beginSessionEffect,
  captureSessionTarget,
} from "../../sessions/session-controller.lifecycle.js";
import { captureCurrentReplyMessageInjectionTarget } from "../../sessions/session-controller.message-injection.js";
import {
  hasRpcSourceForController,
  isRpcSourceRegistered,
} from "../../sessions/session-controller.rpc-sources.js";
import { registerChatAbortController } from "../chat-abort.js";
import type { DedupeEntry } from "../server-shared.js";
import {
  isRetryableUnadoptedChatClaim,
  resolveRestartSafeChatAdmission,
  withRestartSafeChatPlacement,
  type PreparedRestartSafeChatPlacement,
} from "./chat-restart-recovery.js";
import { assertExpectedLeafActive } from "./chat-send-active-leaf.js";
import {
  createAdmittedChatSendCleanup,
  finishAbortedChatSend as publishAbortedChatSend,
  prepareAdmittedChatSendDispatch,
} from "./chat-send-admission-cleanup.js";
import { prepareChatSendAdmissionContext } from "./chat-send-admission-context.js";
import { prepareGoalChatSendRetry } from "./chat-send-goal-retry.js";
import {
  consumeChatSendCurrent,
  resolveChatSendRequestConflict,
  respondChatSessionRoutingChanged,
} from "./chat-send-pre-admission.js";
import type { ChatSendPreAdmissionParams } from "./chat-send-pre-admission.types.js";
import {
  createPendingChatSendReservationAccess,
  inspectGoalChatSendRetry,
  readChatSendDedupeResponse,
} from "./chat-send-reservation.js";
import { bindChatSendPreparedSession } from "./chat-send-session-binding.js";
import { captureAdmittedChatSendSessionSettings } from "./chat-send-session-settings.js";
import { withCurrentChatSendSession, prepareChatSendSessionEntry } from "./chat-send-session.js";
import type { PreparedChatSendSession } from "./chat-send-session.js";
import {
  admitChatSendUploads,
  assertChatSendExclusiveAdmission,
  consumeChatSendAdmissionRetry,
  createChatSendWorkAdmission,
  prepareChatSendAdmissionRetry,
  prepareChatSendInterruptAdmission,
  releaseChatSendCallerAuthority,
  respondChatSendWorkAdmissionFailure,
  withCurrentChatSendRetry,
} from "./chat-send-work-admission.js";
import type { GatewayRequestHandlerOptions, SessionMutationAuthorization } from "./types.js";

/** Reserve the session lifecycle and register the abortable run before attachment work. */
export async function admitChatSend(
  params: ChatSendPreAdmissionParams & {
    session: PreparedChatSendSession;
    withPreparedCurrent?: SessionMutationAuthorization["withPreparedCurrent"];
    hasCurrentClientAuthority?: GatewayRequestHandlerOptions["hasCurrentClientAuthority"];
    onAdmissionOwned?: () => Promise<boolean>;
  },
) {
  params.assertCurrent?.();
  const { request, session, respond, context, client } = params;
  const { p, turnKind } = request;
  const requestIdentity = request.goalOperation?.requestFingerprint ?? request.requestIdentity;
  const progressRefresh = isProgressCardRefreshInputProvenance(request.systemInputProvenance);
  const {
    rawSessionKey,
    clientRunId,
    pendingChatSendKey,
    cfg,
    storePath,
    entry,
    sessionKey,
    selectedAgent,
    requestedSessionId,
    backingSessionId,
    agentId,
    resolvedSessionModel,
    resolvedSessionAuthProvider,
    activeRunScopeKey,
    timeoutMs,
    now,
    restartSafeRequest,
    expectedLeafEntryId,
  } = session;
  const assertSessionTargetCurrent = session.assertSessionTargetCurrent;
  const { chatSendTraceAttributes, originatingRoute } = prepareChatSendAdmissionContext({
    request,
    session,
    client,
  });
  const lifecycleGeneration = getAgentEventLifecycleGeneration();
  const pendingAttemptId = randomUUID();
  const reservation = createPendingChatSendReservationAccess({
    context,
    client,
    key: pendingChatSendKey,
    runId: clientRunId,
    attemptId: pendingAttemptId,
    requestIdentity,
    request,
    session,
  });
  const preparedGoalRetry = request.goalOperation
    ? await prepareGoalChatSendRetry(params)
    : undefined;
  const pendingRetry = prepareChatSendAdmissionRetry(params);
  const preparedRetry = pendingRetry instanceof Promise ? await pendingRetry : pendingRetry;
  const clearReservationAndThrow = (error: unknown): never => {
    reservation.clear();
    throw error;
  };
  const reserved = await consumeChatSendCurrent(params, () => {
    params.assertCurrent?.();
    assertSessionTargetCurrent();
    const goalRetry = inspectGoalChatSendRetry({ ...params, prepared: preparedGoalRetry });
    if (goalRetry.kind !== "new") {
      if (goalRetry.kind === "replay") {
        respond(true, { ...goalRetry.receipt, replayed: true }, undefined, {
          cached: true,
          runId: clientRunId,
        });
      }
      return undefined;
    }
    const retryComparison = consumeChatSendAdmissionRetry(params, preparedRetry);
    if (retryComparison === false) {
      return undefined;
    }
    const uploadAdmission = admitChatSendUploads({ params: p, client, context, respond });
    if (!uploadAdmission.ok) {
      return undefined;
    }
    // Keeps the run abortable; admission rejects it once expired or missing, never revives it.
    reservation.reserve();
    return { retryComparison, uploadAdmission };
  }).catch(clearReservationAndThrow);
  if (!reserved) {
    return { ok: false as const };
  }
  let retryComparison = reserved.retryComparison;
  const uploadAdmission = reserved.uploadAdmission;
  const preparedGoalEntry =
    request.goalOperation?.action === "start" && !entry && !requestedSessionId
      ? await prepareChatSendSessionEntry({
          cfg: session.cfg,
          client,
          agentId,
          getRuntimeConfig: context.getRuntimeConfig,
        }).catch(clearReservationAndThrow)
      : undefined;
  let admittedSessionId = preparedGoalEntry?.entry.sessionId ?? backingSessionId ?? clientRunId;
  let expectedActiveReplyOperation: ReplyOperation | undefined;
  let gatewayWorkAdmission: Awaited<ReturnType<typeof beginSessionEffect>> | undefined;
  let restartSafeAdmission: ReturnType<typeof resolveRestartSafeChatAdmission>;
  let initialSessionEntry: SessionEntry | undefined;
  let admittedSessionEntry: SessionEntry | undefined;
  let admittedSessionSettings: ReturnType<typeof captureAdmittedChatSendSessionSettings>;
  let assertInitialSkillSelection: (() => void) | undefined;
  let messageInjectionTarget: ReplyMessageInjectionTarget | undefined;
  let reservationSuperseded = false;
  let supersedingResult: DedupeEntry | undefined;
  let assertSourceAuthority: (() => void) | undefined = params.assertCurrent;
  const sessionTarget = captureSessionTarget({
    storeScope: storePath,
    sessionKey,
    aliases: [rawSessionKey, session.sessionTarget.storeKey],
    agentId,
    incarnation: admittedSessionId,
  });
  const admittedRunAbort = registerChatAbortController({
    target: sessionTarget,
    policy: resolveQueueSettings({
      cfg,
      channel: originatingRoute.originatingChannel,
      sessionEntry: entry,
      inlineMode: p.queueMode,
    }),
    authority: { assertCurrent: () => assertSourceAuthority?.() },
    runId: clientRunId,
    sessionId: admittedSessionId,
    sessionKey,
    agentId: selectedAgent.agentId,
    timeoutMs,
    now,
    ownerConnId: normalizeOptionalString(client?.connId),
    ownerDeviceId: normalizeOptionalString(client?.connect?.device?.id),
    providerId: resolvedSessionModel.provider,
    authProviderId: resolvedSessionAuthProvider,
    resolveTerminalProducer: (active) =>
      resolveActiveReplyRunOwnerForSignal(active.input.abortSignal),
    kind: "chat-send",
    turnKind,
    ...(progressRefresh ? { controlUiVisible: false, projectSessionActive: false } : {}),
    lifecycleGeneration,
  });
  const runInterruptTarget =
    admittedRunAbort.entry?.input.policy.mode === "interrupt"
      ? captureCurrentSessionRunInterruptTarget(admittedRunAbort.entry.input.mailbox.owner.id)
      : undefined;
  const placementService = context.workerSessionPlacementService;
  const commitChatWorkAdmission = async (
    acpMeta: SessionEntry["acp"] | null,
    preparedPlacement?: PreparedRestartSafeChatPlacement,
  ): Promise<void> => {
    if (context.workerSessionPlacementService !== placementService) {
      throw new Error("Worker placement owner changed during chat admission; retry.");
    }
    if (placementService && preparedPlacement?.sessionId !== admittedSessionId) {
      return withRestartSafeChatPlacement(placementService, admittedSessionId, (prepared) =>
        commitChatWorkAdmission(acpMeta, prepared),
      );
    }
    let refreshPlacement = false;
    await withCurrentChatSendRetry(params, pendingAttemptId, (latestSession, comparison) => {
      retryComparison = comparison;
      params.assertCurrent?.();
      const retainedRequestConflict = resolveChatSendRequestConflict(
        { ...params, session: { ...session, entry: latestSession.entry } },
        retryComparison,
        pendingAttemptId,
      );
      if (retainedRequestConflict) {
        throw new Error(retainedRequestConflict.message);
      }
      if (context.chatRunState.hasAbortMarker(clientRunId)) {
        return;
      }
      const pendingReservation = reservation.read();
      if (
        pendingReservation &&
        normalizeOptionalString(pendingReservation.payload.attemptId) !== pendingAttemptId
      ) {
        reservationSuperseded = true;
        return;
      }
      if (!pendingReservation) {
        const terminalResult = readChatSendDedupeResponse(context.dedupe, clientRunId);
        const admittedSource = admittedRunAbort.entry;
        if (
          terminalResult ||
          (admittedSource &&
            !isRpcSourceRegistered(admittedSource) &&
            hasRpcSourceForController(clientRunId, admittedSource))
        ) {
          reservationSuperseded = true;
          supersedingResult = terminalResult;
          return;
        }
      }
      if (lifecycleGeneration !== getAgentEventLifecycleGeneration()) {
        if (admittedRunAbort.entry) {
          admittedRunAbort.entry.adapter.abortStopReason = "restart";
        }
        admittedRunAbort.controller.abort(createAgentRunRestartAbortError());
        reservation.abort("restart");
        return;
      }
      if (
        !pendingReservation ||
        !isFutureDateTimestampMs(pendingReservation.payload.expiresAtMs, { nowMs: Date.now() })
      ) {
        if (admittedRunAbort.entry) {
          admittedRunAbort.entry.adapter.abortStopReason = "timeout";
        }
        admittedRunAbort.controller.abort();
        reservation.abort("timeout");
        return;
      }
      const latestEntry = latestSession.entry;
      admittedSessionEntry = latestEntry;
      admittedSessionSettings = captureAdmittedChatSendSessionSettings({
        commit: true,
        entry: latestEntry,
        expectedPermissionMode: p.expectedPermissionMode,
        expectedToolOverrides: p.expectedToolOverrides,
      });
      assertChatSendExclusiveAdmission(request, session);
      if (entry && !latestEntry) {
        throw new Error(`Session "${sessionKey}" was deleted while starting work. Retry.`);
      }
      messageInjectionTarget =
        p.queueMode === "steer"
          ? captureCurrentReplyMessageInjectionTarget(
              admittedRunAbort.entry?.input.mailbox.owner.id ?? activeRunScopeKey,
            )
          : undefined;
      if (p.queueMode !== "steer" && expectedLeafEntryId !== undefined) {
        assertExpectedLeafActive(latestSession, agentId, expectedLeafEntryId, requestedSessionId, {
          allowEmptyAncestor: true,
        });
      }
      if (
        backingSessionId &&
        latestEntry?.sessionId &&
        latestEntry.sessionId !== backingSessionId
      ) {
        throw new Error(`Session "${sessionKey}" changed while starting work. Retry.`);
      }
      const retryableClaim = isRetryableUnadoptedChatClaim(latestEntry, clientRunId);
      if (
        (latestEntry?.restartRecoveryDeliveryRunId &&
          latestEntry.restartRecoveryDeliverySourceRunId === clientRunId &&
          !retryableClaim) ||
        hasRestartRecoveryTerminalRun(latestEntry, clientRunId)
      ) {
        reservationSuperseded = true;
        supersedingResult = {
          ts: Date.now(),
          ok: true,
          payload: { runId: clientRunId, status: "ok" as const },
        };
        return;
      }
      const archivedError = resolveSessionWorkStartError(sessionKey, latestEntry, {
        allowPendingWorkspace: true,
        providerReviewAcknowledgment: request.providerReviewAcknowledgment,
        runId: clientRunId,
      });
      if (archivedError) {
        throw new Error(archivedError);
      }
      admittedSessionId = latestEntry?.sessionId ?? backingSessionId ?? clientRunId;
      expectedActiveReplyOperation = admittedRunAbort.entry?.input.mailbox.owner.active;
      if (request.goalOperation?.action === "start" && !latestEntry && !requestedSessionId) {
        const prepared = preparedGoalEntry!;
        initialSessionEntry = prepared.entry;
        assertInitialSkillSelection = prepared.assertSkillSelection;
        admittedSessionId = initialSessionEntry.sessionId;
      }
      if (context.workerSessionPlacementService !== placementService) {
        throw new Error("Worker placement owner changed during chat admission; retry.");
      }
      if (placementService && preparedPlacement?.sessionId !== admittedSessionId) {
        refreshPlacement = true;
        return;
      }
      // Admission reads only the placement route. An interrupted turn may still be releasing
      // its turn claim, and that cleanup must not fail the message that interrupted it.
      preparedPlacement?.facts.assertCurrent("route");
      restartSafeAdmission = resolveRestartSafeChatAdmission({
        activeRunScopeKey,
        agentId,
        cfg: latestSession.cfg,
        clientRunId,
        context,
        entry: latestEntry,
        initialSessionEntry,
        acpMeta,
        now: Date.now(),
        placement: preparedPlacement?.facts.placement,
        request: restartSafeRequest,
        requestedSessionId,
        sessionId: admittedSessionId,
        sessionKey: latestSession.canonicalKey,
        storePath: latestSession.storePath,
      });
      if (request.goalOperation && !restartSafeAdmission) {
        throw new Error(
          "Goal start or resume requires the built-in OpenClaw runtime and an idle local session with recoverable history. This action is unavailable for native Codex and other external runtimes.",
        );
      }
      if (retryableClaim && !restartSafeAdmission) {
        throw new Error("chat retry does not match its durable admission");
      }
    });
    if (refreshPlacement) {
      return commitChatWorkAdmission(acpMeta);
    }
  };

  let capturedOperator: Awaited<ReturnType<typeof prepareChatSendInterruptAdmission>>["operator"];
  let releaseCapturedOperator = () => {};
  let interruptedActiveRun: boolean;
  let retainedRequestConflict: ReturnType<typeof resolveChatSendRequestConflict>;
  try {
    const preparedInterrupt = await prepareChatSendInterruptAdmission({
      operator: { ...params, runId: clientRunId },
      interruptTarget: runInterruptTarget,
      entry: admittedRunAbort.entry,
      assertCurrent: params.assertCurrent,
      assertSessionTargetCurrent,
      abortSignal: admittedRunAbort.controller.signal,
    });
    capturedOperator = preparedInterrupt.operator;
    releaseCapturedOperator = capturedOperator.release;
    interruptedActiveRun = preparedInterrupt.interruptedActiveRun;
    gatewayWorkAdmission = await beginSessionEffect({
      sourceInput: admittedRunAbort.entry?.input,
      target: sessionTarget,
      storeWriterIdentities: [sessionKey, session.sessionTarget.storeKey],
      assertAllowed: () => {
        params.assertCurrent?.();
        assertSessionTargetCurrent();
        assertChatSendExclusiveAdmission(request, session);
      },
      revalidateAllowed: async () => {
        if (!restartSafeRequest) {
          return commitChatWorkAdmission(null);
        }
        const latest = await withCurrentChatSendSession({
          session,
          getRuntimeConfig: context.getRuntimeConfig,
          includeMembership: false,
          consume: (current) => current,
        });
        const [acpMeta] = await readAcpSessionMetaForEntries({
          cfg: latest.cfg,
          entries: [{ agentId, sessionKey: latest.canonicalKey, entry: latest.entry }],
        });
        return commitChatWorkAdmission(acpMeta ?? null);
      },
      onInterrupt: (reason) => {
        const stopReason = isAgentRunDirectAbortReason(reason) ? "rpc" : "restart";
        if (!admittedRunAbort.controller.signal.aborted) {
          // A later lifecycle drain must not overwrite the first abort reason.
          if (admittedRunAbort.entry) {
            admittedRunAbort.entry.adapter.abortStopReason = stopReason;
          }
          admittedRunAbort.controller.abort(
            stopReason === "rpc" ? reason : createAgentRunRestartAbortError(),
          );
        }
      },
    });
    if (
      admittedRunAbort.controller.signal.aborted &&
      !readChatSendDedupeResponse(context.dedupe, clientRunId)
    ) {
      reservation.abort(admittedRunAbort.entry?.adapter.abortStopReason ?? "rpc");
    }
    admittedRunAbort.controller.signal.throwIfAborted();
    params.assertCurrent?.();
    retainedRequestConflict = await consumeChatSendCurrent(params, () =>
      resolveChatSendRequestConflict(params, retryComparison, pendingAttemptId),
    );
  } catch (err) {
    const pendingReservationAtFailure = reservation.read();
    reservation.clear();
    admittedRunAbort.cleanup();
    gatewayWorkAdmission?.release();
    releaseCapturedOperator();
    respondChatSendWorkAdmissionFailure(
      params,
      err,
      {
        attemptId: pendingAttemptId,
        lifecycleGeneration,
        pendingReservation: pendingReservationAtFailure,
        requestIdentity,
        runAbort: admittedRunAbort,
      },
      retryComparison,
    );
    return { ok: false as const };
  }
  if (retainedRequestConflict) {
    reservation.clear();
    admittedRunAbort.cleanup();
    gatewayWorkAdmission.release();
    capturedOperator.release();
    respond(false, undefined, retainedRequestConflict);
    return { ok: false as const };
  }
  if (
    admittedRunAbort.registered &&
    !reservationSuperseded &&
    !readChatSendDedupeResponse(context.dedupe, clientRunId)
  ) {
    // Transfer immutable input identity before retiring the pending reservation.
    // It survives transient pre-ACK failures without inventing a successful response.
    context.dedupe.set(`chat:${clientRunId}`, {
      ts: Date.now(),
      ok: true,
      requestIdentity,
    });
  }
  reservation.clear();
  const releaseAdmissionOwners = () => {
    gatewayWorkAdmission.release();
    capturedOperator.release();
  };
  if (reservationSuperseded) {
    admittedRunAbort.cleanup();
    releaseAdmissionOwners();
    const supersedingCached =
      supersedingResult ?? readChatSendDedupeResponse(context.dedupe, clientRunId);
    if (supersedingCached) {
      respond(supersedingCached.ok, supersedingCached.payload, supersedingCached.error, {
        cached: true,
        runId: clientRunId,
      });
      return { ok: false as const };
    }
    respond(true, { runId: clientRunId, status: "in_flight" as const }, undefined, {
      cached: true,
      runId: clientRunId,
    });
    return { ok: false as const };
  }
  if (lifecycleGeneration !== getAgentEventLifecycleGeneration()) {
    if (admittedRunAbort.entry) {
      admittedRunAbort.entry.adapter.abortStopReason = "restart";
    }
    admittedRunAbort.controller.abort();
    admittedRunAbort.cleanup();
    releaseAdmissionOwners();
    if (!readChatSendDedupeResponse(context.dedupe, clientRunId)) {
      reservation.abort(admittedRunAbort.entry?.adapter.abortStopReason ?? "restart");
    }
    const aborted = readChatSendDedupeResponse(context.dedupe, clientRunId);
    respond(aborted?.ok ?? true, aborted?.payload, aborted?.error, {
      cached: true,
      runId: clientRunId,
    });
    return { ok: false as const };
  }
  if (!admittedRunAbort.registered) {
    releaseAdmissionOwners();
    respond(true, { runId: clientRunId, status: "in_flight" as const }, undefined, {
      cached: true,
      runId: clientRunId,
    });
    return { ok: false as const };
  }
  assertSourceAuthority = () => {
    capturedOperator.authority?.assertCurrent();
    assertSessionTargetCurrent();
    if (!gatewayWorkAdmission?.isActive()) {
      throw new Error("Chat source preparation custody ended");
    }
  };
  const dispatchCustody = await prepareAdmittedChatSendDispatch({
    params,
    assertSessionTargetCurrent,
    runAbort: admittedRunAbort,
    admission: gatewayWorkAdmission,
    releaseCaller: () =>
      releaseChatSendCallerAuthority({ operator: capturedOperator, request, session }),
  });
  if (!dispatchCustody) {
    return { ok: false as const };
  }
  const { releaseCallerAuthority, releaseGatewayRootContinuation } = dispatchCustody;

  const acquiredGatewayWorkAdmission = gatewayWorkAdmission;
  const sourceRef = admittedRunAbort.entry;
  let sessionPreparationActive = true;
  const onSessionPrepared = bindChatSendPreparedSession({
    sessionKey,
    sourceRef,
    lifecycleGeneration,
    admission: {
      isActive: () => sessionPreparationActive && acquiredGatewayWorkAdmission.isActive(),
    },
  });
  const retainedWork = createChatSendWorkAdmission({
    admission: acquiredGatewayWorkAdmission,
    releaseCallerAuthority,
    releaseGatewayRootContinuation,
    logGateway: context.logGateway,
  });
  // Prepared inbound media has no transcript reference until the user turn
  // persists; every abandonment exit funnels through cleanupAdmittedRun, so
  // the armed discard here is the single custody owner for that window. The
  // handler disarms it once the media becomes referenced (durable admission
  // or ACK handing ownership to dispatch, which persists on all paths).
  const admittedCleanup = createAdmittedChatSendCleanup({
    cleanupAbort: () => {
      sessionPreparationActive = false;
      admittedRunAbort.cleanup();
    },
    releaseRetainedWork: retainedWork.release,
  });
  const cleanupAdmittedRun = admittedCleanup.cleanup;
  const rejectSessionRoutingChanged = () => {
    cleanupAdmittedRun();
    clearAgentRunContext(clientRunId, lifecycleGeneration);
    respondChatSessionRoutingChanged(respond);
  };
  const finishAbortedChatSend = () =>
    publishAbortedChatSend({
      context,
      respond,
      runId: clientRunId,
      lifecycleGeneration,
      stopReason: admittedRunAbort.entry?.adapter.abortStopReason,
      sourceRef,
      cleanup: cleanupAdmittedRun,
    });
  claimAgentRunContext(clientRunId, {
    agentId: selectedAgent.agentId ?? agentId,
    sessionKey,
    sessionId: admittedSessionId,
    lifecycleGeneration,
    ...progressCardRefreshRunProjection(request.systemInputProvenance),
  });

  return {
    ok: true as const,
    value: {
      activeRunAbort: admittedRunAbort,
      operatorAuthority: capturedOperator.authority,
      armOperatorRunCancellation: capturedOperator.armCancellation,
      retireOperatorRunCancellation: capturedOperator.retireCancellation,
      admittedSessionSettings,
      admittedSessionId,
      ...(expectedActiveReplyOperation ? { expectedActiveReplyOperation } : {}),
      sourceRef,
      onSessionPrepared,
      initialSessionEntry,
      admittedSessionEntry,
      chatSendTraceAttributes,
      assertInitialSkillSelection,
      assertSessionTargetCurrent,
      cleanupAdmittedRun,
      finishAbortedChatSend,
      gatewayWorkAdmission,
      lifecycleGeneration,
      markInputAccepted: reservation.markInputAccepted,
      interruptedActiveRun,
      messageInjectionTarget,
      originatingRoute,
      rejectSessionRoutingChanged,
      releaseSourceWorkAdmission: retainedWork.release,
      retainGatewayWorkAdmission: retainedWork.retain,
      setPendingInputCleanup: retainedWork.setPendingInputCleanup,
      assertClientUploadAllowed: uploadAdmission.assertClientUploadAllowed,
      assertWorkAdmissionCurrent: () => {
        // Collect retires source cancellation while retaining the original
        // admission until the aggregate commits or settles.
        if (
          !retainedWork.isActive() ||
          !acquiredGatewayWorkAdmission.isActive() ||
          lifecycleGeneration !== getAgentEventLifecycleGeneration() ||
          (admittedRunAbort.controller.signal.aborted &&
            !(isRpcSourceRegistered(sourceRef) && sourceRef.input.custody.cancellationRetired))
        ) {
          throw new Error("Chat admission ended or was cancelled; submit a new turn.");
        }
      },
      restartSafeAdmission,
      setDiscardAbandonedPreparedMedia: admittedCleanup.setDiscardPreparedMedia,
    },
  };
}

type ChatSendAdmissionResult = Awaited<ReturnType<typeof admitChatSend>>;
export type AdmittedChatSend = Extract<ChatSendAdmissionResult, { ok: true }>["value"];
