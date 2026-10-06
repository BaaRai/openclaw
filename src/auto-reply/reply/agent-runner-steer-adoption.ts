import { expectDefined } from "@openclaw/normalization-core";
import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { isIngressAdoptionLostError } from "../../channels/message/ingress-drain.js";
import { resolveRestartRecoverySteeringBlockReason } from "../../config/sessions/restart-recovery-receipt.js";
import { readSessionEntryInWorker } from "../../config/sessions/session-entry-read-runtime.js";
import { logVerbose } from "../../globals.js";
import { formatErrorMessage } from "../../infra/errors.js";
import type { ReplyMessageInjectionOutcome } from "../../sessions/session-controller.contracts.js";
import {
  beginReplyMessageInjectionTarget,
  captureReplyMessageInjectionTarget,
  finalizeReplyMessageInjectionAttempt,
  type ReplyOperation,
  getSessionControllerOperation,
} from "../../sessions/session-controller.js";
import { markReplyPayloadForSourceSuppressionDelivery } from "../reply-payload.js";
import type { ReplyPayload } from "../types.js";
import {
  scheduleFollowupDrainAfterReplyOperationClear,
  type RunReplyAgentParams,
} from "./agent-runner-core.js";
import {
  admitFollowupRunLifecycle,
  reserveSteerCandidate,
  resolveFollowupAbortSignal,
  scheduleFollowupDrain,
  type FollowupRun,
} from "./queue.js";
import type { ReplyOperationRunState } from "./reply-operation-run-state.js";
import { refreshReplyOperationTyping } from "./reply-run-typing.js";
import { buildChannelSourceTurnId } from "./source-turn-id.js";
import type { TypingSignaler } from "./typing-mode.js";

type ActiveReplySteerParams = {
  followupRun: RunReplyAgentParams["followupRun"];
  opts: RunReplyAgentParams["opts"];
  providedReplyOperation: ReplyOperation | undefined;
  automaticFallbackRoute?: ReplyOperation["automaticFallbackRoute"];
  queueKey: string;
  releaseAdmissionTicket: () => void;
  replyOperationRunState: ReplyOperationRunState | undefined;
  resolvedQueue: RunReplyAgentParams["resolvedQueue"];
  restartRecoverySourceTurnId: string | undefined;
  runFollowup: (run: FollowupRun) => Promise<void>;
  sessionCtx: RunReplyAgentParams["sessionCtx"];
  sessionKey: string | undefined;
  sessionEntry?: RunReplyAgentParams["sessionEntry"];
  storePath?: string;
  touchActiveSessionEntry: () => Promise<void>;
  typing: RunReplyAgentParams["typing"];
  typingSignals: TypingSignaler;
  toolAuthorityFingerprint: string;
  pendingInputAuthorityFingerprint?: string;
};

function resolveAcceptedSteerRunId(params: ActiveReplySteerParams): string {
  const { followupRun, sessionCtx } = params;
  return expectDefined(
    params.restartRecoverySourceTurnId ??
      buildChannelSourceTurnId({
        provider:
          followupRun.originatingChannel ?? followupRun.run.messageProvider ?? sessionCtx.Provider,
        accountId:
          followupRun.originatingAccountId ??
          followupRun.run.agentAccountId ??
          sessionCtx.AccountId,
        conversationId:
          followupRun.originatingTo ??
          followupRun.originatingChatId ??
          params.sessionKey ??
          followupRun.run.sessionKey,
        messageId: followupRun.messageId ?? sessionCtx.MessageSidFull ?? sessionCtx.MessageSid,
      }) ??
      params.opts?.runId,
    "steered turn id",
  );
}

export async function runActiveReplySteer(
  params: ActiveReplySteerParams,
): Promise<"handled" | ReplyPayload> {
  const {
    followupRun,
    queueKey,
    releaseAdmissionTicket,
    replyOperationRunState,
    resolvedQueue,
    runFollowup,
    sessionKey,
    touchActiveSessionEntry,
    typing,
    typingSignals,
  } = params;
  // Steer against the operation that owns THIS session's run slot. A native
  // command continuation whose slot adoption was skipped (#104844) still
  // carries a source-keyed reservation; steering by its stale sessionId
  // would miss the live target run.
  const activeReplyOperation = params.providedReplyOperation;
  const steerSessionId = activeReplyOperation?.sessionId ?? followupRun.run.sessionId;
  // Capture exact injection authority before parking or awaiting admission.
  // A same-key successor must never inherit this turn's steer or abort.
  const injectionTarget = captureReplyMessageInjectionTarget(activeReplyOperation);
  const parked = reserveSteerCandidate(queueKey, followupRun, resolvedQueue, runFollowup);
  if (!parked) {
    releaseAdmissionTicket();
    typing.cleanup();
    return "handled";
  }
  const owner = followupRun.controllerInput
    ? followupRun.controllerInput.mailbox.owner.active
    : getSessionControllerOperation(queueKey);
  if (owner) {
    scheduleFollowupDrainAfterReplyOperationClear({ operation: owner, queueKey, runFollowup });
  } else {
    scheduleFollowupDrain(queueKey, runFollowup);
  }
  releaseAdmissionTicket();
  let custodyFinished = false;
  const fallback = async (reason?: string): Promise<"handled"> => {
    parked.fallback();
    custodyFinished = true;
    if (
      replyOperationRunState &&
      !(
        replyOperationRunState.admission?.status === "skipped" &&
        replyOperationRunState.admission.reason === "queue-cap"
      )
    ) {
      replyOperationRunState.admission = { status: "accepted", mode: "followup" };
    }
    if (reason) {
      logVerbose(`queue: active session ${steerSessionId} rejected steering (${reason})`);
    }
    await touchActiveSessionEntry();
    typing.cleanup();
    return "handled";
  };
  // Outcome is the native write receipt. Acceptance callbacks and source
  // cancellation cannot release its custody or authorize replay.
  let nativeOutcome: Promise<ReplyMessageInjectionOutcome> | undefined;
  let replayForbidden = false;
  try {
    const admission = await parked.admit();
    if (admission === "cancelled") {
      parked.consume();
      custodyFinished = true;
      typing.cleanup();
      return "handled";
    }
    if (admission === "fallback") {
      return await fallback();
    }
    if (!injectionTarget) {
      return await fallback("no injectable reply operation");
    }
    // A predecessor's admission may wait past this run's terminal delivery.
    // Keep the parked input in the ordered queue if its target is no longer eligible.
    let entry = params.sessionEntry;
    if (sessionKey && params.storePath) {
      try {
        entry =
          (await readSessionEntryInWorker(
            {
              agentId: followupRun.run.agentId,
              sessionKey,
              storePath: params.storePath,
            },
            () => followupRun.operatorAuthority?.assertCurrent(),
          )) ?? entry;
      } catch (error) {
        return await fallback(`session entry unavailable: ${formatErrorMessage(error)}`);
      }
    }
    const blockReason = resolveRestartRecoverySteeringBlockReason(
      entry,
      steerSessionId,
      injectionTarget.sourceTurnId ??
        normalizeOptionalString(entry?.restartRecoveryDeliverySourceRunId) ??
        "",
    );
    if (blockReason) {
      return await fallback(`terminal source-reply delivery is closed (${blockReason})`);
    }
    const automaticFallbackRoute = params.automaticFallbackRoute;
    const isCurrentFallback = () =>
      !automaticFallbackRoute ||
      (activeReplyOperation?.automaticFallbackRoute === automaticFallbackRoute &&
        activeReplyOperation.toolAuthorityRoute?.provider === automaticFallbackRoute.provider &&
        activeReplyOperation.toolAuthorityRoute.model === automaticFallbackRoute.model);
    if (!isCurrentFallback()) {
      return await fallback("automatic model fallback changed during steering admission");
    }
    const injectionAttempt = beginReplyMessageInjectionTarget(injectionTarget, followupRun.prompt, {
      currentInboundContext: followupRun.currentInboundContext,
      inboundAudio: followupRun.currentInboundAudio === true,
      assertCurrent: automaticFallbackRoute
        ? () => {
            followupRun.operatorAuthority?.assertCurrent();
            if (!isCurrentFallback()) {
              throw new Error("Automatic model fallback changed during steering admission");
            }
          }
        : followupRun.operatorAuthority?.assertCurrent,
      steeringMode: "all",
      isInboundUserMessage:
        followupRun.currentInboundEventKind !== "room_event" &&
        (followupRun.run.inputProvenance?.kind === undefined ||
          followupRun.run.inputProvenance.kind === "external_user"),
      terminalReplyExpectation: followupRun.run.terminalReplyExpectation,
      toolAuthorityFingerprint: params.toolAuthorityFingerprint,
      personalToolParticipant: {
        operatorAuthority: followupRun.operatorAuthority,
        senderId: followupRun.run.senderId,
        senderName: followupRun.run.senderName,
        gatewayUiCommandTarget: followupRun.run.gatewayUiCommandTarget,
      },
      ...(params.pendingInputAuthorityFingerprint
        ? { pendingInputAuthorityFingerprint: params.pendingInputAuthorityFingerprint }
        : {}),
      ...(followupRun.images?.length ? { images: followupRun.images } : {}),
      ...(followupRun.imageOrder?.length ? { imageOrder: followupRun.imageOrder } : {}),
      ...(followupRun.media?.length ? { media: followupRun.media } : {}),
      waitForTranscriptCommit: true,
      queueIdentity: resolveAcceptedSteerRunId(params),
      abortSignal: resolveFollowupAbortSignal(followupRun),
      onQueueAccepted: parked.accepted,
      ...(resolvedQueue.debounceMs !== undefined ? { debounceMs: resolvedQueue.debounceMs } : {}),
      ...(followupRun.run.sourceReplyDeliveryMode
        ? { sourceReplyDeliveryMode: followupRun.run.sourceReplyDeliveryMode }
        : {}),
      taskSuggestionDeliveryMode: followupRun.run.taskSuggestionDeliveryMode,
      ...(followupRun.userTurnTranscriptRecorder
        ? { userTurnTranscriptRecorder: followupRun.userTurnTranscriptRecorder }
        : {}),
    });
    nativeOutcome = injectionAttempt.outcome;
    const finalization = await finalizeReplyMessageInjectionAttempt({
      attempt: injectionAttempt,
      target: injectionTarget,
      inboundAudio: followupRun.currentInboundAudio === true,
      onOutcome: (outcome) => {
        replayForbidden = true;
        if (replyOperationRunState) {
          replyOperationRunState.admission =
            outcome === "indeterminate"
              ? { status: "skipped", reason: "question-response-indeterminate" }
              : { status: "accepted", mode: "steer" };
        }
      },
      onAdopted: () => admitFollowupRunLifecycle(followupRun),
      shouldAbortOnAdoptionError: isIngressAdoptionLostError,
    });
    if (finalization.status === "rejected") {
      if (followupRun.controllerInput?.injection?.accepted !== true) {
        return await fallback(finalization.outcome.reason);
      }
      // A late negative result cannot undo an earlier native acceptance.
      parked.consume("consumed");
      custodyFinished = true;
      if (replyOperationRunState) {
        replyOperationRunState.admission = { status: "accepted", mode: "steer" };
      }
      typing.cleanup();
      return "handled";
    }
    // Accepted or indeterminate input cannot be abandoned for replay, even
    // when the source's later adoption callback rejects.
    parked.consume("consumed");
    custodyFinished = true;
    if (finalization.status === "indeterminate") {
      typing.cleanup();
      return markReplyPayloadForSourceSuppressionDelivery({
        text: finalization.outcome.errorMessage,
        isError: true,
      });
    }
    const transcriptCommitUnconfirmed =
      finalization.outcome.result?.transcriptCommit === "unconfirmed";
    if (finalization.aborted) {
      if (replyOperationRunState) {
        replyOperationRunState.messageInjectionAborted = true;
      }
      const reason = transcriptCommitUnconfirmed
        ? (finalization.outcome.result?.errorMessage ?? "transcript commitment unconfirmed")
        : `adoption lost: ${formatErrorMessage(finalization.adoptionError)}`;
      logVerbose(
        `queue: active session ${steerSessionId} aborted exact steered target without replay (${reason})`,
      );
      typing.cleanup();
      return "handled";
    }
    if (finalization.adoptionError) {
      logVerbose(
        `queue: active session ${steerSessionId} adoption finalizer failed: ${formatErrorMessage(finalization.adoptionError)}`,
      );
    }
    if (activeReplyOperation) {
      await refreshReplyOperationTyping(activeReplyOperation, {
        startIfIdle: typingSignals.shouldStartImmediately,
      });
    }
    await touchActiveSessionEntry();
    typing.cleanup();
    return "handled";
  } finally {
    if (!custodyFinished) {
      // Even a fallible finalizer may not transfer a pending native write to a
      // successor. A rejected outcome Promise carries no safe-replay receipt.
      if (nativeOutcome) {
        try {
          const outcome = await nativeOutcome;
          replayForbidden ||= outcome.status === "accepted" || outcome.status === "indeterminate";
        } catch {
          replayForbidden = true;
        }
      }
      if (replayForbidden || followupRun.controllerInput?.injection?.accepted === true) {
        parked.consume("consumed");
      } else if (resolveFollowupAbortSignal(followupRun)?.aborted) {
        parked.consume();
      } else {
        parked.fallback();
      }
    }
  }
}
