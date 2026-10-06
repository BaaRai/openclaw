import {
  buildAgentRunTerminalOutcomeFromLifecycleEvent,
  classifyAgentRunTerminalOutcome,
} from "../../agents/agent-run-terminal-outcome.js";
import { hasCompletedSourceReplyDeliveryEvidence } from "../../agents/embedded-agent-runner/delivery-evidence.js";
import { normalizeChatType } from "../../channels/chat-type.js";
import type { ProgressContinuationCapability } from "../../channels/progress-continuation.js";
import { clearAgentRunContext } from "../../infra/agent-run-registry.js";
import { formatErrorMessage } from "../../infra/errors.js";
import {
  getPluginRuntimeGatewayRequestScope,
  withPluginRuntimeGatewayContextResolver,
} from "../../plugins/runtime/gateway-request-scope.js";
import { defaultRuntime } from "../../runtime.js";
import { resolveSendPolicy } from "../../sessions/send-policy.js";
import type { ReplyOperation } from "../../sessions/session-controller.js";
import { deferSessionControllerClaimBeforeExecution } from "../../sessions/session-controller.mailbox-claim.js";
import { sessionDeliveryChannel } from "../../utils/delivery-context.read.js";
import {
  getReplyPayloadMetadata,
  markReplyPayloadForSourceSuppressionDelivery,
} from "../reply-payload.js";
import type { ReplyPayload } from "../types.js";
import type { AgentTurnExecutionResult } from "./agent-runner-execution.types.js";
import { buildPreflightCompactionFailureText } from "./agent-runner-failure-reply.js";
import { accountFollowupTurn } from "./agent-runner-result-accounting.js";
import { createCompactionNoticePayload } from "./compaction-notice.js";
import { deliverFollowupDecision, resolveFollowupDeliveryDecision } from "./followup-delivery.js";
import { settleQueuedFollowupPresentation } from "./followup-presentation.js";
import { executeFollowupTurn } from "./followup-turn-execution.js";
import { completeFollowupRunLifecycle, type FollowupRun } from "./queue.js";
import { admitFollowupRunLifecycle } from "./queue/lifecycle.js";
import { isFollowupRunAborted, type QueuedFollowupReplyBatch } from "./queue/types.js";
import {
  prepareReplyAgentTurn,
  type AdmittedFollowupTurn,
  type FollowupRunnerParams,
} from "./reply-agent-turn-preparation.js";
import {
  isReplyOperationStalledBeforeOutput,
  STALLED_TURN_NOTICE_TEXT,
} from "./stalled-turn-recovery.js";

type FollowupDrainDisposition = { kind: "consumed" } | { kind: "retry"; error: unknown };

function resolveQueuedTurnSendPolicy(turn: AdmittedFollowupTurn): "allow" | "deny" {
  const entry = turn.session.current();
  return resolveSendPolicy({
    cfg: turn.config,
    entry,
    sessionKey:
      turn.queued.run.runtimePolicySessionKey ??
      (turn.session.kind === "session" ? turn.session.key : turn.queued.run.sessionKey),
    channel:
      turn.queued.originatingChannel ??
      turn.queued.run.messageProvider ??
      sessionDeliveryChannel(entry),
    chatType: normalizeChatType(
      turn.queued.originatingChatType ?? turn.queued.run.chatType ?? entry?.chatType,
    ),
  });
}

function resolveFollowupCompletion(
  outcome: AgentTurnExecutionResult["outcome"],
): QueuedFollowupReplyBatch["completion"] {
  const meta = outcome.kind === "settled" ? outcome.result.meta : undefined;
  const failed =
    outcome.kind === "rejected" || (outcome.kind === "settled" && outcome.status === "failed");
  const terminal = buildAgentRunTerminalOutcomeFromLifecycleEvent({
    phase: failed ? "error" : "end",
    data: {
      aborted: outcome.kind === "aborted" || meta?.aborted,
      stopReason:
        outcome.kind === "aborted"
          ? outcome.reason === "user"
            ? "aborted"
            : outcome.reason
          : meta?.stopReason,
      timeoutPhase: meta?.timeoutPhase,
      providerStarted: meta?.providerStarted,
      livenessState: meta?.livenessState,
      error:
        outcome.kind === "rejected"
          ? outcome.payload.text
          : outcome.kind === "settled" && outcome.status === "failed"
            ? outcome.terminalFailurePayload.text
            : meta?.error?.message,
    },
  });
  const classification = classifyAgentRunTerminalOutcome(terminal);
  const stopReason = terminal.stopReason ? { stopReason: terminal.stopReason } : {};
  if (classification === "cancellation") {
    return { kind: "aborted", ...stopReason };
  }
  if (classification === "failure" || classification === "timeout") {
    return {
      kind: "failed",
      error: terminal.error ?? "Follow-up failed.",
      ...stopReason,
      ...(classification === "timeout" ? { errorKind: "timeout" } : {}),
    };
  }
  return { kind: "completed", ...stopReason };
}

export function createFollowupRunner(
  initialDefaults: FollowupRunnerParams,
): (queued: FollowupRun) => Promise<void> {
  const resolveGatewayContext = Object.hasOwn(initialDefaults, "resolveGatewayContext")
    ? initialDefaults.resolveGatewayContext
    : getPluginRuntimeGatewayRequestScope()?.resolveGatewayContext;
  const defaults = { ...initialDefaults, resolveGatewayContext };
  // Every queue handoff, including delivery retries, retains this host owner
  // without borrowing the invoking turn's request-local authority.
  const runFollowup = (queued: FollowupRun): Promise<void> =>
    withPluginRuntimeGatewayContextResolver(resolveGatewayContext, () => executeFollowup(queued), {
      inheritRequestScope: false,
    });
  const deliverProgress = async (
    turn: AdmittedFollowupTurn,
    payloads: ReplyPayload[],
    kind: "tool" | "block",
  ) => {
    await deliverFollowupDecision({
      decision: { kind: "deliver", payloads },
      turn,
      defaults,
      runId: turn.runId,
      runFollowup,
      kind,
    });
  };
  const executeFollowup = async (queued: FollowupRun): Promise<void> => {
    let disposition: FollowupDrainDisposition = { kind: "retry", error: undefined };
    let operation: ReplyOperation | undefined;
    let admittedRunId: string | undefined;
    let admittedTurn: AdmittedFollowupTurn | undefined;
    let terminalPayloads: ReplyPayload[] = [];
    let progressContinuation: ProgressContinuationCapability | undefined;
    const admissionNotices: ReplyPayload[] = [];
    const terminalCompactionNotices: ReplyPayload[] = [];
    let completion: QueuedFollowupReplyBatch["completion"] = { kind: "completed" };
    let queuedFollowupAdmitted = false;
    let executionEntered = false;
    const claim = queued.controllerClaim ?? queued.controllerInput?.claim;
    const hasSurvivingSources = () =>
      Boolean(
        claim &&
        claim.sources.length > 1 &&
        claim.sources.some(
          (source) => !isFollowupRunAborted(source) && !source.controllerInput?.retirementRequested,
        ),
      );
    const initiallyAborted = isFollowupRunAborted(queued);
    const endDeliveryCorrelations = initiallyAborted
      ? []
      : (queued.deliveryCorrelations ?? [])
          .map((correlation) => correlation.begin())
          .filter((end): end is () => void => typeof end === "function");
    try {
      if (initiallyAborted) {
        disposition = hasSurvivingSources()
          ? { kind: "retry", error: queued.abortSignal?.reason }
          : { kind: "consumed" };
        return;
      }
      const deliverCompactionNotice = async (
        payload: ReplyPayload,
        phase: import("./compaction-notice.js").CompactionNoticePhase,
        turn: AdmittedFollowupTurn,
      ) => {
        const source = turn.queued.queuedFollowupReplyDisposition;
        if (
          phase !== "memory_flush_degraded" &&
          source?.kind === "deliver" &&
          source.deliver.ownsCompletion?.(turn.queued.originatingChannel)
        ) {
          admissionNotices.push(payload);
          return;
        }
        await deliverProgress(turn, [payload], "block");
      };
      const admission = await prepareReplyAgentTurn({
        queued,
        defaults,
        onCompactionNotice: async (phase, text, turn) => {
          turn.sendPolicy = resolveQueuedTurnSendPolicy(turn);
          if (turn.sendPolicy === "deny") {
            return;
          }
          const currentMessageId =
            queued.run.inputProvenance?.kind === "internal_system" &&
            queued.run.inputProvenance.sourceTool === "restart-sentinel"
              ? queued.originatingReplyToId
              : queued.messageId;
          const payload = createCompactionNoticePayload({ phase, text, currentMessageId });
          if (phase !== "start" && phase !== "memory_flush_degraded") {
            terminalCompactionNotices.push(payload);
            return;
          }
          await deliverCompactionNotice(payload, phase, turn);
        },
      });
      if (admission.kind === "skipped") {
        operation = admission.operation;
        disposition =
          admission.reason === "aborted" && hasSurvivingSources()
            ? { kind: "retry", error: queued.abortSignal?.reason }
            : { kind: "consumed" };
        return;
      }
      const turn: AdmittedFollowupTurn = admission.turn;
      operation = turn.operation;
      await admitFollowupRunLifecycle(turn.queued);
      if (turn.preflightError) {
        turn.operation.fail("run_failed", turn.preflightError);
        const text = buildPreflightCompactionFailureText(formatErrorMessage(turn.preflightError), {
          includeDetails:
            turn.queued.run.verboseLevelOverride === "on" ||
            turn.queued.run.verboseLevelOverride === "full",
        });
        if (text) {
          turn.preflightFailurePayload = markReplyPayloadForSourceSuppressionDelivery({ text });
          turn.preflightError = undefined;
        }
      }
      turn.sendPolicy = resolveQueuedTurnSendPolicy(turn);
      if (turn.sendPolicy === "allow") {
        for (const payload of terminalCompactionNotices) {
          await deliverCompactionNotice(payload, "end", turn);
        }
      }
      admittedTurn = turn;
      admittedRunId = turn.runId;
      queuedFollowupAdmitted = true;
      executionEntered = true;
      const execution = await executeFollowupTurn({
        turn,
        defaults,
        onToolResult: (payload) => deliverProgress(turn, [payload], "tool"),
        onCompactionNoticePayload: (payload) => deliverProgress(turn, [payload], "block"),
      });
      // A closed execution result is terminal queue work. Commit consumption
      // before accounting/delivery so their failures cannot replay model or tool effects.
      disposition = { kind: "consumed" };
      completion =
        turn.queued.stalledTurnRecovery === true &&
        isReplyOperationStalledBeforeOutput(turn.operation)
          ? // A watchdog stall is a failure, not a user cancel: source owners
            // surface its last-resort notice through their terminal error.
            { kind: "failed", error: STALLED_TURN_NOTICE_TEXT }
          : resolveFollowupCompletion(execution.execution.outcome);
      try {
        await execution.progress.drain();
      } catch (error) {
        if (completion.kind === "completed") {
          completion = { kind: "failed", error: formatErrorMessage(error) };
        }
        // Execution already settled; replaying the queued prompt could duplicate side effects.
        defaultRuntime.error?.(
          `followup queue: progress presentation failed after execution: ${formatErrorMessage(error)}`,
        );
        operation.fail("run_failed", error);
      }
      // Admission can fail after compaction. Publish its notices only once this
      // execution is consumed and its terminal delivery owner can close the run.
      if (
        admissionNotices.length > 0 &&
        turn.sendPolicy === "allow" &&
        turn.queued.currentInboundEventKind !== "room_event"
      ) {
        await deliverProgress(turn, admissionNotices, "block");
      }
      if (
        execution.execution.outcome.kind === "settled" &&
        hasCompletedSourceReplyDeliveryEvidence(execution.execution.outcome.result)
      ) {
        await defaults.opts?.onObservedReplyDelivery?.();
      }
      const accounting = await accountFollowupTurn({ turn, defaults, execution });
      const deliveryOpts = {
        ...defaults.opts,
        commentaryPayloadsEnabled: execution.commentaryPayloadsEnabled,
      };
      const decision = await resolveFollowupDeliveryDecision({
        turn,
        execution: execution.execution,
        accounting,
        opts: deliveryOpts,
      });
      if (decision.kind === "deliver") {
        for (const payload of decision.payloads) {
          progressContinuation = getReplyPayloadMetadata(payload)?.progressContinuation;
          if (progressContinuation) {
            break;
          }
        }
      }
      if (
        completion.kind === "completed" &&
        decision.kind === "suppress" &&
        (decision.reason === "silent" || decision.reason === "message-tool-only")
      ) {
        completion = { ...completion, allowCanvasOnly: true };
      }
      const delivery = await deliverFollowupDecision({
        decision,
        turn,
        defaults,
        runId: execution.execution.runId,
        runFollowup,
      });
      // Source recovery has its own queued callback; this execution still closes once.
      terminalPayloads = delivery.kind === "completed" ? delivery.payloads : [];
    } catch (error) {
      let operatorAuthorityLost = false;
      try {
        queued.operatorAuthority?.assertCurrent();
      } catch {
        operatorAuthorityLost = true;
      }
      if (operatorAuthorityLost) {
        // Revoked input is terminal; retrying it would hold the queue indefinitely.
        disposition = { kind: "consumed" };
        completion = { kind: "aborted" };
        defaultRuntime.error?.("followup queue: canceled input after loss of operator authority");
      } else if (
        operation?.result?.kind === "aborted" &&
        operation.result.code === "aborted_by_user"
      ) {
        disposition = hasSurvivingSources() ? { kind: "retry", error } : { kind: "consumed" };
        completion = resolveFollowupCompletion({ kind: "aborted", reason: "user" });
      } else if (disposition.kind === "consumed") {
        completion = { kind: "failed", error: formatErrorMessage(error) };
        defaultRuntime.error?.(
          `followup queue: terminal handling failed after execution; refusing replay: ${formatErrorMessage(error)}`,
        );
        operation?.fail("run_failed", error);
      } else {
        disposition = { kind: "retry", error };
      }
    } finally {
      if (
        disposition.kind === "retry" &&
        claim &&
        (executionEntered || !deferSessionControllerClaimBeforeExecution(claim))
      ) {
        completion = { kind: "failed", error: formatErrorMessage(disposition.error) };
        operation?.fail("run_failed", disposition.error);
        disposition = { kind: "consumed" };
      }
      const sourceDisposition = admittedTurn?.queued.queuedFollowupReplyDisposition;
      if (
        disposition.kind === "consumed" &&
        admittedTurn &&
        sourceDisposition?.kind === "deliver"
      ) {
        try {
          await sourceDisposition.deliver({
            kind: "queued-followup",
            runId: admittedTurn.runId,
            originatingChannel: admittedTurn.queued.originatingChannel,
            payloads: terminalPayloads,
            completion,
          });
        } catch (error) {
          defaultRuntime.error?.(
            `followup queue: completion delivery failed; refusing replay: ${formatErrorMessage(error)}`,
          );
          operation?.fail("run_failed", error);
        }
      }
      try {
        if (queuedFollowupAdmitted) {
          await settleQueuedFollowupPresentation(defaults.opts?.onQueuedFollowupSettled);
        }
      } finally {
        progressContinuation?.close();
      }
      for (const end of endDeliveryCorrelations.toReversed()) {
        try {
          end();
        } catch (error) {
          defaultRuntime.error?.(
            `followup queue: delivery correlation cleanup failed: ${formatErrorMessage(error)}`,
          );
        }
      }
      if (disposition.kind === "consumed") {
        if (claim) {
          claim.retryBeforeExecution = false;
        }
        completeFollowupRunLifecycle(queued);
      }
      if (admittedRunId) {
        clearAgentRunContext(admittedRunId);
      }
      operation?.complete();
      defaults.typing.markRunComplete();
      defaults.typing.markDispatchIdle();
    }

    if (disposition.kind === "retry") {
      throw disposition.error;
    }
  };
  return runFollowup;
}
