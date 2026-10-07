/**
 * Runtime adapter for realtime voice control of active OpenClaw agent runs.
 *
 * The shared module owns classification and message contracts; this adapter
 * binds those contracts to embedded-run abort, status, and steering primitives.
 */
import type { ActiveEmbeddedRunOwner } from "../agents/embedded-agent-runner/runs.js";
import { isAbortError } from "../infra/abort-signal.js";
import { formatErrorMessage } from "../infra/errors.js";
import { getDiagnosticSessionActivitySnapshot } from "../logging/diagnostic-run-activity.js";
import type {
  ReplyMessageInjectionOptions,
  ReplyToolAuthorityOverlay,
} from "../sessions/session-controller.contracts.js";
import type { SessionControllerSteerResult } from "../sessions/session-controller.steer.js";
import type { UserTurnTranscriptRecorder } from "../sessions/user-turn-transcript.types.js";
import { captureRealtimeVoiceRunOwner } from "./agent-run-control-owner.js";
import {
  buildRealtimeVoiceAgentCancelProviderResult,
  buildRealtimeVoiceAgentFollowupSteeringText,
  formatRealtimeVoiceAgentQueueRejection,
  formatRealtimeVoiceAgentStatus,
  resolveRealtimeVoiceAgentControlIntent,
  type RealtimeVoiceAgentControlProviderResult,
  type RealtimeVoiceAgentControlResult,
  type RealtimeVoiceAgentRunActivity,
} from "./agent-run-control-shared.js";
import type { TalkEvent } from "./talk-events.js";

export {
  buildRealtimeVoiceAgentCancelProviderResult,
  buildRealtimeVoiceAgentControlSpeechMessage,
  classifyRealtimeVoiceAgentControlText,
  parseRealtimeVoiceAgentControlToolArgs,
  REALTIME_VOICE_AGENT_CONTROL_TOOL,
  REALTIME_VOICE_AGENT_CONTROL_TOOL_NAME,
  resolveRealtimeVoiceAgentControlIntent,
  shouldAutoControlRealtimeVoiceAgentText,
  type RealtimeVoiceAgentControlProviderResult,
  type RealtimeVoiceAgentControlResult,
} from "./agent-run-control-shared.js";

const controlResultPresentation = { speak: true, show: true, suppress: false };

/** Host error projection needs server-side redaction, outside browser-shared contracts. */
export function buildRealtimeVoiceAgentErrorProviderResult(
  error: unknown,
): RealtimeVoiceAgentControlProviderResult | { error: string } {
  return isAbortError(error)
    ? buildRealtimeVoiceAgentCancelProviderResult()
    : { error: formatErrorMessage(error) };
}

type RealtimeVoiceAgentControlDeps = {
  stopRealtimeVoiceSessionRun: (params: {
    sessionKey: string;
    sessionId: string;
  }) => Promise<boolean>;
  /** Steers one controller-owned turn; a refusal leaves no queued input behind. */
  steerActiveRun: (
    sessionId: string,
    text: string,
    options: ReplyMessageInjectionOptions,
  ) => Promise<SessionControllerSteerResult>;
  getDiagnosticSessionActivitySnapshot: (params: {
    sessionId?: string;
    sessionKey?: string;
  }) => RealtimeVoiceAgentRunActivity;
  resolveActiveSessionRunId: (sessionKey: string) => string | undefined;
  resolveActiveEmbeddedRunOwnerByRunId?: (
    runId: string,
  ) => Pick<ActiveEmbeddedRunOwner, "sessionId" | "sessionKey"> | undefined;
  resolveActiveReplyRunOwnerForSignal?: (
    signal: AbortSignal,
  ) => Pick<ActiveEmbeddedRunOwner, "sessionId" | "sessionKey"> | undefined;
};

/** Apply a spoken status, cancel, steer, or follow-up request to an active run. */
export async function controlRealtimeVoiceAgentRun(
  params: {
    sessionKey: string;
    /** Exact admitted owner; null forbids lookup, omission retains legacy session-key control. */
    runTarget?: {
      runId: string;
      signal: AbortSignal;
      isCurrent: (sessionId?: string) => boolean;
    } | null;
    text: string;
    getToolAuthorityOverlay?: () => ReplyToolAuthorityOverlay;
    /** Host context prepared by the validated authority callback, never provider text. */
    getSteeringContext?: () => string | undefined;
    createUserTurnTranscriptRecorder?: (text: string) => UserTurnTranscriptRecorder;
    mode?: unknown;
    recentEvents?: readonly TalkEvent[];
    /** Gateway Stop for an exact registered run; required to cancel a `runTarget`. */
    cancelRun?: (runId: string) => Promise<boolean>;
  },
  providedDeps?: RealtimeVoiceAgentControlDeps,
): Promise<RealtimeVoiceAgentControlResult> {
  const sessionKey = params.sessionKey.trim();
  const text = params.text.trim();
  const mode = resolveRealtimeVoiceAgentControlIntent({ text, mode: params.mode }).mode;
  const controlResultContext = { mode, sessionKey };
  const target = params.runTarget;
  let commands = providedDeps;
  // Exact registered runs need their owner-bound selector, never a key-only lookup.
  // Cold requests without a live registration do not load the mutating runtime.
  if (!commands && target && !target.signal.aborted && target.isCurrent()) {
    commands = (await import("./agent-run-control.runtime.js")).realtimeVoiceControlRuntime;
  }
  const projections =
    commands ??
    (target === undefined ? await import("../sessions/session-controller.queries.js") : undefined);
  const resolveCurrentRun = () => {
    const candidate =
      target && !target.signal.aborted && target.isCurrent()
        ? (commands?.resolveActiveEmbeddedRunOwnerByRunId?.(target.runId) ??
          commands?.resolveActiveReplyRunOwnerForSignal?.(target.signal))
        : undefined;
    const exactOwner =
      candidate?.sessionKey === sessionKey && target?.isCurrent(candidate.sessionId)
        ? candidate
        : undefined;
    const sessionId =
      target === undefined
        ? projections?.resolveActiveSessionRunId(sessionKey)
        : exactOwner?.sessionId;
    return { sessionId, exactOwner };
  };
  let current = resolveCurrentRun();
  // Custom dependency adapters own their state; the built-in selector pins its registry owner.
  const legacyOwner =
    target === undefined && !providedDeps && current.sessionId
      ? captureRealtimeVoiceRunOwner(current.sessionId, sessionKey)
      : undefined;
  const legacySessionId = current.sessionId;
  const isLegacyCurrent = () =>
    providedDeps !== undefined ||
    (current.sessionId === legacySessionId && legacyOwner?.isCurrent() === true);
  const readActivity =
    providedDeps?.getDiagnosticSessionActivitySnapshot ?? getDiagnosticSessionActivitySnapshot;
  // Global keys are shared across agents. Exact selectors never consult another
  // session's key-only diagnostics, including when their live owner disappeared.
  const activity =
    target === undefined
      ? readActivity({ sessionId: current.sessionId, sessionKey })
      : current.sessionId
        ? readActivity({ sessionId: current.sessionId })
        : undefined;
  const active = Boolean(
    current.sessionId || activity?.activeWorkKind || activity?.hasActiveEmbeddedRun,
  );

  // Without an exact live registration, status stays on lightweight diagnostics
  // and remains available even when the mutating runtime cannot load.
  if (mode === "status") {
    return {
      ok: true,
      ...controlResultContext,
      ...(current.sessionId ? { sessionId: current.sessionId } : {}),
      active,
      message: formatRealtimeVoiceAgentStatus({
        active,
        recentEvents: params.recentEvents,
        activity,
      }),
      ...controlResultPresentation,
    };
  }

  const noActiveRun = (): RealtimeVoiceAgentControlResult => ({
    ok: false,
    ...controlResultContext,
    active: false,
    ...(mode === "cancel" ? { aborted: false } : { queued: false }),
    reason: "no_active_run",
    message: `There is no active OpenClaw run to ${mode === "cancel" ? "cancel" : "steer"}.`,
    ...controlResultPresentation,
  });
  if (!current.sessionId || (target === undefined && !isLegacyCurrent())) {
    return noActiveRun();
  }
  if (!commands) {
    commands = (await import("./agent-run-control.runtime.js")).realtimeVoiceControlRuntime;
    // Loading commands can outlive admission; resolve the exact target again
    // in the continuation that performs the action.
    current = resolveCurrentRun();
  }
  const { sessionId } = current;
  if (!sessionId || (target === undefined && !isLegacyCurrent())) {
    return noActiveRun();
  }
  const toolAuthorityOverlay = params.getToolAuthorityOverlay?.();
  const preparedOwner = resolveCurrentRun();
  if (
    preparedOwner.sessionId !== sessionId ||
    (target ? !target.isCurrent(sessionId) : !isLegacyCurrent())
  ) {
    return noActiveRun();
  }
  if (mode === "cancel") {
    const aborted =
      target === undefined
        ? await commands.stopRealtimeVoiceSessionRun({ sessionKey, sessionId })
        : target !== null && (await params.cancelRun?.(target.runId)) === true;
    const message = aborted
      ? "Cancelled the active OpenClaw run."
      : "OpenClaw could not cancel the active run.";
    return {
      ok: aborted,
      ...controlResultContext,
      sessionId,
      active: true,
      aborted,
      ...(aborted ? {} : { reason: "abort_rejected" }),
      message,
      ...controlResultPresentation,
      ...(aborted ? { providerResult: buildRealtimeVoiceAgentCancelProviderResult(message) } : {}),
    };
  }

  // Steering and follow-up both enqueue to the active run; follow-up is wrapped
  // so the runner treats it as deferred context instead of an immediate pivot.
  const steeringText = [params.getSteeringContext?.(), text].filter(Boolean).join("\n\n");
  const steerText =
    mode === "followup" ? buildRealtimeVoiceAgentFollowupSteeringText(steeringText) : steeringText;
  const options = {
    steeringMode: "all" as const,
    debounceMs: 0,
    isInboundUserMessage: true,
    toolAuthorityOverlay,
    userTurnTranscriptRecorder: params.createUserTurnTranscriptRecorder?.(steerText),
    // Talk cannot present task suggestions, so spoken user input must not inherit
    // a capable TUI run's model-facing task tools.
    taskSuggestionDeliveryMode: undefined,
  };
  // Exact and pinned legacy owners bind the steer to their source authority.
  const sourceCurrent =
    target || legacyOwner
      ? () => {
          if (target) {
            return !target.signal.aborted && target.isCurrent(sessionId);
          }
          const currentOverlay = params.getToolAuthorityOverlay?.();
          return Boolean(
            legacyOwner?.isCurrent() &&
            (!currentOverlay || legacyOwner.matchesCaller(currentOverlay)),
          );
        }
      : undefined;
  let steer: SessionControllerSteerResult;
  try {
    steer = await commands.steerActiveRun(sessionId, steerText, {
      ...options,
      ...(sourceCurrent
        ? {
            assertCurrent: () => {
              if (!sourceCurrent()) {
                throw new Error("Voice control target is no longer current");
              }
            },
          }
        : {}),
    });
  } catch (error) {
    steer = {
      status: "rejected",
      reason: "runtime_rejected",
      errorMessage: formatErrorMessage(error),
    };
  }
  if (steer.status === "rejected") {
    return {
      ok: false,
      ...controlResultContext,
      sessionId,
      active: true,
      queued: false,
      reason: steer.reason,
      message: formatRealtimeVoiceAgentQueueRejection(mode, steer.reason),
      ...controlResultPresentation,
    };
  }

  const unconfirmed =
    steer.status === "indeterminate" || steer.result?.transcriptCommit === "unconfirmed";
  const message = unconfirmed
    ? "OpenClaw could not confirm that input. It was not sent again; check the conversation before retrying."
    : mode === "followup"
      ? "Queued that follow-up for the active OpenClaw run."
      : "Got it. I steered the active run.";
  return {
    ok: !unconfirmed,
    ...controlResultContext,
    sessionId,
    active: true,
    queued: true,
    ...(unconfirmed ? { reason: "delivery_unconfirmed" } : {}),
    message,
    ...controlResultPresentation,
  };
}
