import { resolveAgentRunAbortLifecycleFields } from "../../agents/run-termination.js";
import type { TurnAdoptionLifecycle } from "../../auto-reply/get-reply-options.types.js";
import type {
  QueuedFollowupReplyBatch,
  QueuedFollowupReplyDelivery,
} from "../../auto-reply/reply/queue/types.js";
import { bindReplySourceInput } from "../../auto-reply/reply/reply-source-binding.js";
import { retireSessionControllerSourceCancellation } from "../../sessions/session-controller.mailbox.js";
import {
  getRpcSourceIdentity,
  getRpcSourceLifecycleGeneration,
  isRpcSourceRegistered,
  type RpcSourceRef,
} from "../../sessions/session-controller.rpc-sources.js";
import { captureAgentJobSession, setGatewayDedupeEntry } from "../agent-turn/agent-job.js";
import { buildAbortedChatSendPayload } from "./chat-abort-authorization.js";
import type { WebchatReplyMediaRequesterContext } from "./chat-reply-media.js";
import { createChatSendLateFollowupDisposition } from "./chat-send-late-followup.js";
import type { PreparedChatSendSession } from "./chat-send-session.js";
import { createChatSendLateReplyFinalizer } from "./chat-send-source-finalization.js";
import type { GatewayRequestContext } from "./types.js";

type TerminalCompletion = Exclude<QueuedFollowupReplyBatch["completion"], { kind: "progress" }>;

export function createChatSendTurnAdoptionLifecycle(params: {
  requesterContext?: WebchatReplyMediaRequesterContext;
  accountId: string | undefined;
  sourceRef: RpcSourceRef;
  context: GatewayRequestContext;
  runId: string;
  controller: AbortController;
  sessionKey: string;
  agentId?: string;
  ownerKey?: string;
  originatingLeafEntryId?: string | null;
  originatingChannel: string;
  session: Pick<
    PreparedChatSendSession,
    "agentId" | "backingSessionId" | "cfg" | "clientRunId" | "sessionKey" | "sessionLoadOptions"
  >;
  hasCronCreatorAuthority: boolean;
  suppressReplies?: boolean;
  releaseSourceWorkAdmission: () => void;
  retainWorkAdmission: () => () => void;
  armOperatorRunCancellation?: () => void;
  retireOperatorRunCancellation?: () => void;
}): {
  lifecycle: TurnAdoptionLifecycle;
  isEnqueued: () => boolean;
  isCompleted: () => boolean;
  onQueueDisposition: (reason: string) => void;
  onQueuedFollowupReplyBatch: QueuedFollowupReplyDelivery;
} {
  let terminalKnown = false;
  let terminalCompletion: TerminalCompletion | undefined;
  let completed = false;
  let settlementRecorded = false;
  let releaseWorkAdmission: (() => void) | undefined;
  const recordQueuedTerminal = (completion: TerminalCompletion) => {
    // Before deferral, the active dispatch owns terminal recording. Once queued,
    // this producer must publish completion after the exact source's delivery.
    if (
      !params.suppressReplies &&
      releaseWorkAdmission === undefined &&
      (completion.kind !== "aborted" ||
        (params.sourceRef.input.claim?.operation !== undefined &&
          !params.sourceRef.input.claim.released))
    ) {
      return;
    }
    const now = Date.now();
    setGatewayDedupeEntry({
      dedupe: params.context.dedupe,
      key: `chat:${params.runId}`,
      session: captureAgentJobSession({
        ...getRpcSourceIdentity(params.sourceRef),
        lifecycleGeneration: getRpcSourceLifecycleGeneration(params.sourceRef),
      }),
      entry: {
        ts: now,
        ok: completion.kind !== "failed",
        payload:
          completion.kind === "aborted"
            ? buildAbortedChatSendPayload({
                runId: params.runId,
                endedAt: now,
                stopReason: resolveAgentRunAbortLifecycleFields(params.controller.signal)
                  .stopReason,
              })
            : completion.kind === "failed"
              ? {
                  runId: params.runId,
                  status: completion.errorKind === "timeout" ? "timeout" : "error",
                  summary: completion.error,
                  stopReason: completion.stopReason,
                }
              : { runId: params.runId, status: "completed", stopReason: completion.stopReason },
      },
    });
  };
  const lateFollowup = createChatSendLateFollowupDisposition({
    runId: params.runId,
    originatingChannel: params.originatingChannel,
    logGateway: params.context.logGateway,
    deliver: params.suppressReplies
      ? async ({ completion }) => {
          terminalKnown ||= completion.kind !== "progress";
          if (completion.kind !== "progress") {
            terminalCompletion = completion;
          }
          return { kind: "dropped" as const, reason: "no-visible-content" as const };
        }
      : createChatSendLateReplyFinalizer({
          requesterContext: params.requesterContext,
          abortSignal: params.controller.signal,
          accountId: params.accountId,
          context: params.context,
          session: params.session,
          onTerminalPublished: (completion) => {
            terminalKnown = true;
            terminalCompletion = completion;
          },
        }),
  });
  const priorCancel = params.sourceRef.adapter.cancel?.bind(params.sourceRef.adapter);
  params.sourceRef.adapter.cancel = (reason) => {
    priorCancel?.(reason);
    recordQueuedTerminal({ kind: "aborted" });
  };
  const lifecycle: TurnAdoptionLifecycle = {
    // Gateway cancel identity only — share collect key via ownerKey.
    admission: "cancel-only",
    abortSignal: params.sourceRef.input.abortSignal,
    ...(params.originatingLeafEntryId !== undefined
      ? { originatingLeafEntryId: params.originatingLeafEntryId }
      : {}),
    ownerKey: params.ownerKey,
    onAdopted: () => {
      params.sourceRef.input.abortSignal.throwIfAborted();
    },
    onDeferred: () => {
      if (params.hasCronCreatorAuthority) {
        lifecycle.cronCreatorAuthorityUnavailable = "queued-local-operator";
      }
      const input = params.sourceRef.input;
      if (input.abortSignal.aborted || input.phase === "consumed") {
        return false;
      }
      // Only physical source-publication custody survives ACK; selection lives on input.
      releaseWorkAdmission ??= params.retainWorkAdmission();
      lateFollowup.recordQueued();
      params.armOperatorRunCancellation?.();
      return true;
    },
    onCancellationRetired: () => {
      retireSessionControllerSourceCancellation(params.sourceRef.input);
      params.retireOperatorRunCancellation?.();
    },
    onAbandoned: () => {
      terminalKnown = true;
    },
    onSettled: () => {
      if (settlementRecorded) {
        return;
      }
      settlementRecorded = true;
      const ownsCompletion = isRpcSourceRegistered(params.sourceRef);
      // Consumed steering also settles custody, but has no terminal batch. Only
      // the exact queued owner can retire an executed or abandoned refresh.
      completed = ownsCompletion && terminalKnown;
      try {
        if (ownsCompletion) {
          params.retireOperatorRunCancellation?.();
        }
        if (completed) {
          recordQueuedTerminal(
            terminalCompletion ??
              (params.controller.signal.aborted ? { kind: "aborted" } : { kind: "completed" }),
          );
        }
      } finally {
        releaseWorkAdmission?.();
        releaseWorkAdmission = undefined;
      }
    },
  };
  bindReplySourceInput(lifecycle, params.sourceRef.input);
  const priorSettled = params.sourceRef.adapter.onSettled?.bind(params.sourceRef.adapter);
  params.sourceRef.adapter.onSettled = async () => {
    try {
      await lifecycle.onSettled?.();
    } finally {
      await priorSettled?.();
    }
  };
  return {
    lifecycle,
    isEnqueued: () => params.sourceRef.input.custody.enqueued === true,
    isCompleted: () => completed,
    onQueueDisposition: (reason) => {
      params.context.logGateway.info("chat queue turn intentionally skipped", {
        runId: params.runId,
        sessionKey: params.sessionKey,
        outcome: "skipped",
        reason,
      });
    },
    onQueuedFollowupReplyBatch: lateFollowup.deliver,
  };
}
