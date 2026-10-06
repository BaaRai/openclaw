import { createHash } from "node:crypto";
import { runAgentHarnessBeforeMessageWriteHook } from "../../../agents/harness/hook-helpers.js";
import { runOutsidePreparedModelRuntimePluginGenerationScope } from "../../../agents/prepared-model-runtime-generation-scope.js";
import { normalizeChatType } from "../../../channels/chat-type.js";
import { channelRouteDedupeKey } from "../../../plugin-sdk/channel-route.js";
import {
  getGatewayRestartDrainSignal,
  isGatewayRestartDrainError,
  waitForGatewayRestartFenceSettlement,
  runWithGatewayDetachedWorkContinuation,
} from "../../../process/gateway-work-admission.js";
import { defaultRuntime } from "../../../runtime.js";
import { deferSessionControllerClaimBeforeExecution } from "../../../sessions/session-controller.mailbox-claim.js";
import {
  detachSessionControllerSources,
  getExistingSessionControllerMailbox,
  sessionControllerMailboxes,
  releaseSessionControllerClaim,
  trackSessionControllerSourceWork,
  type SessionControllerMailbox,
  type SessionControllerMailboxClaim,
} from "../../../sessions/session-controller.mailbox.js";
import { createUserTurnTranscriptRecorder } from "../../../sessions/user-turn-transcript.js";
import { buildCollectPrompt, previewQueueSummaryPrompt } from "../../../utils/queue-helpers.js";
import { resolveCollectedRun } from "./collected-run.js";
import { collectRuntimeMetadata, resolveFollowupReplyAnchor } from "./delivery-context.js";
import {
  buildCollectTranscriptInput,
  collectQueuedPromptMedia,
  createAggregateCancellation,
  createCollectUserTurnTranscriptRecorder,
  renderCollectItem,
  resolveAggregateOwner,
  resolveFollowupTranscriptTarget,
  resolveOriginRoutingMetadata,
} from "./envelope.js";
import {
  admitFollowupRunLifecycle,
  completeFollowupRunLifecycle,
  retireFollowupRunCancellation,
} from "./lifecycle.js";
import { clearFollowupQueue, followupQueueSources } from "./state.js";
import { consumeQueueSummaryDelivery } from "./summary-consumption.js";
import { isFollowupRunAborted, type FollowupRun } from "./types.js";

let followedRestartDrainSignal: AbortSignal | undefined;

// Overflow retries must reuse one transcript identity, while the same provider-local
// message ID on a different channel route remains an independent user turn.
function buildOverflowTranscriptIdempotencyKey(source: FollowupRun, prompt: string): string {
  const promptHash = createHash("sha256").update(prompt).digest("hex");
  const routeHash = createHash("sha256")
    .update(
      JSON.stringify([
        channelRouteDedupeKey({
          channel: source.originatingChannel,
          to: source.originatingTo,
          accountId: source.originatingAccountId,
          threadId: source.originatingThreadId,
        }),
        resolveFollowupReplyAnchor(source) ?? "",
        source.originatingReplyToMode ?? "",
        normalizeChatType(source.originatingChatType) ?? "",
      ]),
    )
    .digest("hex");
  return `followup-overflow:${source.run.sessionId}:${routeHash}:${source.messageId ?? source.enqueuedAt}:${promptHash}`;
}

function bindRestart(): void {
  const signal = getGatewayRestartDrainSignal();
  if (followedRestartDrainSignal === signal) {
    return;
  }
  followedRestartDrainSignal = signal;
  signal.addEventListener(
    "abort",
    () => {
      // Capture owners before cleanup can mutate the registry or publish a successor.
      for (const mailbox of Array.from(sessionControllerMailboxes())) {
        clearFollowupQueue(mailbox.key, mailbox);
      }
    },
    { once: true },
  );
}
/** Executes an already-selected claim. This adapter never chooses a successor. */
async function executeClaim(
  claim: SessionControllerMailboxClaim,
  execute: (run: FollowupRun) => Promise<void>,
): Promise<void> {
  const queue = claim.mailbox;
  const sources = [...claim.sources];
  const source = sources.at(-1);
  if (!source) {
    releaseSessionControllerClaim(claim);
    return;
  }
  const cancellation = createAggregateCancellation(sources);
  const aggregateOwner = resolveAggregateOwner(sources);
  const adopt = async () => {
    for (const item of sources) {
      await admitFollowupRunLifecycle(item);
    }
    cancellation.admit();
    detachSessionControllerSources(sources);
    for (const item of sources) {
      if (item !== aggregateOwner) {
        retireFollowupRunCancellation(item);
      }
    }
  };
  for (const item of sources) {
    if (claim.summary) {
      queue.activeSummarySources.add(item);
    }
  }
  const summaryLines = sources.map((item) => {
    const index = queue.summarySources.indexOf(item);
    if (index >= 0) {
      return queue.summaryLines[index] ?? "";
    }
    for (const elision of queue.summaryElisions) {
      const elisionIndex = elision.sources.indexOf(item);
      if (elisionIndex >= 0) {
        return elision.summaryLines[elisionIndex] ?? "";
      }
    }
    return item.summaryLine ?? item.prompt;
  });
  const prompt = claim.summary
    ? (previewQueueSummaryPrompt({
        state: {
          droppedCount: sources.length,
          summaryLines: queue.cap > 0 ? summaryLines.slice(-queue.cap) : summaryLines,
        },
        noun: "message",
      }) ?? "")
    : buildCollectPrompt({
        title: "[Queued messages while agent was busy]",
        items: sources,
        renderItem: renderCollectItem,
      });
  const settleSources = async (outcome?: "consumed") => {
    for (const item of sources) {
      completeFollowupRunLifecycle(item, outcome);
    }
    await Promise.all(sources.flatMap((item) => item.controllerInput?.custody.settling ?? []));
  };
  let run = source;
  if (claim.summary || sources.length > 1) {
    const recorder = claim.summary
      ? createUserTurnTranscriptRecorder({
          input: {
            text: prompt,
            idempotencyKey: buildOverflowTranscriptIdempotencyKey(source, prompt),
            senderIsOwner: source.run.senderIsOwner,
            provenance: source.run.inputProvenance,
          },
          pendingInputSources: sources.flatMap((item) => item.userTurnTranscriptRecorder ?? []),
          target: () => resolveFollowupTranscriptTarget(source),
          beforeMessageWrite: runAgentHarnessBeforeMessageWriteHook,
          errorContext: "mailbox overflow transcript",
        })
      : createCollectUserTurnTranscriptRecorder(sources);
    run = {
      ...source,
      ...resolveOriginRoutingMetadata(sources),
      ...collectRuntimeMetadata(sources, cancellation.signal),
      ...collectQueuedPromptMedia(sources),
      ...(claim.summary ? { currentInboundContext: undefined } : {}),
      prompt,
      transcriptPrompt: claim.summary ? prompt : buildCollectTranscriptInput(sources).text,
      userTurnTranscriptRecorder: recorder,
      run: resolveCollectedRun(sources, source.run),
      controllerInput: source.controllerInput,
      controllerClaim: claim,
      turnAdoptionLifecycle: {
        admission: "cancel-only",
        cronCreatorAuthorityUnavailable: sources.find(
          (item) => item.turnAdoptionLifecycle?.cronCreatorAuthorityUnavailable,
        )?.turnAdoptionLifecycle?.cronCreatorAuthorityUnavailable,
        onAdopted: adopt,
        onAbandoned: () => settleSources(),
        onSettled: () => settleSources("consumed"),
      },
    };
  }
  let adoptionRefused = false;
  try {
    await execute(run);
  } catch (error) {
    adoptionRefused = sources.some((item) => {
      const custody = item.controllerInput?.custody;
      return (
        custody && !custody.adopted && custody.failure !== undefined && custody.failure === error
      );
    });
    if (adoptionRefused) {
      // Adoption is the last reversible producer boundary before model/tool work.
      // Match the custody owner's exact rejection so later execution errors cannot replay.
      deferSessionControllerClaimBeforeExecution(claim);
    }
    defaultRuntime.error?.("mailbox execution failed for " + queue.key + ": " + String(error));
  } finally {
    // Only the execution producer can return an explicit pre-execution refusal.
    // Generic throws/returns and the operation's broader preparation marker never
    // grant or revoke replayability; the producer-local receipt is authoritative.
    const preparationRefused = claim.retryBeforeExecution === true;
    const retrySources = claim.retryBeforeExecution
      ? sources.filter((item) => {
          try {
            item.operatorAuthority?.assertCurrent();
          } catch {
            return false;
          }
          return (
            !isFollowupRunAborted(item) &&
            !item.controllerInput?.retirementRequested &&
            (!adoptionRefused || !item.controllerInput?.custody.adopted) &&
            !item.controllerInput?.custody.completed
          );
        })
      : [];
    if (!retrySources.length) {
      claim.retryBeforeExecution = false;
    }
    const consumed = sources.filter((item) => !retrySources.includes(item));
    detachSessionControllerSources(consumed);
    if (claim.summary) {
      consumeQueueSummaryDelivery(
        queue,
        { sources: consumed, droppedCount: consumed.length },
        false,
      );
    }
    for (const item of retrySources) {
      item.controllerInput!.payload = claim.summary ? "summary" : "ready";
    }
    if (retrySources.length) {
      queue.lastEnqueuedAt = Date.now();
    }
    for (const item of sources) {
      queue.activeSummarySources.delete(item);
      for (const elision of queue.summaryElisions) {
        const compact = elision.sourceRefs.get(item);
        if (compact) {
          queue.activeSummarySources.delete(compact);
        }
      }
      if (retrySources.includes(item)) {
        continue;
      }
      try {
        completeFollowupRunLifecycle(item, preparationRefused ? undefined : "consumed");
      } catch (error) {
        defaultRuntime.error?.("mailbox custody settlement failed: " + String(error));
      }
    }
    cancellation.dispose();
    releaseSessionControllerClaim(claim);
  }
}
export function rememberFollowupDrainCallback(
  key: string,
  runFollowup: (run: FollowupRun) => Promise<void>,
  captured?: SessionControllerMailbox,
): void {
  const mailbox = captured ?? getExistingSessionControllerMailbox(key);
  if (!mailbox) {
    return;
  }
  bindRestart();
  mailbox.dispatch = async (claim) => {
    for (;;) {
      let entered = false;
      try {
        // The turn owns a fresh async work scope: tracked agent work must keep running
        // after the request that triggered the drain has closed its own scope.
        await runWithGatewayDetachedWorkContinuation(
          () =>
            runOutsidePreparedModelRuntimePluginGenerationScope(() => {
              entered = true;
              return executeClaim(claim, runFollowup);
            }),
          "session:mailbox-turn",
        );
        return;
      } catch (error) {
        // The root owner refused before the execution callback entered. Only a
        // reversible fence receipt permits retry of this same retained claim.
        if (
          !entered &&
          isGatewayRestartDrainError(error) &&
          !getGatewayRestartDrainSignal().aborted
        ) {
          await waitForGatewayRestartFenceSettlement();
          if (!getGatewayRestartDrainSignal().aborted) {
            continue;
          }
        }
        defaultRuntime.error?.("mailbox root admission failed: " + String(error));
        for (const source of claim.sources) {
          completeFollowupRunLifecycle(source);
        }
        detachSessionControllerSources(claim.sources);
        if (claim.summary) {
          consumeQueueSummaryDelivery(
            mailbox,
            { sources: claim.sources, droppedCount: claim.sources.length },
            false,
          );
        }
        releaseSessionControllerClaim(claim);
        return;
      }
    }
  };
}

export function scheduleFollowupDrain(
  key: string,
  runFollowup: (run: FollowupRun) => Promise<void>,
): void {
  const mailbox = getExistingSessionControllerMailbox(key);
  if (!mailbox) {
    return;
  }
  if (!mailbox.dispatch) {
    rememberFollowupDrainCallback(key, runFollowup, mailbox);
  }
  mailbox.dispatchEnabled = true;
  mailbox.wake();
}
export async function dropAbortedFollowups(
  queue: SessionControllerMailbox,
  runFollowup: (run: FollowupRun) => Promise<void>,
): Promise<number> {
  const sources = [...new Set(followupQueueSources(queue))].filter(
    (source) =>
      isFollowupRunAborted(source) &&
      source.controllerInput?.phase !== "injecting" &&
      !queue.inFlight.has(source),
  );
  const pending = new Set(queue.items);
  detachSessionControllerSources(sources);
  consumeQueueSummaryDelivery(queue, { sources, droppedCount: 0 }, false);
  for (const source of sources) {
    const presentation = pending.has(source) ? runFollowup(source) : Promise.resolve();
    if (source.controllerInput) {
      trackSessionControllerSourceWork(source.controllerInput, presentation);
    }
    completeFollowupRunLifecycle(source);
    try {
      await presentation;
    } catch (error) {
      defaultRuntime.error?.("mailbox cancellation presentation failed: " + String(error));
    }
  }
  queue.wake();
  return sources.length;
}
