import { getRuntimeConfig } from "../../../config/config.js";
import { isSystemEventStoreCurrent } from "../../../infra/system-event-ownership.js";
import { getSharedGatewayContextResolver } from "../../../plugins/runtime/gateway-request-scope.js";
import { retireSessionControllerInput } from "../../../sessions/session-controller.mailbox.js";
import { isCronSessionKey } from "../../../sessions/session-key-utils.js";
import {
  type DeliveryContext,
  normalizeDeliveryContext,
} from "../../../utils/delivery-context.shared.js";
import {
  isDeliverableMessageChannel,
  normalizeMessageChannel,
} from "../../../utils/message-channel.js";
import { resolveSubagentRequesterAgentId } from "../../subagent-requester-owner.js";
import {
  getFollowupForCohort,
  withFollowupSuccessor,
} from "../completion/session-followup-completion.js";
import { subagentRuns } from "../registry/subagent-registry-memory.js";
import {
  matchesSubagentRequesterSession,
  selectConnectedSettledSubagentWave,
} from "../registry/subagent-registry-queries.js";
import {
  getLatestLiveSubagentRunByChildSessionKey,
  listSubagentRunsForRequester,
} from "../registry/subagent-registry-read.js";
import type { SubagentRunRecord } from "../registry/subagent-registry.types.js";
import {
  buildRequesterSettleWakeIdentity,
  hasRequesterCompletionCohort,
  isSubagentObligationRetired,
  resolveCurrentRequesterSettleWakeBatch,
} from "../registry/subagent-requester-settle-identity.js";
import { isClaimedByLiveRequesterTurn } from "../registry/subagent-requester-turn-liveness.js";
import { isSameSubagentRunOwner } from "../registry/subagent-run-generation.js";
import { hasSubagentRunEnded } from "../registry/subagent-run-liveness.js";
import { captureRequesterContinuationCaller } from "../requester-cron-authority.js";
import {
  finalizeRequesterFinalAttachment,
  transferRequesterFinalAttachment,
} from "../requester-final-attachment.js";
import { getSubagentDepthFromSessionStore } from "../spawn/subagent-depth.js";
import { reserveSubagentControllerSource } from "./subagent-announce-controller-source.js";
/** Deliver drained requester waves; lifecycle owns their persisted outbox on retained run rows. */
import { hasAnnounceSendEvidence } from "./subagent-announce-delivery-retry.js";
import {
  deliverSubagentAnnouncement,
  loadRequesterSessionEntry,
} from "./subagent-announce-delivery.js";
import type { SubagentAnnounceDeliveryResult } from "./subagent-announce-dispatch.js";
import { resolveAnnounceOrigin } from "./subagent-announce-origin.js";
import { readChildCompletionFindings } from "./subagent-announce-output.js";
import { hasUsableSessionEntry } from "./subagent-announce.js";
import { selectCurrentRequesterCompletionRows } from "./subagent-announce.requester-settle-cohort.js";
import { createRequesterDescendantReader } from "./subagent-announce.requester-settle-descendants.js";
import { buildRequesterSettleWakeMessage } from "./subagent-announce.requester-settle-message.js";
import { createRequesterSettleReceiptAdmission } from "./subagent-announce.requester-settle-receipt.js";
import {
  readSharedBatchState,
  captureRequesterRunOwner,
  resolvePrivateSettlePolicy,
  type RequesterSettleWakeBatchState,
  type RequesterSettleWakeBatchCallbacks,
} from "./subagent-announce.requester-settle-state.js";

/**
 * Arms the one requester continuation a settled batch owes. The continuation is a single
 * controller input on the requester session: the mailbox decides when it runs, and its
 * outcome is recorded once. Nothing here retries, backs off, or waits for the requester.
 */
export async function maybeWakeRequesterAfterAllChildrenSettled(
  params: RequesterSettleWakeBatchCallbacks & {
    requesterSessionKey: string;
    requesterOrigin?: DeliveryContext;
    settledEntry: SubagentRunRecord;
    signal?: AbortSignal;
    isSourceCurrent: () => boolean;
  },
): Promise<boolean> {
  if (params.signal?.aborted || !params.isSourceCurrent()) {
    return false;
  }
  const requesterSessionKey = params.requesterSessionKey.trim();
  const cfg = getRuntimeConfig();
  const requesterAgentId = resolveSubagentRequesterAgentId(cfg, params.settledEntry);
  const requesterStorePath = params.settledEntry.requesterStorePath ?? null;
  const initialState = params.settledEntry.requesterSettleWake;
  const pauseNotice =
    params.settledEntry.pauseReason === "sessions_yield" ? initialState?.pauseNotice : undefined;
  const pause = pauseNotice !== undefined;
  if (!requesterSessionKey || !initialState) {
    return false;
  }
  const readRequesterRuns = () =>
    listSubagentRunsForRequester(requesterSessionKey, { requesterAgentId, requesterStorePath });
  const completeBatch = async (
    batch: readonly SubagentRunRecord[],
    state: RequesterSettleWakeBatchState,
    delivery?: SubagentAnnounceDeliveryResult,
    requesterSessionId?: string,
  ): Promise<void> => {
    await params.completeBatch(batch, state.rearmGeneration, delivery, () =>
      finalizeRequesterFinalAttachment({
        requesterAgentId,
        requesterSessionKey,
        requesterSessionId,
        batchRunIds: batch.map((entry) => entry.runId).toSorted(),
        rearmGeneration: state.rearmGeneration,
        requesterYieldBatch: state.requesterYieldBatch,
        pause,
        delivered: delivery?.delivered,
        finalAssistantVisibleText: delivery?.finalAssistantVisibleText,
      }),
    );
  };
  if (isCronSessionKey(requesterSessionKey)) {
    await completeBatch([params.settledEntry], initialState);
    return false;
  }

  const requesterRuns = readRequesterRuns();
  const currentSettledEntry = requesterRuns.find(
    (entry) => entry.runId === params.settledEntry.runId,
  );
  const currentState = currentSettledEntry?.requesterSettleWake;
  // A requester yield may re-arm this row while runtime loading is in flight.
  // Only the admitted generation may inspect descendants or settle its batch.
  if (
    !currentSettledEntry ||
    !isSameSubagentRunOwner(currentSettledEntry, params.settledEntry) ||
    !currentState ||
    currentState.rearmGeneration !== initialState.rearmGeneration
  ) {
    return false;
  }
  const frozenBatchRunIds = currentState.batchRunIds;
  const frozen = Boolean(frozenBatchRunIds?.length);
  const currentRearmGeneration = currentState.rearmGeneration;
  let settledBatch: SubagentRunRecord[];
  if (pauseNotice) {
    settledBatch = [currentSettledEntry];
  } else if (frozenBatchRunIds && frozen) {
    const runsById = new Map(requesterRuns.map((entry) => [entry.runId, entry]));
    // Retired members no longer hold a wake; every surviving member must be terminal.
    settledBatch = frozenBatchRunIds
      .map((runId) => runsById.get(runId))
      .filter(
        (entry): entry is SubagentRunRecord =>
          entry?.requesterSettleWake?.rearmGeneration === currentRearmGeneration,
      );
    if (
      settledBatch.some(
        (entry) =>
          entry.execution.status === "running" ||
          entry.pauseReason === "sessions_yield" ||
          !hasSubagentRunEnded(entry),
      )
    ) {
      return false;
    }
  } else {
    // An unfrozen wave cannot absorb a different requester-yield generation.
    settledBatch = selectConnectedSettledSubagentWave(
      requesterRuns.filter(
        (entry) =>
          entry.requesterSettleWake &&
          entry.requesterSettleWake.rearmGeneration === currentRearmGeneration &&
          entry.execution.status !== "running" &&
          entry.pauseReason !== "sessions_yield" &&
          hasSubagentRunEnded(entry),
      ),
      currentSettledEntry,
    );
  }
  // A live requester turn owns its bound children until its own transfer releases them.
  if (settledBatch.length === 0 || settledBatch.some(isClaimedByLiveRequesterTurn)) {
    return false;
  }

  const resolveGatewayContext = getSharedGatewayContextResolver(settledBatch);
  const hadGatewayContext = resolveGatewayContext ? Boolean(resolveGatewayContext()) : false;
  if (resolveGatewayContext && !hadGatewayContext) {
    return false;
  }
  const isGatewayClosed = () => {
    try {
      return hadGatewayContext && !resolveGatewayContext?.();
    } catch {
      return hadGatewayContext;
    }
  };
  const batchRunIds = settledBatch.map((entry) => entry.runId).toSorted();
  const batchSessionKeys = [...new Set(settledBatch.map((run) => run.childSessionKey))].toSorted();
  const currentCompletionRows = (rows: SubagentRunRecord[]) =>
    selectCurrentRequesterCompletionRows({
      rows,
      requesterSessionKey,
      requesterAgentId,
      frozenBatch: frozen,
      latestForSession: getLatestLiveSubagentRunByChildSessionKey,
    });
  const readCurrentBatch = () =>
    resolveCurrentRequesterSettleWakeBatch({
      observed: settledBatch,
      currentRuns: currentCompletionRows(readRequesterRuns()),
      rearmGeneration: currentRearmGeneration,
      pause,
      isClaimedByLiveRequesterTurn,
    });
  const isBatchCurrent = () => readCurrentBatch() !== undefined;
  const refreshBatch = (): boolean => {
    const current = readCurrentBatch();
    if (!current) {
      return false;
    }
    settledBatch = current;
    return true;
  };
  const selectedState = readSharedBatchState(settledBatch);

  // Keep the batch members themselves in the settle check, including paused work.
  const readRequesterDescendants = createRequesterDescendantReader({
    requesterSessionKey,
    requesterAgentId,
    requesterStorePath,
    settledEntry: currentSettledEntry,
    settledBefore: Math.min(...settledBatch.map((entry) => entry.createdAt)),
    rootRunIds: frozen ? new Set(frozenBatchRunIds) : undefined,
    signal: params.signal,
    isSourceCurrent: params.isSourceCurrent,
  });
  // Descendant settlement re-arms this wake; an unsettled tree is not yet owed.
  const initialDescendants = await readRequesterDescendants();
  const hasUnsettledDescendants = !pauseNotice && initialDescendants?.unsettled === true;
  if (!initialDescendants || (!frozen && hasUnsettledDescendants)) {
    return false;
  }
  const isStoreCurrent = () =>
    settledBatch.every((entry) =>
      isSystemEventStoreCurrent(requesterSessionKey, entry.requesterStorePath, requesterAgentId),
    );
  const retireReplacedStore = async (): Promise<boolean> => {
    if (isStoreCurrent()) {
      return false;
    }
    await completeBatch(settledBatch, selectedState, {
      delivered: false,
      path: "none",
      error: "store replaced",
      storeReplaced: true,
      disposition: "intentional_non_delivery",
    });
    return true;
  };
  if (await retireReplacedStore()) {
    return false;
  }
  const followup = pauseNotice ? undefined : getFollowupForCohort(settledBatch);
  const getRequesterRun = () =>
    followup
      ? undefined
      : (getLatestLiveSubagentRunByChildSessionKey(
          requesterSessionKey,
          (entry) => entry.pauseReason === "sessions_yield",
          requesterAgentId,
        ) ??
        getLatestLiveSubagentRunByChildSessionKey(
          requesterSessionKey,
          undefined,
          requesterAgentId,
        ));
  const requesterRun = getRequesterRun();
  const isRequesterRunCurrent = captureRequesterRunOwner(requesterRun);
  const isBatchDeliveryClosed = () => {
    const currentRequester = getRequesterRun();
    const currentRuns = readRequesterRuns();
    const currentBatch = settledBatch.map(
      (entry) => currentRuns.find((current) => isSameSubagentRunOwner(current, entry)) ?? entry,
    );
    return (
      requesterRun?.killReconciliation?.suppressTaskDelivery === true ||
      requesterRun?.suppressCompletionDelivery === true ||
      currentRequester?.killReconciliation?.suppressTaskDelivery === true ||
      currentRequester?.suppressCompletionDelivery === true ||
      // A yielded cohort reports the members it still holds; requester-wide cancellation
      // already removed its own. An ordinary wave owes nothing once every member retired.
      (selectedState.requesterYieldBatch !== true &&
        currentBatch.every(isSubagentObligationRetired))
    );
  };
  if (isBatchDeliveryClosed()) {
    // Cancellation already owns the task result; only consume its obsolete wake.
    await completeBatch(settledBatch, selectedState);
    return false;
  }
  // A frozen batch closed by cancellation retires above; otherwise it waits for its tree.
  if (hasUnsettledDescendants) {
    return false;
  }
  const requiredSettled = settledBatch.filter((entry) => entry.expectsCompletionMessage === true);
  // A yielded batch owns a rearm generation even when its child settles later.
  // Otherwise a delivered single child clears the batch before its requester wakes.
  const requesterYieldedAfterDelivery =
    selectedState.afterRequesterYield === true ||
    (selectedState.requesterYieldBatch === true && selectedState.rearmGeneration !== undefined);
  const requesterDepth = getSubagentDepthFromSessionStore(requesterSessionKey, {
    cfg,
    agentId: requesterAgentId,
  });
  // A retained completion cohort owns continuation at every depth.
  // Ordinary nested waves remain owned by the descendant-settle path.
  if (
    !pauseNotice &&
    (requiredSettled.length === 0 ||
      (requiredSettled.length < 2 &&
        !requiredSettled.some((entry) => entry.delivery?.status !== "delivered") &&
        !requesterYieldedAfterDelivery) ||
      (!requesterYieldedAfterDelivery &&
        !hasRequesterCompletionCohort(currentSettledEntry) &&
        requesterDepth >= 1))
  ) {
    await completeBatch(settledBatch, selectedState);
    return false;
  }

  const requester = loadRequesterSessionEntry(requesterSessionKey, requesterAgentId);
  const requesterEntry = requester.entry;
  if (!hasUsableSessionEntry(requesterEntry)) {
    await completeBatch(settledBatch, selectedState, {
      delivered: false,
      path: "none",
      error: "requester session unavailable",
    });
    return false;
  }

  const requesterIdentity = {
    sessionId: requesterEntry.sessionId,
    lifecycleRevision: requesterEntry.lifecycleRevision,
  };
  const completionRows = currentCompletionRows(settledBatch);
  // Delivered children remain in yield cohorts; the admitted marker owns whether
  // the requester may deliver its final under the conversation's reply policy.
  const { privateRows, requireVisibleReply, parentOnly, privateBinding, admissionMarker } =
    resolvePrivateSettlePolicy(
      completionRows,
      requesterYieldedAfterDelivery,
      selectedState,
      requesterIdentity,
    );
  // `/new` keeps the session id but rotates the lifecycle revision, so compare the
  // whole incarnation; a deliverable continuation must not post old findings into a reset session.
  if (privateRows.some((entry) => !matchesSubagentRequesterSession(entry, requesterIdentity))) {
    await completeBatch(settledBatch, selectedState, {
      delivered: false,
      path: "none",
      reason: "completion_handoff_unavailable",
      error: "private completion requester session was replaced",
      terminal: true,
      disposition: "intentional_non_delivery",
    });
    return false;
  }
  const recoveryRows = completionRows.filter((entry) =>
    matchesSubagentRequesterSession(entry, requesterIdentity),
  );
  const preparedFindings = pauseNotice
    ? { text: pauseNotice.acknowledgment, isCurrent: () => true }
    : await readChildCompletionFindings(completionRows);
  if (await retireReplacedStore()) {
    return false;
  }
  if (params.signal?.aborted || !refreshBatch()) {
    return false;
  }
  // Recheck owned descendants after loading findings and before dispatch.
  const currentDescendants = await readRequesterDescendants();
  if (
    !currentDescendants ||
    !params.isSourceCurrent() ||
    !refreshBatch() ||
    (!pauseNotice && currentDescendants.unsettled)
  ) {
    return false;
  }
  const state = { ...readSharedBatchState(settledBatch), ...admissionMarker };
  const { batchKey, runId: directIdempotencyKey } = buildRequesterSettleWakeIdentity({
    requesterSessionKey,
    requesterAgentId,
    batchRunIds,
    rearmGeneration: selectedState.rearmGeneration,
    pause,
  });
  const sourceReceiptAdmission = createRequesterSettleReceiptAdmission({
    requester,
    identity: requesterIdentity,
    storeSessionKey: requesterSessionKey,
    storeAgentId: requesterAgentId,
    storePaths: () => settledBatch.map((entry) => entry.requesterStorePath),
    isRecoveryCurrent: () =>
      recoveryRows.every((entry) => matchesSubagentRequesterSession(entry, requesterIdentity)),
    readCurrent: () => loadRequesterSessionEntry(requesterSessionKey, requesterAgentId).entry,
    isStoreCurrent,
  });
  const isRequesterCurrent = () => {
    if (!sourceReceiptAdmission.isRequesterCurrent()) {
      return false;
    }
    if (followup) {
      try {
        followup.assertCurrent();
        return true;
      } catch {
        return false;
      }
    }
    return isRequesterRunCurrent(getRequesterRun(), directIdempotencyKey);
  };
  const isSourceSessionEffectsAllowed = () =>
    !params.signal?.aborted &&
    params.isSourceCurrent() &&
    sourceReceiptAdmission.isStoreCurrent() &&
    preparedFindings.isCurrent() &&
    !isGatewayClosed() &&
    isBatchCurrent() &&
    isRequesterCurrent() &&
    !isBatchDeliveryClosed();
  const continuationCaller = captureRequesterContinuationCaller({
    requesterSessionKey,
    requesterSessionId: requesterIdentity.sessionId,
    requesterAgentId,
    batch: settledBatch,
    rearmGeneration: state.requesterYieldBatch ? state.rearmGeneration : undefined,
    runId: directIdempotencyKey,
    isCurrent: isSourceSessionEffectsAllowed,
    deliveryRoute: normalizeDeliveryContext(params.requesterOrigin),
  });
  const requesterSessionOrigin = continuationCaller.deliveryRoute;
  const directOrigin = resolveAnnounceOrigin(requesterEntry, requesterSessionOrigin);
  const completionChannel = normalizeMessageChannel(directOrigin?.channel);
  const wakeMessage = buildRequesterSettleWakeMessage({
    findings: preparedFindings.text,
    requireVisibleReply,
    parentOnly,
    yieldedFinalDeliverable: admissionMarker.yieldedFinalDeliverable,
    children: completionRows,
    recoveryChildren: recoveryRows,
    preserveModelRouteNotice: !completionChannel || !isDeliverableMessageChannel(completionChannel),
  });
  // The stable reservation is this obligation's one mailbox input, including a restart reservation.
  const controllerInput = reserveSubagentControllerSource(
    settledBatch[0]!,
    batchKey,
    directIdempotencyKey,
    continuationCaller,
  );
  const retireUnadoptedInput = () => {
    if (controllerInput && !controllerInput.custody.rpcAdopted) {
      retireSessionControllerInput(controllerInput);
    }
  };
  // Revocation or retirement owns a changed batch: record nothing, or consume the obsolete wake.
  // A stopped evaluation records nothing; the durable wake stays owed.
  const settleRevokedBatch = async (): Promise<boolean> => {
    if (
      params.signal?.aborted ||
      isGatewayClosed() ||
      !isBatchCurrent() ||
      (await retireReplacedStore())
    ) {
      return true;
    }
    if (isBatchDeliveryClosed() || !isRequesterCurrent()) {
      await completeBatch(settledBatch, state);
      return true;
    }
    return false;
  };
  if (
    isSourceSessionEffectsAllowed() &&
    !pauseNotice &&
    requesterAgentId &&
    state.requesterYieldBatch &&
    state.rearmGeneration !== undefined
  ) {
    transferRequesterFinalAttachment({
      requesterAgentId,
      requesterSessionKey,
      requesterSessionId: requesterEntry.sessionId,
      batchRunIds,
      rearmGeneration: state.rearmGeneration,
      requesterTurnRunId: directIdempotencyKey,
    });
  }
  let delivery: Awaited<ReturnType<typeof deliverSubagentAnnouncement>>;
  try {
    const dispatch = () =>
      subagentRuns.runWithCompletionBatchAuthority(settledBatch, () =>
        deliverSubagentAnnouncement({
          requesterSessionKey,
          requesterAgentId,
          triggerMessage: wakeMessage,
          requesterSessionOrigin,
          directOrigin,
          sourceSessionKey: batchSessionKeys[0],
          settleWakeSourceSessionKeys: batchSessionKeys,
          sourceTool: "subagent_settle",
          targetRequesterSessionKey: requesterSessionKey,
          requesterIsSubagent: requesterDepth >= 1,
          expectsCompletionMessage: false,
          requireDirectDelivery: true,
          ...privateBinding,
          ...(!pauseNotice && requireVisibleReply ? { requireVisibleReply } : {}),
          directIdempotencyKey,
          signal: params.signal,
          resolveGatewayContext,
          isSourceSessionEffectsAllowed,
          sourceReceiptAdmission,
          controllerInput,
        }),
      );
    delivery = followup
      ? await withFollowupSuccessor(
          followup.successor(settledBatch, directIdempotencyKey, () => {
            if (!isSourceSessionEffectsAllowed()) {
              throw new Error("Followup completion cohort changed.");
            }
          }),
          dispatch,
        )
      : await dispatch();
  } catch (error) {
    retireUnadoptedInput();
    if (await settleRevokedBatch()) {
      return false;
    }
    // A transport exception can follow admission; it is recorded once and never replayed.
    await completeBatch(settledBatch, state, {
      delivered: false,
      path: "none",
      disposition: hasAnnounceSendEvidence(error) ? "ambiguous" : "permanent_failure",
      error: error instanceof Error ? error.message : String(error),
    });
    return false;
  }
  if (delivery.disposition !== "session_queued") {
    retireUnadoptedInput();
  }
  if (delivery.delivered) {
    await completeBatch(settledBatch, state, delivery, requesterEntry.sessionId);
    return true;
  }
  // The durable session queue owns a queued handoff and settles it itself.
  if ((await settleRevokedBatch()) || delivery.disposition === "session_queued") {
    return false;
  }
  await completeBatch(settledBatch, state, delivery, requesterEntry.sessionId);
  return false;
}
