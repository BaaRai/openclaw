import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { ErrorCodes, type AgentWaitParams } from "../../../packages/gateway-protocol/src/index.js";
import type { MainSessionRecoveryOwnerLease } from "../../agents/main-session-recovery/main-session-recovery-store.js";
import { mergeSessionEntry, type SessionEntry } from "../../config/sessions.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { getAgentEventLifecycleGeneration } from "../../infra/agent-events.js";
import type { SessionControllerInput } from "../../sessions/session-controller.mailbox.js";
import { isRpcSourceRegistered } from "../../sessions/session-controller.rpc-sources.js";
import { createDeferredCore } from "../../shared/deferred.js";
import { normalizeDeliveryContext } from "../../utils/delivery-context.shared.js";
import { registerChatAbortController } from "../chat-abort.js";
import type { OffloadedRef } from "../chat-attachments.js";
import { errorShapeFromError } from "../error-shape.js";
import { readInProcessSubagentResume } from "../in-process-subagent-resume.js";
import { createCronContinuationController } from "../server-methods/agent-cron-continuation.js";
import { runAgentResetPhase } from "../server-methods/agent-reset-phase.js";
import { createAgentSessionPatchBuilder } from "../server-methods/agent-session-patch.js";
import { prepareAgentSession } from "../server-methods/agent-session-prepare.js";
import type { GatewayRequestHandlerOptions, RespondFn } from "../server-methods/shared-types.js";
import { resolveAgentRunSessionCreation } from "../session-creation-provenance.js";
import { prepareSkillLibrarySessionCreation } from "../skill-library-session.js";
import { createAgentAdmissionController } from "./agent-admission-controller.js";
import { prepareAgentContentPhase } from "./agent-content-phase.js";
import { createAgentDedupeLifecycle } from "./agent-dedupe-lifecycle.js";
import { replayAgentTurnIfCached } from "./agent-dedupe.js";
import { resolveAgentDeliveryPhase } from "./agent-delivery-phase.js";
import type { RestoredCronContinuation } from "./agent-handler-helpers.js";
import type { AgentRequestPreflight } from "./agent-request-preflight.js";
import { prepareAgentRequestRouting } from "./agent-request-routing.js";
import { prepareAgentRunDispatch } from "./agent-run-admission-phase.js";
import { startAgentRunExecution } from "./agent-run-execution-phase.js";
import { persistAgentSessionPhase } from "./agent-session-persist.js";
import { finishAgentTurnPreparation } from "./agent-turn-admission-cleanup.js";
import {
  authorizeAgentTurnSession,
  registerAgentTurnRunAbort,
  registerAgentTurnSourceAdmission,
} from "./agent-turn-source-admission.js";
import { prepareAgentWaitForTurn } from "./agent-wait.js";
import type { RequesterSettleWakeReplay } from "./internal-facade.types.js";
import type { AgentTurnIo, AgentTurnPrincipal } from "./types.js";

type AgentTurnStartRequest = {
  controllerInput?: SessionControllerInput;
  privateCompletion?: true;
  settleWakeReplay?: RequesterSettleWakeReplay;
  assertAdmissionCurrent?: () => void;
  assertInputCommitAllowed?: () => void;
  hasCurrentClientAuthority?: () => boolean;
  preflight: AgentRequestPreflight;
  principal: AgentTurnPrincipal | null;
  io: AgentTurnIo;
  onRunObserved?: (runId: string) => void;
};

export function createAgentTurnService(
  { context, isWebchatConnect }: Pick<GatewayRequestHandlerOptions, "context" | "isWebchatConnect">,
  assertContextCurrent?: () => void,
) {
  const startTurn = async ({
    controllerInput,
    privateCompletion,
    settleWakeReplay,
    assertAdmissionCurrent,
    assertInputCommitAllowed,
    hasCurrentClientAuthority,
    preflight,
    principal,
    io,
    onRunObserved,
  }: AgentTurnStartRequest): Promise<void> => {
    const promptedAt = Date.now();
    assertAdmissionCurrent?.();
    // Durable input decides replay for private continuations and trusted parent
    // resumes after their prior source retires, including pre-admission Stop.
    const reconcileDurableInput =
      privateCompletion === true || readInProcessSubagentResume(principal?.internal) !== undefined;
    if (replayAgentTurnIfCached({ preflight, context, io, acceptedOnly: reconcileDurableInput })) {
      return;
    }
    assertInputCommitAllowed?.();
    const respond: RespondFn = (ok, payload, error, meta) =>
      io.emitAcceptance([ok, payload, error], meta);
    const {
      request,
      cfg,
      runId,
      allowModelOverride,
      canUseInternalRuntimeHandoff,
      canUseCronRunContinuation,
      expectedSession,
      expectedExistingSessionId,
      providerOverride,
      modelOverride,
      execApprovalFollowupApprovalId,
      normalizedSpawned,
      inputProvenance,
      isRestartRecoveryResumeRun,
      preserveUserFacingSessionModelState,
      sessionEffects,
      suppressVisibleSessionEffects,
      requestedPromptPersistenceSuppression,
      isOneShotModelRun,
      isRawModelRun,
      agentDedupeKeys,
      swarmExecutionLane,
    } = preflight;
    // Cached replay returns before a new lifecycle generation is observed, matching
    // the idempotency path that preceded this service extraction.
    const lifecycleGeneration = getAgentEventLifecycleGeneration();
    let resolvedGroupId: string | undefined = normalizedSpawned.groupId;
    let resolvedGroupChannel: string | undefined = normalizedSpawned.groupChannel;
    let resolvedGroupSpace: string | undefined = normalizedSpawned.groupSpace;
    let spawnedByValue: string | undefined;
    const ownerConnId = typeof principal?.connId === "string" ? principal.connId : undefined;
    const ownerDeviceId =
      typeof principal?.connect?.device?.id === "string" ? principal.connect.device.id : undefined;
    const dedupeLifecycle = createAgentDedupeLifecycle({
      reconcileDurableInput,
      inputProvenance,
      cfg,
      request,
      runId,
      lifecycleGeneration,
      agentDedupeKeys,
      suppressVisibleSessionEffects,
      ownerConnId,
      ownerDeviceId,
      context,
      io,
    });
    const routing = await prepareAgentRequestRouting({
      request,
      cfg,
      expectedSession,
      isRawModelRun,
      execApprovalFollowupApprovalId,
      runId,
      agentDedupeKeys,
      context,
      respond,
      reserveDedupe: dedupeLifecycle.reserve,
      bindDedupeSessionTarget: dedupeLifecycle.bindSessionTarget,
      clearDedupe: dedupeLifecycle.clearUnaccepted,
    });
    if (!routing) {
      return;
    }
    const {
      normalizedAttachments,
      requestedBestEffortDeliver,
      knownAgents,
      requestedSessionId,
      requestedToRaw,
      sessionKeyFromTo,
      requestedSessionKeyRaw,
      explicitRecipientSession,
      preAcceptedReservedSessionKey,
      preAttachmentSession,
    } = routing;
    // The source owns preparation and replay publication even before a turn claim exists.
    const sourceWork = createDeferredCore();
    let sourcePreparationComplete = false;
    let earlyRunAbort: ReturnType<typeof registerChatAbortController> | undefined;
    const assertRequestCurrent = () => {
      assertAdmissionCurrent?.();
      earlyRunAbort?.controller.signal.throwIfAborted();
      dedupeLifecycle.assertReservationCurrent();
      assertInputCommitAllowed?.();
      if (earlyRunAbort?.entry && !isRpcSourceRegistered(earlyRunAbort.entry)) {
        throw new Error("Agent request no longer owns its RPC source");
      }
    };
    const runAbortSource = {
      preflight,
      io,
      sourceWork: sourceWork.promise,
      lifecycleGeneration,
      ownerConnId,
      ownerDeviceId,
      assertAdmissionCurrent,
      isSourcePreparationComplete: () => sourcePreparationComplete,
      onCancelled: dedupeLifecycle.cancelOwnedReservation,
      controllerInput,
    };
    let agentId = routing.agentId;
    let requestedSessionKey = routing.requestedSessionKey;
    let gatewayAdmissionTransferred = false;
    let preparedOffloadedRefs: OffloadedRef[] = [];
    let mainRestartRecoveryOwnerLease: MainSessionRecoveryOwnerLease | undefined;
    let releaseGatewayAdmission = () => {};
    const cronContinuation = createCronContinuationController({
      runId,
      lifecycleGeneration,
      context,
    });
    try {
      assertAdmissionCurrent?.();
      const content = await prepareAgentContentPhase({
        assertAdmissionCurrent: assertRequestCurrent,
        request,
        cfg,
        context,
        respond,
        isRawModelRun,
        inputProvenance,
        normalizedAttachments,
        requestedSessionKeyRaw,
        requestedSessionKey,
        requestedSessionId,
        requestedToRaw,
        sessionKeyFromTo,
        agentId,
        providerOverride,
        modelOverride,
        explicitRecipientSession,
        knownAgents,
        onTargetResolved: (target) =>
          registerAgentTurnSourceAdmission({
            ...target,
            ...runAbortSource,
            principal,
            assertRequestCurrent,
            onRegistered: (registration) => {
              earlyRunAbort = registration;
            },
          }),
      }).catch(dedupeLifecycle.handlePreparationFailure(assertAdmissionCurrent));
      if (!content) {
        return;
      }
      preparedOffloadedRefs = content.offloadedRefs;
      assertAdmissionCurrent?.();
      agentId = content.agentId;
      requestedSessionKey = content.requestedSessionKey;
      // Participation is authorized below against the canonical session the run
      // actually targets (see prepareAgentSession). A keyless request resolves its
      // default/effective session there, so authorizing only an explicit key here
      // would let a non-member drive a restricted default session.
      let effectiveTranscriptInputText = content.effectiveTranscriptInputText;
      let message = content.message;
      const {
        images,
        imageOrder,
        media,
        offloadedRefs,
        replyTo,
        recipientChannel,
        recipientAccountId,
        recipientThreadId,
        to,
      } = content;
      let resolvedSessionId = requestedSessionId;
      let sessionEntry: SessionEntry | undefined;
      let effectiveBootstrapContextRunKind = request.bootstrapContextRunKind;
      let restoredCronContinuation: RestoredCronContinuation | undefined;
      let restoredCronContinuationIdentity:
        | Pick<RestoredCronContinuation, "lifecycleRevision" | "sessionId">
        | undefined;
      let sessionPersistedBeforeGatewayAdmission = false;
      let bestEffortDeliver = requestedBestEffortDeliver ?? false;
      let cfgForAgent: OpenClawConfig | undefined;
      let resolvedSessionKey = requestedSessionKey;
      let resolvedSessionAgentId: string | undefined;
      let isNewSession = false;
      let supersededSessionId: string | undefined;
      let skipAgentInitialSessionTouch = false;
      let pendingChatRun: { sessionKey: string; agentId?: string } | undefined;
      let admittedSessionId = resolvedSessionId ?? runId;
      const admissionController = createAgentAdmissionController({
        assertAdmissionCurrent,
        runId,
        lifecycleGeneration,
        agentDedupeKeys,
        preAcceptedReservedSessionKey,
        expectedSession,
        context,
        io,
        dedupeLifecycle,
        getRequestedSessionKey: () => requestedSessionKey,
        getResolvedSessionKey: () => resolvedSessionKey,
        getResolvedSessionId: () => resolvedSessionId,
        getResolvedSessionAgentId: () => resolvedSessionAgentId,
        getAgentId: () => agentId,
        getSessionPersisted: () => sessionPersistedBeforeGatewayAdmission,
        getSupersededSessionId: () => supersededSessionId,
        setAdmittedSessionId: (sessionId) => {
          admittedSessionId = sessionId;
        },
      });
      if (earlyRunAbort) {
        admissionController.setAdmittedRunAbort(earlyRunAbort);
      }
      releaseGatewayAdmission = admissionController.release;
      const resetPhase = await runAgentResetPhase({
        assertAdmissionCurrent: assertRequestCurrent,
        request,
        cfg,
        requestedSessionKey,
        resolvedSessionId,
        effectiveTranscriptInputText,
        message,
        agentId,
        sessionKeyFromTo,
        lifecycleGeneration,
        runId,
        agentDedupeKeys,
        client: principal,
        context,
        respond,
        abortForLifecycleRotation: dedupeLifecycle.abortForLifecycleRotation,
        setCommittedResetCompletion: dedupeLifecycle.setCommittedResetCompletion,
      });
      requestedSessionKey = resetPhase.requestedSessionKey;
      resolvedSessionId = resetPhase.resolvedSessionId;
      effectiveTranscriptInputText = resetPhase.effectiveTranscriptInputText;
      message = resetPhase.message;
      if (resetPhase.accepted) {
        dedupeLifecycle.markAccepted(true);
      }
      if (resetPhase.stop) {
        return;
      }

      if (requestedSessionKey) {
        const preparedSession = prepareAgentSession({
          cfg,
          requestedSessionKey,
          requestedSessionId,
          expectedExistingSessionId,
          agentId,
          recipientChannel,
          request,
          canUseCronRunContinuation,
          lifecycleGeneration,
          effectiveBootstrapContextRunKind,
          preAttachmentSession,
          respond,
        });
        if (!preparedSession) {
          return;
        }
        const {
          cfg: cfgLocal,
          storePath,
          entry,
          canonicalKey: canonicalSessionKey,
          storeKeys,
          maintenanceConfig: sessionMaintenanceConfig,
          canonicalSessionAgentId: sessionAgentId,
          mainSessionKey,
          sessionId,
          touchInteraction,
        } = preparedSession;
        cfgForAgent = cfgLocal;
        // Authorize the canonical session the run will actually target — covering
        // keyless requests whose default/effective session is resolved only here —
        // before any run side effects (admission, dispatch).
        const sessionAuthorizationError = authorizeAgentTurnSession({
          cfg: cfgLocal,
          principal,
          sessionKey: canonicalSessionKey,
          agentId: sessionAgentId,
        });
        if (sessionAuthorizationError) {
          io.emitAcceptance([false, undefined, sessionAuthorizationError]);
          return;
        }
        effectiveBootstrapContextRunKind = preparedSession.effectiveBootstrapContextRunKind;
        restoredCronContinuationIdentity = preparedSession.restoredCronContinuationIdentity;
        sessionPersistedBeforeGatewayAdmission =
          preparedSession.sessionPersistedBeforeGatewayAdmission;
        isNewSession = preparedSession.isNewSession;
        const requestDeliveryHint = normalizeDeliveryContext({
          channel: recipientChannel?.trim(),
          to,
          accountId: recipientAccountId?.trim(),
          // Pass threadId directly — normalizeDeliveryContext handles both
          // string and numeric threadIds (e.g., Matrix uses integers).
          threadId: recipientThreadId,
        });
        const buildSessionPatch = createAgentSessionPatchBuilder({
          session: preparedSession,
          normalizedSpawned,
          requestDeliveryHint,
          getRequestLabel: () => request.label,
          explicitSessionKey: normalizeOptionalString(request.sessionKey),
          getPluginOwnerId: () =>
            normalizeOptionalString(principal?.internal?.pluginRuntimeOwnerId),
          expectedExistingSessionId,
          hasRestoredCronContinuation: restoredCronContinuationIdentity !== undefined,
          requestedSessionId,
        });
        const patchBuild = buildSessionPatch(entry);
        isNewSession = patchBuild.isNewSession;
        sessionEntry = mergeSessionEntry(entry, patchBuild.patch);
        resolvedSessionId = sessionEntry?.sessionId ?? sessionId;
        admittedSessionId = resolvedSessionId ?? runId;
        resolvedSessionKey = canonicalSessionKey;
        resolvedSessionAgentId = sessionAgentId;
        if (!admissionController.getAdmittedRunAbort()) {
          registerAgentTurnRunAbort(
            {
              ...runAbortSource,
              onRegistered: (registration) => {
                earlyRunAbort = registration;
                admissionController.setAdmittedRunAbort(registration);
              },
            },
            {
              cfg: cfgLocal,
              storeScope: storePath,
              sessionKey: canonicalSessionKey,
              aliases: [requestedSessionKey],
              agentId: sessionAgentId,
              sessionId: resolvedSessionId,
              incarnation: entry?.sessionId,
            },
          );
        }
        try {
          await admissionController.acquire(storePath);
        } catch (err) {
          io.emitAcceptance([
            false,
            undefined,
            errorShapeFromError(ErrorCodes.INVALID_REQUEST, err),
          ]);
          return;
        }
        if (admissionController.respondToOutcome()) {
          return;
        }
        const persistedSession = await persistAgentSessionPhase({
          onSessionCommitted: (committedEntry) =>
            dedupeLifecycle.bindSessionTarget({
              sessionKey: canonicalSessionKey,
              agentId: sessionAgentId,
              sessionId: committedEntry.sessionId,
            }),
          assertAdmissionCurrent: assertRequestCurrent,
          request,
          cfg: cfgLocal,
          storePath,
          storeKeys,
          entry,
          canonicalSessionKey,
          sessionAgentId,
          mainSessionKey,
          creation: await prepareSkillLibrarySessionCreation(
            principal,
            () => context.getRuntimeConfig(),
            resolveAgentRunSessionCreation(principal),
          ),
          ...(principal?.authenticatedUserProfile
            ? { requestingOperatorProfileId: principal.authenticatedUserProfile.profileId }
            : {}),
          ...(principal?.internal?.operatorRoleActor
            ? { operatorRoleActor: principal.internal.operatorRoleActor }
            : {}),
          lifecycleGeneration,
          isRestartRecoveryResumeRun,
          runId,
          agentId,
          suppressVisibleSessionEffects,
          restoredCronContinuationIdentity,
          initialPatchBuild: patchBuild,
          buildSessionPatch,
          initialSessionEntry: sessionEntry,
          initialResolvedSessionId: resolvedSessionId,
          initialSessionPersistedBeforeGatewayAdmission: sessionPersistedBeforeGatewayAdmission,
          initialSupersededSessionId: supersededSessionId,
          touchInteraction,
          requestedBestEffortDeliver,
          bestEffortDeliver,
          expectedSession,
          maintenanceConfig: sessionMaintenanceConfig,
          abortForLifecycleRotation: dedupeLifecycle.abortForLifecycleRotation,
          assertGatewayWorkAdmissionAllowed: admissionController.assertAllowed,
          respondToGatewayAdmissionOutcome: admissionController.respondToOutcome,
          updateAdmissionState: (state) => {
            resolvedSessionId = state.resolvedSessionId;
            admittedSessionId = state.admittedSessionId;
            supersededSessionId = state.supersededSessionId;
            sessionPersistedBeforeGatewayAdmission = state.sessionPersistedBeforeGatewayAdmission;
          },
          getAdmittedSessionId: () => admittedSessionId,
          setCronContinuationClaim: cronContinuation.setClaim,
          setMainRestartRecoveryOwnerLease: (lease) => {
            mainRestartRecoveryOwnerLease = lease;
          },
          respond,
        });
        if (!persistedSession) {
          return;
        }
        sessionEntry = persistedSession.sessionEntry;
        resolvedSessionId = persistedSession.resolvedSessionId;
        sessionPersistedBeforeGatewayAdmission =
          persistedSession.sessionPersistedBeforeGatewayAdmission;
        supersededSessionId = persistedSession.supersededSessionId;
        admittedSessionId = persistedSession.admittedSessionId;
        skipAgentInitialSessionTouch = persistedSession.skipAgentInitialSessionTouch;
        isNewSession = persistedSession.isNewSession;
        spawnedByValue = persistedSession.spawnedBy;
        resolvedGroupId = persistedSession.groupId;
        resolvedGroupChannel = persistedSession.groupChannel;
        resolvedGroupSpace = persistedSession.groupSpace;
        pendingChatRun = persistedSession.pendingChatRun;
        bestEffortDeliver = persistedSession.bestEffortDeliver;
        restoredCronContinuation = persistedSession.restoredCronContinuation;
      }

      const delivery = await resolveAgentDeliveryPhase({
        request,
        cfg,
        cfgForAgent,
        sessionEntry,
        resolvedSessionKey,
        resolvedSessionAgentId,
        agentId,
        replyTo,
        to,
        recipientChannel,
        recipientAccountId,
        recipientThreadId,
        bestEffortDeliver,
        runId,
        client: principal,
        context,
        respond,
        isWebchatConnect,
        onRunObserved,
      });
      if (!delivery) {
        return;
      }
      const { activeSessionAgentId } = delivery;

      const preparedDispatch = await prepareAgentRunDispatch({
        sourceWork: sourceWork.promise,
        assertAdmissionCurrent: assertRequestCurrent,
        hasCurrentClientAuthority,
        promptedAt,
        request,
        cfg,
        cfgForAgent,
        sessionEntry,
        resolvedSessionKey,
        requestedSessionKeyRaw,
        requestedSessionKey,
        preAcceptedReservedSessionKey,
        activeSessionAgentId,
        delivery,
        restoredCronContinuationIdentity,
        restoredCronContinuation,
        providerOverride,
        modelOverride,
        allowModelOverride,
        lifecycleGeneration,
        getAdmittedSessionId: () => admittedSessionId,
        ownerConnId,
        ownerDeviceId,
        suppressVisibleSessionEffects,
        pendingChatRun,
        inputProvenance,
        isOneShotModelRun,
        isRestartRecoveryResumeRun,
        canUseInternalRuntimeHandoff,
        execApprovalFollowupApprovalId,
        message,
        effectiveTranscriptInputText,
        images,
        offloadedRefs,
        onUserTurnMediaPersisted: () => {
          preparedOffloadedRefs = [];
        },
        requestedPromptPersistenceSuppression,
        privateCompletion,
        settleWakeReplay,
        runId,
        agentDedupeKeys,
        context,
        client: principal,
        io,
        abortForLifecycleRotation: dedupeLifecycle.abortForLifecycleRotation,
        acquireGatewayWorkAdmission: admissionController.acquire,
        assertGatewayWorkAdmissionAllowed: admissionController.assertAllowed,
        hasGatewayAdmissionOutcome: admissionController.hasOutcome,
        respondToGatewayAdmissionOutcome: admissionController.respondToOutcome,
        admissionAgentId: admissionController.admissionAgentId,
        getGatewayWorkAdmission: admissionController.getAdmission,
        setAdmittedRunAbort: admissionController.setAdmittedRunAbort,
        getAdmittedRunAbort: admissionController.getAdmittedRunAbort,
        markAgentRunAccepted: (accepted) => {
          if (accepted) {
            sourcePreparationComplete = true;
          }
          dedupeLifecycle.markAccepted(accepted);
        },
        getOwnedAgentDedupeKeys: dedupeLifecycle.ownedReservationKeys,
      });
      if (!preparedDispatch) {
        return;
      }
      resolvedSessionId = admittedSessionId;
      // The prepared dispatch now owns either transcript-persisted media or its
      // closed unpersisted ref set; admission must not retain a second owner.
      preparedOffloadedRefs = [];
      sourcePreparationComplete = true;
      // Retain the original command and cleanup after the caller receives acceptance.
      void context
        .trackExecution(() =>
          startAgentRunExecution({
            assertContextCurrent,
            prepared: preparedDispatch,
            mainRestartRecoveryOwnerLease,
            request,
            cfg,
            cfgForAgent,
            sessionEntry,
            resolvedSessionKey,
            requestedSessionKey,
            resolvedSessionId,
            agentId,
            activeSessionAgentId,
            delivery,
            isNewSession,
            isRawModelRun,
            isOneShotModelRun,
            isRestartRecoveryResumeRun,
            suppressVisibleSessionEffects,
            images,
            imageOrder,
            media,
            inputProvenance: preparedDispatch.userTurn.inputProvenance,
            runId,
            agentDedupeKeys,
            swarmExecutionLane,
            spawnedBy: spawnedByValue,
            groupId: resolvedGroupId,
            groupChannel: resolvedGroupChannel,
            groupSpace: resolvedGroupSpace,
            bestEffortDeliver,
            lifecycleGeneration,
            effectiveBootstrapContextRunKind,
            preserveUserFacingSessionModelState,
            sessionEffects,
            skipAgentInitialSessionTouch,
            restoredCronContinuation,
            canUseInternalRuntimeHandoff,
            client: principal,
            context,
            io,
            releaseCronContinuationClaimWithRecovery: cronContinuation.releaseWithRecovery,
          }),
        )
        .catch((error: unknown) => {
          preparedDispatch.releaseCallerAuthority?.();
          context.logGateway.warn(`agent execution cleanup failed: ${String(error)}`);
        });
      sourceWork.resolve();
      gatewayAdmissionTransferred = true;
      mainRestartRecoveryOwnerLease = undefined;
    } finally {
      await finishAgentTurnPreparation({
        transferred: gatewayAdmissionTransferred,
        lease: mainRestartRecoveryOwnerLease,
        cleanupRunAbort: () => earlyRunAbort?.cleanup(),
        releaseGatewayAdmission,
        releaseCronContinuation: cronContinuation.releaseWithRecovery,
        getOffloadedRefs: () => preparedOffloadedRefs,
        clearUnaccepted: dedupeLifecycle.clearUnaccepted,
        settleSourceWork: () => sourceWork.resolve(),
      });
    }
  };

  const prepareWaitForTurn = (params: AgentWaitParams) =>
    prepareAgentWaitForTurn(context, params, { exactTurnSource: true });
  const waitForTurn = async (params: AgentWaitParams) => await prepareWaitForTurn(params).wait();

  return { startTurn, prepareWaitForTurn, waitForTurn };
}
