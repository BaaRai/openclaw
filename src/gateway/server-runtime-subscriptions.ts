// Gateway event subscription wiring for agent, heartbeat, transcript, and lifecycle broadcasts.
import { isDefinitiveRunLifecycle } from "../agents/agent-run-terminal-outcome.js";
import {
  isAuditLedgerEnabled,
  isExecutionIdentityCollectionEnabled,
  resolveAuditMessageMode,
} from "../audit/audit-config.js";
import { createAuditEventRecorder } from "../audit/audit-recorder.js";
import { configureExecutionDecisionWorkSink } from "../audit/execution-decision-work.js";
import { configureExecutionIdentityAdmissionSink } from "../audit/execution-identity-admission.js";
import { configureMessageActionDecisionSink } from "../audit/message-action-decision.js";
import { onTrustedMessageAuditEvent } from "../audit/message-audit-events.js";
import { configureRuntimeActionDecisionSink } from "../audit/runtime-action-decision.js";
import { createChannelAdmissionAudit } from "../channels/message-access/admission-evidence.js";
import { getRuntimeConfig } from "../config/io.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import {
  type AgentEventRuntimePayload,
  onAgentAuditEvent,
  onAgentRuntimeEvent,
} from "../infra/agent-events.js";
import { clearAgentRunContext, getAgentRunContext } from "../infra/agent-run-registry.js";
import { captureAgentRunTerminalWriteContext } from "../infra/agent-run-terminal-writes.js";
import { onTrustedToolExecutionEvent } from "../infra/diagnostic-events.js";
import type { GatewayScheduler } from "../infra/gateway-scheduler.js";
import { onHeartbeatEvent } from "../infra/heartbeat-events.js";
import type { SubsystemLogger } from "../logging/subsystem.js";
import {
  onGatewaySuspendAdmissionChange,
  runWithRetainedGatewayRootWork,
} from "../process/gateway-work-admission.js";
import {
  getRpcSource,
  getRpcSourceIdentity,
  getRpcSourceLifecycleGeneration,
  hasRpcSourceForController,
  isRpcSourceRegistered,
  retireRpcSource,
  setRpcSourceProjectSessionActive,
  type RpcSourceRef,
} from "../sessions/session-controller.rpc-sources.js";
import {
  onSessionIdentityMutation,
  onSessionLifecycleEvent,
} from "../sessions/session-lifecycle-events.js";
import { onInternalSessionTranscriptUpdate } from "../sessions/transcript-events.js";
import {
  createLazyPromise,
  createLazyPromiseLoader,
  createLazyRuntimeSurface,
} from "../shared/lazy-runtime.js";
import { onUserProfilesChanged } from "../state/user-profile-events.js";
import {
  bindChatAbortTerminalDispatch,
  markChatAbortTerminalPersistenceError,
  type ChatAbortTerminalDispatch,
} from "./chat-abort-lifecycle-internal.js";
import type { RestartRecoveryCandidate } from "./chat-abort.js";
import { bumpGatewayAccessRevision } from "./gateway-access-revision.js";
import type { GatewayBroadcastFn, GatewayBroadcastToConnIdsFn } from "./server-broadcast-types.js";
import type {
  ChatRunState,
  SessionEventSubscriberRegistry,
  SessionMessageSubscriberRegistry,
} from "./server-chat-state.js";
import type { ToolEventRecipientRegistry } from "./server-chat-tool-recipients.js";
import { resolveVisibleActiveSessionRunState } from "./server-methods/session-active-runs.js";
import { createSessionActivitySummaries } from "./session-activity-summaries.js";
import { broadcastSessionActivitySummary } from "./session-activity-summary-events.js";
import { defaultSessionCompanionContextReader } from "./session-companion-context.js";
import { createSessionCompanion } from "./session-companion.js";
import { createSessionLifecyclePersistenceOwner } from "./session-lifecycle-persistence-owner.js";
import { sessionObserverScopeKey } from "./session-observer-model.js";
import { createSessionObserver } from "./session-observer.js";
import {
  tryResolveSessionCompatibilityOwnerAgentId,
  resolveSessionEventAgentScope,
} from "./session-request-agent.js";
import type { SessionRowProjection } from "./session-row-projection.js";

function dispatchEventHandler<TEvent>(params: {
  loadHandler: () => Promise<(event: TEvent) => unknown>;
  event: TEvent;
  log: SubsystemLogger;
  failureMessage: string;
  context: Record<string, unknown>;
  onFailure?: (error: unknown) => void;
}) {
  return runWithRetainedGatewayRootWork(() =>
    params
      .loadHandler()
      .then((handler) => handler(params.event))
      .then(() => undefined)
      .catch((error: unknown) => {
        params.log.warn(params.failureMessage, { ...params.context, error });
        params.onFailure?.(error);
      }),
  );
}

/** Register gateway runtime event subscriptions and return unsubscribe handles. */
export function startGatewayEventSubscriptions(params: {
  scheduler: GatewayScheduler;
  signal: AbortSignal;
  log: SubsystemLogger;
  broadcast: GatewayBroadcastFn;
  broadcastToConnIds: GatewayBroadcastToConnIdsFn;
  nodeHasSessionSubscribers: (sessionKey: string) => boolean;
  nodeSendToSession: (sessionKey: string, event: string, payload: unknown) => void;
  agentRunSeq: Map<string, number>;
  chatRunState: ChatRunState;
  toolEventRecipients: ToolEventRecipientRegistry;
  sessionEventSubscribers: SessionEventSubscriberRegistry;
  sessionMessageSubscribers: SessionMessageSubscriberRegistry;
  restartRecoveryCandidates: Map<string, RestartRecoveryCandidate>;
  refreshConnectedUserProfiles: () => void;
  getSessionRowProjection?: () => SessionRowProjection | undefined;
}) {
  // Collection changes gate new work; the writer retains accepted work and maintenance.
  const auditRecorder = createAuditEventRecorder({
    getConfig: getRuntimeConfig,
    scheduler: params.scheduler,
  });
  const clearAuditSinks = [
    configureExecutionIdentityAdmissionSink(auditRecorder.recordExecutionIdentity),
    configureExecutionDecisionWorkSink(auditRecorder.recordExecutionDecisionWork),
    configureMessageActionDecisionSink(auditRecorder.recordExecutionDecision),
    configureRuntimeActionDecisionSink(auditRecorder.recordExecutionDecision),
  ];
  const channelAdmissionAudit = createChannelAdmissionAudit({
    enabled: isExecutionIdentityCollectionEnabled(getRuntimeConfig()),
    decisionSink: auditRecorder.recordExecutionDecision,
  });
  let auditPolicyClosed = false;
  let unsubscribeMessageAuditEvents: (() => void) | undefined;
  const reconcileAuditPolicy = (config: OpenClawConfig) => {
    if (auditPolicyClosed) {
      return;
    }
    channelAdmissionAudit.configure(isExecutionIdentityCollectionEnabled(config));
    if (isAuditLedgerEnabled(config) && resolveAuditMessageMode(config) !== "off") {
      unsubscribeMessageAuditEvents ??= onTrustedMessageAuditEvent(auditRecorder.recordMessage);
    } else {
      unsubscribeMessageAuditEvents?.();
      unsubscribeMessageAuditEvents = undefined;
    }
  };
  reconcileAuditPolicy(getRuntimeConfig());
  const sessionActivitySummaries = createSessionActivitySummaries({
    scheduler: params.scheduler,
    getConfig: getRuntimeConfig,
    getSessionRowProjection: params.getSessionRowProjection,
    onChanged: (target) => {
      const publication = broadcastSessionActivitySummary(target, params).catch((error: unknown) =>
        params.log.warn("Activity summary publication failed", { error }),
      );
      agentEventDispatches.add(publication);
      void publication.then(() => agentEventDispatches.delete(publication));
    },
  });
  const sessionObserver = createSessionObserver({
    getConfig: getRuntimeConfig,
    subscribers: params.sessionMessageSubscribers,
    sessionEventSubscribers: params.sessionEventSubscribers,
    broadcastToConnIds: params.broadcastToConnIds,
  });
  const sessionCompanion = createSessionCompanion({
    scheduler: params.scheduler,
    contextReader: defaultSessionCompanionContextReader,
    getConfig: getRuntimeConfig,
    sessionObserver,
  });
  let sessionBackgroundStop: Promise<void> | undefined;
  // Auxiliary model calls can inherit request work; cancel before that work drains.
  const stopSessionBackgroundWork = (): void => {
    if (!sessionBackgroundStop) {
      sessionCompanion.dispose();
      sessionObserver.dispose();
      sessionBackgroundStop = sessionActivitySummaries.dispose();
      void sessionBackgroundStop.catch((error: unknown) => {
        params.log.warn(`session background cleanup failed: ${String(error)}`);
      });
    }
  };
  params.signal.addEventListener("abort", stopSessionBackgroundWork, { once: true });
  if (params.signal.aborted) {
    stopSessionBackgroundWork();
  }
  const unsubscribePrivateAuditEvents = onAgentAuditEvent(auditRecorder.record);
  const unsubscribeToolAuditEvents = onTrustedToolExecutionEvent(auditRecorder.recordTool);
  const sessionLifecyclePersistence = createSessionLifecyclePersistenceOwner(params.scheduler);
  const agentEventDispatches = new Set<Promise<void>>();
  const eventRowOwners = new WeakMap<
    AgentEventRuntimePayload,
    { projection: SessionRowProjection; record: ReturnType<SessionRowProjection["capture"]> }
  >();
  const trackedRunIds = (runId: string, clientRunId: string) =>
    runId === clientRunId ? [runId] : [runId, clientRunId];
  const clearTrackedActiveRun = (run: { runId: string; clientRunId: string }) => {
    for (const candidateRunId of trackedRunIds(run.runId, run.clientRunId)) {
      const entry = getRpcSource(candidateRunId);
      if (!entry) {
        continue;
      }
      setRpcSourceProjectSessionActive(entry, false);
    }
  };
  const settleTrackedTerminal = (run: { runId: string; clientRunId: string }) => {
    for (const candidateRunId of trackedRunIds(run.runId, run.clientRunId)) {
      const entry = getRpcSource(candidateRunId);
      if (!entry || entry.adapter.projectSessionTerminalPersistence) {
        continue;
      }
      entry.adapter.projectSessionTerminalPending = false;
      entry.adapter.projectSessionTerminalPersisted = false;
      if (entry.input.retirementRequested) {
        retireRpcSource(candidateRunId, entry);
      }
    }
  };
  const trackedTerminalWrites = new WeakSet<Promise<void>>();
  const trackTrackedRunTerminalPersistence = (run: {
    runId: string;
    clientRunId: string;
    sessionId?: string;
    persistence: Promise<void>;
  }) => {
    // Ingress and the lazy chat consumer adopt the same prepared write once.
    if (trackedTerminalWrites.has(run.persistence)) {
      return true;
    }
    let tracked = false;
    for (const candidateRunId of trackedRunIds(run.runId, run.clientRunId)) {
      const entry = getRpcSource(candidateRunId);
      if (!entry) {
        continue;
      }
      tracked = true;
      entry.adapter.projectSessionTerminalPersisted = false;
      markChatAbortTerminalPersistenceError(entry, undefined);
      entry.adapter.projectSessionTerminalPersistence = run.persistence;
      const lifecycleGeneration = getRpcSourceLifecycleGeneration(entry);
      const identity = getRpcSourceIdentity(entry);
      const sessionKey = identity.sessionKey;
      const sessionId = run.sessionId || identity.sessionId;
      // Lazy chat consumption must retain the terminal time stamped at ingress.
      const observedAt = entry.adapter.projectSessionTerminalObservedAt;
      const settle = (persisted: boolean, error?: unknown) => {
        if (entry.adapter.projectSessionTerminalPersistence !== run.persistence) {
          return;
        }
        // Maintenance can retire the registration before its write settles.
        // Captured drain targets still need this exact owner's final facts.
        entry.adapter.projectSessionTerminalPending = false;
        entry.adapter.projectSessionTerminalPersistence = undefined;
        entry.adapter.projectSessionTerminalPersisted = persisted;
        markChatAbortTerminalPersistenceError(entry, error);
        if (!isRpcSourceRegistered(entry) && hasRpcSourceForController(candidateRunId, entry)) {
          return;
        }
        if (persisted) {
          params.restartRecoveryCandidates.delete(candidateRunId);
        } else if (
          entry.adapter.controlUiVisible !== false &&
          lifecycleGeneration &&
          sessionKey &&
          sessionId
        ) {
          params.restartRecoveryCandidates.set(candidateRunId, {
            runId: candidateRunId,
            lifecycleGeneration,
            sessionKey,
            sessionId,
            observedAt,
          });
        }
        if (isRpcSourceRegistered(entry) && entry.input.retirementRequested) {
          retireRpcSource(candidateRunId, entry);
        }
      };
      void run.persistence.then(
        () => settle(true),
        (error: unknown) => settle(false, error),
      );
    }
    if (tracked) {
      trackedTerminalWrites.add(run.persistence);
    }
    return tracked;
  };
  const getSessionKeyModule = createLazyPromise(() => import("./server-session-key.js"), {
    cacheRejections: true,
  });
  const agentEventHandlerLoader = createLazyPromiseLoader(
    () => {
      // Lazy-load heavy chat modules only after the first agent event reaches the gateway.
      return Promise.all([import("./server-chat.js"), getSessionKeyModule()]).then(
        ([{ createAgentEventHandler }, { resolveSessionForRun }]) =>
          createAgentEventHandler({
            broadcast: params.broadcast,
            broadcastToConnIds: params.broadcastToConnIds,
            nodeHasSessionSubscribers: params.nodeHasSessionSubscribers,
            nodeSendToSession: params.nodeSendToSession,
            agentRunSeq: params.agentRunSeq,
            chatRunState: params.chatRunState,
            resolveSessionKeyForRun: (runId, options) =>
              resolveSessionForRun(runId, {
                ...options,
                projection: params.getSessionRowProjection?.(),
              })?.sessionKey,
            clearAgentRunContext,
            toolEventRecipients: params.toolEventRecipients,
            sessionEventSubscribers: params.sessionEventSubscribers,
            sessionMessageSubscribers: params.sessionMessageSubscribers,
            getSessionRowProjection: params.getSessionRowProjection,
            loadGatewaySessionLifecycleSnapshotForEvent: (key, options) => {
              // Tool progress must not wait for optional row enrichment before reply capture.
              if (
                !options?.ownerEvent &&
                params.getSessionRowProjection?.()?.needsMaterialization
              ) {
                return { row: null };
              }
              const owner = options?.ownerEvent
                ? eventRowOwners.get(options.ownerEvent)
                : undefined;
              if (
                options?.ownerEvent &&
                (!owner?.record || !owner.projection.isCurrent(owner.record))
              ) {
                return { row: null };
              }
              const scope = resolveSessionEventAgentScope(
                getRuntimeConfig(),
                key,
                options?.agentId,
              );
              const read = options?.sessionRows;
              const prepared = scope?.[1] ? read?.describe({ key, agentId: scope[1] }) : undefined;
              const snapshot = scope?.[1]
                ? read
                  ? prepared
                    ? { row: read.present(prepared), lifecycleRunId: prepared.entry.lifecycleRunId }
                    : { row: null }
                  : (params.getSessionRowProjection?.()?.snapshot({ key, agentId: scope[1] }) ?? {
                      row: null,
                    })
                : { row: null };
              return options?.ownerEvent?.sessionId &&
                snapshot.row?.sessionId !== options.ownerEvent.sessionId
                ? { row: null }
                : snapshot;
            },
            persistGatewaySessionLifecycleEventForEvent: sessionLifecyclePersistence.persist,
            updateRunToolErrorSummary: ({ runId, clientRunId, summary }) => {
              for (const candidateRunId of new Set([runId, clientRunId])) {
                const entry = getRpcSource(candidateRunId);
                if (entry) {
                  entry.adapter.toolErrorSummary = summary;
                }
              }
            },
            clearTrackedActiveRun,
            settleTrackedTerminal,
            trackTrackedRunTerminalPersistence,
            isChatSendRunActive: (runId) => {
              const entry = getRpcSource(runId);
              // This callback identifies the terminal-response owner, not the
              // sessions.list activity projection. Retain it while the source settles.
              return entry !== undefined && entry.adapter.kind !== "agent";
            },
            resolveActiveLifecycleGenerationForRun: (runId) => {
              const entry = getRpcSource(runId);
              return entry ? getRpcSourceLifecycleGeneration(entry) : undefined;
            },
            resolveSessionActiveRunState: (session) =>
              resolveVisibleActiveSessionRunState({
                ...session,
                projectedAgentRunIndex:
                  params.getSessionRowProjection?.()?.state.rowContext.projectedAgentRuns,
                defaultAgentId: tryResolveSessionCompatibilityOwnerAgentId(
                  getRuntimeConfig(),
                  session.requestedKey,
                ),
              }),
          }),
      );
    },
    { cacheRejections: true },
  );
  const getAgentEventHandler = agentEventHandlerLoader.load;

  const getSessionEventsModule = createLazyPromise(() => import("./server-session-events.js"), {
    cacheRejections: true,
  });

  const getTranscriptUpdateHandler = createLazyRuntimeSurface(
    getSessionEventsModule,
    ({ createTranscriptUpdateBroadcastHandler }) => createTranscriptUpdateBroadcastHandler(params),
  );
  const getLifecycleEventHandler = createLazyRuntimeSurface(
    getSessionEventsModule,
    ({ createLifecycleEventBroadcastHandler }) => createLifecycleEventBroadcastHandler(params),
  );

  const unsubscribeAgentEvents = onAgentRuntimeEvent((evt) => {
    if (evt.stream === "lifecycle") {
      const projection = params.getSessionRowProjection?.();
      if (projection) {
        const link = params.chatRunState.registry.peek(evt.runId);
        const run = getAgentRunContext(evt.runId);
        const key = link?.sessionKey ?? evt.deliverySessionKey ?? evt.sessionKey ?? run?.sessionKey;
        const scope = key
          ? resolveSessionEventAgentScope(
              getRuntimeConfig(),
              key,
              link?.agentId ?? evt.agentId ?? run?.agentId,
            )
          : undefined;
        if (key && scope?.[1]) {
          eventRowOwners.set(evt, {
            projection,
            record: projection.capture({ key, agentId: scope[1] }),
          });
        }
      }
    }
    let failedDispatchCleanup: (() => void) | undefined;
    let terminalPreparation: Promise<void> | undefined;
    let terminalEntries: RpcSourceRef[] | undefined;
    sessionObserver.handleEvent(evt);
    sessionActivitySummaries.handleEvent(evt);
    auditRecorder.record(evt);
    const lifecyclePhase =
      evt.stream === "lifecycle" && typeof evt.data?.phase === "string"
        ? evt.data.phase
        : undefined;
    if (lifecyclePhase === "start" || lifecyclePhase === "end" || lifecyclePhase === "error") {
      const terminal = lifecyclePhase !== "start";
      const chatLink = evt.contextClaimId
        ? undefined
        : params.chatRunState.registry.peek(evt.runId);
      const clientRunId = chatLink?.clientRunId ?? evt.runId;
      const candidateRunIds = trackedRunIds(evt.runId, clientRunId);
      const eventLifecycleGeneration = evt.lifecycleGeneration;
      const observedAt = terminal
        ? typeof evt.data.endedAt === "number" && Number.isFinite(evt.data.endedAt)
          ? evt.data.endedAt
          : evt.ts
        : undefined;
      for (const candidateRunId of candidateRunIds) {
        const entry = getRpcSource(candidateRunId);
        const lifecycleGeneration = entry ? getRpcSourceLifecycleGeneration(entry) : undefined;
        if (
          entry &&
          (!eventLifecycleGeneration ||
            !lifecycleGeneration ||
            lifecycleGeneration === eventLifecycleGeneration)
        ) {
          entry.adapter.projectSessionTerminalPending = terminal;
          entry.adapter.projectSessionTerminalObservedAt = observedAt;
          if (terminal) {
            (terminalEntries ??= []).push(entry);
          }
        }
      }
      if (terminal) {
        const trackedEntry = candidateRunIds
          .map((candidateRunId) => getRpcSource(candidateRunId))
          .find((entry) => entry !== undefined);
        const runContext = getAgentRunContext(evt.runId);
        const trackedIdentity = trackedEntry ? getRpcSourceIdentity(trackedEntry) : undefined;
        // Match the chat projection owner before preparing the shared terminal write.
        // A bound ACP runtime emits its target key, but the chat link owns the source run.
        const sessionAgentId =
          chatLink?.agentId ?? evt.agentId ?? trackedIdentity?.agentId ?? runContext?.agentId;
        const knownSessionKey =
          chatLink?.sessionKey ??
          evt.deliverySessionKey ??
          evt.sessionKey ??
          trackedIdentity?.sessionKey ??
          runContext?.sessionKey;
        const trackedLifecycleGeneration = trackedEntry
          ? getRpcSourceLifecycleGeneration(trackedEntry)
          : undefined;
        const terminalAuthority =
          evt.contextClaimId && eventLifecycleGeneration
            ? {
                claimId: evt.contextClaimId,
                lifecycleGeneration: eventLifecycleGeneration,
                runId: evt.runId,
              }
            : undefined;
        const trackedOwnerIsCurrent =
          !trackedEntry ||
          !eventLifecycleGeneration ||
          !trackedLifecycleGeneration ||
          trackedLifecycleGeneration === eventLifecycleGeneration;
        const claimIsComplete = !evt.contextClaimId || terminalAuthority !== undefined;
        const canPersistTerminal =
          isDefinitiveRunLifecycle({ phase: lifecyclePhase, data: evt.data }) &&
          evt.projectSessionLifecycle !== false &&
          trackedOwnerIsCurrent &&
          claimIsComplete;
        const writeContext = captureAgentRunTerminalWriteContext(evt.runId);
        const prepareTerminalPersistence = (sessionKey: string, agentId = sessionAgentId) => {
          const persistence = sessionLifecyclePersistence.observe({
            sessionKey,
            ...(agentId ? { agentId } : {}),
            event: evt,
            ...(terminalAuthority ? { authority: terminalAuthority } : {}),
            ...(writeContext ? { writeContext } : {}),
            ...(clientRunId !== evt.runId ? { clientRunId } : {}),
          });
          if (terminalAuthority) {
            // A failed lazy handler cannot consume the prepared write and release
            // its claim. Persistence settlement becomes that cleanup boundary.
            const clearTerminalAuthority = () =>
              clearAgentRunContext(
                terminalAuthority.runId,
                terminalAuthority.lifecycleGeneration,
                terminalAuthority.claimId,
              );
            failedDispatchCleanup = () => {
              void persistence.then(clearTerminalAuthority, clearTerminalAuthority);
            };
          }
          clearTrackedActiveRun({ runId: evt.runId, clientRunId });
          const tracked = trackTrackedRunTerminalPersistence({
            runId: evt.runId,
            clientRunId,
            sessionId: evt.sessionId,
            persistence,
          });
          if (!tracked) {
            void persistence.catch((error: unknown) => {
              params.log.warn("Terminal session persistence failed", { runId: evt.runId, error });
            });
          }
          return persistence;
        };
        if (canPersistTerminal) {
          if (knownSessionKey) {
            const persistence = prepareTerminalPersistence(knownSessionKey);
            writeContext?.track(persistence);
          } else {
            // Context cleanup can precede a terminal event. Resolve its persisted
            // run mapping before the lazy chat handler consumes the same event.
            terminalPreparation = getSessionKeyModule().then(async ({ resolveSessionForRun }) => {
              const selected = resolveSessionForRun(evt.runId, {
                agentId: sessionAgentId,
                projection: params.getSessionRowProjection?.(),
              });
              if (selected) {
                await prepareTerminalPersistence(selected.sessionKey, selected.agentId);
              }
            });
            writeContext?.track(terminalPreparation);
          }
        }
      }
    }
    const dispatchPreparation = terminalPreparation;
    const terminalDispatch: Pick<ChatAbortTerminalDispatch, "failure"> | undefined = terminalEntries
      ? {}
      : undefined;
    const dispatch = dispatchEventHandler<AgentEventRuntimePayload>({
      loadHandler: dispatchPreparation
        ? async () => {
            await dispatchPreparation;
            return getAgentEventHandler();
          }
        : getAgentEventHandler,
      event: evt,
      log: params.log,
      failureMessage: "Agent event dispatch failed",
      context: { runId: evt.runId, stream: evt.stream },
      onFailure: (error) => {
        if (terminalDispatch) {
          terminalDispatch.failure = { error };
        }
        failedDispatchCleanup?.();
      },
    });
    bindChatAbortTerminalDispatch(terminalEntries, dispatch, terminalDispatch);
    agentEventDispatches.add(dispatch);
    void dispatch.then(() => agentEventDispatches.delete(dispatch));
  });
  const agentUnsub = async () => {
    auditPolicyClosed = true;
    unsubscribeAgentEvents();
    params.signal.removeEventListener("abort", stopSessionBackgroundWork);
    stopSessionBackgroundWork();
    await sessionBackgroundStop;
    unsubscribePrivateAuditEvents();
    unsubscribeToolAuditEvents();
    unsubscribeMessageAuditEvents?.();
    clearAuditSinks.forEach((clear) => clear());
    channelAdmissionAudit.close();
    // A missing-key terminal can still be resolving its persisted run mapping.
    // Join dispatch first so handler consumption precedes persistence drain.
    await Promise.allSettled(agentEventDispatches);
    await agentEventHandlerLoader
      .peek()
      ?.then((handler) => handler.dispose())
      .catch(() => undefined);
    await sessionLifecyclePersistence.drain();
    await auditRecorder.stop();
  };

  const heartbeatUnsub = onHeartbeatEvent((evt) => {
    params.broadcast("heartbeat", evt, { dropIfSlow: true });
  });

  const transcriptUnsub = onInternalSessionTranscriptUpdate((evt) => {
    sessionActivitySummaries.handleTranscript(evt);
    void dispatchEventHandler({
      loadHandler: getTranscriptUpdateHandler,
      event: evt,
      log: params.log,
      failureMessage: "Transcript update dispatch failed",
      context: { sessionKey: evt.sessionKey },
    });
  });

  // Committed resets/rotations can change access after the originating run is gone.
  // Invalidate synchronously before any yielded reader can accept its old access snapshot.
  // Each runtime owns its callback so late disposal cannot remove a replacement's listener.
  const unsubscribeSessionIdentity = onSessionIdentityMutation(() => bumpGatewayAccessRevision());
  const unsubscribeProfileChanges = onUserProfilesChanged(() => {
    params.refreshConnectedUserProfiles();
    params.broadcastToConnIds(
      "sessions.changed",
      { reason: "profile-identity" },
      params.sessionEventSubscribers.getAll(),
    );
  });
  const unsubscribeLifecycle = onSessionLifecycleEvent((evt) => {
    sessionActivitySummaries.handleLifecycle(evt);
    if (evt.reason === "progress-card-reset" && evt.agentId) {
      // Card readers need not subscribe to session lists. Preserve the canonical
      // owner tuple even when distinct global rows share a display key.
      params.broadcast(
        "progressCard.changed",
        { sessionKey: sessionObserverScopeKey(evt.sessionKey, evt.agentId), revision: null },
        { sessionKeys: [evt.sessionKey], agentId: evt.agentId },
      );
      return;
    }
    void dispatchEventHandler({
      loadHandler: getLifecycleEventHandler,
      event: evt,
      log: params.log,
      failureMessage: "Lifecycle event dispatch failed",
      context: { sessionKey: evt.sessionKey },
    });
  });
  const unsubscribeSuspension = onGatewaySuspendAdmissionChange((phase) => {
    params.broadcast("gateway.suspension", { phase });
  });
  const lifecycleUnsub = () => {
    unsubscribeSessionIdentity();
    unsubscribeSuspension();
    unsubscribeProfileChanges();
    unsubscribeLifecycle();
  };

  return {
    channelAdmissionAudit,
    reconcileAuditPolicy,
    sessionActivitySummaries,
    sessionCompanion,
    sessionObserver,
    agentUnsub,
    heartbeatUnsub,
    transcriptUnsub,
    lifecycleUnsub,
  };
}
