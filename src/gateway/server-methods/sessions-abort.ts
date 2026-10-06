import { asOptionalObjectRecord } from "@openclaw/normalization-core/record-coerce";
import {
  hasNonEmptyString,
  normalizeOptionalString,
} from "@openclaw/normalization-core/string-coerce";
import {
  ErrorCodes,
  errorShape,
  validateSessionsAbortParams,
} from "../../../packages/gateway-protocol/src/index.js";
import { retireSessionMcpRuntime } from "../../agents/agent-bundle-mcp-manager-api.js";
import {
  resolveActiveEmbeddedRunOwner,
  resolveActiveEmbeddedRunOwnerByRunId,
  type ActiveEmbeddedRunOwner,
} from "../../agents/embedded-agent-runner/runs.js";
import { captureYieldedMainSessionContinuation } from "../../agents/main-session-recovery/main-session-restart-recovery-target.js";
import {
  isConfiguredSessionStoreAgentId,
  resolveExistingAgentSessionStoreTargetsSync,
} from "../../config/sessions.js";
import {
  assertAgentRunLifecycleGenerationCurrent,
  getAgentEventLifecycleGeneration,
} from "../../infra/agent-events.js";
import { normalizeAgentId, parseAgentSessionKey } from "../../routing/session-key.js";
import { captureSessionTarget } from "../../sessions/session-controller.lifecycle.js";
import {
  getRpcSource,
  getRpcSourceForTarget,
  getRpcSourceIdentity,
  getRpcSourceLifecycleGeneration,
  hasRpcSource,
  listRpcSourceEntries,
  listRpcSourceEntriesForTarget,
} from "../../sessions/session-controller.rpc-sources.js";
import {
  captureSessionControllerStop,
  stopSession,
} from "../../sessions/session-controller.stop.js";
import { captureAgentJobSession, setGatewayDedupeEntry } from "../agent-turn/agent-job.js";
import { waitForChatAbortTerminalPersistence } from "../chat-abort-lifecycle-internal.js";
import { resolveChatRunOwnerAgentId } from "../chat-run-owner.js";
import { formatStopRequest } from "../control-plane-audit.js";
import { resolveSessionForRun } from "../server-session-key.js";
import { persistGatewaySessionLifecycleEvent } from "../session-lifecycle-state.js";
import {
  resolveRequestedSessionAgentId as resolveRequestedGlobalAgentId,
  tryResolveSessionCompatibilityOwnerAgentId,
} from "../session-request-agent.js";
import { getSessionRowProjection } from "../session-row-projection-access.js";
import { resolveSessionStoreKey } from "../session-store-key.js";
import { loadSessionEntry } from "../session-utils.js";
import { getWorkerInferenceSessionControl } from "../worker-environments/inference-control-internal.js";
import { resolveChatAbortRequester } from "./chat-abort-authorization.js";
import { handleChatAbortRequestWithLifecycle } from "./chat-abort-handler.js";
import {
  abortControlledSubagents,
  abortQueuedCollectorSession,
  descendantAbortError,
} from "./chat-abort-runtime.js";
import { abortedPartialPersistenceError } from "./chat-aborted-partial.js";
import { emitSessionsChanged } from "./session-change-event.js";
import {
  bindGatewayRequestHandlerMutationAuthority,
  readGatewayRequestMutationAuthority,
} from "./session-mutation-guards.js";
import {
  resolveAbortSessionKey,
  resolveScopedAbortKey,
  resolveSessionKeyAgentId,
  sessionKeyBelongsToAgent,
} from "./sessions-abort-target.js";
import { requireSessionKey } from "./sessions-shared.js";
import type { GatewayRequestHandlers } from "./types.js";
import { assertValidParams } from "./validation.js";

export const sessionAbortHandlers: GatewayRequestHandlers = {
  "sessions.abort": async (options) => {
    const { params, respond, context, client, sessionMutationAuthorization } = options;
    const authority = readGatewayRequestMutationAuthority(options);
    const requester = resolveChatAbortRequester(client, sessionMutationAuthorization);
    const narrow =
      authority.sessionScope === "operator.sessions.write" ||
      requester.sessionAuthority !== undefined;
    if (!assertValidParams(params, validateSessionsAbortParams, "sessions.abort", respond)) {
      return;
    }
    context.logGateway.info(formatStopRequest("sessions.abort", client, params));
    const p = params;
    const cfg = context.getRuntimeConfig();
    const requestedRunId = typeof p.runId === "string" ? p.runId : undefined;
    const requestedKey = normalizeOptionalString(p.key);
    const requestedParamAgentId = normalizeOptionalString(p.agentId);
    const workerRunTarget = requestedRunId
      ? getWorkerInferenceSessionControl(
          context.workerEnvironmentService,
        )?.resolveSessionTargetForRunId(requestedRunId)
      : undefined;
    const embeddedCandidate = requestedRunId
      ? resolveActiveEmbeddedRunOwnerByRunId(requestedRunId)
      : undefined;
    const embeddedRun = embeddedCandidate?.runId === requestedRunId ? embeddedCandidate : undefined;
    const embeddedRunSessionKey = embeddedRun?.sessionKey;
    const scopedRequestedKey = resolveScopedAbortKey({
      cfg,
      key: requestedKey,
      agentId: requestedParamAgentId,
    });
    if (requestedKey && requestedParamAgentId && !scopedRequestedKey) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "session key agent does not match agentId"),
      );
      return;
    }
    const requestedKeyAgentId = scopedRequestedKey
      ? resolveSessionKeyAgentId(scopedRequestedKey, cfg)
      : undefined;
    // A run ID without its physical target is inference only: duplicate protocol IDs can be
    // active in independent stores, and the index deliberately fails closed in that case.
    const inferredActiveRun = requestedRunId ? getRpcSource(requestedRunId) : undefined;
    const inferredActiveRunIdentity = inferredActiveRun && getRpcSourceIdentity(inferredActiveRun);
    const inferredActiveRunSessionKey = inferredActiveRunIdentity?.sessionKey;
    const inferredActiveRunAgentId = normalizeOptionalString(inferredActiveRunIdentity?.agentId);
    let inferredRunAgentId =
      requestedParamAgentId ??
      inferredActiveRunAgentId ??
      requestedKeyAgentId ??
      workerRunTarget?.agentId ??
      resolveSessionKeyAgentId(inferredActiveRunSessionKey, cfg) ??
      resolveSessionKeyAgentId(embeddedRunSessionKey, cfg);
    if (requestedRunId && !inferredRunAgentId) {
      const runOwner = resolveRequestedGlobalAgentId(
        cfg,
        scopedRequestedKey ?? inferredActiveRunSessionKey ?? workerRunTarget?.sessionKey ?? "main",
      );
      if (!runOwner.ok) {
        respond(false, undefined, runOwner.error);
        return;
      }
      inferredRunAgentId = runOwner.agentId;
    }
    const requestedRunAgentId = requestedRunId
      ? inferredRunAgentId
        ? normalizeAgentId(inferredRunAgentId)
        : undefined
      : undefined;
    const scopedInferredActiveRunSessionKey =
      inferredActiveRunSessionKey &&
      (!requestedRunAgentId ||
        sessionKeyBelongsToAgent(inferredActiveRunSessionKey, requestedRunAgentId, cfg))
        ? inferredActiveRunSessionKey
        : undefined;
    const keyCandidate =
      scopedRequestedKey ??
      scopedInferredActiveRunSessionKey ??
      (requestedRunId
        ? resolveSessionForRun(requestedRunId, {
            agentId: requestedRunAgentId,
            projection: getSessionRowProjection(context),
          })?.sessionKey
        : undefined) ??
      workerRunTarget?.sessionKey ??
      embeddedRunSessionKey;
    if (!keyCandidate && requestedRunId) {
      respond(true, { ok: true, abortedRunId: null, status: "no-active-run" });
      return;
    }
    const key = requireSessionKey(keyCandidate, respond);
    if (!key) {
      return;
    }
    const requestedGlobalAgent = resolveRequestedGlobalAgentId(
      cfg,
      key,
      // An inferred canonical-key owner is not an explicit configured-agent request.
      // The exact live/persisted target check below also admits retired owners.
      requestedParamAgentId ?? (parseAgentSessionKey(key) ? undefined : requestedRunAgentId),
    );
    if (!requestedGlobalAgent.ok) {
      respond(false, undefined, requestedGlobalAgent.error);
      return;
    }
    const targetAgentId = requestedGlobalAgent.agentId;
    const configuredTarget = isConfiguredSessionStoreAgentId(cfg, targetAgentId);
    const existingTargets = configuredTarget
      ? []
      : resolveExistingAgentSessionStoreTargetsSync(cfg, targetAgentId);
    const stableTargetOwner = tryResolveSessionCompatibilityOwnerAgentId(cfg, key);
    // Avoid opening a fallback store for a retired owner. A unique live source already carries
    // its exact physical target; configured or persisted owners can be resolved from storage.
    const loadedSession =
      configuredTarget || existingTargets.length > 0
        ? loadSessionEntry(key, { agentId: targetAgentId })
        : undefined;
    const canonicalKey =
      loadedSession?.canonicalKey ??
      resolveSessionStoreKey({
        cfg,
        sessionKey: key,
        storeAgentId: targetAgentId,
      });
    const sessionEntry = loadedSession?.entry;
    const requestedKeyAliases =
      requestedKey &&
      requestedKey !== key &&
      (!requestedParamAgentId || sessionKeyBelongsToAgent(requestedKey, requestedParamAgentId, cfg))
        ? [requestedKey]
        : undefined;
    const activeRunTarget = loadedSession
      ? captureSessionTarget({
          storeScope: loadedSession.storePath,
          sessionKey: canonicalKey,
          aliases: [key, ...(requestedKeyAliases ?? [])],
          agentId: targetAgentId,
          incarnation: sessionEntry?.sessionId,
        })
      : inferredActiveRun?.input.target;
    const activeRun =
      inferredActiveRun ??
      (requestedRunId && activeRunTarget
        ? getRpcSourceForTarget(requestedRunId, activeRunTarget)
        : undefined);
    const activeRunIdentity = activeRun && getRpcSourceIdentity(activeRun);
    const activeRunSessionKey = activeRunIdentity?.sessionKey;
    const activeRunAgentId = normalizeOptionalString(activeRunIdentity?.agentId);
    const scopedActiveRunSessionKey =
      activeRunSessionKey &&
      (!requestedRunAgentId ||
        sessionKeyBelongsToAgent(activeRunSessionKey, requestedRunAgentId, cfg))
        ? activeRunSessionKey
        : undefined;
    const hasExactActiveRun = requestedRunId
      ? (scopedActiveRunSessionKey === key &&
          resolveChatRunOwnerAgentId({
            agentId: activeRunAgentId,
            sessionKey: activeRunSessionKey,
            defaultAgentId: stableTargetOwner,
          }) === normalizeAgentId(targetAgentId)) ||
        (embeddedRun !== undefined &&
          resolveSessionKeyAgentId(embeddedRunSessionKey, cfg) === normalizeAgentId(targetAgentId))
      : listRpcSourceEntries().some(([, entry]) => {
          const identity = getRpcSourceIdentity(entry);
          return (
            entry.adapter.controlUiVisible !== false &&
            identity.sessionKey === key &&
            resolveChatRunOwnerAgentId({
              agentId: identity.agentId,
              sessionKey: identity.sessionKey,
              defaultAgentId: stableTargetOwner,
            }) === normalizeAgentId(targetAgentId)
          );
        });
    if (!configuredTarget && existingTargets.length === 0 && !hasExactActiveRun) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, `agent "${targetAgentId}" not found`),
      );
      return;
    }
    const admittedTarget = sessionMutationAuthorization?.admittedTarget;
    if (
      narrow &&
      (!admittedTarget?.sessionId.trim() ||
        admittedTarget.sessionKey !== canonicalKey ||
        admittedTarget.agentId !== normalizeAgentId(targetAgentId) ||
        sessionEntry?.sessionId !== admittedTarget.sessionId)
    ) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "session target is unavailable"),
      );
      return;
    }
    const requiredSessionId = narrow ? admittedTarget?.sessionId : undefined;
    const embeddedRunMatchesSession = Boolean(
      embeddedRun &&
      resolveSessionKeyAgentId(embeddedRun.sessionKey, cfg) === normalizeAgentId(targetAgentId) &&
      (narrow
        ? embeddedRun.sessionId === requiredSessionId &&
          (embeddedRun.sessionKey === key || embeddedRun.sessionKey === canonicalKey)
        : embeddedRun.sessionKey === key ||
          embeddedRun.sessionKey === canonicalKey ||
          sessionEntry?.sessionId === embeddedRun.sessionId),
    );
    if (embeddedRun && !embeddedRunMatchesSession) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "runId does not match session"),
      );
      return;
    }
    const resolvedAbortSessionKey = resolveAbortSessionKey({
      requestedKey: key,
      canonicalKey,
      activeRunSessionKey: narrow ? undefined : scopedActiveRunSessionKey,
      aliasKeys: requestedKeyAliases,
      agentId: targetAgentId,
      defaultAgentId: stableTargetOwner,
    });
    const abortSessionKey = canonicalKey === "global" ? "global" : resolvedAbortSessionKey;
    const lifecycleGeneration = getAgentEventLifecycleGeneration();
    const lifecycleRevision = sessionEntry?.lifecycleRevision;
    const persistSessionAbort = (
      owner: Pick<ActiveEmbeddedRunOwner, "runId" | "sessionId" | "startedAtMs">,
    ) => {
      const endedAt = Date.now();
      return persistGatewaySessionLifecycleEvent({
        sessionKey: canonicalKey,
        agentId: targetAgentId,
        assertCommitAllowed: () => assertAgentRunLifecycleGenerationCurrent(lifecycleGeneration),
        expectedWriter: {
          runId: owner.runId,
          sessionId: owner.sessionId,
          lifecycleRevision,
        },
        event: {
          runId: owner.runId,
          sessionId: owner.sessionId,
          lifecycleGeneration,
          ts: endedAt,
          data: {
            phase: "end",
            status: "cancelled",
            aborted: true,
            stopReason: "rpc",
            startedAt: owner.startedAtMs ?? sessionEntry?.startedAt,
            endedAt,
          },
        },
      });
    };
    const assertAbortCurrent = () => {
      authority.assertCurrent();
      sessionMutationAuthorization?.assertCurrent();
      requester.sessionAuthority?.assertCurrent();
      assertAgentRunLifecycleGenerationCurrent(lifecycleGeneration);
    };
    const stopHookContext = {
      sessionKey: canonicalKey,
      sessionEntry,
      sessionId: sessionEntry?.sessionId,
      commandSource: "gateway:sessions.abort",
      senderId: requester.deviceId ?? requester.connId,
    };
    // Controller-backed runs must keep the requester checks and lifecycle cleanup below.
    if (embeddedRun && !activeRun && (!requestedRunId || !hasRpcSource(requestedRunId))) {
      let parentStatus: ReturnType<ActiveEmbeddedRunOwner["stop"]> = "unchanged";
      let descendants: Awaited<ReturnType<typeof abortControlledSubagents>> | undefined;
      const { aborted } = await stopSession({
        source: "client-run",
        capture: captureSessionControllerStop({}),
        assertCurrent: assertAbortCurrent,
        reason: "rpc",
        hookContext: stopHookContext,
        externalParent: {
          stop: () => {
            assertAbortCurrent();
            parentStatus = embeddedRun.stop();
            return parentStatus;
          },
          settled: embeddedRun.waitForSettlement(),
        },
        stopChildren: async (applyParentStop) => {
          descendants = await abortControlledSubagents({
            cfg,
            sessionKey: embeddedRun.sessionKey ?? canonicalKey,
            agentId: targetAgentId,
            requesterTurnRunId: embeddedRun.runId,
            beforeKill: applyParentStop,
          });
          return {
            stopped: descendants?.killed ?? 0,
            failed: descendants?.status === "error" ? descendants.failed : 0,
          };
        },
        continueChildStop: () => parentStatus !== "unchanged",
      }).completed;
      if (aborted) {
        await Promise.all([persistSessionAbort(embeddedRun), embeddedRun.waitForSettlement()]);
      }
      const error = descendantAbortError(descendants, "Parent run");
      if (error) {
        respond(false, undefined, error);
      } else {
        respond(true, {
          ok: true,
          abortedRunId: aborted ? embeddedRun.runId : null,
          status: aborted ? "aborted" : "no-active-run",
        });
      }
      if (aborted) {
        emitSessionsChanged(context, {
          sessionKey: canonicalKey,
          agentId: targetAgentId,
          reason: "abort",
        });
      }
      return;
    }
    // Snapshot before abort can remove controllers. Agent run IDs are idempotency
    // keys, so preserve their dedupe namespace instead of colliding with chat.send.
    const persistedSessionId = sessionEntry?.sessionId;
    const abortTarget = loadedSession
      ? captureSessionTarget({
          storeScope: loadedSession.storePath,
          sessionKey: canonicalKey,
          aliases: [key, abortSessionKey, ...(requestedKeyAliases ?? [])],
          agentId: targetAgentId,
          incarnation: persistedSessionId,
        })
      : activeRun?.input.target;
    const preAbortRuns = new Map(abortTarget ? listRpcSourceEntriesForTarget(abortTarget) : []);
    const preAbortDedupe = new Map(context.dedupe);
    const preAbortSessions = new Map(
      [...preAbortRuns].map(([runId, entry]) => [
        runId,
        captureAgentJobSession({
          ...getRpcSourceIdentity(entry),
          lifecycleGeneration: getRpcSourceLifecycleGeneration(entry),
        }),
      ]),
    );
    let abortedRunIds: string[] = [];
    let abortedRunId: string | null = null;
    let aborted = false;
    let chatAbortSucceeded = false;
    let failedResponse: Parameters<typeof respond> | undefined;
    let descendantsCancelled = false;
    let responseMeta: Record<string, unknown> | undefined;
    let abortWarning: string | undefined;
    const capturedSessionEmbeddedRun = persistedSessionId
      ? resolveActiveEmbeddedRunOwner(persistedSessionId)
      : undefined;
    const sessionEmbeddedRun =
      !narrow ||
      (capturedSessionEmbeddedRun &&
        capturedSessionEmbeddedRun.sessionId === requiredSessionId &&
        (capturedSessionEmbeddedRun.sessionKey === key ||
          capturedSessionEmbeddedRun.sessionKey === canonicalKey))
        ? capturedSessionEmbeddedRun
        : undefined;
    const yieldedRunId =
      typeof sessionEntry?.lifecycleRunId === "string" ? sessionEntry.lifecycleRunId : undefined;
    const yieldedParent =
      !requestedRunId &&
      !sessionEmbeddedRun &&
      yieldedRunId &&
      !preAbortRuns.has(yieldedRunId) &&
      loadedSession &&
      sessionEntry &&
      captureYieldedMainSessionContinuation({
        cfg,
        agentId: targetAgentId,
        sessionKey: canonicalKey,
        storePath: loadedSession.storePath,
        entry: sessionEntry,
      })
        ? {
            runId: yieldedRunId,
            sessionId: sessionEntry.sessionId,
            startedAtMs: sessionEntry.startedAt,
          }
        : undefined;
    const settleAbortPersistence = async (runIds: readonly string[], stopSucceeded: boolean) => {
      try {
        await Promise.all(
          runIds.flatMap((runId) => {
            const entry = preAbortRuns.get(runId);
            return entry ? [waitForChatAbortTerminalPersistence(entry)] : [];
          }),
        );
        if (aborted && sessionEmbeddedRun && !preAbortRuns.has(sessionEmbeddedRun.runId)) {
          await persistSessionAbort(sessionEmbeddedRun);
        }
        if (stopSucceeded && !requestedRunId && persistedSessionId) {
          assertAgentRunLifecycleGenerationCurrent(lifecycleGeneration);
          await retireSessionMcpRuntime({
            sessionId: persistedSessionId,
            reason: "session-stop",
          });
        }
        if (descendantsCancelled && yieldedParent) {
          // Child cancellation consumes the wake; join its parent's terminal write too.
          await persistSessionAbort(yieldedParent);
        }
      } catch (error) {
        throw abortedPartialPersistenceError(error, abortWarning);
      }
    };
    const queuedAbort = abortQueuedCollectorSession({
      context,
      sessionKey: canonicalKey,
      sessionKeyAliases: [key, ...(requestedKeyAliases ?? [])],
      agentId: targetAgentId,
      sessionId: persistedSessionId,
      requiredSessionId,
      session: loadedSession ? { ok: true, value: loadedSession } : undefined,
      defaultAgentId: stableTargetOwner,
      runId: requestedRunId,
      abortOrigin: "rpc",
      stopReason: "rpc",
      requester,
      stopSource: requestedRunId ? "client-run" : "client-session",
      hookContext: stopHookContext,
      assertCurrent: assertAbortCurrent,
    });
    if (queuedAbort) {
      const result = await queuedAbort;
      if (result.ok) {
        abortWarning = result.value.warning;
      }
      await settleAbortPersistence(result.ok ? result.value.runIds : [], result.ok);
      if (!result.ok) {
        respond(false, undefined, result.error);
      } else {
        respond(
          true,
          {
            ok: true,
            abortedRunId: result.value.runIds[0] ?? null,
            status: result.value.aborted ? "aborted" : "no-active-run",
            ...(abortWarning ? { warning: abortWarning } : {}),
          },
          undefined,
          undefined,
        );
      }
      return;
    }
    await handleChatAbortRequestWithLifecycle(
      bindGatewayRequestHandlerMutationAuthority(
        options,
        {
          ...options,
          params: {
            sessionKey: abortSessionKey,
            runId: requestedRunId,
            agentId: targetAgentId,
          },
          respond: (ok, payload, error, meta) => {
            if (!ok) {
              failedResponse = [ok, payload, error, meta];
              return;
            }
            chatAbortSucceeded = true;
            responseMeta = meta;
            const result = asOptionalObjectRecord(payload);
            abortWarning = normalizeOptionalString(result?.warning);
            const runIds = Array.isArray(result?.runIds)
              ? result.runIds.filter(hasNonEmptyString)
              : [];
            const firstAbortedRunId = runIds[0] ?? null;
            abortedRunIds = runIds;
            abortedRunId = firstAbortedRunId;
            aborted = firstAbortedRunId !== null || result?.aborted === true;
            const workerOnly = Boolean(workerRunTarget && !activeRun);
            if (firstAbortedRunId && !workerOnly) {
              const endedAt = Date.now();
              const runKind = preAbortRuns.get(firstAbortedRunId)?.adapter.kind;
              const dedupePrefix = runKind === "agent" ? "agent" : "chat";
              const dedupeKey = `${dedupePrefix}:${firstAbortedRunId}`;
              // Nested cancellation can yield after the old controller ends. A new
              // receipt owns its outcome; this supplemental timeout must not replace it.
              if (context.dedupe.get(dedupeKey) !== preAbortDedupe.get(dedupeKey)) {
                return;
              }
              setGatewayDedupeEntry({
                dedupe: context.dedupe,
                key: dedupeKey,
                session: preAbortSessions.get(firstAbortedRunId),
                entry: {
                  ts: endedAt,
                  ok: true,
                  payload: {
                    status: "timeout",
                    runId: firstAbortedRunId,
                    agentId: targetAgentId,
                    stopReason: "rpc",
                    endedAt,
                  },
                },
              });
            }
          },
        },
        undefined,
      ),
      {
        ...(!requestedRunId ? { cascadeDescendants: true as const } : {}),
        hookContext: stopHookContext,
        onDescendantsCancelled: () => {
          descendantsCancelled = true;
        },
      },
    );
    await settleAbortPersistence(abortedRunIds, chatAbortSucceeded);
    if (!chatAbortSucceeded) {
      if (failedResponse) {
        respond(...failedResponse);
      }
      return;
    }
    respond(
      true,
      {
        ok: true,
        abortedRunId,
        status: aborted ? "aborted" : "no-active-run",
        ...(abortWarning ? { warning: abortWarning } : {}),
      },
      undefined,
      responseMeta,
    );
    if (aborted) {
      emitSessionsChanged(context, {
        sessionKey: canonicalKey,
        agentId: targetAgentId,
        reason: "abort",
      });
    }
  },
};
