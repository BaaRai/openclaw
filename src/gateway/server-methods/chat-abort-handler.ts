import type { Result } from "@openclaw/normalization-core/result";
import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import {
  ErrorCodes,
  errorShape,
  validateChatAbortParams,
} from "../../../packages/gateway-protocol/src/index.js";
import { discardSessionPendingInput } from "../../config/sessions/session-pending-input-withdrawal.js";
import { isIncognitoSessionKey, normalizeAgentId } from "../../routing/session-key.js";
import { captureSessionTarget } from "../../sessions/session-controller.lifecycle.js";
import {
  captureSessionControllerSourceSettlement,
  holdSessionControllerSourceWithdrawal,
} from "../../sessions/session-controller.mailbox.js";
import {
  getRpcSource,
  getRpcSourceForTarget,
  getRpcSourceIdentity,
  getReservedRpcSourceInput,
  getSessionControllerSourceIdentity,
  isRpcSourceQueued,
  isRpcSourceRegistered,
} from "../../sessions/session-controller.rpc-sources.js";
import {
  captureSessionControllerStop,
  stopSession,
  type SessionStopHookContext,
} from "../../sessions/session-controller.stop.js";
import { parseAgentSessionKey } from "../../sessions/session-key-utils.js";
import { resolveStateContentionPresentation } from "../../sessions/session-run-error-presentation.js";
import {
  waitForChatAbortAcknowledgment,
  waitForChatAbortTerminalPersistence,
} from "../chat-abort-lifecycle-internal.js";
import { captureWorkerInferenceForSession, createChatAbortOps } from "../chat-abort-ops.js";
import { abortChatRunById, captureChatRunAbortPresentation } from "../chat-abort.js";
import { formatStopRequest } from "../control-plane-audit.js";
import {
  resolveRequestedSessionAgentId,
  tryResolveSessionCompatibilityOwnerAgentId,
} from "../session-request-agent.js";
import { loadSessionEntry, resolveSessionStoreKey } from "../session-utils.js";
import { getWorkerInferenceSessionControl } from "../worker-environments/inference-control-internal.js";
import {
  resolveChatAbortTargetRejection,
  resolveChatAbortRequester,
} from "./chat-abort-authorization.js";
import {
  abortChatRunsForSessionKeyWithPartials,
  abortControlledSubagents,
  descendantAbortError,
} from "./chat-abort-runtime.js";
import {
  abortedPartialPersistenceError,
  captureAbortedPartial,
  deferAbortedPartialPersistence,
  withAbortedPartialPersistenceWarning,
} from "./chat-aborted-partial.js";
import { persistAbortedPartials } from "./chat-transcript-persistence.js";
import { emitSessionsChanged } from "./session-change-event.js";
import { readGatewayRequestMutationAuthority } from "./session-mutation-guards.js";
import type { GatewayRequestHandlerOptions } from "./types.js";
import { assertValidParams } from "./validation.js";

type ChatAbortLifecycle = {
  onDescendantsCancelled?: () => void;
  cascadeDescendants?: true;
  hookContext?: SessionStopHookContext;
};

export async function handleChatAbortRequestWithLifecycle(
  options: GatewayRequestHandlerOptions,
  lifecycle: ChatAbortLifecycle = {},
): Promise<void> {
  const { params, respond, context, client, sessionMutationAuthorization } = options;
  const authority = readGatewayRequestMutationAuthority(options);
  const requester = resolveChatAbortRequester(client, sessionMutationAuthorization);
  const assertCurrent = () => {
    authority.assertCurrent();
    sessionMutationAuthorization?.assertCurrent();
    requester.sessionAuthority?.assertCurrent();
  };
  if (!assertValidParams(params, validateChatAbortParams, "chat.abort", respond)) {
    return;
  }
  const { sessionKey: rawSessionKey, runId, preserveSideRuns, discardPendingInput } = params;
  if (discardPendingInput && !runId) {
    respond(
      false,
      undefined,
      errorShape(ErrorCodes.INVALID_REQUEST, "discardPendingInput requires an exact runId"),
    );
    return;
  }
  const agentIdOverride = normalizeOptionalString(params.agentId);
  const abortCfg = context.getRuntimeConfig();
  const parsedAbortSessionKey = parseAgentSessionKey(rawSessionKey);
  const compatibilityDefaultAgentId = tryResolveSessionCompatibilityOwnerAgentId(
    abortCfg,
    rawSessionKey,
  );
  const inferredSessionAgentId =
    !agentIdOverride && parsedAbortSessionKey
      ? normalizeAgentId(parsedAbortSessionKey.agentId)
      : undefined;
  const bareSessionAgentResolution = !parsedAbortSessionKey
    ? resolveRequestedSessionAgentId(abortCfg, rawSessionKey, agentIdOverride)
    : undefined;
  if (bareSessionAgentResolution && !bareSessionAgentResolution.ok) {
    respond(false, undefined, bareSessionAgentResolution.error);
    return;
  }
  const abortAgentId = parsedAbortSessionKey
    ? (agentIdOverride ?? inferredSessionAgentId)
    : bareSessionAgentResolution?.agentId;
  if (!abortAgentId) {
    respond(
      false,
      undefined,
      errorShape(
        ErrorCodes.INVALID_REQUEST,
        rawSessionKey.trim().toLowerCase() === "global"
          ? "agentId is required for global chat.abort when no compatibility owner exists"
          : "agentId is required for unscoped chat.abort when no compatibility owner exists",
      ),
    );
    return;
  }
  if (
    agentIdOverride &&
    parsedAbortSessionKey &&
    normalizeAgentId(parsedAbortSessionKey.agentId) !== normalizeAgentId(agentIdOverride)
  ) {
    respond(
      false,
      undefined,
      errorShape(
        ErrorCodes.INVALID_REQUEST,
        `agentId "${agentIdOverride}" does not match session key "${rawSessionKey}"`,
      ),
    );
    return;
  }
  const canonicalAbortSessionKey = resolveSessionStoreKey({
    cfg: abortCfg,
    sessionKey: rawSessionKey,
    storeAgentId: abortAgentId,
  });
  if (discardPendingInput && isIncognitoSessionKey(canonicalAbortSessionKey)) {
    respond(
      false,
      undefined,
      errorShape(
        ErrorCodes.INVALID_REQUEST,
        "Removing accepted queued input is unavailable in incognito sessions. Use Stop to cancel it.",
      ),
    );
    return;
  }
  const narrow =
    authority.sessionScope === "operator.sessions.write" ||
    requester.sessionAuthority !== undefined;
  const admittedTarget = sessionMutationAuthorization?.admittedTarget;
  if (
    narrow &&
    (!admittedTarget?.sessionId.trim() ||
      admittedTarget.sessionKey !== canonicalAbortSessionKey ||
      admittedTarget.agentId !== normalizeAgentId(abortAgentId))
  ) {
    respond(
      false,
      undefined,
      errorShape(ErrorCodes.INVALID_REQUEST, "session target is unavailable"),
    );
    return;
  }
  const requiredSessionId = narrow ? admittedTarget?.sessionId : undefined;
  const ops = createChatAbortOps(context);

  const abortSession: Result<ReturnType<typeof loadSessionEntry>, unknown> = (() => {
    try {
      return {
        ok: true,
        value: loadSessionEntry(canonicalAbortSessionKey, { agentId: abortAgentId }),
      };
    } catch (error) {
      return { ok: false, error };
    }
  })();
  const abortSessionEntry = abortSession.ok ? abortSession.value.entry : undefined;
  const controllerTargets =
    abortSession.ok && abortSession.value.storePath
      ? [
          captureSessionTarget({
            storeScope: abortSession.value.storePath,
            sessionKey: canonicalAbortSessionKey,
            aliases: canonicalAbortSessionKey === rawSessionKey ? undefined : [rawSessionKey],
            agentId: abortAgentId,
            incarnation: abortSessionEntry?.sessionId,
          }),
        ]
      : [];
  const stopHookContext = lifecycle.hookContext ?? {
    sessionKey: canonicalAbortSessionKey,
    sessionEntry: abortSessionEntry,
    sessionId: abortSessionEntry?.sessionId,
    commandSource: "gateway:chat.abort",
    senderId: requester.deviceId ?? requester.connId,
  };
  if (!runId) {
    const res = await abortChatRunsForSessionKeyWithPartials({
      context,
      ops,
      sessionKey: canonicalAbortSessionKey,
      sessionKeyAliases: canonicalAbortSessionKey === rawSessionKey ? undefined : [rawSessionKey],
      agentId: abortAgentId,
      sessionId: abortSessionEntry?.sessionId,
      requiredSessionId,
      session: abortSession,
      defaultAgentId: compatibilityDefaultAgentId,
      abortOrigin: "rpc",
      stopReason: "rpc",
      requester,
      stopSource: "client-session",
      controllerTargets,
      hookContext: stopHookContext,
      assertCurrent,
      preserveSideRuns,
      cascadeDescendants: lifecycle.cascadeDescendants,
    });
    if (res.unauthorized) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "unauthorized"));
      return;
    }
    if (res.descendants?.killed) {
      lifecycle.onDescendantsCancelled?.();
    }
    const error = res.error ?? descendantAbortError(res.descendants, "Session");
    if (error) {
      respond(false, undefined, withAbortedPartialPersistenceWarning(error, res.warning));
      return;
    }
    respond(true, {
      ok: true,
      aborted: res.aborted,
      runIds: res.runIds,
      ...(res.warning ? { warning: res.warning } : {}),
    });
    return;
  }
  const normalizedAgentIdOverride = normalizeAgentId(abortAgentId);
  // Runs the command Stop sequence when no exact run source was captured.
  const stopWithoutCapturedRun = (reason?: string) =>
    stopSession({
      source: "client-run",
      capture: captureSessionControllerStop({}),
      assertCurrent,
      reason,
      hookContext: stopHookContext,
    }).completed;
  const authorizeRunTarget = (
    target: Parameters<typeof resolveChatAbortTargetRejection>[0]["target"],
  ): boolean => {
    const rejection = resolveChatAbortTargetRejection({
      target,
      requester,
      requestedSessionKey: rawSessionKey,
      canonicalSessionKey: canonicalAbortSessionKey,
      requestedAgentId: normalizedAgentIdOverride,
      defaultAgentId: compatibilityDefaultAgentId,
      requiredSessionId,
      discardPendingInput,
      narrow,
    });
    if (rejection) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, rejection));
    }
    return rejection === undefined;
  };

  // Capture the exact input once; an await must never select a successor by run ID.
  const targetMatches = new Set(
    controllerTargets.flatMap((target) => {
      const source = getRpcSourceForTarget(runId, target);
      return source ? [source] : [];
    }),
  );
  const active =
    getRpcSource(runId) ??
    (targetMatches.size === 1 ? targetMatches.values().next().value : undefined);
  const reserved = active ? undefined : getReservedRpcSourceInput(runId);
  const activeIdentity = active
    ? getRpcSourceIdentity(active)
    : reserved
      ? getSessionControllerSourceIdentity(reserved)
      : undefined;
  const workerTarget = getWorkerInferenceSessionControl(
    context.workerEnvironmentService,
  )?.resolveSessionTargetForRunId(runId);

  const workerCancellation = captureWorkerInferenceForSession({
    context,
    sessionId: activeIdentity?.sessionId ?? workerTarget?.sessionId ?? abortSessionEntry?.sessionId,
    runId,
  });
  let inputWithdrawn = false;
  if (discardPendingInput) {
    if (
      active &&
      !authorizeRunTarget({ ...getRpcSourceIdentity(active), requester: active.adapter.requester })
    ) {
      return;
    }
    if (
      !active ||
      !isRpcSourceQueued(active) ||
      active.input.phase === "injecting" ||
      active.input.claim ||
      active.input.retirementRequested
    ) {
      await stopWithoutCapturedRun();
      respond(true, { ok: true, aborted: false, runIds: [] });
      return;
    }
    const withdrawalScope = getRpcSourceIdentity(active);
    const withdrawalTarget = active.input.target;
    const captured = {
      ...withdrawalScope,
      agentId: withdrawalScope.agentId ?? abortAgentId,
    };
    const hold = holdSessionControllerSourceWithdrawal(active.input);
    try {
      if (!abortSession.ok) {
        throw abortSession.error;
      }
      inputWithdrawn = await discardSessionPendingInput(
        { ...captured, storePath: abortSession.value.storePath },
        runId,
        () => {
          assertCurrent();
          const currentIdentity = getRpcSourceIdentity(active);
          // The write retains this source and physical owner across each awaited phase.
          if (
            !isRpcSourceRegistered(active) ||
            !isRpcSourceQueued(active) ||
            !withdrawalTarget ||
            active.input.target !== withdrawalTarget ||
            currentIdentity.sessionKey !== withdrawalScope.sessionKey ||
            currentIdentity.sessionId !== withdrawalScope.sessionId ||
            currentIdentity.agentId !== withdrawalScope.agentId
          ) {
            throw new Error("Run changed before input removal; refresh and retry.");
          }
        },
      );
      if (inputWithdrawn) {
        active.adapter.abortStopReason = "rpc";
        inputWithdrawn = hold.commit("rpc");
        if (inputWithdrawn) {
          emitSessionsChanged(
            context,
            { ...captured, reason: "agent.input.settled" },
            { accessChanged: false },
          );
          await captureSessionControllerSourceSettlement(active.input);
        }
      }
    } finally {
      hold();
    }
    respond(true, { ok: true, aborted: inputWithdrawn, runIds: inputWithdrawn ? [runId] : [] });
    return;
  }
  const workerRunIds = new Set<string>();
  let workerSettlement: Promise<string[]> | undefined;
  const cancelWorker = () => {
    if (!requester.isAdmin || !workerCancellation?.runIds.length) {
      return;
    }
    assertCurrent();
    const settlement = workerCancellation.cancel({
      assertCurrent,
      onCancelled: (id) => workerRunIds.add(id),
    });
    workerSettlement = settlement;
    void settlement.catch(() => undefined);
  };
  const respondWithWorkerRuns = async (localRunIds: string[], warning?: string): Promise<void> => {
    await workerSettlement;
    const runIds = new Set([...localRunIds, ...workerRunIds]);
    if (!abortSession.ok) {
      throw abortSession.error;
    }
    respond(true, {
      ok: true,
      aborted: runIds.size > 0,
      runIds: [...runIds],
      ...(warning ? { warning } : {}),
    });
  };
  if (!active) {
    if (reserved && activeIdentity) {
      if (
        !authorizeRunTarget({
          ...activeIdentity,
          requester: reserved.sourceAdapter?.requester,
        })
      ) {
        return;
      }
      let aborted = false;
      let descendants: Awaited<ReturnType<typeof abortControlledSubagents>> | undefined;
      const stopped = stopSession({
        source: "client-run",
        capture: captureSessionControllerStop({ inputs: [reserved] }),
        assertCurrent,
        reason: "rpc",
        hookContext: {
          ...stopHookContext,
          sessionKey: activeIdentity.sessionKey,
          sessionId: activeIdentity.sessionId,
        },
        afterParent: cancelWorker,
        onCancelled: (target) => {
          if (target === reserved) {
            aborted = true;
          }
        },
        stopChildren: async (applyParentStop) => {
          // The settle producer can still own the registry operation needed for
          // descendant cleanup. Cancel its captured source before entering that owner.
          await applyParentStop();
          descendants = await abortControlledSubagents({
            cfg: abortCfg,
            sessionKey: activeIdentity.sessionKey,
            agentId: activeIdentity.agentId,
            requesterTurnRunId: runId,
          });
          return {
            stopped: descendants?.killed ?? 0,
            failed: descendants?.status === "error" ? descendants.failed : 0,
          };
        },
      });
      await stopped.completed;
      const descendantError = descendantAbortError(descendants, "Parent run");
      if (descendantError) {
        respond(false, undefined, descendantError);
        return;
      }
      await respondWithWorkerRuns(aborted ? [runId] : []);
      return;
    }
    if (!workerCancellation?.runIds.length) {
      if (!abortSession.ok) {
        throw abortSession.error;
      }
      await stopWithoutCapturedRun();
      respond(true, { ok: true, aborted: false, runIds: [] });
      return;
    }
    if (!requester.isAdmin) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "unauthorized"));
      return;
    }
    cancelWorker();
    await stopWithoutCapturedRun("rpc");
    await respondWithWorkerRuns([]);
    return;
  }
  if (
    !authorizeRunTarget({ ...getRpcSourceIdentity(active), requester: active.adapter.requester })
  ) {
    return;
  }
  let aborted = false;
  const stopCapture = captureSessionControllerStop({ inputs: [active.input] });
  const presentation = captureChatRunAbortPresentation(ops, runId);
  const { sessionKey, sessionId, agentId } = getRpcSourceIdentity(active);
  const { controlUiVisible } = active.adapter;
  assertCurrent();
  const partialText = context.chatRunState.resolveBuffer(runId, { final: true }).text;
  const snapshot =
    controlUiVisible !== false && partialText?.trim()
      ? captureAbortedPartial({
          runId,
          sessionKey,
          sessionId,
          agentId: agentId ?? abortAgentId,
          text: partialText,
          abortOrigin: "rpc",
          resolveTerminalProducer: active.adapter.resolveTerminalProducer,
          ...(sessionKey === rawSessionKey || sessionKey === canonicalAbortSessionKey
            ? { session: abortSession }
            : {}),
        })
      : undefined;
  let descendants: Awaited<ReturnType<typeof abortControlledSubagents>> | undefined;
  let failure: { error: unknown } | undefined;
  let warning: string | undefined;
  try {
    const stopped = stopSession({
      source: "client-run",
      capture: stopCapture,
      assertCurrent,
      reason: "rpc",
      hookContext: { ...stopHookContext, sessionKey, sessionId },
      afterParent: cancelWorker,
      onCancelled: (target) => {
        if (target === active.input) {
          aborted = true;
        }
      },
      cancelInput: (_input, cancel) =>
        abortChatRunById(ops, {
          runId,
          sessionKey,
          expectedEntry: active,
          presentation,
          cancel,
          assertCurrent,
          stopReason: "rpc",
          onAbortPrepared: () => deferAbortedPartialPersistence(snapshot, context),
          onAbortCommitted: () => {
            aborted = true;
          },
        }).aborted,
      stopChildren: async (applyParentStop) => {
        descendants = await abortControlledSubagents({
          cfg: abortCfg,
          sessionKey,
          agentId,
          requesterTurnRunId: runId,
          beforeKill: applyParentStop,
        });
        return {
          stopped: descendants?.killed ?? 0,
          failed: descendants?.status === "error" ? descendants.failed : 0,
        };
      },
    });
    await stopped.completed;
  } catch (error) {
    failure = { error };
  }
  // A later child fence can reject after the parent consumed its buffer. The
  // transcript owner must still settle that already-committed cancellation.
  if (aborted) {
    const settled = await waitForChatAbortAcknowledgment(
      Promise.allSettled([
        snapshot ? persistAbortedPartials({ context, snapshots: [snapshot] }) : undefined,
        active.adapter.kind === "agent" ? undefined : waitForChatAbortTerminalPersistence(active),
      ]),
    );
    warning = settled[0].status === "fulfilled" ? settled[0].value : undefined;
    const errors = settled.flatMap((item) => (item.status === "rejected" ? [item.reason] : []));
    if (errors.length) {
      if (failure) {
        errors.unshift(failure.error);
      }
      throw abortedPartialPersistenceError(
        errors.length === 1
          ? errors[0]
          : new AggregateError(errors, "Chat cancellation and persistence failed"),
        warning,
      );
    }
  }
  if (failure) {
    throw abortedPartialPersistenceError(failure.error, warning);
  }
  if (!abortSession.ok) {
    throw abortedPartialPersistenceError(abortSession.error, warning);
  }
  const descendantError = descendantAbortError(descendants, "Parent run");
  if (descendantError) {
    respond(false, undefined, withAbortedPartialPersistenceWarning(descendantError, warning));
    return;
  }
  try {
    await respondWithWorkerRuns(aborted ? [runId] : [], warning);
  } catch (error) {
    throw abortedPartialPersistenceError(error, warning);
  }
}

export async function handleChatAbortRequest(options: GatewayRequestHandlerOptions): Promise<void> {
  if (validateChatAbortParams(options.params)) {
    options.context.logGateway.info(
      formatStopRequest("chat.abort", options.client, options.params),
    );
  }
  try {
    await handleChatAbortRequestWithLifecycle(options);
  } catch (error) {
    const contention = resolveStateContentionPresentation(error);
    if (!contention) {
      throw error;
    }
    // A session read can fail even though cancellation takes effect. Do not
    // replay Stop or claim it had no effect; preserve uncertainty at the RPC boundary.
    options.respond(
      false,
      undefined,
      errorShape(
        ErrorCodes.UNAVAILABLE,
        "The server is busy. Check this turn's status before trying Stop again.\n\n" +
          "SQLite transaction admission remained busy. Stopping may already have taken effect.",
        { details: { errorKind: contention.errorKind } },
      ),
    );
  }
}
