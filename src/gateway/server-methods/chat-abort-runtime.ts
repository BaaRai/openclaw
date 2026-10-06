import { uniqueStrings } from "@openclaw/normalization-core/string-normalization";
import {
  ErrorCodes,
  errorShape,
  type ErrorShape,
} from "../../../packages/gateway-protocol/src/index.js";
import { killSubagentRunAdmin } from "../../agents/subagents/registry/subagent-control-kill.js";
import { ensureSubagentControllerOwnsRun } from "../../agents/subagents/registry/subagent-control-scope.js";
import {
  getCurrentSubagentRunOwner,
  subagentRuns,
} from "../../agents/subagents/registry/subagent-registry-memory.js";
import {
  getLatestLiveSubagentRunByChildSessionKey,
  isSubagentRunQueued,
} from "../../agents/subagents/registry/subagent-registry-read.js";
import { isAgentEventLifecycleGenerationCurrent } from "../../infra/agent-events.js";
import { inputMatchesSessionId } from "../../sessions/session-controller.lifecycle-projections.js";
import type { SessionTarget } from "../../sessions/session-controller.lifecycle.js";
import { captureSessionControllerSourceSettlement } from "../../sessions/session-controller.mailbox.js";
import {
  getRpcSource,
  getRpcSourceIdentity,
  getRpcSourceLifecycleGeneration,
  getRpcSourceProjectSessionActive,
  getSessionControllerSourceIdentity,
  isRpcSourceRegistered,
  listRpcSourceEntries,
  type RpcSourceRef,
} from "../../sessions/session-controller.rpc-sources.js";
import {
  captureSessionControllerStop,
  stopSession,
  type SessionStopRequest,
  type SessionStopHookContext,
  type SessionStopExecution,
  type SessionStopSource,
} from "../../sessions/session-controller.stop.js";
import {
  waitForChatAbortAcknowledgment,
  waitForChatAbortTerminalPersistence,
} from "../chat-abort-lifecycle-internal.js";
import { captureWorkerInferenceForSession, createChatAbortOps } from "../chat-abort-ops.js";
import {
  abortChatRunById,
  captureChatRunAbortPresentation,
  type ChatAbortOps,
} from "../chat-abort.js";
import { resolveChatRunOwnerAgentId } from "../chat-run-owner.js";
import { errorShapeFromError } from "../error-shape.js";
import { getSessionRowProjection } from "../session-row-projection-access.js";
import { resolveSessionStoreKey } from "../session-utils.js";
import type { WorkerInferenceCancellation } from "../worker-environments/inference-control-internal.js";
import {
  canRequesterAbortChatRun,
  resolveAuthorizedRunsForSessionKeys,
  resolveAuthorizedQueuedTurnsForSession,
  type ChatAbortRequester,
} from "./chat-abort-authorization.js";
import { abortControlledSubagents } from "./chat-abort-descendants.js";
import {
  createQueuedCollectorPublication,
  getQueuedCollectorCancellationRunId,
} from "./chat-abort-queued-collector-publication.js";
import {
  abortedPartialPersistenceError,
  captureAbortedPartial,
  deferAbortedPartialPersistence,
  withQueuedCollectorWarning,
  type QueuedCollectorAbortOutcome,
  type ChatAbortOrigin,
  type ChatAbortSessionSnapshot,
} from "./chat-aborted-partial.js";
import { persistAbortedPartials } from "./chat-transcript-persistence.js";
import type { GatewayRequestContext } from "./types.js";

export { abortControlledSubagents, descendantAbortError } from "./chat-abort-descendants.js";

/** Stops an unstarted collector through its scheduler owner before controller cancellation. */
export function abortQueuedCollectorSession(
  params: Omit<ChatSessionAbortParams, "ops"> & { runId?: string },
): Promise<QueuedCollectorAbortOutcome> | undefined {
  const entry = getLatestLiveSubagentRunByChildSessionKey(
    params.sessionKey,
    undefined,
    params.agentId,
  );
  if (!entry || !isSubagentRunQueued(entry) || (params.runId && entry.runId !== params.runId)) {
    return undefined;
  }
  const cfg = params.session?.ok
    ? params.session.value.cfg
    : (params.context.getRuntimeConfig() ?? {});
  // The queued child does not grant cancellation authority. Capture its live
  // parent source and carry that exact requester claim through the awaited kill.
  const parentRunId = entry.requesterTurnRunId;
  const parentSource = parentRunId ? getRpcSource(parentRunId) : undefined;
  const parentKey = entry.controllerSessionKey?.trim() || entry.requesterSessionKey;
  const controller = {
    controllerSessionKey: parentKey,
    controllerAgentId: resolveChatRunOwnerAgentId({
      sessionKey: parentKey,
      defaultAgentId: entry.requesterAgentId,
    }),
  };
  const assertCurrent = () => {
    params.assertCurrent?.();
    // Registry generation and controller scope protect the child reservation;
    // the source checks below independently protect its parent requester.
    const current = getCurrentSubagentRunOwner(subagentRuns, entry);
    if (!current || (current.execution.status === "queued" && !isSubagentRunQueued(current))) {
      throw new Error("Queued collector reservation changed; retry Stop.");
    }
    const ownershipError = ensureSubagentControllerOwnsRun({ cfg, controller, entry: current });
    if (ownershipError) {
      throw new Error(ownershipError);
    }
    if (params.requester.isAdmin) {
      return;
    }
    const parentIdentity = parentSource && getRpcSourceIdentity(parentSource);
    const parentLifecycleGeneration = parentSource && getRpcSourceLifecycleGeneration(parentSource);
    if (
      !parentRunId ||
      !parentSource ||
      !parentIdentity ||
      !isRpcSourceRegistered(parentSource) ||
      parentSource.input.phase === "consumed" ||
      parentSource.input.retirementRequested ||
      parentSource.input.custody.cancellationRetired ||
      parentSource.input.abortSignal.aborted ||
      !parentLifecycleGeneration ||
      !isAgentEventLifecycleGenerationCurrent(parentLifecycleGeneration) ||
      getRpcSourceProjectSessionActive(parentSource) === false ||
      resolveSessionStoreKey({
        cfg,
        sessionKey: parentIdentity.sessionKey,
        storeAgentId: controller.controllerAgentId,
      }) !== parentKey ||
      resolveChatRunOwnerAgentId(parentIdentity) !== controller.controllerAgentId ||
      !canRequesterAbortChatRun(
        { ...parentIdentity, requester: parentSource.adapter.requester },
        params.requester,
        { requireOwnerMatch: true },
      )
    ) {
      throw new Error(
        "Unauthorized queued collector Stop; use its active parent requester connection or an administrator.",
      );
    }
  };
  return (async () => {
    let plan: ReturnType<typeof prepareChatSessionAbort> | undefined;
    let blocked: ErrorShape | undefined;
    let outcome: QueuedCollectorAbortOutcome = {
      ok: false,
      error: errorShape(
        ErrorCodes.UNAVAILABLE,
        "Queued collector cancellation was not published; retry Stop.",
      ),
    };
    let failure: { error: unknown } | undefined;
    try {
      assertCurrent();
      const projection = getSessionRowProjection(params.context);
      if (projection) {
        do {
          await projection.ensureMaterialized();
        } while (projection.needsMaterialization);
      }
      const publication = createQueuedCollectorPublication({
        context: params.context,
        sessionKey: params.sessionKey,
        agentId: params.agentId,
        sessionId: params.sessionId,
        defaultAgentId: params.defaultAgentId,
        projection,
        canPublish: () =>
          blocked === undefined && (!plan || (!plan.result.unauthorized && plan.canCascade)),
      });
      await killSubagentRunAdmin(
        {
          cfg,
          sessionKey: params.sessionKey,
          agentId: params.agentId,
          expectedRunId: entry.runId,
          expectedGeneration: entry.generation,
          expectedOwnerKey: entry.requesterSessionKey,
          onResult: (result) => {
            publication.publishSnapshot(result, true);
            if (blocked) {
              outcome = { ok: false, error: blocked };
              return;
            }
            if (plan?.result.unauthorized) {
              outcome = {
                ok: false,
                error: errorShape(ErrorCodes.INVALID_REQUEST, "unauthorized"),
              };
              return;
            }
            if (result.found && result.error) {
              outcome = { ok: false, error: errorShape(ErrorCodes.UNAVAILABLE, result.error) };
              return;
            }
            if (plan && !plan.canCascade) {
              outcome = {
                ok: false,
                error: errorShape(
                  ErrorCodes.UNAVAILABLE,
                  "Queued collector was not stopped; other session work was preserved. Wait for it to finish or cancel it through its owner, then retry.",
                ),
              };
              return;
            }
            const cancelledRunId = getQueuedCollectorCancellationRunId(result);
            outcome = {
              ok: true,
              value: {
                aborted: cancelledRunId !== undefined || plan?.result.aborted === true,
                runIds: uniqueStrings([
                  ...(cancelledRunId ? [cancelledRunId] : []),
                  ...(plan?.result.runIds ?? []),
                ]),
              },
            };
          },
        },
        {
          assertCurrent,
          requiredSessionId: params.requiredSessionId,
          preparePublication: publication,
          beforeSessionKill: () => {
            plan = prepareChatSessionAbort(
              {
                ...params,
                ops: createChatAbortOps(params.context),
                cascadeDescendants: true,
                includeProtectedRuns: params.runId ? true : params.includeProtectedRuns,
              },
              captureWorkerInferenceForSession({
                context: params.context,
                sessionId: params.sessionId,
              }),
              entry.runId,
            );
            if (params.runId && plan.hasOtherWork) {
              blocked = errorShape(
                ErrorCodes.UNAVAILABLE,
                "Other work is active in this child session; use a full-session Stop.",
              );
              return false;
            }
            plan.abort();
            return plan.canCascade;
          },
        },
      );
    } catch (error) {
      failure = { error };
      outcome = { ok: false, error: errorShapeFromError(ErrorCodes.INVALID_REQUEST, error) };
    }
    if (plan) {
      try {
        const warning = await plan.finish();
        if (warning) {
          outcome = withQueuedCollectorWarning(outcome, warning);
        }
      } catch (error) {
        if (outcome.ok) {
          throw error;
        }
        throw new AggregateError(
          [failure?.error ?? outcome.error, error],
          "Queued collector cancellation and persistence failed",
          { cause: error },
        );
      }
    }
    return outcome;
  })();
}

export type ChatSessionAbortParams = {
  context: GatewayRequestContext;
  ops: ChatAbortOps;
  sessionKey: string;
  sessionKeyAliases?: string[];
  agentId?: string;
  sessionId?: string;
  /** Supplied only by narrow admission, from its original materialized target. */
  requiredSessionId?: string;
  session?: ChatAbortSessionSnapshot;
  defaultAgentId?: string;
  abortOrigin: ChatAbortOrigin;
  stopReason?: string;
  requester: ChatAbortRequester;
  stopSource: SessionStopSource;
  hookContext?: SessionStopHookContext;
  assertCurrent?: () => void;
  preserveSideRuns?: boolean;
  cascadeDescendants?: true;
  /** Exact lifecycle owners may include hidden and side runs for this one session. */
  includeProtectedRuns?: boolean;
  /** Captures exact registrations before cancellation can remove them. */
  onControllerTargets?: (targets: Array<{ runId: string; entry: RpcSourceRef }>) => void;
  /** Runs after authorized synchronous abort, before terminal/partial persistence can yield. */
  onCancellationStarted?: () => void;
  controllerTargets?: readonly SessionTarget[];
};

export type ChatSessionAbortResult = {
  aborted: boolean;
  runIds: string[];
  unauthorized: boolean;
  error?: ErrorShape;
  warning?: string;
  descendants?: Awaited<ReturnType<typeof abortControlledSubagents>>;
};

/** Resolve once at the cancellation boundary; persist captured partials only after Stop. */
function prepareChatSessionAbort(
  params: ChatSessionAbortParams,
  workerCancellation: WorkerInferenceCancellation | undefined,
  selectedRunId?: string,
) {
  const sessionKeys = [params.sessionKey, ...(params.sessionKeyAliases ?? [])];
  const ownerScope = {
    sessionKeys,
    requiredSessionId: params.requiredSessionId,
    agentId: params.agentId,
    defaultAgentId: params.defaultAgentId,
    requester: params.requester,
    preserveSideRuns: params.preserveSideRuns,
    includeProtectedRuns: params.includeProtectedRuns,
  };
  const queuedPlan = resolveAuthorizedQueuedTurnsForSession({
    ...ownerScope,
    sessionId: params.sessionId,
  });
  const {
    authorizedRuns,
    matchedRunIds: matchedActiveRunIds,
    hasUnauthorizedRuns: hasUnauthorizedActiveRuns,
    hasUnauthorizedProtectedRuns: hasUnauthorizedProtectedActiveRuns,
    hasProtectedRuns: hasProtectedActiveRuns,
  } = resolveAuthorizedRunsForSessionKeys({
    ...ownerScope,
    sessionIds: [params.sessionId],
  });
  const hasAuthorizedGatewayRuns = authorizedRuns.length > 0 || queuedPlan.authorized.length > 0;
  const isLifecycleAbort = Boolean(params.cascadeDescendants);
  const hasWorkerRun = Boolean(
    (!hasAuthorizedGatewayRuns || isLifecycleAbort) && workerCancellation?.runIds.length,
  );
  // The worker manager admits at most one active inference per session, and a
  // worker-backed turn shares its controller's runId. One exact match therefore
  // represents the only worker owner instead of inventing a second owner.
  const hasControllerRepresentedWorkerRun =
    hasWorkerRun && matchedActiveRunIds.some((runId) => workerCancellation?.runIds.includes(runId));
  const hasUnauthorizedOwner =
    hasUnauthorizedActiveRuns ||
    queuedPlan.hasUnauthorizedRuns ||
    (hasWorkerRun && !hasControllerRepresentedWorkerRun && !params.requester.isAdmin);
  const hasProtectedLifecycleRuns = hasProtectedActiveRuns || queuedPlan.hasProtectedRuns;
  const hasUnauthorizedProtectedOwner =
    hasUnauthorizedProtectedActiveRuns || queuedPlan.hasUnauthorizedProtectedRuns;
  const hasUnauthorizedLifecycleOwner = isLifecycleAbort && hasUnauthorizedProtectedOwner;
  const canRunLifecycleCleanup = !hasUnauthorizedOwner && !hasProtectedLifecycleRuns;
  // Keep ordinary chat.abort's admin worker behavior; only the injected broad
  // lifecycle path must preserve hidden or explicitly preserved Gateway runs.
  const canCancelWorkerSession = !isLifecycleAbort || !hasProtectedLifecycleRuns;
  const snapshots = authorizedRuns.flatMap(({ runId, entry }) => {
    const text = params.context.chatRunState.resolveBuffer(runId, { final: true }).text;
    const identity = getRpcSourceIdentity(entry);
    return text?.trim()
      ? [
          captureAbortedPartial({
            runId,
            sessionKey: identity.sessionKey,
            sessionId: identity.sessionId,
            agentId: identity.agentId ?? params.agentId,
            text,
            abortOrigin: params.abortOrigin,
            resolveTerminalProducer: entry.adapter.resolveTerminalProducer,
            session: params.session,
          }),
        ]
      : [];
  });
  const capturedTargets = [...queuedPlan.authorized, ...authorizedRuns];
  const targetByInput = new Map(capturedTargets.map((target) => [target.entry.input, target]));
  const presentations = new Map(
    capturedTargets.map(({ runId, entry }) => [
      entry.input,
      captureChatRunAbortPresentation(params.ops, runId),
    ]),
  );
  const controllerStop = params.controllerTargets
    ? captureSessionControllerStop({ targets: params.controllerTargets })
    : undefined;
  const rpcSourceByInput = new Map(listRpcSourceEntries().map(([, entry]) => [entry.input, entry]));
  const stopCapture = captureSessionControllerStop({
    inputs: [...targetByInput.keys(), ...(controllerStop?.inputs ?? [])],
    operations: controllerStop?.operations,
  });
  const sourceSettlements = new Map(
    stopCapture.inputs.map((input) => [input, captureSessionControllerSourceSettlement(input)]),
  );

  let workerCancellationPersistence: Promise<string[]> | undefined;
  let stopExecution: SessionStopExecution | undefined;
  // Reentrant cancellation can revoke the next effect. Keep committed outcomes
  // available to the partial-persistence owner even when abort() then throws.
  const result: ChatSessionAbortResult = { aborted: false, runIds: [], unauthorized: false };
  const recordRun = (runId: string) => {
    result.aborted = true;
    if (!result.runIds.includes(runId)) {
      result.runIds.push(runId);
    }
  };
  const cancelWorker = () => {
    workerCancellationPersistence = workerCancellation?.cancel({
      assertCurrent: params.assertCurrent,
      onCancelled: recordRun,
    });
    // The synchronous abort owner must return before persistence settles. Observe
    // rejection now, but finish() still joins the original operation and its cause.
    void workerCancellationPersistence?.catch(() => undefined);
  };
  const abortAuthorizedRuns = () => {
    params.assertCurrent?.();
    params.onControllerTargets?.([...capturedTargets]);
    if (!hasAuthorizedGatewayRuns) {
      // A persisted session id must not bypass a matching connection or protected run owner.
      if (hasUnauthorizedOwner || hasUnauthorizedLifecycleOwner) {
        result.unauthorized = true;
        return result;
      }
    }
    const onCancelled: NonNullable<SessionStopRequest["onCancelled"]> = (target) => {
      const source = "instance" in target ? targetByInput.get(target) : undefined;
      if (source) {
        recordRun(source.runId);
      } else {
        result.aborted = true;
      }
    };
    const cancelInput: NonNullable<SessionStopRequest["cancelInput"]> = (input, cancel) => {
      const target = targetByInput.get(input);
      if (!target) {
        if (!controllerStop?.inputs.includes(input)) {
          return false;
        }
        params.assertCurrent?.();
        if (!inputMatchesSessionId(input, params.requiredSessionId)) {
          return false;
        }
        const source = rpcSourceByInput.get(input);
        const adapter = source ? source.adapter : input.sourceAdapter;
        if (
          (source && !isRpcSourceRegistered(source)) ||
          (params.includeProtectedRuns !== true &&
            (adapter?.controlUiVisible === false ||
              (params.preserveSideRuns && adapter?.turnKind === "btw"))) ||
          !canRequesterAbortChatRun(
            { ...getSessionControllerSourceIdentity(input), requester: adapter?.requester },
            params.requester,
          )
        ) {
          return false;
        }
        return cancel();
      }
      const { runId, sessionKey, sessionId, agentId, entry } = target;
      const identity = getRpcSourceIdentity(entry);
      if (
        !isRpcSourceRegistered(entry) ||
        identity.sessionKey !== sessionKey ||
        identity.sessionId !== sessionId ||
        identity.agentId !== agentId
      ) {
        return false;
      }
      return abortChatRunById(params.ops, {
        runId,
        sessionKey,
        expectedEntry: entry,
        presentation: presentations.get(input),
        cancel,
        assertCurrent: params.assertCurrent,
        stopReason: params.stopReason,
        onAbortPrepared: () =>
          deferAbortedPartialPersistence(
            snapshots.find((snapshot) => snapshot.runId === runId),
            params.context,
          ),
        onAbortCommitted: () => recordRun(runId),
      }).aborted;
    };
    const afterParent = () => {
      if (!result.unauthorized && !result.error) {
        params.assertCurrent?.();
        if (
          params.requester.isAdmin &&
          canCancelWorkerSession &&
          workerCancellation?.runIds.length
        ) {
          cancelWorker();
        }
        params.onCancellationStarted?.();
      }
    };
    const isCommandStop = ["channel-user", "client-session", "client-run"].includes(
      params.stopSource,
    );
    if (!params.hookContext && isCommandStop) {
      throw new Error(`Stop source ${params.stopSource} requires command hook context`);
    }
    const stopRequest = {
      capture: stopCapture,
      assertCurrent: params.assertCurrent,
      reason: params.stopReason,
      hookContext: params.hookContext,
      onCancelled,
      cancelInput,
      afterParent,
      stopChildren:
        canRunLifecycleCleanup && isCommandStop
          ? async (applyParentStop) => {
              result.descendants = await abortControlledSubagents({
                cfg:
                  (params.session?.ok ? params.session.value.cfg : undefined) ??
                  params.context.getRuntimeConfig() ??
                  {},
                sessionKey: params.sessionKey,
                agentId: params.agentId,
                requesterTurnRunId: selectedRunId,
                beforeKill: applyParentStop,
              });
              return {
                stopped: result.descendants?.killed ?? 0,
                failed: result.descendants?.status === "error" ? result.descendants.failed : 0,
              };
            }
          : undefined,
    } satisfies Omit<SessionStopRequest, "source" | "mutation">;
    stopExecution =
      params.stopSource === "mutation"
        ? stopSession({
            ...stopRequest,
            source: params.stopSource,
            mutation: { cancelQueued: true, stopChildren: false },
          })
        : stopSession({ ...stopRequest, source: params.stopSource });
    return result;
  };
  const hasOtherWork =
    matchedActiveRunIds.some((runId) => runId !== selectedRunId) ||
    queuedPlan.matchedRunIds.some((runId) => runId !== selectedRunId) ||
    (hasWorkerRun && (!selectedRunId || !workerCancellation?.runIds.includes(selectedRunId)));
  return {
    canCascade: canRunLifecycleCleanup && !hasUnauthorizedLifecycleOwner,
    hasOtherWork,
    result,
    abort: abortAuthorizedRuns,
    async finish() {
      let stopFailure: unknown;
      try {
        await stopExecution?.completed;
      } catch (error) {
        // Cancellation can commit for an earlier exact target before authority
        // revocation blocks a later one. Preserve the committed target's output.
        stopFailure = error;
      }
      const abortedRunIds = new Set(result.runIds);
      const handedOffRunIds = new Set(
        snapshots
          .filter((snapshot) => snapshot.ok && snapshot.settlement.deferred)
          .map((snapshot) => snapshot.runId),
      );
      const [worker, partial, terminal] = await waitForChatAbortAcknowledgment(
        Promise.allSettled([
          workerCancellationPersistence,
          result.aborted && snapshots.length > 0
            ? persistAbortedPartials({
                context: params.context,
                snapshots: snapshots.filter((snapshot) => abortedRunIds.has(snapshot.runId)),
              })
            : undefined,
          Promise.all(
            capturedTargets
              .filter(
                ({ runId, entry }) => abortedRunIds.has(runId) && entry.adapter.kind !== "agent",
              )
              .flatMap(({ runId, entry }) => {
                const settlement = sourceSettlements.get(entry.input);
                return settlement !== undefined && !handedOffRunIds.has(runId)
                  ? [waitForChatAbortTerminalPersistence(entry), settlement]
                  : [waitForChatAbortTerminalPersistence(entry)];
              }),
          ),
        ]),
      );

      // A captured session failure can also surface through partial persistence.
      const failures = new Set<unknown>();
      if (stopFailure !== undefined) {
        failures.add(stopFailure);
      }
      for (const settled of [worker, partial, terminal]) {
        if (settled.status === "rejected") {
          failures.add(settled.reason);
        }
      }
      if (params.session && !params.session.ok) {
        failures.add(params.session.error);
      }
      const warning = partial.status === "fulfilled" ? partial.value : undefined;
      if (failures.size > 0) {
        const errors = [...failures];
        throw abortedPartialPersistenceError(
          errors.length === 1
            ? errors[0]
            : new AggregateError(errors, "Chat cancellation persistence failed"),
          warning,
        );
      }
      return warning;
    },
  };
}

export async function abortChatRunsForSessionKeyWithPartials(
  params: ChatSessionAbortParams,
): Promise<ChatSessionAbortResult> {
  if (params.cascadeDescendants) {
    const queuedAbort = abortQueuedCollectorSession(params);
    if (queuedAbort) {
      const result = await queuedAbort;
      return result.ok
        ? { ...result.value, unauthorized: false }
        : { aborted: false, runIds: [], unauthorized: false, error: result.error };
    }
  }
  const plan = prepareChatSessionAbort(params, captureWorkerInferenceForSession(params));
  let result = plan.result;
  let failure: { error: unknown } | undefined;
  try {
    result = plan.abort();
  } catch (error) {
    failure = { error };
  }
  // Cancellation consumed these buffers before awaited descendant work could fail.
  let warning: string | undefined;
  try {
    warning = await plan.finish();
  } catch (error) {
    if (!failure) {
      throw error;
    }
    throw new AggregateError([failure.error, error], "Chat cancellation and persistence failed", {
      cause: error,
    });
  }
  if (failure) {
    throw abortedPartialPersistenceError(failure.error, warning);
  }
  return {
    ...result,
    aborted: result.aborted || Boolean(result.descendants?.killed),
    ...(warning ? { warning } : {}),
  };
}
