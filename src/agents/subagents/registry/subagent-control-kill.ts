import { resolveSubagentLabel } from "../../../auto-reply/reply/subagents-utils.js";
import type { OpenClawConfig } from "../../../config/types.openclaw.js";
import { formatErrorMessage } from "../../../infra/errors.js";
import { hasSqliteWorkerOutcomeUnknown } from "../../../infra/sqlite-worker-contract.js";
import { getCurrentSessionControllerOwner } from "../../../sessions/session-controller.context.js";
import {
  SESSION_CONTROLLER_DRAIN_TIMEOUT_MS,
  waitForSessionControllerSettlement,
} from "../../../sessions/session-controller.lifecycle.js";
import { getRpcSource } from "../../../sessions/session-controller.rpc-sources.js";
import { resolveSubagentRequesterAgentId } from "../../subagent-requester-owner.js";
import { mutateSubagentRunForKill } from "./subagent-control-kill-runtime.js";
import {
  withSubagentKillScope,
  type KillTree,
  type KillScope,
  type KillSelection,
  type KillPublicationPreparation,
} from "./subagent-control-kill-scope.js";
import {
  ensureSubagentControllerOwnsRun,
  getLatestOwnedSubagentRun,
  type ResolvedSubagentController,
} from "./subagent-control-scope.js";
import {
  persistSubagentAbortedLastRun,
  type SubagentKillSession,
} from "./subagent-control-session.js";
import type { SubagentAdminKillParams, SubagentAdminKillResult } from "./subagent-control.types.js";
import { SUBAGENT_KILL_TASK_ERROR } from "./subagent-control.types.js";
import { resolveSubagentKillTargetState } from "./subagent-registry-completion.js";
import { captureSubagentExecution } from "./subagent-registry-execution-cleanup.js";
import { getCurrentSubagentRunOwner, subagentRuns } from "./subagent-registry-memory.js";
import {
  listSubagentRunsForController,
  listSubagentRunsForRequester,
} from "./subagent-registry-read.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";
import { owesRequesterCompletion } from "./subagent-requester-settle-identity.js";

async function killSubagentRun(
  params: Parameters<typeof mutateSubagentRunForKill>[0],
): ReturnType<typeof mutateSubagentRunForKill> {
  let captured = captureSubagentExecution(params);
  const stopAcceptance = { accepted: false };
  let result: Awaited<ReturnType<typeof mutateSubagentRunForKill>> = {
    killed: false,
    targetState: resolveSubagentKillTargetState(params.entry),
  };
  let settlementFailure: { error: unknown; settlement: Promise<void> } | undefined;
  try {
    result = await mutateSubagentRunForKill(
      params,
      () => {
        captured ??= captureSubagentExecution(params);
        return captured;
      },
      stopAcceptance,
    );
  } finally {
    // Disposal may mutate the session. Join outside the exclusive mutation while
    // the caller still owns this execution's retirement and scheduler holds.
    captured ??= captureSubagentExecution(params);
    const execution = captured?.execution;
    const settlement = execution?.input.settlement.promise;
    if (
      settlement &&
      getCurrentSessionControllerOwner() !== execution.input.claim?.operation &&
      (result.killed ||
        result.targetState ||
        stopAcceptance.accepted ||
        (!result.declined &&
          (execution.input.abortSignal.aborted || execution.input.retirementRequested)))
    ) {
      try {
        const settled = await waitForSessionControllerSettlement(
          settlement,
          SESSION_CONTROLLER_DRAIN_TIMEOUT_MS,
        );
        if (!settled) {
          settlementFailure = {
            error: new Error(
              "Subagent execution cleanup remains pending after its drain deadline.",
            ),
            settlement,
          };
        }
      } catch (error) {
        settlementFailure = { error, settlement };
      }
    }
  }
  if (!stopAcceptance.accepted) {
    params.cancellationControl.assertCurrent();
  }
  if (captured) {
    const entry = getCurrentSubagentRunOwner(subagentRuns, params.entry) ?? captured.entry;
    const rpcSource = getRpcSource(captured.runId);
    if (
      entry.runId !== captured.runId ||
      (rpcSource !== undefined && rpcSource !== captured.execution)
    ) {
      throw new Error("Subagent execution owner changed during cancellation");
    }
  }
  if (settlementFailure) {
    const message = `Subagent execution settlement failed: ${formatErrorMessage(settlementFailure.error)}`;
    const { settlement } = settlementFailure;
    if (captured?.execution.input.settlement.promise === settlement) {
      result = { ...result, completedCleanupError: message };
    } else {
      result = { ...result, error: [result.error, message].filter(Boolean).join(" ") };
    }
  }
  return result;
}

async function killLatestSubagentRun(params: {
  cfg: OpenClawConfig;
  tree: KillTree;
  scope: KillScope;
  suppressTaskDelivery?: boolean;
  beforeSessionKill?: () => boolean;
  requiredSessionId?: string;
  expectedRunId?: string;
  expectedGeneration?: number;
  expectedOwnerKey?: string;
}): Promise<{
  entry: SubagentRunRecord;
  session?: SubagentKillSession;
  result: Awaited<ReturnType<typeof killSubagentRun>>;
}> {
  const { tree, scope } = params;
  const cancellationControl = {
    assertCurrent: scope.cancellationControl.assertCurrent,
    prepareRead: () => {
      const pending = [scope.cancellationControl.prepareRead?.(), tree.prepareRead()].filter(
        (publication) => publication !== undefined,
      );
      return pending.length > 0 ? Promise.all(pending).then(() => {}) : undefined;
    },
  };
  for (
    let pending = cancellationControl.prepareRead();
    pending;
    pending = cancellationControl.prepareRead()
  ) {
    await pending;
    cancellationControl.assertCurrent();
  }
  const matchesExpected = (entry: SubagentRunRecord) =>
    (params.expectedGeneration === undefined || entry.generation === params.expectedGeneration) &&
    (!params.expectedOwnerKey || entry.requesterSessionKey === params.expectedOwnerKey);
  scope.cancellationControl.assertCurrent();
  const entry = tree.entry;
  const session = tree.session;
  if (!session) {
    return { entry, result: { killed: false } };
  }
  if (params.requiredSessionId !== undefined && !session.entry?.sessionId.trim()) {
    return {
      entry,
      session,
      result: {
        killed: false,
        error: "Subagent session target is unavailable for narrow cancellation.",
      },
    };
  }
  if (!matchesExpected(entry)) {
    return { entry, session, result: { killed: false, superseded: true } };
  }
  const result = tree.isCurrent(entry)
    ? await killSubagentRun({
        ...params,
        entry,
        session,
        requiredSessionId: params.requiredSessionId,
        stateContext: scope.stateContext,
        cancellationControl,
        isCurrent: (candidate, requirePreparedSession) =>
          tree.isCurrent(candidate, requirePreparedSession) && matchesExpected(candidate),
        withdrawQueuedReservation: () => tree.dispatchHold?.withdraw(),
        refreshDescendants: scope.refresh,
      })
    : { killed: false, superseded: true };
  tree.completedCleanupError = result.completedCleanupError;
  // A committed retirement ends mutation/discovery of this ancestor, but not
  // cancellation of its captured descendants. Refusals on a live row stay fenced.
  if (result.superseded && !tree.isCurrent(entry) && tree.canTraverse() && matchesExpected(entry)) {
    return {
      entry: tree.entry,
      session,
      result: {
        killed: false,
        targetState: resolveSubagentKillTargetState(tree.entry),
        ...(result.error !== undefined ? { error: result.error } : {}),
        ...(result.completedCleanupError !== undefined
          ? { completedCleanupError: result.completedCleanupError }
          : {}),
      },
    };
  }
  return { entry: tree.entry, session, result };
}

function collectKillErrors(trees: KillTree[], unlabeledRoot?: KillTree) {
  let failed = 0;
  const errors: string[] = [];
  const collect = (tree: KillTree) => {
    const diagnostics = [...tree.errors];
    if (tree.completedCleanupError) {
      diagnostics.push(tree.completedCleanupError);
    }
    if (diagnostics.length > 0) {
      failed += 1;
      for (const error of diagnostics) {
        errors.push(
          tree === unlabeledRoot ? error : `${resolveSubagentLabel(tree.entry)}: ${error}`,
        );
      }
    }
    tree.children.forEach(collect);
  };
  // No authority checks or I/O here: a later sibling can fault an already-visited node.
  // Failures count stable selected nodes, independent of later replacements and killed counts.
  trees.forEach(collect);
  return { errors, failed };
}

type KillTraversal = {
  cfg: OpenClawConfig;
  scope: KillScope;
  /** A requester-wide Stop or reset cancels the tree; that requester needs no report. */
  suppressTaskDelivery?: boolean;
};

async function visitAll(work: Promise<void>[]): Promise<void> {
  const results = await Promise.allSettled(work);
  for (const result of results) {
    if (result.status === "rejected") {
      throw result.reason;
    }
  }
}

async function killSubagentRunTree(
  params: KillTraversal & { trees: KillTree[]; suppressCompletedWakes?: boolean },
): Promise<{ killed: number; labels: string[] }> {
  const visits = new Map<
    KillTree,
    { label?: string; descendants: boolean; suppressCompletedWakes: boolean }
  >();
  const visit = async (tree: KillTree, suppressCompletedWakes: boolean): Promise<void> => {
    let result = visits.get(tree);
    try {
      if (!result) {
        result = { descendants: false, suppressCompletedWakes };
        visits.set(tree, result);
        if (
          !tree.entry.execution.endedAt ||
          (tree.session &&
            captureSubagentExecution({ entry: tree.entry, session: tree.session }) !== undefined) ||
          tree.entry.pauseReason === "sessions_yield" ||
          (params.suppressTaskDelivery && suppressCompletedWakes && tree.entry.requesterSettleWake)
        ) {
          const stopped = await killLatestSubagentRun({ ...params, tree });
          if (stopped.result.error) {
            tree.errors.add(stopped.result.error);
          }
          if (
            stopped.result.error ||
            stopped.result.completedCleanupError ||
            stopped.result.declined
          ) {
            // A parent's failed Stop cannot retire the completion it still owns.
            // Keep trying live descendants under the existing best-effort policy.
            result.suppressCompletedWakes = false;
          }
          if (stopped.result.killed) {
            result.label = resolveSubagentLabel(stopped.entry);
          }
          if (stopped.result.superseded) {
            return;
          }
        }
        result.descendants = true;
      }
      for (let pending = tree.prepareRead(); pending; pending = tree.prepareRead()) {
        await pending;
      }
      if (result.descendants && tree.canTraverse()) {
        const suppressDescendantWakes = result.suppressCompletedWakes;
        await visitAll(tree.children.map((child) => visit(child, suppressDescendantWakes)));
      }
    } catch (error) {
      if (hasSqliteWorkerOutcomeUnknown(error)) {
        throw error;
      }
      tree.errors.add(formatErrorMessage(error));
      if (result) {
        result.descendants = false;
      }
    }
  };
  let selected: number;
  do {
    selected = await params.scope.refresh();
    // First visits interrupt siblings together; descendants still wait for their parent.
    await visitAll(
      params.trees.map((tree) => visit(tree, params.suppressCompletedWakes !== false)),
    );
    // A sibling's drain can capture children beneath an already visited branch.
    // Complete that frontier before releasing holds, without stopping a session twice.
  } while ((await params.scope.refresh()) !== selected);
  const collectLabels = (trees: KillTree[]): string[] =>
    trees.flatMap((tree) => {
      const label = visits.get(tree)?.label;
      return [...(label === undefined ? [] : [label]), ...collectLabels(tree.children)];
    });
  const labels = collectLabels(params.trees);
  return { killed: labels.length, labels };
}

async function killSubagentRoot(params: Parameters<typeof killLatestSubagentRun>[0]) {
  let stopped: Awaited<ReturnType<typeof killLatestSubagentRun>> = {
    entry: params.tree.entry,
    result: { killed: false },
  };
  let cascade: Awaited<ReturnType<typeof killSubagentRunTree>> = { killed: 0, labels: [] };
  try {
    // Explicit root cancellation also reconciles terminal execution state.
    stopped = await killLatestSubagentRun(params);
    if (stopped.result.error) {
      params.tree.errors.add(stopped.result.error);
    }
    for (let pending = params.tree.prepareRead(); pending; pending = params.tree.prepareRead()) {
      await pending;
    }
    if (!stopped.result.superseded && !stopped.result.declined && params.tree.canTraverse()) {
      // Exact admin constraints belong only to its selected root, not each descendant.
      // Descendants of a cancelled root lose their requester; nothing is reported to it.
      cascade = await killSubagentRunTree({
        cfg: params.cfg,
        suppressTaskDelivery: true,
        suppressCompletedWakes: !stopped.result.error && !stopped.result.completedCleanupError,
        scope: params.scope,
        trees: params.tree.children,
      });
    }
  } catch (error) {
    if (hasSqliteWorkerOutcomeUnknown(error)) {
      throw error;
    }
    params.tree.errors.add(formatErrorMessage(error));
  }
  return { ...stopped, cascade };
}

/** Kills every currently controlled child run and its descendants. */
export async function killAllControlledSubagentRuns(params: {
  cfg: OpenClawConfig;
  controller: ResolvedSubagentController;
  runs: SubagentRunRecord[];
  assertCurrent?: () => void;
  suppressTaskDelivery?: boolean;
  /** False declines traversal; the scope still releases every reservation hold. */
  beforeKill?: () => boolean | Promise<boolean>;
}) {
  if (params.controller.controlScope !== "children") {
    await params.beforeKill?.();
    return {
      status: "forbidden" as const,
      error: "Leaf subagents cannot control other sessions.",
      killed: 0,
      labels: [],
    };
  }
  return killSelectedSubagentRuns(params);
}

/** Lifecycle cleanup owns both the completion requester and its separately scoped controller. */
export async function killSessionSubagentRuns(params: {
  cfg: OpenClawConfig;
  sessionKey: string;
  agentId: string;
  assertCurrent?: () => void;
  beforeKill?: () => boolean | Promise<boolean>;
}) {
  const controller = { controllerSessionKey: params.sessionKey, controllerAgentId: params.agentId };
  return killSelectedSubagentRuns({
    cfg: params.cfg,
    assertCurrent: params.assertCurrent,
    beforeKill: params.beforeKill,
    runs: [
      ...listSubagentRunsForRequester(params.sessionKey, { requesterAgentId: params.agentId }),
      ...listSubagentRunsForController(params.sessionKey, params.agentId),
    ],
    // Ordinary controller mutations retain their narrower authority. Only an admitted
    // lifecycle boundary can retire work whose completion belongs to this session.
    ownsRoot: (entry) =>
      !ensureSubagentControllerOwnsRun({ cfg: params.cfg, controller, entry }) ||
      (entry.requesterSessionKey === params.sessionKey &&
        resolveSubagentRequesterAgentId(params.cfg, entry) === params.agentId),
    suppressTaskDelivery: true,
  });
}

async function killSelectedSubagentRuns(
  params: KillSelection & {
    suppressTaskDelivery?: boolean;
    beforeKill?: () => boolean | Promise<boolean>;
  },
) {
  const result = await withSubagentKillScope(params, async (scope, trees) => {
    const accepted = params.beforeKill ? await params.beforeKill() : true;
    if (accepted) {
      await scope.refresh();
    }
    const acceptedTrees = accepted ? trees : [];
    // The bulk signal was consumed above; never forward caller hooks into child kills.
    const stopped = await killSubagentRunTree({
      cfg: params.cfg,
      suppressTaskDelivery: params.suppressTaskDelivery,
      trees: acceptedTrees,
      scope,
    });
    return { ...stopped, ...collectKillErrors(acceptedTrees) };
  });
  if (result.errors.length > 0) {
    return {
      status: "error" as const,
      error: result.errors.join("; "),
      failed: result.failed,
      killed: result.killed,
      labels: result.labels,
    };
  }
  return { status: "ok" as const, killed: result.killed, labels: result.labels };
}

/** Admin kill path for a subagent session key, bypassing caller ownership checks. */
export async function killSubagentRunAdmin(
  params: SubagentAdminKillParams,
  control?: {
    assertCurrent: () => void;
    prepareRead?: () => Promise<void> | undefined;
    beforeSessionKill?: () => boolean;
    requiredSessionId?: string;
    preparePublication?: KillPublicationPreparation<SubagentAdminKillResult>;
  },
): Promise<SubagentAdminKillResult> {
  const publish = (result: SubagentAdminKillResult): SubagentAdminKillResult => {
    if (params.onResult?.(result) !== undefined) {
      throw new TypeError("Subagent cancellation publication must be synchronous.");
    }
    return result;
  };
  const targetSessionKey = params.sessionKey.trim();
  if (!targetSessionKey) {
    return publish({ found: false as const, killed: false as const });
  }
  const entry = getLatestOwnedSubagentRun(targetSessionKey, params.agentId, params.cfg);
  if (!entry) {
    return publish({ found: false as const, killed: false as const });
  }
  const expectedRunId = params.expectedRunId?.trim();
  const expectedTaskRunId = params.expectedTaskRunId?.trim();
  if (
    (expectedRunId && entry.runId !== expectedRunId) ||
    (expectedTaskRunId && (entry.taskRunId ?? entry.runId) !== expectedTaskRunId)
  ) {
    return publish({ found: false as const, killed: false as const });
  }
  if (
    (params.expectedGeneration !== undefined && entry.generation !== params.expectedGeneration) ||
    (params.expectedOwnerKey?.trim() &&
      entry.requesterSessionKey !== params.expectedOwnerKey.trim())
  ) {
    return publish({ found: false as const, killed: false as const });
  }

  let rootStopSuperseded = false;
  return withSubagentKillScope<SubagentAdminKillResult>(
    {
      cfg: params.cfg,
      runs: [entry],
      assertCurrent: control?.assertCurrent,
      prepareRead: control?.prepareRead,
    },
    async (scope, [tree]) => {
      if (!tree) {
        return { found: false as const, killed: false as const };
      }
      const stopped = await killSubagentRoot({
        cfg: params.cfg,
        tree,
        scope,
        beforeSessionKill: control?.beforeSessionKill,
        requiredSessionId: control?.requiredSessionId,
        // Resolve stable task identity once; a later replacement must not inherit this Stop.
        expectedRunId: expectedRunId || (expectedTaskRunId ? entry.runId : undefined),
        expectedGeneration: params.expectedGeneration,
        expectedOwnerKey: params.expectedOwnerKey?.trim() || undefined,
      });
      const { result: stopResult, cascade } = stopped;
      rootStopSuperseded = stopResult.superseded === true;
      // Descendant cleanup can yield long enough for the target run to finish.
      // Return the freshest registry state so task cancellation cannot make a stale kill sticky.
      const targetState = resolveSubagentKillTargetState(tree.entry) ?? stopResult.targetState;
      const killedTarget =
        targetState?.state === "terminal" &&
        targetState.task.status === "cancelled" &&
        targetState.task.error === SUBAGENT_KILL_TASK_ERROR;
      const stopResultAlreadyClearedAbort =
        stopResult.targetState !== undefined &&
        !(
          stopResult.targetState.state === "terminal" &&
          stopResult.targetState.task.status === "cancelled" &&
          stopResult.targetState.task.error === SUBAGENT_KILL_TASK_ERROR
        );
      const resolved = stopped.session;
      if (targetState && !killedTarget && !stopResultAlreadyClearedAbort && resolved) {
        await persistSubagentAbortedLastRun({
          childSessionKey: targetSessionKey,
          storePath: resolved.storePath,
          hasSessionEntry: resolved.entry !== undefined,
          expectedSessionId: resolved.entry?.sessionId,
          expectedLifecycleRevision: resolved.entry?.lifecycleRevision,
          abortedLastRun: false,
          isCurrent: () => tree.isCurrent(stopped.entry),
        });
      }

      return {
        found: true as const,
        killed: stopResult.killed || cascade.killed > 0,
        runId: stopped.entry.runId,
        sessionKey: stopped.entry.childSessionKey,
        cascadeKilled: cascade.killed,
        cascadeLabels: cascade.killed > 0 ? cascade.labels : undefined,
      };
    },
    (result, [tree]) => {
      if (!result.found || !tree) {
        return result;
      }
      // Completion can commit during the awaited handoff. Fence both the retained
      // run and its session incarnation before any synchronous result publication.
      const ownsOutcome = !rootStopSuperseded && tree.ownsRun() && tree.canTraverse();
      if (!ownsOutcome) {
        tree.errors.add("Subagent ownership changed during cancellation; retry.");
      }
      const targetState = ownsOutcome ? resolveSubagentKillTargetState(tree.entry) : undefined;
      const { errors } = collectKillErrors([tree], tree);
      return {
        ...result,
        ...(targetState ? { targetState } : {}),
        ...(errors.length > 0 ? { error: errors.join("; ") } : {}),
      };
    },
    control?.preparePublication,
    publish,
  );
}

/**
 * Deleting a child session stops the work it still owes its requester, which hears of it
 * once like any other kill. Rows that owe nothing, such as a collector its own spawn is
 * cleaning up, stay with their owner.
 */
export async function retireDeletedSubagentSession(
  params: SubagentAdminKillParams,
  control: Parameters<typeof killSubagentRunAdmin>[1],
): Promise<SubagentAdminKillResult> {
  const entry = getLatestOwnedSubagentRun(params.sessionKey.trim(), params.agentId, params.cfg);
  if (!entry || !owesRequesterCompletion(entry)) {
    return { found: false, killed: false };
  }
  return await killSubagentRunAdmin(params, control);
}
