import {
  createAgentRunRestartAbortError,
  createAgentRunSupersededAbortError,
} from "../agents/run-termination.js";
import type { SessionEntry } from "../config/sessions/types.js";
import { createInternalHookEvent, triggerInternalHook } from "../hooks/internal-hooks.js";
import type { ReplyOperation } from "./session-controller.contracts.js";
import { waitForSessionControllerSettlement } from "./session-controller.lifecycle-observation.js";
import {
  SESSION_CONTROLLER_DRAIN_TIMEOUT_MS,
  type SessionTarget,
} from "./session-controller.lifecycle.js";
import {
  abortSessionControllerInput,
  captureSessionControllerSourceSettlement,
  type SessionControllerInput,
} from "./session-controller.mailbox.js";
import {
  findSessionControllerEntries,
  isReplyOperationAbortable,
  isCurrentSessionControllerOperation,
  sessionControllers,
} from "./session-controller.state.js";

export type SessionControllerStopCapture = Readonly<{
  inputs: readonly SessionControllerInput[];
  queuedInputs: readonly SessionControllerInput[];
  activeInputs: readonly SessionControllerInput[];
  operations: readonly ReplyOperation[];
  /** Actual captured producer/adoption completion, never a signal or timeout proxy. */
  settled: Promise<void>;
}>;

/** The scope adapter resolves/authorizes targets; this owner captures instances before it yields. */
export function captureSessionControllerStop(params: {
  inputs?: Iterable<SessionControllerInput>;
  operations?: Iterable<ReplyOperation | undefined>;
  targets?: Iterable<SessionTarget>;
  includeQueued?: boolean;
  includeActive?: boolean;
}): SessionControllerStopCapture {
  const inputs = new Set(params.inputs);
  const operations = new Set(
    [...(params.operations ?? [])].filter((operation): operation is ReplyOperation =>
      Boolean(operation),
    ),
  );
  for (const target of params.targets ?? []) {
    for (const owner of findSessionControllerEntries(target.sessionKey, target)) {
      if (params.includeActive !== false && owner.active) {
        operations.add(owner.active);
      }
      for (const input of owner.mailbox?.entries ?? []) {
        if (input.phase === "consumed") {
          continue;
        }
        const selected = Boolean(input.claim);
        if (selected ? params.includeActive !== false : params.includeQueued !== false) {
          inputs.add(input);
        }
      }
    }
  }
  // Collected siblings keep receipt identity, but the latest eligible source owns aggregate Stop.
  const sourceByOperation = new Map<ReplyOperation, SessionControllerInput>();
  const queuedInputs: SessionControllerInput[] = [];
  const selectedInputs: SessionControllerInput[] = [];
  for (const input of inputs) {
    const operation = input.claim?.operation;
    if (!operation) {
      if (!input.custody.cancellationRetired && input.phase !== "consumed") {
        if (input.claim && !input.claim.released) {
          selectedInputs.push(input);
        } else {
          queuedInputs.push(input);
        }
      }
      continue;
    }
    if (input.custody.cancellationRetired) {
      continue;
    }
    operations.add(operation);
    const prior = sourceByOperation.get(operation);
    if (!prior || prior.sequence < input.sequence) {
      sourceByOperation.set(operation, input);
    }
  }
  const activeInputs = [...selectedInputs, ...sourceByOperation.values()];
  const bareOperations = [...operations].filter((operation) => !sourceByOperation.has(operation));
  // Captured retained sources remain in settlement even when cancellation moved to a sibling.
  const raw = [
    ...[...inputs].map(captureSessionControllerSourceSettlement),
    ...[...operations].map((operation) => operation.ownerSettlement),
  ];
  // Defer the aggregate receipt so unselected routing snapshots retain no producer observers.
  let settled: Promise<void> | undefined;
  return Object.freeze({
    inputs: Object.freeze([...inputs]),
    queuedInputs: Object.freeze(queuedInputs),
    activeInputs: Object.freeze(activeInputs),
    operations: Object.freeze(bareOperations),
    get settled() {
      if (!settled) {
        settled = Promise.all(raw).then(() => undefined);
        void settled.catch(() => {});
      }
      return settled;
    },
  });
}

/** Request-local selection for a target discovered by asynchronous routing. */
export function captureSessionControllerStopCandidates() {
  return [...sessionControllers.values()].flatMap((entry) => {
    // Storeless aliases such as `global` overlap agents; group by agent for Stop authority.
    const resources = new Map<
      string | undefined,
      { inputs: SessionControllerInput[]; operations: ReplyOperation[] }
    >();
    const group = (agentId: string | undefined) => {
      const captured = resources.get(agentId) ?? { inputs: [], operations: [] };
      resources.set(agentId, captured);
      return captured;
    };
    for (const input of entry.mailbox?.entries ?? []) {
      if (input.phase !== "consumed") {
        group(
          input.source?.run.agentId ??
            input.claim?.operation?.agentId ??
            input.target?.agentId ??
            entry.target?.agentId,
        ).inputs.push(input);
      }
    }
    if (entry.active) {
      group(entry.active.agentId ?? entry.target?.agentId).operations.push(entry.active);
    }
    return [...resources].map(([agentId, captured]) => ({
      storeScope: entry.target?.storeScope,
      agentId,
      aliases: new Set(entry.aliases),
      capture: captureSessionControllerStop(captured),
    }));
  });
}

export type SessionControllerStopResult = {
  queuedCancelled: number;
  activeCancelled: number;
  abortedInputs: SessionControllerInput[];
  abortedOperations: ReplyOperation[];
  settled: Promise<void>;
  failures: Array<{ target: SessionControllerInput | ReplyOperation; error: unknown }>;
  finalizing: number;
};

export type SessionStopSource =
  | "channel-user"
  | "client-session"
  | "client-run"
  | "mutation"
  | "interrupt"
  | "restart"
  | "watchdog"
  | "operator-revocation"
  | "supersede";

type SessionStopPolicy = Readonly<{
  cancelQueued: boolean;
  stopChildren: boolean;
  recordMessageCutoff: boolean;
  fireCommandHook: boolean;
}>;

const commandStop = {
  cancelQueued: true,
  stopChildren: true,
  recordMessageCutoff: false,
  fireCommandHook: true,
} as const;
const ownerStop = {
  cancelQueued: false,
  stopChildren: false,
  recordMessageCutoff: false,
  fireCommandHook: false,
} as const;
const SESSION_STOP_POLICY = {
  "channel-user": { ...commandStop, recordMessageCutoff: true },
  "client-session": commandStop,
  "client-run": commandStop,
  mutation: ownerStop,
  interrupt: ownerStop,
  restart: { ...ownerStop, cancelQueued: true },
  watchdog: ownerStop,
  "operator-revocation": { ...ownerStop, cancelQueued: true },
  supersede: ownerStop,
} as const satisfies Record<SessionStopSource, SessionStopPolicy>;

export type SessionStopHookContext = Readonly<{
  sessionKey: string;
  sessionEntry?: SessionEntry;
  sessionId?: string;
  commandSource?: string;
  senderId?: string;
}>;

export type SessionStopChildrenResult = Readonly<{ stopped: number; failed: number }>;
type SessionStopTargetStatus = "aborted" | "finalizing" | "unchanged";

/** An active parent owned outside the controller, stopped after its captured controller owners. */
type SessionStopExternalParent = Readonly<{
  stop: () => SessionStopTargetStatus | Promise<SessionStopTargetStatus>;
  settled?: Promise<unknown>;
}>;

type SessionStopOutcome = Readonly<{
  aborted: boolean;
  alreadyFinalizing: boolean;
  queuedCancelled: number;
  activeCancelled: number;
  childrenStopped: number;
  childFailures: number;
  settled: Promise<void>;
  failures: SessionControllerStopResult["failures"];
}>;

export type SessionStopExecution = SessionStopOutcome & {
  /** Joins cutoff persistence, the command hook, and captured child cancellation. */
  completed: Promise<SessionStopOutcome>;
};

type SessionStopRequestBase = {
  source: SessionStopSource;
  /** Captured synchronously by ingress; a resolver may select among captured candidates later. */
  capture: SessionControllerStopCapture | (() => SessionControllerStopCapture);
  assertCurrent?: () => void;
  reason?: unknown;
  messageIdentity?: unknown;
  recordAbortTarget?: (options: { recordCutoff: boolean }) => Promise<void>;
  hookContext?: SessionStopHookContext;
  stopChildren?: (applyParentStop: () => Promise<boolean>) => Promise<SessionStopChildrenResult>;
  externalParent?: SessionStopExternalParent;
  /** Authorize and reserve presentation or partial custody before invoking cancel exactly once. */
  cancelInput?: (input: SessionControllerInput, cancel: () => boolean) => boolean;
  cancelOperation?: (operation: ReplyOperation, cancel: () => boolean) => boolean;
  /** Authorized effects owned by another runtime, sequenced after queued withdrawal. */
  afterQueued?: () => void;
  /** Publication effects that require the captured parent cancellation result. */
  afterParent?: (result: SessionControllerStopResult) => void;
  /** Exact external parents may decline the child cancellation they provisionally captured. */
  continueChildStop?: () => boolean;
  onCancelled?: (target: SessionControllerInput | ReplyOperation) => void;
  onError?: (target: SessionControllerInput | ReplyOperation, error: unknown) => "continue" | void;
};

export type SessionStopRequest = SessionStopRequestBase &
  (
    | {
        source: "mutation";
        mutation: Readonly<{ cancelQueued: boolean; stopChildren: boolean }>;
      }
    | {
        source: Exclude<SessionStopSource, "mutation">;
        mutation?: never;
      }
  );

/** One sequencer for captured Stop. Publication adapters wrap, but never replace, its primitive. */
function applySessionControllerStop(
  capture: SessionControllerStopCapture,
  params: Pick<
    SessionStopRequestBase,
    | "source"
    | "assertCurrent"
    | "reason"
    | "cancelInput"
    | "cancelOperation"
    | "afterQueued"
    | "onCancelled"
    | "onError"
  > & { phase?: "all" | "queued" | "active" },
): SessionControllerStopResult {
  const result: SessionControllerStopResult = {
    queuedCancelled: 0,
    activeCancelled: 0,
    abortedInputs: [],
    abortedOperations: [],
    settled: capture.settled,
    failures: [],
    finalizing: 0,
  };
  const assertCurrent = params.assertCurrent ?? (() => {});
  const reason =
    params.reason ??
    (params.source === "restart"
      ? createAgentRunRestartAbortError()
      : params.source === "supersede"
        ? createAgentRunSupersededAbortError()
        : params.source === "channel-user"
          ? "stop"
          : undefined);
  const once = (
    effect: () => boolean,
    committedAfterFailure: () => boolean,
    record: () => void,
  ) => {
    let called = false;
    let accepted = false;
    return () => {
      if (called) {
        return accepted;
      }
      assertCurrent();
      called = true;
      try {
        accepted = effect();
      } catch (error) {
        // Exact owner records distinguish a failed observer from an uncommitted refusal.
        accepted = committedAfterFailure();
        if (accepted) {
          record();
        }
        throw error;
      }
      if (accepted) {
        record();
      }
      return accepted;
    };
  };
  const countFinalizing = (aborted: boolean, operation: ReplyOperation | undefined) => {
    if (
      !aborted &&
      operation &&
      isCurrentSessionControllerOperation(operation) &&
      !operation.result &&
      (operation.abortFrozen || !isReplyOperationAbortable(operation))
    ) {
      result.finalizing++;
    }
  };
  const cancelSource = (input: SessionControllerInput, queued: boolean) => {
    // Adapters may reserve custody first; this primitive revalidates authority at the side effect.
    assertCurrent();
    const operation = input.claim?.operation;
    const hadResult = Boolean(operation?.result);
    const wasRetiring = input.retirementRequested;
    const wasAborted = input.abortSignal.aborted;
    const cancel = once(
      () => abortSessionControllerInput(input, reason, assertCurrent),
      () =>
        (!hadResult && operation?.result?.kind === "aborted") ||
        (!wasRetiring &&
          input.retirementRequested === true &&
          !wasAborted &&
          input.abortSignal.aborted),
      () => {
        result.abortedInputs.push(input);
        if (queued) {
          result.queuedCancelled++;
        } else {
          result.activeCancelled++;
        }
        params.onCancelled?.(input);
      },
    );
    if (params.cancelInput) {
      params.cancelInput(input, cancel);
    } else {
      cancel();
    }
    if (!queued) {
      countFinalizing(input.abortSignal.aborted, operation);
    }
  };
  const effect = (target: SessionControllerInput | ReplyOperation, run: () => void) => {
    try {
      run();
    } catch (error) {
      result.failures.push({ target, error });
      const committed =
        result.abortedInputs.some((input) => input === target) ||
        result.abortedOperations.some((operation) => operation === target);
      if (params.onError?.(target, error) !== "continue" && !committed) {
        throw error;
      }
    }
  };
  if (params.phase !== "active") {
    for (const input of capture.queuedInputs) {
      effect(input, () => cancelSource(input, true));
    }
    if (params.afterQueued) {
      assertCurrent();
      params.afterQueued();
    }
  }
  if (params.phase === "queued") {
    return result;
  }
  for (const input of capture.activeInputs) {
    effect(input, () => cancelSource(input, false));
  }
  for (const operation of capture.operations) {
    effect(operation, () => {
      assertCurrent();
      const hadResult = Boolean(operation.result);
      let recorded = false;
      const record = () => {
        if (recorded) {
          return;
        }
        recorded = true;
        result.abortedOperations.push(operation);
        result.activeCancelled++;
        params.onCancelled?.(operation);
      };
      const cancel = once(
        () => {
          if (!isCurrentSessionControllerOperation(operation)) {
            return false;
          }
          return params.source === "watchdog" ? operation.abortForStall() : operation.abort(reason);
        },
        () =>
          !hadResult &&
          (operation.result?.kind === "aborted" ||
            (params.source === "watchdog" &&
              operation.result?.kind === "failed" &&
              operation.result.code === "run_stalled")),
        record,
      );
      if (params.cancelOperation) {
        if (params.cancelOperation(operation, cancel)) {
          record();
        }
      } else {
        cancel();
      }
      countFinalizing(operation.abortSignal.aborted, operation);
    });
  }
  return result;
}

/** Exact source teardown cancels its captured input without adopting session-wide Stop policy. */
export function cancelCapturedSessionControllerSource(
  capture: SessionControllerStopCapture,
  params: Omit<Parameters<typeof applySessionControllerStop>[1], "source" | "phase"> = {},
): SessionControllerStopResult {
  return applySessionControllerStop(capture, {
    ...params,
    source: "operator-revocation",
    phase: "all",
  });
}

function resolveStopOutcome(
  result: SessionControllerStopResult,
  external: SessionStopTargetStatus,
  children: SessionStopChildrenResult,
): SessionStopOutcome {
  const externalActiveCancelled = external === "aborted" ? 1 : 0;
  return Object.freeze({
    aborted: result.activeCancelled + externalActiveCancelled > 0,
    alreadyFinalizing: result.finalizing > 0 || external === "finalizing",
    queuedCancelled: result.queuedCancelled,
    activeCancelled: result.activeCancelled + externalActiveCancelled,
    childrenStopped: children.stopped,
    childFailures: children.failed,
    settled: result.settled,
    failures: result.failures,
  });
}

/** Applies the source policy to one captured, caller-authorized stop request. */
export function stopSession(request: SessionStopRequest): SessionStopExecution {
  const sourcePolicy = SESSION_STOP_POLICY[request.source];
  const policy =
    request.source === "mutation"
      ? {
          ...sourcePolicy,
          cancelQueued: request.mutation.cancelQueued,
          stopChildren: request.mutation.stopChildren,
        }
      : sourcePolicy;
  let externalStatus: SessionStopTargetStatus = "unchanged";
  let parentResult: SessionControllerStopResult | undefined;
  const unhandledFailures: unknown[] = [];
  let runPostParent: (() => Promise<void>) | undefined;
  let resolveParentResult!: (result: SessionControllerStopResult) => void;
  let rejectParentResult!: (error: unknown) => void;
  const parentResultReady = new Promise<SessionControllerStopResult>((resolve, reject) => {
    resolveParentResult = resolve;
    rejectParentResult = reject;
  });
  void parentResultReady.catch(() => {});
  const applyParentStop = async (): Promise<boolean> => {
    if (parentResult) {
      return true;
    }
    const capture = typeof request.capture === "function" ? request.capture() : request.capture;
    request.assertCurrent?.();
    const applied = applySessionControllerStop(capture, {
      ...request,
      phase: policy.cancelQueued ? "all" : "active",
      onError: (target, error) => {
        const decision = request.onError?.(target, error);
        if (decision !== "continue") {
          unhandledFailures.push(error);
        }
        return decision;
      },
    });
    parentResult = applied;
    // The external parent stops after controller owners, and Stop settles only after it does.
    const external = request.externalParent;
    const externalStop = external
      ? Promise.resolve(external.stop()).then((status) => {
          externalStatus = status;
        })
      : undefined;
    if (external?.settled) {
      applied.settled = Promise.all([applied.settled, external.settled]).then(() => undefined);
    }
    resolveParentResult(applied);
    request.afterParent?.(applied);
    runPostParent = async () => {
      await externalStop;
      const outcome = resolveStopOutcome(applied, externalStatus, { stopped: 0, failed: 0 });
      if (
        (!outcome.alreadyFinalizing || outcome.activeCancelled > 0) &&
        request.recordAbortTarget
      ) {
        await request.recordAbortTarget({
          recordCutoff: policy.recordMessageCutoff && request.messageIdentity !== undefined,
        });
      }
      if (policy.fireCommandHook) {
        const hookContext = request.hookContext;
        if (!hookContext) {
          throw new Error(`Stop source ${request.source} requires command hook context`);
        }
        await triggerInternalHook(
          createInternalHookEvent("command", "stop", hookContext.sessionKey, {
            sessionEntry: hookContext.sessionEntry,
            sessionId: hookContext.sessionId,
            commandSource: hookContext.commandSource,
            senderId: hookContext.senderId,
          }),
        );
      }
    };
    // Let synchronous cancellation observers publish replacement identity before descendants run.
    await Promise.resolve();
    request.assertCurrent?.();
    return request.continueChildStop?.() ?? true;
  };

  let children: Promise<SessionStopChildrenResult>;
  try {
    children = policy.stopChildren
      ? (request.stopChildren?.(applyParentStop) ??
        applyParentStop().then(() => ({ stopped: 0, failed: 0 })))
      : applyParentStop().then(() => ({ stopped: 0, failed: 0 }));
  } catch (error) {
    rejectParentResult(error);
    throw error;
  }
  const pendingSettlement = parentResultReady.then((result) => result.settled);
  void pendingSettlement.catch(() => {});
  const initial = parentResult
    ? resolveStopOutcome(parentResult, externalStatus, { stopped: 0, failed: 0 })
    : resolveStopOutcome(
        {
          queuedCancelled: 0,
          activeCancelled: 0,
          abortedInputs: [],
          abortedOperations: [],
          settled: pendingSettlement,
          failures: [],
          finalizing: 0,
        },
        "unchanged",
        { stopped: 0, failed: 0 },
      );
  const completed = children.then(
    async (childResult) => {
      const postParent = runPostParent;
      const currentParentResult = parentResult;
      if (!postParent || !currentParentResult) {
        throw new Error("Parent Stop result is unavailable");
      }
      await postParent();
      // Committed failures never strand later owners but still fail default callers afterward.
      if (unhandledFailures.length === 1) {
        throw unhandledFailures[0];
      }
      if (unhandledFailures.length > 1) {
        throw new AggregateError(unhandledFailures, "Session cancellation failed");
      }
      return resolveStopOutcome(currentParentResult, externalStatus, childResult);
    },
    async (error: unknown) => {
      await runPostParent?.();
      throw error;
    },
  );
  void completed.catch(rejectParentResult);
  void completed.catch(() => {});
  return Object.freeze({ ...initial, completed });
}

async function drainSessionControllerOwnersForTest(): Promise<void> {
  const candidates = captureSessionControllerStopCandidates();
  const capture = captureSessionControllerStop({
    inputs: candidates.flatMap((candidate) => candidate.capture.inputs),
    operations: candidates.flatMap((candidate) => candidate.capture.operations),
  });
  if (capture.inputs.length === 0 && capture.operations.length === 0) {
    return;
  }

  // Teardown must use the same Stop owner as production and retain committed failures.
  const failures: unknown[] = [];
  const execution = stopSession({
    source: "restart",
    capture,
    onError: () => "continue",
  });
  const completion = execution.completed.then(
    async (outcome) => {
      failures.push(...outcome.failures.map(({ error }) => error));
      await outcome.settled;
    },
    (error: unknown) => {
      failures.push(error);
    },
  );
  const settlement = Promise.all([capture.settled, completion])
    .then(() => undefined)
    .catch((error: unknown) => {
      failures.push(error);
    });
  if (
    !(await waitForSessionControllerSettlement(settlement, SESSION_CONTROLLER_DRAIN_TIMEOUT_MS))
  ) {
    failures.push(
      new Error(
        `Session-controller cleanup remains pending after ${SESSION_CONTROLLER_DRAIN_TIMEOUT_MS}ms`,
      ),
    );
  }
  if (failures.length > 0) {
    throw new AggregateError(failures, "Session-controller cleanup failed");
  }
}

type ReplyRunRegistryTestApi = {
  drainReplyRunRegistry?: () => Promise<void>;
};

if (process.env.VITEST || process.env.NODE_ENV === "test") {
  // SAFETY: globalThis hosts the symbol-keyed internal test API publications.
  const publications = globalThis as Record<PropertyKey, unknown>;
  const registryKey = Symbol.for("openclaw.replyRunRegistryTestApi");
  // SAFETY: Storage publishes this mutable registry API before Stop can load.
  const api = publications[registryKey] as ReplyRunRegistryTestApi | undefined;
  if (api) {
    api.drainReplyRunRegistry = drainSessionControllerOwnersForTest;
  }
}
