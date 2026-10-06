import { resolveTimerTimeoutMs } from "@openclaw/normalization-core/number-coercion";
import {
  decideSessionWatchdog,
  SESSION_WATCHDOG_ABORT_FLOOR_MS,
  SESSION_WATCHDOG_CLEANUP_MS,
  SESSION_WATCHDOG_QUIET_TOOL_MS,
  SESSION_WATCHDOG_WARNING_MS,
  type SessionWatchdogDecision,
  type SessionWatchdogPhase,
  type SessionWatchdogSnapshot,
  type SessionWatchdogWaitKind,
} from "./session-controller.watchdog-state.js";

type SessionWatchdogProgress = "semantic" | "transport" | "source_arrival" | "finalization";
type OwnedWait = { kind: SessionWatchdogWaitKind; deadlineAtMs?: number; isCurrent: () => boolean };
export type SessionWatchdogWait = {
  close(): void;
  updateDeadline(deadlineAtMs: number): void;
};
export type SessionControllerWatchdogAttempt = {
  isCurrent(): boolean;
  progress(kind: SessionWatchdogProgress, reason?: string): boolean;
  observeStagnation(params: { active: boolean; existingOnly?: boolean }): void;
  progressTool(toolCallId: string, allowedNames: readonly string[]): boolean;
  beginWait(wait: OwnedWait): SessionWatchdogWait;
  beginTool(tool: {
    toolCallId: string;
    toolName: string;
    deadlineAtMs?: number;
  }): (completed?: boolean) => void;
  toolEvent(event: {
    phase: "start" | "end";
    toolCallId: string;
    toolName: string;
    deadlineAtMs?: number;
  }): void;
  beginRequest(params: { deadlineAtMs?: number; requestTimeoutMs?: number }): () => void;
  requestEvent(event: { phase: "start" | "end"; callId: string; requestTimeoutMs?: number }): void;
  setExecutionDeadline(deadlineAtMs: number | undefined): void;
  close(): void;
};
export type SessionWatchdogEffect = {
  cause: "run_stalled";
  reason: string;
  attempt?: SessionControllerWatchdogAttempt;
  /** Revalidate at each actual asynchronous stop/cleanup side effect. */
  isCurrent(): boolean;
};
export type SessionControllerWatchdog = {
  start(): void;
  close(): void;
  snapshot(now?: number): SessionWatchdogSnapshot;
  decide(now?: number): SessionWatchdogDecision;
  tick(now?: number): Promise<SessionWatchdogDecision>;
  progress(kind: SessionWatchdogProgress, reason?: string): boolean;
  beginWait(wait: OwnedWait): SessionWatchdogWait;
  attachAttempt(params: { assertCurrent: () => void }): SessionControllerWatchdogAttempt;
  beginFinalization(): void;
  beginTerminal(deadlineAtMs?: number): void;
  beginExecution(getDeadlineAtMs?: () => number | undefined): {
    setDeadline(deadlineAtMs: number | undefined): void;
    close(): void;
  };
  beginFinalizationWork(timeoutMs: number): () => void;
};

const inertWait: SessionWatchdogWait = { close() {}, updateDeadline() {} };
const noop = () => {};
function live(assertion: () => boolean): boolean {
  try {
    return assertion();
  } catch {
    return false;
  }
}

/** State and timer belong to one exact operation; there is no recovery registry. */
export function createSessionControllerWatchdog(params: {
  startedAtMs: number;
  isCurrent: () => boolean;
  readPhase: () => SessionWatchdogPhase;
  requestStop: (
    effect: SessionWatchdogEffect,
  ) => "settled" | "blocked" | Promise<"settled" | "blocked">;
  expireCleanup: (
    effect: SessionWatchdogEffect,
  ) => "settled" | "blocked" | Promise<"settled" | "blocked">;
  onWarning?: (decision: SessionWatchdogDecision) => void;
  warningMs?: number;
  now?: () => number;
}): SessionControllerWatchdog {
  const now = params.now ?? Date.now;
  const warningMs = resolveTimerTimeoutMs(params.warningMs, SESSION_WATCHDOG_WARNING_MS, 1);
  const abortMs = Math.max(SESSION_WATCHDOG_ABORT_FLOOR_MS, 3 * warningMs);
  let closed = false;
  let semanticAt = params.startedAtMs;
  let transportAt = params.startedAtMs;
  let reason: string | undefined;
  let semanticSequence = 0;
  let stagnation:
    | { startedAtMs: number; observedAtMs: number; semanticSequence: number }
    | undefined;
  const stagnationCurrent = (at: number) =>
    stagnation &&
    !(semanticSequence > stagnation.semanticSequence && at - stagnation.observedAtMs >= 60_000);
  const progressOrigin = (at: number) =>
    stagnationCurrent(at) ? stagnation!.startedAtMs : semanticAt;
  let requestAllowanceAt: number | undefined;
  let retryAllowanceAt: number | undefined;
  let cleanupAt: number | undefined;
  let executionAt: number | undefined;
  let attempt: SessionControllerWatchdogAttempt | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let started = false;
  let warningAt: number | undefined;
  let blockedWarning = false;
  let recovery: SessionWatchdogSnapshot["recovery"];
  let recoveryAction: "stop" | "expire_cleanup" | undefined;
  let pendingTick:
    | { decision: SessionWatchdogDecision; resolve: (decision: SessionWatchdogDecision) => void }
    | undefined;
  const requests = new Set<{ requestTimeoutMs?: number }>();
  const waits = new Set<OwnedWait>();
  const tools = new Set<SessionWatchdogSnapshot["tools"][number] & { touch(): void }>();
  const executions = new Set<{
    deadlineAtMs?: number;
    getDeadlineAtMs?: () => number | undefined;
  }>();
  const finalizationWork = new Set<{ deadlineAtMs: number }>();
  const current = () => !closed && live(params.isCurrent);
  const semanticDeadline = (at = now()) =>
    Math.max(progressOrigin(at) + abortMs, requestAllowanceAt ?? 0);

  const beginWait = (input: OwnedWait, valid = current): SessionWatchdogWait => {
    if (!valid()) {
      return inertWait;
    }
    // An approval deadline is supplied by the real approval owner, never invented here.
    if (
      (input.kind === "approval" || input.kind === "retry" || input.kind === "backend") &&
      !Number.isFinite(input.deadlineAtMs)
    ) {
      throw new Error("Owned bounded wait requires its deadline");
    }
    const wait: OwnedWait = { ...input, isCurrent: () => valid() && live(input.isCurrent) };
    if (wait.kind === "retry") {
      retryAllowanceAt ??= Math.max(semanticDeadline(), wait.deadlineAtMs!);
    }
    waits.add(wait);
    return {
      close() {
        waits.delete(wait);
      },
      updateDeadline(deadlineAtMs) {
        if (waits.has(wait) && wait.isCurrent() && Number.isFinite(deadlineAtMs)) {
          wait.deadlineAtMs = deadlineAtMs;
        }
      },
    };
  };
  const progress = (kind: SessionWatchdogProgress, nextReason?: string): boolean => {
    if (!current() || recovery) {
      return false;
    }
    if (kind === "source_arrival") {
      return true;
    }
    transportAt = now();
    if (kind === "semantic") {
      semanticAt = transportAt;
      semanticSequence += 1;
      if (!stagnationCurrent(transportAt)) {
        requestAllowanceAt = undefined;
        retryAllowanceAt = undefined;
      }
      reason = nextReason;
      warningAt = undefined;
    } else if (kind === "finalization" && cleanupAt !== undefined) {
      cleanupAt = transportAt + SESSION_WATCHDOG_CLEANUP_MS;
      reason = nextReason;
    } else if (nextReason !== undefined) {
      reason = nextReason;
    }
    return true;
  };
  const snapshot = (at = now()): SessionWatchdogSnapshot => {
    const isCurrent = current();
    const liveWaits = [...waits]
      .filter((wait) => live(wait.isCurrent))
      .map((wait) => ({
        kind: wait.kind,
        // Mechanical traffic/retry rearming never moves the logical semantic horizon.
        deadlineAtMs:
          wait.kind === "backend"
            ? Math.min(wait.deadlineAtMs!, semanticDeadline(at))
            : wait.kind === "retry"
              ? Math.min(wait.deadlineAtMs!, retryAllowanceAt!)
              : wait.deadlineAtMs,
      }));
    return {
      current: isCurrent && current(),
      phase: closed ? "settled" : params.readPhase(),
      hasActiveAttempt: attempt?.isCurrent() === true,
      semanticProgressAtMs: progressOrigin(at),
      transportProgressAtMs: transportAt,
      lastProgressReason: stagnationCurrent(at) ? "tool_loop:argument_churn" : reason,
      warningMs,
      semanticDeadlineAtMs: semanticDeadline(at),
      executionDeadlineAtMs: [
        executionAt,
        ...[...executions].map((execution) =>
          execution.getDeadlineAtMs ? execution.getDeadlineAtMs() : execution.deadlineAtMs,
        ),
      ]
        .filter((deadline): deadline is number => deadline !== undefined)
        .reduce<number | undefined>(
          (earliest, deadline) =>
            earliest === undefined ? deadline : Math.min(earliest, deadline),
          undefined,
        ),
      cleanupDeadlineAtMs:
        cleanupAt === undefined
          ? undefined
          : Math.max(cleanupAt, ...[...finalizationWork].map((work) => work.deadlineAtMs)),
      waits: liveWaits,
      requests: requests.size
        ? [...requests].map(({ requestTimeoutMs }) => ({ requestTimeoutMs }))
        : undefined,
      tools: [...tools].map(({ toolName, toolCallId, startedAtMs, deadlineAtMs }) => ({
        toolName,
        toolCallId,
        startedAtMs,
        deadlineAtMs,
      })),
      recovery: recovery && { ...recovery },
    };
  };
  const decide = (at = now()) => decideSessionWatchdog(snapshot(at), at);
  const tick = async (at = now()): Promise<SessionWatchdogDecision> => {
    const cleanupDeadlineAtMs = snapshot(at).cleanupDeadlineAtMs;
    const phase = params.readPhase();
    if (
      recovery?.status === "blocked" &&
      recoveryAction === "stop" &&
      (phase === "finishing" || phase === "terminal") &&
      cleanupDeadlineAtMs !== undefined &&
      at >= cleanupDeadlineAtMs
    ) {
      recovery = undefined;
      recoveryAction = undefined;
      blockedWarning = false;
    }
    const decision = decide(at);
    if (decision.action === "warn" || decision.action === "blocked") {
      if (warningAt !== semanticAt || (decision.action === "blocked" && !blockedWarning)) {
        warningAt = semanticAt;
        if (decision.action === "blocked" && recovery) {
          blockedWarning = true;
          recovery = { ...recovery, status: "blocked" };
          pendingTick?.resolve(decision);
          pendingTick = undefined;
        }
        params.onWarning?.(decision);
      }
      return decision;
    }
    if (decision.action !== "stop" && decision.action !== "expire_cleanup") {
      return decision;
    }
    const targetAttempt = attempt;
    const effect: SessionWatchdogEffect = {
      cause: "run_stalled",
      reason: decision.reason,
      attempt: targetAttempt,
      isCurrent: () => current() && attempt === targetAttempt,
    };
    if (!effect.isCurrent()) {
      return { action: "observe", reason: "retired" };
    }
    // Install before invoking an effect: cancellation can synchronously reenter tick.
    recovery = { status: "pending", startedAtMs: at };
    recoveryAction = decision.action;
    const deadlineResult = new Promise<SessionWatchdogDecision>((resolve) => {
      pendingTick = { decision, resolve };
    });
    const effectResult = (async (): Promise<SessionWatchdogDecision> => {
      try {
        const result = await (
          decision.action === "expire_cleanup" ? params.expireCleanup : params.requestStop
        )(effect);
        // A committed Stop may close its attempt; recovery still belongs to this owner.
        if (recovery?.status !== "settled" && (result === "settled" || current())) {
          recovery = { status: result, startedAtMs: at };
        }
      } catch (error) {
        if (recovery?.status !== "settled" && current()) {
          recovery = { status: "blocked", startedAtMs: at, error: String(error) };
        }
      }
      pendingTick = undefined;
      return recovery?.status === "blocked"
        ? { action: "blocked", reason: "cleanup_pending" }
        : decision;
    })();
    // The owner timer reports a hung cleanup as blocked; custody stays until real settlement.
    return Promise.race([effectResult, deadlineResult]);
  };
  const schedule = () => {
    if (!started || !current()) {
      return;
    }
    timer = setTimeout(() => {
      timer = undefined;
      // Schedule independently of pending cleanup; a hung effect becomes visibly blocked.
      void tick().catch(() => {});
      schedule();
    }, 1_000);
    timer.unref?.();
  };
  const beginCleanup = () => {
    if (current()) {
      cleanupAt ??= now() + SESSION_WATCHDOG_CLEANUP_MS;
    }
  };
  return {
    start() {
      if (!started && !closed) {
        started = true;
        schedule();
      }
    },
    close() {
      if (recovery && recovery.status !== "settled") {
        recovery = { ...recovery, status: "settled" };
      }
      pendingTick?.resolve(pendingTick.decision);
      pendingTick = undefined;
      closed = true;
      recoveryAction = undefined;
      started = false;
      clearTimeout(timer);
      timer = undefined;
      waits.clear();
      tools.clear();
      requests.clear();
      finalizationWork.clear();
      executions.clear();
      attempt = undefined;
    },
    snapshot,
    decide,
    tick,
    progress,
    beginWait: (input) => beginWait(input),
    beginFinalization: beginCleanup,
    beginTerminal(deadlineAtMs) {
      beginCleanup();
      if (current() && deadlineAtMs !== undefined) {
        cleanupAt = Math.min(cleanupAt!, deadlineAtMs);
      }
    },
    beginExecution(getDeadlineAtMs) {
      const execution: { deadlineAtMs?: number; getDeadlineAtMs?: () => number | undefined } = {
        getDeadlineAtMs,
      };
      if (current()) {
        executions.add(execution);
      }
      return {
        setDeadline(deadlineAtMs) {
          if (current() && executions.has(execution)) {
            execution.deadlineAtMs = deadlineAtMs;
          }
        },
        close() {
          executions.delete(execution);
        },
      };
    },
    beginFinalizationWork(timeoutMs) {
      if (!current()) {
        return noop;
      }
      const work = {
        deadlineAtMs: now() + resolveTimerTimeoutMs(timeoutMs, SESSION_WATCHDOG_CLEANUP_MS, 1),
      };
      finalizationWork.add(work);
      return () => {
        finalizationWork.delete(work);
      };
    },
    attachAttempt({ assertCurrent }) {
      if (!current() || recovery) {
        throw new Error("Cannot attach attempt to retired or recovering operation");
      }
      const previousAttempt = attempt;
      assertCurrent();
      if (!current() || recovery || attempt !== previousAttempt) {
        throw new Error("Operation retired while attaching attempt");
      }
      previousAttempt?.close();
      let attemptClosed = false;
      const ownedReleases = new Set<() => void>();
      const emittedTools = new Map<string, (completed?: boolean) => void>();
      const emittedRequests = new Map<string, () => void>();
      const valid = () =>
        current() &&
        !attemptClosed &&
        attempt === next &&
        live(() => {
          assertCurrent();
          return true;
        }) &&
        current() &&
        !attemptClosed &&
        attempt === next;
      const next: SessionControllerWatchdogAttempt = {
        isCurrent: valid,
        progress: (kind, nextReason) => valid() && progress(kind, nextReason),
        progressTool(toolCallId, allowedNames) {
          if (!valid()) {
            return false;
          }
          for (const tool of tools) {
            if (tool.toolCallId === toolCallId && allowedNames.includes(tool.toolName)) {
              tool.touch();
              return progress("semantic", "tool:owned_progress");
            }
          }
          return false;
        },
        observeStagnation(observation) {
          if (!valid() || (observation.existingOnly && !stagnation)) {
            return;
          }
          if (!observation.active) {
            stagnation = undefined;
            return;
          }
          const at = now();
          const startedAtMs = stagnationCurrent(at) ? stagnation!.startedAtMs : at;
          stagnation = { startedAtMs, observedAtMs: at, semanticSequence };
        },
        beginWait(input) {
          const wait = beginWait(input, valid);
          const close = () => {
            wait.close();
            ownedReleases.delete(close);
          };
          ownedReleases.add(close);
          return { close, updateDeadline: (deadlineAtMs) => wait.updateDeadline(deadlineAtMs) };
        },
        beginTool(tool) {
          if (!valid()) {
            return noop;
          }
          const startedAtMs = now();
          let lastProgressAtMs = startedAtMs;
          const record = {
            toolName: tool.toolName,
            toolCallId: tool.toolCallId,
            startedAtMs,
            touch() {
              lastProgressAtMs = now();
            },
            get deadlineAtMs() {
              return (tool.deadlineAtMs ?? lastProgressAtMs) + SESSION_WATCHDOG_QUIET_TOOL_MS;
            },
          };
          tools.add(record);
          const close = (completed = false) => {
            if (!tools.delete(record)) {
              return;
            }
            ownedReleases.delete(close);
            if (completed && valid()) {
              progress("semantic", "tool:completed");
            }
          };
          ownedReleases.add(close);
          return close;
        },
        toolEvent(event) {
          if (!valid()) {
            return;
          }
          if (event.phase === "start") {
            if (!emittedTools.has(event.toolCallId)) {
              emittedTools.set(event.toolCallId, next.beginTool(event));
            }
          } else {
            emittedTools.get(event.toolCallId)?.(true);
            emittedTools.delete(event.toolCallId);
          }
        },
        beginRequest({ deadlineAtMs, requestTimeoutMs }) {
          if (!valid()) {
            return noop;
          }
          if (requestAllowanceAt === undefined && deadlineAtMs !== undefined) {
            requestAllowanceAt = Math.max(progressOrigin(now()) + abortMs, deadlineAtMs);
          }
          const request = { requestTimeoutMs };
          requests.add(request);
          const close = () => {
            requests.delete(request);
            ownedReleases.delete(close);
          };
          ownedReleases.add(close);
          return close;
        },
        requestEvent(event) {
          if (event.phase === "start") {
            if (!valid() || emittedRequests.has(event.callId)) {
              return;
            }
            emittedRequests.set(
              event.callId,
              next.beginRequest({
                requestTimeoutMs: event.requestTimeoutMs,
                deadlineAtMs:
                  event.requestTimeoutMs === undefined ? undefined : now() + event.requestTimeoutMs,
              }),
            );
          } else {
            emittedRequests.get(event.callId)?.();
            emittedRequests.delete(event.callId);
          }
        },
        setExecutionDeadline(deadlineAtMs) {
          if (valid()) {
            executionAt = deadlineAtMs;
          }
        },
        close() {
          attemptClosed = true;
          for (const release of ownedReleases) {
            release();
          }
          emittedTools.clear();
          emittedRequests.clear();
          if (attempt === next) {
            attempt = undefined;
            executionAt = undefined;
          }
        },
      };
      attempt = next;
      return next;
    },
  };
}
