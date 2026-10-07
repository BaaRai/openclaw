import { isDeepStrictEqual } from "node:util";
import type { ProgressContinuationState } from "../../../channels/progress-continuation.js";
import {
  runWithGatewayDetachedWorkContinuation,
  runWithGatewayIndependentRootWorkContinuation,
} from "../../../process/gateway-work-admission.js";
import { createDeferredCore } from "../../../shared/deferred.js";
import { captureOpenClawStateWorkerContext } from "../../../state/openclaw-state-worker-context.js";
import type { OpenClawStateWorkerContext } from "../../../state/openclaw-state-worker-context.types.js";
import type { AcceptedSessionSpawn } from "../../accepted-session-spawn.js";
import {
  prepareRequesterCronAuthority,
  type PreparedRequesterCronAuthority,
} from "../requester-cron-authority.js";
import {
  ensureCompletionState,
  ensureDeliveryState,
  getDeliveryLastError,
} from "./subagent-delivery-state.js";
import { shouldSuppressSubagentRecoverySessionEffects } from "./subagent-recovery-state.js";
import {
  resumeAncestorCleanup,
  startSubagentAnnounceCleanupFlow,
} from "./subagent-registry-lifecycle-announce-cleanup.js";
import { completeCleanupBookkeeping } from "./subagent-registry-lifecycle-bookkeeping.js";
import { completeSubagentRunAttempt } from "./subagent-registry-lifecycle-completion.js";
import type {
  CleanupBookkeepingParams,
  RequesterSettleWakeEvaluation,
  SubagentLifecycleOptions,
} from "./subagent-registry-lifecycle-context.js";
import { refreshFrozenResultFromSession } from "./subagent-registry-lifecycle-delivery.js";
import { finalizeResumedAnnounceGiveUp } from "./subagent-registry-lifecycle-give-up.js";
import {
  retireSubagentObligations,
  scheduleRequesterSettleWake,
} from "./subagent-registry-lifecycle-wake.js";
import { getCurrentSubagentRunOwner, subagentRuns } from "./subagent-registry-memory.js";
import {
  assertSubagentRegistryWriteSourceCurrent,
  mutateSubagentRuns,
  SubagentRegistryMutationRejectedError,
} from "./subagent-registry-persistence.js";
import { subscribeSubagentRunChanges } from "./subagent-registry-publication.js";
import {
  adoptSubagentRunForRequesterTurnInRuns,
  commitRequesterTransfer,
  markRequesterTurnYieldedInRuns,
  settleRequesterTurnAfterSessionSpawns,
  type RequesterInitialTransfer,
} from "./subagent-registry-requester-yield.js";
import type {
  SubagentCompletionRequest,
  SubagentRunRecord,
  SubagentSessionEffects,
} from "./subagent-registry.types.js";
import {
  compareSubagentRunGeneration,
  getSubagentRunRuntimeKey,
  isSameSubagentRunOwner,
} from "./subagent-run-generation.js";

export type { SubagentLifecycleOptions } from "./subagent-registry-lifecycle-context.js";

export class SubagentSessionCleanupRevocationChangedError extends SubagentRegistryMutationRejectedError {
  override name = "SubagentSessionCleanupRevocationChangedError";
}

const COMPLETION_SETTLING_ERROR = "Subagent completion is still settling; retry the session reset.";

/** Terminal rows still carrying session effects must revoke them before a reset. */
const ownsSessionEffects = (entry: SubagentRunRecord): boolean =>
  entry.execution.status === "terminal" &&
  entry.pauseReason !== "sessions_yield" &&
  entry.execution.suppressSessionEffects !== true;

function terminalPublication(entry: SubagentRunRecord): readonly unknown[] {
  const execution = entry.execution;
  const outcome = execution.outcome;
  const completion = entry.completion;
  return [
    execution.status,
    execution.startedAt,
    execution.endedAt,
    outcome?.status,
    outcome?.status === "error" ? outcome.error : undefined,
    outcome?.startedAt,
    outcome?.endedAt,
    outcome?.elapsedMs,
    execution.interruptionReason,
    execution.suppressSessionEffects,
    entry.pauseReason,
    entry.endedReason,
    entry.killIntent,
    entry.killReconciliation?.killedAt,
    entry.killReconciliation?.supersededAt,
    entry.killReconciliation?.suppressTaskDelivery,
    completion?.resultText,
    completion?.capturedAt,
    completion?.terminalReply,
  ];
}

export class SubagentLifecycleController {
  readonly activeRequesterSettleWakes = new Map<string, RequesterSettleWakeEvaluation>();
  private readonly terminalCompletionLocks = new Map<object, Promise<void>>();
  private readonly terminalGenerations = new WeakMap<object, number>();
  private readonly terminalPublications = new WeakMap<object, readonly unknown[]>();
  private readonly terminalSessionEffects = new WeakMap<object, SubagentSessionEffects>();
  private readonly cleanupGenerations = new WeakMap<object, number>();
  readonly progressEndedEntries = new WeakSet<object>();
  readonly cleanupReservations = new Set<object>();
  readonly activeCleanupAttempts = new Map<object, number>();

  private readonly runtimeRuns = new Map<object, SubagentRunRecord>();
  private readonly terminalEffectUsers = new Map<object, number>();
  private stopRuntimePruning?: () => void;

  constructor(readonly options: SubagentLifecycleOptions) {}

  private trackRun(entry: SubagentRunRecord): object {
    this.stopRuntimePruning ??= subscribeSubagentRunChanges("projection", ({ runIds }) =>
      this.pruneRetiredRuns(runIds),
    );
    const identity = getSubagentRunRuntimeKey(entry);
    this.runtimeRuns.set(identity, entry);
    return identity;
  }

  pruneRetiredRuns = (changedRunIds?: readonly string[]): void => {
    const changed = changedRunIds && new Set(changedRunIds);
    for (const [identity, observed] of this.runtimeRuns) {
      const current = getCurrentSubagentRunOwner(this.options.runs, observed);
      if (
        changed &&
        !changed.has(observed.runId) &&
        !(observed.collect && observed.swarmRunId && changed.has(observed.swarmRunId)) &&
        !(current && changed.has(current.runId))
      ) {
        continue;
      }
      if (current) {
        this.runtimeRuns.set(identity, current);
      }
      if (
        current &&
        getSubagentRunRuntimeKey(current) === identity &&
        this.terminalPublications.has(identity)
      ) {
        this.bumpTerminalGeneration(current);
      }
      if (
        (current && getSubagentRunRuntimeKey(current) === identity) ||
        this.terminalEffectUsers.has(identity) ||
        this.cleanupReservations.has(identity) ||
        this.activeCleanupAttempts.has(identity)
      ) {
        continue;
      }
      this.runtimeRuns.delete(identity);
      this.options.resumedRuns.delete(identity);
    }
  };

  newerGenerationOwnsSession(entry: SubagentRunRecord): boolean {
    const current = getCurrentSubagentRunOwner(this.options.runs, entry) ?? entry;
    if (current.killReconciliation?.supersededAt !== undefined) {
      return true;
    }
    const latest = this.options.getLatestRunForChildSession(
      current.childSessionKey,
      (candidate) => candidate.runId !== current.runId,
      current.childAgentId,
    );
    return latest !== null && compareSubagentRunGeneration(latest, current) > 0;
  }

  bindTerminalSessionEffects(entry: SubagentRunRecord, effects?: SubagentSessionEffects): void {
    if (effects) {
      this.terminalSessionEffects.set(this.trackRun(entry), effects);
    }
  }

  private liveRow(entry: SubagentRunRecord): SubagentRunRecord | undefined {
    return (
      this.options.runs.get(entry.runId) ?? getCurrentSubagentRunOwner(this.options.runs, entry)
    );
  }

  private sessionEffectsSuppressed(entry: SubagentRunRecord): boolean {
    const current = this.liveRow(entry);
    return (
      (current !== undefined && !isSameSubagentRunOwner(current, entry)) ||
      this.newerGenerationOwnsSession(entry) ||
      shouldSuppressSubagentRecoverySessionEffects(current ?? entry)
    );
  }

  async shouldSuppressSessionEffects(
    entry: SubagentRunRecord,
    prospectiveEffects?: SubagentSessionEffects,
  ): Promise<boolean> {
    const boundEffects = this.terminalSessionEffects.get(getSubagentRunRuntimeKey(entry));
    const effects = prospectiveEffects ?? boundEffects;
    return (
      this.sessionEffectsSuppressed(entry) ||
      (await effects?.isCurrent()) === false ||
      this.terminalSessionEffects.get(getSubagentRunRuntimeKey(entry)) !== boundEffects ||
      this.sessionEffectsSuppressed(entry)
    );
  }

  sessionEffectsHostCurrent(entry: SubagentRunRecord): boolean {
    if (this.sessionEffectsSuppressed(entry)) {
      return false;
    }
    try {
      this.terminalSessionEffects.get(getSubagentRunRuntimeKey(entry))?.assertHostCurrent();
      return true;
    } catch {
      return false;
    }
  }

  getSessionEffects(entry: SubagentRunRecord): SubagentSessionEffects | undefined {
    return this.terminalSessionEffects.get(getSubagentRunRuntimeKey(entry));
  }

  async acquireTerminalCompletionLock(runId: string): Promise<() => void> {
    const entry = this.options.runs.get(runId);
    if (!entry) {
      return () => {};
    }
    const owner = getSubagentRunRuntimeKey(entry);
    const previous = this.terminalCompletionLocks.get(owner) ?? Promise.resolve();
    const { promise: current, resolve: releaseLock } = createDeferredCore();
    this.terminalCompletionLocks.set(owner, current);
    await previous;
    return () => {
      releaseLock();
      if (this.terminalCompletionLocks.get(owner) === current) {
        this.terminalCompletionLocks.delete(owner);
      }
    };
  }

  /** Persist revocation before reset prepares its synchronous successor guard. */
  async revokeTerminalSessionEffects(
    entries: Iterable<SubagentRunRecord>,
    assertCurrent?: () => void,
  ): Promise<void> {
    const selected = new Map([...entries].map((entry) => [entry.runId, entry]));
    await mutateSubagentRuns(
      [...selected.keys()],
      (rows) => {
        const postimages = new Map<string, SubagentRunRecord>();
        for (const [runId, expected] of selected) {
          const entry = rows.get(runId);
          if (!entry) {
            continue;
          }
          if (!isSameSubagentRunOwner(entry, expected)) {
            throw new SubagentRegistryMutationRejectedError(
              "Subagent cleanup owner changed before reset",
            );
          }
          this.assertCompletionSettled(entry);
          if (ownsSessionEffects(entry)) {
            postimages.set(runId, {
              ...entry,
              execution: { ...entry.execution, suppressSessionEffects: true },
            });
          }
        }
        return { value: undefined, postimages };
      },
      { runs: this.options.runs, assertCurrent },
    );
  }

  private assertCompletionSettled(entry: SubagentRunRecord): void {
    if (this.terminalCompletionLocks.has(getSubagentRunRuntimeKey(entry))) {
      throw new Error(COMPLETION_SETTLING_ERROR);
    }
  }

  assertTerminalSessionEffectsRevoked(currentEntries: Iterable<SubagentRunRecord>): void {
    for (const entry of currentEntries) {
      this.assertCompletionSettled(entry);
      if (ownsSessionEffects(entry)) {
        throw new SubagentSessionCleanupRevocationChangedError(
          "Subagent cleanup revocation changed before reset",
        );
      }
    }
  }

  clearRuntimeState = () => {
    this.activeRequesterSettleWakes.clear();
    this.cleanupReservations.clear();
    this.runtimeRuns.clear();
    this.stopRuntimePruning?.();
    this.stopRuntimePruning = undefined;
  };

  bumpCleanupGeneration(entry: SubagentRunRecord): number {
    const identity = this.trackRun(entry);
    const generation = (this.cleanupGenerations.get(identity) ?? 0) + 1;
    this.cleanupGenerations.set(identity, generation);
    return generation;
  }

  isCleanupGeneration = (entry: SubagentRunRecord, generation: number): boolean =>
    this.cleanupGenerations.get(getSubagentRunRuntimeKey(entry)) === generation;
  isCleanupGenerationCurrent = (
    _runId: string,
    entry: SubagentRunRecord,
    generation: number,
  ): boolean => {
    const current = getCurrentSubagentRunOwner(this.options.runs, entry);
    return (
      current !== undefined &&
      current.pauseReason !== "sessions_yield" &&
      this.isCleanupGeneration(entry, generation)
    );
  };
  isCleanupAttemptCurrent = (
    runId: string,
    entry: SubagentRunRecord,
    generation: number,
  ): boolean =>
    getCurrentSubagentRunOwner(this.options.runs, entry)?.cleanupHandled === true &&
    this.isCleanupGenerationCurrent(runId, entry, generation);
  isCleanupOwnerCurrent = (_runId: string, entry: SubagentRunRecord): boolean => {
    const current = this.liveRow(entry);
    return (
      (current === undefined || isSameSubagentRunOwner(current, entry)) &&
      (current ?? entry).pauseReason !== "sessions_yield"
    );
  };
  isEndedHookOwnerCurrent = (runId: string, entry: SubagentRunRecord): boolean =>
    this.isCleanupOwnerCurrent(runId, entry) && !this.newerGenerationOwnsSession(entry);

  bumpTerminalGeneration(entry: SubagentRunRecord, bindingChanged = false): number {
    const identity = this.trackRun(entry);
    const previous = this.terminalGenerations.get(identity) ?? 0;
    const publication = terminalPublication(entry);
    const changed = !isDeepStrictEqual(this.terminalPublications.get(identity), publication);
    const generation = changed || bindingChanged || previous === 0 ? previous + 1 : previous;
    this.terminalGenerations.set(identity, generation);
    this.terminalPublications.set(identity, publication);
    return generation;
  }

  isTerminalCallbackCurrent = (
    _runId: string,
    entry: SubagentRunRecord,
    generation: number,
  ): boolean => {
    const current = getCurrentSubagentRunOwner(this.options.runs, entry);
    return (
      current !== undefined &&
      current.pauseReason !== "sessions_yield" &&
      this.terminalGenerations.get(getSubagentRunRuntimeKey(entry)) === generation &&
      isDeepStrictEqual(
        this.terminalPublications.get(getSubagentRunRuntimeKey(entry)),
        terminalPublication(current),
      )
    );
  };
  runRequesterSettleWake = (
    entry: SubagentRunRecord,
    run: () => Promise<unknown>,
    isCurrent: () => boolean,
  ): Promise<unknown> => {
    this.trackRun(entry);
    // The detached Gateway root keeps restart drain aware of an evaluation in flight.
    return runWithGatewayDetachedWorkContinuation(
      async () => (isCurrent() ? run() : undefined),
      "subagents:lifecycle-wake",
    );
  };

  completeSubagentRun = async (completeParams: SubagentCompletionRequest) => {
    // Task finalization can make the run disappear from suspension blockers
    // before browser/MCP retirement and cleanup delivery hand off. Own this
    // entire transition as an independent root so that boundary stays atomic.
    // Callers can detach while retaining parent ALS, so nesting is intentional.
    await runWithGatewayIndependentRootWorkContinuation(async () => {
      const entry = completeParams.expectedEntry
        ? getCurrentSubagentRunOwner(this.options.runs, completeParams.expectedEntry)
        : this.options.runs.get(completeParams.runId);
      const identity = entry && this.trackRun(entry);
      if (identity) {
        this.terminalEffectUsers.set(identity, (this.terminalEffectUsers.get(identity) ?? 0) + 1);
      }
      try {
        await completeSubagentRunAttempt(this, completeParams);
      } finally {
        if (identity) {
          const count = (this.terminalEffectUsers.get(identity) ?? 1) - 1;
          if (count) {
            this.terminalEffectUsers.set(identity, count);
          } else {
            this.terminalEffectUsers.delete(identity);
          }
        }
        this.pruneRetiredRuns([completeParams.runId]);
      }
    }, "subagents:lifecycle-complete");
  };

  completeCleanupBookkeeping = (params: CleanupBookkeepingParams) =>
    completeCleanupBookkeeping(this, params);

  resumeAncestorCleanup = (settledEntry: SubagentRunRecord): void =>
    resumeAncestorCleanup(this, settledEntry);

  static discardTerminalDelivery(
    this: void,
    entry: SubagentRunRecord,
    completedAt: number,
    reason: "dismissed" | "expired" = "dismissed",
  ): void {
    const delivery = ensureDeliveryState(entry);
    const payload = delivery.payload;
    if (reason === "dismissed") {
      delivery.disposition = "intentional_non_delivery";
      delivery.dismissedAt = completedAt;
    } else {
      delivery.discardedAt = completedAt;
      delivery.discardReason = "expired";
      delivery.discardedPayloadSummary = {
        requesterSessionKey: payload?.requesterSessionKey ?? entry.requesterSessionKey,
        childSessionKey: payload?.childSessionKey ?? entry.childSessionKey,
        childRunId: payload?.childRunId ?? entry.runId,
        endedAt: payload?.endedAt ?? entry.execution.endedAt,
        status: payload?.outcome?.status ?? entry.execution.outcome?.status,
        lastError: getDeliveryLastError(entry) ?? null,
      };
    }
    Object.assign(delivery, {
      status: "discarded",
      queueId: undefined,
      payload: undefined,
      createdAt: undefined,
      lastError: undefined,
      announcedAt: undefined,
      suspendedAt: undefined,
      suspendedReason: undefined,
    });
    Object.assign(entry, { wakeOnDescendantSettle: undefined, cleanupHandled: true });
    const completion = ensureCompletionState(entry);
    Object.assign(completion, { fallbackResultText: undefined, fallbackCapturedAt: undefined });
    entry.cleanupCompletedAt = completedAt;
  }

  finalizeResumedAnnounceGiveUp = (params: Parameters<typeof finalizeResumedAnnounceGiveUp>[1]) =>
    finalizeResumedAnnounceGiveUp(this, params);

  refreshFrozenResultFromSession = (sessionKey: string) =>
    refreshFrozenResultFromSession(this, sessionKey);

  resumeRequesterSettleWake = (runId: string, entry: SubagentRunRecord) => {
    this.trackRun(entry);
    scheduleRequesterSettleWake(this, runId, entry, undefined, true);
  };

  retireSubagentObligations = (
    entry: SubagentRunRecord,
    assertCurrent: () => void,
    options?: { requesterNotified?: boolean },
  ) => retireSubagentObligations(this, entry, assertCurrent, options);

  adoptSubagentRunForRequesterTurn = (
    params: Omit<Parameters<typeof adoptSubagentRunForRequesterTurnInRuns>[0], "runs">,
  ) => {
    if (this.newerGenerationOwnsSession(params.expected)) {
      return Promise.resolve(undefined);
    }
    return adoptSubagentRunForRequesterTurnInRuns({
      ...params,
      runs: this.options.runs,
      assertCurrent: () =>
        subagentRuns.runWithCompletionAuthority(params.expected, () => {
          params.assertCurrent();
          if (this.newerGenerationOwnsSession(params.expected)) {
            throw new Error("Steered completion no longer owns its execution");
          }
        }),
    });
  };

  private prepareRequesterInitialTransfer(
    assertCurrent?: () => void,
    stateContext = captureOpenClawStateWorkerContext(),
  ): RequesterInitialTransfer {
    // Logical settlement retains run or reply-operation authority beyond individual tool calls.
    return (params) =>
      commitRequesterTransfer(params, {
        runs: this.options.runs,
        stateContext,
        assertCurrent: () => {
          assertSubagentRegistryWriteSourceCurrent(stateContext);
          assertCurrent?.();
        },
      });
  }

  markRequesterTurnYielded = async (args: {
    requesterSessionKey: string;
    requesterAgentId?: string;
    requesterTurnRunId: string;
    assertCurrent?: () => void;
    stateContext?: OpenClawStateWorkerContext;
    preparedAuthority?: PreparedRequesterCronAuthority | null;
  }) => {
    const ownsPreparation = args.preparedAuthority === undefined;
    const preparedAuthority =
      args.preparedAuthority === undefined
        ? prepareRequesterCronAuthority(args)
        : args.preparedAuthority;
    try {
      return await markRequesterTurnYieldedInRuns({
        ...args,
        preparedAuthority: preparedAuthority ?? null,
        runs: this.options.runs,
        transfer: this.prepareRequesterInitialTransfer(() => {
          args.assertCurrent?.();
          preparedAuthority?.assertCurrent();
        }, args.stateContext),
      });
    } finally {
      const release = ownsPreparation ? preparedAuthority?.release() : undefined;
      if (release) {
        await release;
      }
    }
  };

  settleRequesterTurnAfterSessionSpawns = (args: {
    requesterSessionKey: string;
    requesterAgentId?: string;
    requesterTurnRunId: string;
    requesterYielded: boolean;
    acceptedSessionSpawns: readonly AcceptedSessionSpawn[];
    progressPresentation?: ProgressContinuationState;
    assertCurrent?: () => void;
    stateContext?: OpenClawStateWorkerContext;
  }) =>
    settleRequesterTurnAfterSessionSpawns({
      ...args,
      runs: this.options.runs,
      transfer: this.prepareRequesterInitialTransfer(args.assertCurrent, args.stateContext),
      schedule: (runId, entry, kind) => {
        this.trackRun(entry);
        if (kind === "completion") {
          this.options.resumedRuns.delete(getSubagentRunRuntimeKey(entry));
          this.options.resumeSubagentRun(runId);
          return;
        }
        scheduleRequesterSettleWake(this, runId, entry);
      },
    });

  startSubagentAnnounceCleanupFlow = (runId: string, entry: SubagentRunRecord): boolean =>
    startSubagentAnnounceCleanupFlow(this, runId, entry);
}
