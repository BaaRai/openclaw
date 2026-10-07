import type { cleanupBrowserSessionsForLifecycleEnd } from "../../../browser-lifecycle-cleanup.js";
import type { OpenClawConfig } from "../../../config/types.openclaw.js";
import type { callGateway as defaultCallGateway } from "../../../gateway/call.js";
import type { OpenClawStateWorkerContext } from "../../../state/openclaw-state-worker-context.types.js";
import type { createSubagentRegistryContextCleanup } from "./subagent-registry-context-cleanup.js";
import type { SubagentRunRecord, SubagentSessionEffects } from "./subagent-registry.types.js";

type CaptureSubagentCompletionReply =
  (typeof import("../announce/subagent-announce.js"))["captureSubagentCompletionReply"];
type RunSubagentAnnounceFlow =
  (typeof import("../announce/subagent-announce.js"))["runSubagentAnnounceFlow"];
type MaybeWakeRequesterAfterAllChildrenSettled =
  (typeof import("../announce/subagent-announce.requester-settle-wake.js"))["maybeWakeRequesterAfterAllChildrenSettled"];
type BrowserCleanup = typeof cleanupBrowserSessionsForLifecycleEnd;
type ContextCleanup = ReturnType<typeof createSubagentRegistryContextCleanup>;

export type SubagentLifecycleOptions = {
  runs: Map<string, SubagentRunRecord>;
  resumedRuns: Set<object>;
  subagentAnnounceTimeoutMs: number;
  getRuntimeConfig(): OpenClawConfig;
  clearPendingLifecycleError(runId: string): void;
  countPendingDescendantRuns(rootSessionKey: string, assertCurrent: () => void): Promise<number>;
  getLatestRunForChildSession(
    childSessionKey: string,
    matches?: (entry: SubagentRunRecord) => boolean,
    childAgentId?: string,
  ): SubagentRunRecord | null;
  suppressAnnounceForSteerRestart(entry?: SubagentRunRecord): boolean;
  shouldEmitEndedHookForRun: ContextCleanup["shouldEmitEndedHookForRun"];
  emitSubagentEndedHookForRun: ContextCleanup["emitSubagentEndedHookForRun"];
  emitSubagentProgressEndedForRun(entry: SubagentRunRecord): Promise<void>;
  notifyContextEngineSubagentEnded: ContextCleanup["notifyContextEngineSubagentEnded"];
  retireSupersededRun(runId: string, entry: SubagentRunRecord): Promise<void>;
  resumeSubagentRun(runId: string): void;
  /** Confirms a provisional kill once its run's controller operation settles. */
  confirmProvisionalKill(entry: SubagentRunRecord): void;
  callGateway: typeof defaultCallGateway;
  captureSubagentCompletionReply: CaptureSubagentCompletionReply;
  cleanupBrowserSessionsForLifecycleEnd?: BrowserCleanup;
  loadCleanupBrowserSessionsForLifecycleEnd?: () => Promise<BrowserCleanup>;
  runSubagentAnnounceFlow: RunSubagentAnnounceFlow;
  maybeWakeRequesterAfterAllChildrenSettled: MaybeWakeRequesterAfterAllChildrenSettled;
  warn(message: string, meta?: Record<string, unknown>): void;
};

export interface SubagentLifecycleCommonContext {
  readonly options: SubagentLifecycleOptions;
  newerGenerationOwnsSession(entry: SubagentRunRecord): boolean;
  shouldSuppressSessionEffects(
    entry: SubagentRunRecord,
    prospectiveEffects?: SubagentSessionEffects,
  ): Promise<boolean>;
  sessionEffectsHostCurrent(entry: SubagentRunRecord): boolean;
  getSessionEffects(entry: SubagentRunRecord): SubagentSessionEffects | undefined;
}

export interface SubagentLifecycleCompletionContext extends SubagentLifecycleCommonContext {
  readonly progressEndedEntries: WeakSet<object>;
  acquireTerminalCompletionLock(runId: string): Promise<() => void>;
  bindTerminalSessionEffects(entry: SubagentRunRecord, effects?: SubagentSessionEffects): void;
  bumpCleanupGeneration(entry: SubagentRunRecord): number;
  bumpTerminalGeneration(entry: SubagentRunRecord, bindingChanged?: boolean): number;
  isTerminalCallbackCurrent(runId: string, entry: SubagentRunRecord, generation: number): boolean;
  startSubagentAnnounceCleanupFlow(runId: string, entry: SubagentRunRecord): boolean;
}

export interface SubagentLifecycleCleanupContext extends SubagentLifecycleCommonContext {
  readonly cleanupReservations: Set<object>;
  readonly activeCleanupAttempts: Map<object, number>;
  pruneRetiredRuns(runIds?: readonly string[]): void;
  bumpCleanupGeneration(entry: SubagentRunRecord): number;
  isCleanupAttemptCurrent(runId: string, entry: SubagentRunRecord, generation: number): boolean;
  isCleanupGeneration(entry: SubagentRunRecord, generation: number): boolean;
  isCleanupGenerationCurrent(runId: string, entry: SubagentRunRecord, generation: number): boolean;
  isCleanupOwnerCurrent(runId: string, entry: SubagentRunRecord): boolean;
  startSubagentAnnounceCleanupFlow(runId: string, entry: SubagentRunRecord): boolean;
}

export interface SubagentLifecycleAnnounceCleanupContext
  extends SubagentLifecycleCleanupContext, SubagentLifecycleWakeContext {
  completeCleanupBookkeeping(args: CleanupBookkeepingParams): Promise<void>;
}

export interface SubagentLifecycleWakeContext extends SubagentLifecycleCommonContext {
  /** In-flight requester wake evaluations; a trigger during one requests a rerun. */
  readonly activeRequesterSettleWakes: Map<string, { rearm?: SubagentRunRecord }>;
  resumeAncestorCleanup(settledEntry: SubagentRunRecord): void;
  runRequesterSettleWake(
    entry: SubagentRunRecord,
    run: () => Promise<unknown>,
    isCurrent: () => boolean,
  ): Promise<unknown>;
}

export type CleanupBookkeepingParams = {
  stateContext?: OpenClawStateWorkerContext;
  runId: string;
  entry: SubagentRunRecord;
  cleanup: "delete" | "keep";
  completedAt: number;
  preserveTranscript?: boolean;
  provisionalKill?: boolean;
  skipRequesterSettleWake?: boolean;
  isCurrent?: () => boolean;
  discardDelivery?: (draft: SubagentRunRecord) => void;
};
