import type { FollowupRun, QueueSettings } from "../auto-reply/reply/queue/types.js";
import type { createDeferredCore } from "../shared/deferred.js";
import type { DeliveryContext } from "../utils/delivery-context.types.js";
import type { ReplyOperation, ReplyTurnKind } from "./session-controller.contracts.js";
import type { inputCancellation } from "./session-controller.mailbox-source.js";
import type { SessionControllerEntry } from "./session-controller.state.js";
import type { SessionTarget } from "./session-controller.target.js";

export type SessionControllerSourceCustody = {
  enqueued?: boolean;
  adopted?: boolean;
  adopting?: Promise<void>;
  work?: Set<Promise<void>>;
  settling?: Promise<void>;
  completed?: boolean;
  cancellationRetired?: boolean;
  stopHeartbeat?: () => void;
  releaseAuthority?: () => void;
  failure?: unknown;
  disposeSource?: () => void;
  /** One in-process Gateway registration may adopt a pre-reserved source. */
  rpcAdopted?: boolean;
  /** The Gateway accepted this source into its protocol run registry. */
  rpcAccepted?: boolean;
};

export type SessionControllerInput = {
  readonly [inputCancellation]: AbortController;
  readonly abortSignal: AbortSignal;
  readonly instance: Readonly<{ id: string }>;
  sequence: number;
  readonly sourceTurnId?: string;
  protocolRunId?: string;
  policy: Readonly<QueueSettings>;
  mailbox: SessionControllerMailbox;
  readonly custody: SessionControllerSourceCustody;
  readonly settlement: ReturnType<typeof createDeferredCore<void>>;
  /** Current protocol-facing session identity before an operation owns it. */
  sourceSessionId?: string;
  /** Source admission identity survives later mailbox incarnation bindings. */
  target?: SessionTarget;
  source?: FollowupRun;
  sourceAdapter?: SessionControllerSourceAdapter;
  /** Process-local requester identity reference; never serialized or accepted over RPC. */
  readonly continuationCaller?: Readonly<{
    deliveryRoute?: DeliveryContext;
    run<T>(run: () => Promise<T>): Promise<T>;
  }>;
  phase: "preparing" | "waiting" | "injecting" | "claimed" | "consumed";
  readonly injectionOrder: {
    settled: Promise<boolean>;
    settle(consumed: boolean): void;
  };
  injectionAttempted?: true;
  injection?: {
    predecessor: Promise<boolean>;
    settled: Promise<boolean>;
    settle(accepted: boolean): void;
    accepted?: boolean;
  };
  claim?: SessionControllerMailboxClaim;
  ready?: (claim: SessionControllerMailboxClaim) => void;
  reject?: (error: unknown) => void;
  task?: (claim: SessionControllerMailboxClaim) => void;
  taskTurnKind?: ReplyTurnKind;
  withdrawalHolds: number;
  retirementRequested?: boolean;
  cancelling?: boolean;
  retirementPending?: boolean;
  payload: "unbound" | "ready" | "summary";
};

export type SessionControllerMailboxClaim = {
  readonly mailbox: SessionControllerMailbox;
  inputs: readonly SessionControllerInput[];
  sources: readonly FollowupRun[];
  readonly custody: SessionControllerSourceCustody;
  readonly summary: boolean;
  readonly settlement: ReturnType<typeof createDeferredCore<void>>;
  readonly abortController: AbortController;
  operation?: ReplyOperation;
  released: boolean;
  releaseRequested?: boolean;
  retryBeforeExecution?: boolean;
};

/** All runnable inputs, including direct native producers, share this ordered owner. */
export type SessionControllerMailbox = {
  readonly key: string;
  readonly owner: SessionControllerEntry;
  clearing?: boolean;
  nextSequence: number;
  entries: SessionControllerInput[];
  claim?: SessionControllerMailboxClaim;
  priority?: SessionControllerInput;
  wake(): void;
  dispatch?: (claim: SessionControllerMailboxClaim) => Promise<void>;
  dispatchEnabled?: boolean;
  timer?: ReturnType<typeof setTimeout>;
  abortController: AbortController;
  readonly items: FollowupRun[];
  readonly draining: boolean;
  readonly inFlight: ReadonlySet<FollowupRun>;
  lastEnqueuedAt: number;
  mode: QueueSettings["mode"];
  debounceMs: number;
  cap: number;
  dropPolicy: "old" | "new" | "summarize";
  droppedCount: number;
  summaryLines: string[];
  summarySources: FollowupRun[];
  activeSummarySources: Set<FollowupRun>;
  summaryElisions: Array<{
    contextKey: string;
    count: number;
    sources: FollowupRun[];
    summaryLines: string[];
    sourceRefs: Map<FollowupRun, FollowupRun>;
  }>;
  evictedSummaryCount: number;
  lastRun?: FollowupRun["run"];
  recentSources: Map<string, { input: SessionControllerInput; expires: number }>;
};

export type SessionControllerSourceAdapter = {
  readonly scope?: string;
  readonly authority?: { assertCurrent(): void; signal?: AbortSignal };
  readonly signal?: AbortSignal;
  readonly requester?: Readonly<{ deviceId?: string; connectionId?: string; clientId?: string }>;
  /** False for internal work that ordinary operator chat surfaces must preserve. */
  readonly controlUiVisible?: boolean;
  /** Initial presentation copied to the claimed operation; the adapter is not a runtime owner. */
  readonly projectSessionActive?: boolean;
  /** Side questions stay independent from main-turn session stops. */
  readonly turnKind?: "main" | "btw";
  cancel?(reason?: unknown): void;
  onSettled?(): void | Promise<void>;
};

export type SessionControllerSourceInjection = {
  admit(): Promise<boolean>;
  accepted(accepted: boolean): void;
  finish(consumed: boolean): void;
};

export type SessionControllerWithdrawalHold = (() => void) & {
  release(): void;
  commit(reason?: unknown): boolean;
  cancel(assertCurrent: () => void, reason?: unknown): boolean;
};
