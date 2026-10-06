import type { OperationalRunInstanceRef } from "../agents/admitted-run-context.js";
import type { ChatAbortDiagnosticReason } from "../gateway/chat-abort-diagnostics.js";
import { chatRunBelongsToAgent } from "../gateway/chat-run-owner.js";
import type { AgentRunDelegatedAuthority } from "../infra/agent-run-authority.types.js";
import {
  isSessionControllerSourceQueued,
  retireSessionControllerInput,
  type SessionControllerInput,
  type SessionControllerSourceAdapter,
} from "./session-controller.mailbox.js";
import {
  findSessionControllerEntries,
  getSessionControllerEntryForOperation,
  hasReplyOperationExecutionStarted,
  isCurrentSessionControllerOperation,
  sessionControllers,
} from "./session-controller.state.js";
import {
  cancelCapturedSessionControllerSource,
  captureSessionControllerStop,
} from "./session-controller.stop.js";
import { rpcSourceRemovalByRef, rpcSourcesByRunId } from "./session-controller.storage.js";
import type { SessionTarget } from "./session-controller.target.js";

type ChatTerminalProducer = {
  sessionId: string;
  sessionKey: string;
  handoff: (settle: (producerCompleted: Promise<void>) => Promise<void>) => boolean;
};

export type RpcSourceAdapter = SessionControllerSourceAdapter & {
  /** Captures this run's canonical producer before cancellation releases its live slot. */
  resolveTerminalProducer?: () => ChatTerminalProducer | undefined;
  lifecycleGeneration?: string;
  /** Exact operational instance created by this controller registration. */
  operationalRunInstance?: OperationalRunInstanceRef;
  /** Exact approval lease captured when this controller's execution was admitted. */
  agentRunDelegatedAuthority?: AgentRunDelegatedAuthority;
  providerId?: string;
  authProviderId?: string;
  abortStopReason?: string;
  /** Owner-recorded diagnostic cause; does not change terminal lifecycle semantics. */
  abortDiagnosticReason?: ChatAbortDiagnosticReason;
  /** Latest argument-free validation diagnostic for operator-initiated aborts. */
  toolErrorSummary?: string;
  /** True after the terminal session-store update has completed. */
  projectSessionTerminalPersisted?: boolean;
  /** A terminal lifecycle event was observed and is awaiting persistence. */
  projectSessionTerminalPending?: boolean;
  /** Store timestamp expected from the observed terminal lifecycle event. */
  projectSessionTerminalObservedAt?: number;
  /** In-flight terminal session-store update used by restart shutdown. */
  projectSessionTerminalPersistence?: Promise<void>;
  /** Which Gateway RPC owns this protocol projection. */
  kind?: "chat-send" | "agent";
};

/** Byte-exact protocol correlation; scheduling and cancellation belong to input. */
export type RpcSourceRef = Readonly<{ input: SessionControllerInput; adapter: RpcSourceAdapter }>;

export type RpcSourceIdentity = Readonly<{
  sessionId: string;
  sessionKey: string;
  agentId?: string;
}>;

/** Reads logical identity from the exact operation, then its captured source target. */
export function getRpcSourceIdentity(ref: RpcSourceRef): RpcSourceIdentity {
  return getSessionControllerSourceIdentity(ref.input);
}

/** Reads logical identity from an exact reserved or adopted controller source. */
export function getSessionControllerSourceIdentity(
  input: SessionControllerInput,
): RpcSourceIdentity {
  const operation = input.claim?.operation;
  return {
    sessionId: operation?.sessionId ?? input.sourceSessionId ?? input.target?.incarnation ?? "",
    sessionKey: operation?.key ?? input.target?.sessionKey ?? input.mailbox.key,
    agentId: operation?.agentId ?? input.target?.agentId,
  };
}

/** Reads lifecycle generation from the operation once a turn owns the source. */
export function getRpcSourceLifecycleGeneration(ref: RpcSourceRef): string | undefined {
  return ref.input.claim?.operation?.lifecycleGeneration ?? ref.adapter.lifecycleGeneration;
}

/** Updates the controller-owned source identity and its operation, when claimed. */
export function updateRpcSourceSessionId(ref: RpcSourceRef, sessionId: string): void {
  const normalized = sessionId.trim();
  if (!normalized) {
    return;
  }
  ref.input.sourceSessionId = normalized;
  ref.input.claim?.operation?.updateSessionId(normalized);
}

function removeRpcSource(runId: string, ref: RpcSourceRef): boolean {
  const sources = rpcSourcesByRunId.get(runId);
  if (!sources?.delete(ref)) {
    return false;
  }
  if (sources.size === 0) {
    rpcSourcesByRunId.delete(runId);
  }
  const onRemoved = rpcSourceRemovalByRef.get(ref);
  rpcSourceRemovalByRef.delete(ref);
  try {
    onRemoved?.();
  } catch {
    // Removal observers cannot reject controller settlement.
  }
  return true;
}

/** Resolves the exact controller-owned source registered for a protocol run. */
export function getRpcSource(runId: string): RpcSourceRef | undefined {
  const sources = rpcSourcesByRunId.get(runId);
  return sources?.size === 1 ? sources.values().next().value : undefined;
}

/** Resolves one source for the exact controller target; ambiguity fails closed. */
export function getRpcSourceForTarget(
  runId: string,
  target: SessionTarget,
): RpcSourceRef | undefined {
  const owners = new Set(findSessionControllerEntries(target.sessionKey, target));
  const matches = [...(rpcSourcesByRunId.get(runId) ?? [])].filter((source) =>
    owners.has(source.input.mailbox.owner),
  );
  return matches.length === 1 ? matches[0] : undefined;
}

/** Reports whether this exact source remains in the controller-owned index. */
export function isRpcSourceRegistered(ref: RpcSourceRef): boolean {
  const runId = ref.input.protocolRunId;
  return runId !== undefined && rpcSourcesByRunId.get(runId)?.has(ref) === true;
}

/** Reports whether this protocol ID is still owned by the source's controller target. */
export function hasRpcSourceForController(runId: string, ref: RpcSourceRef): boolean {
  return [...(rpcSourcesByRunId.get(runId) ?? [])].some(
    (source) => source.input.mailbox.owner === ref.input.mailbox.owner,
  );
}

/** Resolves one exact pre-registration source; ambiguous protocol IDs fail closed. */
export function getReservedRpcSourceInput(runId: string): SessionControllerInput | undefined {
  if (hasRpcSource(runId)) {
    return undefined;
  }
  let match: SessionControllerInput | undefined;
  for (const entry of sessionControllers.values()) {
    for (const input of entry.mailbox?.entries ?? []) {
      if (
        input.protocolRunId !== runId ||
        input.phase === "consumed" ||
        input.retirementRequested ||
        input.custody.cancellationRetired
      ) {
        continue;
      }
      if (match && match !== input) {
        return undefined;
      }
      match = input;
    }
  }
  return match;
}

/** Reports whether the controller owns a source for a protocol run. */
export function hasRpcSource(runId: string): boolean {
  return (rpcSourcesByRunId.get(runId)?.size ?? 0) > 0;
}

/** Reports whether an indexed source can still own the matching protocol attempt. */
export function hasUnretiredRpcSource(
  runId: string,
  scope: Partial<RpcSourceIdentity> = {},
): boolean {
  return [...(rpcSourcesByRunId.get(runId) ?? [])].some((ref) => {
    const identity = getRpcSourceIdentity(ref);
    return (
      ref.input.phase !== "consumed" &&
      !ref.input.retirementRequested &&
      !ref.input.custody.cancellationRetired &&
      !ref.input.abortSignal.aborted &&
      (scope.sessionId === undefined || identity.sessionId === scope.sessionId) &&
      (scope.sessionKey === undefined || identity.sessionKey === scope.sessionKey) &&
      (scope.agentId === undefined || identity.agentId === scope.agentId)
    );
  });
}

/** Returns a stable snapshot for Gateway projection, shutdown, and abort iteration. */
export function listRpcSourceEntries(): Array<[runId: string, ref: RpcSourceRef]> {
  return [...rpcSourcesByRunId].flatMap(([runId, sources]) =>
    [...sources].map((ref): [string, RpcSourceRef] => [runId, ref]),
  );
}

/** Returns sources owned by the exact physical controller selected by a captured target. */
export function listRpcSourceEntriesForTarget(
  target: SessionTarget,
): Array<[runId: string, ref: RpcSourceRef]> {
  const owners = new Set(findSessionControllerEntries(target.sessionKey, target));
  return listRpcSourceEntries().filter(([, source]) => owners.has(source.input.mailbox.owner));
}

/** Registers protocol correlation after the controller has reserved the source input. */
export function registerRpcSource(runId: string, ref: RpcSourceRef, onRemoved?: () => void): void {
  if (ref.input.protocolRunId !== runId) {
    throw new Error("RPC source protocol ID does not match its index key");
  }
  const sources = rpcSourcesByRunId.get(runId) ?? new Set<RpcSourceRef>();
  if ([...sources].some((source) => source.input.mailbox.owner === ref.input.mailbox.owner)) {
    throw new Error(`RPC source already registered for run ${runId} on this session controller`);
  }
  sources.add(ref);
  rpcSourcesByRunId.set(runId, sources);
  ref.input.custody.rpcAccepted = true;
  if (onRemoved) {
    rpcSourceRemovalByRef.set(ref, onRemoved);
  }
  const settled = () => removeRpcSource(runId, ref);
  void ref.input.settlement.promise.then(settled, settled);
}

/** Requests retirement; the index leaves only when the exact controller input settles. */
export function retireRpcSource(runId: string, expected?: RpcSourceRef): boolean {
  const ref = expected ?? getRpcSource(runId);
  if (
    !ref ||
    ref.input.protocolRunId !== runId ||
    rpcSourcesByRunId.get(runId)?.has(ref) !== true
  ) {
    return false;
  }
  if (
    ref.adapter.projectSessionTerminalPending === true &&
    !ref.adapter.projectSessionTerminalPersistence
  ) {
    ref.input.retirementRequested = true;
    return true;
  }
  retireSessionControllerInput(ref.input);
  // Unclaimed retirement settles synchronously; drop the index now so the same
  // protocol run ID can be reserved again before the settlement observer runs.
  if (ref.input.phase === "consumed") {
    removeRpcSource(runId, ref);
  }
  return true;
}

export function isRpcSourceQueued(ref: RpcSourceRef | undefined): boolean {
  return (
    ref !== undefined &&
    isSessionControllerSourceQueued(ref.input) &&
    !ref.input.custody.cancellationRetired &&
    !ref.input.abortSignal.aborted
  );
}

/** Projection only: reservations, preparing input and retained receipts are not execution. */
export function isRpcSourceExecuting(ref: RpcSourceRef | undefined): boolean {
  const claim = ref?.input.claim;
  const operation = claim?.operation;
  return (
    ref !== undefined &&
    operation !== undefined &&
    !claim?.released &&
    !operation.result &&
    isCurrentSessionControllerOperation(operation) &&
    hasReplyOperationExecutionStarted(operation) &&
    !operation.abortSignal.aborted &&
    !ref.input.abortSignal.aborted
  );
}

export function isRpcSourceActive(ref: RpcSourceRef | undefined): boolean {
  return isRpcSourceExecuting(ref) && getRpcSourceProjectSessionActive(ref) !== false;
}

/** Presentation only: accepted controller custody remains queued until backend execution starts. */
export function resolveRpcSourceSessionProgressState(
  ref: RpcSourceRef | undefined,
): "queued" | "running" | undefined {
  if (
    !ref ||
    ref.input.phase === "consumed" ||
    ref.input.retirementRequested ||
    ref.input.custody.cancellationRetired ||
    ref.input.abortSignal.aborted ||
    getRpcSourceProjectSessionActive(ref) === false
  ) {
    return undefined;
  }
  const claim = ref.input.claim;
  const operation = claim?.operation;
  if (
    (ref.input.custody.rpcAccepted !== true && claim === undefined) ||
    claim?.released ||
    operation?.result ||
    (operation !== undefined &&
      (!isCurrentSessionControllerOperation(operation) || operation.abortSignal.aborted))
  ) {
    return undefined;
  }
  return isRpcSourceExecuting(ref) ? "running" : "queued";
}

/** Reads the active-session presentation fact from the exact controller attachment. */
export function getRpcSourceProjectSessionActive(
  ref: RpcSourceRef | undefined,
): boolean | undefined {
  if (
    ref?.adapter.projectSessionTerminalPending === true ||
    ref?.adapter.projectSessionTerminalPersisted === true
  ) {
    return false;
  }
  const operation = ref?.input.claim?.operation;
  const attachment = operation && getSessionControllerEntryForOperation(operation).attachment;
  if (attachment && attachment.operation === operation) {
    return attachment.projectSessionActive;
  }
  return ref?.input.retirementRequested === true ? false : undefined;
}

/** Updates presentation on the exact operation attachment without creating a second owner. */
export function setRpcSourceProjectSessionActive(
  ref: RpcSourceRef,
  active: boolean | undefined,
): void {
  const operation = ref.input.claim?.operation;
  if (!operation) {
    return;
  }
  const entry = getSessionControllerEntryForOperation(operation);
  if (entry.active !== operation) {
    return;
  }
  if (entry.attachment?.operation === operation) {
    entry.attachment.projectSessionActive = active;
    return;
  }
  entry.attachment = { operation, projectSessionActive: active };
}

export function getRpcSourceStartedAt(ref: RpcSourceRef): number | undefined {
  const operation = ref.input.claim?.operation;
  return operation && hasReplyOperationExecutionStarted(operation)
    ? operation.startedAtMs
    : undefined;
}

/** Exact owner operation, not a run-ID lookup; controller owns cancellation and settlement. */
export function requestRpcSourceCancellation(
  ref: RpcSourceRef,
  reason?: unknown,
  assertCurrent: () => void = () => {},
): boolean {
  const capture = captureSessionControllerStop({ inputs: [ref.input] });
  return cancelCapturedSessionControllerSource(capture, {
    reason,
    assertCurrent,
  }).abortedInputs.includes(ref.input);
}

export function isRpcSourceQueuedForSession(runId: string, scope: RpcSourceIdentity): boolean {
  const matches = [...(rpcSourcesByRunId.get(runId) ?? [])].filter((ref) => {
    const identity = getRpcSourceIdentity(ref);
    return (
      identity.sessionId === scope.sessionId &&
      identity.sessionKey === scope.sessionKey &&
      identity.agentId === scope.agentId
    );
  });
  const ref = matches.length === 1 ? matches[0] : undefined;
  return ref !== undefined && isRpcSourceQueued(ref) && isRpcSourceRegistered(ref);
}

/** Capture presentation correlation with exact inputs; no scheduler state is copied. */
export function listRpcSourceEntriesForSession(params: {
  sessionKeys: Iterable<string>;
  sessionIds?: Iterable<string | undefined>;
  requiredSessionId?: string;
  agentId?: string;
  defaultAgentId?: string;
  queuedOnly?: boolean;
}): Array<{ runId: string; entry: RpcSourceRef }> {
  const keys = new Set(params.sessionKeys);
  const ids = new Set(params.sessionIds ?? []);
  return listRpcSourceEntries().flatMap(([runId, entry]) => {
    const identity = getRpcSourceIdentity(entry);
    const matches =
      (!params.queuedOnly || isRpcSourceQueued(entry)) &&
      (keys.has(identity.sessionKey) || ids.has(identity.sessionId)) &&
      (params.requiredSessionId === undefined ||
        (keys.has(identity.sessionKey) && identity.sessionId === params.requiredSessionId)) &&
      (!params.agentId ||
        chatRunBelongsToAgent(
          {
            agentId: identity.agentId,
            sessionKey: identity.sessionKey,
            defaultAgentId: params.defaultAgentId,
          },
          params.agentId,
        ));
    return matches ? [{ runId, entry }] : [];
  });
}
