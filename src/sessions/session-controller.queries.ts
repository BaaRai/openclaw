import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { normalizeAgentId, parseAgentSessionKey } from "../routing/session-key.js";
import { ReplyRunAlreadyActiveError, type ReplyOperation } from "./session-controller.contracts.js";
import {
  activeSessionOperations,
  findSessionControllerEntries,
  getAttachedBackend,
  isReplyRunEvidenceStale,
  getSessionControllerOperation,
  hasReplyOperationExecutionStarted,
  isReplyOperationPreBackendPhase,
  resolveReplyRunForCurrentSessionId,
  getSessionControllerEntryForOperation,
} from "./session-controller.state.js";
import type { SessionTarget } from "./session-controller.target.js";

function resolveCurrentOperations(sessionId: string): ReplyOperation[] {
  const resolution = resolveReplyRunForCurrentSessionId(sessionId);
  return resolution.kind === "none"
    ? []
    : resolution.kind === "one"
      ? [resolution.operation]
      : resolution.operations;
}

export function isSessionRunActive(sessionId: string): boolean {
  return resolveReplyRunForCurrentSessionId(sessionId).kind !== "none";
}

/** Native liveness is a fact of the active turn, never a second busy-state vote. */
export function resolveSessionRunProgressState(
  sessionId: string,
  owner?: { agentId?: string; defaultAgentId?: string },
): "queued" | "running" | undefined {
  const eligible = resolveCurrentOperations(sessionId).filter((operation) => {
    if (operation.result) {
      return false;
    }
    if (!owner) {
      return true;
    }
    const requested = owner.agentId ?? owner.defaultAgentId;
    const recorded =
      operation.agentId ?? parseAgentSessionKey(operation.key)?.agentId ?? owner.defaultAgentId;
    const attachment = getSessionControllerEntryForOperation(operation).attachment;
    return Boolean(
      requested &&
      recorded &&
      normalizeAgentId(requested) === normalizeAgentId(recorded) &&
      attachment?.projectSessionActive !== false,
    );
  });
  if (eligible.length === 0) {
    return undefined;
  }
  return eligible.every(
    (operation) =>
      operation.phase === "waiting_for_global_lane" ||
      !hasReplyOperationExecutionStarted(operation),
  )
    ? "queued"
    : "running";
}
export function isSessionRunCompactionBlocked(sessionId: string): boolean {
  return resolveCurrentOperations(sessionId).some(
    (operation) => !isReplyOperationPreBackendPhase(operation.phase),
  );
}
export function getActiveSessionRunCount(): number {
  return [...activeSessionOperations()].length;
}
export function listActiveSessionRunKeys(): string[] {
  return [...activeSessionOperations()].map((operation) => operation.key).toSorted();
}
export function listActiveSessionRunIds(): string[] {
  return [...activeSessionOperations()].map((operation) => operation.sessionId).toSorted();
}
/** Resolves the active run for one logical key, narrowed to a physical owner when provided. */
export function resolveActiveSessionRunId(
  sessionKey: string,
  target?: SessionTarget,
): string | undefined {
  return getSessionControllerOperation(sessionKey.trim(), target)?.sessionId;
}

/** A logical key is busy even when it selects multiple physical owners. */
export function isSessionRunActiveForKey(sessionKey: string): boolean {
  const normalizedSessionKey = normalizeOptionalString(sessionKey);
  return Boolean(
    normalizedSessionKey &&
    findSessionControllerEntries(normalizedSessionKey).some((entry) => entry.active),
  );
}

/** Reads the active operation's delivery thread without selecting an ambiguous owner. */
export function resolveActiveSessionRunThreadId(sessionKey: string): string | number | undefined {
  return getSessionControllerOperation(sessionKey)?.routeThreadId;
}

export function isReplyRunEvidenceStaleBySessionId(sessionId: string): boolean {
  return resolveCurrentOperations(sessionId).some(isReplyRunEvidenceStale);
}

export function listActiveReplyRunSessionKeys(): string[] {
  return [...activeSessionOperations()].map((operation) => operation.key);
}

/** Resolves a single active physical owner, rejecting ambiguous current identities. */
export function resolveActiveReplyOperationForSessionId(
  sessionId: string,
): ReplyOperation | undefined {
  const resolution = resolveReplyRunForCurrentSessionId(sessionId);
  if (resolution.kind === "ambiguous") {
    throw new ReplyRunAlreadyActiveError(sessionId);
  }
  return resolution.kind === "one" ? resolution.operation : undefined;
}

/** Resolves the one slot-owning operation executing a backend or protocol run ID; ambiguity fails closed. */
export function findSessionControllerOperationByRunId(runId: string): ReplyOperation | undefined {
  const id = normalizeOptionalString(runId);
  if (!id) {
    return undefined;
  }
  const matches = [...activeSessionOperations()].filter((operation) => {
    const claim = getSessionControllerEntryForOperation(operation).mailbox?.claim;
    return (
      getAttachedBackend(operation)?.runId === id ||
      (claim?.operation === operation && claim.inputs.some((input) => input.protocolRunId === id))
    );
  });
  return matches.length === 1 ? matches[0] : undefined;
}
