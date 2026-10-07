import type {
  ReplyMessageInjectionOptions,
  ReplyOperation,
} from "../../sessions/session-controller.contracts.js";
import {
  createReplyOperation,
  resolveActiveReplyOperationForSessionId,
} from "../../sessions/session-controller.js";
import { steerSessionControllerOperation } from "../../sessions/session-controller.steer.js";
import { getActiveNativeAttempt } from "./run-state.js";
import { setActiveEmbeddedRun, clearActiveEmbeddedRun } from "./runs.js";
const fixtureOperations = new Set<ReplyOperation>();
/** Native boundary fixtures explicitly reserve their logical turn before publication. */
export function registerTestEmbeddedRun(
  ...args: Parameters<typeof setActiveEmbeddedRun>
): ReplyOperation {
  const [sessionId, handle, sessionKey, sessionFile, agentId, supplied, lifecycleGeneration] = args;
  let operation = supplied ?? resolveActiveReplyOperationForSessionId(sessionId);
  if (
    operation &&
    (operation.result || operation.abortSignal.aborted) &&
    fixtureOperations.delete(operation)
  ) {
    operation.complete();
    operation = undefined;
  }
  const created = !operation;
  if (!operation) {
    operation = createReplyOperation({
      sessionKey: sessionKey ?? "agent:test:" + sessionId,
      sessionId,
      agentId,
      resetTriggered: false,
    });
    fixtureOperations.add(operation);
  }
  try {
    setActiveEmbeddedRun(
      sessionId,
      handle,
      sessionKey ?? operation.key,
      sessionFile,
      agentId,
      operation,
      lifecycleGeneration,
    );
    return operation;
  } finally {
    // A rejected registration may retire only the reservation made by this call.
    if (created && getActiveNativeAttempt(sessionId) !== handle) {
      fixtureOperations.delete(operation);
      operation.complete();
    }
  }
}
/** Steers a session's active turn the way in-process callers do: through its controller. */
export function steerTestSessionTurn(
  sessionId: string,
  text: string,
  options: ReplyMessageInjectionOptions = {},
) {
  return steerSessionControllerOperation({
    operation: resolveActiveReplyOperationForSessionId(sessionId),
    text,
    options,
  });
}
export function clearTestEmbeddedRun(...args: Parameters<typeof clearActiveEmbeddedRun>): void {
  const [sessionId, handle] = args;
  const operation =
    getActiveNativeAttempt(sessionId) === handle
      ? resolveActiveReplyOperationForSessionId(sessionId)
      : undefined;
  clearActiveEmbeddedRun(...args);
  if (operation && fixtureOperations.delete(operation)) {
    operation.complete();
  }
}
import type { EmbeddedAgentQueueHandle } from "./run-state.js";

type RunHandle = EmbeddedAgentQueueHandle;

export function createEmbeddedRunHandle(
  overrides: {
    abort?: () => void;
    /** Expose guarded V2 injection over queueMessage, as host-authority runtimes do. */
    guarded?: boolean;
    isAbortable?: boolean;
    isCompacting?: boolean;
    isStreaming?: boolean;
    isStopped?: () => boolean;
    messageInjection?: RunHandle["messageInjection"];
    runId?: string;
    toolAuthorityFingerprint?: string;
    queueMessage?: RunHandle["queueMessage"];
    supportsQueueMessageImages?: boolean;
    supportsTranscriptCommitWait?: boolean;
  } = {},
): RunHandle {
  // Minimal handle fixture with overrideable lifecycle probes for registry
  // behavior; individual tests supply queue/abort behavior when needed.
  const abort = overrides.abort ?? (() => {});
  const queueMessage = overrides.queueMessage ?? (async () => {});
  return {
    runId: overrides.runId,
    toolAuthorityFingerprint: overrides.toolAuthorityFingerprint,
    queueMessage,
    ...(overrides.messageInjection ? { messageInjection: overrides.messageInjection } : {}),
    ...(overrides.guarded
      ? {
          messageInjectionV2: {
            version: 2 as const,
            isAvailable: () => true,
            queueMessage: async (
              text: string,
              options: Parameters<RunHandle["queueMessage"]>[1],
              assertCurrent: () => void,
            ) => {
              assertCurrent();
              return queueMessage(text, options);
            },
          },
        }
      : {}),
    isStreaming: () => overrides.isStreaming ?? true,
    ...(overrides.isStopped ? { isStopped: overrides.isStopped } : {}),
    ...(overrides.isAbortable !== undefined
      ? { isAbortable: () => overrides.isAbortable !== false }
      : {}),
    isCompacting: () => overrides.isCompacting ?? false,
    supportsQueueMessageImages: overrides.supportsQueueMessageImages,
    supportsTranscriptCommitWait: overrides.supportsTranscriptCommitWait,
    abort,
  };
}

type EmbeddedRunsTestApi = {
  resetActiveEmbeddedRuns(): void;
};
function getTestApi(): EmbeddedRunsTestApi {
  const api = (globalThis as Record<PropertyKey, unknown>)[
    Symbol.for("openclaw.embeddedRunsTestApi")
  ];
  if (!api) {
    throw new Error("embedded runs test API is unavailable");
  }
  return api as EmbeddedRunsTestApi;
}

export const testing = {
  ...getTestApi(),
  resetActiveEmbeddedRuns() {
    getTestApi().resetActiveEmbeddedRuns();
    for (const operation of fixtureOperations) {
      operation.complete();
    }
    fixtureOperations.clear();
  },
};
