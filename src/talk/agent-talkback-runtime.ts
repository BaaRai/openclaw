/**
 * Debounced realtime voice talkback for delegated OpenClaw consults.
 *
 * Fragments from the same lane that arrive within the debounce window merge into
 * one question. Each question then starts its own consult; the session mailbox
 * orders those turns. Close aborts every in-flight consult.
 */
import type { RuntimeLogger } from "../plugins/runtime/types-core.js";

/** Overflow policy: questions beyond this many in-flight consults are dropped with a warning and spoken fallback. */
const MAX_IN_FLIGHT_CONSULTS = 8;

export type RealtimeVoiceAgentTalkbackResult = {
  text: string;
};

export type RealtimeVoiceAgentTalkbackQueue = {
  close(): void;
  enqueue(question: string, metadata?: unknown): void;
  isIdle(): boolean;
};

export type RealtimeVoiceAgentTalkbackQueueParams = {
  /** Delay used to merge nearby transcript fragments into one consult. */
  debounceMs: number;
  isStopped: () => boolean;
  logger: Pick<RuntimeLogger, "info" | "warn">;
  logPrefix: string;
  responseStyle: string;
  fallbackText: string;
  /** Delegates a batched question to OpenClaw and respects the abort signal. */
  consult: (args: {
    question: string;
    metadata?: unknown;
    responseStyle: string;
    signal: AbortSignal;
  }) => Promise<RealtimeVoiceAgentTalkbackResult>;
  /** Delivers final speakable text back to the realtime provider/session. */
  deliver: (text: string) => void;
};

type PendingQuestion = {
  question: string;
  metadata?: unknown;
};

export function createRealtimeVoiceAgentTalkbackQueue(
  params: RealtimeVoiceAgentTalkbackQueueParams,
): RealtimeVoiceAgentTalkbackQueue {
  let closed = false;
  let batch: PendingQuestion | undefined;
  let debounceTimer: ReturnType<typeof setTimeout> | undefined;
  const inFlight = new Set<AbortController>();

  const shouldStop = () => closed || params.isStopped();

  // Runs one consult and delivers its answer, or the fallback on failure.
  const consult = async (pending: PendingQuestion, controller: AbortController) => {
    const startedAt = Date.now();
    params.logger.info(
      `${params.logPrefix} consult: chars=${pending.question.length} inFlight=${inFlight.size}`,
    );
    try {
      const result = await params.consult({
        question: pending.question,
        metadata: pending.metadata,
        responseStyle: params.responseStyle,
        signal: controller.signal,
      });
      const text = result.text.trim();
      params.logger.info(
        `${params.logPrefix} consult done: elapsedMs=${Date.now() - startedAt} answerChars=${text.length}`,
      );
      if (!shouldStop() && text) {
        params.deliver(text);
      }
    } catch (error) {
      if (shouldStop() || isAbortError(error)) {
        return;
      }
      const message = error instanceof Error ? error.message : String(error);
      params.logger.warn(
        `${params.logPrefix} consult failed: elapsedMs=${Date.now() - startedAt} ${message}`,
      );
      params.deliver(params.fallbackText);
    } finally {
      inFlight.delete(controller);
    }
  };

  // Ends the debounce window and starts the merged question as its own consult.
  const flush = () => {
    clearTimeout(debounceTimer);
    debounceTimer = undefined;
    const pending = batch;
    batch = undefined;
    if (!pending || shouldStop()) {
      return;
    }
    if (inFlight.size >= MAX_IN_FLIGHT_CONSULTS) {
      params.logger.warn(
        `${params.logPrefix} consult dropped: inFlight=${inFlight.size} droppedChars=${pending.question.length}`,
      );
      params.deliver(params.fallbackText);
      return;
    }
    const controller = new AbortController();
    inFlight.add(controller);
    void consult(pending, controller);
  };

  return {
    isIdle: () => debounceTimer === undefined && inFlight.size === 0,
    close: () => {
      if (closed) {
        return;
      }
      closed = true;
      clearTimeout(debounceTimer);
      debounceTimer = undefined;
      batch = undefined;
      for (const controller of inFlight) {
        controller.abort();
      }
    },
    enqueue: (question, metadata) => {
      const trimmed = question.trim();
      if (!trimmed || shouldStop()) {
        return;
      }
      // Metadata identity is the caller/context lane; only one lane merges per window.
      if (batch && !Object.is(batch.metadata, metadata)) {
        flush();
      }
      if (batch) {
        batch.question = `${batch.question}\n${trimmed}`;
      } else {
        batch = { question: trimmed, metadata };
      }
      // Debounce short transcript bursts so partial ASR fragments become a
      // single consult question instead of multiple back-to-back agent turns.
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(flush, params.debounceMs);
      debounceTimer.unref?.();
    },
  };
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}
