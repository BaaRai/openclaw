// Agent talkback runtime tests cover agent response playback into talk sessions.
import { afterEach, describe, expect, it, vi } from "vitest";
import { createRealtimeVoiceAgentTalkbackQueue } from "./agent-talkback-runtime.js";

afterEach(() => {
  vi.useRealTimers();
});

function makeLogger() {
  return {
    info: vi.fn(),
    warn: vi.fn(),
  };
}

type QueueParams = Parameters<typeof createRealtimeVoiceAgentTalkbackQueue>[0];

function createQueue(params: Pick<QueueParams, "consult"> & Partial<QueueParams>) {
  return createRealtimeVoiceAgentTalkbackQueue({
    debounceMs: 1,
    isStopped: () => false,
    logger: makeLogger(),
    logPrefix: "[test]",
    responseStyle: "brief",
    fallbackText: "fallback",
    deliver: vi.fn(),
    ...params,
  });
}

function expectConsultRequest(
  call: unknown,
  expected: { metadata: unknown; question: string; responseStyle: string },
) {
  if (!call || typeof call !== "object") {
    throw new Error("Expected talkback consult request object");
  }
  const { signal, ...request } = call as { signal?: unknown };
  expect(signal).toBeInstanceOf(AbortSignal);
  expect(request).toStrictEqual(expected);
}

function expectConsultCall(
  consult: { mock: { calls: unknown[][] } },
  callIndex: number,
  expected: { metadata: unknown; question: string; responseStyle: string },
) {
  const call = consult.mock.calls[callIndex]?.[0];
  if (call === undefined) {
    throw new Error(`Expected talkback consult call ${callIndex}`);
  }
  expectConsultRequest(call, expected);
}

describe("realtime voice agent talkback queue", () => {
  it("merges same-lane fragments within the debounce window and splits lanes", async () => {
    vi.useFakeTimers();
    const ownerMetadata = { senderIsOwner: true };
    const guestMetadata = { senderIsOwner: false };
    const consult = vi.fn(async ({ question }: { question: string }) => ({
      text: `answer: ${question}`,
    }));
    const deliver = vi.fn();
    const queue = createQueue({ debounceMs: 10, consult, deliver });

    queue.enqueue("owner one", ownerMetadata);
    queue.enqueue("owner two", ownerMetadata);
    queue.enqueue("guest", guestMetadata);
    expect(queue.isIdle()).toBe(false);
    await vi.advanceTimersByTimeAsync(10);

    expectConsultCall(consult, 0, {
      metadata: ownerMetadata,
      question: "owner one\nowner two",
      responseStyle: "brief",
    });
    expectConsultCall(consult, 1, {
      metadata: guestMetadata,
      question: "guest",
      responseStyle: "brief",
    });
    expect(deliver.mock.calls).toEqual([["answer: owner one\nowner two"], ["answer: guest"]]);
    expect(queue.isIdle()).toBe(true);
  });

  it("starts a later question while an earlier consult is still in flight", async () => {
    vi.useFakeTimers();
    let finishFirst: ((value: { text: string }) => void) | undefined;
    const consult = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise<{ text: string }>((resolve) => {
            finishFirst = resolve;
          }),
      )
      .mockResolvedValueOnce({ text: "second-answer" });
    const deliver = vi.fn();
    const queue = createQueue({ consult, deliver });

    queue.enqueue("first");
    await vi.advanceTimersByTimeAsync(1);
    queue.enqueue("second");
    await vi.advanceTimersByTimeAsync(1);

    expect(consult).toHaveBeenCalledTimes(2);
    expect(deliver).toHaveBeenCalledExactlyOnceWith("second-answer");
    expect(queue.isIdle()).toBe(false);
    finishFirst?.({ text: "first-answer" });
    await vi.runAllTimersAsync();
    expect(deliver).toHaveBeenLastCalledWith("first-answer");
    expect(queue.isIdle()).toBe(true);
  });

  it("speaks the fallback and warns for questions beyond the in-flight cap", async () => {
    vi.useFakeTimers();
    const logger = makeLogger();
    const deliver = vi.fn();
    const consult = vi.fn(() => new Promise<{ text: string }>(() => {}));
    const queue = createQueue({ logger, consult, deliver });

    for (let index = 0; index < 9; index += 1) {
      queue.enqueue(`question-${index}`);
      await vi.advanceTimersByTimeAsync(1);
    }

    expect(consult).toHaveBeenCalledTimes(8);
    expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
      "[test] consult dropped: inFlight=8 droppedChars=10",
    );
    expect(deliver).toHaveBeenCalledExactlyOnceWith("fallback");
  });

  it("delivers fallback text when consult fails", async () => {
    vi.useFakeTimers();
    const logger = makeLogger();
    const deliver = vi.fn();
    const queue = createQueue({
      logger,
      consult: vi.fn(async () => {
        throw new Error("boom");
      }),
      deliver,
    });

    queue.enqueue("question");
    await vi.advanceTimersByTimeAsync(1);

    expect(logger.warn).toHaveBeenCalledExactlyOnceWith("[test] consult failed: elapsedMs=0 boom");
    expect(deliver).toHaveBeenCalledWith("fallback");
  });

  it("cancels pending debounced work on close", async () => {
    vi.useFakeTimers();
    const consult = vi.fn(async () => ({ text: "answer" }));
    const queue = createQueue({
      debounceMs: 100,
      consult,
    });

    queue.enqueue("question");
    queue.close();
    await vi.advanceTimersByTimeAsync(100);

    expect(consult).not.toHaveBeenCalled();
  });

  it("aborts every in-flight consult on close without delivering fallback", async () => {
    vi.useFakeTimers();
    const logger = makeLogger();
    const signals: AbortSignal[] = [];
    const consult = vi.fn(
      ({ signal }: { signal: AbortSignal }) =>
        new Promise<{ text: string }>((_resolve, reject) => {
          signals.push(signal);
          signal.addEventListener("abort", () => {
            const error = new Error("aborted");
            error.name = "AbortError";
            reject(error);
          });
        }),
    );
    const deliver = vi.fn();
    const queue = createQueue({ logger, consult, deliver });

    queue.enqueue("first");
    await vi.advanceTimersByTimeAsync(1);
    queue.enqueue("second");
    await vi.advanceTimersByTimeAsync(1);
    queue.close();
    queue.close();
    queue.enqueue("late question");
    await vi.runAllTimersAsync();

    expect(signals).toHaveLength(2);
    expect(signals.every((signal) => signal.aborted)).toBe(true);
    expect(consult).toHaveBeenCalledTimes(2);
    expect(deliver).not.toHaveBeenCalled();
    expect(logger.warn).not.toHaveBeenCalled();
    expect(queue.isIdle()).toBe(true);
  });
});
