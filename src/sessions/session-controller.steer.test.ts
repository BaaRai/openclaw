import { afterEach, describe, expect, it } from "vitest";
import { QuestionAnswerUnconfirmedError } from "../agents/harness/gateway-question-dispatch.js";
import { createDeferredCore } from "../shared/deferred.js";
import type { ReplyBackendHandle } from "./session-controller.contracts.js";
import {
  clearSessionControllerMailbox,
  reserveSessionControllerSource,
  retireSessionControllerInput,
  tryClaimSessionControllerTask,
  type SessionControllerInput,
} from "./session-controller.mailbox.js";
import { captureCurrentReplyMessageInjectionTarget } from "./session-controller.message-injection.js";
import { createReplyOperation } from "./session-controller.operation.js";
import { findSessionControllerOperationByRunId } from "./session-controller.queries.js";
import { submitSessionControllerSteer } from "./session-controller.steer.js";

const key = "agent:main:controller-steer";
const cleanups: Array<() => void> = [];
afterEach(() => {
  for (const cleanup of cleanups.splice(0)) {
    cleanup();
  }
});

type QueueMessage = (options: { onQueueAccepted?: (accepted: boolean) => void }) => Promise<void>;

/** One running turn whose backend injection behavior is chosen per case. */
function startTurn(params: {
  queueMessage: QueueMessage;
  injection?: "v2" | "legacy";
  compacting?: boolean;
}) {
  const operation = createReplyOperation({
    sessionKey: key,
    sessionId: "steer-session",
    resetTriggered: false,
  });
  operation.setPhase("running");
  const queueMessage = (_text: string, options?: Parameters<QueueMessage>[0]) =>
    params.queueMessage(options ?? {});
  const backend: ReplyBackendHandle = {
    kind: "embedded",
    runId: "steer-run",
    cancel: () => {},
    isCompacting: () => params.compacting === true,
    ...(params.injection === "legacy"
      ? { messageInjection: { isAvailable: () => true, queueMessage } }
      : { messageInjectionV2: { version: 2, isAvailable: () => true, queueMessage } }),
  };
  operation.attachBackend(backend);
  const input = reserveSessionControllerSource(key, { policy: { mode: "steer" } });
  cleanups.push(() => {
    operation.complete();
    retireSessionControllerInput(input);
    clearSessionControllerMailbox(input.mailbox, () => {});
  });
  return { operation, input };
}

const steer = (input: SessionControllerInput) =>
  submitSessionControllerSteer({ input, text: "steer", options: { steeringMode: "all" } });

function expectQueued(input: SessionControllerInput) {
  expect(input.phase).toBe("preparing");
  expect(input.retirementRequested).toBeUndefined();
  expect(input.mailbox.entries).toContain(input);
}

describe("submitSessionControllerSteer", () => {
  it("consumes an accepted steer into the captured turn", async () => {
    const { input } = startTurn({
      queueMessage: async ({ onQueueAccepted }) => onQueueAccepted?.(true),
    });

    await expect(steer(input)).resolves.toEqual({
      status: "accepted",
      targetRunId: "steer-run",
      result: undefined,
    });
    await input.settlement.promise;
    expect(input.phase).toBe("consumed");
  });

  it("leaves a definitely rejected steer queued for its followup", async () => {
    const { input } = startTurn({
      queueMessage: async () => {
        throw new Error("backend rejected");
      },
    });

    await expect(steer(input)).resolves.toMatchObject({
      status: "rejected",
      reason: "runtime_rejected",
    });
    expectQueued(input);
  });

  it("keeps custody of an indeterminate steer and never returns it to the queue", async () => {
    const receipt = createDeferredCore();
    const { input } = startTurn({ queueMessage: () => receipt.promise });

    const result = steer(input);
    await Promise.resolve();
    await Promise.resolve();
    expect(input.phase).toBe("injecting");
    expect(tryClaimSessionControllerTask(input)).toBeUndefined();
    receipt.reject(new QuestionAnswerUnconfirmedError("unknown acceptance"));

    await expect(result).resolves.toMatchObject({ status: "indeterminate" });
    await input.settlement.promise;
    expect(input.phase).toBe("consumed");
  });

  it.each([
    ["v2", "accepted"],
    ["legacy", "rejected"],
  ] as const)("during compaction a %s backend is %s", async (injection, status) => {
    const { input } = startTurn({
      injection,
      compacting: true,
      queueMessage: async ({ onQueueAccepted }) => onQueueAccepted?.(true),
    });

    const result = await steer(input);

    expect(result.status).toBe(status);
    if (status === "rejected") {
      expect(result).toMatchObject({ reason: "injection_unavailable" });
      expectQueued(input);
    }
  });

  it("refuses injection into a captured turn after it yields and records the result once", async () => {
    let injected = false;
    const { operation, input } = startTurn({
      queueMessage: async () => {
        injected = true;
      },
    });
    const target = captureCurrentReplyMessageInjectionTarget(key);

    expect(operation.yield()).toBe(true);
    expect(operation.yield()).toBe(false);

    await expect(
      submitSessionControllerSteer({ input, target, text: "late", options: {} }),
    ).resolves.toMatchObject({ status: "rejected", reason: "not_running" });
    expect(injected).toBe(false);
    expectQueued(input);
    operation.complete();
    expect(operation.result).toEqual({ kind: "yielded" });
  });
});

describe("findSessionControllerOperationByRunId", () => {
  it("resolves the slot-owning operation by its backend run ID only while it owns the slot", () => {
    const { operation } = startTurn({ queueMessage: async () => {} });

    expect(findSessionControllerOperationByRunId("steer-run")).toBe(operation);
    expect(findSessionControllerOperationByRunId("other-run")).toBeUndefined();
    operation.complete();
    expect(findSessionControllerOperationByRunId("steer-run")).toBeUndefined();
  });
});
