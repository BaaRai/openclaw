import { afterEach, expect, it, vi } from "vitest";
import { testing } from "../../../auto-reply/reply/reply-run-registry.test-support.js";
import { createReplyOperation } from "../../../sessions/session-controller.operation.js";
import { createSubagentRunRecord } from "../../subagent-test-fixtures.test-helpers.js";
import { isClaimedByLiveRequesterTurn } from "./subagent-requester-turn-liveness.js";

afterEach(() => testing.resetReplyRunRegistry());

// Settlement runs after the last attempt detaches its backend, before the operation settles.
it("keeps a requester turn live between backend detach and operation settlement", () => {
  const operation = createReplyOperation({
    sessionKey: "agent:main:liveness-requester",
    sessionId: "liveness-requester-session",
    resetTriggered: false,
  });
  operation.setPhase("running");
  const backend = { kind: "embedded" as const, runId: "requester-turn", cancel: vi.fn() };
  const child = createSubagentRunRecord({
    runId: "liveness-child",
    childSessionKey: "agent:main:subagent:liveness-child",
    requesterSessionKey: "agent:main:liveness-requester",
    requesterTurnRunId: "requester-turn",
    expectsCompletionMessage: true,
  });

  operation.attachBackend(backend);
  expect(isClaimedByLiveRequesterTurn(child)).toBe(true);
  operation.detachBackend(backend);
  expect(isClaimedByLiveRequesterTurn(child)).toBe(true);
  operation.complete();
  expect(isClaimedByLiveRequesterTurn(child)).toBe(false);
});
