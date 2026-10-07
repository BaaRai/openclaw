// Requester settle wake tests cover the registry-less top-level requester.
import { describe, expect, it } from "vitest";
import { createDeferred } from "../../../../test/helpers/promise.js";
import { matchesTranscriptEvent } from "../../../sessions/transcript-visible-record.js";
import { buildAgentRunTerminalReplySnapshot } from "../../agent-run-terminal-reply.js";
import {
  sessionStore,
  setSessionStore,
  registryRuntimeMock,
  readDescendantFacts,
  findTranscriptEventMock,
  listedRequesterRuns,
  wakeParams,
} from "./subagent-announce.requester-settle-fixture.test-support.js";
import {
  requesterSettleKey,
  deliverSpy,
  makeSettledChild,
  completeBatchSpy,
  deliveredCallArg,
} from "./subagent-announce.requester-settle-wake.test-support.js";

const { maybeWakeRequesterAfterAllChildrenSettled } =
  await import("./subagent-announce.requester-settle-wake.js");

describe("maybeWakeRequesterAfterAllChildrenSettled", () => {
  it("lets a healthy sibling decide while an earlier descendant read loses its source", async () => {
    const children = ["run-a", "run-b"].map((runId) => makeSettledChild({ runId }));
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue(children);
    const readStarted = createDeferred();
    const staleRead = createDeferred<undefined>();
    readDescendantFacts.mockImplementationOnce(() => {
      readStarted.resolve();
      return staleRead.promise;
    });
    const first = maybeWakeRequesterAfterAllChildrenSettled(
      wakeParams({ settledEntry: children[0] }),
    );
    try {
      await readStarted.promise;
      await expect(
        maybeWakeRequesterAfterAllChildrenSettled(wakeParams({ settledEntry: children[1] })),
      ).resolves.toBe(true);
      expect(deliverSpy).toHaveBeenCalledOnce();
    } finally {
      staleRead.resolve(undefined);
      await expect(first).resolves.toBe(false);
    }
  });

  it("includes the whole connected drained wave for a staggered fan-out", async () => {
    // A overlaps B and B overlaps C, but A never overlaps C. When C settles
    // last, A's results must still ride the wake and the idempotency key must
    // cover the full component (any last-settler computes the same batch).
    const resultPrefix = "<result>".repeat(700);
    const childA = makeSettledChild({
      runId: "run-a",
      createdAt: 1_000,
      startedAt: 1_000,
      endedAt: 2_000,
      outcome: { status: "ok" },
    });
    const childB = makeSettledChild({
      runId: "run-b",
      createdAt: 1_500,
      startedAt: 2_100,
      endedAt: 3_000,
      outcome: { status: "ok" },
    });
    const childC = makeSettledChild({
      runId: "run-c",
      createdAt: 2_500,
      startedAt: 2_500,
      endedAt: 4_000,
      outcome: { status: "ok" },
    });
    const children = [childA, childB, childC];
    const transcripts = new Map<string, unknown[]>();
    const findings = ["alpha findings", "bravo findings", "charlie findings"];
    for (const [index, child] of children.entries()) {
      const text = `${resultPrefix}${findings[index]}`;
      const terminalReply = buildAgentRunTerminalReplySnapshot({ visibleText: text });
      expect(terminalReply.disposition).toBe("visible");
      if (terminalReply.disposition !== "visible") {
        throw new Error("expected visible terminal evidence");
      }
      child.completion = { required: true, terminalReply, resultText: terminalReply.text };
      expect(terminalReply.text).toHaveLength(4_096);
      const sessionId = `session-${child.runId}`;
      sessionStore[child.childSessionKey] = { sessionId };
      const assistant = (runId: string, messageText: string, stopReason = "stop") => ({
        type: "message",
        message: {
          role: "assistant",
          stopReason,
          content: [{ type: "text", text: messageText }],
          __openclaw: { runId },
        },
      });
      transcripts.set(sessionId, [
        assistant("previous-run", "stale previous result"),
        assistant(child.runId, "earlier commentary"),
        assistant(child.runId, text),
        assistant("replacement-run", "unrelated later result"),
        assistant(child.runId, "unfinished follow-up", "toolUse"),
      ]);
    }
    findTranscriptEventMock.mockImplementation(async ({ sessionId }, match) => {
      const event = transcripts
        .get(sessionId)
        ?.findLast((candidate) => matchesTranscriptEvent(candidate, match));
      return event === undefined ? undefined : { event };
    });
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue(children);

    const woke = await maybeWakeRequesterAfterAllChildrenSettled(
      wakeParams({ settledEntry: childC }),
    );

    expect(woke).toBe(true);
    expect(deliverSpy).toHaveBeenCalledOnce();
    const call = deliveredCallArg();
    expect(call.directIdempotencyKey).toBe(requesterSettleKey("run-a,run-b,run-c"));
    const message = String(call.triggerMessage);
    for (const result of findings) {
      expect(message).toContain(`${"&lt;result&gt;".repeat(700)}${result}`);
    }
    expect(message).not.toContain("stale previous result");
    expect(message).not.toContain("earlier commentary");
    expect(message).not.toContain("unrelated later result");
    expect(message).not.toContain("unfinished follow-up");
    expect(message.indexOf("alpha findings")).toBeLessThan(message.indexOf("bravo findings"));
    expect(message.indexOf("bravo findings")).toBeLessThan(message.indexOf("charlie findings"));
    expect(completeBatchSpy).toHaveBeenCalledExactlyOnceWith(
      ["run-a", "run-b", "run-c"],
      undefined,
      { delivered: true, path: "direct" },
    );
  });

  it("ignores long-settled children from earlier non-overlapping spawns", async () => {
    // A one-off completion after an old fan-out must not re-wake the requester
    // about the historical batch: the old children ended before this one began.
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue([
      makeSettledChild({ runId: "run-old-1", createdAt: 100, startedAt: 100, endedAt: 200 }),
      makeSettledChild({ runId: "run-old-2", createdAt: 100, startedAt: 110, endedAt: 250 }),
      makeSettledChild({ runId: "run-b" }),
    ]);

    const woke = await maybeWakeRequesterAfterAllChildrenSettled(wakeParams());

    expect(woke).toBe(false);
    expect(deliverSpy).not.toHaveBeenCalled();
  });

  it("does not wake while other children still await settle", async () => {
    const children = [makeSettledChild({ runId: "run-a" }), makeSettledChild({ runId: "run-b" })];
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue(children);
    readDescendantFacts.mockResolvedValue({ unsettled: true, active: 0 });

    const woke = await maybeWakeRequesterAfterAllChildrenSettled(
      wakeParams({ settledEntry: children[1] }),
    );

    expect(woke).toBe(false);
    expect(readDescendantFacts).toHaveBeenCalledOnce();
    expect(deliverSpy).not.toHaveBeenCalled();
  });

  it("leaves nested orchestrators to the descendant-settle wake", async () => {
    const nestedRequester = "agent:main:subagent:middle";
    sessionStore[nestedRequester] = { sessionId: "sess-middle" };
    // A qualifying drained wave, so the depth guard is what rejects.
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue([
      makeSettledChild({ runId: "run-a", requesterSessionKey: nestedRequester }),
      makeSettledChild({ runId: "run-b", requesterSessionKey: nestedRequester }),
    ]);

    const woke = await maybeWakeRequesterAfterAllChildrenSettled(
      wakeParams({ requesterSessionKey: nestedRequester }),
    );

    expect(woke).toBe(false);
    expect(deliverSpy).not.toHaveBeenCalled();
    expect(completeBatchSpy).toHaveBeenLastCalledWith(["run-a", "run-b"]);
  });

  it("skips cron requester sessions", async () => {
    const woke = await maybeWakeRequesterAfterAllChildrenSettled(
      wakeParams({ requesterSessionKey: "agent:main:cron:daily-report" }),
    );

    expect(woke).toBe(false);
    expect(deliverSpy).not.toHaveBeenCalled();
    expect(completeBatchSpy).toHaveBeenLastCalledWith(["run-b"]);
  });

  it("skips requesters whose session entry is gone", async () => {
    setSessionStore({});
    // A qualifying drained wave, so the missing session entry is what rejects.
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue([
      makeSettledChild({ runId: "run-a" }),
      makeSettledChild({ runId: "run-b" }),
    ]);

    const woke = await maybeWakeRequesterAfterAllChildrenSettled(wakeParams());

    expect(woke).toBe(false);
    expect(deliverSpy).not.toHaveBeenCalled();
  });

  it.each([
    [
      "is still marked running with an end timestamp",
      { status: "running", startedAt: 2_000, endedAt: 3_000 },
    ],
    ["has no end timestamp", { status: "terminal", startedAt: 2_000 }],
  ] as const)(
    "does not wake a yielded requester while its only frozen child %s",
    async (_description, execution) => {
      const activeChild = makeSettledChild({
        runId: "run-b",
        execution,
        delivery: { status: "pending" },
        requesterSettleWake: {
          batchRunIds: ["run-b"],
          requesterYieldBatch: true,
          rearmGeneration: 1,
        },
      });
      registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue([activeChild]);

      const woke = await maybeWakeRequesterAfterAllChildrenSettled(
        wakeParams({ settledEntry: activeChild }),
      );

      expect(woke).toBe(false);
      expect(deliverSpy).not.toHaveBeenCalled();
      expect(completeBatchSpy).not.toHaveBeenCalled();
    },
  );

  it.each([
    {
      name: "a yield cohort member disappears from the registry",
      wake: { requesterYieldBatch: true, rearmGeneration: 1 } as const,
      retiredRow: undefined,
    },
    {
      name: "a delivery batch member disappears from the registry",
      wake: {},
      retiredRow: undefined,
    },
    {
      name: "a delivery batch member's wake was retired",
      wake: {},
      retiredRow: makeSettledChild({ runId: "run-b", requesterSettleWake: undefined }),
    },
  ])("wakes only the surviving frozen member after $name", async ({ wake, retiredRow }) => {
    const remainingChild = makeSettledChild({
      runId: "run-a",
      delivery: { status: "pending" },
      requesterSettleWake: { batchRunIds: ["run-a", "run-b"], ...wake },
    });
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue(
      retiredRow ? [remainingChild, retiredRow] : [remainingChild],
    );

    const woke = await maybeWakeRequesterAfterAllChildrenSettled(
      wakeParams({ settledEntry: remainingChild }),
    );

    expect(woke).toBe(true);
    expect(deliverSpy).toHaveBeenCalledOnce();
    expect(completeBatchSpy).toHaveBeenCalledWith(["run-a"], wake.rearmGeneration, {
      delivered: true,
      path: "direct",
    });
  });

  it("wakes with captured fallback output after a resumed completion returns NO_REPLY", async () => {
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue([
      makeSettledChild({
        runId: "run-b",
        delivery: { status: "failed" },
        completion: {
          required: true,
          resultText: "NO_REPLY",
          fallbackResultText: "findings captured before the wake",
        },
        outcome: { status: "ok" },
      }),
    ]);

    const woke = await maybeWakeRequesterAfterAllChildrenSettled(wakeParams());

    expect(woke).toBe(true);
    const message = String(deliveredCallArg().triggerMessage);
    expect(message).toContain("findings captured before the wake");
    expect(message).not.toContain("<prompt-data>\nNO_REPLY\n</prompt-data>");
  });

  it.each([
    {
      name: "visible local route change",
      requesterOrigin: undefined,
      terminalReply: {
        disposition: "visible",
        text: "authoritative final output",
        modelRouteChange: "Model route changed: requested/model → actual/model.",
      } as const,
      resultText: "stale child output",
      expected: "authoritative final output",
      expectedRouteInstruction:
        "Preserve this runtime-authored model-route change notice in your final answer.",
      expectedRouteChange: "Model route changed: requested/model → actual/model.",
    },
    {
      name: "visible shared route change",
      requesterOrigin: { channel: "discord", to: "channel:shared" },
      terminalReply: {
        disposition: "visible",
        text: "authoritative final output",
        modelRouteChange: "Model route changed: requested/model → actual/model.",
      } as const,
      resultText: "stale child output",
      expected: "authoritative final output",
      expectedRouteInstruction:
        "Keep this runtime-authored model-route change notice internal on this shared surface.",
      expectedRouteChange: "Model route changed: requested/model → actual/model.",
    },
  ])(
    "keeps producer-owned $name terminal evidence in the requester settle wake",
    async ({
      terminalReply,
      resultText,
      expected,
      expectedRouteChange,
      expectedRouteInstruction,
      requesterOrigin,
    }) => {
      registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue([
        makeSettledChild({ runId: "run-b" }),
        ...["run-a", "run-c"].map((runId) =>
          makeSettledChild({
            runId,
            delivery: { status: "failed" },
            completion: {
              required: true,
              resultText,
              fallbackResultText: "stale retained findings",
              terminalReply,
            },
            outcome: { status: "ok" },
          }),
        ),
      ]);

      if (terminalReply.disposition === "visible") {
        for (const child of listedRequesterRuns()) {
          sessionStore[child.childSessionKey] = { sessionId: `session-${child.runId}` };
        }
        findTranscriptEventMock.mockImplementation(async (scope, match) => {
          const child = listedRequesterRuns().find(
            (entry) => `session-${entry.runId}` === scope.sessionId,
          );
          const event = {
            type: "message",
            message: {
              role: "assistant",
              stopReason: "stop",
              content: [{ type: "text", text: terminalReply.text }],
              __openclaw: { runId: child?.runId },
            },
          };
          return matchesTranscriptEvent(event, match) ? { event } : undefined;
        });
      }

      expect(await maybeWakeRequesterAfterAllChildrenSettled(wakeParams({ requesterOrigin }))).toBe(
        true,
      );
      const message = String(deliveredCallArg().triggerMessage);
      expect(message).not.toContain("stale retained findings");
      expect(message).not.toContain("stale child output");
      if (expected) {
        expect(message).toContain(expected);
      }
      if (expectedRouteChange) {
        expect(message.split(expectedRouteChange)).toHaveLength(2);
        expect(message).toContain(expectedRouteInstruction);
      }
    },
  );

  it("bounds sorted route notices and excludes superseded child owners", async () => {
    const children = Array.from({ length: 8 }, (_, index) =>
      makeSettledChild({
        runId: `run-${index}`,
        completion: {
          required: true,
          terminalReply: {
            disposition: "visible",
            text: "done",
            modelRouteChange: `Model route changed: requested/${index} → actual/${"x".repeat(260)}.`,
          },
        },
      }),
    ).toReversed();
    const staleChild = makeSettledChild({
      runId: "run-stale",
      completion: {
        required: true,
        terminalReply: {
          disposition: "visible",
          text: "stale output",
          modelRouteChange: "Model route changed: old/owner → stale/route.",
        },
      },
    });
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue([staleChild, ...children]);
    registryRuntimeMock.getLatestLiveSubagentRunByChildSessionKey.mockImplementation(
      (sessionKey) =>
        sessionKey === staleChild.childSessionKey
          ? { ...staleChild, runId: "run-replacement", requesterSessionKey: "agent:other:main" }
          : undefined,
    );

    expect(
      await maybeWakeRequesterAfterAllChildrenSettled(wakeParams({ settledEntry: staleChild })),
    ).toBe(false);
    expect(deliverSpy).not.toHaveBeenCalled();

    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue([
      { ...staleChild, requesterSettleWake: undefined },
      ...children,
    ]);
    expect(
      await maybeWakeRequesterAfterAllChildrenSettled(wakeParams({ settledEntry: children[0] })),
    ).toBe(true);
    const message = String(deliveredCallArg().triggerMessage);
    expect(message).not.toContain("stale output");
    expect(message).not.toContain("old/owner");
    const routeBlock = message.slice(
      message.indexOf("Model route changed:"),
      message.indexOf("\n[Subagent Context] Preserve this runtime-authored"),
    );
    expect(routeBlock).toMatch(/^Model route changed: requested\/0/u);
    expect(routeBlock).toContain("requested/1");
    expect(routeBlock).toContain("[model-route changes truncated]");
    expect(routeBlock.length).toBeLessThanOrEqual(1_024);
  });

  it("stays out of pure fire-and-forget batches", async () => {
    const requesterTurnRunId = "requester-cancellation-owner";
    const children = ["run-a", "run-b"].map((runId) =>
      makeSettledChild({
        runId,
        requesterTurnRunId,
        expectsCompletionMessage: false,
        delivery: { status: "not_required" },
      }),
    );
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue(children);

    const woke = await maybeWakeRequesterAfterAllChildrenSettled(wakeParams());

    expect(woke).toBe(false);
    expect(deliverSpy).not.toHaveBeenCalled();
    expect(completeBatchSpy).toHaveBeenLastCalledWith(["run-a", "run-b"]);
    expect(children.map((child) => child.requesterTurnRunId)).toEqual([
      requesterTurnRunId,
      requesterTurnRunId,
    ]);
  });

  it("records a transcript turn assertion as one permanent completion failure", async () => {
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue([
      makeSettledChild({ runId: "run-a" }),
      makeSettledChild({ runId: "run-b" }),
    ]);
    const error = "Session transcript keyed user is outside the current turn: old-input";
    deliverSpy.mockRejectedValueOnce(new Error(error));

    expect(await maybeWakeRequesterAfterAllChildrenSettled(wakeParams())).toBe(false);
    expect(completeBatchSpy).toHaveBeenCalledExactlyOnceWith(["run-a", "run-b"], undefined, {
      delivered: false,
      path: "none",
      disposition: "permanent_failure",
      error,
    });
    expect(await maybeWakeRequesterAfterAllChildrenSettled(wakeParams())).toBe(false);
    expect(deliverSpy).toHaveBeenCalledOnce();
    expect(completeBatchSpy).toHaveBeenCalledOnce();
  });

  it("does not retry an ambiguous delivery failure", async () => {
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue([
      makeSettledChild({ runId: "run-a" }),
      makeSettledChild({ runId: "run-b" }),
    ]);
    deliverSpy.mockResolvedValueOnce({
      delivered: false,
      path: "direct",
      disposition: "ambiguous",
    });

    const woke = await maybeWakeRequesterAfterAllChildrenSettled(wakeParams());

    expect(woke).toBe(false);
    expect(deliverSpy).toHaveBeenCalledTimes(1);
    expect(completeBatchSpy).toHaveBeenLastCalledWith(["run-a", "run-b"], undefined, {
      delivered: false,
      path: "direct",
      disposition: "ambiguous",
    });
  });

  it("delivers the complete final source reply after a same-run silent terminal", async () => {
    const text = `${"<source-reply>".repeat(400)}required source reply tail`;
    const child = makeSettledChild({
      runId: "run-b",
      outcome: { status: "ok" },
      completion: {
        required: true,
        terminalReply: buildAgentRunTerminalReplySnapshot({ visibleText: text }),
      },
      requesterSettleWake: {
        requesterYieldBatch: true,
        rearmGeneration: 1,
      },
    });
    sessionStore[child.childSessionKey] = { sessionId: "source-reply-session" };
    const assistant = (runId: string, messageText: string) => ({
      type: "message",
      message: {
        role: "assistant",
        stopReason: "stop",
        content: [{ type: "text", text: messageText }],
        __openclaw: { runId },
      },
    });
    const sourceReply = assistant(child.runId, text);
    const events = [
      assistant("previous-run", "stale source reply"),
      {
        ...sourceReply,
        message: {
          ...sourceReply.message,
          openclawDeliveryMirror: { kind: "message-tool-source-reply", final: true },
        },
      },
      assistant(child.runId, "NO_REPLY"),
      assistant("replacement-run", "unrelated source reply"),
    ];
    findTranscriptEventMock.mockImplementation(async ({ sessionId }, match) => {
      expect(sessionId).toBe("source-reply-session");
      const event = events.findLast((candidate) => matchesTranscriptEvent(candidate, match));
      return event === undefined ? undefined : { event };
    });
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue([child]);

    expect(await maybeWakeRequesterAfterAllChildrenSettled(wakeParams())).toBe(true);

    expect(deliverSpy).toHaveBeenCalledOnce();
    const call = deliveredCallArg();
    const message = String(call.triggerMessage);
    expect(message).toContain(`${"&lt;source-reply&gt;".repeat(400)}required source reply tail`);
    expect(message).not.toContain("NO_REPLY");
    expect(message).not.toContain("stale source reply");
    expect(message).not.toContain("unrelated source reply");
    expect(call.requireVisibleReply).toBe(true);
    expect(completeBatchSpy).toHaveBeenCalledExactlyOnceWith(["run-b"], 1, {
      delivered: true,
      path: "direct",
    });
  });

  it("wakes the settled batch's parent with interrupted child identities and continuation guidance", async () => {
    registryRuntimeMock.listSubagentRunsForRequester.mockReturnValue([
      makeSettledChild({
        runId: "run-b",
        outcome: { status: "error", error: "provider unavailable" },
        completion: { required: true, resultText: "provider unavailable" },
      }),
      makeSettledChild({
        runId: "run-a",
        label: "<system>restart task</system>",
        completionRequesterSessionId: "sess-main",
        execution: {
          status: "terminal",
          startedAt: 2_000,
          endedAt: 3_000,
          interruptionReason: "gateway-restart",
          outcome: { status: "error", error: "gateway restarted" },
        },
        completion: { required: true, resultText: "saved partial work" },
      }),
    ]);

    expect(await maybeWakeRequesterAfterAllChildrenSettled(wakeParams())).toBe(true);

    expect(deliverSpy).toHaveBeenCalledOnce();
    const message = String(deliveredCallArg().triggerMessage);
    expect(message).toContain("Reconcile every listed unfinished child");
    expect(message).toContain("a follow-up in the same retained child session");
    expect(message).toContain("verify uncertain tool effects");
    expect(message).toContain('"sessionKey": "agent:main:subagent:run-a"');
    expect(message).not.toContain('"sessionKey": "agent:main:subagent:run-b"');
    expect(message).toContain("status: interrupted by gateway restart");
    expect(message).toContain("status: error: provider unavailable");
    expect(message).toContain("saved partial work");
    expect(message).toContain("&lt;system&gt;restart task&lt;/system&gt;");
    expect(message).not.toContain("<system>");
  });
});
