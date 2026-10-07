import { expect, it, vi, type Mock } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { createAdmittedRunOperatorAuthority } from "../../agents/admitted-run-context.js";
import {
  claimEmbeddedPendingUserInputAnswer,
  steerActiveSessionWithOptionalDeliveryWait,
} from "../../agents/embedded-agent-runner/run/attempt-queue-message.js";
import type { AgentQuestionDispatcher } from "../../agents/harness/gateway-question-dispatch.js";
import { registerPendingAgentQuestion } from "../../agents/harness/gateway-question.js";
import {
  createAgentQuestionAnswerAuthority,
  withAgentQuestionAnswerAuthority,
} from "../../agents/harness/host-private-capabilities.js";
import type { SessionEntry } from "../../config/sessions.js";
import { loadTranscriptEvents } from "../../config/sessions/session-accessor.js";
import { getSessionControllerOperation } from "../../sessions/session-controller.js";
import { createUserTurnTranscriptRecorder } from "../../sessions/user-turn-transcript.js";
import type { ReplyPayload } from "../types.js";
import { enqueueFollowupRun, type FollowupRun } from "./queue.js";

type QuestionRefusalFixture = {
  createMinimalRun: (params: {
    isActive?: boolean;
    shouldSteer?: boolean;
    shouldFollowup?: boolean;
    resolvedQueueMode?: string;
    sessionEntry?: SessionEntry;
    sessionStore?: Record<string, SessionEntry>;
    storePath?: string;
    sessionCtx?: { MessageSid?: string };
    bindActiveAuthority?: boolean;
    attachSteerBackend?: boolean;
  }) => {
    followupRun: FollowupRun;
    run: () => Promise<ReplyPayload | ReplyPayload[] | undefined>;
  };
  makeSessionFixture: () => Promise<{
    sessionEntry: SessionEntry;
    sessionStore: Record<string, SessionEntry>;
    storePath: string;
  }>;
  requireScheduledFollowupRunner: () => (source: FollowupRun) => Promise<void>;
  state: { activeBackendCancelMock: Mock; runEmbeddedAgentMock: Mock };
};

type SessionFixture = Awaited<ReturnType<QuestionRefusalFixture["makeSessionFixture"]>>;

/** Registers runReplyAgent cases for input a pending question refuses to take as its answer. */
export function registerQuestionRefusalCases({
  createMinimalRun,
  makeSessionFixture,
  requireScheduledFollowupRunner,
  state,
}: QuestionRefusalFixture): void {
  // The asking turn is a real reply whose runtime registers the question and
  // its creator policy, then waits until the test body releases it.
  async function withAskingTurn(
    params: {
      project: () => string;
      session?: SessionFixture;
    },
    body: (turn: {
      settle: () => Promise<unknown>;
      resolveQuestion: Mock;
      nativeSteer: Mock;
    }) => Promise<void>,
  ) {
    const asked = createDeferred();
    const release = createDeferred();
    const resolveQuestion = vi.fn(async () => ({ status: "answered" }));
    const nativeSteer = vi.fn(async () => {});
    const gatewayCall: AgentQuestionDispatcher = { version: 2, call: resolveQuestion };
    state.runEmbeddedAgentMock.mockImplementationOnce(async () => {
      const operation = getSessionControllerOperation("main")!;
      operation.attachBackend({
        kind: "embedded",
        cancel: state.activeBackendCancelMock,
        messageInjectionV2: {
          version: 2,
          isAvailable: () => true,
          // Runtime steering and its answer claim check source authority only.
          queueMessage: (message, options, assertCurrent, kind) =>
            steerActiveSessionWithOptionalDeliveryWait(
              { steer: nativeSteer, subscribe: () => () => {} },
              message,
              options,
              "main",
              undefined,
              { kind, assertCurrent },
            ),
          claimPendingUserInputAnswer: (message, options, assertCurrent, kind) =>
            claimEmbeddedPendingUserInputAnswer(message, options, "main", undefined, {
              kind,
              assertCurrent,
            }),
        },
      });
      const authority = createAgentQuestionAnswerAuthority({
        sessionKey: "main",
        fingerprint: "creator",
        project: params.project,
        assertActive: () => {},
      });
      const question = withAgentQuestionAnswerAuthority(authority, () =>
        registerPendingAgentQuestion({
          questionId: "ask_refusal",
          sessionKey: "main",
          questions: [{ id: "answer", header: "Answer", question: "Continue?" }],
          gatewayCall,
          answer: Promise.resolve({ status: "pending" }),
        }),
      );
      question.attachRegistration(Promise.resolve());
      asked.resolve();
      await release.promise;
      question.dispose();
      return { payloads: [{ text: "asking turn done" }], meta: {} };
    });
    const asking = createMinimalRun({
      ...params.session,
      sessionCtx: { MessageSid: "asking" },
      attachSteerBackend: false,
    }).run();
    const settle = () => {
      release.resolve();
      return asking;
    };
    try {
      await asked.promise;
      await body({ settle, resolveQuestion, nativeSteer });
    } finally {
      await settle().catch(() => undefined);
    }
  }

  // Steer mode is the case where the incoming message would otherwise reach
  // the asking turn and its runtime answer claim.
  function createIncomingRun(session?: SessionFixture) {
    return createMinimalRun({
      ...session,
      isActive: true,
      shouldSteer: true,
      shouldFollowup: true,
      resolvedQueueMode: "steer",
      sessionCtx: { MessageSid: "incoming" },
      bindActiveAuthority: false,
      attachSteerBackend: false,
    });
  }

  it("queues steer input the pending question refuses until the asking turn settles", async () => {
    await withAskingTurn({ project: () => "changed-policy" }, async (turn) => {
      const incoming = createIncomingRun();

      const notice = await incoming.run();

      // Neither the question nor the asking turn saw the refused input.
      expect(turn.resolveQuestion).not.toHaveBeenCalled();
      expect(turn.nativeSteer).not.toHaveBeenCalled();
      expect(state.activeBackendCancelMock).not.toHaveBeenCalled();
      expect(notice).toMatchObject({ text: expect.stringContaining("It was queued") });
      expect(notice).not.toHaveProperty("isError");
      expect(vi.mocked(enqueueFollowupRun)).toHaveBeenCalledOnce();
      expect(state.runEmbeddedAgentMock).toHaveBeenCalledOnce();

      await turn.settle();
      await requireScheduledFollowupRunner()(incoming.followupRun);
      expect(state.runEmbeddedAgentMock).toHaveBeenCalledTimes(2);
    });
  });

  it("refuses without queueing when the incoming source loses authority", async () => {
    let revoked = false;
    const project = () => {
      revoked = true;
      return "creator";
    };
    await withAskingTurn({ project }, async (turn) => {
      const incoming = createIncomingRun();
      incoming.followupRun.operatorAuthority = createAdmittedRunOperatorAuthority({
        profileId: "guest",
        scopes: ["operator.write"],
        source: {},
        assertCurrent: () => {
          if (revoked) {
            throw new Error("operator authority revoked");
          }
        },
      });

      await expect(incoming.run()).resolves.toMatchObject({
        text: expect.stringContaining("The answer was not sent: operator authority revoked"),
        isError: true,
      });
      expect(vi.mocked(enqueueFollowupRun)).not.toHaveBeenCalled();
      expect(turn.resolveQuestion).not.toHaveBeenCalled();
      await turn.settle();
      expect(state.runEmbeddedAgentMock).toHaveBeenCalledOnce();
    });
  });

  it("queues input refused after its transcript row without writing it twice", async () => {
    const session = await makeSessionFixture();
    const incoming = createIncomingRun(session);
    const recorder = createUserTurnTranscriptRecorder({
      input: { text: "refused after persistence", idempotencyKey: "incoming" },
      target: {
        agentId: "main",
        cwd: "/tmp",
        sessionId: "session",
        sessionKey: "main",
        ...session,
      },
    });
    incoming.followupRun.userTurnTranscriptRecorder = recorder;
    // The creator policy changes while the answer's source row is committed.
    const project = () => (recorder.hasPersisted() ? "changed-policy" : "creator");
    await withAskingTurn({ session, project }, async (turn) => {
      await expect(incoming.run()).resolves.toMatchObject({
        text: expect.stringContaining("It was queued"),
      });
      expect(recorder.hasPersisted()).toBe(true);
      expect(turn.resolveQuestion).not.toHaveBeenCalled();
      await turn.settle();
    });
    await requireScheduledFollowupRunner()(incoming.followupRun);
    expect(state.runEmbeddedAgentMock).toHaveBeenCalledTimes(2);
    expect(state.runEmbeddedAgentMock.mock.calls[1]?.[0]).toMatchObject({
      userTurnTranscriptRecorder: recorder,
    });
    const transcript = await loadTranscriptEvents({
      agentId: "main",
      sessionId: "session",
      sessionKey: "main",
      storePath: session.storePath,
    });
    expect(
      transcript.filter(
        (entry) =>
          (entry as { message?: { role?: string; content?: unknown } }).message?.content ===
          "refused after persistence",
      ),
    ).toHaveLength(1);
  });
}
