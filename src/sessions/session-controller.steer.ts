/** One controller-owned steer of a reserved input into its captured execution owner. */
import type {
  ReplyBackendQueueMessageResult,
  ReplyMessageInjectionAttempt,
  ReplyMessageInjectionOptions,
  ReplyMessageInjectionOutcome,
  ReplyMessageInjectionTarget,
} from "./session-controller.contracts.js";
import { beginSessionControllerSourceInjection } from "./session-controller.mailbox-source.js";
import type { SessionControllerInput } from "./session-controller.mailbox.types.js";
import {
  beginReplyMessageInjectionTarget,
  captureReplyMessageInjectionTarget,
  finalizeReplyMessageInjectionAttempt,
} from "./session-controller.message-injection.js";

type SessionControllerSteerParams = {
  input: SessionControllerInput;
  target: ReplyMessageInjectionTarget;
  text: string;
  options: ReplyMessageInjectionOptions;
};

export type SessionControllerSteerResult =
  | { status: "accepted"; targetRunId?: string; result?: ReplyBackendQueueMessageResult }
  /** Consumed without a confirmed commit; never replayable. */
  | { status: "indeterminate"; targetRunId?: string; errorMessage: string }
  /** The input stays with the mailbox, or was retired by its own cancellation. */
  | {
      status: "rejected";
      reason:
        | Extract<ReplyMessageInjectionOutcome, { status: "rejected" }>["reason"]
        | "admission_declined";
      errorMessage?: string;
    };

/** Admits the input behind older reservations and injects it; custody settles on the native outcome. */
export async function beginSessionControllerSteer(
  params: SessionControllerSteerParams,
): Promise<ReplyMessageInjectionAttempt | undefined> {
  const injection = beginSessionControllerSourceInjection(params.input);
  if (!(await injection.admit())) {
    return undefined;
  }
  let attempt: ReplyMessageInjectionAttempt;
  try {
    params.options.assertCurrent?.();
    params.options.abortSignal?.throwIfAborted();
    attempt = beginReplyMessageInjectionTarget(params.target, params.text, params.options);
  } catch (error) {
    injection.finish(false);
    throw error;
  }
  // Failed acknowledgement does not settle accepted native work.
  const acceptance = attempt.acceptance.then((accepted) => {
    injection.accepted(accepted);
    return accepted;
  });
  void acceptance.catch(() => {});
  const outcome = attempt.outcome.then(
    async (nativeOutcome) => {
      // Native ownership and an indeterminate commit are never replayable.
      let accepted: boolean;
      try {
        accepted = await acceptance;
      } catch (error) {
        // The native outcome has now settled; an unknown ACK never permits replay.
        injection.finish(true);
        throw error;
      }
      injection.finish(
        accepted || nativeOutcome.status === "accepted" || nativeOutcome.status === "indeterminate",
      );
      return nativeOutcome;
    },
    (error: unknown) => {
      // An exceptional result after handoff cannot prove that input was rejected.
      injection.finish(true);
      throw error;
    },
  );
  void outcome.catch(() => {});
  return { ...attempt, acceptance, outcome };
}

/** Steers once into the exact target (default: the input owner's current turn); authority failures throw. */
export async function submitSessionControllerSteer(
  params: Omit<SessionControllerSteerParams, "target"> & {
    target?: ReplyMessageInjectionTarget;
    /** Status-only and completion inputs cannot abort the target when its commit is unconfirmed. */
    abortOnUnconfirmedTranscript?: false;
  },
): Promise<SessionControllerSteerResult> {
  const active = params.input.mailbox.owner.active;
  const target = params.target ?? captureReplyMessageInjectionTarget(active);
  if (!target) {
    return { status: "rejected", reason: active ? "injection_unavailable" : "no_active_run" };
  }
  const attempt = await beginSessionControllerSteer({ ...params, target });
  if (!attempt) {
    return { status: "rejected", reason: "admission_declined" };
  }
  const finalization = await finalizeReplyMessageInjectionAttempt({
    attempt,
    target,
    inboundAudio: params.options.inboundAudio,
    abortOnUnconfirmedTranscript: params.abortOnUnconfirmedTranscript,
  });
  const { targetRunId } = finalization;
  if (finalization.status === "rejected") {
    const { reason, errorMessage } = finalization.outcome;
    return { status: "rejected", reason, ...(errorMessage ? { errorMessage } : {}) };
  }
  if (finalization.status === "indeterminate") {
    return {
      status: "indeterminate",
      targetRunId,
      errorMessage: finalization.outcome.errorMessage,
    };
  }
  return { status: "accepted", targetRunId, result: finalization.outcome.result };
}
