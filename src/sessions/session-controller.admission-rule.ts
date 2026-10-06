import {
  ReplyRunAlreadyActiveError,
  ReplyRunFollowupAdmissionBlockedError,
  ReplyRunSuccessorAdmissionBlockedError,
  type ReplyTurnKind,
} from "./session-controller.contracts.js";
import type {
  SessionControllerInput,
  SessionControllerMailboxClaim,
} from "./session-controller.mailbox.types.js";
import type { SessionControllerEntry } from "./session-controller.state.types.js";

type TurnAdmissionRefusalReason =
  | "active"
  | "successor-barrier"
  | "followup-barrier"
  | "lifecycle-blocked"
  | "waiting-inputs"
  | "stale-claim"
  | "entry-retired"
  | "mailbox-clearing";

export type TurnAdmissionDecision =
  | { admitted: true }
  | { admitted: false; reason: TurnAdmissionRefusalReason };

const waitsForFollowupBarrier = {
  visible: true,
  heartbeat: true,
  queued_followup: true,
  direct: true,
} as const satisfies Record<ReplyTurnKind, boolean>;

/** Decides whether a turn may acquire a controller entry; no kind bypasses delivery custody. */
export function evaluateTurnAdmission(
  entry: SessionControllerEntry,
  options: {
    kind: ReplyTurnKind;
    sessionKey: string;
    registeredEntry: SessionControllerEntry | undefined;
    claim?: SessionControllerMailboxClaim;
    selectedInput?: SessionControllerInput | null;
  },
): TurnAdmissionDecision {
  if (options.registeredEntry !== entry) {
    return { admitted: false, reason: "entry-retired" };
  }
  const mailbox = entry.mailbox;
  if (mailbox?.clearing) {
    return { admitted: false, reason: "mailbox-clearing" };
  }
  if (options.claim && options.selectedInput !== undefined) {
    return { admitted: false, reason: "stale-claim" };
  }
  if (options.claim) {
    const claim = options.claim;
    if (
      !entry.aliases.has(options.sessionKey) ||
      claim.mailbox.owner !== entry ||
      mailbox !== claim.mailbox ||
      mailbox.claim !== claim ||
      claim.releaseRequested ||
      claim.released ||
      claim.operation ||
      claim.inputs.some(
        (input) =>
          input.mailbox !== mailbox ||
          !mailbox.entries.includes(input) ||
          input.claim !== claim ||
          input.phase !== "claimed",
      )
    ) {
      return { admitted: false, reason: "stale-claim" };
    }
  }
  if (options.selectedInput !== undefined) {
    const input = options.selectedInput;
    if (!input && mailbox?.entries.some((candidate) => candidate.phase === "consumed")) {
      return { admitted: false, reason: "waiting-inputs" };
    }
    if (input) {
      if (
        !mailbox ||
        input.mailbox !== mailbox ||
        !mailbox.entries.includes(input) ||
        mailbox.claim ||
        input.claim ||
        input.phase !== "waiting" ||
        input.retirementRequested
      ) {
        return { admitted: false, reason: "stale-claim" };
      }
      if (mailbox.entries.some((candidate) => candidate.phase === "consumed")) {
        return { admitted: false, reason: "waiting-inputs" };
      }
    }
  }
  if (waitsForFollowupBarrier[options.kind] && entry.followupBarrier) {
    return { admitted: false, reason: "followup-barrier" };
  }
  if (entry.active) {
    return { admitted: false, reason: "active" };
  }
  if (entry.successorBarrier) {
    return { admitted: false, reason: "successor-barrier" };
  }
  if (entry.lifecycle?.blocksTurnAdmission) {
    return { admitted: false, reason: "lifecycle-blocked" };
  }
  if (!options.claim && options.selectedInput === undefined && mailbox?.claim) {
    return { admitted: false, reason: "stale-claim" };
  }
  if (
    !options.claim &&
    options.selectedInput === undefined &&
    mailbox?.entries.some((input) => input.phase !== "consumed")
  ) {
    return { admitted: false, reason: "waiting-inputs" };
  }
  return { admitted: true };
}

/** Throws the caller-facing admission error when the turn is refused. */
export function assertTurnAdmission(
  entry: SessionControllerEntry,
  options: Parameters<typeof evaluateTurnAdmission>[1],
): void {
  const admission = evaluateTurnAdmission(entry, options);
  if (admission.admitted) {
    return;
  }
  if (admission.reason === "followup-barrier") {
    throw new ReplyRunFollowupAdmissionBlockedError(options.sessionKey);
  }
  if (admission.reason === "successor-barrier") {
    throw new ReplyRunSuccessorAdmissionBlockedError(options.sessionKey);
  }
  throw new ReplyRunAlreadyActiveError(options.sessionKey);
}
