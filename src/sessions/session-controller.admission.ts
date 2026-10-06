import type { ReplyOperation } from "./session-controller.contracts.js";
import {
  getCurrentSessionControllerOwner,
  getCurrentSessionControllerClaim,
  withSessionControllerOwner,
  withSessionControllerClaim,
  bindSessionControllerTarget,
  captureSessionTarget,
  type SessionTarget,
} from "./session-controller.lifecycle.js";
import {
  submitSessionControllerTask,
  claimSessionControllerTask,
  releaseSessionControllerClaim,
  type SessionControllerMailboxClaim,
  type SessionControllerInput,
} from "./session-controller.mailbox.js";
import { createReplyOperation } from "./session-controller.operation.js";
import {
  assertSessionControllerOperation,
  getSessionControllerEntryForOperation,
  bindSessionControllerEntryTarget,
} from "./session-controller.state.js";

export type SessionTurnAdmission = {
  sessionKey?: string;
  /** Absent until guarded row creation. The exact mailbox claim owns preparation. */
  sessionId?: string;
  agentId?: string;
  storePath?: string;
  sessionTarget?: { storePath?: string };
  target?: SessionTarget;
  detached?: boolean;
  replyOperation?: ReplyOperation;
  controllerInput?: SessionControllerInput;
  abortSignal?: AbortSignal;
};

/** One selector before preparation. Nested native work borrows exact async claim custody,
 * never ID equality; only the outer producer completes the adopted operation. */
export async function withSessionTurn<T>(
  params: SessionTurnAdmission,
  run: (operation: ReplyOperation | undefined, signal: AbortSignal) => Promise<T>,
): Promise<T> {
  const sessionKey = params.sessionKey?.trim();
  const storeScope = params.sessionTarget?.storePath ?? params.storePath;
  const target =
    params.target ??
    (storeScope && sessionKey
      ? captureSessionTarget({
          storeScope,
          sessionKey,
          agentId: params.agentId,
          incarnation: params.sessionId,
        })
      : undefined);
  const ambient = getCurrentSessionControllerOwner();
  const reservation = getCurrentSessionControllerClaim();
  const ownsInput = (
    claim: SessionControllerMailboxClaim | undefined,
    operation?: ReplyOperation,
  ) =>
    !params.controllerInput ||
    (claim?.inputs.includes(params.controllerInput) === true &&
      (!operation || claim.operation === operation));
  const ambientEntry = ambient && getSessionControllerEntryForOperation(ambient);
  const matches = (owner: { aliases: Set<string>; target?: SessionTarget }) =>
    sessionKey !== undefined &&
    sessionKey.length > 0 &&
    owner.aliases.has(sessionKey) &&
    (!target || !owner.target || owner.target.storeScope === target.storeScope);
  const inherited =
    params.replyOperation ??
    (!params.detached &&
    ambient &&
    ambientEntry &&
    matches(ambientEntry) &&
    ownsInput(ambientEntry.mailbox?.claim, ambient) &&
    (!params.sessionId || ambient.hasOwnedSessionId(params.sessionId))
      ? ambient
      : undefined);
  if (inherited) {
    assertSessionControllerOperation(inherited);
    const owner = getSessionControllerEntryForOperation(inherited);
    if (!ownsInput(owner.mailbox?.claim, inherited)) {
      throw new Error("Backend turn does not own the supplied source input");
    }
    if (!matches(owner) || (params.sessionId && !inherited.hasOwnedSessionId(params.sessionId))) {
      throw new Error("Backend turn does not match its session controller admission");
    }
    if (target) {
      bindSessionControllerTarget(inherited, target);
    }
    params.abortSignal?.throwIfAborted();
    const signal = params.abortSignal
      ? AbortSignal.any([inherited.abortSignal, params.abortSignal])
      : inherited.abortSignal;
    return await withSessionControllerOwner(inherited, () => run(inherited, signal));
  }
  if (params.detached || !sessionKey) {
    const signal = params.abortSignal ?? new AbortController().signal;
    signal.throwIfAborted();
    return await run(undefined, signal);
  }
  const materialize = (claim: SessionControllerMailboxClaim): ReplyOperation | undefined => {
    if (claim.released || claim.releaseRequested || claim.mailbox.claim !== claim) {
      throw new Error("Mailbox claim no longer admits work");
    }
    claim.abortController.signal.throwIfAborted();
    params.abortSignal?.throwIfAborted();
    if (target) {
      bindSessionControllerEntryTarget(claim.mailbox.owner, target);
    }
    if (claim.operation) {
      assertSessionControllerOperation(claim.operation);
      if (params.sessionId && !claim.operation.hasOwnedSessionId(params.sessionId)) {
        throw new Error("Nested native turn changed incarnation without owner adoption");
      }
      return claim.operation;
    }
    if (!params.sessionId?.trim()) {
      return undefined;
    }
    return createReplyOperation({
      sessionKey,
      sessionId: params.sessionId,
      agentId: params.agentId,
      resetTriggered: false,
      turnKind: "direct",
      upstreamAbortSignal: claim.abortController.signal,
      mailboxClaim: claim,
      target,
    });
  };
  const invoke = (claim: SessionControllerMailboxClaim) => {
    const operation = materialize(claim);
    return withSessionControllerClaim(claim, () =>
      operation
        ? withSessionControllerOwner(operation, () => run(operation, operation.abortSignal))
        : run(undefined, claim.abortController.signal),
    );
  };
  if (
    reservation &&
    !params.detached &&
    matches(reservation.mailbox.owner) &&
    ownsInput(reservation)
  ) {
    return await invoke(reservation);
  }
  const input = params.controllerInput;
  if (input && !matches(input.mailbox.owner)) {
    throw new Error("Reserved source belongs to a different physical session");
  }
  if (input?.claim && !input.claim.released) {
    // Only the exact in-band owner above may borrow an executing source.
    throw new Error("Source is already executing under another turn admission");
  }
  const claim = input
    ? await claimSessionControllerTask(input, materialize)
    : await submitSessionControllerTask(sessionKey, {
        target,
        signal: params.abortSignal,
        start: materialize,
      });
  const abort = () => claim.abortController.abort(params.abortSignal?.reason);
  params.abortSignal?.addEventListener("abort", abort, { once: true });
  if (params.abortSignal?.aborted) {
    abort();
  }
  try {
    return await invoke(claim);
  } catch (error) {
    if (claim.operation && !claim.operation.result) {
      claim.operation.fail("run_failed", error);
    }
    throw error;
  } finally {
    params.abortSignal?.removeEventListener("abort", abort);
    try {
      claim.operation?.complete();
    } finally {
      releaseSessionControllerClaim(claim);
    }
  }
}
