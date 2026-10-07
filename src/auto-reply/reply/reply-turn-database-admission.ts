import { SessionWorkStartChangedError } from "../../config/sessions/lifecycle.js";
import type { SessionAdmissionDatabaseClaim } from "../../config/sessions/session-accessor.sqlite-entry-admission.js";
import type { SessionWorkAdmissionLease } from "../../sessions/session-lifecycle-admission.js";
import { replyRunRegistry, type ReplyOperation } from "./reply-run-registry.js";
import {
  lifecycleAdmissionByOperation,
  type ReplyOperationAdmission,
} from "./reply-run-registry.state.js";

/** Bind the original database borrow to the operation and join initialization handoffs on release. */
export function bindReplyOperationDatabaseAdmission(
  readerOperation: ReplyOperation,
  params: { sessionKey: string },
  lease: SessionWorkAdmissionLease | undefined,
  databaseClaim: SessionAdmissionDatabaseClaim | undefined,
) {
  let handoff: Promise<void> | undefined;
  let releasing: Promise<void> | undefined;
  const assertReaderOperation = () => {
    readerOperation.abortSignal.throwIfAborted();
    if (
      releasing ||
      lifecycleAdmissionByOperation.get(readerOperation) !== operationAdmission ||
      replyRunRegistry.get(readerOperation.key) !== readerOperation ||
      readerOperation.key !== params.sessionKey
    ) {
      throw new SessionWorkStartChangedError("Session reader operation is no longer current");
    }
  };
  const bindReader = (borrowedReader: ReplyOperationAdmission["reader"]) =>
    borrowedReader && {
      ...borrowedReader,
      assertCurrent: () => {
        assertReaderOperation();
        borrowedReader.assertCurrent();
      },
      withRead: ((request, assertCallerCurrent, consume) =>
        borrowedReader.withRead(
          request,
          () => {
            assertReaderOperation();
            assertCallerCurrent();
          },
          consume,
        )) satisfies typeof borrowedReader.withRead,
    };
  const operationAdmission: ReplyOperationAdmission = {
    lease,
    databaseIdentity: databaseClaim?.identity,
    databaseClaim,
    reader: bindReader(databaseClaim && "kind" in databaseClaim ? databaseClaim.reader : undefined),
    async afterInitialization(initialized) {
      const assertInitializing = () => {
        assertReaderOperation();
        if (readerOperation.result !== null) {
          throw new SessionWorkStartChangedError("Reply initialization is no longer active");
        }
      };
      assertInitializing();
      const current = operationAdmission.databaseClaim;
      const prepare = current && "kind" in current ? current.afterInitialization : undefined;
      if (!current || !prepare) {
        return;
      }
      if (handoff) {
        throw new Error("Session initialization admission handoff is already pending");
      }
      handoff = (async () => {
        const next = await prepare(initialized, assertInitializing);
        try {
          assertInitializing();
          current.assertCurrent();
          next.assertCurrent();
        } catch (error) {
          await next.release();
          throw error;
        }
        operationAdmission.databaseClaim = next;
        operationAdmission.reader = bindReader(next.reader);
        // Revoke the old view synchronously, then join its accepted work before returning.
        await current.release();
        assertInitializing();
        next.assertCurrent();
      })();
      try {
        await handoff;
      } finally {
        handoff = undefined;
      }
    },
  };
  lifecycleAdmissionByOperation.set(readerOperation, operationAdmission);
  const releaseWorkerDatabaseClaim =
    databaseClaim && "kind" in databaseClaim
      ? () => {
          if (!releasing) {
            const settlement = operationAdmission.databaseClaim?.release();
            releasing = (async () => {
              await Promise.allSettled([handoff, settlement]);
              await settlement;
            })();
          }
          return releasing;
        }
      : undefined;
  return { operationAdmission, releaseWorkerDatabaseClaim };
}
