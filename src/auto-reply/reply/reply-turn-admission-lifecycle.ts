import { scheduleMainSessionRecoveryPendingTarget } from "../../agents/main-session-recovery/main-session-recovery-owner-release.js";
import {
  releaseMainSessionRecoveryOwner,
  type MainSessionRecoveryOwnerLease,
  type MainSessionRecoveryPendingTarget,
} from "../../agents/main-session-recovery/main-session-recovery-store.js";
import type { SessionAdmissionDatabaseClaim } from "../../config/sessions/session-accessor.sqlite-entry-admission.js";
import { formatErrorMessage } from "../../infra/errors.js";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import {
  getGatewayContextResolver,
  withPluginRuntimeGatewayContextResolver,
} from "../../plugins/runtime/gateway-request-scope.js";
import {
  registerReplyOperationSuccessorBarrier,
  runAfterReplyOperationClear,
  type ReplyOperation,
} from "../../sessions/session-controller.js";
import {
  withSessionControllerOwner,
  type SessionEffectRef,
} from "../../sessions/session-controller.lifecycle.js";
import { lifecycleAdmissionByOperation } from "../../sessions/session-controller.state.js";

const log = createSubsystemLogger("auto-reply/reply-turn-admission");

export async function releaseReplyRecoveryOwner(
  lease: MainSessionRecoveryOwnerLease | undefined,
): Promise<MainSessionRecoveryPendingTarget | undefined> {
  if (!lease) {
    return undefined;
  }
  try {
    return await releaseMainSessionRecoveryOwner(lease);
  } catch (error) {
    log.warn(`failed to release main-session recovery reply owner: ${formatErrorMessage(error)}`);
    // The durable owner schedules exact-token retries. A completed reply must
    // not keep its successor barrier and lifecycle admission until that
    // background repair wins a contested SQLite write.
    return undefined;
  }
}

export function bindReplyAdmissionRelease(params: {
  operation: ReplyOperation;
  admission: SessionEffectRef | undefined;
  databaseClaim: SessionAdmissionDatabaseClaim | undefined;
  recoveryOwnerLease: MainSessionRecoveryOwnerLease | undefined;
  sessionId: string;
  sessionKey: string;
}): void {
  const { operation, admission, databaseClaim, recoveryOwnerLease, sessionId, sessionKey } = params;
  const operationAdmission = {
    lease: admission,
    databaseIdentity: databaseClaim?.identity,
  };
  lifecycleAdmissionByOperation.set(operation, operationAdmission);
  const releaseWorkerDatabaseClaim =
    databaseClaim && "kind" in databaseClaim ? () => databaseClaim.release() : undefined;
  if (releaseWorkerDatabaseClaim) {
    registerReplyOperationSuccessorBarrier({
      operation,
      sessionId,
      sessionKeys: [sessionKey],
      start: releaseWorkerDatabaseClaim,
      deferUntilClear: true,
    });
  }
  if (admission) {
    // The lifecycle fence spans hooks, media, execution, and final delivery;
    // reset/delete waits for the owner to clear before mutating the session.
    let recoveryOwnerRelease: Promise<MainSessionRecoveryPendingTarget | undefined> | undefined;
    const releaseRecoveryOwner = () =>
      (recoveryOwnerRelease ??= releaseReplyRecoveryOwner(recoveryOwnerLease));
    if (recoveryOwnerLease) {
      registerReplyOperationSuccessorBarrier({
        operation,
        sessionId: recoveryOwnerLease.sessionId,
        sessionKeys: [sessionKey, recoveryOwnerLease.sessionKey],
        start: releaseRecoveryOwner,
      });
    }
    runAfterReplyOperationClear(operation, () => {
      // Keep immutable store correlation after releasing only this admission's lease.
      operationAdmission.lease = undefined;
      // Keep reset/delete behind durable owner release and its writer lock.
      void Promise.all([releaseRecoveryOwner(), releaseWorkerDatabaseClaim?.()]).then(
        ([pendingTarget]) => {
          admission.release();
          scheduleMainSessionRecoveryPendingTarget(pendingTarget);
        },
        (error: unknown) => {
          log.warn(`failed to release reply database owner: ${formatErrorMessage(error)}`);
        },
      );
    });
  }
  if (databaseClaim && !("kind" in databaseClaim)) {
    runAfterReplyOperationClear(operation, databaseClaim.release);
  }
}

/** Runs owner work with its admission marked as the initiating lifecycle context. */
export async function runWithReplyOperationLifecycleAdmission<T>(
  operation: ReplyOperation,
  run: () => Promise<T>,
): Promise<T> {
  const admission = lifecycleAdmissionByOperation.get(operation)?.lease;
  if (admission) {
    return await admission.run(() => withSessionControllerOwner(operation, run));
  }
  const resolver = getGatewayContextResolver(operation);
  return await withPluginRuntimeGatewayContextResolver(resolver, () =>
    withSessionControllerOwner(operation, run),
  );
}
