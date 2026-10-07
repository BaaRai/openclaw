import { isDeepStrictEqual } from "node:util";
import { runWithoutOwnedSessionTranscriptWrites } from "../../../config/sessions/transcript-write-context.js";
import { isGatewayRestartDrainError } from "../../../process/gateway-work-admission.js";
import { captureOpenClawStateWorkerContext } from "../../../state/openclaw-state-worker-context.js";
import type { OpenClawStateWorkerContext } from "../../../state/openclaw-state-worker-context.types.js";
import { resolveSubagentRequesterAgentId } from "../../subagent-requester-owner.js";
import { revokeRequesterCronAuthorityBatch } from "../requester-cron-authority.js";
import { revokeRequesterFinalAttachment } from "../requester-final-attachment.js";
import { retireSubagentControllerInputs } from "./subagent-controller-inputs.js";
import { isCompletedRequesterDeliveryBlocked } from "./subagent-delivery-state.js";
import { retireSubagentGatewayBinding } from "./subagent-registry-execution-cleanup.js";
import type { SubagentLifecycleWakeContext } from "./subagent-registry-lifecycle-context.js";
import {
  buildSafeLifecycleErrorMeta,
  maskLifecycleIdentifier,
} from "./subagent-registry-lifecycle-log.js";
import { subagentRuns } from "./subagent-registry-memory.js";
import {
  assertSubagentRegistryWriteSourceCurrent,
  mutateSubagentRuns,
} from "./subagent-registry-persistence.js";
import { settleRequesterSettleWakeBatch } from "./subagent-registry-requester-wake-mutation.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";
import { captureRequesterSettleRunIdentity } from "./subagent-requester-settle-identity.js";
import { isClaimedByLiveRequesterTurn } from "./subagent-requester-turn-liveness.js";
import {
  currentSubagentRunOrObserved,
  getSubagentRunRuntimeKey,
  isSameSubagentRunOwner,
} from "./subagent-run-generation.js";
import { hasSubagentRunEnded } from "./subagent-run-liveness.js";

/** Releases a settled batch's process-local custody, then re-evaluates the requester's other wakes. */
function releaseRequesterSettleWakeBatch(
  context: SubagentLifecycleWakeContext,
  observedEntries: readonly SubagentRunRecord[],
  rearmGeneration: number | undefined,
  stateContext: OpenClawStateWorkerContext,
): void {
  const params = context.options;
  const entries = observedEntries.map((entry) => currentSubagentRunOrObserved(params.runs, entry));
  revokeRequesterCronAuthorityBatch(entries, rearmGeneration);
  const retiredEntries = entries.filter((entry) => !params.runs.has(entry.runId));
  for (const entry of retiredEntries) {
    subagentRuns.confirmRetirement(entry);
  }
  for (const entry of entries) {
    if (entry.requesterSettleWake === undefined || !params.runs.has(entry.runId)) {
      retireSubagentGatewayBinding(entry);
      params.resumedRuns.delete(getSubagentRunRuntimeKey(entry));
      params.clearPendingLifecycleError(entry.runId);
    }
  }
  const requesterSessionKeys = new Set(entries.map((entry) => entry.requesterSessionKey));
  for (const [runId, entry] of params.runs) {
    if (entry.requesterSettleWake && requesterSessionKeys.has(entry.requesterSessionKey)) {
      scheduleRequesterSettleWake(context, runId, entry, stateContext);
    }
  }
  for (const entry of retiredEntries) {
    if (!params.runs.has(entry.runId)) {
      context.resumeAncestorCleanup(entry);
    }
  }
}

/**
 * Kill, suppression, and deletion retire a child's owed inputs and its cohort membership.
 * Whatever wake the current row still owes is retired; a wake already settled needs nothing.
 */
export async function retireSubagentObligations(
  context: SubagentLifecycleWakeContext,
  entry: SubagentRunRecord,
  assertCurrent: () => void,
): Promise<void> {
  assertCurrent();
  retireSubagentControllerInputs(entry);
  if (!entry.requesterSettleWake) {
    return;
  }
  const workerContext = captureOpenClawStateWorkerContext();
  await mutateSubagentRuns(
    [entry.runId],
    (rows) => {
      const current = rows.get(entry.runId);
      const wake = current?.requesterSettleWake;
      if (
        !current ||
        !isSameSubagentRunOwner(current, entry) ||
        !wake ||
        current.execution.status !== "terminal" ||
        current.pauseReason === "sessions_yield"
      ) {
        return { value: undefined };
      }
      const next = structuredClone(current);
      next.suppressCompletionDelivery = true;
      next.requesterSettleWake = undefined;
      return { value: wake, postimages: new Map([[next.runId, next]]) };
    },
    {
      runs: context.options.runs,
      context: workerContext,
      assertCurrent,
      onPublished: (postimages, wake) => {
        const published = postimages.get(entry.runId);
        if (!wake || !published) {
          return;
        }
        const requesterAgentId = resolveSubagentRequesterAgentId(
          context.options.getRuntimeConfig(),
          published,
        );
        if (requesterAgentId && wake.requesterYieldBatch && wake.rearmGeneration !== undefined) {
          revokeRequesterFinalAttachment({
            requesterAgentId,
            requesterSessionKey: published.requesterSessionKey,
            batchRunIds: wake.batchRunIds ?? [published.runId],
            rearmGeneration: wake.rearmGeneration,
          });
        }
        // Retiring a member re-evaluates its cohort; it can no longer hold the batch.
        releaseRequesterSettleWakeBatch(context, [published], wake.rearmGeneration, workerContext);
      },
    },
  );
}

/** One in-flight evaluation per owed continuation; members of one cohort share it. */
function requesterSettleWakeScope(entry: SubagentRunRecord): string {
  const wake = entry.requesterSettleWake;
  const owner = `${entry.requesterAgentId ?? ""}\0${entry.requesterSessionKey}`;
  if (entry.pauseReason === "sessions_yield" && wake?.pauseNotice) {
    return `${owner}\0pause\0${entry.runId}`;
  }
  return `${owner}\0${wake?.batchRunIds?.toSorted().join(",") ?? ""}\0${wake?.rearmGeneration ?? ""}`;
}

/**
 * Evaluates the requester continuation this row owes. Only the requester's mailbox
 * decides when the continuation runs; this owner decides only whether it is owed.
 */
export function scheduleRequesterSettleWake(
  context: SubagentLifecycleWakeContext,
  runId: string,
  observedEntry: SubagentRunRecord,
  originalContext?: OpenClawStateWorkerContext,
): void {
  const params = context.options;
  const published = params.runs.get(runId);
  if (published && !isSameSubagentRunOwner(published, observedEntry)) {
    return;
  }
  let entry = published ?? observedEntry;
  const admittedWake = entry.requesterSettleWake;
  const requesterSessionKey = entry.requesterSessionKey?.trim();
  if (
    !admittedWake ||
    entry.collect ||
    !requesterSessionKey ||
    (isCompletedRequesterDeliveryBlocked(entry) && admittedWake.requesterYieldBatch !== true) ||
    entry.execution.status === "running" ||
    !hasSubagentRunEnded(entry) ||
    // A live requester turn transfers its own children when it ends.
    isClaimedByLiveRequesterTurn(entry)
  ) {
    return;
  }
  const scope = requesterSettleWakeScope(entry);
  const active = context.activeRequesterSettleWakes.get(scope);
  if (active) {
    active.rearm = entry;
    return;
  }
  const evaluation: { rearm?: SubagentRunRecord; evaluated?: SubagentRunRecord } = {};
  context.activeRequesterSettleWakes.set(scope, evaluation);
  const stateContext = originalContext ?? captureOpenClawStateWorkerContext();
  const admittedIdentity = captureRequesterSettleRunIdentity(entry);
  const isSourceCurrent = () => {
    try {
      assertSubagentRegistryWriteSourceCurrent(stateContext);
      entry = currentSubagentRunOrObserved(params.runs, entry);
      return (
        params.runs.get(entry.runId) === entry &&
        isDeepStrictEqual(captureRequesterSettleRunIdentity(entry), admittedIdentity)
      );
    } catch {
      return false;
    }
  };
  const admittedBatch = (
    entry.pauseReason === "sessions_yield" && admittedWake.pauseNotice
      ? [runId]
      : (admittedWake.batchRunIds ?? [runId])
  ).flatMap((id) => {
    const member = params.runs.get(id);
    return member ? [member] : [];
  });
  const completeBatch = async (
    batch: readonly SubagentRunRecord[],
    rearmGeneration: number | undefined,
    outcome?: Parameters<typeof settleRequesterSettleWakeBatch>[3],
    onCommitted?: () => void,
  ) => {
    if (Boolean(admittedWake.pauseNotice) !== Boolean(entry.requesterSettleWake?.pauseNotice)) {
      return;
    }
    if (
      await settleRequesterSettleWakeBatch(context, batch, rearmGeneration, outcome, stateContext)
    ) {
      onCommitted?.();
      releaseRequesterSettleWakeBatch(context, batch, rearmGeneration, stateContext);
    }
  };
  runWithoutOwnedSessionTranscriptWrites(() => {
    void context
      .runRequesterSettleWake(
        entry,
        async () => {
          // Admission can wait; reread the row before a blocked receipt is executed again.
          if (
            !isSourceCurrent() ||
            (isCompletedRequesterDeliveryBlocked(entry) &&
              entry.requesterSettleWake?.requesterYieldBatch !== true)
          ) {
            return;
          }
          evaluation.evaluated = entry;
          try {
            await params.maybeWakeRequesterAfterAllChildrenSettled({
              requesterSessionKey,
              requesterOrigin: entry.requesterOrigin,
              settledEntry: entry,
              isSourceCurrent,
              completeBatch,
            });
          } catch (error: unknown) {
            if (isGatewayRestartDrainError(error)) {
              return;
            }
            const safeError = buildSafeLifecycleErrorMeta(error);
            params.warn("requester settle wake failed", {
              error: safeError,
              runId: maskLifecycleIdentifier(runId, "run"),
              requesterSessionKey: maskLifecycleIdentifier(requesterSessionKey, "session"),
            });
            if (!isSourceCurrent()) {
              return;
            }
            // The failure is this wake's one recorded outcome; nothing retries it.
            await completeBatch(admittedBatch, admittedWake.rearmGeneration, {
              delivered: false,
              path: "none",
              error: safeError.message,
            }).catch((settleError: unknown) => {
              params.warn("failed to persist requester settle wake rejection", {
                error: buildSafeLifecycleErrorMeta(settleError),
                runId: maskLifecycleIdentifier(runId, "run"),
              });
            });
          }
        },
        isSourceCurrent,
      )
      .catch((error: unknown) => {
        if (!isGatewayRestartDrainError(error)) {
          params.warn("requester settle wake admission failed", {
            error: buildSafeLifecycleErrorMeta(error),
            runId: maskLifecycleIdentifier(runId, "run"),
          });
        }
      })
      .finally(() => {
        context.activeRequesterSettleWakes.delete(scope);
        // A trigger carrying the very row this evaluation read adds nothing new.
        const rearm = evaluation.rearm;
        if (rearm && rearm !== evaluation.evaluated) {
          scheduleRequesterSettleWake(context, rearm.runId, rearm, stateContext);
        }
      });
  });
}
