import { logVerbose } from "../../../globals.js";
import { formatErrorMessage } from "../../../infra/errors.js";
import { hasSqliteWorkerOutcomeUnknown } from "../../../infra/sqlite-worker-contract.js";
import {
  selectedOperations,
  matchingEntries,
  selectedClaims,
  inputMatchesSessionId,
} from "../../../sessions/session-controller.lifecycle-projections.js";
import {
  runSessionMutation,
  captureSessionTarget,
  SESSION_CONTROLLER_DRAIN_TIMEOUT_MS,
  startSessionControllerInterruption,
  waitForSessionControllerSettlement,
} from "../../../sessions/session-controller.lifecycle.js";
import {
  cancelCapturedSessionControllerSource,
  captureSessionControllerStop,
} from "../../../sessions/session-controller.stop.js";
import { createLazyImportLoader } from "../../../shared/lazy-promise.js";
import type { OpenClawStateWorkerContext } from "../../../state/openclaw-state-worker-context.types.js";
import { createAgentRunDirectAbortError } from "../../run-termination.js";
import {
  createSubagentKillSettlementOwner,
  isKilledSubagentTarget,
} from "./subagent-control-kill-settlement.js";
import type { SubagentKillSession } from "./subagent-control-session.js";
import type {
  SubagentCancellationControl,
  SubagentKillInputSnapshot,
  SubagentKillMutationResult,
} from "./subagent-control.types.js";
import { SUBAGENT_ENDED_REASON_KILLED } from "./subagent-lifecycle-events.js";
import { resolveSubagentKillTargetState } from "./subagent-registry-completion.js";
import type { captureSubagentExecution } from "./subagent-registry-execution-cleanup.js";
import { getCurrentSubagentRunOwner, subagentRuns } from "./subagent-registry-memory.js";
import {
  assertSubagentRegistryWriteOutcomeKnown,
  assertSubagentRegistryWriteSourceCurrent,
  SubagentRegistryWriteError,
} from "./subagent-registry-persistence.js";
import {
  claimSubagentRunKill,
  markSubagentRunTerminated,
  releaseSubagentRunKillClaim,
  retireSubagentObligations,
} from "./subagent-registry.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

const subagentKillRuntimeLoader = createLazyImportLoader(
  () => import("./subagent-control.runtime.js"),
);

async function markSubagentRunTerminatedBestEffort(
  params: Parameters<typeof markSubagentRunTerminated>[0],
): Promise<number> {
  try {
    return await markSubagentRunTerminated(params);
  } catch (error) {
    if (hasSqliteWorkerOutcomeUnknown(error)) {
      throw error;
    }
    // A persistence failure must not leave the other siblings running.
    logVerbose(
      `subagents control kill: failed to persist ${params.runId ?? params.childSessionKey ?? "unknown"}: ${formatErrorMessage(error)}`,
    );
    return 0;
  }
}

export async function mutateSubagentRunForKill(
  params: {
    entry: SubagentRunRecord;
    session: SubagentKillSession;
    stateContext: OpenClawStateWorkerContext;
    cancellationControl: SubagentCancellationControl;
    suppressTaskDelivery?: boolean;
    beforeSessionKill?: () => boolean;
    requiredSessionId?: string;
    isCurrent: (entry: SubagentRunRecord, requirePreparedSession?: boolean) => boolean;
    withdrawQueuedReservation: () => void;
    refreshDescendants: () => Promise<number>;
  },
  captureExecution: () => ReturnType<typeof captureSubagentExecution>,
  stopAcceptance: { accepted: boolean },
): Promise<SubagentKillMutationResult> {
  const { stateContext } = params;
  const currentEntry = () => getCurrentSubagentRunOwner(subagentRuns, params.entry);
  const assertKnownOutcome = () => {
    const current = currentEntry();
    assertSubagentRegistryWriteOutcomeKnown(
      [current?.runId ?? params.entry.runId],
      stateContext.admission,
    );
  };
  const assertState = () => {
    stateContext.admission.assertCurrent();
    assertKnownOutcome();
    params.session.assertCurrent();
  };
  assertState();
  const targetState = () => {
    const current = currentEntry();
    return current ? resolveSubagentKillTargetState(current) : undefined;
  };
  const isCurrent = (requirePreparedSession = true) => {
    const current = currentEntry();
    return current !== undefined && params.isCurrent(current, requirePreparedSession);
  };
  const assertSelectedNativeRun = () => {
    assertSubagentRegistryWriteSourceCurrent(stateContext);
    assertKnownOutcome();
    if (!isCurrent(false)) {
      throw new Error("Subagent kill settlement lost its original run");
    }
  };
  const assertSelectedRun = () => {
    assertSelectedNativeRun();
    params.session.assertCurrent();
  };
  const markKilledBestEffort = () => {
    const selected = currentEntry();
    return selected
      ? markSubagentRunTerminatedBestEffort({
          runId: selected.runId,
          session: params.session,
          withdrawQueuedReservation: params.withdrawQueuedReservation,
          reason: "killed",
          suppressTaskDelivery: params.suppressTaskDelivery,
          context: stateContext,
          assertCurrent: assertSelectedRun,
        })
      : Promise.resolve(0);
  };
  const initial = currentEntry();
  if (!initial || !isCurrent()) {
    return { killed: false, superseded: true };
  }
  if (resolveSubagentKillTargetState(initial)) {
    if (params.suppressTaskDelivery) {
      await retireSubagentObligations(initial, () => {
        params.cancellationControl.assertCurrent();
        if (!isCurrent()) {
          throw new Error("Subagent ownership changed during cancellation; retry.");
        }
      });
    }
    if (
      currentEntry()?.endedReason === SUBAGENT_ENDED_REASON_KILLED &&
      currentEntry()?.suppressAnnounceReason !== "steer-restart"
    ) {
      await markKilledBestEffort();
    }
    if (!isCurrent()) {
      return { killed: false, superseded: true };
    }
    return { killed: false, targetState: targetState() };
  }
  if (initial.execution.endedAt && initial.pauseReason !== "sessions_yield") {
    return { killed: false };
  }
  const childSessionKey = params.entry.childSessionKey;
  const gatewaySourceSelection =
    params.requiredSessionId !== undefined && params.beforeSessionKill !== undefined;
  const resolved = params.session;
  const sessionId = resolved.entry?.sessionId;
  const captureSessionId = params.requiredSessionId ?? sessionId;
  const sessionLifecycleRevision = resolved.entry?.lifecycleRevision;
  const target = captureSessionTarget({
    storeScope: resolved.storePath,
    sessionKey: childSessionKey,
    agentId: resolved.agentId,
    incarnation: sessionId,
  });
  let capturedStop: ReturnType<typeof captureSessionControllerStop> | undefined;
  let capturedQueued: SubagentKillInputSnapshot[] = [];
  let runtime: Awaited<ReturnType<typeof subagentKillRuntimeLoader.load>> | undefined;
  let admission: "ready" | "declined" | "busy" = "ready";
  let killClaim: Awaited<ReturnType<typeof claimSubagentRunKill>>;
  const claimSelectedRunKill = async () => {
    try {
      let selected = currentEntry();
      for (let attempt = 0; selected && attempt < 2; attempt++) {
        const claim = await claimSubagentRunKill({
          runId: selected.runId,
          expected: params.entry,
          sessionId,
          sessionLifecycleRevision,
          suppressTaskDelivery: params.suppressTaskDelivery,
          context: stateContext,
          assertCurrent: () => {
            assertState();
            params.cancellationControl.assertCurrent();
          },
          assertPublicationCurrent: assertSelectedNativeRun,
        });
        if (claim) {
          return { claim };
        }
        const accepted = currentEntry();
        if (!accepted || accepted.runId === selected.runId) {
          return { claim: undefined };
        }
        // Only the same owner's acknowledged queued-to-running rekey can reselect admission.
        selected = accepted;
      }
      return { claim: undefined };
    } catch (error) {
      if (hasSqliteWorkerOutcomeUnknown(error)) {
        throw error;
      }
      return {
        failure: {
          killed: false,
          error: `Failed to persist subagent kill intent: ${formatErrorMessage(
            error instanceof SubagentRegistryWriteError ? error.cause : error,
          )}`,
        },
      };
    }
  };
  let preparationResult: Awaited<ReturnType<typeof mutateSubagentRunForKill>> | undefined;
  const releaseKillClaim = (claim: NonNullable<typeof killClaim>) => {
    const selected = currentEntry();
    return selected
      ? releaseSubagentRunKillClaim({
          runId: selected.runId,
          expected: params.entry,
          claim,
          context: stateContext,
        })
      : Promise.resolve(false);
  };
  const stillActive = async () => {
    try {
      if (killClaim && !stopAcceptance.accepted) {
        await releaseKillClaim(killClaim);
      }
    } catch (error) {
      if (hasSqliteWorkerOutcomeUnknown(error)) {
        throw error;
      }
      return {
        killed: false,
        error: `Subagent remained active and its kill intent could not be released: ${formatErrorMessage(error)}`,
      };
    }
    return {
      killed: false,
      error: stopAcceptance.accepted
        ? "Subagent accepted cancellation but is still active; cleanup is pending."
        : "Subagent is still active; try the kill again in a moment.",
    };
  };
  const cancellationFailure = async (
    error: unknown,
    declined?: true,
  ): Promise<NonNullable<typeof preparationResult>> => {
    let reason = formatErrorMessage(error);
    if (killClaim && !stopAcceptance.accepted) {
      try {
        await releaseKillClaim(killClaim);
      } catch (releaseError) {
        if (hasSqliteWorkerOutcomeUnknown(releaseError)) {
          throw releaseError;
        }
        reason += ` Kill intent could not be released: ${formatErrorMessage(releaseError)}`;
      }
    }
    return { killed: false, ...(declined ? { declined } : {}), error: reason };
  };
  const declineRevokedCancellation = ():
    | Promise<NonNullable<typeof preparationResult>>
    | undefined => {
    try {
      params.cancellationControl.assertCurrent();
      return undefined;
    } catch (error) {
      if (hasSqliteWorkerOutcomeUnknown(error)) {
        throw error;
      }
      return cancellationFailure(error, true);
    }
  };
  const killSettlement = createSubagentKillSettlementOwner({
    currentEntry,
    isCurrent,
    assertState,
    targetState,
    getClaim: () => killClaim,
    releaseClaim: releaseKillClaim,
    session: params.session,
    stateContext,
    cancellationControl: params.cancellationControl,
    stopAcceptance,
    suppressTaskDelivery: params.suppressTaskDelivery,
    withdrawQueuedReservation: params.withdrawQueuedReservation,
  });
  return await runSessionMutation({
    requiredSessionId: params.requiredSessionId,
    scope: resolved.storePath,
    identities: [childSessionKey, sessionId],
    prepare: async () => {
      for (
        let pending = params.cancellationControl.prepareRead?.();
        pending;
        pending = params.cancellationControl.prepareRead?.()
      ) {
        await pending;
      }
      if (!isCurrent()) {
        return;
      }
      {
        const declined = declineRevokedCancellation();
        if (declined) {
          preparationResult = await declined;
          return;
        }
      }
      capturedStop = captureSessionControllerStop({
        inputs: selectedClaims(target).flatMap((claim) =>
          claim.inputs.filter((input) => inputMatchesSessionId(input, captureSessionId)),
        ),
        operations: [...selectedOperations([target])].filter(
          (operation) =>
            captureSessionId === undefined || operation.hasOwnedSessionId(captureSessionId),
        ),
      });
      const capturedPreparation = captureSessionControllerStop({
        inputs: capturedStop.activeInputs.filter((input) => !input.claim?.operation),
      });
      capturedQueued = !gatewaySourceSelection
        ? matchingEntries(target).flatMap(
            (owner) =>
              owner.mailbox?.entries
                .filter(
                  (input) =>
                    !input.claim &&
                    input.phase !== "consumed" &&
                    inputMatchesSessionId(input, captureSessionId),
                )
                .map((input) => ({
                  input,
                  source: input.source,
                  target: input.target,
                  mailbox: input.mailbox,
                })) ?? [],
          )
        : [];
      // Admissions can release scheduler capacity synchronously when interrupted.
      await params.refreshDescendants();
      assertState();
      // The session fence is active before resolving/signaling other owners.
      // A refused full-session Stop must not interrupt their admissions or this collector.
      const execution = captureExecution()?.execution;
      const executionAborted = () => execution?.input.abortSignal.aborted === true;
      const alreadyAborted = executionAborted();
      try {
        if (params.beforeSessionKill?.() === false) {
          admission = "declined";
          return;
        }
      } finally {
        // A caller hook can accept this exact abort before refusing or throwing.
        stopAcceptance.accepted ||= !alreadyAborted && executionAborted();
      }
      if (!isCurrent()) {
        return;
      }
      {
        const declined = declineRevokedCancellation();
        if (declined) {
          preparationResult = await declined;
          return;
        }
      }
      const beforeInterruption = currentEntry();
      if (!beforeInterruption) {
        return;
      }
      if (
        beforeInterruption.swarmLaunchPending !== true &&
        beforeInterruption.execution.restartRecovery === undefined &&
        !resolveSubagentKillTargetState(beforeInterruption)
      ) {
        // Active completion must see cancellation before admission interruption.
        // Pending launch/recovery owners first need the drain to commit their identity.
        const claimed = await claimSelectedRunKill();
        if (claimed.failure) {
          preparationResult = claimed.failure;
          return;
        }
        killClaim = claimed.claim;
        if (killClaim) {
          if (!killSettlement.ownsSessionIncarnation()) {
            preparationResult = await killSettlement.releaseChangedSession(killClaim);
            return;
          }
          if (!killSettlement.isCurrent()) {
            preparationResult = { killed: false, superseded: true };
            return;
          }
        }
      }
      {
        const declined = declineRevokedCancellation();
        if (declined) {
          preparationResult = await declined;
          return;
        }
      }
      assertState();
      if (!killSettlement.isCurrent()) {
        preparationResult = { killed: false, superseded: true };
        return;
      }
      // An unbound selected claim is already a source owner. Cancel its captured
      // inputs before joining admission cleanup; a claim signal alone cannot retire them.
      const preparationStop = cancelCapturedSessionControllerSource(capturedPreparation, {
        assertCurrent: killSettlement.assertCancellationCurrent,
        reason: createAgentRunDirectAbortError(),
      });
      stopAcceptance.accepted ||= preparationStop.activeCancelled > 0 && killSettlement.isCurrent();
      killSettlement.assertCancellationCurrent();
      captureExecution();
      const interruption = startSessionControllerInterruption({
        scope: resolved.storePath,
        identities: [childSessionKey, sessionId],
        reason: createAgentRunDirectAbortError(),
        requiredSessionId: params.requiredSessionId,
        admissionsOnly: true,
      });
      const interruptedSelectedRun = () => {
        const selected = currentEntry();
        return (
          selected !== undefined &&
          killSettlement.isCurrent() &&
          (interruption.interruptedRunIds.has(params.entry.runId) ||
            interruption.interruptedRunIds.has(selected.runId))
        );
      };
      stopAcceptance.accepted ||= interruptedSelectedRun();
      const released = await waitForSessionControllerSettlement(
        interruption.released,
        SESSION_CONTROLLER_DRAIN_TIMEOUT_MS,
      );
      const settled =
        released &&
        (!stopAcceptance.accepted ||
          !capturedStop ||
          (await waitForSessionControllerSettlement(
            capturedStop.settled,
            SESSION_CONTROLLER_DRAIN_TIMEOUT_MS,
          )));
      admission = settled ? "ready" : "busy";
      stopAcceptance.accepted ||= interruptedSelectedRun();
      // Native preaccept cancellation first returns its recorded abort outcome.
      // Claim before another worker read lets that response adopt the queued row.
      const afterInterruption = currentEntry();
      if (
        released &&
        params.beforeSessionKill &&
        afterInterruption?.swarmLaunchPending === true &&
        afterInterruption.execution.restartRecovery === undefined &&
        !resolveSubagentKillTargetState(afterInterruption) &&
        isCurrent()
      ) {
        const claimed = await claimSelectedRunKill();
        if (claimed.failure) {
          preparationResult = claimed.failure;
        } else {
          killClaim = claimed.claim;
        }
      }
    },
    run: async function run(): Promise<Awaited<ReturnType<typeof mutateSubagentRunForKill>>> {
      if (preparationResult) {
        return preparationResult;
      }
      if (admission === "declined") {
        return { killed: false, declined: true as const };
      }
      if (admission === "busy") {
        return stillActive();
      }
      let readFailure: { error: unknown } | undefined;
      try {
        for (
          let pending = params.cancellationControl.prepareRead?.();
          pending;
          pending = params.cancellationControl.prepareRead?.()
        ) {
          await pending;
        }
      } catch (error) {
        if (hasSqliteWorkerOutcomeUnknown(error)) {
          throw error;
        }
        if (!stopAcceptance.accepted) {
          return cancellationFailure(error);
        }
        readFailure = { error };
      }
      // Admission draining yields. Fence the exact row before
      // touching session-owned queues so a successor cannot inherit an older kill.
      if (!isCurrent()) {
        return { killed: false, superseded: true };
      }
      if (killClaim && !killSettlement.ownsSessionIncarnation()) {
        return killSettlement.releaseChangedSession(killClaim);
      }
      if (!readFailure) {
        await params.refreshDescendants();
      }
      if (!isCurrent()) {
        return { killed: false, superseded: true };
      }
      const targetStateAfterAdmission = targetState();
      if (targetStateAfterAdmission) {
        const killedTarget = isKilledSubagentTarget(targetStateAfterAdmission);
        const claimedCurrentKill = killClaim !== undefined && killSettlement.isCurrent();
        if (killedTarget && (!killClaim || claimedCurrentKill)) {
          await markKilledBestEffort();
        }
        return await killSettlement.retireObligations({
          killed: killedTarget && claimedCurrentKill,
          targetState: targetStateAfterAdmission,
          ...(readFailure ? { error: formatErrorMessage(readFailure.error) } : {}),
        });
      }
      const declined = readFailure ? undefined : declineRevokedCancellation();
      if (declined && !stopAcceptance.accepted) {
        return declined;
      }
      if (!killClaim) {
        const claimed = await claimSelectedRunKill();
        if (claimed.failure) {
          return claimed.failure;
        }
        killClaim = claimed.claim;
      }
      if (!killClaim) {
        return {
          killed: false,
          superseded: true,
        };
      }
      const claimedKill = killClaim;
      try {
        if (!killSettlement.ownsSessionIncarnation()) {
          return killSettlement.releaseChangedSession(claimedKill);
        }
        if (!killSettlement.isCurrent()) {
          return { killed: false, superseded: true };
        }
        if (readFailure || declined) {
          // Missing caller facts or revocation fence new effects, but the accepted
          // interruption's exact claim still owns settlement.
          const settled: Awaited<ReturnType<typeof mutateSubagentRunForKill>> =
            await killSettlement.settle(claimedKill);
          return readFailure
            ? {
                ...settled,
                error: [settled.error, formatErrorMessage(readFailure.error)]
                  .filter(Boolean)
                  .join(" "),
              }
            : settled;
        }
        if (!runtime) {
          try {
            runtime = await subagentKillRuntimeLoader.load();
          } catch (error) {
            if (hasSqliteWorkerOutcomeUnknown(error)) {
              throw error;
            }
            return cancellationFailure(error);
          }
          // Loading can yield; repeat authority checks inside the retained mutation.
          return await run();
        }
        const active = capturedStop?.operations.some((operation) => !operation.result) === true;
        if (!killSettlement.ownsSessionIncarnation()) {
          return killSettlement.releaseChangedSession(claimedKill);
        }
        const declinedBeforeAbort = declineRevokedCancellation();
        if (declinedBeforeAbort) {
          return stopAcceptance.accepted
            ? await killSettlement.settle(claimedKill)
            : declinedBeforeAbort;
        }
        if (!killSettlement.isCurrent()) {
          return { killed: false, superseded: true };
        }
        const stopped = capturedStop
          ? cancelCapturedSessionControllerSource(capturedStop, {
              assertCurrent: killSettlement.assertCancellationCurrent,
              reason: createAgentRunDirectAbortError(),
            })
          : undefined;
        stopAcceptance.accepted ||= (stopped?.activeCancelled ?? 0) > 0;
        // Without a captured controller record nothing is cancelled here; the kill
        // settles registry state alone and never re-selects a turn by session ID.
        const noControllerRecord = !capturedStop?.inputs.length && !capturedStop?.operations.length;
        // Native cancellation is a request. Only the captured producer can settle its raw work;
        // a producer that ignores it must not hold the caller's Stop past the drain bound.
        const refused =
          capturedStop &&
          [
            ...capturedStop.operations,
            ...capturedStop.activeInputs.flatMap((input) => input.claim?.operation ?? []),
          ].some((operation) => !operation.result && !operation.abortSignal.aborted);
        if (capturedStop && !refused) {
          await waitForSessionControllerSettlement(
            capturedStop.settled,
            SESSION_CONTROLLER_DRAIN_TIMEOUT_MS,
          );
        }
        if (!killSettlement.ownsSessionIncarnation()) {
          return killSettlement.releaseChangedSession(claimedKill);
        }
        const declinedBeforeQueueClear = declineRevokedCancellation();
        if (declinedBeforeQueueClear) {
          return stopAcceptance.accepted
            ? await killSettlement.settle(claimedKill)
            : declinedBeforeQueueClear;
        }
        // Narrow Gateway Stop already cancelled its captured authorized sources.
        // Rediscovering key-wide queues here would include preserved incarnations.
        const selected = capturedQueued
          .filter(
            ({ input, source, target: sourceTarget, mailbox }) =>
              !input.claim &&
              input.source === source &&
              input.target === sourceTarget &&
              input.mailbox === mailbox,
          )
          .map(({ input }) => input);
        const cleared = !gatewaySourceSelection
          ? runtime.clearSessionQueues([childSessionKey, sessionId], target, selected)
          : { followupCleared: 0, keys: [] };
        const queuedCleanupSettled = await waitForSessionControllerSettlement(
          Promise.all(
            selected
              .filter((input) => input.retirementRequested)
              .map((input) => input.settlement.promise),
          ).then(() => undefined),
          SESSION_CONTROLLER_DRAIN_TIMEOUT_MS,
        );
        const completedCleanupError = queuedCleanupSettled
          ? undefined
          : "Subagent queued input cleanup remains pending after its drain deadline.";
        if (cleared.followupCleared > 0) {
          logVerbose(
            `subagents control kill: cleared followups=${cleared.followupCleared} keys=${cleared.keys.join(",")}`,
          );
        }
        if (completedCleanupError) {
          // Retain the durable kill intent until reconciliation can observe settled custody.
          return { killed: false, completedCleanupError };
        }
        if (refused || (active && !stopAcceptance.accepted)) {
          return stillActive();
        }
        const settledTarget = targetState();
        if (settledTarget) {
          const killedTarget = isKilledSubagentTarget(settledTarget);
          if (killedTarget) {
            await markKilledBestEffort();
          } else {
            try {
              await releaseKillClaim(killClaim);
            } catch (error) {
              if (hasSqliteWorkerOutcomeUnknown(error)) {
                throw error;
              }
              return {
                killed: false,
                targetState: settledTarget,
                error: `Completed subagent kill intent could not be released: ${formatErrorMessage(error)}`,
              };
            }
          }
          return await killSettlement.retireObligations({
            killed: killedTarget,
            targetState: settledTarget,
          });
        }
        const settled = await killSettlement.settle(claimedKill);
        return noControllerRecord ? { ...settled, reason: "no_controller_record" } : settled;
      } catch (error) {
        if (hasSqliteWorkerOutcomeUnknown(error)) {
          throw error;
        }
        return { killed: false, error: formatErrorMessage(error) };
      }
    },
    finalize: async () => {
      // Preparation now owns the claim, including failed drains and persistence.
      // Only its exact retained claim may withdraw the captured reservation.
      if (killClaim && killSettlement.ownsIntent(currentEntry(), killClaim)) {
        params.withdrawQueuedReservation();
      }
    },
  });
}
