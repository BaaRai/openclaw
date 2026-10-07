import { AsyncLocalStorage } from "node:async_hooks";
import { expect, it, onTestFinished, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import type { AgentCommandOpts } from "../agents/command/types.js";
import * as killSession from "../agents/subagents/registry/subagent-control-session.js";
import * as bookkeeping from "../agents/subagents/registry/subagent-registry-lifecycle-bookkeeping.js";
import { SubagentLifecycleController } from "../agents/subagents/registry/subagent-registry-lifecycle.js";
import { subagentRuns } from "../agents/subagents/registry/subagent-registry-memory.js";
import { registerSubagentRun } from "../agents/subagents/registry/subagent-registry.js";
import {
  writeSubagentSessionEntry,
  settleSubagentRegistryPersistenceWork,
} from "../agents/subagents/registry/subagent-registry.persistence.test-support.js";
import { loadSubagentRunsForControllerFromSqlite } from "../agents/subagents/registry/subagent-registry.store.sqlite.js";
import { applySessionEntryExactReplacements } from "../config/sessions/session-accessor.sqlite-replacement-projection.js";
import { runWithGatewayIndependentRootWorkAdmission } from "../process/gateway-work-admission.js";
import {
  observeSessionWorkAdmissionDrain,
  rpcSourceTesting,
} from "../sessions/session-lifecycle-admission.test-support.js";
import type { UserTurnTranscriptRecorder } from "../sessions/user-turn-transcript.types.js";
import { runOutsideAsyncWorkScope } from "../shared/async-work-scope.js";
import { createDeferredCore } from "../shared/deferred.js";
import * as writerQueue from "../shared/store-writer-queue.js";
import type { AgentDatabaseExecutionScope } from "../state/openclaw-agent-execution-contract.js";
import * as executionOwner from "../state/openclaw-agent-execution.js";
import { SQLITE_SESSION_WRITER_QUEUES } from "../state/openclaw-agent-write-admission.js";
import * as agentJobs from "./agent-turn/agent-job.js";
import { dispatchGatewayMethodInProcess } from "./server-plugin-in-process-dispatch.js";
import { loadSessionEntry } from "./session-utils.js";
import type { agentCommandMock as gatewayAgentCommandMock } from "./test-helpers.js";

// Holds the metadata result until descendant Stop enqueues its mandatory marker.
function holdMetadataThroughSubagentStop(target: {
  sessionKey: string;
  sessionId: string;
  storePath: string;
  signal: AbortSignal;
}) {
  const producerScope = new AsyncLocalStorage<boolean>();
  const markerScope = new AsyncLocalStorage<boolean>();
  const nativeResultHeld = createDeferredCore();
  const releaseResult = createDeferredCore();
  const restores: Array<() => void> = [];
  const label = `concurrent metadata ${target.sessionId}`;
  let intercepted = false;
  let resultHeld = false;
  let bookkeepingReturned = false;
  let markerEnqueued = false;
  let producerPath: string | undefined;
  let releaseReason: "marker-enqueued" | "bookkeeping-error" | "cleanup" | "test-abort" | undefined;
  let producer: Promise<void> | undefined;
  let producerOutcome: Promise<{ ok: true } | { ok: false; error: unknown }> | undefined;
  let disposal: Promise<void> | undefined;
  const release = (reason: NonNullable<typeof releaseReason>) => {
    if (releaseReason === undefined) {
      releaseReason = reason;
      releaseResult.resolve();
    }
  };
  const onAbort = () => release("test-abort");
  target.signal.addEventListener("abort", onAbort, { once: true });

  const runQueued = writerQueue.runQueuedStoreWrite;
  const queueSpy = vi.spyOn(writerQueue, "runQueuedStoreWrite").mockImplementation((params) => {
    if (params.queues !== SQLITE_SESSION_WRITER_QUEUES) {
      return runQueued(params);
    }
    if (producerScope.getStore() === true) {
      producerPath ??= params.storePath;
    }
    const selectingMarker =
      markerScope.getStore() === true &&
      bookkeepingReturned &&
      resultHeld &&
      releaseReason === undefined &&
      params.storePath === producerPath;
    const before = selectingMarker
      ? new Set(params.queues.get(params.storePath)?.pending ?? [])
      : undefined;
    const pending = runQueued(params);
    // Inspect only after the canonical queue has synchronously admitted its real waiter.
    if (
      before &&
      params.queues.get(params.storePath)?.pending.some((entry) => !before.has(entry))
    ) {
      markerEnqueued = true;
      release("marker-enqueued");
    }
    return pending;
  });
  restores.push(() => queueSpy.mockRestore());

  const captureExecution = executionOwner.captureOpenClawAgentDatabaseExecution;
  const executionSpy = vi
    .spyOn(executionOwner, "captureOpenClawAgentDatabaseExecution")
    .mockImplementation((...args) => {
      const execution = captureExecution(...args);
      if (producerScope.getStore() !== true) {
        return execution;
      }
      const runExisting: typeof execution.runExisting = (source, operation, options) =>
        execution.runExisting(
          source,
          (scope) => {
            const wrapped: AgentDatabaseExecutionScope = {
              execute(command, commandOptions) {
                const pending = scope.execute(command, commandOptions);
                if (command.type !== "session.entries.replace" || resultHeld) {
                  return pending;
                }
                resultHeld = true;
                return pending.then(async (value) => {
                  nativeResultHeld.resolve();
                  await releaseResult.promise;
                  return value;
                });
              },
            };
            return operation(wrapped);
          },
          options,
        );
      return new Proxy(execution, {
        get(original, key, receiver) {
          return key === "runExisting" ? runExisting : Reflect.get(original, key, receiver);
        },
      });
    });
  restores.push(() => executionSpy.mockRestore());

  const persistMarker = killSession.persistSubagentAbortedLastRun;
  const markerSpy = vi
    .spyOn(killSession, "persistSubagentAbortedLastRun")
    .mockImplementation((params) => {
      if (
        params.childSessionKey !== target.sessionKey ||
        !params.abortedLastRun ||
        !bookkeepingReturned
      ) {
        return persistMarker(params);
      }
      return markerScope.run(true, () => persistMarker(params));
    });
  restores.push(() => markerSpy.mockRestore());

  const completeBookkeeping = bookkeeping.completeCleanupBookkeeping;
  const bookkeepingSpy = vi
    .spyOn(bookkeeping, "completeCleanupBookkeeping")
    .mockImplementation(async (context, params) => {
      if (
        intercepted ||
        !params.provisionalKill ||
        params.entry.childSessionKey !== target.sessionKey
      ) {
        return completeBookkeeping(context, params);
      }
      intercepted = true;
      // A new root alone retains the RPC's AsyncWorkScope. This producer must not block
      // that scope while waiting for the mandatory marker which the RPC has yet to enqueue.
      producer = runOutsideAsyncWorkScope(() =>
        runWithGatewayIndependentRootWorkAdmission(
          () =>
            producerScope.run(true, () =>
              applySessionEntryExactReplacements({
                agentId: "main",
                storePath: target.storePath,
                sessionKeys: [target.sessionKey],
                activeSessionKey: target.sessionKey,
                skipMaintenance: true,
                requireWriteSuccess: true,
                update(entries) {
                  const row = entries.find((entry) => entry.sessionKey === target.sessionKey);
                  if (
                    !row ||
                    row.entry.sessionId !== target.sessionId ||
                    row.entry.label === label
                  ) {
                    throw new Error("Concurrent metadata writer lost its original child");
                  }
                  return {
                    result: undefined,
                    replacements: [{ sessionKey: row.sessionKey, entry: { ...row.entry, label } }],
                  };
                },
              }),
            ),
          "test:private-completion-metadata",
          target.signal,
        ),
      );
      producerOutcome = producer.then(
        () => ({ ok: true as const }),
        (error: unknown) => ({ ok: false as const, error }),
      );
      try {
        await Promise.race([
          nativeResultHeld.promise,
          producer.then(() => {
            throw new Error("Metadata writer finished without its native-result barrier");
          }),
        ]);
        target.signal.throwIfAborted();
        await completeBookkeeping(context, params);
        bookkeepingReturned = true;
        // Success deliberately leaves the writer held until the real marker's FIFO enqueue.
      } catch (error) {
        release("bookkeeping-error");
        await producerOutcome;
        throw error;
      }
    });
  restores.push(() => bookkeepingSpy.mockRestore());

  return {
    label,
    async assertCompleted() {
      expect(intercepted).toBe(true);
      expect(resultHeld).toBe(true);
      expect(bookkeepingReturned).toBe(true);
      expect(markerEnqueued).toBe(true);
      expect(releaseReason).toBe("marker-enqueued");
      expect(await producerOutcome).toEqual({ ok: true });
    },
    releaseForCleanup() {
      release("cleanup");
    },
    dispose(): Promise<void> {
      disposal ??= (async () => {
        release("cleanup");
        target.signal.removeEventListener("abort", onAbort);
        try {
          const outcome = await producerOutcome;
          if (outcome && !outcome.ok) {
            throw outcome.error;
          }
        } finally {
          for (const restore of restores.toReversed()) {
            restore();
          }
          producerScope.disable();
          markerScope.disable();
        }
      })();
      return disposal;
    },
  };
}

type PrivateCompletionStopMetadataFixture = {
  kernel: Awaited<ReturnType<(typeof import("./server-kernel.js"))["createGatewayKernel"]>>;
  sequence: number;
  sessionKey: string;
  sessionId: string;
  runId: string;
  storePath: string;
  agentCommandMock: typeof gatewayAgentCommandMock;
  recorder: (input: unknown) => UserTurnTranscriptRecorder;
  dispatch: (
    message?: string,
    onAccepted?: () => void,
  ) => ReturnType<typeof dispatchGatewayMethodInProcess<Record<string, unknown>>>;
  completions: () => Record<string, unknown>[];
  pending: () => Record<string, unknown>[];
  restart: () => Promise<void>;
};

/** Registers the private Stop table that overlaps descendant cleanup with a metadata writer. */
export function registerPrivateCompletionStopMetadataTests(
  getFixture: () => PrivateCompletionStopMetadataFixture,
) {
  it.for([false, true])(
    "preserves an operator stop after private input consumption across retry and restart (terminal first=%s)",
    async (terminalFirst, { signal }) => {
      const {
        kernel,
        sequence,
        sessionKey,
        sessionId,
        runId,
        storePath,
        agentCommandMock,
        recorder,
        dispatch,
        completions,
        pending,
        restart,
      } = getFixture();
      const metadataOverlap = holdMetadataThroughSubagentStop({
        sessionKey: `agent:main:subagent:private-descendant-${sequence}`,
        sessionId: `private-descendant-${sequence}-session`,
        storePath,
        signal,
      });
      onTestFinished(() => metadataOverlap.dispose());
      const consumed = createDeferred();
      const release = createDeferred();
      signal.addEventListener("abort", () => release.resolve(), { once: true });
      agentCommandMock.mockImplementationOnce(async (input) => {
        const command = input as AgentCommandOpts;
        await command.onExecutionStarted?.();
        await recorder(input).persistApproved();
        consumed.resolve();
        command.abortSignal!.addEventListener("abort", () => release.resolve(), { once: true });
        await release.promise;
        command.abortSignal!.throwIfAborted();
        throw new Error("operator stop must prevent further work");
      });
      const first = dispatch();
      const observed = first.then(
        (value) => ({ value }),
        (error: unknown) => ({ error }),
      );
      await consumed.promise;
      const descendantRunId = `private-descendant-${sequence}`;
      const childSessionKey = `agent:main:subagent:${descendantRunId}`;
      const childSessionId = `${descendantRunId}-session`;
      const terminalPublished = createDeferred();
      const releaseTerminalWait = () => terminalPublished.resolve();
      signal.addEventListener("abort", releaseTerminalWait, { once: true });
      let observedTerminal = false;
      if (terminalFirst) {
        const acquire = vi.spyOn(
          SubagentLifecycleController.prototype,
          "acquireTerminalCompletionLock",
        );
        acquire.mockRestore();
        const lock = vi
          .spyOn(SubagentLifecycleController.prototype, "acquireTerminalCompletionLock")
          .mockImplementation(async function (this: SubagentLifecycleController, targetRunId) {
            const unlock = await acquire.call(this, targetRunId);
            return () => {
              unlock();
              const entry = subagentRuns.get(targetRunId);
              if (
                targetRunId === descendantRunId &&
                entry?.endedReason === "subagent-killed" &&
                entry.killReconciliation?.taskCancellationAccepted === true
              ) {
                observedTerminal = true;
                terminalPublished.resolve();
              }
            };
          });
        const stop = observeSessionWorkAdmissionDrain(async (params, released) => {
          const identities =
            "target" in params
              ? [params.target.sessionKey, params.target.incarnation, ...params.target.aliases]
              : params.identities;
          if (released && [...identities].includes(childSessionKey)) {
            await terminalPublished.promise;
            signal.throwIfAborted();
          }
        });
        onTestFinished(() => {
          stop();
          lock.mockRestore();
        });
      }
      onTestFinished(() => {
        terminalPublished.resolve();
        signal.removeEventListener("abort", releaseTerminalWait);
      });
      const childStarted = createDeferred();
      const releaseChild = createDeferred();
      let childAbortSignal: AbortSignal | undefined;
      signal.addEventListener("abort", () => releaseChild.resolve(), { once: true });
      await writeSubagentSessionEntry({
        stateDir: process.env.OPENCLAW_STATE_DIR!,
        agentId: "main",
        sessionKey: childSessionKey,
        defaultSessionId: childSessionId,
      });
      agentCommandMock.mockImplementationOnce(async (input) => {
        const command = input as AgentCommandOpts;
        expect(command.runId).toBe(descendantRunId);
        expect(command.sessionId).toBe(childSessionId);
        childAbortSignal = command.abortSignal;
        await command.onExecutionStarted?.();
        await command.userTurnTranscriptRecorder?.persistApproved();
        childStarted.resolve();
        command.abortSignal!.addEventListener("abort", () => releaseChild.resolve(), {
          once: true,
        });
        await releaseChild.promise;
        command.abortSignal!.throwIfAborted();
        throw new Error("operator stop must interrupt the continuation child");
      });
      const child = dispatchGatewayMethodInProcess<Record<string, unknown>>(
        "agent",
        {
          sessionKey: childSessionKey,
          expectedExistingSessionId: childSessionId,
          idempotencyKey: descendantRunId,
          message: "Synthetic continuation child",
          deliver: false,
        },
        {
          expectFinal: true,
          forceSyntheticClient: true,
          operatorRoleActor: { kind: "system" },
          resolveGatewayContext: () => kernel.gatewayRequestContext,
        },
      );
      const observedChild = child.then(
        (value) => ({ value }),
        (error: unknown) => ({ error }),
      );
      const childWaitEntered = createDeferred();
      const waitForAgentJob = agentJobs.waitForAgentJob;
      const waitObservation = vi
        .spyOn(agentJobs, "waitForAgentJob")
        .mockImplementation((params) => {
          const wait = waitForAgentJob(params);
          if (params.runId === descendantRunId) {
            childWaitEntered.resolve();
          }
          return wait;
        });
      // Cancellation must release the admission barrier so cleanup can join both producers.
      const releaseWaitAdmission = () => childWaitEntered.resolve();
      signal.addEventListener("abort", releaseWaitAdmission, { once: true });
      try {
        await Promise.race([
          childStarted.promise,
          observedChild.then(() => {
            throw new Error("continuation child finished before execution started");
          }),
        ]);
        await registerSubagentRun({
          runId: descendantRunId,
          childSessionKey,
          requesterSessionKey: sessionKey,
          requesterAgentId: "main",
          requesterTurnRunId: runId,
          requesterDisplayKey: sessionKey,
          task: "synthetic continuation child",
          cleanup: "keep",
          expectsCompletionMessage: false,
        });
        // Keep both producers live until the child's registration and wait request
        // are admitted; a slow socket handshake must not hide the outstanding wait.
        await expect
          .poll(() =>
            loadSubagentRunsForControllerFromSqlite(sessionKey).some(
              (run) => run.runId === descendantRunId,
            ),
          )
          .toBe(true);
        signal.throwIfAborted();
        await childWaitEntered.promise;
        signal.throwIfAborted();
        expect(
          await kernel.gatewayInstanceRuntime.recovery.dispatchSessionMethod("chat.abort", {
            sessionKey,
            runId,
          }),
        ).toMatchObject({ aborted: true });
        expect(childAbortSignal?.aborted).toBe(true);
        if (terminalFirst) {
          expect(observedTerminal).toBe(true);
        }
        await metadataOverlap.assertCompleted();
        expect(loadSessionEntry(childSessionKey).entry).toMatchObject({
          sessionId: childSessionId,
          label: metadataOverlap.label,
          abortedLastRun: true,
        });
      } finally {
        metadataOverlap.releaseForCleanup();
        try {
          signal.removeEventListener("abort", releaseWaitAdmission);
          waitObservation.mockRestore();
          if (rpcSourceTesting.has(runId)) {
            await kernel.gatewayInstanceRuntime.recovery.dispatchSessionMethod("chat.abort", {
              sessionKey,
              runId,
            });
          }
          if (rpcSourceTesting.has(descendantRunId)) {
            await kernel.gatewayInstanceRuntime.recovery.dispatchSessionMethod("chat.abort", {
              sessionKey: childSessionKey,
              runId: descendantRunId,
            });
          }
          release.resolve();
          releaseChild.resolve();
          await Promise.all([observed, observedChild]);
        } finally {
          await metadataOverlap.dispose();
        }
      }
      await settleSubagentRegistryPersistenceWork();
      expect(await observedChild).toMatchObject({
        value: { status: "timeout", stopReason: "rpc" },
      });
      // The descendant's kill is confirmed once its operation settles (3ae092f8f79): its row
      // retires, and the durable receipt below keeps the cancelled outcome.
      expect(
        loadSubagentRunsForControllerFromSqlite(sessionKey).find(
          (run) => run.runId === descendantRunId,
        ),
      ).toBeUndefined();
      expect(completions()).toMatchObject([{ succeeded: 0 }]);
      expect(JSON.parse(String(completions()[0]?.outcome_json))).toMatchObject({
        reason: "cancelled",
        stopReason: "rpc",
      });
      expect(pending()).toEqual([]);
      expect(await observed).toMatchObject({ value: { status: "timeout", stopReason: "rpc" } });
      // Retire only this run's process projection to exercise the durable receipt.
      // Matching pre-admission Stop cache replay is covered separately above.
      kernel.gatewayRequestContext.dedupe.delete(`agent:${runId}`);
      expect(await dispatch()).toMatchObject({ status: "error", stopReason: "rpc" });
      await restart();
      expect(await dispatch()).toMatchObject({ status: "error", stopReason: "rpc" });
      expect(
        agentCommandMock.mock.calls.map(([input]) => {
          const command = input as AgentCommandOpts;
          return { runId: command.runId, sessionId: command.sessionId };
        }),
      ).toEqual([
        { runId, sessionId },
        { runId: descendantRunId, sessionId: childSessionId },
      ]);
    },
  );
}
