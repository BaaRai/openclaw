import { expect, it, vi } from "vitest";
import { createDeferred } from "../../../../test/helpers/promise.js";
import { tryFastAbortFromMessage } from "../../../auto-reply/reply/abort.js";
import { buildTestCtx } from "../../../auto-reply/reply/test-ctx.js";
import type { OpenClawConfig } from "../../../config/types.openclaw.js";
import { createReplyOperation } from "../../../sessions/session-controller.js";
import { captureSessionTarget } from "../../../sessions/session-controller.lifecycle.js";
import { createSubagentRunRecord } from "../../subagent-test-fixtures.test-helpers.js";
import { enqueueSwarmRun, releaseSwarmRun } from "../swarm/swarm-scheduler.js";
import { testing as swarmSchedulerTesting } from "../swarm/swarm-scheduler.test-support.js";
import { killAllControlledSubagentRuns, killSubagentRunAdmin } from "./subagent-control.js";
import { SUBAGENT_ENDED_REASON_KILLED } from "./subagent-lifecycle-events.js";
import {
  addSubagentRunForTests,
  getSubagentRunByChildSessionKey,
  getSubagentRunByRunId,
} from "./subagent-registry.test-helpers.js";
type ControlRuntime = typeof import("./subagent-control.runtime.js");

export function registerQueueStopControlTests({
  cfgWithSessionStore,
  setSubagentControlDepsForTest,
  writeSessionStoreFixture,
}: {
  cfgWithSessionStore: (storePath?: string) => OpenClawConfig;
  setSubagentControlDepsForTest: (overrides?: Partial<ControlRuntime>) => void;
  writeSessionStoreFixture: (label: string, store: Record<string, unknown>) => Promise<string>;
}) {
  it.each(["bulk", "first cancellation await", "controlled tree", "admin tree", "channel stop"])(
    "does not dispatch selected queued work during %s cancellation",
    async (kind) => {
      const controllerSessionKey = "agent:main:main";
      let running = createSubagentRunRecord({
        runId: "running-collector",
        childSessionKey: "agent:main:subagent:running-collector",
        controllerSessionKey,
        requesterSessionKey: controllerSessionKey,
        task: "running collector",
        collect: true,
        createdAt: 1,
        startedAt: 2,
      });
      let queued = createSubagentRunRecord({
        ...running,
        runId: "queued-collector",
        childSessionKey: "agent:main:subagent:queued-collector",
        controllerSessionKey: kind.endsWith("tree")
          ? running.childSessionKey
          : controllerSessionKey,
        requesterSessionKey: kind.endsWith("tree") ? running.childSessionKey : controllerSessionKey,
        execution: { status: "queued" },
        swarmLaunchPending: true,
      });
      await addSubagentRunForTests(running);
      await addSubagentRunForTests(queued);
      running = getSubagentRunByRunId(running.runId)!;
      queued = getSubagentRunByRunId(queued.runId)!;
      const storePath = await writeSessionStoreFixture("abort-dispatch", {
        [running.childSessionKey]: { sessionId: "running-session", updatedAt: 1 },
      });
      const started: string[] = [];
      for (const runId of [queued.runId, "unselected"]) {
        enqueueSwarmRun({
          groupId: "cancelled-group",
          runId,
          maxConcurrent: 1,
          activeRunIds: [running.runId],
          start: async () => {
            started.push(runId);
          },
          onStartFailure: () => true,
        });
      }
      setSubagentControlDepsForTest({
        clearSessionQueues: () => ({ followupCleared: 0, keys: [] }),
      });
      const controller = {
        controllerSessionKey,
        controllerAgentId: "main",
        callerSessionKey: controllerSessionKey,
        callerIsSubagent: false,
        controlScope: "children" as const,
      };
      const cfg = cfgWithSessionStore(storePath);
      const parent =
        kind === "channel stop"
          ? createReplyOperation({
              sessionKey: controllerSessionKey,
              sessionId: "parent-session",
              resetTriggered: false,
              target: captureSessionTarget({
                storeScope: storePath,
                sessionKey: controllerSessionKey,
                incarnation: "parent-session",
              }),
            })
          : undefined;
      const parentFinish = createDeferred();
      const parentProducer = parent
        ? (async () => {
            try {
              await parentFinish.promise;
            } finally {
              parent.complete();
            }
          })()
        : undefined;
      parent?.attachBackend({
        kind: "embedded",
        cancel: () => {
          expect(releaseSwarmRun(running.runId)).toBe(true);
          parentFinish.resolve();
        },
        isStreaming: () => true,
      });
      try {
        if (kind === "first cancellation await") {
          const cancellation = killAllControlledSubagentRuns({
            cfg,
            controller,
            runs: [running, queued],
          });
          // Natural terminal cleanup calls this same capacity owner while kill
          // admission is pending; no synthetic execution outcome is needed.
          expect(releaseSwarmRun(running.runId)).toBe(true);
          expect(await cancellation).toMatchObject({ status: "ok", killed: 2 });
        } else if (kind === "bulk") {
          expect(
            await killAllControlledSubagentRuns({ cfg, controller, runs: [running, queued] }),
          ).toMatchObject({ status: "ok", killed: 2 });
        } else if (kind === "controlled tree") {
          expect(
            await killAllControlledSubagentRuns({ cfg, controller, runs: [running] }),
          ).toMatchObject({ status: "ok", killed: 2 });
        } else if (kind === "admin tree") {
          expect(
            await killSubagentRunAdmin({
              cfg,
              sessionKey: running.childSessionKey,
              expectedRunId: running.runId,
              expectedGeneration: running.generation,
              expectedOwnerKey: controllerSessionKey,
            }),
          ).toMatchObject({ found: true, killed: true, cascadeKilled: 1 });
        } else {
          expect(
            await tryFastAbortFromMessage({
              cfg,
              ctx: buildTestCtx({
                CommandBody: "/stop",
                RawBody: "/stop",
                CommandAuthorized: true,
                Provider: "telegram",
                Surface: "telegram",
                SessionKey: controllerSessionKey,
                From: "telegram:queue-owner",
                To: "telegram:queue-owner",
              }),
            }),
          ).toMatchObject({ handled: true, stoppedSubagents: 2, failedSubagents: 0 });
          expect(parent?.abortSignal.aborted).toBe(true);
        }
        if (kind !== "channel stop" && kind !== "first cancellation await") {
          // No live collector run exists here; release capacity as its terminal cleanup would.
          expect(releaseSwarmRun(running.runId)).toBe(true);
        }
        for (const entry of [running, queued]) {
          expect(getSubagentRunByChildSessionKey(entry.childSessionKey)).toMatchObject({
            execution: { status: "terminal" },
            endedReason: SUBAGENT_ENDED_REASON_KILLED,
          });
        }
        expect(
          started,
          "selected queued child must never dispatch during cancellation",
        ).not.toContain(queued.runId);
        await vi.waitFor(() => expect(started).toEqual(["unselected"]));
      } finally {
        parentFinish.resolve();
        await parentProducer;
        swarmSchedulerTesting.reset();
      }
    },
  );
}
