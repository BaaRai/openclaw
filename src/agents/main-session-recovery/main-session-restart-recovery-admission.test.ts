import fs from "node:fs/promises";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { useAutoCleanupTempDirTracker } from "../../../test/helpers/temp-dir.js";
import type { InternalSessionEntry } from "../../config/sessions.js";
import {
  loadSessionEntry as loadSessionEntryRaw,
  replaceSessionEntry,
} from "../../config/sessions/session-accessor.js";
import { callGateway } from "../../gateway/call.js";
import type { GatewayRecoveryRuntime } from "../../gateway/server-instance-runtime.types.js";
import { resetAgentEventsForTest } from "../../infra/agent-events.js";
import * as gatewayWorkAdmission from "../../process/gateway-work-admission.js";
import {
  getActiveGatewayRootWorkCount,
  resetGatewayWorkAdmission,
  tryBeginGatewaySuspendAdmission,
} from "../../process/gateway-work-admission.js";
import { captureSessionTarget } from "../../sessions/session-controller.lifecycle.js";
import { getExistingSessionControllerMailbox } from "../../sessions/session-controller.mailbox.js";
import { withEnvAsync } from "../../test-utils/env.js";
import { cleanupSessionStateForTest } from "../../test-utils/session-state-cleanup.js";
import { createRecoveryRuntimeFixture } from "./main-session-recovery-runtime.test-support.js";
import { mainSessionRecoveryLog } from "./main-session-restart-recovery-shared.js";
import {
  retryRestartAbortedMainSessionRecovery,
  scheduleRestartAbortedMainSessionRecovery,
} from "./main-session-restart-recovery.js";

vi.mock("../../gateway/call.js", () => ({
  callGateway: vi.fn(async () => ({ runId: "run-resumed" })),
}));

const tempDirs = useAutoCleanupTempDirTracker(afterEach);
let dispatchSettlement = createDeferred();
const gatewayRuntime = createRecoveryRuntimeFixture({
  callGateway,
  getDispatchSettlement: () => dispatchSettlement.promise,
  sendRecoveryNotice: async () => ({ suppressed: false }),
});

function loadSessionEntry(scope: Parameters<typeof loadSessionEntryRaw>[0]) {
  return loadSessionEntryRaw(scope) as InternalSessionEntry | undefined;
}

/** Reads the durable reservation at the moment the resend's mailbox input settles. */
function observeReservationAtResendSettlement(params: {
  storePath: string;
  sessionKey: string;
  runId: unknown;
}) {
  const input = getExistingSessionControllerMailbox(
    params.sessionKey,
    captureSessionTarget({ storeScope: params.storePath, sessionKey: params.sessionKey }),
  )?.entries.find((entry) => entry.protocolRunId === params.runId);
  expect(input, "the resend must be a mailbox input").toBeDefined();
  return input!.settlement.promise.then(
    () => loadSessionEntry(params)?.mainRestartRecovery?.reservation,
  );
}

function makePendingFinalDelivery(): InternalSessionEntry["pendingFinalDelivery"] {
  return {
    kind: "replayable",
    text: "interrupted response",
    createdAt: Date.now(),
    intentId: "intent-prepared-default",
    deliveries: [{ id: "delivery-prepared-default", state: "prepared" }],
  };
}

describe("startup recovery admission", () => {
  let tmpDir: string;

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(callGateway).mockReset().mockResolvedValue({ runId: "run-resumed" });
    dispatchSettlement = createDeferred();
    resetAgentEventsForTest();
    resetGatewayWorkAdmission();
    tmpDir = tempDirs.make("openclaw-recovery-admission-");
  });

  afterEach(async () => {
    resetGatewayWorkAdmission();
    await cleanupSessionStateForTest({ stateDir: tmpDir });
  });

  async function makeMainSessionFixture(
    overrides: Partial<InternalSessionEntry> & { agentId?: string; sessionKey?: string } = {},
  ) {
    const { agentId = "main", sessionKey = "agent:main:main", ...entry } = overrides;
    const sessionsDir = path.join(tmpDir, "agents", agentId, "sessions");
    const storePath = path.join(sessionsDir, "sessions.json");
    await fs.mkdir(sessionsDir, { recursive: true });
    await replaceSessionEntry(
      { sessionKey, storePath },
      {
        sessionId: "main-session",
        permissionMode: "guarded",
        updatedAt: Date.now() - 10_000,
        status: "running",
        abortedLastRun: true,
        ...entry,
      },
    );
    return { sessionsDir, storePath, sessionKey };
  }

  it("admits each scheduled recovery attempt as independent root work", async () => {
    const { storePath, sessionKey } = await makeMainSessionFixture({
      pendingFinalDelivery: makePendingFinalDelivery(),
    });

    const suspensionRef: {
      current: ReturnType<typeof tryBeginGatewaySuspendAdmission>;
    } = { current: null };
    vi.mocked(callGateway)
      .mockImplementationOnce(async () => {
        expect(getActiveGatewayRootWorkCount()).toBe(1);
        suspensionRef.current = tryBeginGatewaySuspendAdmission(() => {});
        expect(suspensionRef.current?.commit()).toBe(true);
        throw new Error("retry after suspension");
      })
      .mockImplementationOnce(async () => {
        expect(getActiveGatewayRootWorkCount()).toBe(1);
        return { runId: "run-resumed", status: "timeout" };
      })
      .mockImplementationOnce(async () => {
        expect(getActiveGatewayRootWorkCount()).toBe(1);
        return { runId: "run-resumed" };
      });

    const firstAttempt = createDeferred();
    const secondAttempt = createDeferred();
    const admit = gatewayWorkAdmission.runWithGatewayIndependentRootWorkAdmission;
    let attempt = 0;
    const admissionSpy = vi
      .spyOn(gatewayWorkAdmission, "runWithGatewayIndependentRootWorkAdmission")
      .mockImplementation(
        async <T>(run: () => Promise<T>, origin?: string, signal?: AbortSignal) => {
          const settled = attempt++ === 0 ? firstAttempt : secondAttempt;
          try {
            return await admit(run, origin, signal);
          } finally {
            settled.resolve();
          }
        },
      );
    vi.useFakeTimers();
    const recovery = scheduleRestartAbortedMainSessionRecovery({
      getConfig: () => ({}),
      delayMs: 0,
      maxRetries: 2,
      stateDir: tmpDir,
      gatewayRuntime,
    });

    try {
      await firstAttempt.promise;
      expect(callGateway).toHaveBeenCalledTimes(2);
      expect(getActiveGatewayRootWorkCount()).toBe(0);

      await vi.advanceTimersByTimeAsync(5_000);
      expect(callGateway).toHaveBeenCalledTimes(2);
      expect(getActiveGatewayRootWorkCount()).toBe(0);
      expect(suspensionRef.current?.release()).toBe(true);

      await secondAttempt.promise;
      expect(callGateway).toHaveBeenCalledTimes(3);
      const entry = loadSessionEntry({ storePath, sessionKey });
      expect(entry?.abortedLastRun).toBe(false);
      const runIds = vi
        .mocked(callGateway)
        .mock.calls.map(([request]) =>
          request.method === "agent"
            ? (request.params as { idempotencyKey?: unknown }).idempotencyKey
            : undefined,
        )
        .filter((runId) => runId !== undefined);
      expect(new Set(runIds).size).toBe(1);
      expect(getActiveGatewayRootWorkCount()).toBe(0);
    } finally {
      suspensionRef.current?.release();
      await recovery.stop();
      admissionSpy.mockRestore();
      vi.useRealTimers();
    }
  });

  it("keeps a queued resend waiting past the start observation without another attempt", async () => {
    const { storePath, sessionKey } = await makeMainSessionFixture({
      pendingFinalDelivery: makePendingFinalDelivery(),
    });
    const accepted = createDeferred();
    let executionStarted = false;
    let markStarted: (() => void) | undefined;
    // The Gateway reports a queued input's start budget as null (no deadline).
    const queuedRuntime: GatewayRecoveryRuntime = {
      ...gatewayRuntime,
      dispatchAgent: async <T>(
        request: Parameters<GatewayRecoveryRuntime["dispatchAgent"]>[0],
        _timeoutMs?: number,
        options?: Parameters<GatewayRecoveryRuntime["dispatchAgent"]>[2],
      ) => {
        await callGateway({ method: "agent", params: request });
        options?.onStartOwner?.({
          observe: () =>
            executionStarted ? { executionStarted } : { executionStarted, startDeadlineAtMs: null },
          abort: () => false,
        });
        options?.onAccepted?.({ runId: request.idempotencyKey, status: "accepted" });
        markStarted = options?.onExecutionStarted;
        accepted.resolve();
        await dispatchSettlement.promise;
        return { runId: request.idempotencyKey, status: "ok" } as T;
      },
    };
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      const recovery = retryRestartAbortedMainSessionRecovery({
        expectedSessionId: "main-session",
        gatewayRuntime: queuedRuntime,
        sessionKey,
        storePath,
      });
      await accepted.promise;
      await vi.advanceTimersByTimeAsync(60_000);
      executionStarted = true;
      markStarted?.();
      await expect(recovery).resolves.toEqual({ started: 1, settled: 0, failed: 0, skipped: 0 });
      expect(callGateway).toHaveBeenCalledOnce();
      expect(loadSessionEntry({ storePath, sessionKey })?.mainRestartRecovery).toMatchObject({
        chargedAttempts: 1,
      });
    } finally {
      dispatchSettlement.resolve();
      vi.useRealTimers();
    }
  });

  it.for([
    { name: "the final startup retry consumes the last charge", agentId: "main", dirs: ["main"] },
    {
      name: "distinct stores contain the same logical session",
      agentId: "ops",
      dirs: ["ops", " ops "],
    },
  ])("tombstones exhausted targets when $name", async ({ agentId, dirs }, { signal }) => {
    const run = async () => {
      const multipleStores = dirs.length > 1;
      const sessionKey = `agent:${agentId}:main`;
      const targets: Array<{ agentId: string; sessionKey: string; storePath: string }> = [];
      for (const [index, directory] of dirs.entries()) {
        const fixture = await makeMainSessionFixture({
          agentId: directory,
          sessionKey,
          sessionId: multipleStores ? `ops-session-${index}` : "main-session",
          mainRestartRecovery: {
            cycleId: multipleStores ? `cycle-ops-${index}` : "cycle-final-startup-attempt",
            revision: 1,
            chargedAttempts: 2,
          },
          pendingFinalDelivery: makePendingFinalDelivery(),
        });
        targets.push({ agentId, sessionKey, storePath: fixture.storePath });
      }
      const { storePath } = targets[0]!;
      let reservationAtSettlement: Promise<unknown> | undefined;
      if (multipleStores) {
        vi.mocked(callGateway).mockImplementation(async ({ method }) => {
          if (method === "agent") {
            throw new Error("final ambiguous dispatch failure");
          }
          return { status: "timeout" };
        });
      } else {
        vi.mocked(callGateway)
          .mockImplementationOnce(async (request) => {
            reservationAtSettlement = observeReservationAtResendSettlement({
              storePath,
              sessionKey,
              runId: (request.params as { idempotencyKey?: unknown }).idempotencyKey,
            });
            await replaceSessionEntry(
              { sessionKey: "agent:main:fresh", storePath },
              {
                sessionId: "fresh-session",
                updatedAt: Date.now(),
                status: "running",
                abortedLastRun: true,
                mainRestartRecovery: {
                  cycleId: "cycle-fresh-exhausted",
                  revision: 1,
                  chargedAttempts: 3,
                },
              },
            );
            throw new Error("final ambiguous dispatch failure");
          })
          .mockResolvedValueOnce({ runId: "run-resumed" });
      }
      const recovery = scheduleRestartAbortedMainSessionRecovery({
        getConfig: () => ({ agents: { entries: { [agentId]: {} } } }),
        delayMs: 0,
        maxRetries: 1,
        stateDir: tmpDir,
        gatewayRuntime,
      });
      await gatewayRuntime.expectFailedRecovery(2 * targets.length, recovery, signal, ...targets);
      for (const [index, target] of targets.entries()) {
        const entry = loadSessionEntry(target);
        expect(entry).toMatchObject({
          status: "failed",
          mainRestartRecovery: { tombstone: expect.any(Object) },
        });
        if (multipleStores) {
          expect(entry?.mainRestartRecovery?.chargedAttempts).toBe(3);
          expect(entry?.mainRestartRecovery?.reservation).toBeUndefined();
          expect(entry).toMatchObject({ sessionId: `ops-session-${index}`, abortedLastRun: false });
        }
      }
      if (multipleStores) {
        expect(
          vi.mocked(callGateway).mock.calls.filter(([call]) => call.method === "agent"),
        ).toHaveLength(2);
      } else {
        // The failed attempt's rollback commits before its mailbox input settles.
        await expect(reservationAtSettlement).resolves.toBeUndefined();
        const freshEntry = loadSessionEntry({ sessionKey: "agent:main:fresh", storePath });
        expect(freshEntry).toMatchObject({
          sessionId: "fresh-session",
          status: "running",
          abortedLastRun: true,
          mainRestartRecovery: { chargedAttempts: 3 },
        });
        expect(freshEntry?.mainRestartRecovery?.tombstone).toBeUndefined();
      }
    };
    await (dirs.length > 1 ? withEnvAsync({ OPENCLAW_STATE_DIR: tmpDir }, run) : run());
  });

  it("stops exhaustion reconciliation while its Gateway admission is suspended", async () => {
    const { storePath } = await makeMainSessionFixture({
      mainRestartRecovery: {
        cycleId: "cycle-suspended-exhaustion",
        revision: 1,
        chargedAttempts: 2,
      },
      pendingFinalDelivery: makePendingFinalDelivery(),
    });
    const suspension = { lease: null as ReturnType<typeof tryBeginGatewaySuspendAdmission> };
    vi.mocked(callGateway)
      .mockImplementationOnce(async () => {
        suspension.lease = tryBeginGatewaySuspendAdmission(() => {});
        throw new Error("final ambiguous dispatch failure");
      })
      .mockResolvedValueOnce({ runId: "run-resumed" });
    const warn = vi.spyOn(mainSessionRecoveryLog, "warn");
    const reconciliationEntered = createDeferred();
    const admit = gatewayWorkAdmission.runWithGatewayIndependentRootWorkAdmission;
    const admissionSpy = vi
      .spyOn(gatewayWorkAdmission, "runWithGatewayIndependentRootWorkAdmission")
      .mockImplementation(<T>(run: () => Promise<T>, origin?: string, signal?: AbortSignal) => {
        const admitted = admit(run, origin, signal);
        if (origin === "main-session:target-recovery") {
          reconciliationEntered.resolve();
        }
        return admitted;
      });
    const recovery = scheduleRestartAbortedMainSessionRecovery({
      getConfig: () => ({}),
      delayMs: 0,
      maxRetries: 1,
      stateDir: tmpDir,
      gatewayRuntime,
    });
    try {
      await reconciliationEntered.promise;
      expect(suspension.lease).not.toBeNull();
      expect(callGateway).toHaveBeenCalledTimes(2);
      expect(getActiveGatewayRootWorkCount()).toBe(0);
      // Stop must settle while admission remains closed, not after reopening it.
      await recovery.stop();
      expect(suspension.lease?.rollback()).toBe(true);
      expect(callGateway).toHaveBeenCalledTimes(2);
      expect(loadSessionEntry({ sessionKey: "agent:main:main", storePath })).toMatchObject({
        status: "running",
        abortedLastRun: true,
        mainRestartRecovery: { chargedAttempts: 3 },
      });
      expect(warn).not.toHaveBeenCalledWith(
        expect.stringContaining("main-session exhaustion reconciliation failed"),
      );
    } finally {
      suspension.lease?.rollback();
      await recovery.stop();
      admissionSpy.mockRestore();
      warn.mockRestore();
    }
  });
});
