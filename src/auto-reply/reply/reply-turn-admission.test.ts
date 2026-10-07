// Tests reply turn admission decisions for active, queued, and aborted runs.
import { afterEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { SESSION_RESTART_RECOVERY_TOMBSTONE_ERROR_CODE } from "../../config/sessions/lifecycle.js";
import {
  deleteSessionEntryLifecycle,
  loadSessionEntry,
  replaceSessionEntry,
} from "../../config/sessions/session-accessor.js";
import type { InternalSessionEntry as SessionEntry } from "../../config/sessions/types.js";
import { resetDiagnosticRunActivityForTest } from "../../logging/diagnostic-run-activity.js";
import { getSessionControllerOperation } from "../../sessions/session-controller.js";
import {
  interruptSessionControllerEffects,
  runSessionMutation,
} from "../../sessions/session-controller.lifecycle.js";
import { createTestReplyOperation } from "./reply-run-registry.test-helpers.js";
import { testing } from "./reply-run-registry.test-support.js";
import { runWithReplyOperationLifecycleAdmission } from "./reply-turn-admission.js";
import {
  admitTestReplyTurn,
  createSessionStore,
  createSessionStoreFor,
} from "./reply-turn-admission.test-support.js";

const recoveryOwnerReleaseMocks = vi.hoisted(() => ({
  beforeRelease: vi.fn(async () => {}),
  schedulePendingTarget: vi.fn(),
}));

vi.mock(
  "../../agents/main-session-recovery/main-session-recovery-store.js",
  async (importOriginal) => {
    const actual =
      await importOriginal<
        typeof import("../../agents/main-session-recovery/main-session-recovery-store.js")
      >();
    return {
      ...actual,
      releaseMainSessionRecoveryOwner: async (
        lease: Parameters<typeof actual.releaseMainSessionRecoveryOwner>[0],
      ) => {
        await recoveryOwnerReleaseMocks.beforeRelease();
        return await actual.releaseMainSessionRecoveryOwner(lease);
      },
    };
  },
);

vi.mock(
  "../../agents/main-session-recovery/main-session-recovery-owner-release.js",
  async (importOriginal) => ({
    ...(await importOriginal<
      typeof import("../../agents/main-session-recovery/main-session-recovery-owner-release.js")
    >()),
    scheduleMainSessionRecoveryPendingTarget: recoveryOwnerReleaseMocks.schedulePendingTarget,
  }),
);

async function readSessionEntry(
  storePath: string,
  sessionKey: string,
): Promise<SessionEntry | undefined> {
  return loadSessionEntry({ sessionKey, storePath });
}

describe("reply turn admission", () => {
  afterEach(() => {
    testing.resetReplyRunRegistry();
    resetDiagnosticRunActivityForTest();
    recoveryOwnerReleaseMocks.beforeRelease.mockClear();
    recoveryOwnerReleaseMocks.schedulePendingTarget.mockClear();
  });

  it("binds the originating transcript leaf to the admitted operation", async () => {
    const admission = await admitTestReplyTurn({
      sessionKey: "agent:main:main",
      sessionId: "session-originating-leaf",
      originatingLeafEntryId: "leaf-before-run",
    });

    expect(admission.status).toBe("owned");
    if (admission.status === "owned") {
      expect(admission.operation.originatingLeafEntryId).toBe("leaf-before-run");
      admission.operation.complete();
    }
  });

  it("rejects a reply when an archive commits before admission", async () => {
    const sessionKey = "agent:main:telegram:topic:archived";
    const sessionId = "session-before-archive";
    const storePath = createSessionStoreFor(sessionKey, sessionId);
    const mutationStarted = createDeferred();
    const releaseMutation = createDeferred();
    const mutation = runSessionMutation({
      scope: storePath,
      identities: [sessionKey, sessionId],
      run: async () => {
        mutationStarted.resolve();
        await releaseMutation.promise;
        await replaceSessionEntry({ sessionKey, storePath }, {
          sessionId,
          updatedAt: Date.now(),
          archivedAt: Date.now(),
        } as SessionEntry);
      },
    });
    await mutationStarted.promise;

    const admission = admitTestReplyTurn({
      sessionKey,
      sessionId,
      storePath,
    });
    releaseMutation.resolve();
    await mutation;

    await expect(admission).rejects.toThrow(
      `Session "${sessionKey}" is archived. Restore it before starting new work.`,
    );
  });

  it("rejects a reply when deletion commits before admission", async () => {
    const sessionKey = "agent:main:telegram:topic:deleted";
    const sessionId = "session-before-delete";
    const storePath = createSessionStoreFor(sessionKey, sessionId);
    const mutationStarted = createDeferred();
    const releaseMutation = createDeferred();
    const mutation = runSessionMutation({
      scope: storePath,
      identities: [sessionKey, sessionId],
      run: async () => {
        mutationStarted.resolve();
        await releaseMutation.promise;
        await deleteSessionEntryLifecycle({
          storePath,
          archiveTranscript: false,
          target: { canonicalKey: sessionKey, storeKeys: [sessionKey] },
        });
      },
    });
    await mutationStarted.promise;

    const admission = admitTestReplyTurn({
      sessionKey,
      sessionId,
      expectedSessionId: sessionId,
      storePath,
    });
    releaseMutation.resolve();
    await mutation;

    await expect(admission).rejects.toMatchObject({ code: "SESSION_WORK_START_CHANGED" });
  });

  it("uses the persisted session id when reset commits before admission", async () => {
    const sessionKey = "agent:main:telegram:topic:reset";
    const sessionId = "session-before-reset";
    const nextSessionId = "session-after-reset";
    const storePath = createSessionStoreFor(sessionKey, sessionId);
    const mutationStarted = createDeferred();
    const releaseMutation = createDeferred();
    const mutation = runSessionMutation({
      scope: storePath,
      identities: [sessionKey, sessionId],
      run: async () => {
        mutationStarted.resolve();
        await releaseMutation.promise;
        await replaceSessionEntry({ sessionKey, storePath }, {
          sessionId: nextSessionId,
          updatedAt: Date.now(),
        } as SessionEntry);
      },
    });
    await mutationStarted.promise;

    const admission = admitTestReplyTurn({
      sessionKey,
      sessionId,
      storePath,
    });
    releaseMutation.resolve();
    await mutation;
    const result = await admission;

    expect(result.status).toBe("owned");
    if (result.status === "owned") {
      expect(result.operation.sessionId).toBe(nextSessionId);
      result.operation.complete();
    }
  });

  it("rejects expected-session work when reset commits before admission", async () => {
    const sessionKey = "agent:main:telegram:topic:reset-expected";
    const sessionId = "session-before-reset";
    const nextSessionId = "session-after-reset";
    const storePath = createSessionStoreFor(sessionKey, sessionId);
    const mutationStarted = createDeferred();
    const releaseMutation = createDeferred();
    const mutation = runSessionMutation({
      scope: storePath,
      identities: [sessionKey, sessionId],
      run: async () => {
        mutationStarted.resolve();
        await releaseMutation.promise;
        await replaceSessionEntry({ sessionKey, storePath }, {
          sessionId: nextSessionId,
          updatedAt: Date.now(),
        } as SessionEntry);
      },
    });
    await mutationStarted.promise;

    const admission = admitTestReplyTurn({
      sessionKey,
      sessionId,
      expectedSessionId: sessionId,
      storePath,
    });
    releaseMutation.resolve();
    await mutation;

    await expect(admission).rejects.toMatchObject({ code: "SESSION_WORK_START_CHANGED" });
  });

  it("drops queued work when reset cleanup cancels admission", async () => {
    const sessionKey = "agent:main:telegram:topic:queued-reset";
    const sessionId = "session-before-reset";
    const storePath = createSessionStoreFor(sessionKey, sessionId);
    const mutationStarted = createDeferred();
    const releaseMutation = createDeferred();
    const abortController = new AbortController();
    const mutation = runSessionMutation({
      scope: storePath,
      identities: [sessionKey, sessionId],
      run: async () => {
        mutationStarted.resolve();
        await releaseMutation.promise;
        abortController.abort();
        await replaceSessionEntry({ sessionKey, storePath }, {
          sessionId: "session-after-reset",
          updatedAt: Date.now(),
        } as SessionEntry);
      },
    });
    await mutationStarted.promise;

    const admission = admitTestReplyTurn({
      sessionKey,
      sessionId,
      expectedSessionId: sessionId,
      storePath,
      kind: "queued_followup",
      upstreamAbortSignal: abortController.signal,
    });
    releaseMutation.resolve();
    await mutation;

    await expect(admission).resolves.toEqual({
      status: "skipped",
      reason: "aborted",
    });
  });

  it("drops queued work when the session is archived", async () => {
    const sessionKey = "agent:main:telegram:topic:queued-archive";
    const sessionId = "session-before-archive";
    const storePath = createSessionStore({
      [sessionKey]: {
        sessionId,
        updatedAt: Date.now(),
        archivedAt: Date.now(),
      },
    });

    await expect(
      admitTestReplyTurn({
        sessionKey,
        sessionId,
        expectedSessionId: sessionId,
        storePath,
        kind: "queued_followup",
      }),
    ).resolves.toEqual({
      status: "skipped",
      reason: "lifecycle-invalidated",
    });
  });

  it("drops a heartbeat without claiming an owed restart-recovery resend", async () => {
    const sessionKey = "agent:main:telegram:topic:recovery-race:heartbeat";
    const sessionId = "interrupted-session";
    const entry = {
      sessionId,
      updatedAt: 100,
      status: "running" as const,
      abortedLastRun: true,
      mainRestartRecovery: { cycleId: "cycle-1", revision: 1, chargedAttempts: 0 },
    };
    const storePath = createSessionStore({ [sessionKey]: entry });

    await expect(
      admitTestReplyTurn({
        sessionKey,
        sessionId,
        expectedSessionId: sessionId,
        storePath,
        kind: "heartbeat",
      }),
    ).resolves.toEqual({ status: "skipped", reason: "active-run" });
    await expect(readSessionEntry(storePath, sessionKey)).resolves.toMatchObject(entry);
    expect(
      (await readSessionEntry(storePath, sessionKey))?.mainRestartRecovery?.foregroundClaims,
    ).toBeUndefined();
  });

  it.each(["visible", "queued_followup"] as const)(
    "waits for restart-recovery owner release before %s successor admission",
    async (kind) => {
      const sessionKey = `agent:main:telegram:topic:recovery-successor:${kind}`;
      const sessionId = "interrupted-session";
      const storePath = createSessionStore({
        [sessionKey]: {
          sessionId,
          updatedAt: 100,
          status: "running",
          abortedLastRun: true,
          mainRestartRecovery: {
            cycleId: "cycle-1",
            revision: 1,
            chargedAttempts: 0,
          },
        },
      });
      const owner = await admitTestReplyTurn({
        sessionKey,
        sessionId,
        expectedSessionId: sessionId,
        storePath,
      });
      expect(owner.status).toBe("owned");
      if (owner.status !== "owned") {
        return;
      }

      const releaseStarted = createDeferred();
      const allowRelease = createDeferred();
      recoveryOwnerReleaseMocks.beforeRelease.mockImplementationOnce(async () => {
        releaseStarted.resolve();
        await allowRelease.promise;
      });
      owner.operation.complete();
      await releaseStarted.promise;

      const successor = admitTestReplyTurn({
        sessionKey,
        sessionId,
        expectedSessionId: sessionId,
        storePath,
        kind,
      });
      let successorSettled = false;
      void successor.then(() => {
        successorSettled = true;
      });
      await Promise.resolve();
      expect(successorSettled).toBe(false);
      await expect(
        admitTestReplyTurn({
          sessionKey,
          sessionId,
          expectedSessionId: sessionId,
          storePath,
          kind: "heartbeat",
        }),
      ).resolves.toEqual({ status: "skipped", reason: "active-run" });

      allowRelease.resolve();
      const admitted = await successor;
      expect(admitted.status).toBe("owned");
      if (admitted.status === "owned") {
        admitted.operation.complete();
      }
      await vi.waitFor(async () => {
        const entry = await readSessionEntry(storePath, sessionKey);
        expect(entry?.mainRestartRecovery?.foregroundClaims).toBeUndefined();
      });
    },
  );

  it("preserves a source recovery identity after adopting a distinct target session", async () => {
    const sourceSessionKey = "agent:main:telegram:slash:recovery-source";
    const sourceSessionId = "recovery-source-session";
    const targetSessionKey = "agent:main:telegram:group:recovery-target";
    const targetSessionId = "recovery-target-session";
    const storePath = createSessionStore({
      [sourceSessionKey]: {
        sessionId: sourceSessionId,
        updatedAt: 100,
        status: "running",
        abortedLastRun: true,
        mainRestartRecovery: {
          cycleId: "cycle-1",
          revision: 1,
          chargedAttempts: 0,
        },
      },
      [targetSessionKey]: { sessionId: targetSessionId, updatedAt: 100 },
    });
    const source = await admitTestReplyTurn({
      sessionKey: sourceSessionKey,
      sessionId: sourceSessionId,
      expectedSessionId: sourceSessionId,
      storePath,
    });
    expect(source.status).toBe("owned");
    if (source.status !== "owned") {
      return;
    }

    const adoption = await admitTestReplyTurn({
      sessionKey: targetSessionKey,
      sessionId: source.operation.sessionId,
      expectedSessionId: targetSessionId,
      storePath,
      waitForActive: false,
      adoptOperation: source.operation,
    });
    expect(adoption.status).toBe("owned");
    if (adoption.status !== "owned") {
      source.operation.complete();
      return;
    }
    adoption.operation.updateSessionId(targetSessionId);
    expect(adoption.operation).toBe(source.operation);
    expect(adoption.operation.key).toBe(targetSessionKey);
    expect(adoption.operation.sessionId).toBe(targetSessionId);

    const releaseStarted = createDeferred();
    const allowRelease = createDeferred();
    recoveryOwnerReleaseMocks.beforeRelease.mockImplementationOnce(async () => {
      releaseStarted.resolve();
      await allowRelease.promise;
    });
    adoption.operation.complete();
    await releaseStarted.promise;

    const successor = admitTestReplyTurn({
      sessionKey: sourceSessionKey,
      sessionId: sourceSessionId,
      expectedSessionId: sourceSessionId,
      storePath,
    });
    let successorSettled = false;
    void successor.then(() => {
      successorSettled = true;
    });
    await Promise.resolve();
    expect(successorSettled).toBe(false);

    allowRelease.resolve();
    const admitted = await successor;
    expect(admitted.status).toBe("owned");
    if (admitted.status === "owned") {
      expect(admitted.operation.sessionId).toBe(sourceSessionId);
      admitted.operation.complete();
    }
    await vi.waitFor(async () => {
      const entry = await readSessionEntry(storePath, sourceSessionKey);
      expect(entry?.mainRestartRecovery?.foregroundClaims).toBeUndefined();
    });
  });

  it("rejects heartbeat admission for a tombstoned recovery session", async () => {
    const kind = "heartbeat";
    const sessionKey = `agent:main:telegram:topic:recovery-tombstone:${kind}`;
    const sessionId = "tombstoned-session";
    const storePath = createSessionStore({
      [sessionKey]: {
        sessionId,
        updatedAt: 100,
        status: "failed",
        abortedLastRun: false,
        mainRestartRecovery: {
          cycleId: "cycle-1",
          revision: 4,
          chargedAttempts: 3,
          tombstone: { reason: "automatic recovery exhausted" },
        },
      },
    });

    const rejection = await admitTestReplyTurn({
      sessionKey,
      sessionId,
      expectedSessionId: sessionId,
      storePath,
      kind,
    }).catch((error: unknown) => error);

    expect(rejection).toBeInstanceOf(Error);
    expect(rejection).toMatchObject({ code: SESSION_RESTART_RECOVERY_TOMBSTONE_ERROR_CODE });
    expect((rejection as Error).message).toMatch(/ended during restart recovery/i);
  });

  it("admits an explicit reset without reopening its restart tombstone", async () => {
    const sessionKey = "agent:main:matrix:channel:recovery-reset";
    const sessionId = "tombstoned-session";
    const archivedAt = Date.now() - 1_000;
    const storePath = createSessionStore({
      [sessionKey]: {
        sessionId,
        updatedAt: 100,
        archivedAt,
        status: "failed",
        mainRestartRecovery: {
          cycleId: "cycle-1",
          revision: 4,
          chargedAttempts: 3,
          tombstone: {
            reason: "automatic recovery exhausted",
            recoveredSessionId: "dashboard-successor",
            recoveredSessionKey: "agent:main:dashboard:successor",
          },
        },
      },
    });

    const admission = await admitTestReplyTurn({
      sessionKey,
      sessionId,
      expectedSessionId: sessionId,
      storePath,
      resetTriggered: true,
      allowRestartTombstoneReset: true,
    });

    expect(admission.status).toBe("owned");
    expect(await readSessionEntry(storePath, sessionKey)).toMatchObject({
      sessionId,
      archivedAt,
      mainRestartRecovery: {
        tombstone: { recoveredSessionId: "dashboard-successor" },
      },
    });
    if (admission.status === "owned") {
      admission.operation.complete();
    }
  });

  it("does not treat resetTriggered alone as restart-tombstone authority", async () => {
    const sessionKey = "agent:main:matrix:channel:untrusted-reset-flag";
    const sessionId = "tombstoned-session";
    const storePath = createSessionStore({
      [sessionKey]: {
        sessionId,
        updatedAt: 100,
        mainRestartRecovery: {
          cycleId: "cycle-1",
          revision: 4,
          chargedAttempts: 3,
          tombstone: { reason: "automatic recovery exhausted" },
        },
      },
    });

    await expect(
      admitTestReplyTurn({
        sessionKey,
        sessionId,
        expectedSessionId: sessionId,
        storePath,
        resetTriggered: true,
      }),
    ).rejects.toThrow(/ended during restart recovery/i);
  });

  it("admits a visible turn after clearing orphaned restart-recovery fences", async () => {
    const sessionKey = "agent:main:telegram:topic:orphaned-recovery-fence";
    const sessionId = "healthy-session";
    const storePath = createSessionStore({
      [sessionKey]: {
        sessionId,
        updatedAt: 100,
        status: "running",
        abortedLastRun: false,
        restartRecoveryRuns: [{ runId: "stale-run", lifecycleGeneration: "stale-generation" }],
      },
    });

    const admission = await admitTestReplyTurn({
      sessionKey,
      sessionId,
      expectedSessionId: sessionId,
      storePath,
    });
    expect(admission.status).toBe("owned");
    const persisted = await readSessionEntry(storePath, sessionKey);
    expect(persisted?.restartRecoveryRuns).toBeUndefined();
    expect(persisted?.mainRestartRecovery).toBeUndefined();
    if (admission.status === "owned") {
      admission.operation.complete();
    }
  });

  it("drops a queued followup for an admitted recovery fence", async () => {
    const sessionKey = "agent:main:telegram:topic:admitted-recovery";
    const sessionId = "admitted-recovery-session";
    const storePath = createSessionStore({
      [sessionKey]: {
        sessionId,
        updatedAt: 100,
        status: "running",
        abortedLastRun: false,
        restartRecoveryRuns: [{ runId: "recovery-run", lifecycleGeneration: "generation-1" }],
        mainRestartRecovery: {
          cycleId: "cycle-1",
          revision: 3,
          chargedAttempts: 1,
        },
      },
    });

    await expect(
      admitTestReplyTurn({
        sessionKey,
        sessionId,
        expectedSessionId: sessionId,
        storePath,
        kind: "queued_followup",
      }),
    ).resolves.toEqual({ status: "skipped", reason: "lifecycle-invalidated" });
  });

  it("schedules released recovery only after retained admission exits", async () => {
    const sourceSessionKey = "agent:main:telegram:slash:recovery-adoption";
    const sessionKey = "agent:main:telegram:topic:recovery-adoption";
    const sessionId = "interrupted-session";
    const storePath = createSessionStore({
      [sessionKey]: {
        sessionId,
        updatedAt: 100,
        status: "running",
        abortedLastRun: true,
        mainRestartRecovery: {
          cycleId: "cycle-1",
          revision: 1,
          chargedAttempts: 0,
        },
      },
    });
    const blocker = createTestReplyOperation({
      sessionKey,
      sessionId,
    });
    const reservation = createTestReplyOperation({
      sessionKey: sourceSessionKey,
      sessionId: "source-session",
    });

    const result = await admitTestReplyTurn({
      sessionKey,
      sessionId: reservation.sessionId,
      expectedSessionId: sessionId,
      storePath,
      waitForActive: false,
      retainLifecycleAdmissionOnActive: true,
      adoptOperation: reservation,
    });

    expect(result).toMatchObject({ status: "skipped", reason: "active-run" });
    expect(recoveryOwnerReleaseMocks.schedulePendingTarget).not.toHaveBeenCalled();
    await expect(readSessionEntry(storePath, sessionKey)).resolves.not.toHaveProperty(
      "mainRestartRecovery.foregroundClaims",
    );
    if (result.status === "skipped") {
      result.lifecycleAdmission?.release();
    }
    await vi.waitFor(() => {
      expect(recoveryOwnerReleaseMocks.schedulePendingTarget).toHaveBeenCalledWith({
        sessionId,
        sessionKey,
        storePath,
      });
    });

    blocker.complete();
    reservation.complete();
  });

  it("leaves interrupted subagent sessions to the subagent recovery owner", async () => {
    const sessionKey = "agent:main:subagent:child-1";
    const sessionId = "subagent-session";
    const storePath = createSessionStore({
      [sessionKey]: {
        sessionId,
        updatedAt: 100,
        status: "running",
        abortedLastRun: true,
        spawnDepth: 1,
      },
    });

    const admission = await admitTestReplyTurn({
      sessionKey,
      sessionId,
      expectedSessionId: sessionId,
      storePath,
    });

    expect(admission.status).toBe("owned");
    if (admission.status === "owned") {
      admission.operation.complete();
    }
    await expect(readSessionEntry(storePath, sessionKey)).resolves.not.toHaveProperty(
      "mainRestartRecovery",
    );
  });

  it("holds interrupted queued reply work until its owner exits", async () => {
    const sessionKey = "agent:main:telegram:topic:queued-delete";
    const sessionId = "session-before-delete";
    const storePath = createSessionStoreFor(sessionKey, sessionId);
    const admission = await admitTestReplyTurn({
      sessionKey,
      sessionId,
      expectedSessionId: sessionId,
      storePath,
    });
    expect(admission.status).toBe("owned");
    if (admission.status !== "owned") {
      return;
    }

    let mutationRan = false;
    const mutation = runSessionMutation({
      scope: storePath,
      identities: [sessionKey, sessionId],
      prepare: async () => {
        await interruptSessionControllerEffects({
          scope: storePath,
          identities: [sessionKey, sessionId],
        });
      },
      run: async () => {
        mutationRan = true;
      },
    });

    await vi.waitFor(() => {
      expect(admission.operation.abortSignal.aborted).toBe(true);
    });
    expect(admission.operation.result).toEqual({
      kind: "aborted",
      code: "aborted_for_restart",
    });
    expect(mutationRan).toBe(false);
    expect(getSessionControllerOperation(sessionKey)).toBe(admission.operation);

    admission.operation.complete();
    await mutation;
    expect(mutationRan).toBe(true);
    expect(getSessionControllerOperation(sessionKey)).toBeUndefined();
  });

  it("excludes the initiating reply admission from an in-band lifecycle mutation", async () => {
    const sessionKey = "agent:main:telegram:topic:in-band-reset";
    const sessionId = "session-before-reset";
    const storePath = createSessionStoreFor(sessionKey, sessionId);
    const admission = await admitTestReplyTurn({
      sessionKey,
      sessionId,
      expectedSessionId: sessionId,
      storePath,
    });
    expect(admission.status).toBe("owned");
    if (admission.status !== "owned") {
      return;
    }

    await runWithReplyOperationLifecycleAdmission(admission.operation, async () => {
      await runSessionMutation({
        scope: storePath,
        identities: [sessionKey, sessionId],
        prepare: async () => {
          await interruptSessionControllerEffects({
            scope: storePath,
            identities: [sessionKey, sessionId],
          });
        },
        run: async () => undefined,
      });
    });

    expect(admission.operation.abortSignal.aborted).toBe(false);
    admission.operation.complete();
  });

  it("skips an aborted reply waiting behind a lifecycle mutation", async () => {
    const sessionKey = "agent:main:telegram:topic:aborted";
    const sessionId = "session-before-abort";
    const storePath = createSessionStoreFor(sessionKey, sessionId);
    const mutationStarted = createDeferred();
    const releaseMutation = createDeferred();
    const mutation = runSessionMutation({
      scope: storePath,
      identities: [sessionKey, sessionId],
      run: async () => {
        mutationStarted.resolve();
        await releaseMutation.promise;
      },
    });
    await mutationStarted.promise;
    const controller = new AbortController();
    const admission = admitTestReplyTurn({
      sessionKey,
      sessionId,
      storePath,
      upstreamAbortSignal: controller.signal,
    });
    controller.abort();
    releaseMutation.resolve();
    await mutation;

    await expect(admission).resolves.toEqual({ status: "skipped", reason: "aborted" });
  });
  it("adopts a source-keyed command reservation into the target run slot", async () => {
    const sourceSessionKey = "agent:main:telegram:slash:adopt-user";
    const targetSessionKey = "agent:main:telegram:group:adopt-target";
    const targetSessionId = "target-session-adopt";
    const storePath = createSessionStore({
      [targetSessionKey]: { sessionId: targetSessionId, updatedAt: Date.now() },
    });
    const reservation = createTestReplyOperation({
      sessionKey: sourceSessionKey,
      sessionId: "source-reservation-adopt",
    });

    const admission = await admitTestReplyTurn({
      sessionKey: targetSessionKey,
      sessionId: reservation.sessionId,
      expectedSessionId: targetSessionId,
      storePath,
      waitForActive: false,
      adoptOperation: reservation,
    });

    expect(admission.status).toBe("owned");
    if (admission.status !== "owned") {
      return;
    }
    expect(admission.operation).toBe(reservation);
    expect(reservation.key).toBe(targetSessionKey);
    expect(getSessionControllerOperation(sourceSessionKey)).toBeUndefined();
    expect(getSessionControllerOperation(targetSessionKey)).toBe(reservation);

    // Target lifecycle interrupts must reach the adopted operation: reset or
    // delete on the target session interlocks with the continuation run.
    reservation.setPhase("running");
    let mutationRan = false;
    const mutation = runSessionMutation({
      scope: storePath,
      identities: [targetSessionKey, targetSessionId],
      prepare: async () => {
        await interruptSessionControllerEffects({
          scope: storePath,
          identities: [targetSessionKey, targetSessionId],
        });
      },
      run: async () => {
        mutationRan = true;
      },
    });
    await vi.waitFor(() => {
      expect(reservation.abortSignal.aborted).toBe(true);
    });
    expect(reservation.result).toEqual({ kind: "aborted", code: "aborted_for_restart" });
    expect(mutationRan).toBe(false);

    reservation.complete();
    await mutation;
    expect(mutationRan).toBe(true);
  });

  it("skips adoption without waiting when the target run slot is owned", async () => {
    const sourceSessionKey = "agent:main:telegram:slash:busy-user";
    const targetSessionKey = "agent:main:telegram:group:busy-target";
    const targetSessionId = "target-session-busy";
    const storePath = createSessionStore({
      [targetSessionKey]: { sessionId: targetSessionId, updatedAt: Date.now() },
    });
    const blocker = createTestReplyOperation({
      sessionKey: targetSessionKey,
      sessionId: targetSessionId,
    });
    blocker.setPhase("running");
    const reservation = createTestReplyOperation({
      sessionKey: sourceSessionKey,
      sessionId: "source-reservation-busy",
    });

    const admission = await admitTestReplyTurn({
      sessionKey: targetSessionKey,
      sessionId: reservation.sessionId,
      expectedSessionId: targetSessionId,
      storePath,
      waitForActive: false,
      adoptOperation: reservation,
    });

    expect(admission).toMatchObject({
      status: "skipped",
      reason: "active-run",
      activeOperation: blocker,
    });
    // The reservation stays source-keyed so the command turn's own delivery
    // lifecycle is unaffected; queue policy handles the busy target.
    expect(reservation.key).toBe(sourceSessionKey);
    expect(getSessionControllerOperation(sourceSessionKey)).toBe(reservation);
    expect(getSessionControllerOperation(targetSessionKey)).toBe(blocker);
    expect(reservation.result).toBeNull();

    blocker.complete();
    reservation.complete();
  });
});
