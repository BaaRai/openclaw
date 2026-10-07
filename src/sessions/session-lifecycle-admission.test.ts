// Tests lifecycle/work admission ordering across canonical keys and backing ids.
import { setImmediate as waitForImmediate } from "node:timers/promises";
import { importFreshModule } from "openclaw/plugin-sdk/test-fixtures";
import { expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import { runExclusiveSessionStoreWrite } from "../config/sessions/store-writer.js";
import {
  resetGatewayWorkAdmission,
  tryBeginGatewayRootWorkAdmission,
  tryBeginGatewaySuspendAdmission,
} from "../process/gateway-work-admission.js";
import {
  beginSessionEffect,
  cancelSessionEffectHandoff,
  consumeSessionEffectHandoff,
  getSessionMutationCount,
  getSessionControllerWorkCount,
  captureSessionControllerSettlement,
  hasOnlySessionMutationKindActive,
  interruptSessionControllerEffects,
  isCompetingSessionControllerWorkActive,
  isSessionMutationActive,
  isSessionControllerWorkActive,
  runSessionMutation,
} from "./session-controller.lifecycle.js";

it("counts one multi-identity admission once", async () => {
  const admission = await beginSessionEffect({
    scope: "store-count",
    identities: ["agent:main:child", "session-count"],
    assertAllowed: () => {},
  });
  try {
    expect(getSessionControllerWorkCount()).toBe(1);
  } finally {
    admission.release();
  }
  expect(getSessionControllerWorkCount()).toBe(0);
});

it("waits for a competing session admission outside the caller context", async () => {
  const scope = "store-competing-self-archive";
  const sessionKey = "agent:main:competing-self-archive";
  const admission = await beginSessionEffect({
    scope,
    identities: [sessionKey, "competing-session"],
    assertAllowed: () => {},
  });
  let settled = false;
  let release: Promise<void> | undefined;

  try {
    release = captureSessionControllerSettlement({ scope, identities: [sessionKey] });
    expect(release).toBeInstanceOf(Promise);
    void release?.then(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);
  } finally {
    admission.release();
  }

  await release;
  await Promise.resolve();
  expect(settled).toBe(true);
});

it("atomically hands admitted work across an interrupted RPC boundary", async () => {
  const scope = "store-rpc-handoff";
  const identities = ["agent:main:main", "session-rpc-handoff"];
  const admission = await beginSessionEffect({
    scope,
    identities,
    assertAllowed: () => {},
  });
  const handoffId = admission.createHandoff();
  const mutationStarted = createDeferred();
  let mutationRan = false;
  const mutation = runSessionMutation({
    scope,
    identities,
    prepare: async () => {
      mutationStarted.resolve();
      expect(await interruptSessionControllerEffects({ scope, identities, timeoutMs: 1_000 })).toBe(
        true,
      );
    },
    run: async () => {
      mutationRan = true;
    },
  });
  await mutationStarted.promise;
  let interrupted = false;
  const adopted = consumeSessionEffectHandoff({
    handoffId,
    scope,
    identities,
    onInterrupt: () => {
      interrupted = true;
    },
  });

  try {
    expect(adopted).toBe(admission);
    expect(interrupted).toBe(true);
    expect(cancelSessionEffectHandoff(handoffId)).toBe(false);
    expect(mutationRan).toBe(false);
  } finally {
    adopted?.release();
    admission.release();
    await mutation;
  }
  expect(mutationRan).toBe(true);
});

it("keeps an admission handoff bound to its original identities", async () => {
  const admission = await beginSessionEffect({
    scope: "store-bound-handoff",
    identities: ["agent:main:main", "session-bound-handoff"],
    assertAllowed: () => {},
  });
  const handoffId = admission.createHandoff();

  expect(
    consumeSessionEffectHandoff({
      handoffId,
      scope: "store-bound-handoff",
      identities: ["agent:main:other", "session-bound-handoff"],
    }),
  ).toBeUndefined();
  expect(cancelSessionEffectHandoff(handoffId)).toBe(true);
  expect(isSessionControllerWorkActive("store-bound-handoff", ["session-bound-handoff"])).toBe(
    false,
  );
});

it("counts one multi-identity lifecycle mutation once across module instances", async () => {
  const first = await importFreshModule<typeof import("./session-controller.lifecycle.js")>(
    import.meta.url,
    "./session-controller.lifecycle.js?scope=session-mutation-count-a",
  );
  const second = await importFreshModule<typeof import("./session-controller.lifecycle.js")>(
    import.meta.url,
    "./session-controller.lifecycle.js?scope=session-mutation-count-b",
  );
  const mutationStarted = createDeferred();
  const releaseMutation = createDeferred();
  const mutation = first.runSessionMutation({
    scope: "store-mutation-count",
    identities: ["agent:main:child", "session-mutation-count"],
    run: async () => {
      mutationStarted.resolve();
      await releaseMutation.promise;
    },
  });
  await mutationStarted.promise;

  try {
    expect(first.getSessionMutationCount()).toBe(1);
    expect(second.getSessionMutationCount()).toBe(1);
  } finally {
    releaseMutation.resolve();
    await mutation;
  }
  expect(second.getSessionMutationCount()).toBe(0);
});

it("keeps a same-identity mutation queued until finalization completes", async () => {
  const target = { scope: "store-finalize-order", identities: ["session-finalize-order"] };
  const finalizeStarted = createDeferred();
  const releaseFinalize = createDeferred();
  let secondRan = false;
  const first = runSessionMutation({
    ...target,
    run: async () => {},
    finalize: async () => {
      finalizeStarted.resolve();
      await releaseFinalize.promise;
    },
  });
  await finalizeStarted.promise;

  const second = runSessionMutation({
    ...target,
    run: async () => {
      secondRan = true;
    },
  });
  await waitForImmediate();
  expect(secondRan).toBe(false);

  releaseFinalize.resolve();
  await Promise.all([first, second]);
});

it("finalizes a lifecycle mutation when its run throws", async () => {
  const runError = new Error("lifecycle run failed");
  const finalize = vi.fn(async () => {});

  await expect(
    runSessionMutation({
      scope: "store-finalize-run-error",
      identities: ["session-finalize-run-error"],
      run: async () => {
        throw runError;
      },
      finalize,
    }),
  ).rejects.toBe(runError);
  expect(finalize).toHaveBeenCalledOnce();
});

it("releases lifecycle state when finalization throws", async () => {
  const target = { scope: "store-finalize-error", identities: ["session-finalize-error"] };
  const finalizeError = new Error("lifecycle finalizer failed");

  await expect(
    runSessionMutation({
      ...target,
      run: async () => {},
      finalize: async () => {
        throw finalizeError;
      },
    }),
  ).rejects.toBe(finalizeError);
  expect(isSessionMutationActive(target.scope, target.identities)).toBe(false);
  await expect(runSessionMutation({ ...target, run: async () => "next" })).resolves.toBe("next");
});

it("counts a cross-store lifecycle mutation once and fences every target", async () => {
  const mutationStarted = createDeferred();
  const releaseMutation = createDeferred();
  const mutation = runSessionMutation({
    targets: [
      {
        scope: "store-cross-count-b",
        identities: ["agent:work:main", "session-cross-count-b"],
      },
      {
        scope: "store-cross-count-a",
        identities: ["agent:main:main", "session-cross-count-a"],
      },
      {
        scope: "store-cross-count-a",
        identities: ["session-cross-count-a", undefined],
      },
    ],
    run: async () => {
      mutationStarted.resolve();
      await releaseMutation.promise;
    },
  });
  await mutationStarted.promise;

  try {
    expect(getSessionMutationCount()).toBe(1);
    expect(isSessionMutationActive("store-cross-count-a", ["agent:main:main"])).toBe(true);
    expect(isSessionMutationActive("store-cross-count-b", ["session-cross-count-b"])).toBe(true);
    expect(isSessionMutationActive("store-cross-count-b", ["agent:main:main"])).toBe(false);
  } finally {
    releaseMutation.resolve();
    await mutation;
  }

  expect(getSessionMutationCount()).toBe(0);
  expect(isSessionMutationActive("store-cross-count-a", ["session-cross-count-a"])).toBe(false);
  expect(isSessionMutationActive("store-cross-count-b", ["session-cross-count-b"])).toBe(false);
});

it("serializes opposite-direction cross-store lifecycle mutations", async () => {
  const main = {
    scope: "store-cross-order-a",
    identities: ["agent:main:main", "session-cross-order-a"],
  };
  const work = {
    scope: "store-cross-order-b",
    identities: ["agent:work:main", "session-cross-order-b"],
  };
  let activeMutations = 0;
  let maximumActiveMutations = 0;
  let completedMutations = 0;

  await Promise.all(
    Array.from({ length: 48 }, async (_, index) =>
      runSessionMutation({
        targets: index % 2 === 0 ? [main, work] : [work, main],
        run: async () => {
          activeMutations += 1;
          maximumActiveMutations = Math.max(maximumActiveMutations, activeMutations);
          try {
            await Promise.resolve();
            completedMutations += 1;
          } finally {
            activeMutations -= 1;
          }
        },
      }),
    ),
  );

  expect(completedMutations).toBe(48);
  expect(maximumActiveMutations).toBe(1);
  expect(activeMutations).toBe(0);
  expect(getSessionMutationCount()).toBe(0);
});

it("interrupts admitted work in both stores before a cross-store mutation", async () => {
  const mainTarget = {
    scope: "store-cross-interrupt-a",
    identities: ["agent:main:main", "session-cross-interrupt-a"],
  };
  const workTarget = {
    scope: "store-cross-interrupt-b",
    identities: ["agent:work:main", "session-cross-interrupt-b"],
  };
  let mainInterrupted = false;
  let workInterrupted = false;
  const mainAdmission = await beginSessionEffect({
    ...mainTarget,
    assertAllowed: () => {},
    onInterrupt: () => {
      mainInterrupted = true;
      mainAdmission.release();
    },
  });
  const workAdmission = await beginSessionEffect({
    ...workTarget,
    assertAllowed: () => {},
    onInterrupt: () => {
      workInterrupted = true;
      workAdmission.release();
    },
  });

  try {
    await runSessionMutation({
      targets: [workTarget, mainTarget],
      prepare: async () => {
        const interrupted = await Promise.all([
          interruptSessionControllerEffects({ ...mainTarget, timeoutMs: 1_000 }),
          interruptSessionControllerEffects({ ...workTarget, timeoutMs: 1_000 }),
        ]);
        expect(interrupted).toEqual([true, true]);
      },
      run: async () => {
        expect(mainInterrupted).toBe(true);
        expect(workInterrupted).toBe(true);
        expect(isSessionControllerWorkActive(mainTarget.scope, mainTarget.identities)).toBe(false);
        expect(isSessionControllerWorkActive(workTarget.scope, workTarget.identities)).toBe(false);
      },
    });
  } finally {
    mainAdmission.release();
    workAdmission.release();
  }
});

it("cancels an opposite-direction cross-store mutation before activation", async () => {
  const main = {
    scope: "store-cross-cancel-a",
    identities: ["agent:main:main", "session-cross-cancel-a"],
  };
  const work = {
    scope: "store-cross-cancel-b",
    identities: ["agent:work:main", "session-cross-cancel-b"],
  };
  const mutationStarted = createDeferred();
  const releaseMutation = createDeferred();
  const first = runSessionMutation({
    targets: [main, work],
    run: async () => {
      mutationStarted.resolve();
      await releaseMutation.promise;
    },
  });
  await mutationStarted.promise;

  const controller = new AbortController();
  const abortError = new Error("cancel queued cross-store lifecycle mutation");
  let cancelledMutationRan = false;
  const cancelled = runSessionMutation({
    targets: [work, main],
    signal: controller.signal,
    run: async () => {
      cancelledMutationRan = true;
    },
  });
  controller.abort(abortError);

  try {
    await expect(cancelled).rejects.toBe(abortError);
  } finally {
    releaseMutation.resolve();
    await first;
  }

  expect(cancelledMutationRan).toBe(false);
  expect(getSessionMutationCount()).toBe(0);
});

it("rejects an admission that resumes after suspension closes the async gap", async () => {
  resetGatewayWorkAdmission();
  const mutationStarted = createDeferred();
  const releaseMutation = createDeferred();
  const mutation = runSessionMutation({
    scope: "store-suspend-race",
    identities: ["session-suspend-race", "backing-suspend-race"],
    run: async () => {
      mutationStarted.resolve();
      await releaseMutation.promise;
    },
  });
  await mutationStarted.promise;
  expect(getSessionMutationCount()).toBeGreaterThan(0);

  const admission = beginSessionEffect({
    scope: "store-suspend-race",
    identities: ["session-suspend-race", "backing-suspend-race"],
    assertAllowed: () => {},
  });
  const suspension = tryBeginGatewaySuspendAdmission(() => {});
  expect(suspension?.commit()).toBe(true);
  releaseMutation.resolve();
  await mutation;
  expect(getSessionMutationCount()).toBe(0);

  await expect(admission).rejects.toMatchObject({ name: "GatewayDrainingError" });
  expect(getSessionControllerWorkCount()).toBe(0);
  suspension?.release();
  resetGatewayWorkAdmission();
});

it("lets an admitted root enter session work while suspension preparation refuses new roots", async () => {
  resetGatewayWorkAdmission();
  const continueRoot = createDeferred();
  const root = tryBeginGatewayRootWorkAdmission();
  expect(root).not.toBeNull();
  const active = root?.run(async () => {
    await continueRoot.promise;
    const admission = await beginSessionEffect({
      scope: "store-admitted-root",
      identities: ["session-admitted-root"],
      assertAllowed: () => {},
    });
    admission.release();
  });
  const suspension = tryBeginGatewaySuspendAdmission(() => {});

  try {
    continueRoot.resolve();
    await expect(active).resolves.toBeUndefined();
    await expect(
      beginSessionEffect({
        scope: "store-new-root",
        identities: ["session-new-root"],
        assertAllowed: () => {},
      }),
    ).rejects.toMatchObject({ name: "GatewayDrainingError" });
  } finally {
    suspension?.rollback();
    root?.release();
    resetGatewayWorkAdmission();
  }
});

it("revalidates inline when admission begins inside the active store writer", async () => {
  const storePath = "store-writer-reentrant-admission";
  const order: string[] = [];
  const admission = await runExclusiveSessionStoreWrite(storePath, async () => {
    order.push("writer:start");
    const lease = await beginSessionEffect({
      scope: storePath,
      identities: ["session-writer-reentrant-admission"],
      assertAllowed: () => {
        order.push("validate");
      },
    });
    order.push("writer:end");
    return lease;
  });

  try {
    expect(order).toEqual(["writer:start", "validate", "validate", "writer:end"]);
    expect(isSessionControllerWorkActive(storePath, ["session-writer-reentrant-admission"])).toBe(
      true,
    );
  } finally {
    admission.release();
  }
});

it("runs one-time admission work only during writer-barrier revalidation", async () => {
  let initialChecks = 0;
  let finalChecks = 0;
  const admission = await beginSessionEffect({
    scope: "store-dedicated-revalidation",
    identities: ["session-dedicated-revalidation"],
    assertAllowed: () => {
      initialChecks += 1;
    },
    revalidateAllowed: () => {
      finalChecks += 1;
    },
  });

  try {
    expect(initialChecks).toBe(1);
    expect(finalChecks).toBe(1);
  } finally {
    admission.release();
  }
});

it.each([false, true])(
  "excludes its own lease during revalidation while retaining competing work (%s)",
  async (hasCompetingWork) => {
    const target = {
      scope: "store-revalidation-owner",
      identities: ["agent:main:main", "session-revalidation-owner"],
    };
    const competing = hasCompetingWork
      ? await beginSessionEffect({ ...target, assertAllowed: () => {} })
      : undefined;
    try {
      const admission = await beginSessionEffect({
        ...target,
        assertAllowed: () => {},
        revalidateAllowed: async () => {
          await Promise.resolve();
          expect(isSessionControllerWorkActive(target.scope, target.identities)).toBe(true);
          expect(isCompetingSessionControllerWorkActive(target.scope, target.identities)).toBe(
            hasCompetingWork,
          );
        },
      });
      try {
        expect(isCompetingSessionControllerWorkActive(target.scope, target.identities)).toBe(true);
      } finally {
        admission.release();
      }
      expect(isSessionControllerWorkActive(target.scope, target.identities)).toBe(hasCompetingWork);
    } finally {
      competing?.release();
    }
  },
);

it("rejects and releases an admission invalidated by an earlier store writer", async () => {
  const storePath = "store-writer-revalidation";
  const writerStarted = createDeferred();
  const releaseWriter = createDeferred();
  const firstValidation = createDeferred();
  let allowed = true;
  let validationCount = 0;
  const writer = runExclusiveSessionStoreWrite(storePath, async () => {
    writerStarted.resolve();
    await releaseWriter.promise;
    allowed = false;
  });
  await writerStarted.promise;

  const admission = beginSessionEffect({
    scope: storePath,
    identities: ["agent:main:child", "session-writer-revalidation"],
    assertAllowed: () => {
      validationCount += 1;
      if (validationCount === 1) {
        firstValidation.resolve();
      }
      if (!allowed) {
        throw new Error("session changed");
      }
    },
  });
  const outcome = expect(admission).rejects.toThrow("session changed");
  try {
    await firstValidation.promise;
    // Drain microtasks so the validated admission has queued behind the held writer.
    await waitForImmediate();
    expect(isSessionControllerWorkActive(storePath, ["session-writer-revalidation"])).toBe(true);
  } finally {
    releaseWriter.resolve();
  }
  await writer;
  await outcome;
  expect(validationCount).toBe(2);
  expect(isSessionControllerWorkActive(storePath, ["session-writer-revalidation"])).toBe(false);
});

it("admits an independent session while revalidating a conflicting writer's authority", async () => {
  const scope = "store-keyed-admission";
  const blockedKey = "agent:main:blocked";
  const independentKey = "agent:main:independent";
  const releaseWriter = createDeferred();
  const initialValidation = createDeferred();
  const independentEntered = createDeferred();
  const releaseIndependent = createDeferred();
  let allowed = true;
  const validateBlocked = vi.fn(() => {
    if (!allowed) {
      throw new Error("session authority revoked");
    }
  });
  const writer = runExclusiveSessionStoreWrite(
    scope,
    async () => {
      await releaseWriter.promise;
      allowed = false;
    },
    { identities: [blockedKey] },
  );
  const blocked = beginSessionEffect({
    scope,
    identities: [blockedKey, "blocked-id"],
    storeWriterIdentities: [blockedKey],
    assertAllowed: () => {
      validateBlocked();
      initialValidation.resolve();
    },
    revalidateAllowed: validateBlocked,
  });
  const blockedOutcome = expect(blocked).rejects.toThrow("session authority revoked");
  await initialValidation.promise;
  const independent = beginSessionEffect({
    scope,
    identities: [independentKey, "independent-id"],
    storeWriterIdentities: [independentKey],
    assertAllowed: () => {},
    revalidateAllowed: async () => {
      independentEntered.resolve();
      await releaseIndependent.promise;
    },
  });

  try {
    await independentEntered.promise;
    expect(validateBlocked).toHaveBeenCalledTimes(1);
    releaseWriter.resolve();
    await writer;
    await blockedOutcome;
    expect(validateBlocked).toHaveBeenCalledTimes(2);
    expect(isSessionControllerWorkActive(scope, [blockedKey])).toBe(false);
    expect(isSessionControllerWorkActive(scope, [independentKey])).toBe(true);
    releaseIndependent.resolve();
    const lease = await independent;
    lease.release();
  } finally {
    releaseWriter.resolve();
    releaseIndependent.resolve();
    const results = await Promise.allSettled([blocked, independent]);
    for (const result of results) {
      if (result.status === "fulfilled") {
        result.value.release();
      }
    }
    await Promise.allSettled([writer, blockedOutcome]);
  }
});

it("releases an admission aborted while waiting for the store writer barrier", async () => {
  const storePath = "store-writer-abort";
  const writerStarted = createDeferred();
  const releaseWriter = createDeferred();
  const firstValidation = createDeferred();
  const controller = new AbortController();
  const abortError = new Error("admission aborted behind writer");
  const writer = runExclusiveSessionStoreWrite(storePath, async () => {
    writerStarted.resolve();
    await releaseWriter.promise;
  });
  await writerStarted.promise;

  const admission = beginSessionEffect({
    scope: storePath,
    identities: ["session-writer-abort"],
    signal: controller.signal,
    assertAllowed: () => {
      firstValidation.resolve();
    },
  });
  await firstValidation.promise;
  controller.abort(abortError);

  await expect(admission).rejects.toBe(abortError);
  expect(isSessionControllerWorkActive(storePath, ["session-writer-abort"])).toBe(false);

  releaseWriter.resolve();
  await writer;
});

it("revalidates without inheriting a released gateway root from the writer queue", async () => {
  resetGatewayWorkAdmission();
  const storePath = "store-released-gateway-root";
  const writerStarted = createDeferred();
  const releaseWriter = createDeferred();
  const firstValidation = createDeferred();
  const root = tryBeginGatewayRootWorkAdmission();
  expect(root).not.toBeNull();
  if (!root) {
    throw new Error("gateway root admission unavailable");
  }
  const writer = root.run(
    async () =>
      await runExclusiveSessionStoreWrite(storePath, async () => {
        writerStarted.resolve();
        await releaseWriter.promise;
      }),
  );
  await writerStarted.promise;

  let validationCount = 0;
  const admissionPromise = beginSessionEffect({
    scope: storePath,
    identities: ["session-released-gateway-root"],
    assertAllowed: () => {
      validationCount += 1;
      if (validationCount === 1) {
        firstValidation.resolve();
      }
    },
  });
  await firstValidation.promise;

  root.release();
  releaseWriter.resolve();
  const admission = await admissionPromise;
  try {
    expect(validationCount).toBe(2);
  } finally {
    admission.release();
    await writer;
    resetGatewayWorkAdmission();
  }
});

it("serializes lifecycle mutation and work admission across identity aliases", async () => {
  const mutationStarted = createDeferred();
  const releaseMutation = createDeferred();
  const mutation = runSessionMutation({
    scope: "store-a",
    identities: ["agent:main:child", "session-1"],
    run: async () => {
      mutationStarted.resolve();
      await releaseMutation.promise;
    },
  });
  let admitted = false;
  const admission = beginSessionEffect({
    scope: "store-a",
    identities: ["session-1"],
    assertAllowed: () => {
      admitted = true;
    },
  });
  try {
    await mutationStarted.promise;
    expect(admitted).toBe(false);

    releaseMutation.resolve();
    await mutation;
    await admission;
    expect(admitted).toBe(true);
    expect(isSessionControllerWorkActive("store-a", ["agent:main:child", "session-1"])).toBe(true);
  } finally {
    releaseMutation.resolve();
    await mutation;
    (await admission).release();
  }
  expect(isSessionControllerWorkActive("store-a", ["session-1"])).toBe(false);
});

it("tracks the active lifecycle mutation kind across identity aliases", async () => {
  const mutationStarted = createDeferred();
  const releaseMutation = createDeferred();
  const mutation = runSessionMutation({
    scope: "store-kind",
    identities: ["agent:main:child", "session-kind"],
    kind: "compaction",
    run: async () => {
      mutationStarted.resolve();
      await releaseMutation.promise;
    },
  });
  await mutationStarted.promise;

  expect(hasOnlySessionMutationKindActive("store-kind", ["session-kind"], "compaction")).toBe(true);
  expect(hasOnlySessionMutationKindActive("store-other", ["session-kind"], "compaction")).toBe(
    false,
  );

  releaseMutation.resolve();
  await mutation;
  expect(hasOnlySessionMutationKindActive("store-kind", ["session-kind"], "compaction")).toBe(
    false,
  );
});

it("keeps identical session keys isolated by store", async () => {
  const admissionLease = await beginSessionEffect({
    scope: "store-a",
    identities: ["global", "session-a"],
    assertAllowed: () => {},
  });

  try {
    expect(isSessionControllerWorkActive("store-a", ["global"])).toBe(true);
    expect(isSessionControllerWorkActive("store-b", ["global"])).toBe(false);
    let storeBMutationRan = false;
    await runSessionMutation({
      scope: "store-b",
      identities: ["global"],
      run: async () => {
        storeBMutationRan = true;
      },
    });
    expect(storeBMutationRan).toBe(true);
  } finally {
    admissionLease.release();
  }
});

it("cancels work admission waiting behind a lifecycle mutation", async () => {
  const mutationPrepared = createDeferred();
  const releaseMutation = createDeferred();
  const mutation = runSessionMutation({
    scope: "store-a",
    identities: ["agent:main:child", "session-1"],
    prepare: async () => {
      mutationPrepared.resolve();
      await releaseMutation.promise;
    },
    run: async () => {},
  });
  await mutationPrepared.promise;

  const controller = new AbortController();
  const abortError = new Error("reset interrupted admission");
  const admission = beginSessionEffect({
    scope: "store-a",
    identities: ["session-1"],
    signal: controller.signal,
    assertAllowed: () => {},
  });
  controller.abort(abortError);

  await expect(admission).rejects.toBe(abortError);
  releaseMutation.resolve();
  await mutation;
});

it("cancels a queued lifecycle mutation before it becomes active", async () => {
  const firstStarted = createDeferred();
  const releaseFirst = createDeferred();
  const first = runSessionMutation({
    scope: "store-a",
    identities: ["agent:main:child", "session-1"],
    run: async () => {
      firstStarted.resolve();
      await releaseFirst.promise;
    },
  });
  await firstStarted.promise;

  const controller = new AbortController();
  const abortError = new Error("cancel queued lifecycle mutation");
  let cancelledMutationRan = false;
  const cancelled = runSessionMutation({
    scope: "store-a",
    identities: ["agent:main:child", "session-1"],
    signal: controller.signal,
    run: async () => {
      cancelledMutationRan = true;
    },
  });
  controller.abort(abortError);

  await expect(cancelled).rejects.toBe(abortError);
  releaseFirst.resolve();
  await first;
  await runSessionMutation({
    scope: "store-a",
    identities: ["agent:main:child", "session-1"],
    run: async () => {},
  });
  expect(cancelledMutationRan).toBe(false);
});

it("preserves the initiating admission across a queued lifecycle mutation", async () => {
  let selfInterrupted = false;
  const admission = await beginSessionEffect({
    scope: "store-a",
    identities: ["agent:main:child", "session-1"],
    assertAllowed: () => {},
    onInterrupt: () => {
      selfInterrupted = true;
    },
  });
  const firstStarted = createDeferred();
  const releaseFirst = createDeferred();
  const first = runSessionMutation({
    scope: "store-a",
    identities: ["agent:main:child", "session-1"],
    run: async () => {
      firstStarted.resolve();
      await releaseFirst.promise;
    },
  });
  await firstStarted.promise;

  let initiatingAdmissionExcluded = false;
  const queued = admission.run(
    async () =>
      await runSessionMutation({
        scope: "store-a",
        identities: ["agent:main:child", "session-1"],
        prepare: async () => {
          initiatingAdmissionExcluded = await interruptSessionControllerEffects({
            scope: "store-a",
            identities: ["agent:main:child", "session-1"],
            timeoutMs: 1,
          });
        },
        run: async () => {},
      }),
  );

  try {
    releaseFirst.resolve();
    await first;
    await queued;
    expect(initiatingAdmissionExcluded).toBe(true);
    expect(selfInterrupted).toBe(false);
  } finally {
    releaseFirst.resolve();
    admission.release();
    await Promise.allSettled([first, queued]);
  }
});

it("bounds interruption waits for non-cooperative work", async () => {
  const admissionLease = await beginSessionEffect({
    scope: "store-a",
    identities: ["agent:main:child", "session-1"],
    assertAllowed: () => {},
    onInterrupt: () => {},
  });

  try {
    await expect(
      interruptSessionControllerEffects({
        scope: "store-a",
        identities: ["session-1"],
        timeoutMs: 1,
      }),
    ).resolves.toBe(false);
  } finally {
    admissionLease.release();
  }
});

it("shares lifecycle coordination across duplicate module instances", async () => {
  const first = await importFreshModule<typeof import("./session-controller.lifecycle.js")>(
    import.meta.url,
    "./session-controller.lifecycle.js?scope=session-lifecycle-a",
  );
  const second = await importFreshModule<typeof import("./session-controller.lifecycle.js")>(
    import.meta.url,
    "./session-controller.lifecycle.js?scope=session-lifecycle-b",
  );
  let releaseLease = () => {};
  let interrupted = false;
  const lease = await first.beginSessionEffect({
    scope: "store-duplicate",
    identities: ["agent:main:child", "session-duplicate"],
    assertAllowed: () => {},
    onInterrupt: () => {
      interrupted = true;
      releaseLease();
    },
  });
  releaseLease = lease.release;

  try {
    expect(second.isSessionControllerWorkActive("store-duplicate", ["session-duplicate"])).toBe(
      true,
    );
    await expect(
      second.interruptSessionControllerEffects({
        scope: "store-duplicate",
        identities: ["agent:main:child"],
        timeoutMs: 50,
      }),
    ).resolves.toBe(true);
    expect(interrupted).toBe(true);
    expect(first.isSessionControllerWorkActive("store-duplicate", ["session-duplicate"])).toBe(
      false,
    );
  } finally {
    lease.release();
  }
});
