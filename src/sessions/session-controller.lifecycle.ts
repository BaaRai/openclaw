import { randomUUID } from "node:crypto";
import { createAgentRunRestartAbortError } from "../agents/run-termination.js";
import { runExclusiveSessionStoreWrite } from "../config/sessions/store-writer.js";
import type { GatewayContextResolver } from "../gateway/server-methods/types.js";
import { getAgentRunLifecycleGeneration } from "../infra/agent-run-registry.js";
import {
  bindGatewayContextResolver,
  getPluginRuntimeGatewayRequestScope,
  withPluginRuntimeGatewayContextResolver,
} from "../plugins/runtime/gateway-request-scope.js";
import {
  GatewayDrainingError,
  isGatewaySubordinateWorkAdmissionClosed,
} from "../process/gateway-work-admission.js";
import { createDeferredCore } from "../shared/deferred.js";
import { ownerContext, sourceSettlements } from "./session-controller.context.js";
import type { ReplyOperation } from "./session-controller.contracts.js";
import { createSessionControllerPhaseLogger } from "./session-controller.diagnostics.js";
import {
  runAfterRetiringSessionSources,
  waitForSessionControllerSettlement,
  waitUnlessAborted,
} from "./session-controller.lifecycle-observation.js";
import {
  matchingEntries,
  claimMatchesSessionId,
  effectMatchesSessionId,
  selectedEffects,
  selectSessionControllerInterruptionOwners,
  resolveSessionEffectAdmission,
} from "./session-controller.lifecycle-projections.js";
import type {
  Effect,
  Mutation,
  OwnerContext,
  SessionEffectInterrupt,
  SessionEffectRef,
  SessionControllerLifecycle,
} from "./session-controller.lifecycle.types.js";
import type { SessionControllerInput } from "./session-controller.mailbox.types.js";
import {
  prepareSessionMutationCompetition,
  runPreemptedSessionMutation,
  type SessionMutationPolicy,
} from "./session-controller.mutation-preemption.js";
import {
  assertSessionControllerOperation,
  getSessionControllerEntry,
  getSessionControllerEntryForOperation,
  bindSessionControllerEntryTarget,
  refreshSessionControllerEntryAliases,
  pruneSessionControllerEntry,
  sessionControllers,
  type SessionControllerEntry,
} from "./session-controller.state.js";
import {
  targetFrom,
  type TargetInput,
  type IdentityTarget,
  type SessionTarget,
} from "./session-controller.target.js";

export {
  waitForSessionControllerSettlement,
  captureSessionControllerSettlement,
  captureSessionEffectOwnerSettlement,
  isSessionControllerWorkActive,
  consumeSessionEffectHandoff,
  cancelSessionEffectHandoff,
} from "./session-controller.lifecycle-observation.js";
export {
  withSessionControllerOwner,
  getCurrentSessionControllerOwner,
  getCurrentSessionControllerClaim,
  withSessionControllerClaim,
  hasSessionControllerQueuedWork,
  isCompetingSessionControllerWorkActive,
} from "./session-controller.context.js";
export { captureSessionTarget, type SessionTarget } from "./session-controller.target.js";
export type {
  SessionEffectInterrupt,
  SessionEffectRef,
} from "./session-controller.lifecycle.types.js";
export {
  collectSessionControllerTargets,
  captureGatewaySessionControllerWork,
  getSessionControllerWorkCount,
  getSessionMutationCount,
  isSessionMutationActive,
  hasOnlySessionMutationKindActive,
  collectSessionMutationIdentities,
} from "./session-controller.lifecycle-projections.js";
export { SessionMutationPreemptTimeoutError } from "./session-controller.mutation-preemption.js";

export const SESSION_CONTROLLER_DRAIN_TIMEOUT_MS = 15_000;

function lifecycle(entry: SessionControllerEntry): SessionControllerLifecycle {
  return (entry.lifecycle ??= {
    targets: new Map(),
    operations: new Set(),
    effects: new Set(),
    mutations: [],
    closures: new Set(),
    changed: createDeferredCore(),
    get blocksTurnAdmission() {
      return (
        this.mutations.length > 0 ||
        this.closures.size > 0 ||
        [...this.operations].some((operation) => operation !== entry.active)
      );
    },
  });
}
function bindTarget(target: SessionTarget): SessionControllerEntry {
  const entry = getSessionControllerEntry(target.sessionKey, target);
  retainTarget(entry, target);
  return entry;
}
function retainTarget(entry: SessionControllerEntry, target: SessionTarget) {
  const targets = lifecycle(entry).targets;
  targets.set(target, (targets.get(target) ?? 0) + 1);
  refreshSessionControllerEntryAliases(entry);
}
function releaseTarget(entry: SessionControllerEntry, target: SessionTarget) {
  const targets = entry.lifecycle?.targets;
  const count = targets?.get(target) ?? 0;
  if (count > 1) {
    targets?.set(target, count - 1);
  } else {
    targets?.delete(target);
  }
  refreshSessionControllerEntryAliases(entry);
}
function notify(entry: SessionControllerEntry) {
  const state = entry.lifecycle;
  if (!state) {
    return;
  }
  const changed = state.changed;
  state.changed = createDeferredCore();
  changed.resolve();
  for (const mutation of state.mutations) {
    activate(mutation);
  }
  if (
    !state.targets.size &&
    !state.operations.size &&
    !state.effects.size &&
    !state.mutations.length &&
    !state.closures.size &&
    !entry.active
  ) {
    entry.lifecycle = undefined;
    pruneSessionControllerEntry(entry);
  }
  entry.mailbox?.wake();
}
function activate(mutation: Mutation) {
  if (
    mutation.phase !== "queued" ||
    mutation.entries.some((entry) => entry.lifecycle?.mutations[0] !== mutation)
  ) {
    return;
  }
  mutation.phase = "active";
  for (const entry of mutation.entries) {
    if (entry.active) {
      mutation.operations.add(entry.active);
    }
    for (const operation of entry.lifecycle?.operations ?? []) {
      mutation.operations.add(operation);
    }
  }
  mutation.ready.resolve();
}
/** Raw finishing custody stays on the entry even when physical scope is not known yet. */
export function retainSessionControllerOperation(operation: ReplyOperation): void {
  const entry = getSessionControllerEntryForOperation(operation);
  const state = lifecycle(entry);
  if (state.operations.has(operation)) {
    return;
  }
  state.operations.add(operation);
  const released = () => {
    state.operations.delete(operation);
    refreshSessionControllerEntryAliases(entry);
    notify(entry);
  };
  void operation.ownerSettlement.then(released, released);
}
/** Called only by the producer after resolving its actual owner-settlement receipt. */
export function releaseSessionControllerOperation(operation: ReplyOperation): void {
  for (const entry of sessionControllers.values()) {
    if (entry.lifecycle?.operations.delete(operation)) {
      refreshSessionControllerEntryAliases(entry);
      notify(entry);
    }
  }
}
export function bindSessionControllerTarget(
  operation: ReplyOperation,
  target: SessionTarget,
): void {
  assertSessionControllerOperation(operation);
  const entry = getSessionControllerEntryForOperation(operation);
  if (
    !target.aliases.includes(operation.key) &&
    !target.aliases.some((alias) => entry.aliases.has(alias))
  ) {
    throw new Error("Session target does not cover the admitted controller owner");
  }
  bindSessionControllerEntryTarget(entry, target);
  retainSessionControllerOperation(operation);
  retainTarget(entry, target);
  const released = () => {
    releaseTarget(entry, target);
    notify(entry);
  };
  void operation.ownerSettlement.then(released, released);
}
/** Queues a mutation on canonical entries; ingress closes before awaited preemption or drain. */
export async function runSessionMutation<T>(
  params: {
    requiredSessionId?: string;
    prepare?: (owner: {
      closeWorkAdmissions: (reason: Error) => void;
      operations: readonly ReplyOperation[];
    }) => Promise<void>;
    finalize?: () => Promise<void>;
    run: () => Promise<T>;
    signal?: AbortSignal;
  } & SessionMutationPolicy &
    (TargetInput | { targets: Iterable<SessionTarget | IdentityTarget> }),
): Promise<T> {
  const current = ownerContext.getStore();
  const inheritedSignal = current?.claim?.abortController.signal ?? current?.operation?.abortSignal;
  const signal =
    params.signal && inheritedSignal
      ? AbortSignal.any([params.signal, inheritedSignal])
      : (params.signal ?? inheritedSignal);
  signal?.throwIfAborted();
  const targets =
    "targets" in params
      ? [...params.targets].map((target) => ("storeScope" in target ? target : targetFrom(target)))
      : [targetFrom(params)];
  const bindings: Array<{ entry: SessionControllerEntry; target: SessionTarget }> = [];
  const releaseBindings = () => {
    for (const { entry, target } of bindings) {
      releaseTarget(entry, target);
    }
  };
  const entries = [
    ...new Set(
      targets.flatMap((target) => {
        const entry = bindTarget(target);
        bindings.push({ entry, target });
        return [entry, ...matchingEntries(target)];
      }),
    ),
  ].toSorted((a, b) => a.key.localeCompare(b.key));
  const matchesOperation = (operation: ReplyOperation) =>
    params.requiredSessionId === undefined || operation.hasOwnedSessionId(params.requiredSessionId);
  const borrowed = [...(current?.mutations ?? [])].find(
    (mutation) =>
      mutation.phase === "active" && entries.every((entry) => mutation.entries.includes(entry)),
  );
  const releases: Array<() => void> = [];
  const closeWorkAdmissions = (reason: Error) => {
    for (const target of targets) {
      releases.push(closeSessionControllerAdmission({ target, reason }));
    }
  };
  if (borrowed) {
    try {
      await params.prepare?.({
        closeWorkAdmissions,
        operations: [...borrowed.operations].filter(matchesOperation),
      });
      return await params.run();
    } finally {
      try {
        await params.finalize?.();
      } finally {
        for (const release of releases) {
          release();
        }
        releaseBindings();
        for (const entry of entries) {
          notify(entry);
        }
      }
    }
  }
  const mutation: Mutation = {
    entries,
    targets,
    kind: params.kind,
    phase: "queued",
    operations: new Set(),
    ready: createDeferredCore(),
  };
  for (const entry of entries) {
    lifecycle(entry).mutations.push(mutation);
  }
  const dequeue = () => {
    mutation.phase = "released";
    releaseBindings();
    for (const entry of entries) {
      const queue = lifecycle(entry).mutations;
      const index = queue.indexOf(mutation);
      if (index >= 0) {
        queue.splice(index, 1);
      }
      notify(entry);
    }
  };
  const withdraw = () => {
    if (mutation.phase !== "queued") {
      return;
    }
    dequeue();
    mutation.ready.reject(signal?.reason ?? new Error("Session mutation cancelled"));
  };
  signal?.addEventListener("abort", withdraw, { once: true });
  activate(mutation);
  if (signal?.aborted) {
    withdraw();
  }
  try {
    await mutation.ready.promise;
  } finally {
    signal?.removeEventListener("abort", withdraw);
  }
  const context: OwnerContext = {
    operation: current?.operation,
    claim: current?.claim,
    effects: current?.effects ?? new Set(),
    mutations: new Set([...(current?.mutations ?? []), mutation]),
  };
  return await ownerContext.run(context, async () => {
    try {
      signal?.throwIfAborted();
      const competitors = [...mutation.operations].filter(
        (operation) => operation !== current?.operation && matchesOperation(operation),
      );
      const claims = entries.flatMap((entry) =>
        entry.mailbox?.claim &&
        claimMatchesSessionId(entry.mailbox.claim, params.requiredSessionId) &&
        entry.mailbox.claim !== current?.claim &&
        (!current?.operation || entry.mailbox.claim.operation !== current.operation)
          ? [entry.mailbox.claim]
          : [],
      );
      await params.prepare?.({ operations: competitors, closeWorkAdmissions });
      const { waitForCompetitors, preemption } = await prepareSessionMutationCompetition({
        policy: params,
        claims,
        entries,
        competitors,
        requiredSessionId: params.requiredSessionId,
        targets,
        signal: params.signal,
      });
      // Only started effects can write; pending validators stay owned through their real return.
      const effects = new Set(
        entries.flatMap((entry) =>
          [...(entry.lifecycle?.effects ?? [])].filter(
            (effect) =>
              effectMatchesSessionId(effect, params.requiredSessionId) &&
              (effect.phase === "validating" ||
                effect.phase === "writer" ||
                (waitForCompetitors && effect.phase === "acquired")) &&
              !current?.effects.has(effect),
          ),
        ),
      );
      if (preemption) {
        for (const effect of effects) {
          interruptEffect(effect, preemption.reason);
        }
      }
      const effectsSettled = Promise.all(
        [...effects].map((effect) =>
          waitForCompetitors ? effect.ref.released : effect.validated.promise,
        ),
      );
      if (preemption) {
        return await runPreemptedSessionMutation({
          preemption,
          effectsSettled,
          targets,
          requiredSessionId: params.requiredSessionId,
          run: params.run,
        });
      }
      // Only the caller's signal cancels this wait; an inherited turn signal may be the target.
      await waitUnlessAborted(effectsSettled, params.signal);
      if (waitForCompetitors) {
        return await runAfterRetiringSessionSources(
          targets,
          params.requiredSessionId,
          params.run,
          params.signal,
        );
      }
      return await params.run();
    } finally {
      try {
        await params.finalize?.();
      } finally {
        for (const release of releases) {
          release();
        }
        dequeue();
      }
    }
  });
}

/** Subordinate physical-effect custody. This never reserves or serializes a turn. */
export async function beginSessionEffect(
  params: TargetInput & {
    storeWriterIdentities?: Iterable<string | undefined>;
    owner?: symbol;
    resolveGatewayContext?: GatewayContextResolver;
    operation?: ReplyOperation;
    sourceInput?: SessionControllerInput;
    assertAllowed: (signal: AbortSignal) => Promise<void> | void;
    revalidateAllowed?: () => Promise<void> | void;
    onInterrupt?: SessionEffectInterrupt;
    signal?: AbortSignal;
  },
): Promise<SessionEffectRef> {
  if (isGatewaySubordinateWorkAdmissionClosed()) {
    throw new GatewayDrainingError();
  }
  const target = targetFrom(params);
  const entry = bindTarget(target);
  const state = lifecycle(entry);
  const current = ownerContext.getStore();
  const operation =
    params.operation ??
    (current?.operation && getSessionControllerEntryForOperation(current.operation) === entry
      ? current.operation
      : undefined);
  if (params.operation) {
    assertSessionControllerOperation(params.operation);
  }
  const resolver = Object.hasOwn(params, "resolveGatewayContext")
    ? params.resolveGatewayContext
    : getPluginRuntimeGatewayRequestScope()?.resolveGatewayContext;
  const cancel = new AbortController();
  const signal = params.signal ? AbortSignal.any([params.signal, cancel.signal]) : cancel.signal;
  const logAdmission = createSessionControllerPhaseLogger("lifecycle-admission", {
    sessionKey: target.sessionKey,
    sessionId: target.incarnation,
    sourceId: params.sourceInput?.protocolRunId ?? current?.claim?.inputs[0]?.protocolRunId,
  });
  const settled = createDeferredCore();
  let running = 0;
  let releaseRequested = false;
  const ref: SessionEffectRef = {
    target,
    operation,
    isActive: () => effect.phase !== "released" && !releaseRequested && !effect.interrupted,
    released: settled.promise,
    release: () => {
      if (effect.phase === "released") {
        return;
      }
      releaseRequested = true;
      if (running > 0) {
        return;
      }
      effect.phase = "released";
      effect.handoffs.clear();
      state.effects.delete(effect);
      releaseTarget(entry, target);
      effect.validated.resolve();
      settled.resolve();
      notify(entry);
    },
    createHandoff: () => {
      if (effect.phase === "released") {
        throw new Error("Cannot hand off a released session effect");
      }
      const token = randomUUID();
      effect.handoffs.add(token);
      return token;
    },
    run: async <T>(run: () => Promise<T>): Promise<T> => {
      const caller = ownerContext.getStore();
      if (effect.phase === "released" || (releaseRequested && !caller?.effects.has(effect))) {
        throw new Error("Session effect custody has closed");
      }
      if (effect.interrupted) {
        throw effect.interrupted;
      }
      running++;
      try {
        return await ownerContext.run(
          {
            operation,
            effects: new Set([...(caller?.effects ?? []), effect]),
            mutations: caller?.mutations ?? new Set(),
          },
          () => withPluginRuntimeGatewayContextResolver(resolver, run),
        );
      } finally {
        running--;
        if (releaseRequested) {
          ref.release();
        }
      }
    },
  };
  const effect: Effect = {
    ref,
    entry,
    owner: params.owner,
    phase: "queued",
    generation: getAgentRunLifecycleGeneration(),
    cancel,
    handoffs: new Set(),
    interrupt: params.onInterrupt,
    validated: createDeferredCore(),
  };
  bindGatewayContextResolver(effect, resolver);
  state.effects.add(effect);
  const admission = () => resolveSessionEffectAdmission(effect, state, current, params.sourceInput);
  logAdmission("waiting", "controller-admission");
  try {
    const closure = state.closures.values().next().value;
    if (closure && !admission().finishingCapturedTurn) {
      throw closure.reason;
    }
    // Source preparation owns no turn; the mailbox still fences execution behind the predecessor.
    while (!admission().allowed) {
      const pendingClosure = state.closures.values().next().value;
      if (pendingClosure) {
        throw pendingClosure.reason;
      }
      await waitUnlessAborted(lifecycle(entry).changed.promise, signal);
    }
    signal.throwIfAborted();
    effect.phase = "validating";
    logAdmission("waiting", "validation");
    // Once validator I/O starts, custody is retained until it and the writer barrier settle.
    await ref.run(async () => await params.assertAllowed(signal));
    signal.throwIfAborted();
    if (isGatewaySubordinateWorkAdmissionClosed()) {
      throw new GatewayDrainingError();
    }
    effect.phase = "writer";
    logAdmission("waiting", "store-writer");
    let writerStarted = false;
    let removeAbort = () => {};
    const aborted = new Promise<never>((_, reject) => {
      const abort = () => {
        if (!writerStarted) {
          reject(
            signal.reason instanceof Error
              ? signal.reason
              : new Error("Session effect cancelled", { cause: signal.reason }),
          );
        }
      };
      signal.addEventListener("abort", abort, { once: true });
      removeAbort = () => signal.removeEventListener("abort", abort);
      if (signal.aborted) {
        abort();
      }
    });
    const writer = runExclusiveSessionStoreWrite(
      target.storeScope,
      async () => {
        // A withdrawn queue callback has no writer capability or user work left.
        signal.throwIfAborted();
        writerStarted = true;
        await ref.run(async () => {
          if (params.revalidateAllowed) {
            await params.revalidateAllowed();
          } else {
            await params.assertAllowed(signal);
          }
        });
        signal.throwIfAborted();
      },
      { reentrant: true, identities: params.storeWriterIdentities },
    );
    try {
      await Promise.race([writer, aborted]);
    } finally {
      removeAbort();
    }
    effect.phase = "acquired";
    logAdmission("settled", "acquired");
    effect.validated.resolve();
    return ref;
  } catch (error) {
    logAdmission("failed", signal.aborted ? "aborted" : effect.phase);
    ref.release();
    throw error;
  }
}

function interruptEffect(effect: Effect, reason?: Error) {
  effect.interrupted ??= reason ?? new Error("Session effect interrupted");
  try {
    return effect.interrupt?.(effect.interrupted);
  } finally {
    effect.cancel.abort(effect.interrupted);
  }
}
export function closeSessionControllerAdmission(
  params: TargetInput & { reason: Error },
): () => void {
  const target = targetFrom(params);
  const entry = bindTarget(target);
  const closure = { reason: params.reason };
  lifecycle(entry).closures.add(closure);
  let released = false;
  const release = () => {
    if (released) {
      return;
    }
    released = true;
    entry.lifecycle?.closures.delete(closure);
    releaseTarget(entry, target);
    notify(entry);
  };
  try {
    for (const effect of selectedEffects([target])) {
      if (effect.phase === "queued") {
        interruptEffect(effect, params.reason);
      }
    }
  } catch (error) {
    release();
    throw error;
  }
  return release;
}
/** Capture first, then interrupt: a replacement cannot inherit the cancellation. */
export function startSessionControllerInterruption(
  params: TargetInput & { reason?: Error; requiredSessionId?: string; admissionsOnly?: boolean },
): {
  released: Promise<void>;
  interruptedRunIds: ReadonlySet<string>;
} {
  const target = targetFrom(params);
  // Unspecified interruption keeps restart recovery semantics.
  const reason = params.reason ?? createAgentRunRestartAbortError();
  const { effects, operations, claims } = selectSessionControllerInterruptionOwners(
    target,
    ownerContext.getStore(),
    params,
  );
  const settlements = [
    ...sourceSettlements(
      target,
      params.requiredSessionId,
      params.admissionsOnly ? "admissions" : "all",
    ),
    ...claims.map((claim) => claim.settlement.promise),
    ...effects.map((effect) => effect.ref.released),
    ...operations.map((operation) => operation.ownerSettlement),
  ];
  const interruptedRunIds = new Set<string>();
  const failures: unknown[] = [];
  for (const claim of claims) {
    claim.abortController.abort(reason);
  }
  for (const effect of effects) {
    try {
      const receipt = interruptEffect(effect, reason);
      if (receipt) {
        interruptedRunIds.add(receipt.runId);
      }
    } catch (error) {
      failures.push(error);
    }
  }
  for (const operation of operations) {
    try {
      operation.abort(reason);
    } catch (error) {
      failures.push(error);
    }
  }
  return {
    interruptedRunIds,
    released: Promise.all(settlements).then(() => {
      if (failures.length) {
        throw new AggregateError(failures, "Session interruption failed");
      }
    }),
  };
}

export async function interruptSessionControllerEffects(
  params: TargetInput & { reason?: Error; timeoutMs?: number },
): Promise<boolean> {
  return await waitForSessionControllerSettlement(
    startSessionControllerInterruption(params).released,
    params.timeoutMs,
  );
}
