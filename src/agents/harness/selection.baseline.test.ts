import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { awaitGateBeforeSettlement, createDeferred } from "../../../test/helpers/promise.js";
import type { InternalSessionEntry } from "../../config/sessions/types.js";
import {
  createOperationalRunInstanceRef,
  prepareAgentRunAdmission,
  type AdmittedRunContext,
  type PreparedAgentRunAdmission,
} from "../admitted-run-context.js";
import { runEmbeddedAttempt } from "../embedded-agent-runner/run/attempt.js";
import { makeEmbeddedRunnerAttempt } from "../test-helpers/embedded-agent-runner-e2e-fixtures.js";
import { clearAgentHarnesses, registerAgentHarness } from "./registry.js";
import { runAgentHarnessAttempt } from "./selection.js";
import { createHarnessAttemptParams } from "./selection.test-support.js";
import type { AgentHarness } from "./types.js";

// mock-isolation: Exercise real harness selection without starting provider inference.
vi.mock("../embedded-agent-runner/run/attempt.js", () => ({ runEmbeddedAttempt: vi.fn() }));

const captureState = vi.hoisted(() => ({
  ready: undefined as Promise<InternalSessionEntry> | undefined,
}));
vi.mock("../../sessions/session-diff-capture.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../sessions/session-diff-capture.js")>()),
  // A mock spy observes promise settlement itself, which would falsely signal a capture join.
  getSessionDiffBaselineCapture: () => captureState.ready,
}));

let admission: PreparedAgentRunAdmission;
let admittedRunContext: AdmittedRunContext;

beforeEach(async () => {
  clearAgentHarnesses();
  captureState.ready = undefined;
  admission = prepareAgentRunAdmission({
    cfg: {},
    facts: {
      runId: "baseline-dispatch",
      agentId: "main",
      ingress: { kind: "system", boundary: "baseline-dispatch-test", state: "present" },
    },
    operationalRunInstance: createOperationalRunInstanceRef("baseline-dispatch"),
  });
  admittedRunContext = await admission.admit("plugin-harness", "baseline-dispatch-test");
});

afterEach(() => {
  admission.close();
  clearAgentHarnesses();
  vi.restoreAllMocks();
});

function holdCapture() {
  const capture = createDeferred<InternalSessionEntry>();
  const joined = createDeferred();
  // A Promise subclass exposes the actual await, rather than a getter read while binding tools.
  class ObservableCapture extends Promise<InternalSessionEntry> {
    // oxlint-disable-next-line unicorn/no-thenable -- Observe await on a real Promise, not a host binding read.
    override then<T = InternalSessionEntry, E = never>(
      onfulfilled?: ((value: InternalSessionEntry) => T | PromiseLike<T>) | null,
      onrejected?: ((reason: unknown) => E | PromiseLike<E>) | null,
    ): Promise<T | E> {
      joined.resolve();
      return super.then(onfulfilled, onrejected);
    }
  }
  captureState.ready = new ObservableCapture((resolve, reject) => {
    void capture.promise.then(resolve, reject);
  });
  return {
    joined: joined.promise,
    release: () => capture.resolve({ sessionId: "s1", updatedAt: 1 }),
  };
}

describe("workspace baseline at harness dispatch", () => {
  it("settles capture before dispatching a native turn or starting its tool hook deadline", async () => {
    const capture = holdCapture();
    const dispatched = createDeferred();
    const runAttempt = vi.fn<AgentHarness["runAttempt"]>(async (params) => {
      dispatched.resolve();
      await params.hostCapabilities?.runBeforeToolCall({ toolName: "write", params: {} });
      return makeEmbeddedRunnerAttempt({ agentHarnessId: "codex" });
    });
    registerAgentHarness(
      {
        id: "codex",
        label: "Codex",
        supports: () => ({ supported: true, priority: 100 }),
        runAttempt,
      },
      { ownerPluginId: "codex" },
    );
    const operation = runAgentHarnessAttempt({
      ...createHarnessAttemptParams(admittedRunContext),
      agentHarnessRuntimeOverride: "codex",
    });
    try {
      await awaitGateBeforeSettlement(
        capture.joined,
        Promise.race([dispatched.promise, operation]),
        "Native backend dispatched before joining the held workspace capture",
      );
      expect(runAttempt).not.toHaveBeenCalled();
    } finally {
      capture.release();
      await operation;
    }
    expect(runAttempt).toHaveBeenCalledOnce();
  });

  it("allows the built-in runtime to prepare inference while capture is held", async () => {
    const capture = holdCapture();
    const dispatched = createDeferred();
    vi.mocked(runEmbeddedAttempt).mockImplementationOnce(async () => {
      dispatched.resolve();
      return makeEmbeddedRunnerAttempt({ agentHarnessId: "openclaw" });
    });
    const operation = runAgentHarnessAttempt({
      ...createHarnessAttemptParams(admittedRunContext),
      agentHarnessRuntimeOverride: "openclaw",
    });
    try {
      await awaitGateBeforeSettlement(
        dispatched.promise,
        capture.joined,
        "Built-in model preparation waited for the workspace capture",
      );
    } finally {
      capture.release();
      await operation;
    }
  });
});
