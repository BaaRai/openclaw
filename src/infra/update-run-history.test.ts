import { describe, expect, it } from "vitest";
import {
  formatUpdateRunTimings,
  readUpdateRunCompaction,
  recordUpdateRunCompaction,
  summarizeUpdateRunTimings,
  UPDATE_RUN_COMPACTION_STEP,
} from "./update-run-history.js";
import type { UpdateRunStep } from "./update-run-record.js";

function completed(step: string, start: number, end: number): UpdateRunStep {
  return { step, status: "completed", startedAtMs: start, endedAtMs: end };
}

describe("update timing accounting", () => {
  it("retains phase-only timing summaries from older writers", () => {
    const report = formatUpdateRunTimings(
      summarizeUpdateRunTimings([completed("validating", 0, 100)]),
    ).join("\n");
    expect(report).toContain(
      "validating: 100 ms inclusive; 0 ms named-operation union; 100 ms other/unattributed",
    );
    expect(report).toContain("Missing intervals are unknown");
    expect(formatUpdateRunTimings(summarizeUpdateRunTimings([]))).toEqual([]);
  });

  it("unions overlapping and nested operations instead of adding them to their phase", () => {
    const summary = summarizeUpdateRunTimings([
      completed("validating", 100, 1_100),
      completed("updater-runtime-retention", 0, 400),
      completed("candidate-state-snapshot", 300, 500),
      completed("candidate-doctor", 350, 450),
      completed("preflight-build", 700, 1_200),
      completed("diagnostic:unrelated", 0, 2_000),
    ]);
    expect(summary.phases).toEqual([
      { step: "validating", durationMs: 1_000, majorOperationsMs: 800, otherMs: 200 },
    ]);
    expect(summary.operations.map((step) => step.durationMs)).toEqual([400, 200, 100, 500]);
  });

  it("clips cross-phase operations and counts adjacent intervals once", () => {
    const summary = summarizeUpdateRunTimings([
      completed("validating", 0, 100),
      completed("activating", 100, 200),
      completed("candidate-doctor", 20, 120),
      completed("post-update verification", 120, 180),
    ]);
    expect(summary.phases).toEqual([
      { step: "validating", durationMs: 100, majorOperationsMs: 80, otherMs: 20 },
      { step: "activating", durationMs: 100, majorOperationsMs: 80, otherMs: 20 },
    ]);
  });

  it("does not turn missing, unfinished or reversed intervals into measured zero or success", () => {
    const steps: UpdateRunStep[] = [
      completed("validating", 0, 100),
      { step: "candidate-state-snapshot", status: "completed" },
      { ...completed("candidate-doctor", 0, 20), status: "in_progress" },
      completed("preflight-build", 50, 40),
      { ...completed("candidate-doctor-lint", 40, 40), status: "skipped" },
      { ...completed("candidate-plugins", 60, 70), status: "failed" },
    ];
    const summary = summarizeUpdateRunTimings(steps);
    expect(
      summary.operations.map(({ step, status, durationMs }) => ({ step, status, durationMs })),
    ).toEqual([
      { step: "candidate-doctor-lint", status: "skipped", durationMs: 0 },
      { step: "candidate-plugins", status: "failed", durationMs: 10 },
    ]);
    expect(summary.phases[0]?.otherMs).toBe(90);
    const report = formatUpdateRunTimings(summarizeUpdateRunTimings(steps)).join("\n");
    expect(report).toContain("not validation verdicts");
    expect(report).toContain("Missing intervals are unknown");
    expect(report).toContain("candidate-doctor-lint: 0 ms (skipped)");
    expect(report).toContain("candidate-plugins: 10 ms (failed)");
  });
});

describe("history-only compaction receipt", () => {
  it("accumulates omitted history independently of execution outcomes", () => {
    const steps: UpdateRunStep[] = [
      completed("candidate-doctor", 1, 5),
      { step: "candidate-doctor-lint", status: "skipped" },
    ];
    recordUpdateRunCompaction(steps, 30, 2);
    recordUpdateRunCompaction(steps, 5, 1);
    expect(readUpdateRunCompaction(steps)).toEqual({ omittedSteps: 35, compactedDetails: 3 });
    expect(steps.filter((step) => step.step === UPDATE_RUN_COMPACTION_STEP)).toHaveLength(1);
    expect(steps[0]?.status).toBe("completed");
    expect(steps[1]?.status).toBe("skipped");
    expect(steps[2]?.detail).toContain("not skipped execution");
  });

  it.each([
    undefined,
    "malformed",
    '{"version":2,"omittedSteps":1,"compactedDetails":0}',
    '{"version":1,"omittedSteps":-1,"compactedDetails":0}',
    '{"version":1,"omittedSteps":1.5,"compactedDetails":0}',
  ])("keeps legacy or unknown compaction metadata unknown: %s", (detail) => {
    const steps: UpdateRunStep[] =
      detail === undefined
        ? []
        : [{ step: UPDATE_RUN_COMPACTION_STEP, status: "completed", detail }];
    expect(readUpdateRunCompaction(steps)).toBeUndefined();
  });
});
