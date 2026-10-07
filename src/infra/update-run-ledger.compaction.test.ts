import * as Value from "typebox/value";
import { afterEach, describe, expect, it } from "vitest";
import { UpdateRunRecordSchema as WireRunSchema } from "../../packages/gateway-protocol/src/schema/update-runs.js";
import { UPDATE_RUN_PHASES } from "../../packages/gateway-protocol/src/update-run-vocabulary.js";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import {
  closeOpenClawStateDatabaseAsync,
  closeOpenClawStateDatabaseForTest,
} from "../state/openclaw-state-db.js";
import { compactUpdateRunStepCount, encodeRun } from "./update-run-codec.js";
import { readUpdateRunCompaction, summarizeUpdateRunTimings } from "./update-run-history.js";
import {
  createUpdateRun,
  getUpdateRun,
  recordUpdateRunStep,
  recordUpdateRunPhase,
} from "./update-run-ledger.js";
import { decodeRun } from "./update-run-read.kernel.js";
import { toPublicUpdateRun } from "./update-run-record.js";
import { renderUpdateRunReport } from "./update-run-report.js";

const tempDirs = useAutoCleanupTempDirTracker(afterEach);
afterEach(async () => {
  await closeOpenClawStateDatabaseAsync();
  closeOpenClawStateDatabaseForTest();
});

function isolatedOptions() {
  return { env: { OPENCLAW_STATE_DIR: tempDirs.make("openclaw-update-compaction-") } };
}

describe("update run ledger compaction", () => {
  it.each([
    { name: "step count", count: 130, detail: undefined },
    { name: "diagnostic bytes", count: 30, detail: "diagnostic ".repeat(80) },
    { name: "retained phase bytes", count: 0, detail: "🦞".repeat(512) },
  ])(
    "retains admission warnings, failure steps, notice custody, and finalization history across the $name bound and database reopen",
    ({ count, detail }) => {
      const options = isolatedOptions();
      const run = createUpdateRun({ trigger: "chat" }, options);
      const evidence = [
        "candidate-admission",
        "warning:update-admission-unsupported-target",
        "warning:update-admission-fallback",
        "warning:managed-service-membership",
        "warning:finalize:plugins:deadline",
        "global update",
        "global update (omit optional)",
        "candidate-doctor-lint",
      ].map((step) => ({
        step,
        status:
          step.startsWith("global update") || step === "candidate-doctor-lint"
            ? ("failed" as const)
            : ("completed" as const),
        startedAtMs: 1_000,
        endedAtMs: 2_000,
      }));
      for (const step of evidence) {
        recordUpdateRunStep(run.runId, { ...step, detail }, options);
      }
      const notices = [
        "notice:ack",
        "notice:activating",
        "notice:verifying",
        "previous generation restoration",
        "finalize:doctor",
        "finalize:future-phase",
        // Candidate Doctor's predecessor-stop receipt: identity lives in the key.
        "finalize:predecessor-stop:1758600000000:1000:631:0123456789abcdef",
        "post-update verification",
      ];
      for (const step of [...UPDATE_RUN_PHASES, ...notices]) {
        recordUpdateRunStep(run.runId, { step, status: "completed", detail }, options);
      }
      for (let index = 0; index < count; index++) {
        recordUpdateRunStep(
          run.runId,
          { step: `warning:diagnostic-${index}`, status: "completed", detail },
          options,
        );
      }
      closeOpenClawStateDatabaseForTest();
      const persisted = getUpdateRun(run.runId, options)!;
      for (const expected of evidence) {
        expect(persisted.steps.filter((step) => step.step === expected.step)).toEqual([
          expect.objectContaining(expected),
        ]);
      }
      expect(persisted.steps.map((step) => step.step)).toEqual(
        expect.arrayContaining([...UPDATE_RUN_PHASES, ...notices]),
      );
      expect(persisted.steps.length).toBeLessThanOrEqual(128);
      expect(Buffer.byteLength(JSON.stringify(persisted.steps))).toBeLessThanOrEqual(16 * 1024);
    },
  );
});

// Real SQLite roundtrips exercise both producer count compaction and codec byte compaction.
it.each([
  { bound: "count", count: 150, detail: "warning" },
  { bound: "UTF-8 bytes", count: 25, detail: "🦞".repeat(500) },
])(
  "keeps major elapsed observations, identities and skipped outcomes after $bound compaction",
  ({ count, detail }) => {
    const options = isolatedOptions();
    const run = createUpdateRun({ trigger: "cli" }, options);
    const before = { version: "2026.9.4", sha: "a".repeat(40), buildId: "fixture-old-build" };
    const after = { version: "2026.9.5", sha: "b".repeat(40), buildId: "fixture-new-build" };
    recordUpdateRunPhase(
      run.runId,
      "validating",
      { before, after, target: { kind: "git", sha: after.sha } },
      options,
    );
    const observations = [
      { step: "validating", status: "completed" as const, startedAtMs: 1_000, endedAtMs: 11_000 },
      {
        step: "updater-runtime-retention",
        status: "completed" as const,
        startedAtMs: 1_000,
        endedAtMs: 5_000,
      },
      {
        step: "candidate-state-snapshot",
        status: "completed" as const,
        startedAtMs: 5_000,
        endedAtMs: 6_000,
      },
      {
        step: "candidate-doctor",
        status: "completed" as const,
        startedAtMs: 6_000,
        endedAtMs: 9_000,
      },
      {
        step: "candidate-doctor-lint",
        status: "skipped" as const,
        startedAtMs: 9_000,
        endedAtMs: 9_000,
      },
    ];
    for (const step of observations) {
      recordUpdateRunStep(run.runId, step, options);
    }
    const receipt = {
      step: "diagnostic:database rollback",
      status: "completed" as const,
      detail: "Rollback proof: retained snapshot digest and recovery receipt.",
    };
    recordUpdateRunStep(run.runId, receipt, options);
    for (let i = 0; i < count; i++) {
      recordUpdateRunStep(
        run.runId,
        { step: `warning:large-history:${i}`, status: "completed", detail },
        options,
      );
    }
    closeOpenClawStateDatabaseForTest();
    const retained = getUpdateRun(run.runId, options)!;
    expect(retained).toMatchObject({ runId: run.runId, before, after, target: { sha: after.sha } });
    for (const step of [...observations, receipt]) {
      expect(retained.steps).toContainEqual(expect.objectContaining(step));
    }
    expect(readUpdateRunCompaction(retained.steps)?.omittedSteps).toBeGreaterThan(0);
    expect(summarizeUpdateRunTimings(retained.steps).phases).toContainEqual({
      step: "validating",
      durationMs: 10_000,
      majorOperationsMs: 8_000,
      otherMs: 2_000,
    });
    expect(retained.steps.length).toBeLessThanOrEqual(128);
    const encoded = encodeRun(retained, options);
    expect(Buffer.byteLength(encoded.steps_json)).toBeLessThanOrEqual(16 * 1024);
    // Re-encoding cannot count already-omitted history twice.
    expect(decodeRun(encoded)).toEqual(retained);
    // The wire schema is deliberately unchanged: old consumers accept every new row.
    expect(Value.Check(WireRunSchema, toPublicUpdateRun(retained))).toBe(true);
    const report = renderUpdateRunReport(retained);
    expect(report.markdown).toContain("not skipped validation");
    expect(report.lines.join("\n")).toContain("candidate-doctor: 3000 ms (completed)");
    expect(report.lines.join("\n")).toContain("candidate-doctor-lint: 0 ms (skipped)");
  },
);

it("does not reject a full legacy protected-step set just to insert an omission marker", () => {
  const protectedSteps = Array.from({ length: 128 }, (_, i) => ({
    step: `finalize:receipt:${i}`,
    status: "completed" as const,
  }));
  const steps = [...protectedSteps, { step: "warning:discardable", status: "completed" as const }];
  expect(() => compactUpdateRunStepCount(steps)).not.toThrow();
  expect(steps).toEqual(protectedSteps);
  expect(readUpdateRunCompaction(steps)).toBeUndefined();
});

it("keeps near-capacity safety receipts ahead of timing and omission bookkeeping", () => {
  const options = isolatedOptions();
  const run = createUpdateRun({ trigger: "cli" }, options);
  const receipts = Array.from({ length: 16 }, (_, i) => ({
    step: `finalize:doctor-lint:receipt:${i}`,
    status: "completed" as const,
    detail: "r".repeat(930),
  }));
  expect(Buffer.byteLength(JSON.stringify(receipts))).toBeLessThanOrEqual(16 * 1024);
  const timed = ["candidate-doctor", "candidate-state-snapshot", "updater-runtime-retention"].map(
    (step) => ({
      step,
      status: "completed" as const,
      startedAtMs: 10,
      endedAtMs: 20,
    }),
  );
  const row = encodeRun(
    {
      ...run,
      steps: [
        ...receipts,
        ...timed,
        { step: "warning:discardable", status: "completed", detail: "x".repeat(1_000) },
      ],
    },
    options,
  );
  expect(Buffer.byteLength(row.steps_json)).toBeLessThanOrEqual(16 * 1024);
  const retained = decodeRun(row);
  for (const receipt of receipts) {
    expect(retained.steps).toContainEqual(receipt);
  }
  expect(Value.Check(WireRunSchema, toPublicUpdateRun(retained))).toBe(true);
});

it.each([125, 126, 127, 128])(
  "keeps %i protected receipts when compaction bookkeeping is already present",
  (count) => {
    const receipts = Array.from({ length: count }, (_, i) => ({
      step: `finalize:receipt:${i}`,
      status: "completed" as const,
    }));
    const steps = [...receipts];
    for (let i = 0; i < 5; i++) {
      steps.push({ step: `warning:discardable:${i}`, status: "completed" });
      compactUpdateRunStepCount(steps);
    }
    expect(steps.length).toBeLessThanOrEqual(128);
    for (const receipt of receipts) {
      expect(steps).toContainEqual(receipt);
    }
    const nextReceipt = { step: `finalize:receipt:${count}`, status: "completed" as const };
    steps.push(nextReceipt);
    if (count === 128) {
      // Genuinely oversized safety metadata still refuses rather than discarding a receipt.
      expect(() => compactUpdateRunStepCount(steps)).toThrow("retained steps exceed");
    } else {
      expect(() => compactUpdateRunStepCount(steps)).not.toThrow();
      for (const receipt of [...receipts, nextReceipt]) {
        expect(steps).toContainEqual(receipt);
      }
      expect(steps.length).toBeLessThanOrEqual(128);
    }
  },
);
