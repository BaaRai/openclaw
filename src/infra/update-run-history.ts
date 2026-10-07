import { UPDATE_RUN_PHASES } from "../../packages/gateway-protocol/src/update-run-vocabulary.js";
import type { UpdateRunStep } from "./update-run-record.js";

// A fixed inventory reserves compact elapsed-time facts, not their verbose diagnostics.
// Step keys retain the released reader contract, including the latest attempt per key.
const MAJOR_OPERATIONS = new Set([
  "updater-runtime-retention",
  "preflight-worktree",
  "preflight-deps-install",
  "preflight-build",
  "candidate-state-snapshot",
  "candidate-doctor",
  "candidate-doctor-lint",
  "candidate-config",
  "candidate-plugins",
  "candidate-recovery",
  "candidate-gateway-startup",
  "candidate-state-cleanup",
  "preflight-runtime-stage",
  "previous gateway verification",
  "database snapshot",
  "openclaw doctor",
  "post-update verification",
  "managed-service restart",
  "gateway verification",
  "preflight-cleanup",
  "global update",
  "global update (omit optional)",
]);
const PHASES = new Set<string>(UPDATE_RUN_PHASES);
export const UPDATE_RUN_COMPACTION_STEP = "history:compaction";

export function isMajorUpdateOperation(step: string): boolean {
  return MAJOR_OPERATIONS.has(step);
}

type Compaction = { omittedSteps: number; compactedDetails: number };

/** Absence (including old writers) is unknown, not proof of a complete history. */
export function readUpdateRunCompaction(steps: readonly UpdateRunStep[]): Compaction | undefined {
  const detail = steps.find((step) => step.step === UPDATE_RUN_COMPACTION_STEP)?.detail;
  if (!detail) {
    return undefined;
  }
  try {
    const value: unknown = JSON.parse(detail);
    if (
      typeof value !== "object" ||
      value === null ||
      !("version" in value) ||
      value.version !== 1 ||
      !("omittedSteps" in value) ||
      typeof value.omittedSteps !== "number" ||
      !Number.isSafeInteger(value.omittedSteps) ||
      value.omittedSteps < 0 ||
      !("compactedDetails" in value) ||
      typeof value.compactedDetails !== "number" ||
      !Number.isSafeInteger(value.compactedDetails) ||
      value.compactedDetails < 0
    ) {
      return undefined;
    }
    return { omittedSteps: value.omittedSteps, compactedDetails: value.compactedDetails };
  } catch {
    return undefined;
  }
}

/** Use an existing step shape so published readers need no schema or protocol upgrade. */
export function recordUpdateRunCompaction(
  steps: UpdateRunStep[],
  omittedSteps: number,
  compactedDetails = 0,
): void {
  const previous = readUpdateRunCompaction(steps);
  const marker: UpdateRunStep = {
    step: UPDATE_RUN_COMPACTION_STEP,
    status: "completed",
    detail: JSON.stringify({
      version: 1,
      omittedSteps: Math.min(Number.MAX_SAFE_INTEGER, (previous?.omittedSteps ?? 0) + omittedSteps),
      compactedDetails: Math.min(
        Number.MAX_SAFE_INTEGER,
        (previous?.compactedDetails ?? 0) + compactedDetails,
      ),
      meaning: "history compaction, not skipped execution",
    }),
  };
  const index = steps.findIndex((step) => step.step === UPDATE_RUN_COMPACTION_STEP);
  if (index < 0) {
    steps.push(marker);
  } else {
    steps[index] = marker;
  }
}

type Interval = { start: number; end: number };
function interval(step: UpdateRunStep): Interval | undefined {
  const start = step.startedAtMs;
  const end = step.endedAtMs;
  if (
    start === undefined ||
    end === undefined ||
    end < start ||
    step.status === "in_progress" ||
    step.status === "pending"
  ) {
    return undefined;
  }
  return { start, end };
}

function unionMs(intervals: Interval[]): number {
  let total = 0;
  let coveredEnd = -Infinity;
  for (const { start, end } of intervals.toSorted((a, b) => a.start - b.start)) {
    total += Math.max(0, end - Math.max(start, coveredEnd));
    coveredEnd = Math.max(coveredEnd, end);
  }
  return total;
}

/** Derived only from retained observations: never infer execution, fill gaps, or add nested spans. */
export function summarizeUpdateRunTimings(steps: readonly UpdateRunStep[]) {
  const operations = steps.flatMap((step) => {
    const span = isMajorUpdateOperation(step.step) ? interval(step) : undefined;
    return span
      ? [{ step: step.step, status: step.status, ...span, durationMs: span.end - span.start }]
      : [];
  });
  const phases = steps.flatMap((step) => {
    const span = PHASES.has(step.step) ? interval(step) : undefined;
    if (!span) {
      return [];
    }
    const nested = operations.flatMap((operation) => {
      const start = Math.max(span.start, operation.start);
      const end = Math.min(span.end, operation.end);
      return end > start ? [{ start, end }] : [];
    });
    const durationMs = span.end - span.start;
    const majorOperationsMs = unionMs(nested);
    return [
      { step: step.step, durationMs, majorOperationsMs, otherMs: durationMs - majorOperationsMs },
    ];
  });
  return { operations, phases };
}

/** Detailed output can live in the saved report without spending the short chat budget. */
export function formatUpdateRunTimings(
  summary: ReturnType<typeof summarizeUpdateRunTimings>,
): string[] {
  if (!summary.operations.length && !summary.phases.length) {
    return [];
  }
  return [
    "Recorded elapsed timings (latest interval per key; not validation verdicts).",
    "Operations are nested in phases and may overlap; do not add them to phase totals. Missing intervals are unknown.",
    ...summary.phases.map(
      (phase) =>
        `${phase.step}: ${phase.durationMs} ms inclusive; ${phase.majorOperationsMs} ms named-operation union; ${phase.otherMs} ms other/unattributed (exclusive of that union).`,
    ),
    ...summary.operations.map(
      (operation) => `${operation.step}: ${operation.durationMs} ms (${operation.status}).`,
    ),
  ];
}
