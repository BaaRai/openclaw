import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { truncateUtf16Safe } from "@openclaw/normalization-core/utf16-slice";
import { UPDATE_RUN_PHASES } from "../../packages/gateway-protocol/src/update-run-vocabulary.js";
import { resolveStateDir } from "../config/paths.js";
import { redactSensitiveText } from "../logging/redact.js";
import { escapeRegExp } from "../shared/regexp.js";
import type { OpenClawStateDatabaseOptions } from "../state/openclaw-state-db-contract.js";
import type { UpdateRuns } from "../state/openclaw-state-db.generated.js";
import { resolveRequiredHomeDir } from "./home-dir.js";
import { normalizeUpdateFailureFacts } from "./update-failure-facts.js";
import {
  isMajorUpdateOperation,
  recordUpdateRunCompaction,
  UPDATE_RUN_COMPACTION_STEP,
} from "./update-run-history.js";
import { UPDATE_RUN_TEXT_LIMIT } from "./update-run-limits.js";
import type { UpdateRunRedactionFacts } from "./update-run-mutation.types.js";
import type { UpdateRunRecord, UpdateRunStep } from "./update-run-record.js";
import { UpdateRunRecordSchema } from "./update-run-schema.js";

const JSON_BYTES = 16 * 1024;
const RETAINED_STEP_NAMES = [
  ...UPDATE_RUN_PHASES,
  "candidate-admission",
  // Keep named admission/lifecycle receipts, not the unbounded warning:* namespace.
  "warning:update-admission-unsupported-target",
  "warning:update-admission-fallback",
  "warning:managed-service-membership",
  "warning:finalize:plugins:deadline",
  "global update",
  "global update (omit optional)",
  "candidate-doctor-lint",
  "notice:ack",
  "notice:activating",
  "notice:verifying",
  "previous generation restoration",
  "post-update verification",
  "diagnostic:database snapshot",
  "diagnostic:database migration writes",
  "diagnostic:database rollback",
  "task-delivery-recovery",
  "driver:adopted",
  "driver:identity-unavailable",
  "reconcile:abandoned",
  "reconcile:superseded",
  "reconcile:acknowledged",
  "reconcile:settle",
];
export type UpdateRunLedgerOptions = OpenClawStateDatabaseOptions & {
  busyTimeoutMs?: number;
  redactPaths?: readonly string[];
};

/** Capture only path redaction facts; the state worker keeps its own authority environment. */
export function captureUpdateRunRedactionFacts(
  env: NodeJS.ProcessEnv = process.env,
): UpdateRunRedactionFacts {
  return {
    effectiveHome: resolveRequiredHomeDir(env),
    home: env.HOME,
    userProfile: env.USERPROFILE,
    configPath: env.OPENCLAW_CONFIG_PATH,
  };
}

export function resolveUpdateRunCodecEnv(
  stateEnv: NodeJS.ProcessEnv | undefined,
  facts: UpdateRunRedactionFacts,
): NodeJS.ProcessEnv {
  return {
    ...(stateEnv ?? process.env),
    OPENCLAW_HOME: facts.effectiveHome,
    HOME: facts.home,
    USERPROFILE: facts.userProfile,
    OPENCLAW_CONFIG_PATH: facts.configPath,
  };
}

function mapJsonText(
  value: unknown,
  transform: (text: string, key?: string) => string,
  key?: string,
): unknown {
  if (typeof value === "string") {
    return transform(value, key);
  }
  if (Array.isArray(value)) {
    return value.map((entry) => mapJsonText(entry, transform, key));
  }
  if (isRecord(value)) {
    return Object.fromEntries(
      Object.keys(value)
        .toSorted()
        .map((field) => [field, mapJsonText(value[field], transform, field)]),
    );
  }
  return value;
}

function isProtectedHistoryStep(item: unknown): boolean {
  return (
    isRecord(item) &&
    typeof item.step === "string" &&
    (item.step === UPDATE_RUN_COMPACTION_STEP ||
      item.termination === "signal" ||
      item.step.startsWith("finalize:") ||
      RETAINED_STEP_NAMES.some((name) => name === item.step))
  );
}

function isRetainedStep(step: UpdateRunStep): boolean {
  return isProtectedHistoryStep(step) || isMajorUpdateOperation(step.step);
}

function noteCompaction(steps: UpdateRunStep[], omitted: number, details = 0): void {
  // A full legacy receipt set has no spare slot. Bookkeeping must not make a
  // previously admissible write fail; absent compaction metadata stays unknown.
  if (
    steps.length >= 128 &&
    !steps.some((step) => step.step === UPDATE_RUN_COMPACTION_STEP) &&
    steps.every(isRetainedStep)
  ) {
    return;
  }
  recordUpdateRunCompaction(steps, omitted, details);
}

function compactOperation(step: UpdateRunStep): UpdateRunStep {
  const { step: name, status, startedAtMs, endedAtMs, exitCode, termination, signal } = step;
  return { step: name, status, startedAtMs, endedAtMs, exitCode, termination, signal };
}

/** Bound non-step metadata; step history has its own retention owner below. */
function boundedJson(
  input: unknown,
  maxBytes = JSON_BYTES,
  preservedTextFields?: ReadonlySet<string>,
): string {
  let value = input;
  let json = JSON.stringify(value);
  while (Buffer.byteLength(json) > maxBytes) {
    if (Array.isArray(value)) {
      value = value.slice(1);
    } else if (isRecord(value)) {
      const object = value;
      const arrayField = Object.keys(object)
        .toSorted()
        .find((field) => Array.isArray(object[field]) && object[field].length > 0);
      const array = arrayField ? object[arrayField] : undefined;
      if (arrayField && Array.isArray(array)) {
        value = { ...object, [arrayField]: array.slice(1) };
      } else {
        value = mapJsonText(value, (text, key) =>
          key && preservedTextFields?.has(key)
            ? text
            : truncateUtf16Safe(text, Math.floor(text.length / 2)),
        );
      }
    } else {
      throw new Error("Update run metadata exceeds its bounded schema");
    }
    const nextJson = JSON.stringify(value);
    if (nextJson === json) {
      throw new Error("Update run retained metadata exceeds its byte limit");
    }
    json = nextJson;
  }
  return json;
}

/** Count and byte compaction share the same custody and omission accounting. */
export function compactUpdateRunStepCount(steps: UpdateRunStep[]): void {
  while (steps.length > 128) {
    const ordinary = steps.findIndex((entry) => !isRetainedStep(entry));
    if (ordinary >= 0) {
      steps.splice(ordinary, 1);
      noteCompaction(steps, 1);
      continue;
    }
    // Bookkeeping must yield even when a previous writer already inserted it.
    const marker = steps.findIndex((entry) => entry.step === UPDATE_RUN_COMPACTION_STEP);
    if (marker >= 0) {
      steps.splice(marker, 1);
      continue;
    }
    const timing = steps.findIndex((entry) => !isProtectedHistoryStep(entry));
    if (timing < 0) {
      throw new Error("Update run retained steps exceed the step limit");
    }
    steps.splice(timing, 1);
    noteCompaction(steps, 1);
  }
}

function boundedRunSteps(input: UpdateRunStep[]): string {
  const steps = input.map((step) => ({ ...step }));
  compactUpdateRunStepCount(steps);
  let json = JSON.stringify(steps);
  while (Buffer.byteLength(json) > JSON_BYTES) {
    const bulkyOperation = steps.findIndex(
      (step) =>
        !isProtectedHistoryStep(step) &&
        isMajorUpdateOperation(step.step) &&
        (step.detail !== undefined ||
          step.failureFacts !== undefined ||
          step.configChange !== undefined ||
          step.configWriteRefusal !== undefined ||
          step.snapshotCapacity !== undefined ||
          step.stderrTail !== undefined),
    );
    const disposable = steps.findIndex((step) => !isRetainedStep(step));
    if (disposable >= 0) {
      steps.splice(disposable, 1);
      noteCompaction(steps, 1);
    } else if (bulkyOperation >= 0) {
      // Previously this entire operation was evicted. Preserve its compact time
      // and outcome without reserving unbounded command/Doctor payloads.
      steps[bulkyOperation] = compactOperation(steps[bulkyOperation]!);
      noteCompaction(steps, 0, 1);
    } else {
      let compacted = 0;
      for (const step of steps) {
        // These receipts keep their existing durable safety/rollback contract.
        if (
          step.termination === "signal" ||
          step.step === UPDATE_RUN_COMPACTION_STEP ||
          step.step === "task-delivery-recovery" ||
          step.step === "diagnostic:database snapshot" ||
          step.step === "diagnostic:database migration writes" ||
          step.step === "diagnostic:database rollback" ||
          step.step.startsWith("finalize:doctor-lint:")
        ) {
          continue;
        }
        if (step.detail !== undefined || step.failureFacts !== undefined) {
          delete step.detail;
          delete step.failureFacts;
          compacted++;
        }
      }
      if (compacted) {
        noteCompaction(steps, 0, compacted);
      } else {
        const step = steps.findLast((entry) => Boolean(entry.stderrTail?.length));
        if (step?.stderrTail) {
          step.stderrTail = truncateUtf16Safe(
            step.stderrTail,
            Math.floor(step.stderrTail.length / 2),
          );
          noteCompaction(steps, 0, 1);
        } else {
          // Observability cannot crowd out a full legacy safety receipt set.
          const marker = steps.findIndex((entry) => entry.step === UPDATE_RUN_COMPACTION_STEP);
          const timing = steps.findIndex((entry) => !isProtectedHistoryStep(entry));
          if (
            marker >= 0 &&
            Buffer.byteLength(JSON.stringify(steps.toSpliced(marker, 1))) <= JSON_BYTES
          ) {
            steps.splice(marker, 1);
          } else if (timing >= 0) {
            steps.splice(timing, 1);
            noteCompaction(steps, 1);
          } else if (marker >= 0) {
            steps.splice(marker, 1);
          } else {
            throw new Error("Update run retained step metadata exceeds its byte limit");
          }
        }
      }
    }
    compactUpdateRunStepCount(steps);
    json = JSON.stringify(steps);
  }
  return json;
}

function boundedOriginJson(origin: UpdateRunRecord["origin"]): string {
  const {
    driver,
    previousDrivers,
    updateRecoveryCapture,
    requester,
    sessionKey,
    deliveryContext,
    campaignId,
    ...admissionDiagnostics
  } = origin;
  // Operational receipts are not expendable diagnostics. Keep them exact inside
  // the existing database byte budget; oversized sets fail before replacing a row.
  const retained = JSON.stringify({ driver, previousDrivers, updateRecoveryCapture });
  if (Buffer.byteLength(retained) > JSON_BYTES) {
    throw new Error("Update run recovery receipts exceed the origin byte limit");
  }
  const routing = { requester, sessionKey, deliveryContext, campaignId };
  const hasAdmission = origin.admission !== undefined || origin.candidateAdmission !== undefined;
  // Admission diagnostics cannot shorten routing; both yield to recovery receipts.
  const identities = hasAdmission
    ? JSON.stringify({ driver, previousDrivers, updateRecoveryCapture, ...routing })
    : retained;
  const diagnostics = hasAdmission ? admissionDiagnostics : { ...admissionDiagnostics, ...routing };
  // Merging removes the diagnostic braces and needs a comma only when identities exist.
  const diagnosticBudget =
    JSON_BYTES - Buffer.byteLength(identities) + 2 - (identities === "{}" ? 0 : 1);
  if (
    diagnostics.candidateAdmission?.warnings.length &&
    Buffer.byteLength(JSON.stringify(diagnostics)) > diagnosticBudget
  ) {
    diagnostics.candidateAdmission = { ...diagnostics.candidateAdmission, warnings: [] };
  }
  const preservedTextFields = new Set([
    "owner",
    "verdict",
    "status",
    "code",
    "name",
    "candidateVersion",
    "installedVersion",
    "fallbackReason",
  ]);
  const minimumDiagnostics = JSON.stringify(
    mapJsonText(diagnostics, (text, key) => (key && preservedTextFields.has(key) ? text : "")),
  );
  if (Buffer.byteLength(minimumDiagnostics) > diagnosticBudget) {
    return retained;
  }
  const boundedDiagnostics = boundedJson(diagnostics, diagnosticBudget, preservedTextFields);
  return `{${[identities.slice(1, -1), boundedDiagnostics.slice(1, -1)].filter(Boolean).join(",")}}`;
}

export function encodeRun(input: UpdateRunRecord, options: UpdateRunLedgerOptions): UpdateRuns {
  const env = options.env ?? process.env;
  // Home-relative selectors remain actionable in reports. Other captured roots
  // are diagnostic only; model refs, slash commands, and URLs are not paths.
  const roots: [string | undefined, string][] = [
    [resolveRequiredHomeDir(env), "~"],
    [env.HOME, "~"],
    [env.USERPROFILE, "~"],
    [resolveStateDir(env), "$OPENCLAW_STATE_DIR"],
    [env.OPENCLAW_CONFIG_PATH, "[path]"],
    ...(options.redactPaths ?? []).map((root): [string, string] => [root, "[path]"]),
  ];
  const redactPaths: [RegExp, string][] = roots.flatMap(([root, replacement]) => {
    if (!root) {
      return [];
    }
    const prefix = root
      .replaceAll("\\", "/")
      .replace(/\/+$/u, "")
      .split("/")
      .map(escapeRegExp)
      .join("[\\\\/]");
    const flags = /^(?:[A-Za-z]:|\\\\)/u.test(root) ? "giu" : "gu";
    return prefix
      ? [
          [
            new RegExp(
              `(?<!https?:)(?:(?<![\\w/])|(?<=file:///?))${prefix}(?=$|[\\\\/\\s"'<>.,;:)])`,
              flags,
            ),
            replacement,
          ],
        ]
      : [];
  });
  // Process identities and recovery receipts are operational facts, not diagnostics.
  const { driver, previousDrivers, updateRecoveryCapture, ...originDiagnostics } = input.origin;
  const record = UpdateRunRecordSchema.parse(
    mapJsonText(
      {
        ...input,
        origin: originDiagnostics,
        steps: input.steps.map((step) => ({
          ...step,
          ...(step.failureFacts
            ? { failureFacts: normalizeUpdateFailureFacts(step.failureFacts, env) }
            : {}),
        })),
      },
      (value, key) => {
        let text = redactSensitiveText(value, { mode: "tools" });
        for (const [pattern, replacement] of redactPaths) {
          text = text.replace(pattern, () => replacement);
        }
        return truncateUtf16Safe(text, key === "stderrTail" ? 8192 : UPDATE_RUN_TEXT_LIMIT);
      },
    ),
  );
  record.origin = UpdateRunRecordSchema.shape.origin.parse({
    ...record.origin,
    driver,
    previousDrivers,
    updateRecoveryCapture,
  });
  return {
    run_id: record.runId,
    created_at_ms: record.createdAtMs,
    updated_at_ms: record.updatedAtMs,
    trigger: record.trigger,
    phase: record.phase,
    status: record.status,
    reason: record.reason,
    origin_json: boundedOriginJson(record.origin),
    target_json: boundedJson(record.target),
    before_json: boundedJson(record.before),
    after_json: boundedJson(record.after),
    steps_json: boundedRunSteps(record.steps),
    verification_json: boundedJson(record.verification),
    repair_json: boundedJson(record.repair),
    confirmed_at_ms: record.confirmedAtMs,
    finished_at_ms: record.finishedAtMs,
    downtime_ms: record.downtimeMs,
  };
}
