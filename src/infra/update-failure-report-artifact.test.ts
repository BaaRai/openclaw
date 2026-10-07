import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import { writeUpdateRunReportArtifact } from "./update-failure-report-artifact.js";
import { encodeRun } from "./update-run-codec.js";
import { decodeRun } from "./update-run-read.kernel.js";
import type { UpdateRunRecord } from "./update-run-record.js";
import { renderUpdateRunReport } from "./update-run-report.js";
import type { UpdateRunResult } from "./update-runner-types.js";

const tempDirs = useAutoCleanupTempDirTracker(afterEach);

function updateResult(status: "ok" | "error"): UpdateRunResult {
  return {
    status,
    mode: "unknown",
    reason: status === "error" ? "update-refused" : undefined,
    steps:
      status === "error"
        ? [
            {
              name: "doctor",
              command: "openclaw doctor --lint",
              cwd: "/opt/openclaw",
              durationMs: 0,
              exitCode: 1,
              doctorLintFindings: [
                { checkId: "core/config", severity: "error", message: "Invalid configuration" },
              ],
            },
          ]
        : [],
    durationMs: 0,
  };
}

describe("update report artifact ownership", () => {
  it.each(["ok", "error"] as const)(
    "retains detached %s reports privately without changing shared temp or selected state",
    async (status) => {
      const root = tempDirs.make("update-report-artifact-");
      const sharedTemp = path.join(root, "shared-temp");
      const stateDir = path.join(root, "unadmitted-state");
      await fs.mkdir(sharedTemp);
      if (process.platform !== "win32") {
        await fs.chmod(sharedTemp, 0o1777);
      }
      const sentinel = path.join(sharedTemp, "unrelated.txt");
      await fs.writeFile(sentinel, "unrelated artifact");
      const parentBefore = await fs.stat(sharedTemp);
      const sentinelBefore = await fs.stat(sentinel);
      const tmpdir = vi.spyOn(os, "tmpdir").mockReturnValue(sharedTemp);
      try {
        const reportPath = await writeUpdateRunReportArtifact({
          result: updateResult(status),
          report: { markdown: "Update report" },
          env: { OPENCLAW_STATE_DIR: stateDir },
          detached: true,
        });
        const reportDir = path.dirname(reportPath);
        expect(reportDir).not.toBe(sharedTemp);
        expect(path.dirname(reportDir)).toBe(sharedTemp);
        expect(await fs.readFile(reportPath, "utf8")).toContain("Update report");
        const entries = await fs.readdir(reportDir);
        expect(entries).toHaveLength(status === "error" ? 3 : 1);
        if (status === "error") {
          const diagnostic = entries.find((entry) => entry.startsWith("openclaw-update-failure-"))!;
          expect(
            JSON.parse(await fs.readFile(path.join(reportDir, diagnostic), "utf8")),
          ).toMatchObject({
            result: { status: "error" },
          });
          expect(await fs.readFile(reportPath, "utf8")).toContain(diagnostic);
          const inventory = entries.find((entry) => entry.startsWith("openclaw-update-lint-"))!;
          expect(
            JSON.parse(await fs.readFile(path.join(reportDir, inventory), "utf8")),
          ).toMatchObject({
            result: {
              steps: [
                expect.objectContaining({
                  doctorLintFindings: [expect.objectContaining({ checkId: "core/config" })],
                }),
              ],
            },
          });
        }
        expect(await fs.stat(sharedTemp)).toMatchObject({
          mode: parentBefore.mode,
          uid: parentBefore.uid,
          ino: parentBefore.ino,
        });
        expect(await fs.stat(sentinel)).toMatchObject({
          mode: sentinelBefore.mode,
          uid: sentinelBefore.uid,
          ino: sentinelBefore.ino,
        });
        expect(await fs.readFile(sentinel, "utf8")).toBe("unrelated artifact");
        if (process.platform !== "win32") {
          expect((await fs.stat(reportDir)).mode & 0o777).toBe(0o700);
          for (const entry of entries) {
            expect((await fs.stat(path.join(reportDir, entry))).mode & 0o777).toBe(0o600);
          }
        }
        await expect(fs.stat(stateDir)).rejects.toMatchObject({ code: "ENOENT" });
      } finally {
        tmpdir.mockRestore();
      }
    },
  );

  it("keeps admitted reports in the selected state directory", async () => {
    const stateDir = tempDirs.make("update-report-admitted-");
    const runId = "a4396fd1-6a2b-42fc-84d5-1c0abbdc3e5c";
    const reportPath = await writeUpdateRunReportArtifact({
      result: { ...updateResult("ok"), runId },
      report: { markdown: "Admitted update" },
      env: { OPENCLAW_STATE_DIR: stateDir },
    });
    expect(reportPath).toBe(path.join(stateDir, "update-reports", `${runId}.md`));
    expect(await fs.readFile(reportPath, "utf8")).toContain("Admitted update");
  });
});

it("keeps complete lint and diagnostic artifacts alongside source-bound compact timings", async () => {
  const stateDir = tempDirs.make("update-report-timings-");
  const result = updateResult("error");
  const runId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  result.runId = runId;
  result.steps[0]!.doctorLintFindings = Array.from({ length: 100 }, (_, i) => ({
    checkId: "core/config",
    severity: "error" as const,
    message: `synthetic finding ${i}`,
  }));
  const input: UpdateRunRecord = {
    runId,
    createdAtMs: 0,
    updatedAtMs: 100,
    trigger: "cli",
    phase: "finished",
    status: "failed",
    reason: "update-refused",
    origin: {},
    target: { kind: "git", sha: "b".repeat(40) },
    before: { sha: "a".repeat(40) },
    after: { sha: "b".repeat(40) },
    steps: [
      { step: "validating", status: "completed", startedAtMs: 0, endedAtMs: 100 },
      { step: "candidate-doctor", status: "failed", startedAtMs: 10, endedAtMs: 90 },
      {
        step: "diagnostic:database rollback",
        status: "completed",
        detail: "Retained rollback receipt.",
      },
      ...Array.from({ length: 30 }, (_, i) => ({
        step: `diagnostic:large:${i}`,
        status: "completed" as const,
        detail: "🦞".repeat(500),
      })),
    ],
    verification: {},
    repair: [],
    confirmedAtMs: null,
    finishedAtMs: 100,
    downtimeMs: null,
  };
  const env = { OPENCLAW_STATE_DIR: stateDir };
  const run = decodeRun(encodeRun(input, { env }));
  const reportPath = await writeUpdateRunReportArtifact({
    result,
    report: renderUpdateRunReport(run),
    readRun: () => run,
    env,
  });
  const report = await fs.readFile(reportPath, "utf8");
  expect(report).toContain("not skipped validation");
  expect(report).toContain("Retained rollback receipt.");
  expect(report).toContain("## Recorded elapsed timings");
  expect(report).toContain(`Run: ${runId}`);
  expect(report).toContain(`Source before: {"sha":"${"a".repeat(40)}"}`);
  expect(report).toContain(`Source after: {"sha":"${"b".repeat(40)}"}`);
  expect(report).toContain("candidate-doctor: 80 ms (failed)");
  expect(report).toContain(
    "100 ms inclusive; 80 ms named-operation union; 20 ms other/unattributed",
  );
  expect(report).toContain("Complete Doctor lint findings (100)");
  expect(report).toContain("synthetic finding 99");
  const link = /Bounded diagnostic JSON: (.+)/u.exec(report)![1]!;
  const failurePath = path.resolve(path.dirname(reportPath), link);
  const failure = await fs.readFile(failurePath, "utf8");
  expect(failure).toContain("Complete Doctor lint inventory:");
});
