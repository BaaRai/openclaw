import { describe, expect, it, vi } from "vitest";
import { digestClawValue } from "./digest.js";
import type { ClawDiagnostic } from "./types.js";
import { applyClawUpdatePlan } from "./update-apply.js";
import { manifest, plan, source } from "./update-apply.test-helpers.js";
import type { ClawUpdatePlan } from "./update-plan.js";

const warning: ClawDiagnostic = {
  level: "warning",
  code: "legacy_openclaw_model_ignored",
  phase: "schema",
  path: "$.profiles.openclaw.agent.model",
  message:
    "Legacy agent.model is ignored during local Claw Update; host model settings are operator-owned.",
};

function signedPlan(diagnostics: ClawDiagnostic[]): ClawUpdatePlan {
  const value = { ...plan([]), diagnostics };
  const body = Object.fromEntries(Object.entries(value).filter(([key]) => key !== "planIntegrity"));
  return { ...value, planIntegrity: digestClawValue(body) };
}

describe("Claw Update legacy warning consent", () => {
  it("rejects warnings that differ from the reviewed plan", async () => {
    const reviewed = signedPlan([warning]);
    const changedWarning = { ...warning, message: "Changed after preview." };
    const readInstall = vi.fn();
    const rebuildPlan = vi.fn(async ({ diagnostics }: { diagnostics?: ClawDiagnostic[] }) =>
      signedPlan(diagnostics ?? []),
    );

    await expect(
      applyClawUpdatePlan(
        reviewed,
        {
          targetManifest: manifest,
          targetSource: source,
          targetDiagnostics: [changedWarning],
        },
        {
          config: {},
          sourceMcpServers: {},
          consentPlanIntegrity: reviewed.planIntegrity,
          rebuildPlan,
          readInstall,
        },
      ),
    ).rejects.toMatchObject({ code: "update_changed" });
    expect(readInstall).not.toHaveBeenCalled();
  });

  it("passes the same warnings through apply-time rebuild", async () => {
    const reviewed = signedPlan([warning]);
    const readInstall = vi.fn();
    const rebuildPlan = vi.fn(async ({ diagnostics }: { diagnostics?: ClawDiagnostic[] }) =>
      signedPlan(diagnostics ?? []),
    );

    await expect(
      applyClawUpdatePlan(
        reviewed,
        { targetManifest: manifest, targetSource: source, targetDiagnostics: [warning] },
        {
          config: {},
          sourceMcpServers: {},
          consentPlanIntegrity: reviewed.planIntegrity,
          rebuildPlan,
          readInstall,
        },
      ),
    ).rejects.toMatchObject({
      code: "update_changed",
      message: "The Claw install record disappeared.",
    });
    expect(rebuildPlan).toHaveBeenCalledWith(expect.objectContaining({ diagnostics: [warning] }));
    expect(readInstall).toHaveBeenCalledOnce();
  });
});
