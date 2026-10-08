import type { GatewayRestartSnapshot } from "./restart-health.types.js";

// Both callers pass a fresh snapshot that has not escaped inspection.
export function finalizeGatewayRestartSnapshot(
  snapshot: GatewayRestartSnapshot,
  expectedVersion: string | undefined,
  expectedBuildId: string | undefined,
  requirePluginHealth: boolean,
): GatewayRestartSnapshot {
  if (expectedVersion) {
    snapshot.expectedVersion = expectedVersion;
    if (snapshot.gatewayVersion !== expectedVersion) {
      snapshot.healthy = false;
      if (snapshot.gatewayVersion != null) {
        snapshot.versionMismatch = {
          expected: expectedVersion,
          actual: snapshot.gatewayVersion,
        };
      }
    }
  }
  // Runtime identity remains required even with a separately configured UI root.
  if (expectedBuildId) {
    snapshot.expectedBuildId = expectedBuildId;
    if (snapshot.gatewayBuildId !== expectedBuildId) {
      snapshot.healthy = false;
      if (snapshot.gatewayBuildId !== undefined) {
        snapshot.buildIdMismatch = {
          expected: expectedBuildId,
          actual: snapshot.gatewayBuildId ?? null,
        };
      }
    }
  }
  // Some maintenance observations intentionally defer plugin verification. Keep
  // that caller policy without hiding the overall degraded projection.
  const optionalPluginFailure =
    !requirePluginHealth &&
    snapshot.readiness?.state === "degraded" &&
    snapshot.readiness.reasons.length > 0 &&
    snapshot.readiness.reasons.every((reason) => reason.startsWith("plugin:"));
  if (snapshot.readiness && snapshot.readiness.state !== "ready" && !optionalPluginFailure) {
    snapshot.healthy = false;
    if (snapshot.readiness.state === "starting") {
      snapshot.startupPhase = snapshot.readiness.reasons.join(", ") || "Gateway startup";
    }
  }
  if (
    (requirePluginHealth && snapshot.activatedPluginErrors?.length) ||
    snapshot.channelProbeErrors?.length
  ) {
    snapshot.healthy = false;
  }
  return snapshot;
}
