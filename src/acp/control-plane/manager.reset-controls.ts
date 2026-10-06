import type { OpenClawConfig } from "../../config/types.openclaw.js";
import type { AcpSessionCancellationCapture } from "./manager.cancel-session.js";

type AcpResetTarget = {
  cfg: OpenClawConfig;
  sessionKey: string;
  agentId?: string;
};

type AcpSessionResetControls = {
  /** Core-only lifecycle custody, kept off the plugin-facing session manager. */
  captureCancellation: () => AcpSessionCancellationCapture;
  captureSessionRuntimeOwnership: (target: AcpResetTarget) => {
    isCurrent: () => boolean;
    release: () => void;
  };
  forceDiscardSessionRuntime: (
    target: AcpResetTarget & {
      reason: string;
      isCurrent?: () => boolean;
      assertCurrent?: () => void;
    },
  ) => Promise<void>;
};

const RESET_CONTROLS = new WeakMap<object, AcpSessionResetControls>();

export function registerAcpSessionResetControls(
  manager: object,
  controls: AcpSessionResetControls,
): void {
  RESET_CONTROLS.set(manager, controls);
}

export function getAcpSessionResetControls(manager: object): AcpSessionResetControls {
  const controls = RESET_CONTROLS.get(manager);
  if (!controls) {
    throw new Error("ACP session manager reset controls unavailable");
  }
  return controls;
}
