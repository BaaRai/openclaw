import { describe, expect, it } from "vitest";
import {
  AcpSessionManager,
  baseCfg,
  createRuntime,
  extractStatesFromUpserts,
  hoisted,
  installAcpSessionManagerTestLifecycle,
  installMutableAcpSessionMetaUpsert,
  readySessionMeta,
} from "./manager.test-helpers.js";

const sessionKey = "agent:codex:acp:child-1";
const target = { cfg: baseCfg, sessionKey };

describe("AcpSessionManager cancelSession", () => {
  installAcpSessionManagerTestLifecycle();

  it("records idle cancellation failure and preserves its cause", async () => {
    const runtime = createRuntime();
    const state = { currentMeta: readySessionMeta() };
    installMutableAcpSessionMetaUpsert(state);
    hoisted.requireAcpRuntimeBackendMock.mockReturnValue({ id: "acpx", runtime: runtime.runtime });
    hoisted.readAcpSessionEntryMock.mockImplementation(() => ({
      sessionKey,
      storeSessionKey: sessionKey,
      acp: state.currentMeta,
    }));
    const manager = new AcpSessionManager();
    const error = new Error("Cancel transport failed");
    runtime.cancel.mockRejectedValue(error);
    await expect(
      manager.cancelSession({ ...target, reason: "manual-cancel" }),
    ).rejects.toMatchObject({
      code: "ACP_TURN_FAILED",
      message: error.message,
      cause: error,
    });
    expect(runtime.cancel).toHaveBeenCalledOnce();
    expect(runtime.cancel.mock.calls[0]?.[0].reason).toBe("manual-cancel");
    expect(extractStatesFromUpserts().at(-1)).toBe("error");
  });
});
