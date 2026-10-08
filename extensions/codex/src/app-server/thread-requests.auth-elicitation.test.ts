import { describe, expect, it } from "vitest";
import type { CodexAppServerRuntimeOptions } from "./config.js";
import { buildCodexThreadConfiguration } from "./thread-requests.js";
import { createCodexUserInputTestParams } from "./user-input-bridge.test-support.js";

const appServer: CodexAppServerRuntimeOptions = {
  start: { transport: "websocket", command: "codex", args: [], headers: {} },
  connectionClass: "remote",
  remoteAppsSubstrate: "preconfigured",
  codeModeOnly: false,
  loopDetectionPreToolUseRelay: false,
  requestTimeoutMs: 30_000,
  approvalPolicy: "on-request",
  approvalsReviewer: "user",
  sandbox: "workspace-write",
};

describe("Codex connector authentication in unattended conversations", () => {
  it.each(["group", "channel"] as const)(
    "returns auth errors without prompting a %s",
    (chatType) => {
      const params = { ...createCodexUserInputTestParams(), chatType, trigger: "user" as const };
      const request = buildCodexThreadConfiguration(params, {
        appServer,
        config: { "features.auth_elicitation": true },
      });
      expect(request.config["features.auth_elicitation"]).toBe(false);
      expect(request.approvalPolicy).toBe("on-request");
    },
  );

  it.each(["cron", "heartbeat"] as const)("does not wait for login during %s work", (trigger) => {
    const params = { ...createCodexUserInputTestParams(), trigger, chatType: "direct" as const };
    const request = buildCodexThreadConfiguration(params, { appServer });
    expect(request.config["features.auth_elicitation"]).toBe(false);
  });

  it("preserves interactive login in direct conversations", () => {
    const params = {
      ...createCodexUserInputTestParams(),
      trigger: "user" as const,
      chatType: "direct" as const,
    };
    const request = buildCodexThreadConfiguration(params, {
      appServer,
      config: { "features.auth_elicitation": true },
    });
    expect(request.config["features.auth_elicitation"]).toBe(true);
  });
});
