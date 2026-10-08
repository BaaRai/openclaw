import { beforeEach, describe, expect, it } from "vitest";
import { buildEmbeddedRunPayloads } from "../../agents/embedded-agent-runner/run/payloads.js";
import { resetPluginRuntimeStateForTest } from "../../plugins/runtime.js";
import {
  sanitizeAssistantVisibleText,
  stripAssistantInternalScaffolding,
} from "../../shared/text/assistant-visible-text.js";
import type { ReplyPayload } from "../types.js";
import { buildReplyPayloads } from "./agent-runner-payloads.js";
import { normalizeReplyPayload } from "./normalize-reply.js";

function buildTestReplyPayloads({ payloads }: { payloads: ReplyPayload[] }) {
  return buildReplyPayloads({
    isHeartbeat: false,
    didLogHeartbeatStrip: false,
    blockStreamingEnabled: false,
    blockReplyPipeline: null,
    replyToMode: "off",
    payloads,
  });
}

describe("tool failure reply delivery", () => {
  beforeEach(() => resetPluginRuntimeStateForTest());

  it("delivers model tool-failure explanations without provider-error rewriting", async () => {
    const explanation =
      "Slack rate-limited the lookup. Try again after one second; the thread was not read.";
    const payloads = buildEmbeddedRunPayloads({
      assistantTexts: [explanation],
      lastAssistant: undefined,
      lastToolError: { toolName: "slack.read_thread", error: "HTTP 429 Too Many Requests" },
      toolFailureExplanation: true,
      sessionKey: "agent:main:explanation",
    });
    const { replyPayloads } = await buildTestReplyPayloads({ payloads });
    expect(replyPayloads.map((payload) => normalizeReplyPayload(payload))).toEqual([
      expect.objectContaining({ text: explanation, isError: true }),
    ]);
  });

  it("honors a completed silent answer after an exec failure", async () => {
    const payloads = buildEmbeddedRunPayloads({
      assistantTexts: ["NO_REPLY"],
      lastAssistant: undefined,
      lastToolError: { toolName: "exec", error: "Command not found", mutatingAction: true },
      sessionKey: "agent:main:warning",
    });
    const { replyPayloads } = await buildTestReplyPayloads({ payloads });

    expect(replyPayloads).toEqual([]);
  });

  it("delivers the real exec failure warning when the agent produced no answer", async () => {
    const payloads = buildEmbeddedRunPayloads({
      assistantTexts: [],
      lastAssistant: undefined,
      lastToolError: { toolName: "exec", error: "Command not found" },
      sessionKey: "agent:main:warning",
    });
    const { replyPayloads } = await buildTestReplyPayloads({ payloads });
    const delivered = replyPayloads
      .map((payload) => normalizeReplyPayload(payload))
      .filter(Boolean);

    expect(delivered).toEqual([
      expect.objectContaining({
        text: "⚠️ Exec failed",
        isError: true,
      }),
    ]);
    // Both channel text cleanup and Control UI display must retain the warning.
    expect(sanitizeAssistantVisibleText(delivered[0]?.text ?? "")).toBe(delivered[0]?.text);
    expect(stripAssistantInternalScaffolding(delivered[0]?.text ?? "")).toBe(delivered[0]?.text);
    expect(
      normalizeReplyPayload({
        text: "⚠️ 🛠️ Exec failed",
        isError: true,
      }),
    ).toBeNull();
  });
});
