/* @vitest-environment jsdom */

import { render } from "lit";
import { beforeEach, expect, it } from "vitest";
import { i18n } from "../../i18n/index.ts";
import { registerAgentsHomeEnglish } from "../../i18n/locales/en-agents-home.ts";
import { hasCompleteClawActionEffects, renderClawActionEffect } from "./claws-effect-review.ts";

beforeEach(async () => {
  registerAgentsHomeEnglish();
  await i18n.setLocale("en");
});

it("shows exact package MCP effects and ownership without executing markup", () => {
  const container = document.createElement("div");
  render(
    renderClawActionEffect({
      type: "mcp-server",
      desiredDigest: "sha256:declared",
      proposed: {
        transport: "stdio",
        command: "npx",
        arguments: ["-y", "<unsafe-arg>"],
        environment: [{ name: "TOKEN", sourceName: "MCP_TOKEN" }],
        authentication: "none",
        toolFilter: { include: ["search"] },
      },
      ownership: {
        relationship: "referenced",
        origin: "pre-existing",
        independentOwner: true,
        affectedClawCount: 2,
      },
    }),
    container,
  );
  expect(container.textContent).toContain("npx");
  expect(container.textContent).toContain("<unsafe-arg>");
  expect(container.textContent).toContain('["-y","<unsafe-arg>"]');
  expect(container.querySelector("unsafe-arg")).toBeNull();
  expect(container.textContent).toContain("TOKEN <- MCP_TOKEN");
  expect(container.textContent).toContain("Referenced");
  expect(container.textContent).toMatch(/Other Claws\s+2/u);
});

it("shows a redacted MCP endpoint and binds the complete URL by digest", () => {
  const container = document.createElement("div");
  render(
    renderClawActionEffect({
      type: "mcp-server",
      desiredDigest: "sha256:declaration",
      proposed: {
        transport: "streamable-http",
        url: "https://mcp.example.test/search",
        urlDigest: "sha256:full-url",
        queryParameterNames: ["token"],
        authentication: "oauth",
      },
    }),
    container,
  );
  expect(container.textContent).toContain("https://mcp.example.test/search");
  expect(container.textContent).toContain("sha256:full-url");
  expect(container.textContent).toContain("token");
});

it("refuses Add, Update, or Remove when a required effect is absent", () => {
  expect(
    hasCompleteClawActionEffects({
      operation: "add",
      actions: [{ kind: "workspaceFile", id: "SOUL.md", action: "write", blocked: false }],
    }),
  ).toBe(false);
  expect(
    hasCompleteClawActionEffects({
      operation: "update",
      actions: [{ kind: "mcpServer", id: "search", action: "change", blocked: false }],
    }),
  ).toBe(false);
  expect(
    hasCompleteClawActionEffects({
      operation: "remove",
      actions: [{ kind: "packageRef", id: "plugin:search", action: "release", blocked: false }],
    }),
  ).toBe(false);
});
