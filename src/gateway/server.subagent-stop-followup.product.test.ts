// Prove Stop, deletion, and yield of kept children through real Gateway tools.
import { X509Certificate } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, expect, it, vi } from "vitest";
import {
  writeOpenAiResponsesSse,
  writeOpenAiResponsesText,
} from "../../test/helpers/openai-responses-sse.js";
import { createDeferred } from "../../test/helpers/promise.js";
import { TEST_TLS_CERT_PEM, TEST_TLS_KEY_PEM } from "../../test/helpers/tls-fixture.js";
import * as announceOutput from "../agents/subagents/announce/subagent-announce-output.js";
import { subscribeSubagentRunChanges } from "../agents/subagents/registry/subagent-registry-publication.js";
import {
  getSubagentRunByRunId,
  resetSubagentRegistryForTests,
} from "../agents/subagents/registry/subagent-registry.test-helpers.js";
import type { SubagentRunRecord } from "../agents/subagents/registry/subagent-registry.types.js";
import { sessionControllerMailboxes } from "../sessions/session-controller.mailbox.js";
import { setUserProfileRole } from "../state/user-profile-writes.worker.js";
import { ensureProfileForEmail } from "../state/user-profiles.js";
import { createOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import { GATEWAY_CLIENT_MODES, GATEWAY_CLIENT_NAMES } from "../utils/message-channel.js";
import {
  buildSubagentStopConfig,
  subagentStopProxyHeaders,
} from "./server.subagent-stop-settlement.product.test-support.js";
import { disconnectGatewayClient, startGatewayWithClient } from "./test-helpers.e2e.js";

vi.mock("../agents/subagents/announce/subagent-announce-output.js", { spy: true });

const parentPrompt = "Run the exact Stop then kept-child follow-up proof.";
const stoppedTask = "Hold this first child execution until it is stopped.";
const followupTask = "Finish this successor execution with the required success marker.";
const stoppedResult = "STOPPED_RUN_MUST_NOT_DELIVER";
const successorResult = "AFTER_STOP_OK";
const deletedTask = "Pause until this child session is deleted.";
const siblingTask = "Finish the sibling task after the other child is retired.";
const siblingResult = "AFTER_RETIREMENT_OK";
const pauseNotice = "Paused awaiting continuation.";
const proxyUser = "subagent-stop-administrator@example.test";

function toolCallEvents(name: string, args: Record<string, unknown>, sequence: number) {
  const responseId = `resp_stop_followup_${sequence}`;
  const itemId = `fc_stop_followup_${sequence}`;
  const callId = `call_stop_followup_${sequence}`;
  const argumentsText = JSON.stringify(args);
  const item = {
    type: "function_call",
    id: itemId,
    call_id: callId,
    name,
    arguments: argumentsText,
  };
  return [
    {
      type: "response.created",
      response: { id: responseId, object: "response", status: "in_progress", output: [] },
    },
    { type: "response.output_item.added", output_index: 0, item: { ...item, arguments: "" } },
    {
      type: "response.function_call_arguments.delta",
      item_id: itemId,
      output_index: 0,
      delta: argumentsText,
    },
    {
      type: "response.function_call_arguments.done",
      item_id: itemId,
      output_index: 0,
      arguments: argumentsText,
    },
    { type: "response.output_item.done", output_index: 0, item },
    {
      type: "response.completed",
      response: {
        id: responseId,
        object: "response",
        status: "completed",
        output: [item],
        usage: { input_tokens: 8, output_tokens: 4, total_tokens: 12 },
      },
    },
  ];
}

function findRecords(
  value: unknown,
  matches: (record: Record<string, unknown>) => boolean,
): Record<string, unknown>[] {
  if (typeof value === "string") {
    try {
      return findRecords(JSON.parse(value) as unknown, matches);
    } catch {
      return [];
    }
  }
  if (Array.isArray(value)) {
    return value.flatMap((item) => findRecords(item, matches));
  }
  if (!value || typeof value !== "object") {
    return [];
  }
  const record = value as Record<string, unknown>;
  return [
    ...(matches(record) ? [record] : []),
    ...Object.values(record).flatMap((item) => findRecords(item, matches)),
  ];
}

const spawnReceipts = (body: string) =>
  findRecords(
    JSON.parse(body) as unknown,
    (record) => typeof record.childSessionKey === "string" && typeof record.runId === "string",
  ).map((record) => ({
    runId: record.runId as string,
    sessionKey: record.childSessionKey as string,
  }));

async function readRequestBody(request: IncomingMessage): Promise<string> {
  let body = "";
  for await (const chunk of request) {
    body += typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
  }
  return body;
}

/** Serves one scripted parent plus its children; unscripted turns answer NO_REPLY. */
async function startScriptedModel(
  handle: (
    body: string,
    request: IncomingMessage,
    response: ServerResponse,
    sequence: number,
  ) => Promise<boolean>,
) {
  let responseSequence = 0;
  const server = createServer((request, response) => {
    void serve(request, response).catch((error: unknown) => {
      if (!response.headersSent) {
        response.writeHead(500, { "content-type": "application/json" });
      }
      response.end(JSON.stringify({ error: { message: String(error) } }));
    });
  });
  async function serve(request: IncomingMessage, response: ServerResponse) {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    if (request.method === "GET" && url.pathname === "/v1/models") {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ data: [{ id: "stop-proof", object: "model" }] }));
      return;
    }
    if (request.method !== "POST" || url.pathname !== "/v1/responses") {
      response.writeHead(404).end();
      return;
    }
    const body = await readRequestBody(request);
    responseSequence += 1;
    if (!(await handle(body, request, response, responseSequence))) {
      writeOpenAiResponsesText(response, {
        text: "NO_REPLY",
        messageId: `fallback_${responseSequence}`,
        responseId: `fallback_response_${responseSequence}`,
      });
    }
  }
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${address.port}/v1`,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
    },
  };
}

/** A child that holds its model call until cancellation, then attempts a late result. */
async function holdUntilStopped(
  request: IncomingMessage,
  response: ServerResponse,
  started: () => void,
) {
  const stopped = createDeferred();
  started();
  request.once("aborted", () => stopped.resolve());
  response.once("close", () => stopped.resolve());
  await stopped.promise;
  writeOpenAiResponsesText(response, {
    text: stoppedResult,
    messageId: "stopped_child_late_result",
    responseId: "stopped_child_late_result_response",
  });
}

/** Requester continuations answer visibly so delivery records a visible final. */
function writeVisibleReply(response: ServerResponse, text: string, sequence: number) {
  writeOpenAiResponsesText(response, {
    text,
    messageId: `requester_delivery_${sequence}`,
    responseId: `requester_delivery_response_${sequence}`,
  });
  return true;
}

function writeToolCall(
  response: ServerResponse,
  name: string,
  args: Record<string, unknown>,
  sequence: number,
) {
  writeOpenAiResponsesSse(response, toolCallEvents(name, args, sequence));
  return true;
}

function startStopFollowupModel(sourceState: "active" | "paused") {
  const firstChildStarted = createDeferred();
  const sourceIdentified = createDeferred<{ runId: string }>();
  const allowParentStop = createDeferred();
  const successorAccepted = createDeferred<string>();
  const allowParentYield = createDeferred();
  const completionDelivered = createDeferred();
  const counts = { successorDeliveries: 0, stoppedDeliveries: 0 };
  let parentRequestCount = 0;
  return {
    firstChildStarted: firstChildStarted.promise,
    sourceIdentified: sourceIdentified.promise,
    allowParentStop: () => allowParentStop.resolve(),
    successorAccepted: successorAccepted.promise,
    completionDelivered: completionDelivered.promise,
    allowParentYield: () => allowParentYield.resolve(),
    deliveryCounts: () => ({ ...counts }),
    release: () => allowParentYield.resolve(),
    handle: async (
      body: string,
      request: IncomingMessage,
      response: ServerResponse,
      sequence: number,
    ) => {
      if (!body.includes(parentPrompt)) {
        // A follow-up to the kept child starts a distinct successor execution.
        if (body.includes(followupTask)) {
          writeOpenAiResponsesText(response, {
            text: successorResult,
            messageId: "stop_followup_successor",
            responseId: "stop_followup_successor_response",
          });
          return true;
        }
        if (!body.includes(stoppedTask)) {
          return false;
        }
        if (sourceState === "active") {
          await holdUntilStopped(request, response, () => firstChildStarted.resolve());
          return true;
        }
        return body.includes("function_call_output")
          ? false
          : writeToolCall(response, "sessions_yield", { waitFor: "message" }, sequence);
      }
      // A stale notice or late result from the stopped source must never reach the requester.
      if (body.includes(stoppedResult) || body.includes(pauseNotice)) {
        counts.stoppedDeliveries += 1;
        return false;
      }
      // Completion reaches the requester through a fresh delivery-only agent turn.
      if (body.includes(successorResult)) {
        counts.successorDeliveries += 1;
        completionDelivered.resolve();
        return writeVisibleReply(response, "PARENT_RECEIVED_SUCCESSOR", sequence);
      }
      parentRequestCount += 1;
      if (parentRequestCount === 1) {
        return writeToolCall(
          response,
          "sessions_spawn",
          { task: stoppedTask, context: "isolated", cleanup: "keep", completionTarget: "parent" },
          sequence,
        );
      }
      const [child] = spawnReceipts(body);
      if (!child) {
        throw new Error("Parent transport did not retain the child spawn receipt");
      }
      sourceIdentified.resolve({ runId: child.runId });
      if (parentRequestCount === 2) {
        await allowParentStop.promise;
        return writeToolCall(
          response,
          "sessions",
          { action: "stop", sessionKey: child.sessionKey, runId: child.runId },
          sequence,
        );
      }
      if (parentRequestCount === 3) {
        return writeToolCall(
          response,
          "sessions_send",
          {
            sessionKey: child.sessionKey,
            mode: "followup",
            timeoutSeconds: 0,
            message: followupTask,
          },
          sequence,
        );
      }
      if (parentRequestCount === 4) {
        const [followup] = findRecords(
          JSON.parse(body) as unknown,
          (record) =>
            record.status === "accepted" &&
            record.targetDisposition === "queued" &&
            typeof record.runId === "string",
        );
        if (!followup) {
          throw new Error("Parent transport did not retain the queued follow-up receipt");
        }
        successorAccepted.resolve(followup.runId as string);
        await allowParentYield.promise;
        return writeToolCall(response, "sessions_yield", { waitFor: "message" }, sequence);
      }
      return false;
    },
  };
}

/** One parent with a paused child it later retires and a sibling that completes. */
function startRetiredSiblingModel(retirement: "delete" | "stop") {
  const retiredChild = createDeferred<{ runId: string; sessionKey: string }>();
  const retiredChildStarted = createDeferred();
  const siblingChild = createDeferred<{ runId: string; sessionKey: string }>();
  const allowParentRetirement = createDeferred();
  const siblingDelivered = createDeferred();
  const counts = { siblingDeliveries: 0, retiredDeliveries: 0 };
  let parentRequestCount = 0;
  const childSpawn = (task: string) => ({
    task,
    context: "isolated",
    cleanup: "keep",
    ...(retirement === "stop" ? { completionTarget: "parent" } : {}),
  });
  return {
    retiredChild: retiredChild.promise,
    retiredChildStarted: retiredChildStarted.promise,
    siblingChild: siblingChild.promise,
    siblingDelivered: siblingDelivered.promise,
    allowParentRetirement: () => allowParentRetirement.resolve(),
    deliveryCounts: () => ({ ...counts }),
    release: () => allowParentRetirement.resolve(),
    handle: async (
      body: string,
      request: IncomingMessage,
      response: ServerResponse,
      sequence: number,
    ) => {
      if (!body.includes(parentPrompt)) {
        if (body.includes(siblingTask)) {
          writeOpenAiResponsesText(response, {
            text: siblingResult,
            messageId: "retirement_sibling_result",
            responseId: "retirement_sibling_result_response",
          });
          return true;
        }
        if (!body.includes(deletedTask) && !body.includes(stoppedTask)) {
          return false;
        }
        if (retirement === "stop") {
          await holdUntilStopped(request, response, () => retiredChildStarted.resolve());
          return true;
        }
        retiredChildStarted.resolve();
        return body.includes("function_call_output")
          ? false
          : writeToolCall(response, "sessions_yield", { waitFor: "message" }, sequence);
      }
      if (body.includes(pauseNotice) || body.includes(stoppedResult)) {
        counts.retiredDeliveries += 1;
        return false;
      }
      if (body.includes(siblingResult)) {
        counts.siblingDeliveries += 1;
        siblingDelivered.resolve();
        return writeVisibleReply(response, "PARENT_RECEIVED_SIBLING", sequence);
      }
      parentRequestCount += 1;
      if (parentRequestCount === 1) {
        return writeToolCall(
          response,
          "sessions_spawn",
          childSpawn(retirement === "stop" ? stoppedTask : deletedTask),
          sequence,
        );
      }
      const children = spawnReceipts(body);
      if (parentRequestCount === 2) {
        const [child] = children;
        if (!child) {
          throw new Error("Parent did not retain the retired-child spawn receipt");
        }
        retiredChild.resolve(child);
        return writeToolCall(response, "sessions_spawn", childSpawn(siblingTask), sequence);
      }
      const retired = await retiredChild.promise;
      if (parentRequestCount === 3) {
        const child = children.find((entry) => entry.runId !== retired.runId);
        if (!child) {
          throw new Error("Parent did not retain the sibling spawn receipt");
        }
        siblingChild.resolve(child);
        await allowParentRetirement.promise;
        return retirement === "stop"
          ? writeToolCall(
              response,
              "sessions",
              { action: "stop", sessionKey: retired.sessionKey, runId: retired.runId },
              sequence,
            )
          : writeToolCall(response, "sessions_yield", { waitFor: "message" }, sequence);
      }
      if (parentRequestCount === 4 && retirement === "stop") {
        // Yield immediately behind the Stop, while the stopped sibling still settles.
        return writeToolCall(response, "sessions_yield", { waitFor: "message" }, sequence);
      }
      return false;
    },
  };
}

const onlyChildTask = "Hold the only child of a yielded parent until it ends.";
const resolvedCohortMarker = "ended without a result";

/** A parent yields on its only child; the child holds, or pauses, until something ends it. */
function startYieldedOnlyChildModel(child: "hold" | "pause") {
  const spawnedChild = createDeferred<{ runId: string; sessionKey: string }>();
  const childStarted = createDeferred();
  const continued = createDeferred();
  const continuations: string[] = [];
  let parentRequestCount = 0;
  return {
    child: spawnedChild.promise,
    childStarted: childStarted.promise,
    continued: continued.promise,
    continuations: () => [...continuations],
    handle: async (
      body: string,
      request: IncomingMessage,
      response: ServerResponse,
      sequence: number,
    ) => {
      if (!body.includes(parentPrompt)) {
        if (!body.includes(onlyChildTask)) {
          return false;
        }
        if (child === "hold") {
          await holdUntilStopped(request, response, () => childStarted.resolve());
          return true;
        }
        childStarted.resolve();
        return body.includes("function_call_output")
          ? false
          : writeToolCall(response, "sessions_yield", { waitFor: "message" }, sequence);
      }
      if (body.includes(stoppedResult)) {
        throw new Error("A stopped child result reached its requester");
      }
      if (body.includes(resolvedCohortMarker)) {
        continuations.push(body);
        continued.resolve();
        return writeVisibleReply(response, "PARENT_RECEIVED_RESOLVED_COHORT", sequence);
      }
      parentRequestCount += 1;
      if (parentRequestCount === 1) {
        return writeToolCall(
          response,
          "sessions_spawn",
          { task: onlyChildTask, context: "isolated", cleanup: "keep" },
          sequence,
        );
      }
      if (parentRequestCount === 2) {
        const [spawned] = spawnReceipts(body);
        if (!spawned) {
          throw new Error("Parent did not retain the only-child spawn receipt");
        }
        spawnedChild.resolve(spawned);
        return writeToolCall(response, "sessions_yield", { waitFor: "message" }, sequence);
      }
      return false;
    },
  };
}

const awaitedChildTask = "Hold the awaited child of a finished parent turn until it is stopped.";

/** A parent spawns one awaited child and ends its turn without yielding; the child holds. */
function startAwaitedChildModel() {
  const spawnedChild = createDeferred<{ runId: string; sessionKey: string }>();
  const childStarted = createDeferred();
  const notified = createDeferred();
  const notices: string[] = [];
  let parentRequestCount = 0;
  return {
    child: spawnedChild.promise,
    childStarted: childStarted.promise,
    notified: notified.promise,
    notices: () => [...notices],
    handle: async (
      body: string,
      request: IncomingMessage,
      response: ServerResponse,
      sequence: number,
    ) => {
      if (!body.includes(parentPrompt)) {
        if (!body.includes(awaitedChildTask)) {
          return false;
        }
        await holdUntilStopped(request, response, () => childStarted.resolve());
        return true;
      }
      if (body.includes(stoppedResult)) {
        throw new Error("A stopped child result reached its requester");
      }
      parentRequestCount += 1;
      if (parentRequestCount === 1) {
        return writeToolCall(
          response,
          "sessions_spawn",
          { task: awaitedChildTask, context: "isolated", cleanup: "keep" },
          sequence,
        );
      }
      if (parentRequestCount === 2) {
        const [spawned] = spawnReceipts(body);
        if (!spawned) {
          throw new Error("Parent did not retain the awaited-child spawn receipt");
        }
        spawnedChild.resolve(spawned);
        return writeVisibleReply(response, "PARENT_AWAITS_CHILD", sequence);
      }
      // Every later parent request is a turn the stopped child's outcome started.
      notices.push(body);
      notified.resolve();
      return writeVisibleReply(response, "PARENT_RECEIVED_STOPPED_CHILD", sequence);
    },
  };
}

async function startProofGateway(modelUrl: string, label: string) {
  const state = await createOpenClawTestState({ label });
  setUserProfileRole(ensureProfileForEmail(proxyUser).id, "administrator");
  const cfg = buildSubagentStopConfig(state.workspaceDir, {
    certPath: await state.writeText("tls/cert.pem", TEST_TLS_CERT_PEM),
    keyPath: await state.writeText("tls/key.pem", TEST_TLS_KEY_PEM),
  });
  const provider = cfg.models?.providers?.synthetic;
  if (!provider) {
    throw new Error("Missing synthetic model provider");
  }
  provider.baseUrl = modelUrl;
  provider.request = { allowPrivateNetwork: true };
  const gateway = await startGatewayWithClient({
    cfg,
    configPath: state.configPath,
    auth: cfg.gateway?.auth,
    edgeAuthHeaders: subagentStopProxyHeaders,
    secure: true,
    tlsFingerprint: new X509Certificate(TEST_TLS_CERT_PEM).fingerprint256,
    clientName: GATEWAY_CLIENT_NAMES.CONTROL_UI,
    mode: GATEWAY_CLIENT_MODES.WEBCHAT,
    origin: "https://control.example.com",
    scopes: ["operator.admin", "operator.read", "operator.write"],
  });
  await gateway.server.startupSettled;
  return {
    client: gateway.client,
    close: async () => {
      await disconnectGatewayClient(gateway.client);
      await gateway.server.close({ reason: `${label} proof complete` });
      await state.cleanup();
    },
  };
}

/** Resolves once the published registry row satisfies the predicate. */
async function waitForRun(runId: string, matches: (row: SubagentRunRecord) => boolean) {
  const reached = createDeferred();
  const observe = () => {
    const row = getSubagentRunByRunId(runId);
    if (row && matches(row)) {
      reached.resolve();
    }
  };
  const stop = subscribeSubagentRunChanges("persistence", observe);
  try {
    observe();
    await reached.promise;
  } finally {
    stop();
  }
}

/** Resolves once the row is gone or satisfies the predicate. */
async function waitForRunOrRemoval(runId: string, matches: (row: SubagentRunRecord) => boolean) {
  const reached = createDeferred();
  const observe = () => {
    const row = getSubagentRunByRunId(runId);
    if (!row || matches(row)) {
      reached.resolve();
    }
  };
  const stop = subscribeSubagentRunChanges("persistence", observe);
  try {
    observe();
    await reached.promise;
  } finally {
    stop();
  }
}

/** Controller inputs that a run still owes its requester, by stable reservation identity. */
function owedControllerInputs(runId: string) {
  return [...sessionControllerMailboxes()].flatMap((mailbox) =>
    mailbox.entries.filter(
      (input) => input.phase !== "consumed" && input.sourceTurnId?.includes(runId) === true,
    ),
  );
}

afterEach(async () => {
  await resetSubagentRegistryForTests({ persist: false });
});

it.each(["active", "paused"] as const)(
  "delivers exactly one kept-child successor after exact-run Stop of an %s source",
  async (sourceState) => {
    const script = startStopFollowupModel(sourceState);
    const model = await startScriptedModel(script.handle);
    const gateway = await startProofGateway(model.url, `subagent-stop-followup-${sourceState}`);
    const parentRunId = `subagent-stop-followup-parent-${sourceState}`;
    try {
      await expect(
        gateway.client.request("chat.send", {
          sessionKey: `agent:main:${parentRunId}`,
          message: parentPrompt,
          idempotencyKey: parentRunId,
          deliver: false,
        }),
      ).resolves.toMatchObject({ runId: parentRunId, status: "started" });
      const source = await script.sourceIdentified;
      if (sourceState === "active") {
        await script.firstChildStarted;
      } else {
        await waitForRun(source.runId, (row) => row.pauseReason === "sessions_yield");
      }
      script.allowParentStop();
      const successorRunId = await script.successorAccepted;
      await expect(
        gateway.client.request("agent.wait", { runId: successorRunId, timeoutMs: 30_000 }),
      ).resolves.toMatchObject({ status: "ok" });
      script.allowParentYield();
      await expect(
        gateway.client.request("agent.wait", { runId: parentRunId, timeoutMs: 30_000 }),
      ).resolves.toMatchObject({ status: "ok" });
      await waitForRun(successorRunId, (row) => row.delivery?.status === "delivered");
      await script.completionDelivered;
      expect(script.deliveryCounts()).toEqual({ successorDeliveries: 1, stoppedDeliveries: 0 });
      expect(owedControllerInputs(source.runId), "the stopped source owes nothing").toEqual([]);
      expect(owedControllerInputs(successorRunId), "the successor delivered once").toEqual([]);
    } finally {
      script.release();
      await gateway.close();
      await model.close();
    }
  },
);

it.each(["delete", "stop"] as const)(
  "retires a %s child without blocking its completed sibling or the parent yield",
  async (retirement) => {
    const script = startRetiredSiblingModel(retirement);
    const model = await startScriptedModel(script.handle);
    const gateway = await startProofGateway(model.url, `subagent-retired-${retirement}`);
    const parentRunId = `subagent-retired-${retirement}-parent`;
    try {
      await expect(
        gateway.client.request("chat.send", {
          sessionKey: `agent:main:${parentRunId}`,
          message: parentPrompt,
          idempotencyKey: parentRunId,
          deliver: false,
        }),
      ).resolves.toMatchObject({ runId: parentRunId, status: "started" });
      const retired = await script.retiredChild;
      await script.retiredChildStarted;
      if (retirement === "delete") {
        await waitForRun(retired.runId, (row) => row.pauseReason === "sessions_yield");
      }
      const sibling = await script.siblingChild;
      await expect(
        gateway.client.request("agent.wait", { runId: sibling.runId, timeoutMs: 30_000 }),
      ).resolves.toMatchObject({ status: "ok" });
      if (retirement === "delete") {
        await expect(
          gateway.client.request("sessions.delete", { key: retired.sessionKey }),
        ).resolves.toMatchObject({ ok: true, deleted: true });
        expect(owedControllerInputs(retired.runId), "deletion retires its inputs").toEqual([]);
      }
      script.allowParentRetirement();
      await expect(
        gateway.client.request("agent.wait", { runId: parentRunId, timeoutMs: 30_000 }),
      ).resolves.toMatchObject({ status: "ok" });
      await waitForRun(sibling.runId, (row) => row.delivery?.status === "delivered");
      await script.siblingDelivered;
      expect(script.deliveryCounts()).toEqual({ siblingDeliveries: 1, retiredDeliveries: 0 });
      expect(owedControllerInputs(retired.runId), "the retired child owes nothing").toEqual([]);
      expect(owedControllerInputs(sibling.runId), "the sibling delivered once").toEqual([]);
    } finally {
      script.release();
      await gateway.close();
      await model.close();
    }
  },
);

it.each([
  { ending: "child stop", continuations: 1 },
  { ending: "active child stop", continuations: 1 },
  { ending: "child delete", continuations: 1 },
  { ending: "parent stop", continuations: 0 },
] as const)(
  "resolves a yielded parent's only child after $ending with $continuations continuation",
  async ({ ending, continuations }) => {
    // Stop reaches a dormant child through its registry row. Stop of an active child is
    // confirmed when its controller operation settles, with no reconciliation delay.
    const script = startYieldedOnlyChildModel(ending === "child stop" ? "pause" : "hold");
    const model = await startScriptedModel(script.handle);
    const label = `subagent-only-child-${ending.replaceAll(" ", "-")}`;
    const gateway = await startProofGateway(model.url, label);
    const parentRunId = `${label}-parent`;
    const parentSessionKey = `agent:main:${parentRunId}`;
    try {
      await expect(
        gateway.client.request("chat.send", {
          sessionKey: parentSessionKey,
          message: parentPrompt,
          idempotencyKey: parentRunId,
          deliver: false,
        }),
      ).resolves.toMatchObject({ runId: parentRunId, status: "started" });
      const child = await script.child;
      await script.childStarted;
      await expect(
        gateway.client.request("agent.wait", { runId: parentRunId, timeoutMs: 30_000 }),
      ).resolves.toMatchObject({ status: "ok" });
      await waitForRun(
        child.runId,
        (row) =>
          row.requesterSettleWake?.requesterYieldBatch === true &&
          (ending !== "child stop" || row.pauseReason === "sessions_yield"),
      );
      if (ending === "child stop" || ending === "active child stop") {
        await expect(
          gateway.client.request("sessions.abort", { key: child.sessionKey, runId: child.runId }),
        ).resolves.toMatchObject({ ok: true, status: "aborted" });
      } else if (ending === "child delete") {
        await expect(
          gateway.client.request("sessions.delete", { key: child.sessionKey }),
        ).resolves.toMatchObject({ ok: true, deleted: true });
      } else {
        await gateway.client.request("sessions.abort", { key: parentSessionKey });
      }
      // The cohort resolves when its last member's wake is settled or retired.
      await waitForRunOrRemoval(
        child.runId,
        (row) => row.execution.status === "terminal" && row.requesterSettleWake === undefined,
      );
      // The continuation is a later requester turn; the resolved wake only reserves it.
      if (continuations > 0) {
        await script.continued;
      }
      const delivered = script.continuations();
      expect(delivered).toHaveLength(continuations);
      for (const body of delivered) {
        expect(body).toContain(onlyChildTask);
      }
      expect(owedControllerInputs(child.runId), "the resolved cohort owes nothing").toEqual([]);
    } finally {
      await gateway.close();
      await model.close();
    }
  },
);

it("tells a parent once when the user stops the awaited child of its finished turn", async () => {
  const script = startAwaitedChildModel();
  const model = await startScriptedModel(script.handle);
  const gateway = await startProofGateway(model.url, "subagent-awaited-child-stop");
  const parentRunId = "subagent-awaited-child-stop-parent";
  try {
    await expect(
      gateway.client.request("chat.send", {
        sessionKey: `agent:main:${parentRunId}`,
        message: parentPrompt,
        idempotencyKey: parentRunId,
        deliver: false,
      }),
    ).resolves.toMatchObject({ runId: parentRunId, status: "started" });
    const child = await script.child;
    await script.childStarted;
    await expect(
      gateway.client.request("agent.wait", { runId: parentRunId, timeoutMs: 30_000 }),
    ).resolves.toMatchObject({ status: "ok" });
    expect(getSubagentRunByRunId(child.runId)?.requesterSettleWake?.requesterYieldBatch).not.toBe(
      true,
    );
    const outputPolls = vi.mocked(announceOutput.readLatestSubagentOutputWithRetry);
    outputPolls.mockClear();
    await expect(
      gateway.client.request("sessions.abort", { key: child.sessionKey, runId: child.runId }),
    ).resolves.toMatchObject({ ok: true, status: "aborted" });
    await script.notified;
    // A killed child has no further output: its notice is sent without polling for any.
    expect(outputPolls).not.toHaveBeenCalled();
    await waitForRunOrRemoval(
      child.runId,
      (row) =>
        row.execution.status === "terminal" &&
        typeof row.cleanupCompletedAt === "number" &&
        row.requesterSettleWake === undefined,
    );
    const notices = script.notices();
    expect(notices).toHaveLength(1);
    // The notice reports the stopped outcome; the task text alone is already in the history.
    expect(notices[0]).toContain(
      `ended without a result: 1 stopped, killed, or deleted before finishing (${awaitedChildTask})`,
    );
    expect(notices[0]).toContain("(no output)");
    expect(owedControllerInputs(child.runId), "the stopped child owes nothing").toEqual([]);
  } finally {
    await gateway.close();
    await model.close();
  }
});
