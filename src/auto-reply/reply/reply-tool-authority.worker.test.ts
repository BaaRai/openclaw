import { DatabaseSync } from "node:sqlite";
import { afterEach, expect, it, vi } from "vitest";
import {
  createOperationalRunInstanceRef,
  prepareAgentRunAdmission,
} from "../../agents/admitted-run-context.js";
import { withPreparedEmbeddedRunToolAuthority } from "../../agents/harness/tool-authority.runtime.js";
import { upsertSessionEntryCore } from "../../config/sessions/session-accessor.js";
import { loadSessionEntryForAdmission } from "../../config/sessions/session-accessor.sqlite-entry-admission.js";
import { projectionLane } from "../../config/sessions/session-transcript-worker-resources.js";
import * as sessionReaders from "../../gateway/session-utils-store-worker.js";
import { resolveOpenClawAgentSqlitePath } from "../../state/openclaw-agent-db.paths.js";
import { observeMainThreadSql } from "../../test-utils/main-thread-sql-spies.test-support.js";
import { withOpenClawTestState } from "../../test-utils/openclaw-test-state.js";
import { cleanupSessionStateForTest } from "../../test-utils/session-state-cleanup.js";
import { createQueueTestRun } from "./queue.test-helpers.js";
import { createTestReplyOperation } from "./reply-run-registry.test-helpers.js";
import { testing } from "./reply-run-registry.test-support.js";
import { prepareReplyToolAuthority } from "./reply-tool-authority.js";

afterEach(() => {
  testing.resetReplyRunRegistry();
  vi.restoreAllMocks();
});

const executionKey = "agent:main:authority-execution";
const policyKey = "agent:main:authority-policy";

it("prepares embedded tool authority without caller-thread SQL and refuses a closing owner", async () => {
  await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
    await upsertSessionEntryCore(
      { agentId: "main", sessionKey: policyKey },
      { sessionId: "policy", updatedAt: 1, sandboxMode: "off" },
    );
    const admission = prepareAgentRunAdmission({
      cfg: {},
      operationalRunInstance: createOperationalRunInstanceRef("worker-authority"),
      facts: {
        agentId: "main",
        runId: "worker-authority",
        ingress: { kind: "system", state: "present", boundary: "worker-authority-test" },
      },
    });
    try {
      const admittedRunContext = await admission.admit("embedded", "worker-authority-test");
      const attempt = {
        sessionId: "execution",
        sessionKey: executionKey,
        runId: "worker-authority",
        agentId: "main",
        config: {},
        sessionFile: "/tmp/authority-worker.jsonl",
        workspaceDir: state.workspaceDir,
        provider: "openai",
        modelId: "gpt-test",
        sandboxSessionKey: policyKey,
        senderIsOwner: true,
        messageProvider: "webchat",
      };
      const effects = vi.fn(async () => "admitted");
      const calls = observeMainThreadSql();
      try {
        await expect(
          withPreparedEmbeddedRunToolAuthority({ admittedRunContext }, attempt, undefined, effects),
        ).resolves.toBe("admitted");
        calls.expectIdle();
        const pending = withPreparedEmbeddedRunToolAuthority(
          { admittedRunContext },
          attempt,
          undefined,
          effects,
        );
        admission.close();
        await expect(pending).rejects.toThrow();
        expect(effects).toHaveBeenCalledTimes(1);
        calls.expectIdle();
      } finally {
        calls.restore();
      }
    } finally {
      admission.close();
    }
  });
});

it.each(["main", "policy", "borrowed"] as const)(
  "rereads %s-agent sandbox policy after a foreign commit before projecting steering",
  async (policyAgent) => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const policyAgentId = policyAgent === "borrowed" ? "main" : policyAgent;
      const classificationKey =
        policyAgent === "borrowed" ? executionKey : `agent:${policyAgent}:authority-policy`;
      await upsertSessionEntryCore(
        { agentId: policyAgentId, sessionKey: classificationKey },
        { sessionId: "policy", updatedAt: 1, sandboxMode: "off" },
      );
      // Settle setup maintenance before retaining the readers used across the foreign commit.
      await cleanupSessionStateForTest({ stateDir: state.stateDir, rootPath: state.root });
      const run = createQueueTestRun({ prompt: "authority" });
      Object.assign(run.run, {
        sessionKey: executionKey,
        runtimePolicySessionKey: classificationKey,
        agentId: "main",
        config: {
          agents: { defaults: { sandbox: { mode: "all" } }, entries: { main: {}, policy: {} } },
          tools: { sandbox: { tools: { deny: ["exec"] } } },
        },
      });
      const admission =
        policyAgent === "borrowed"
          ? await loadSessionEntryForAdmission({
              agentId: "main",
              sessionKey: classificationKey,
              env: state.env,
            })
          : undefined;
      const reader =
        admission && "kind" in admission.databaseClaim ? admission.databaseClaim.reader : undefined;
      if (admission && !reader) {
        await admission.databaseClaim.release();
        throw new Error("Expected admitted session reader");
      }
      const discovery = vi.spyOn(sessionReaders, "prepareGatewaySessionEntryReadOnlyInWorker");
      const snapshot = prepareReplyToolAuthority(run, undefined, reader);
      try {
        const operation = createTestReplyOperation({
          sessionKey: executionKey,
          sessionId: run.run.sessionId,
        });
        const runRequest = projectionLane.pool.run.bind(projectionLane.pool);
        let initialEntries = 0;
        const initialReads = vi
          .spyOn(projectionLane.pool, "run")
          .mockImplementation(async (...args) => {
            const reply = await runRequest(...args);
            if (
              reply.ok &&
              typeof reply.value === "object" &&
              reply.value !== null &&
              "kind" in reply.value &&
              reply.value.kind === "session-exact-entries"
            ) {
              initialEntries++;
            }
            return reply;
          });
        try {
          await operation.bindToolAuthoritySnapshotAsync(snapshot);
          expect(initialEntries).toBe(1);
        } finally {
          initialReads.mockRestore();
        }
        const admitted = await operation.bindToolAuthorityRouteAsync(run.run);
        const foreign = new DatabaseSync(
          resolveOpenClawAgentSqlitePath({ agentId: policyAgentId, env: state.env }),
        );
        try {
          foreign
            .prepare(
              "UPDATE session_nodes SET entry_json = json_remove(entry_json, '$.sandboxMode') WHERE session_key = ?",
            )
            .run(classificationKey);
        } finally {
          foreign.close();
        }
        const calls = observeMainThreadSql();
        try {
          discovery.mockClear();
          expect(await snapshot.fingerprintAsync(run.run)).not.toBe(admitted);
          if (reader) {
            expect(discovery).not.toHaveBeenCalled();
          }
          await expect(
            operation.projectToolAuthorityFingerprintAsync({
              senderIsOwner: run.run.senderIsOwner === true,
              disableTools: false,
              traceAuthorized: false,
            }),
          ).resolves.toBeUndefined();
          calls.expectIdle();
          expect(discovery).toHaveBeenCalled();
          if (admission) {
            await admission.databaseClaim.release();
            discovery.mockClear();
            await expect(snapshot.fingerprintAsync(run.run)).rejects.toThrow();
            expect(discovery).not.toHaveBeenCalled();
          }
        } finally {
          calls.restore();
        }
      } finally {
        discovery.mockRestore();
        await admission?.databaseClaim.release();
      }
    });
  },
);
