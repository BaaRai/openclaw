import fs from "node:fs";
import { expect, it, vi } from "vitest";
import { awaitGateBeforeSettlement, createDeferred } from "../../../test/helpers/promise.js";
import { getOpenIncognitoAgentDatabase } from "../../state/openclaw-agent-db-lifecycle.js";
import { resolveIncognitoOpenClawAgentSqlitePath } from "../../state/openclaw-agent-db.paths.js";
import { readAcpSessionEntry, upsertAcpSessionMeta } from "../runtime/session-meta.js";
import { withAcpCancellationFixture } from "./manager.cancel-session.worker.test-support.js";
import { DEFAULT_DEPS } from "./manager.types.js";

it.each([
  ["durable", "before-call"],
  ["durable", "cached-status"],
  ["durable", "cancel-rpc"],
  ["incognito", "cached-status"],
  ["incognito", "cancel-rpc"],
  ["incognito", "metadata-write"],
] as const)(
  "preserves a %s same-entry runtime replacement while %s is pending and permits retry",
  async (sourceKind, boundary) => {
    await withAcpCancellationFixture(
      async (f) => {
        const path = resolveIncognitoOpenClawAgentSqlitePath({ agentId: "main", env: f.state.env });
        const memory = getOpenIncognitoAgentDatabase("main", path);
        const assertMemoryCurrent = () => {
          if (sourceKind === "incognito") {
            expect(memory).toBeDefined();
            expect(memory?.db.location()).toBeFalsy();
            expect(getOpenIncognitoAgentDatabase("main", path)).toBe(memory);
            expect(fs.existsSync(path)).toBe(false);
          }
        };
        assertMemoryCurrent();
        // Prime the real cache so ensure cannot publish a new handle over the replacement.
        await f.manager.getSessionStatus(f.target);
        expect(f.ensureSession).toHaveBeenCalledOnce();
        const replaceRuntime = () =>
          upsertAcpSessionMeta({
            ...f.target,
            skipMaintenance: true,
            mutate: (current) => {
              if (!current) {
                throw new Error("Fixture lost its global ACP metadata before replacement.");
              }
              return { ...current, runtimeSessionName: "successor-runtime", state: "running" };
            },
          });
        if (boundary === "before-call") {
          await replaceRuntime();
        }
        const entered = createDeferred();
        const release = createDeferred();
        if (boundary === "cached-status") {
          f.getStatus.mockImplementationOnce(async () => {
            entered.resolve();
            await release.promise;
            return { summary: "ready" };
          });
        } else if (boundary === "cancel-rpc") {
          f.cancel.mockImplementationOnce(async () => {
            entered.resolve();
            await release.promise;
          });
        }
        const upsert = DEFAULT_DEPS.upsertSessionMeta;
        const writer =
          boundary === "metadata-write"
            ? vi.spyOn(DEFAULT_DEPS, "upsertSessionMeta").mockImplementationOnce(async (params) => {
                entered.resolve();
                await release.promise;
                return upsert(params);
              })
            : undefined;
        const close = vi.spyOn(f.runtime, "close");
        const cancellation = f.manager.cancelSession({
          ...f.target,
          reason: "locator-replacement",
        });
        const result = Promise.allSettled([cancellation]);
        try {
          const initialCancelCalls =
            boundary === "cached-status" || boundary === "before-call" ? 0 : 1;
          if (boundary !== "before-call") {
            await awaitGateBeforeSettlement(
              entered.promise,
              result,
              "Cancellation settled before the held runtime boundary.",
            );
            expect(f.cancel).toHaveBeenCalledTimes(initialCancelCalls);
            await replaceRuntime();
          }
          release.resolve();
          const outcomes = await result;
          const current = readAcpSessionEntry(f.target);
          expect(current?.acp).toMatchObject({
            backend: "cancellation-proof",
            runtimeSessionName: "successor-runtime",
            mode: "persistent",
            state: "running",
          });
          expect(current?.entry).toMatchObject({
            sessionId: "cancellation-session",
            lifecycleRevision: "cancellation-lifecycle",
            spawnedBy: "agent:main:main",
          });
          expect(outcomes).toMatchObject([{ status: "rejected" }]);
          expect(f.cancel).toHaveBeenCalledTimes(initialCancelCalls);
          if (initialCancelCalls === 1) {
            expect(f.cancel).toHaveBeenCalledExactlyOnceWith({
              handle: expect.objectContaining({ runtimeSessionName: "retained-runtime" }),
              reason: "locator-replacement",
            });
          }
          expect(f.ensureSession).toHaveBeenCalledOnce();
          expect(close).not.toHaveBeenCalled();
          assertMemoryCurrent();

          f.ensureSession.mockImplementationOnce(async () => ({
            sessionKey: f.target.sessionKey,
            backend: "cancellation-proof",
            runtimeSessionName: "successor-runtime",
          }));
          await f.manager.cancelSession({
            ...f.target,
            reason: "successor-retry",
          });
          expect(f.ensureSession).toHaveBeenCalledTimes(2);
          expect(f.cancel).toHaveBeenCalledTimes(initialCancelCalls + 1);
          expect(f.cancel).toHaveBeenLastCalledWith({
            handle: expect.objectContaining({ runtimeSessionName: "successor-runtime" }),
            reason: "successor-retry",
          });
          expect(readAcpSessionEntry(f.target)?.acp).toMatchObject({
            runtimeSessionName: "successor-runtime",
            state: "idle",
          });
          expect(close).not.toHaveBeenCalled();
          assertMemoryCurrent();
        } finally {
          release.resolve();
          await result;
          writer?.mockRestore();
          // Fixture disposal owns its ordinary close after the cancellation assertions.
          close.mockRestore();
        }
      },
      {
        sessionKey:
          sourceKind === "incognito"
            ? "agent:main:dashboard:incognito-locator-retry"
            : "agent:main:acp:locator-retry",
      },
    );
  },
);

it.each([
  ["durable", "named-backend"],
  ["incognito", "named-backend"],
  ["durable", "empty-backend"],
] as const)(
  "cancels the normalized %s handle from %s and persists its idle state",
  async (sourceKind, returnedBackend) => {
    await withAcpCancellationFixture(
      async (f) => {
        const path = resolveIncognitoOpenClawAgentSqlitePath({ agentId: "main", env: f.state.env });
        const memory = getOpenIncognitoAgentDatabase("main", path);
        if (sourceKind === "incognito") {
          expect(memory).toBeDefined();
          expect(memory?.db.location()).toBeFalsy();
          expect(fs.existsSync(path)).toBe(false);
        }
        f.ensureSession.mockImplementationOnce(async () => ({
          sessionKey: f.target.sessionKey,
          backend: returnedBackend === "empty-backend" ? "" : "cancellation-proof",
          runtimeSessionName: "normalized-runtime",
        }));
        const close = vi.spyOn(f.runtime, "close");
        try {
          await f.manager.cancelSession({
            ...f.target,
            reason: "normalized-cancel",
          });
          expect(f.cancel).toHaveBeenCalledExactlyOnceWith({
            handle: expect.objectContaining({
              backend: "cancellation-proof",
              runtimeSessionName: "normalized-runtime",
            }),
            reason: "normalized-cancel",
          });
          expect(readAcpSessionEntry(f.target)?.acp).toMatchObject({
            backend: "cancellation-proof",
            runtimeSessionName: "normalized-runtime",
            mode: "persistent",
            state: "idle",
          });
          expect(close).not.toHaveBeenCalled();
          if (sourceKind === "incognito") {
            expect(getOpenIncognitoAgentDatabase("main", path)).toBe(memory);
            expect(fs.existsSync(path)).toBe(false);
          }
        } finally {
          close.mockRestore();
        }
      },
      {
        sessionKey:
          sourceKind === "incognito"
            ? "agent:main:dashboard:incognito-locator-normalization"
            : "agent:main:acp:locator-normalization",
      },
    );
  },
);
