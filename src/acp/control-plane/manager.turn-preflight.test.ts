/** ACP preflight failures release their native turn liveness. */
import { describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import * as sessionStateEvents from "../../sessions/session-state-events.js";
import { withStateDirEnv } from "../../test-helpers/state-dir-env.js";
import { getActiveAcpTurnCount, listActiveAcpSessionsForOwner } from "./active-turns.js";
import {
  AcpSessionManager,
  baseCfg,
  createRuntime,
  hoisted,
  installAcpSessionManagerTestLifecycle,
  mockParentedAcpSessionEntries,
} from "./manager.test-helpers.js";

describe("AcpSessionManager", () => {
  installAcpSessionManagerTestLifecycle();

  it.each(["signal failure", "abort"] as const)(
    "releases only the current native turn when preflight ends with %s",
    async (reason) => {
      await withStateDirEnv("openclaw-acp-preflight-", async () => {
        const sessionKey = "agent:codex:acp:preflight-child";
        const parentSessionKey = "agent:main:main";
        const requestId = "preflight-run";
        const runtime = createRuntime();
        hoisted.requireAcpRuntimeBackendMock.mockReturnValue({
          id: "acpx",
          runtime: runtime.runtime,
        });
        mockParentedAcpSessionEntries({
          childSessionKey: sessionKey,
          parentSessionKey,
        });
        const manager = new AcpSessionManager();
        const reached = createDeferred();
        const release = createDeferred();
        const failure = new Error(`ACP preflight ${reason}`);
        const read = manager.resolveSessionAsync.bind(manager);
        const readSpy = vi.spyOn(manager, "resolveSessionAsync");
        if (reason !== "signal failure") {
          readSpy.mockImplementationOnce(async (params) => {
            reached.resolve();
            await release.promise;
            return await read(params);
          });
        }
        const signalSpy =
          reason === "signal failure"
            ? vi
                .spyOn(sessionStateEvents, "recordSessionHumanDirectMessage")
                .mockImplementationOnce(async () => {
                  reached.resolve();
                  await release.promise;
                  throw failure;
                })
            : undefined;
        const controller = new AbortController();
        const input = {
          provenance: "system" as const,
          cfg: baseCfg,
          sessionKey,
          text: "Prepare the child turn",
          mode: "prompt" as const,
          requestId,
        };
        const pending = manager.runTurn({ ...input, signal: controller.signal });
        const outcome = pending.then(
          () => ({ ok: true as const }),
          (error: unknown) => ({ ok: false as const, error }),
        );
        try {
          expect(
            await Promise.race([
              reached.promise.then(() => "preflight"),
              outcome.then(() => "settled"),
            ]),
          ).toBe("preflight");
          expect(getActiveAcpTurnCount()).toBe(1);
          expect(listActiveAcpSessionsForOwner(parentSessionKey)).toEqual(
            reason === "signal failure" ? [sessionKey] : [],
          );
          expect(runtime.ensureSession).not.toHaveBeenCalled();
          expect(hoisted.upsertAcpSessionMetaMock).not.toHaveBeenCalled();
          if (reason === "abort") {
            controller.abort();
          }
          const metadataWrites = hoisted.upsertAcpSessionMetaMock.mock.calls.length;
          release.resolve();
          const settled = await outcome;
          expect(settled.ok).toBe(false);
          expect(hoisted.upsertAcpSessionMetaMock).toHaveBeenCalledTimes(metadataWrites);
          expect(getActiveAcpTurnCount()).toBe(0);
          expect(listActiveAcpSessionsForOwner(parentSessionKey)).toEqual([]);
          expect(
            (await sessionStateEvents.listSessionStateEventsSince(sessionKey, "codex", 0, 200))
              .events,
          ).toMatchObject(
            reason === "signal failure"
              ? [{ kind: "run_failed", runId: requestId, payload: { outcome: "error" } }]
              : [],
          );
        } finally {
          release.resolve();
          await pending.catch(() => {});
          readSpy.mockRestore();
          signalSpy?.mockRestore();
        }
      });
    },
  );
});
