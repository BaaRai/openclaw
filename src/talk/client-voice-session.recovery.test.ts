import fs from "node:fs/promises";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import * as sessionEvents from "../config/sessions/session-accessor.sqlite-events.js";
import { closeOpenClawAgentDatabasesAsync } from "../state/openclaw-agent-db-lifecycle.js";
import { resolveOpenClawAgentSqlitePath } from "../state/openclaw-agent-db.paths.js";
import { captureEnv, setTestEnvValue } from "../test-utils/env.js";
import { cleanupSessionStateForTest } from "../test-utils/session-state-cleanup.js";
import { createTempHomeEnv, type TempHomeEnv } from "../test-utils/temp-home.js";
import { resetClientVoiceConfirmationStateForTest } from "./client-voice-confirmation.test-support.js";
import { prepareClientVoiceSessionClose } from "./client-voice-session-lifecycle.js";
import * as voiceSessionReads from "./client-voice-session-read.js";
import { seedSession } from "./client-voice-session.fixture.test-support.js";
import {
  appendClientVoiceTranscript,
  closeClientVoiceSession,
  closeStaleClientVoiceSessions,
  createOrResumeClientVoiceSession,
} from "./client-voice-session.js";
import { clientVoiceSessionTesting } from "./client-voice-session.test-support.js";
import { VoiceTranscriptOperationRegistry } from "./voice-transcript.js";

describe("client voice session recovery", () => {
  let home: TempHomeEnv;
  beforeEach(async () => {
    home = await createTempHomeEnv("openclaw-voice-recovery-");
  });
  afterEach(async () => {
    clientVoiceSessionTesting.reset();
    resetClientVoiceConfirmationStateForTest();
    await home.restore();
  });

  it.each([
    { phase: "recovery", replacement: false },
    { phase: "recovery", replacement: true },
    { phase: "publication", replacement: false },
    { phase: "publication", replacement: true },
  ])(
    "does not open a displaced metadata file after $phase (replacement=$replacement)",
    async ({ phase, replacement }) => {
      const target = { agentId: "main", sessionKey: "agent:main:main" };
      await seedSession(target.sessionKey);
      const voiceSessionId = createOrResumeClientVoiceSession({
        ...target,
        origin: "client",
        now: 1,
      });
      const metadataPath = resolveOpenClawAgentSqlitePath({ agentId: "main" });
      const displace = async () => {
        await closeOpenClawAgentDatabasesAsync(home.home);
        await fs.rename(metadataPath, `${metadataPath}.displaced`);
        if (replacement) {
          await fs.writeFile(metadataPath, new Uint8Array());
        }
      };
      const lookup = voiceSessionReads.lookupClientVoiceSessions;
      const publish = sessionEvents.publishTranscriptUpdate;
      const boundary =
        phase === "recovery"
          ? vi
              .spyOn(voiceSessionReads, "lookupClientVoiceSessions")
              .mockImplementationOnce(async (...args) => {
                const candidates = await lookup(...args);
                await displace();
                return candidates;
              })
          : vi
              .spyOn(sessionEvents, "publishTranscriptUpdate")
              .mockImplementationOnce(async (...args) => {
                const result = await publish(...args);
                await displace();
                return result;
              });
      try {
        if (phase === "recovery") {
          const warn = vi.fn();
          expect(
            await closeStaleClientVoiceSessions({
              agentId: "main",
              config: {},
              now: 6 * 60 * 60_000 + 2,
              warn,
            }),
          ).toBe(0);
          expect(warn).toHaveBeenCalledOnce();
        } else {
          await expect(
            appendClientVoiceTranscript({
              ...target,
              sessionTarget: { sessionKey: target.sessionKey },
              voiceSessionId,
              entryId: "before-replacement",
              role: "user",
              text: "persisted before replacement",
            }),
          ).rejects.toThrow(/identity|ENOENT/);
        }
        expect(boundary).toHaveBeenCalledOnce();
        if (replacement) {
          expect(await fs.readFile(metadataPath)).toHaveLength(0);
        } else {
          await expect(fs.stat(metadataPath)).rejects.toMatchObject({ code: "ENOENT" });
        }
      } finally {
        boundary.mockRestore();
      }
    },
  );

  it("closes stale records and leaves recent records open", async () => {
    const stale = createOrResumeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:stale",
      origin: "client",
      now: 1,
    });
    const recent = createOrResumeClientVoiceSession({
      agentId: "main",
      sessionKey: "agent:main:recent",
      origin: "client",
      now: 6 * 60 * 60_000,
    });

    expect(
      await closeStaleClientVoiceSessions({
        agentId: "main",
        config: {},
        now: 6 * 60 * 60_000 + 2,
      }),
    ).toBe(1);
    expect(clientVoiceSessionTesting.readRecord("main", stale)?.status).toBe("closed");
    expect(clientVoiceSessionTesting.readRecord("main", recent)?.status).toBe("open");
  });

  it("does not close a call resumed after the stale candidate read", async () => {
    const now = 6 * 60 * 60_000 + 2;
    const target = { agentId: "main", sessionKey: "agent:main:main", origin: "client" as const };
    const voiceSessionId = createOrResumeClientVoiceSession({ ...target, now: 1 });
    const lookup = voiceSessionReads.lookupClientVoiceSessions;
    const read = vi
      .spyOn(voiceSessionReads, "lookupClientVoiceSessions")
      .mockImplementationOnce(async (request) => {
        const candidates = await lookup(request);
        createOrResumeClientVoiceSession({ ...target, voiceSessionId, now });
        return candidates;
      });
    try {
      expect(await closeStaleClientVoiceSessions({ agentId: "main", config: {}, now })).toBe(0);
      expect(clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.status).toBe("open");
    } finally {
      read.mockRestore();
    }
  });

  it("honors an explicit close that joins skipped stale recovery", async () => {
    const now = 6 * 60 * 60_000 + 2;
    const target = { agentId: "main", sessionKey: "agent:main:main", origin: "client" as const };
    const voiceSessionId = createOrResumeClientVoiceSession({ ...target, now: 1 });
    const entered = createDeferred();
    const release = createDeferred();
    // oxlint-disable-next-line typescript/unbound-method -- Invoked below with the original registry receiver.
    const close = VoiceTranscriptOperationRegistry.prototype.close;
    const barrier = vi
      .spyOn(VoiceTranscriptOperationRegistry.prototype, "close")
      .mockImplementationOnce(function (this: VoiceTranscriptOperationRegistry, key, operation) {
        return close.call(this, key, async () => {
          entered.resolve();
          await release.promise;
          await operation();
        });
      });
    const stale = closeStaleClientVoiceSessions({ agentId: "main", config: {}, now });
    try {
      await entered.promise;
      createOrResumeClientVoiceSession({ ...target, voiceSessionId, now });
      const explicit = closeClientVoiceSession({ ...target, voiceSessionId, config: {}, now });
      release.resolve();
      await Promise.all([stale, explicit]);
      expect(clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.status).toBe("closed");
    } finally {
      release.resolve();
      await stale;
      barrier.mockRestore();
    }
  });

  it.each(["original", "successor"] as const)(
    "keeps stale recovery in its original admission when %s closes after lookup",
    async (closed) => {
      const now = 6 * 60 * 60_000 + 2;
      const target = { agentId: "main", sessionKey: "agent:main:main", origin: "client" as const };
      const voiceSessionId = createOrResumeClientVoiceSession({ ...target, now: 1 });
      const originalClose = prepareClientVoiceSessionClose();
      let successorClose: ReturnType<typeof prepareClientVoiceSessionClose> | undefined;
      const successor = path.join(home.home, "successor");
      const env = captureEnv(["OPENCLAW_STATE_DIR"]);
      const lookup = voiceSessionReads.lookupClientVoiceSessions;
      const read = vi
        .spyOn(voiceSessionReads, "lookupClientVoiceSessions")
        .mockImplementationOnce(async (...args) => {
          const candidates = await lookup(...args);
          setTestEnvValue("OPENCLAW_STATE_DIR", successor);
          createOrResumeClientVoiceSession({ ...target, voiceSessionId, now: 1 });
          successorClose = prepareClientVoiceSessionClose();
          await (closed === "original" ? originalClose : successorClose).drain();
          return candidates;
        });
      try {
        const warn = vi.fn();
        expect(
          await closeStaleClientVoiceSessions({ agentId: "main", config: {}, now, warn }),
        ).toBe(closed === "original" ? 0 : 1);
        expect(warn).toHaveBeenCalledTimes(closed === "original" ? 1 : 0);
        expect(clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.status).toBe("open");
        env.restore();
        expect(clientVoiceSessionTesting.readRecord("main", voiceSessionId)?.status).toBe(
          closed === "original" ? "open" : "closed",
        );
      } finally {
        read.mockRestore();
        env.restore();
        await Promise.all([originalClose.drain(), successorClose?.drain()]);
        await cleanupSessionStateForTest({ stateDir: successor, rootPath: successor });
      }
    },
  );
});
