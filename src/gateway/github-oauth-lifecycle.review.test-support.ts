import { describe, expect, it, type Mock } from "vitest";
import {
  inspectGitHubOAuthRecord,
  writeGitHubOAuthRecord,
  type GitHubOAuthRecord,
} from "../agents/github-oauth-records.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import type { SqliteWorkerAdmissionRequest } from "../infra/sqlite-worker-operation-admission.js";
import type { createGitHubOAuthLifecycle } from "./github-oauth-lifecycle.js";
import {
  ACCOUNT,
  configForScope,
  identity,
  NEW_PROFILE,
  oauthRecord,
  OLD_PROFILE,
  OTHER_PROFILE,
  TOKENS,
} from "./github-oauth-lifecycle.test-support.js";

export function installGitHubOAuthReviewRegressionTests(harness: {
  createLifecycle: () => ReturnType<typeof createGitHubOAuthLifecycle>;
  setConfig: (config: OpenClawConfig) => void;
  mocks: {
    beforeWriteAdmission: Mock<(stage: SqliteWorkerAdmissionRequest["stage"]) => void>;
    afterDeleteRecord: Mock<() => void>;
    clearVerificationCache: Mock;
    refreshToken: Mock;
    refreshProfile: Mock;
    removeProfile: Mock;
    writeOAuthRecord: Mock<
      (
        write: (record: GitHubOAuthRecord) => Promise<void>,
        record: GitHubOAuthRecord,
      ) => Promise<void> | void
    >;
  };
}) {
  const { mocks, createLifecycle, setConfig } = harness;
  const pendingInitial = {
    requestId: `github-device-${"a".repeat(32)}`,
    scope: "system" as const,
    agentId: "main",
    expectedIdentity: null,
  };
  describe("GitHub OAuth maintenance authority", () => {
    it("clears verified GitHub credentials when the lifecycle stops", async () => {
      const lifecycle = createLifecycle();
      expect(mocks.clearVerificationCache).not.toHaveBeenCalled();
      await lifecycle.stop();
      expect(mocks.clearVerificationCache).toHaveBeenCalledOnce();
    });

    it.each([
      { pending: true, stage: "transaction" },
      { pending: true, stage: "commit" },
      { pending: true, stage: "profile-removal" },
      { pending: false, stage: "transaction" },
      { pending: false, stage: "commit" },
    ] as const)(
      "retains a newly selected profile during $stage cleanup (pending: $pending)",
      async ({ pending, stage }) => {
        setConfig(configForScope("system"));
        await writeGitHubOAuthRecord(oauthRecord(NEW_PROFILE, pending ? { pendingInitial } : {}));
        const lifecycle = createLifecycle();
        const selectProfile = () => setConfig(configForScope("system", identity(NEW_PROFILE)));
        if (stage === "profile-removal") {
          mocks.afterDeleteRecord.mockImplementationOnce(selectProfile);
        } else {
          mocks.beforeWriteAdmission.mockImplementation((current) => {
            if (current === stage) {
              selectProfile();
            }
          });
        }

        await lifecycle.maintain();

        expect(mocks.removeProfile).not.toHaveBeenCalled();
        if (stage !== "profile-removal") {
          expect(await inspectGitHubOAuthRecord(NEW_PROFILE)).toMatchObject({ state: "valid" });
        }
      },
    );

    it.each(["transaction", "commit"] as const)(
      "retains pending-initial state if ownership changes at the %s grant",
      async (stage) => {
        setConfig(configForScope("system", identity(NEW_PROFILE, { oauth: true })));
        await writeGitHubOAuthRecord(oauthRecord(NEW_PROFILE, { pendingInitial }));
        const lifecycle = createLifecycle();
        mocks.beforeWriteAdmission.mockImplementation((current) => {
          if (current === stage) {
            setConfig(configForScope("system", identity(OTHER_PROFILE)));
          }
        });

        await lifecycle.maintain();

        expect(await inspectGitHubOAuthRecord(NEW_PROFILE)).toMatchObject({
          state: "valid",
          record: { pendingInitial },
        });
      },
    );
  });

  describe("GitHub OAuth retained rotation", () => {
    const retainRotation = async () => {
      setConfig(configForScope("system", identity(OLD_PROFILE, { oauth: true })));
      await writeGitHubOAuthRecord(oauthRecord(OLD_PROFILE));
      mocks.refreshToken
        .mockResolvedValueOnce({ status: "refreshed", tokens: TOKENS })
        .mockResolvedValue({ status: "error", code: "bad_refresh_token" });
      mocks.writeOAuthRecord.mockImplementationOnce(() => {
        throw new Error("SQLite temporarily unavailable");
      });
      const lifecycle = createLifecycle();
      await lifecycle.refreshEffectiveIdentity("main");
      expect(mocks.refreshToken).toHaveBeenCalledOnce();
      expect(mocks.refreshProfile).not.toHaveBeenCalled();
      return lifecycle;
    };

    it("retries the first durable write from lifecycle-owned memory", async () => {
      const lifecycle = await retainRotation();
      expect(await inspectGitHubOAuthRecord(OLD_PROFILE)).toMatchObject({
        state: "valid",
        record: { refreshToken: "refresh-token-current" },
      });

      await lifecycle.maintain();

      expect(mocks.refreshToken).toHaveBeenCalledOnce();
      expect(mocks.refreshProfile).toHaveBeenCalledOnce();
      expect(await inspectGitHubOAuthRecord(OLD_PROFILE)).toMatchObject({
        state: "valid",
        record: { refreshToken: TOKENS.refreshToken },
      });
    });

    it("settles a retained rotation before a manual request can reuse its invalidated refresh token", async () => {
      const lifecycle = await retainRotation();

      await lifecycle.refreshEffectiveIdentity("main");

      expect(mocks.refreshToken).toHaveBeenCalledOnce();
      expect(mocks.refreshProfile).toHaveBeenCalledOnce();
      expect(await inspectGitHubOAuthRecord(OLD_PROFILE)).toMatchObject({
        state: "valid",
        record: { refreshToken: TOKENS.refreshToken },
      });
    });

    it("joins manual refresh to maintenance while retained credentials are materializing", async () => {
      const lifecycle = await retainRotation();
      let manual: Promise<void> | undefined;
      mocks.refreshProfile.mockImplementationOnce(async () => {
        manual = lifecycle.refreshEffectiveIdentity("main");
        // Observe the persisted intermediate state while the same profile's effect is active.
        expect(await inspectGitHubOAuthRecord(OLD_PROFILE)).toMatchObject({
          state: "valid",
          record: { pendingRefresh: true },
        });
        return ACCOUNT;
      });

      await lifecycle.maintain();
      await manual;

      expect(manual).toBeDefined();
      expect(mocks.refreshToken).toHaveBeenCalledOnce();
      expect(mocks.refreshProfile).toHaveBeenCalledOnce();
      expect(await inspectGitHubOAuthRecord(OLD_PROFILE)).toMatchObject({
        state: "valid",
        record: { refreshToken: TOKENS.refreshToken },
      });
    });
  });
}
