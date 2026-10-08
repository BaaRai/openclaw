import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  prepareGitHubPublicationOptionsRead,
  preparePersonalGitHubActionV2,
  preparePersonalGitHubAction,
  preparePersonalGitHubSessionAction,
} from "./github-personal-authorization.js";
import type { GatewayClient, GatewayRequestContext } from "./types.js";

const mocks = vi.hoisted(() => ({
  loadSession: vi.fn<typeof import("../session-utils.js").loadGatewaySessionEntryReadOnly>(),
  roleScopes: undefined as string[] | undefined,
  ownerCurrent: true,
  sourceCurrent: true,
  sourcePath: "/test/github-source.sqlite",
  openDatabase: vi.fn(() => ({ db: {} })),
  resolveOwner: vi.fn((profile: string) => profile),
}));

vi.mock("../../state/openclaw-state-db.js", () => ({
  openOpenClawStateDatabase: mocks.openDatabase,
}));
vi.mock("../../state/openclaw-state-db.paths.js", () => ({
  resolveOpenClawStateSqlitePath: () => mocks.sourcePath,
}));
vi.mock("../../state/openclaw-state-worker-context.js", () => ({
  captureOpenClawStateWorkerContext: () => ({
    admission: {
      databasePath: mocks.sourcePath,
      assertCurrent() {
        if (!mocks.sourceCurrent) {
          throw new Error("Captured physical state was replaced");
        }
      },
    },
    environment: { OPENCLAW_STATE_DIR: "/test" },
  }),
}));
vi.mock("../session-utils.js", () => ({
  loadGatewaySessionEntryReadOnly: mocks.loadSession,
}));
vi.mock("../../agents/tools/gateway-caller-context.js", () => ({
  getGatewayToolCallerIdentity: () => undefined,
}));
vi.mock("../../state/user-channel-identity-operations.js", () => ({
  prepareUserProfileRoleAuthority: async (profileId: string) => ({
    profileId,
    isCurrent: () => mocks.ownerCurrent,
    role: null,
    githubLogin: null,
  }),
}));
vi.mock("../../state/user-github-connections.js", () => ({
  resolvePersonalGitHubOwner: mocks.resolveOwner,
}));
vi.mock("../operator-role-policy.js", () => ({
  resolveOperatorRolePolicy: () => (mocks.roleScopes ? { scopes: mocks.roleScopes } : null),
  resolveOperatorRolePolicyForProfile: () =>
    mocks.roleScopes ? { scopes: mocks.roleScopes } : null,
  resolveOperatorRolePolicyForAssignment: () =>
    mocks.roleScopes ? { scopes: mocks.roleScopes } : null,
}));
vi.mock("../session-sharing.js", () => ({
  createSessionListEntryFilter: () => undefined,
  resolveSessionMutationAuthorization: () => ({}),
}));

function createRequest() {
  const client: GatewayClient = {
    connId: "github-cache-client",
    authenticatedUserProfile: {
      profileId: "profile-cache-test",
      displayName: null,
      hasAvatar: false,
      updatedAt: 1,
    },
    connect: {
      role: "operator",
      scopes: ["operator.read", "operator.write"],
      minProtocol: 1,
      maxProtocol: 1,
      client: { id: "test", mode: "test", platform: "test", version: "1" },
    },
  };
  const context = {
    getRuntimeConfig: () => ({}),
    getClientConnIds: (filter?: (candidate: GatewayClient) => boolean) =>
      new Set(!filter || filter(client) ? ["github-cache-client"] : []),
  } as Partial<GatewayRequestContext> as GatewayRequestContext;
  return {
    client,
    context,
    req: { type: "req" as const, id: "github-read", method: "sessions.github.options" },
  };
}

function sessionRead(agentId = "main") {
  return {
    cfg: {},
    canonicalKey: `agent:${agentId}:main`,
    agentId,
    storePath: "/test/sessions.json",
    store: {},
    storeKeys: [`agent:${agentId}:main`],
    entry: { sessionId: "session-cache-test", updatedAt: 1 },
    legacyKey: undefined,
  };
}

describe("GitHub publication request discovery", () => {
  beforeEach(() => {
    mocks.roleScopes = undefined;
    mocks.ownerCurrent = true;
    mocks.sourceCurrent = true;
    mocks.sourcePath = "/test/github-source.sqlite";
    mocks.openDatabase.mockClear();
    mocks.resolveOwner.mockReset().mockImplementation((profile) => profile);
    mocks.loadSession.mockReset();
    mocks.loadSession.mockReturnValue(sessionRead());
  });

  it("keeps native action reads on the captured store and rejects retargeting or replacement", () => {
    const action = preparePersonalGitHubAction(createRequest());
    expect(mocks.openDatabase).toHaveBeenLastCalledWith({
      path: "/test/github-source.sqlite",
      env: { OPENCLAW_STATE_DIR: "/test" },
    });
    mocks.openDatabase.mockClear();
    mocks.sourcePath = "/other/github-source.sqlite";
    expect(() => action.assertCurrent()).toThrow("state store changed");
    expect(mocks.openDatabase).not.toHaveBeenCalled();
    mocks.sourcePath = "/test/github-source.sqlite";
    mocks.sourceCurrent = false;
    expect(() => action.assertCurrent()).toThrow("physical state was replaced");
    expect(mocks.openDatabase).not.toHaveBeenCalled();
  });

  it("checks worker-supplied authority without reopening the owner database under its writer", async () => {
    const request = createRequest();
    const action = await preparePersonalGitHubActionV2(request);
    mocks.resolveOwner.mockImplementation(() => {
      throw new Error("Synchronous shared-state access while the worker owns its transaction");
    });
    const role = { profileId: action.owner, role: null, githubLogin: null };
    expect(() => action.assertMutationCurrent(role)).not.toThrow();
    mocks.roleScopes = [];
    expect(() => action.assertMutationCurrent(role)).toThrow("current operator.read permission");
    mocks.roleScopes = undefined;
    mocks.ownerCurrent = false;
    expect(() => action.assertMutationCurrent(role)).toThrow("owner changed");
  });

  it.each([
    ["operator.read", "operator.sessions.write"],
    ["operator.sessions.write", "operator.read"],
  ])("retains shared session-read permission for %s capped by %s", async (grant, ceiling) => {
    const request = createRequest();
    request.client.connect.scopes = [grant];
    mocks.roleScopes = [ceiling];

    const read = await prepareGitHubPublicationOptionsRead(request, { sessionKey: "main" });
    expect(read.currentSession()).toEqual(read.session);
    expect(read.personal.kind).toBe("ineligible");

    mocks.roleScopes = ["operator.approvals"];
    expect(() => read.currentSession()).toThrow(
      "GitHub requires current operator.sessions.read permission.",
    );
  });

  it("shares store discovery while re-reading publication options live", async () => {
    const agentId = "research";
    mocks.loadSession.mockReturnValue(sessionRead(agentId));
    const read = await prepareGitHubPublicationOptionsRead(createRequest(), {
      sessionKey: "main",
      agentId,
    });

    expect(read.currentSession()).toEqual(read.session);
    expect(mocks.loadSession).toHaveBeenCalledTimes(2);
    const targetDiscoveryCache = mocks.loadSession.mock.calls[0]?.[1]?.targetDiscoveryCache;
    expect(targetDiscoveryCache).toBeInstanceOf(Map);
    expect(mocks.loadSession).toHaveBeenNthCalledWith(1, "main", {
      agentId,
      targetDiscoveryCache,
    });
    expect(mocks.loadSession.mock.calls[1]?.[1]?.targetDiscoveryCache).toBe(targetDiscoveryCache);
    expect(mocks.loadSession).toHaveBeenNthCalledWith(2, `agent:${agentId}:main`, {
      agentId,
      targetDiscoveryCache,
    });
  });

  it.each([null, 123])(
    "keeps each response archive snapshot immutable from %s",
    async (archivedAt) => {
      const loaded = sessionRead();
      mocks.loadSession.mockReturnValue({
        ...loaded,
        entry: { ...loaded.entry, archivedAt: archivedAt ?? undefined },
      });
      const read = await prepareGitHubPublicationOptionsRead(createRequest(), {
        sessionKey: "main",
      });
      const initial = read.currentSession();
      const changedAt = archivedAt === null ? 123 : null;
      mocks.loadSession.mockReturnValue({
        ...loaded,
        entry: { ...loaded.entry, archivedAt: changedAt ?? undefined },
      });
      const changed = read.currentSession();
      expect(changed.archivedAt).toBe(changedAt);
      expect(() => read.assertSessionUnchanged(changed)).not.toThrow();
      expect(() => read.assertSessionUnchanged(initial)).toThrow("session access changed");

      mocks.loadSession.mockReturnValue({
        ...loaded,
        entry: { ...loaded.entry, archivedAt: archivedAt ?? undefined },
      });
      const restored = read.currentSession();
      expect(restored.archivedAt).toBe(archivedAt);
      expect(() => read.assertSessionUnchanged(changed)).toThrow("session access changed");
      expect(() => read.assertSessionUnchanged(restored)).not.toThrow();
    },
  );

  it("shares store discovery across every personal session authority re-read", () => {
    const agentId = "research";
    mocks.loadSession.mockReturnValue(sessionRead(agentId));
    const action = preparePersonalGitHubSessionAction(createRequest(), {
      sessionKey: "main",
      agentId,
    });
    action.assertCurrent();

    expect(mocks.loadSession).toHaveBeenCalledTimes(3);
    const targetDiscoveryCache = mocks.loadSession.mock.calls[0]?.[1]?.targetDiscoveryCache;
    expect(targetDiscoveryCache).toBeInstanceOf(Map);
    expect(mocks.loadSession).toHaveBeenNthCalledWith(1, "main", {
      agentId,
      targetDiscoveryCache,
    });
    for (const call of [2, 3]) {
      expect(mocks.loadSession.mock.calls[call - 1]?.[1]?.targetDiscoveryCache).toBe(
        targetDiscoveryCache,
      );
      expect(mocks.loadSession).toHaveBeenNthCalledWith(call, `agent:${agentId}:main`, {
        agentId,
        targetDiscoveryCache,
      });
    }
  });
});
