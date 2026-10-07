import fs from "node:fs";
import { expect, it } from "vitest";
import { getOpenIncognitoAgentDatabase } from "../../state/openclaw-agent-db-lifecycle.js";
import { resolveIncognitoOpenClawAgentSqlitePath } from "../../state/openclaw-agent-db.paths.js";
import { readAcpSessionEntry } from "../runtime/session-meta.js";
import { withAcpCancellationFixture } from "./manager.cancel-session.worker.test-support.js";

it("preserves idle Incognito cancellation through the retained native metadata writer", async () => {
  await withAcpCancellationFixture(
    async (f) => {
      const path = resolveIncognitoOpenClawAgentSqlitePath({ agentId: "main", env: f.state.env });
      const memory = getOpenIncognitoAgentDatabase("main", path);
      expect(memory).toBeDefined();
      await f.manager.cancelSession(f.target);
      expect(f.cancel).toHaveBeenCalledOnce();
      expect(readAcpSessionEntry(f.target)?.acp?.state).toBe("idle");
      expect(getOpenIncognitoAgentDatabase("main", path)).toBe(memory);
      expect(fs.existsSync(path)).toBe(false);
    },
    { sessionKey: "agent:main:dashboard:incognito-idle-parity" },
  );
});
