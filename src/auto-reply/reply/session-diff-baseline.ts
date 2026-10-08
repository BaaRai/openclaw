import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { isSessionWorkStartInvalidatedError } from "../../config/sessions/lifecycle.js";
import { logVerbose } from "../../globals.js";
import { formatErrorMessage } from "../../infra/errors.js";
import { ensureSessionDiffBaseline } from "../../sessions/session-diff-baseline.js";
import type { SessionInitResult } from "./session-init.types.js";

export async function prepareReplySessionDiffBaseline(params: {
  agentId: string;
  workspaceDir: string;
  sessionState: Pick<
    SessionInitResult,
    | "sessionEntry"
    | "sessionEntryHandle"
    | "sessionStore"
    | "isNewSession"
    | "sessionKey"
    | "storePath"
  >;
}): Promise<void> {
  const { sessionState } = params;
  try {
    const entry = await ensureSessionDiffBaseline({
      agentId: params.agentId,
      cwd:
        normalizeOptionalString(sessionState.sessionEntry.spawnedCwd) ??
        normalizeOptionalString(sessionState.sessionEntry.spawnedWorkspaceDir) ??
        params.workspaceDir,
      entry: sessionState.sessionEntry,
      isNewSession: sessionState.isNewSession,
      sessionKey: sessionState.sessionKey,
      storePath: sessionState.storePath,
      deferCapture: true,
    });
    sessionState.sessionEntry = entry;
    sessionState.sessionEntryHandle.replaceCurrent(entry);
    sessionState.sessionStore[sessionState.sessionKey] = entry;
  } catch (error) {
    if (isSessionWorkStartInvalidatedError(error)) {
      throw error;
    }
    logVerbose(
      `session diff baseline capture failed; continuing without attribution filtering: ${formatErrorMessage(error)}`,
    );
  }
}
