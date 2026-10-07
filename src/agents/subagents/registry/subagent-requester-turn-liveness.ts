import { findSessionControllerOperationByRunId } from "../../../sessions/session-controller.queries.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

/**
 * A requester turn owns its bound children only while the session controller still runs
 * it. The persisted requesterTurnRunId is cohort identity, never liveness on its own.
 */
export function isClaimedByLiveRequesterTurn(entry: SubagentRunRecord): boolean {
  return (
    entry.expectsCompletionMessage === true &&
    entry.requesterTurnRunId !== undefined &&
    findSessionControllerOperationByRunId(entry.requesterTurnRunId) !== undefined
  );
}
