import {
  abortSessionControllerInput,
  sessionControllerMailboxes,
} from "../../../sessions/session-controller.mailbox.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

/** The stable reservation ID of a run's individual completion input. */
export const subagentCompletionSourceId = (entry: SubagentRunRecord) =>
  `subagent-completion:${entry.runId}:${entry.generation ?? 0}`;

/** Live mailbox inputs that deliver this run, by their stable reservation identities. */
function* subagentControllerInputs(entry: SubagentRunRecord) {
  const completionId = subagentCompletionSourceId(entry);
  for (const mailbox of sessionControllerMailboxes()) {
    for (const input of mailbox.entries.slice()) {
      const id = input.sourceTurnId;
      if (
        input.phase !== "consumed" &&
        (id === completionId ||
          (id?.startsWith("requester-settle:") === true && id.includes(entry.runId)))
      ) {
        yield input;
      }
    }
  }
}

/** Whether any mailbox input still delivers this run. */
export const hasSubagentControllerInput = (entry: SubagentRunRecord): boolean =>
  !subagentControllerInputs(entry).next().done;

/** Withdraws every unclaimed input that delivers this run; claimed turns revalidate at execution. */
export function retireSubagentControllerInputs(entry: SubagentRunRecord): void {
  for (const input of subagentControllerInputs(entry)) {
    if (!input.claim) {
      abortSessionControllerInput(input, new Error("Subagent obligation retired"));
    }
  }
}
