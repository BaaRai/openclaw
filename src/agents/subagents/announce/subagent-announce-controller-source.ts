import { captureSessionTarget } from "../../../sessions/session-controller.lifecycle.js";
import {
  abortSessionControllerInput,
  reserveSessionControllerSource,
  sessionControllerMailboxes,
  type SessionControllerInput,
} from "../../../sessions/session-controller.mailbox.js";
import type { SubagentRunRecord } from "../registry/subagent-registry.types.js";
import {
  buildRequesterSettleWakeIdentity,
  hasRequesterCompletionCohort,
} from "../registry/subagent-requester-settle-identity.js";
import { loadRequesterSessionEntry } from "./subagent-announce-delivery.runtime.js";

const subagentCompletionSourceId = (entry: SubagentRunRecord) =>
  `subagent-completion:${entry.runId}:${entry.generation ?? 0}`;

/** Reserves one followup source against the durable registry row that owns it. */
export function reserveSubagentControllerSource(
  entry: SubagentRunRecord,
  reservationId: string,
  protocolRunId?: string,
  continuationCaller?: SessionControllerInput["continuationCaller"],
  options: { replaceInactiveTarget?: boolean } = {},
): SessionControllerInput | undefined {
  const requester = loadRequesterSessionEntry(entry.requesterSessionKey, entry.requesterAgentId);
  if (!requester.entry?.sessionId || !requester.storePath) {
    return undefined;
  }
  return reserveSessionControllerSource(requester.canonicalKey, {
    reservationId,
    protocolRunId,
    replaceInactiveTarget: options.replaceInactiveTarget,
    continuationCaller,
    policy: { mode: "followup" },
    target: captureSessionTarget({
      storeScope: requester.storePath,
      sessionKey: requester.canonicalKey,
      aliases: [entry.requesterSessionKey],
      agentId: requester.agentId,
      incarnation: requester.entry.sessionId,
    }),
  });
}

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

export const reserveSubagentCompletionControllerSource = (
  entry: SubagentRunRecord,
  protocolRunId?: string,
) => reserveSubagentControllerSource(entry, subagentCompletionSourceId(entry), protocolRunId);

/** Rebuilds process-local mailbox custody from durable completion obligations in owed order. */
export function reserveRestoredSubagentControllerSources(
  entries: readonly SubagentRunRecord[],
): SessionControllerInput[] {
  const reservations: Array<[number, () => SessionControllerInput | undefined]> = [];
  const entriesById = new Map(entries.map((entry) => [entry.runId, entry]));
  for (const entry of entries) {
    if (
      entry.expectsCompletionMessage === true &&
      !hasRequesterCompletionCohort(entry) &&
      (entry.delivery?.status === "pending" || entry.delivery?.status === "in_progress")
    ) {
      reservations.push([
        entry.delivery.createdAt ?? entry.execution.endedAt ?? entry.createdAt,
        () =>
          reserveSubagentControllerSource(
            entry,
            subagentCompletionSourceId(entry),
            undefined,
            undefined,
            { replaceInactiveTarget: true },
          ),
      ]);
    }
    const wake = entry.requesterSettleWake;
    const batchRunIds = wake?.batchRunIds?.toSorted();
    if (!wake || !batchRunIds?.length) {
      continue;
    }
    const batch = batchRunIds.flatMap((runId) => {
      const member = entriesById.get(runId);
      return member ? [member] : [];
    });
    if (entry !== batch[0]) {
      continue;
    }
    const { batchKey: sourceId } = buildRequesterSettleWakeIdentity({
      requesterSessionKey: entry.requesterSessionKey,
      requesterAgentId: entry.requesterAgentId,
      batchRunIds,
      rearmGeneration: wake.rearmGeneration,
    });
    const caller = Object.freeze({
      deliveryRoute: entry.requesterOrigin && Object.freeze(structuredClone(entry.requesterOrigin)),
      run: <T>(run: () => Promise<T>) => run(),
    });
    reservations.push([
      Math.max(...batch.map((member) => member.execution.endedAt ?? member.createdAt)),
      () =>
        reserveSubagentControllerSource(entry, sourceId, undefined, caller, {
          replaceInactiveTarget: true,
        }),
    ]);
  }
  return reservations.toSorted(([a], [b]) => a - b).flatMap(([, reserve]) => reserve() ?? []);
}
