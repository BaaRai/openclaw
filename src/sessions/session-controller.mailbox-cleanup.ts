/** Captured mailbox batch cleanup and summary bookkeeping. */
import type { FollowupRun } from "../auto-reply/reply/queue/types.js";
import { defaultRuntime } from "../runtime.js";
import {
  inputCancellation,
  abortSessionControllerInput,
  retireSessionControllerInput,
} from "./session-controller.mailbox-source.js";
import type {
  SessionControllerInput,
  SessionControllerMailbox,
} from "./session-controller.mailbox.types.js";

/** Snapshots aggregate summary sources shared by selection and captured cleanup. */
export function captureSessionControllerMailboxSummarySources(
  mailbox: SessionControllerMailbox,
): FollowupRun[] {
  return [...mailbox.summaryElisions.flatMap((part) => part.sources), ...mailbox.summarySources];
}

/** Captures and detaches only this generation before invoking reentrant cancellation effects. */
export function clearSessionControllerMailbox(
  mailbox: SessionControllerMailbox,
  settleSource: (source: FollowupRun) => void,
  capturedInputs?: readonly SessionControllerInput[],
): number {
  const inputs = capturedInputs
    ? capturedInputs.filter(
        (input) =>
          input.mailbox === mailbox &&
          mailbox.entries.includes(input) &&
          !input.retirementRequested &&
          input.phase !== "consumed",
      )
    : [...mailbox.entries];
  const selected = new Set(inputs);
  const selectedSource = (source: FollowupRun) =>
    Boolean(source.controllerInput && selected.has(source.controllerInput));
  const sources = [
    ...new Set([
      ...inputs.flatMap((input) => (input.source ? [input.source] : [])),
      ...captureSessionControllerMailboxSummarySources(mailbox).filter(
        (source) => !capturedInputs || selectedSource(source),
      ),
    ]),
  ];
  const cleared = capturedInputs ? inputs.length : mailbox.items.length + mailbox.droppedCount;
  const abort = capturedInputs ? undefined : mailbox.abortController;
  if (!capturedInputs) {
    mailbox.abortController = new AbortController();
  }
  const wasClearing = mailbox.clearing;
  mailbox.clearing = true;
  for (const input of inputs) {
    input.retirementRequested = true;
  }
  if (capturedInputs) {
    let removed = 0;
    for (let index = mailbox.summarySources.length - 1; index >= 0; index--) {
      if (selectedSource(mailbox.summarySources[index]!)) {
        mailbox.summarySources.splice(index, 1);
        mailbox.summaryLines.splice(index, 1);
        removed++;
      }
    }
    for (const entry of mailbox.summaryElisions) {
      for (let index = entry.sources.length - 1; index >= 0; index--) {
        if (selectedSource(entry.sources[index]!)) {
          entry.sources.splice(index, 1);
          entry.summaryLines.splice(index, 1);
          removed++;
        }
      }
      for (const [original, compact] of entry.sourceRefs) {
        if (selectedSource(compact)) {
          entry.sourceRefs.delete(original);
        }
      }
      entry.count = entry.sources.length;
    }
    mailbox.summaryElisions = mailbox.summaryElisions.filter((entry) => entry.count > 0);
    mailbox.droppedCount = Math.max(0, mailbox.droppedCount - removed);
  } else {
    mailbox.summaryLines = [];
    mailbox.summarySources = [];
    mailbox.summaryElisions = [];
    mailbox.droppedCount = 0;
    mailbox.evictedSummaryCount = 0;
    mailbox.dispatch = undefined;
    mailbox.dispatchEnabled = false;
    mailbox.lastRun = undefined;
    mailbox.lastEnqueuedAt = 0;
    clearTimeout(mailbox.timer);
    mailbox.timer = undefined;
  }
  for (const [key, record] of mailbox.recentSources) {
    if (
      selected.has(record.input) &&
      !record.input.custody.adopted &&
      record.input.phase !== "consumed"
    ) {
      mailbox.recentSources.delete(key);
    }
  }
  if (mailbox.priority && inputs.includes(mailbox.priority)) {
    mailbox.priority = undefined;
  }
  try {
    abort?.abort();
    if (capturedInputs) {
      for (const input of inputs) {
        if (!input.withdrawalHolds) {
          abortSessionControllerInput(input, new Error("Session mailbox cleared"));
        }
      }
    }
    for (const source of sources) {
      const input = source.controllerInput;
      if ((input?.claim && !input.claim.released) || input?.phase === "injecting") {
        continue;
      }
      try {
        settleSource(source);
      } catch (error) {
        defaultRuntime.error?.("mailbox clear custody failed: " + String(error));
      }
    }
    for (const input of inputs) {
      // A durable discard owns its exact source until commit/release. Do not
      // consume that capability from a concurrent clear/publication callback.
      if (!input.withdrawalHolds) {
        input[inputCancellation].abort(new Error("Session mailbox cleared"));
      }
      retireSessionControllerInput(input);
    }
  } finally {
    mailbox.clearing = wasClearing;
    mailbox.wake();
  }
  return cleared;
}
