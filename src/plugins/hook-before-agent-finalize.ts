import { concatOptionalTextSegments } from "../shared/text/join-segments.js";
import type {
  BeforeAgentFinalizeResultWithRetryCandidates,
  BeforeAgentFinalizeRetry,
} from "./hook-runner-types.js";
import type { PluginHookBeforeAgentFinalizeResult } from "./hook-types.js";

export const mergeBeforeAgentFinalize = (
  acc: PluginHookBeforeAgentFinalizeResult | undefined,
  next: PluginHookBeforeAgentFinalizeResult,
): PluginHookBeforeAgentFinalizeResult => {
  const normalizeRetry = (
    retry: PluginHookBeforeAgentFinalizeResult["retry"] | undefined,
  ): BeforeAgentFinalizeRetry | undefined => {
    const instruction = typeof retry?.instruction === "string" ? retry.instruction.trim() : "";
    if (!instruction) {
      return undefined;
    }
    return {
      ...retry,
      instruction,
    };
  };
  const readRetryCandidates = (
    result: BeforeAgentFinalizeResultWithRetryCandidates | undefined,
  ): BeforeAgentFinalizeRetry[] => {
    if (!result || result.action !== "revise") {
      return [];
    }
    const candidateList = result.retryCandidates;
    if (Array.isArray(candidateList) && candidateList.length > 0) {
      return candidateList
        .map(normalizeRetry)
        .filter((retry): retry is BeforeAgentFinalizeRetry => retry !== undefined);
    }
    const retry = normalizeRetry(result.retry);
    return retry ? [retry] : [];
  };
  if (acc?.action === "finalize") {
    return acc;
  }
  if (next.action === "finalize") {
    return { action: "finalize", reason: next.reason };
  }
  if (acc?.action === "revise" && next.action === "revise") {
    const retryCandidates = [...readRetryCandidates(acc), ...readRetryCandidates(next)];
    const retry = retryCandidates[0];
    const result: PluginHookBeforeAgentFinalizeResult = {
      action: "revise",
      reason: concatOptionalTextSegments({ left: acc.reason, right: next.reason }),
      ...(retry ? { retry } : {}),
    };
    if (retryCandidates.length > 1) {
      Object.defineProperty(result, "retryCandidates", {
        configurable: true,
        enumerable: false,
        value: retryCandidates,
      });
    }
    return result;
  }
  if (acc?.action === "revise") {
    return acc;
  }
  if (next.action === "revise") {
    const retry = normalizeRetry(next.retry);
    return {
      action: "revise",
      reason: next.reason,
      ...(retry ? { retry } : {}),
    };
  }
  return next.action === "continue" ? { action: "continue", reason: next.reason } : (acc ?? next);
};
