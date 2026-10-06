// Handles abort requests and active reply run cancellation.
import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { getAcpSessionManager } from "../../acp/control-plane/manager.js";
import { getAcpSessionResetControls } from "../../acp/control-plane/manager.reset-controls.js";
import { retireSessionMcpRuntime } from "../../agents/agent-bundle-mcp-manager-api.js";
import { resolveSessionAgentId } from "../../agents/agent-scope.js";
import { killAllControlledSubagentRuns } from "../../agents/subagents/registry/subagent-control.js";
import { listSubagentRunsForController } from "../../agents/subagents/registry/subagent-registry-read.js";
import {
  resolveInternalSessionKey,
  resolveMainSessionAlias,
} from "../../agents/tools/sessions-helpers.js";
import { resolveSessionStorePathCore } from "../../config/sessions.js";
import {
  loadSessionEntry,
  markSessionAbortTarget,
  resolveSessionAbortTarget,
  type SessionAbortTargetIdentity,
  type SessionAbortTargetResult,
} from "../../config/sessions/session-accessor.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { logVerbose } from "../../globals.js";
import { formatErrorMessage } from "../../infra/errors.js";
import {
  isAcpSessionKey,
  isSubagentSessionKey,
  normalizeAgentId,
} from "../../routing/session-key.js";
import type { ReplyOperation } from "../../sessions/session-controller.contracts.js";
import {
  captureSessionTarget,
  SESSION_CONTROLLER_DRAIN_TIMEOUT_MS,
} from "../../sessions/session-controller.lifecycle.js";
import type { SessionControllerInput } from "../../sessions/session-controller.mailbox.js";
import {
  captureSessionControllerStop,
  captureSessionControllerStopCandidates,
  stopSession,
  type SessionControllerStopCapture,
  type SessionStopHookContext,
  type SessionStopRequest,
} from "../../sessions/session-controller.stop.js";
import { settlesWithin } from "../../shared/settle-within.js";
import { resolveCommandAuthorization } from "../command-auth.js";
import type { FinalizedRuntimeMsgContext } from "../templating.js";
import { resolveAbortCutoffFromContext, shouldPersistAbortCutoff } from "./abort-cutoff.js";
import { setAbortMemory } from "./abort-primitives.js";
import type { FastAbortRequestParams, FastAbortResult, PreparedFastAbortRequest } from "./abort.js";
import { resolveEffectiveResetTargetSessionKey } from "./acp-reset-target.js";
import { resolveConversationBindingContextFromMessage } from "./conversation-binding-input.js";

type ChannelStopTarget = SessionControllerInput | ReplyOperation;
export type ChannelStopCapture = {
  controller: SessionControllerStopCapture;
  sessionId?: string;
  mcpSessionIds: ReadonlyMap<ChannelStopTarget, readonly string[]>;
  idleSessionIds: readonly string[];
};

function matchesStopCandidate(
  candidate: Pick<
    ReturnType<typeof captureSessionControllerStopCandidates>[number],
    "aliases" | "agentId" | "storeScope"
  >,
  params: { key: string; storePath: string; agentId?: string },
): boolean {
  if (!candidate.aliases.has(params.key)) {
    return false;
  }
  if (candidate.storeScope && candidate.storeScope !== params.storePath) {
    return false;
  }
  return (
    !candidate.agentId ||
    !params.agentId ||
    normalizeAgentId(candidate.agentId) === normalizeAgentId(params.agentId)
  );
}

/** Physical identity and runtime references are captured together, before channel I/O. */
export function captureChannelSessionStop(params: {
  key?: string;
  sessionId?: string;
  storePath: string;
  agentId?: string;
  aliases?: readonly (string | undefined)[];
  includeQueued?: boolean;
}): ChannelStopCapture {
  const key = normalizeOptionalString(params.key);
  const candidates = key
    ? captureSessionControllerStopCandidates().filter((candidate) =>
        matchesStopCandidate(candidate, {
          key,
          storePath: params.storePath,
          agentId: params.agentId,
        }),
      )
    : [];
  const controller = captureSessionControllerStop({
    targets: key
      ? [
          captureSessionTarget({
            storeScope: params.storePath,
            sessionKey: key,
            incarnation: params.sessionId,
            agentId: params.agentId,
            aliases: params.aliases,
          }),
        ]
      : [],
    inputs: candidates.flatMap((candidate) => candidate.capture.inputs),
    operations: candidates.flatMap((candidate) => candidate.capture.operations),
    includeQueued: params.includeQueued,
  });
  return captureChannelStopResources(controller, params.sessionId);
}

function captureChannelStopResources(
  controller: SessionControllerStopCapture,
  capturedSessionId?: string,
): ChannelStopCapture {
  const mcpSessionIds = new Map<ChannelStopTarget, readonly string[]>();
  for (const operation of controller.operations) {
    mcpSessionIds.set(operation, [...operation.captureOwnedSessionIds()]);
  }
  for (const input of controller.activeInputs) {
    const sessionId = input.source?.run.sessionId ?? capturedSessionId;
    mcpSessionIds.set(
      input,
      input.claim?.operation
        ? [...input.claim.operation.captureOwnedSessionIds()]
        : sessionId
          ? [sessionId]
          : [],
    );
  }
  const active = controller.activeInputs.length > 0 || controller.operations.length > 0;
  return {
    controller,
    sessionId:
      controller.activeInputs[0]?.claim?.operation?.sessionId ??
      controller.operations[0]?.sessionId ??
      capturedSessionId,
    mcpSessionIds,
    idleSessionIds: !active && capturedSessionId ? [capturedSessionId] : [],
  };
}

function combineChannelStopCaptures(captures: readonly ChannelStopCapture[]): ChannelStopCapture {
  return {
    controller: captureSessionControllerStop({
      inputs: captures.flatMap((capture) => [...capture.controller.inputs]),
      operations: captures.flatMap((capture) => [...capture.controller.operations]),
    }),
    mcpSessionIds: new Map(captures.flatMap((capture) => [...capture.mcpSessionIds])),
    idleSessionIds: [...new Set(captures.flatMap((capture) => [...capture.idleSessionIds]))],
  };
}

/** Scope adapter only: cancellation and acceptance belong to the captured Stop kernel. */
export function abortSessionRunTargetWithOutcome(params: {
  capture: ChannelStopCapture | (() => ChannelStopCapture);
  assertCurrent?: () => void;
  afterQueued?: () => void;
  hookContext: SessionStopHookContext;
  messageIdentity?: unknown;
  recordAbortTarget?: SessionStopRequest["recordAbortTarget"];
  stopChildren?: SessionStopRequest["stopChildren"];
  retirements: Promise<void>[];
}) {
  let selectedCapture: ChannelStopCapture | undefined;
  const resolveCapture = () =>
    (selectedCapture ??= typeof params.capture === "function" ? params.capture() : params.capture);
  const retiring = new Set<string>();
  const retire = (sessionIds: readonly string[]) => {
    for (const sessionId of sessionIds) {
      if (retiring.has(sessionId)) {
        continue;
      }
      params.assertCurrent?.();
      retiring.add(sessionId);
      const retirement = retireSessionMcpRuntime({ sessionId, reason: "session-stop" }).then(
        (retired) => {
          if (!retired) {
            throw new Error("Session MCP runtime retirement failed.");
          }
        },
      );
      void retirement.catch(() => {});
      params.retirements.push(retirement);
    }
  };
  let joined = false;
  return stopSession({
    source: "channel-user",
    capture: () => resolveCapture().controller,
    assertCurrent: params.assertCurrent,
    hookContext: params.hookContext,
    messageIdentity: params.messageIdentity,
    recordAbortTarget: params.recordAbortTarget,
    stopChildren: params.stopChildren,
    afterQueued: () => {
      const capture = resolveCapture();
      params.afterQueued?.();
      retire(capture.idleSessionIds);
    },
    onCancelled: (target) => {
      const capture = resolveCapture();
      // Successful active cancellation retains the captured producers until their
      // actual return. A finishing refusal must not make queued-only Stop wait for it.
      if (capture.mcpSessionIds.has(target) && !joined) {
        joined = true;
        params.retirements.push(capture.controller.settled);
      } else if ("mailbox" in target) {
        params.retirements.push(target.settlement.promise);
      }
      retire(capture.mcpSessionIds.get(target) ?? []);
    },
  });
}

function resolveStoredSessionId(params: {
  cfg: OpenClawConfig;
  sessionKey: string;
  agentId?: string;
}): string | undefined {
  const agentId = resolveSessionAgentId({
    sessionKey: params.sessionKey,
    config: params.cfg,
    fallbackAgentId: params.agentId,
  });
  const storePath = resolveSessionStorePathCore(params.cfg.session?.store, { agentId });
  try {
    return loadSessionEntry({
      agentId,
      clone: false,
      sessionKey: params.sessionKey,
      storePath,
    })?.sessionId;
  } catch {
    return undefined;
  }
}

async function resolveBoundAcpAbortTargetSessionKey(params: {
  ctx: FinalizedRuntimeMsgContext;
  cfg: OpenClawConfig;
  activeSessionKey: string;
}): Promise<string | undefined> {
  const bindingContext = resolveConversationBindingContextFromMessage({
    cfg: params.cfg,
    ctx: params.ctx,
  });
  if (!bindingContext) {
    return undefined;
  }
  return await resolveEffectiveResetTargetSessionKey({
    cfg: params.cfg,
    channel: bindingContext.channel,
    accountId: bindingContext.accountId,
    conversationId: bindingContext.conversationId,
    parentConversationId: bindingContext.parentConversationId,
    activeSessionKey: params.activeSessionKey,
    skipConfiguredFallbackWhenActiveSessionNonAcp: false,
    fallbackToActiveAcpWhenUnbound: false,
  });
}

function normalizeRequesterSessionKey(
  cfg: OpenClawConfig,
  key: string | undefined,
): string | undefined {
  const cleaned = normalizeOptionalString(key);
  if (!cleaned) {
    return undefined;
  }
  const { alias } = resolveMainSessionAlias(cfg);
  return resolveInternalSessionKey({ key: cleaned, alias });
}

export async function stopSubagentsForRequester(params: {
  cfg: OpenClawConfig;
  requesterSessionKey?: string;
  requesterAgentId?: string;
  beforeKill?: Parameters<typeof killAllControlledSubagentRuns>[0]["beforeKill"];
  assertCurrent?: () => void;
}): Promise<{ stopped: number; failed: number }> {
  const requesterKey = normalizeRequesterSessionKey(params.cfg, params.requesterSessionKey);
  if (!requesterKey) {
    params.assertCurrent?.();
    await params.beforeKill?.();
    return { stopped: 0, failed: 0 };
  }
  const controllerAgentId = resolveSessionAgentId({
    config: params.cfg,
    sessionKey: requesterKey,
    fallbackAgentId: params.requesterAgentId,
  });
  const result = await killAllControlledSubagentRuns({
    cfg: params.cfg,
    controller: {
      controllerSessionKey: requesterKey,
      controllerAgentId,
      callerSessionKey: requesterKey,
      callerIsSubagent: isSubagentSessionKey(requesterKey),
      controlScope: "children",
    },
    runs: listSubagentRunsForController(requesterKey),
    suppressTaskDelivery: true,
    assertCurrent: params.assertCurrent,
    beforeKill: params.beforeKill,
  });
  if (result.status === "error") {
    logVerbose(`abort: failed to stop subagents for ${requesterKey}: ${result.error}`);
  }
  if (result.killed > 0) {
    logVerbose(`abort: stopped ${result.killed} subagent run(s) for ${requesterKey}`);
  }
  return { stopped: result.killed, failed: result.status === "error" ? result.failed : 0 };
}

export async function executeFastAbortRequest(
  params: FastAbortRequestParams,
  request: PreparedFastAbortRequest,
): Promise<FastAbortResult> {
  const { ctx, cfg } = params;
  const { commandSessionKey, targetKey, resolveTargetAgentId } = request;

  const commandAuthorized = ctx.CommandAuthorized;
  const auth = resolveCommandAuthorization({
    ctx,
    cfg,
    commandAuthorized,
  });
  if (!auth.isAuthorizedSender) {
    return { handled: false, aborted: false };
  }

  const assertCurrent = () => {
    if (params.isCommandTargetCurrent?.() === false) {
      throw new Error("The selected session changed before it could be stopped.");
    }
  };

  const agentId = resolveTargetAgentId();
  const abortKey = targetKey ?? auth.from ?? auth.to;
  const requesterSessionKey = targetKey ?? ctx.SessionKey ?? abortKey;

  if (targetKey) {
    const storePath = resolveSessionStorePathCore(cfg.session?.store, { agentId });
    let resolvedAbortTarget: SessionAbortTargetIdentity | null = null;
    try {
      resolvedAbortTarget = resolveSessionAbortTarget({
        agentId,
        sessionKey: targetKey,
        storePath,
      });
    } catch (error) {
      logVerbose(
        `abort: failed to resolve abort metadata for ${targetKey}: ${formatErrorMessage(error)}`,
      );
    }
    const resolvedTargetKey = resolvedAbortTarget?.sessionKey ?? targetKey;
    assertCurrent();
    const captureTarget = (key: string, sessionId?: string, targetAgentId?: string) => {
      const ownerAgentId =
        targetAgentId ??
        resolveSessionAgentId({
          config: cfg,
          sessionKey: key,
          fallbackAgentId: ctx.AgentId,
        });
      return captureChannelSessionStop({
        key,
        sessionId:
          sessionId ?? resolveStoredSessionId({ cfg, sessionKey: key, agentId: ownerAgentId }),
        storePath: resolveSessionStorePathCore(cfg.session?.store, { agentId: ownerAgentId }),
        agentId: ownerAgentId,
      });
    };
    // Capture both possible native targets now. Binding I/O may finish after either
    // lane is reused; the later decision can select only these original references.
    const mainCapture = captureTarget(resolvedTargetKey, resolvedAbortTarget?.sessionId, agentId);
    const sourceCapture =
      commandSessionKey && commandSessionKey !== resolvedTargetKey
        ? captureTarget(commandSessionKey)
        : undefined;
    // Binding lookup selects among already-captured in-memory owners. It cannot
    // discover cancellation authority for a replacement admitted while it awaited.
    const boundCandidates = captureSessionControllerStopCandidates().map((candidate) => ({
      storeScope: candidate.storeScope,
      agentId: candidate.agentId,
      aliases: candidate.aliases,
      channel: captureChannelStopResources(candidate.capture),
    }));
    const acpCapture = getAcpSessionResetControls(getAcpSessionManager()).captureCancellation();
    const acpCancellations: Promise<void>[] = [];
    let acpAborted = false;
    let selectedCapture: ChannelStopCapture | undefined;
    let abortTargetKeys: string[] = [];
    const abortCutoff = shouldPersistAbortCutoff({
      commandSessionKey,
      targetSessionKey: resolvedTargetKey,
    })
      ? resolveAbortCutoffFromContext(ctx)
      : undefined;
    const stop = abortSessionRunTargetWithOutcome({
      capture: () => {
        if (!selectedCapture) {
          throw new Error("Fast Stop target was not selected before cancellation");
        }
        return selectedCapture;
      },
      assertCurrent,
      retirements: acpCancellations,
      hookContext: {
        sessionKey: resolvedTargetKey,
        sessionEntry: resolvedAbortTarget?.entry,
        sessionId: mainCapture.sessionId,
        commandSource: ctx.Surface ?? ctx.Provider ?? ctx.OriginatingChannel,
        senderId: ctx.SenderId,
      },
      messageIdentity: abortCutoff,
      afterQueued: () => {
        const cancellation = (async () => {
          for (const acpTargetKey of abortTargetKeys) {
            assertCurrent();
            try {
              acpAborted =
                (await acpCapture.cancel({
                  cfg,
                  sessionKey: acpTargetKey,
                  agentId: acpTargetKey === resolvedTargetKey ? agentId : undefined,
                  assertActive: assertCurrent,
                  reason: "fast-abort",
                })) || acpAborted;
            } catch (error) {
              logVerbose(
                `abort: ACP cancel failed for ${acpTargetKey}: ${formatErrorMessage(error)}`,
              );
            }
          }
        })();
        acpCancellations.push(cancellation);
      },
      recordAbortTarget: async ({ recordCutoff }) => {
        let persistedAbortTarget: SessionAbortTargetResult | null = null;
        try {
          persistedAbortTarget = await markSessionAbortTarget({
            isCurrent: params.isCommandTargetCurrent,
            scope: { agentId, sessionKey: targetKey, storePath },
            resolveAbortCutoff: recordCutoff ? () => abortCutoff : undefined,
          });
        } catch (error) {
          logVerbose(
            `abort: failed to persist abort metadata for ${targetKey}: ${formatErrorMessage(error)}`,
          );
        }
        if (persistedAbortTarget?.persisted === false) {
          logVerbose(
            `abort: failed to persist abort metadata for ${targetKey}: ${persistedAbortTarget.persistenceError ?? "unknown error"}`,
          );
        }
        const abortMemoryKey =
          persistedAbortTarget?.sessionKey ?? resolvedAbortTarget?.sessionKey ?? abortKey;
        const hasAbortTargetEntry = Boolean(
          persistedAbortTarget?.entry ?? resolvedAbortTarget?.entry,
        );
        if (
          persistedAbortTarget?.persisted !== true &&
          abortMemoryKey &&
          !hasAbortTargetEntry &&
          params.isCommandTargetCurrent?.() !== false
        ) {
          setAbortMemory(abortMemoryKey, true);
        }
      },
      stopChildren: (applyParentStop) =>
        stopSubagentsForRequester({
          cfg,
          requesterSessionKey,
          requesterAgentId: agentId,
          assertCurrent,
          beforeKill: async () => {
            const conversationBoundAcpTargetKey = commandSessionKey
              ? await resolveBoundAcpAbortTargetSessionKey({
                  ctx,
                  cfg,
                  activeSessionKey: commandSessionKey,
                })
              : undefined;
            assertCurrent();
            const boundAcpTargetKey = !isAcpSessionKey(resolvedTargetKey)
              ? conversationBoundAcpTargetKey
              : undefined;
            const captures = [mainCapture];
            abortTargetKeys = [resolvedTargetKey];
            if (boundAcpTargetKey && boundAcpTargetKey !== resolvedTargetKey) {
              const boundAgentId = resolveSessionAgentId({
                config: cfg,
                sessionKey: boundAcpTargetKey,
              });
              const boundStore = resolveSessionStorePathCore(cfg.session?.store, {
                agentId: boundAgentId,
              });
              captures.push(
                ...boundCandidates
                  .filter((candidate) =>
                    matchesStopCandidate(candidate, {
                      key: boundAcpTargetKey,
                      storePath: boundStore,
                      agentId: boundAgentId,
                    }),
                  )
                  .map((candidate) => candidate.channel),
              );
              abortTargetKeys.push(boundAcpTargetKey);
            }
            if (
              sourceCapture &&
              conversationBoundAcpTargetKey &&
              abortTargetKeys.includes(conversationBoundAcpTargetKey)
            ) {
              captures.push(sourceCapture);
            }
            selectedCapture = combineChannelStopCaptures(captures);
            return await applyParentStop();
          },
        }),
    });
    let result: FastAbortResult;
    let retirementFailure: PromiseRejectedResult | undefined;
    try {
      const outcome = await stop.completed;
      const rejectionReason = outcome.alreadyFinalizing ? "finalizing" : undefined;
      result = {
        handled: true,
        aborted: outcome.aborted,
        ...(rejectionReason ? { rejectionReason } : {}),
        stoppedSubagents: outcome.childrenStopped,
        failedSubagents: outcome.childFailures,
      };
    } finally {
      // Bound acknowledgment without releasing the exact producer or retirement custody.
      const settled = Promise.allSettled(acpCancellations);
      const settledInTime = await settlesWithin(settled, SESSION_CONTROLLER_DRAIN_TIMEOUT_MS);
      acpCapture.release();
      retirementFailure = settledInTime
        ? (await settled).find((outcome) => outcome.status === "rejected")
        : {
            status: "rejected",
            reason: new Error(
              "Cancellation was requested, but cleanup is still pending. Check the turn status before retrying Stop.",
            ),
          };
    }
    // Preserve a primary cancellation failure; successful signaling still reports
    // failed retirement, and both paths above join every captured producer.
    if (retirementFailure) {
      throw retirementFailure.reason;
    }
    if (acpAborted) {
      result.aborted = true;
    }
    return result;
  }

  const emptyCapture = captureChannelSessionStop({
    storePath: resolveSessionStorePathCore(cfg.session?.store, { agentId }),
  });
  const stop = abortSessionRunTargetWithOutcome({
    capture: emptyCapture,
    retirements: [],
    assertCurrent,
    hookContext: {
      sessionKey: requesterSessionKey ?? "",
      commandSource: ctx.Surface ?? ctx.Provider ?? ctx.OriginatingChannel,
      senderId: ctx.SenderId,
    },
    messageIdentity: resolveAbortCutoffFromContext(ctx),
    recordAbortTarget: async () => {
      if (abortKey) {
        assertCurrent();
        setAbortMemory(abortKey, true);
      }
    },
    stopChildren: (applyParentStop) =>
      stopSubagentsForRequester({
        cfg,
        requesterSessionKey,
        assertCurrent,
        beforeKill: applyParentStop,
      }),
  });
  const outcome = await stop.completed;
  return {
    handled: true,
    aborted: outcome.aborted,
    stoppedSubagents: outcome.childrenStopped,
    failedSubagents: outcome.childFailures,
  };
}
