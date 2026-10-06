import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { resolveConversationCapabilityProfile } from "../../agents/conversation-capability-profile.js";
import { projectConversationToolNames } from "../../agents/conversation-tool-policy-pipeline.js";
import { resolveSandboxRuntimeStatus } from "../../agents/sandbox.js";
import { resolveEffectiveToolFsRootExpansionAllowed } from "../../agents/tool-fs-policy.js";
import type { OpenClawConfig } from "../../config/config.js";
import { resolveGroupSessionKey } from "../../config/sessions/group.js";
import { normalizeMediaFacts } from "../../media/media-facts.js";
import type { GetReplyOptions } from "../get-reply-options.types.js";
import type { RuntimeMsgContext as MsgContext } from "../templating.js";
import { resolveOriginMessageProvider } from "./origin-routing.js";
import { resolveRuntimePolicySessionKey } from "./runtime-policy-session-key.js";

export function canSelfServeLocalPaths(params: {
  ctx: MsgContext;
  cfg: OpenClawConfig;
  agentId: string;
  sessionKey?: string;
  workspaceDir: string;
  provider: string;
  model: string;
  opts?: GetReplyOptions;
  senderIsOwner: boolean;
  spawnedBy?: string;
  stagedPathsAvailable: boolean;
}): boolean {
  if (params.opts?.disableTools === true) {
    return false;
  }
  const policySessionKey = resolveRuntimePolicySessionKey({
    cfg: params.cfg,
    agentId: params.agentId,
    ctx: params.ctx,
    sessionKey: params.sessionKey,
  });
  const sandboxed = resolveSandboxRuntimeStatus({
    cfg: params.cfg,
    agentId: params.agentId,
    sessionKey: params.sessionKey,
    classificationSessionKey: policySessionKey,
  }).sandboxed;
  if (
    (sandboxed && !params.stagedPathsAvailable) ||
    (!sandboxed &&
      !resolveEffectiveToolFsRootExpansionAllowed({ cfg: params.cfg, agentId: params.agentId }))
  ) {
    return false;
  }
  const capabilityProfile = resolveConversationCapabilityProfile({
    config: params.cfg,
    sessionKey: policySessionKey,
    runSessionKey: policySessionKey === params.sessionKey ? undefined : params.sessionKey,
    agentId: params.agentId,
    agentAccountId: params.ctx.AccountId,
    messageProvider: resolveOriginMessageProvider({
      originatingChannel: params.ctx.OriginatingChannel,
      provider: params.ctx.Provider ?? params.ctx.Surface,
    }),
    conversationToolPolicy: params.ctx.ConversationToolPolicy,
    groupId: resolveGroupSessionKey(params.ctx)?.id,
    groupChannel:
      normalizeOptionalString(params.ctx.GroupChannel) ??
      normalizeOptionalString(params.ctx.GroupSubject),
    groupSpace: normalizeOptionalString(params.ctx.GroupSpace),
    spawnedBy: params.spawnedBy,
    senderId: normalizeOptionalString(params.ctx.SenderId),
    senderName: normalizeOptionalString(params.ctx.SenderName),
    senderUsername: normalizeOptionalString(params.ctx.SenderUsername),
    senderE164: normalizeOptionalString(params.ctx.SenderE164),
    senderIsOwner: params.senderIsOwner,
    modelProvider: params.provider,
    modelId: params.model,
    workspaceDir: params.workspaceDir,
    runtimeToolAllowlist: params.opts?.toolsAllow,
    inheritRuntimeToolAllowlist: true,
    inputProvenance: params.ctx.InputProvenance,
  });
  return (
    projectConversationToolNames({
      capabilityProfile,
      toolNames: ["read"],
      warn: () => {},
    }).length === 1
  );
}

export function collectStagedAttachmentPaths(ctx: MsgContext): ReadonlyMap<number, string> {
  return new Map(
    normalizeMediaFacts(ctx.media).flatMap((fact, index) => {
      const mediaPath = normalizeOptionalString(fact.path);
      return mediaPath ? [[index, mediaPath] as const] : [];
    }),
  );
}
