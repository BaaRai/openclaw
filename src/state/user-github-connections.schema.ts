import { z } from "zod";
import {
  githubOAuthTimestamp as timestamp,
  githubOAuthSecret as secret,
  githubOAuthProfileId as profileId,
  githubOAuthScopes as scopes,
  githubOAuthRefreshFields,
  githubOAuthDeviceFields,
  validGitHubDeviceTiming,
} from "../shared/github-oauth-values.js";

const tokenPair = z.strictObject({
  accessToken: secret,
  refreshToken: secret,
  tokenType: z.literal("bearer"),
  scopes,
  expiresInSeconds: z
    .number()
    .int()
    .positive()
    .max(366 * 86400),
  refreshTokenExpiresInSeconds: z
    .number()
    .int()
    .positive()
    .max(366 * 86400),
});
const deviceFields = {
  requestId: z.string().uuid(),
  createdAtMs: timestamp,
  expiresAtMs: timestamp,
};
const device = z.strictObject({
  ...deviceFields,
  kind: z.literal("device"),
  ...githubOAuthDeviceFields,
  candidate: z.strictObject({ profileId, tokens: tokenPair, receivedAtMs: timestamp }).optional(),
});
const connected = z.strictObject({
  kind: z.literal("connected"),
  profileId,
  ...githubOAuthRefreshFields,
  refreshFailure: z.enum(["expired", "failed"]).optional(),
  refresh: z
    .strictObject({
      operationId: z.string().uuid(),
      tokens: tokenPair.optional(),
      receivedAtMs: timestamp.optional(),
    })
    .optional(),
});
export const connectionSchema = z
  .strictObject({
    version: z.literal(1),
    generation: z.string().uuid(),
    selection: z.discriminatedUnion("kind", [
      z.strictObject({ kind: z.literal("disconnected") }),
      connected,
    ]),
    pending: z
      .discriminatedUnion("kind", [
        z.strictObject({ ...deviceFields, kind: z.literal("starting") }),
        device,
      ])
      .optional(),
  })
  .superRefine((record, ctx) => {
    const pending = record.pending;
    if (pending && !validGitHubDeviceTiming(pending)) {
      ctx.addIssue({ code: "custom", message: "Invalid device timing" });
    }
    const selection = record.selection;
    if (
      selection.kind === "connected" &&
      (selection.refreshExpiresAtMs <= selection.accessExpiresAtMs ||
        Boolean(selection.refresh?.tokens) !== (selection.refresh?.receivedAtMs !== undefined))
    ) {
      ctx.addIssue({ code: "custom", message: "Invalid refresh state" });
    }
  });

export type UserGitHubConnection = z.infer<typeof connectionSchema>;
export type UserGitHubConnected = z.infer<typeof connected>;
export type UserGitHubDevice = z.infer<typeof device>;

export type UserGitHubTokenPair = z.infer<typeof tokenPair>;
