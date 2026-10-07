/** Foreground restart recovery ordered ahead of its already-selected inbound source. */
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import type { GatewayRecoveryRuntime } from "../../gateway/server-instance-runtime.types.js";
import { captureSessionTarget } from "../../sessions/session-controller.lifecycle.js";
import { reserveSessionControllerClaimPredecessor } from "../../sessions/session-controller.mailbox-predecessor.js";
import {
  retireSessionControllerInput,
  type SessionControllerMailboxClaim,
} from "../../sessions/session-controller.mailbox.js";

type RestartRecoveryResult = Awaited<
  ReturnType<
    typeof import("../../agents/main-session-recovery/main-session-restart-recovery.js").retryRestartAbortedMainSessionRecovery
  >
>;

/** Dispatches the interrupted turn first, then waits until the inbound claim is restored. */
export async function retryRestartRecoveryBeforeSelectedClaim(params: {
  agentId?: string;
  cfg: OpenClawConfig;
  claim?: SessionControllerMailboxClaim;
  expectedRecoveryRunId?: string;
  expectedRecoverySourceRunId?: string;
  gatewayRuntime: GatewayRecoveryRuntime;
  /** Durable resend identity; a live input with this ID is joined, never dispatched again. */
  reservationId?: string;
  sessionId: string;
  sessionKey: string;
  storePath: string;
  upstreamAbortSignal?: AbortSignal;
}): Promise<RestartRecoveryResult | undefined> {
  const handoff = params.claim
    ? reserveSessionControllerClaimPredecessor(params.claim, {
        reservationId: params.reservationId,
        policy: { mode: "followup" },
        target: captureSessionTarget({
          storeScope: params.storePath,
          sessionKey: params.sessionKey,
          incarnation: params.sessionId,
          agentId: params.agentId,
        }),
        adapter: {
          signal: params.upstreamAbortSignal
            ? AbortSignal.any([params.claim.abortController.signal, params.upstreamAbortSignal])
            : params.claim.abortController.signal,
        },
      })
    : undefined;
  if (handoff && !handoff.created) {
    // Another dispatcher owns this resend; wait for it without a second dispatch or retirement.
    await handoff.restored;
    return undefined;
  }
  const input = handoff?.input;
  let recovery: RestartRecoveryResult;
  try {
    const { retryRestartAbortedMainSessionRecovery } =
      await import("../../agents/main-session-recovery/main-session-restart-recovery.js");
    recovery = await retryRestartAbortedMainSessionRecovery({
      agentId: params.agentId,
      cfg: params.cfg,
      controllerInput: input,
      expectedSessionId: params.sessionId,
      expectedRecoveryRunId: params.expectedRecoveryRunId,
      expectedRecoverySourceRunId: params.expectedRecoverySourceRunId,
      gatewayRuntime: params.gatewayRuntime,
      sessionKey: params.sessionKey,
      storePath: params.storePath,
    });
  } finally {
    if (input && !input.custody.rpcAdopted) {
      retireSessionControllerInput(input);
    }
  }
  return handoff && !(await handoff.restored) ? undefined : recovery;
}
