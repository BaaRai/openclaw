import type { Deferred } from "../shared/deferred.js";
import type { ReplyOperation } from "./session-controller.contracts.js";
import type { SessionControllerMailboxClaim } from "./session-controller.mailbox.js";
import type { SessionControllerEntry } from "./session-controller.state.types.js";
import type { SessionTarget } from "./session-controller.target.js";

export type SessionEffectInterrupt = (reason?: Error) => { runId: string } | void;
export type SessionEffectRef = {
  readonly target: SessionTarget;
  readonly operation?: ReplyOperation;
  createHandoff(): string;
  isActive(): boolean;
  release: () => void;
  readonly released: Promise<void>;
  run: <T>(run: () => Promise<T>) => Promise<T>;
};
export type Effect = {
  ref: SessionEffectRef;
  entry: SessionControllerEntry;
  phase: "queued" | "validating" | "writer" | "acquired" | "released";
  generation: string;
  interrupt?: SessionEffectInterrupt;
  interrupted?: Error;
  cancel: AbortController;
  handoffs: Set<string>;
  validated: Deferred;
};
export type Mutation = {
  entries: SessionControllerEntry[];
  targets: SessionTarget[];
  kind?: "reset" | "delete" | "compaction";
  phase: "queued" | "active" | "released";
  operations: Set<ReplyOperation>;
  ready: Deferred;
};
type Closure = { reason: Error };
export type OwnerContext = {
  claim?: SessionControllerMailboxClaim;
  operation?: ReplyOperation;
  effects: ReadonlySet<Effect>;
  mutations: ReadonlySet<Mutation>;
};
export type SessionControllerLifecycle = {
  targets: Map<SessionTarget, number>;
  operations: Set<ReplyOperation>;
  effects: Set<Effect>;
  mutations: Mutation[];
  closures: Set<Closure>;
  changed: Deferred;
  readonly blocksTurnAdmission: boolean;
};
