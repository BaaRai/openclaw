import { KeyedAsyncQueue } from "../../plugin-sdk/keyed-async-queue.js";

type ActorLane = {
  queue: KeyedAsyncQueue;
  users: number;
  retired: boolean;
};

/** Serializes each current actor lane without retaining a history of retired lanes. */
export class SessionActorQueue {
  private readonly lanes = new Map<string, ActorLane>();
  private pendingCount = 0;

  getTotalPendingCount(): number {
    return this.pendingCount;
  }

  /** Holds a generation until the caller releases its cleanup/operation custody. */
  capture(actorKey: string) {
    let lane = this.lanes.get(actorKey);
    if (!lane) {
      lane = { queue: new KeyedAsyncQueue(), users: 0, retired: false };
      this.lanes.set(actorKey, lane);
    }
    const captured = lane;
    captured.users += 1;
    let released = false;
    return {
      queue: captured.queue,
      isCurrent: () => !released && !captured.retired,
      release: () => {
        if (released) {
          return;
        }
        released = true;
        captured.users -= 1;
        if (captured.users === 0 && this.lanes.get(actorKey) === captured) {
          this.lanes.delete(actorKey);
        }
      },
    };
  }

  async run<T>(actorKey: string, op: (isCurrent: () => boolean) => Promise<T>): Promise<T> {
    const captured = this.capture(actorKey);
    try {
      return await captured.queue.enqueue(
        actorKey,
        async () => {
          if (!captured.isCurrent()) {
            throw new Error(`ACP session actor was superseded for ${actorKey}.`);
          }
          return await op(captured.isCurrent);
        },
        {
          onEnqueue: () => {
            this.pendingCount += 1;
          },
          onSettle: () => {
            this.pendingCount -= 1;
          },
        },
      );
    } finally {
      captured.release();
    }
  }

  /** Retain existing lanes while an async selector discovers its target. */
  captureSelection(keys: Iterable<string>) {
    const captured = new Map(
      [...new Set([...this.lanes.keys(), ...keys])].map((key) => [key, this.capture(key)]),
    );
    let released = false;
    return {
      select: (key: string) => {
        if (released) {
          throw new Error("ACP actor selection was released.");
        }
        let actor = captured.get(key);
        if (!actor) {
          // Open an uncached idle session, but never adopt work admitted during routing.
          if (this.lanes.has(key)) {
            throw new Error("ACP session actor changed during selection.");
          }
          actor = this.capture(key);
          captured.set(key, actor);
        }
        return actor;
      },
      release: () => {
        released = true;
        for (const actor of captured.values()) {
          actor.release();
        }
        captured.clear();
      },
    };
  }

  /** Fresh work bypasses a stuck lane; only outstanding operations retain the retired token. */
  rotate(actorKey: string): void {
    const lane = this.lanes.get(actorKey);
    if (lane) {
      lane.retired = true;
      this.lanes.delete(actorKey);
    }
  }
}
