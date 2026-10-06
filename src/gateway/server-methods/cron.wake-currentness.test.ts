import { expect, it } from "vitest";
import {
  awaitGateBeforeSettlement,
  createDeferred,
  withinTest,
} from "../../../test/helpers/promise.js";
import { getRuntimeConfig } from "../../config/io.js";
import {
  clearRuntimeConfigSnapshot,
  getRuntimeConfigSnapshot,
  getRuntimeConfigSourceSnapshot,
  setRuntimeConfigSnapshot,
} from "../../config/runtime-snapshot.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import type { CronService } from "../../cron/service.js";
import { cronWakeHandler } from "./cron-wake.js";
import { createCronTestContext, createCronTestInvoker } from "./cron.validation.test-support.js";

const invokeCron = createCronTestInvoker({ wake: cronWakeHandler }, getRuntimeConfig);

it.for(
  (["preparation", "commit"] as const).flatMap((stage) =>
    (["replacement", "same object"] as const).map((publication) => ({ stage, publication })),
  ),
)(
  "rejects a changed wake owner at $stage after $publication publication",
  async ({ stage, publication }, { signal }) => {
    const previous = getRuntimeConfigSnapshot();
    const previousSource = getRuntimeConfigSourceSnapshot();
    const config: OpenClawConfig = { agents: { ownership: "explicit", entries: { main: {} } } };
    setRuntimeConfigSnapshot(config, config);
    const publishReload = () => {
      const replacement: OpenClawConfig = {
        agents: { ownership: "explicit", entries: { ops: {} } },
      };
      const next = publication === "same object" ? Object.assign(config, replacement) : replacement;
      setRuntimeConfigSnapshot(next, next);
    };
    const entered = createDeferred();
    const release = createDeferred();
    const context = createCronTestContext(undefined, getRuntimeConfig);
    const committed: Array<Parameters<CronService["wake"]>[0]> = [];
    context.cron.prepareWake.mockImplementationOnce(async () => {
      if (stage === "preparation") {
        entered.resolve();
        await release.promise;
      }
    });
    context.cron.wake.mockImplementationOnce((options) => {
      if (stage === "commit") {
        publishReload();
      }
      options.commitGuard?.();
      committed.push(options);
      return { ok: true };
    });
    const invocation = invokeCron(
      "wake",
      { mode: "now", text: "bound wake", sessionKey: "main" },
      { context },
    );
    const outcome = invocation.then(
      () => undefined,
      (error: unknown) => error,
    );
    try {
      if (stage === "preparation") {
        await withinTest(
          awaitGateBeforeSettlement(entered.promise, invocation, "wake settled before preparation"),
          signal,
        );
        publishReload();
        release.resolve();
      }
      const error = await withinTest(outcome, signal);
      expect(committed).toEqual([]);
      expect(error).toBeInstanceOf(Error);
      expect(String(error)).toContain("Wake configuration changed during preparation");
    } finally {
      release.resolve();
      await outcome;
      if (previous) {
        setRuntimeConfigSnapshot(previous, previousSource ?? undefined);
      } else {
        clearRuntimeConfigSnapshot();
      }
    }
  },
);
