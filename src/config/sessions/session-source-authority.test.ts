import { expect, it, vi } from "vitest";
import {
  bindPreparedSessionSourceAssertion,
  composeSessionSourceAssertion,
  prepareSessionSourceAuthority,
  prepareSessionSourceScope,
  runWithSessionSourceScope,
} from "./session-source-authority.js";

it("allows a retained source to acquire a new fence but never renews released custody", async () => {
  let initialFenceCurrent = true;
  const release = vi.fn();
  const prepare = vi.fn(async () => ({ assertCurrent() {}, checks: [] }));
  const source = composeSessionSourceAssertion([
    Object.assign(() => {}, {
      prepareSessionSource: prepare,
      prepareSessionSourceScope: prepare,
    }),
  ]);
  const bound = bindPreparedSessionSourceAssertion(source, {
    assertCurrent() {
      if (!initialFenceCurrent) {
        throw new Error("initial fence expired");
      }
    },
    checks: [],
    release,
  });
  initialFenceCurrent = false;
  expect(bound).toThrow("initial fence expired");
  await expect(
    runWithSessionSourceScope(bound, async () => {
      bound();
      await bound.release();
      expect(bound).toThrow("released");
    }),
  ).rejects.toThrow("released");
  expect(release).toHaveBeenCalledOnce();
  const preparedCount = prepare.mock.calls.length;
  await expect(prepareSessionSourceAuthority(bound)).rejects.toThrow("released");
  await expect(prepareSessionSourceScope(bound)).rejects.toThrow("released");
  expect(prepare).toHaveBeenCalledTimes(preparedCount);
});
