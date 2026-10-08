import { EventEmitter } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { terminateCodexAppServerDescendants } from "./transport-process-containment.js";
import { closeCodexAppServerTransportAndWait } from "./transport.js";

vi.mock("openclaw/plugin-sdk/process-runtime", async (importOriginal) => ({
  ...(await importOriginal<typeof import("openclaw/plugin-sdk/process-runtime")>()),
  scheduleAdoptedChildZombieReapAfterExit: vi.fn(),
  scheduleAdoptedDescendantReapAfterRootExit: vi.fn(),
}));
vi.mock("./transport-process-containment.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./transport-process-containment.js")>()),
  terminateCodexAppServerDescendants: vi.fn(),
}));
vi.mock("./transport-process-registration.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./transport-process-registration.js")>()),
  waitForCodexAppServerProcessRegistrationCleanup: vi.fn(async () => {}),
}));

const { scheduleAdoptedChildZombieReapAfterExit, scheduleAdoptedDescendantReapAfterRootExit } =
  await import("openclaw/plugin-sdk/process-runtime");

type FakeTransport = EventEmitter & {
  pid: number;
  exitCode: number | null;
  signalCode: string | null;
  kill: ReturnType<typeof vi.fn>;
  unref: ReturnType<typeof vi.fn>;
  stdin: {
    write: ReturnType<typeof vi.fn>;
    end: ReturnType<typeof vi.fn>;
    destroy: ReturnType<typeof vi.fn>;
    unref: ReturnType<typeof vi.fn>;
  };
  stdout: { destroy: ReturnType<typeof vi.fn>; unref: ReturnType<typeof vi.fn> };
  stderr: { destroy: ReturnType<typeof vi.fn>; unref: ReturnType<typeof vi.fn> };
};

function fakeTransport(overrides: Partial<FakeTransport> = {}): FakeTransport {
  return Object.assign(new EventEmitter(), {
    pid: 700,
    exitCode: null,
    signalCode: null,
    killed: false,
    kill: vi.fn(),
    unref: vi.fn(),
    stdin: {
      write: vi.fn(),
      end: vi.fn(),
      destroy: vi.fn(),
      unref: vi.fn(),
      on: vi.fn(),
    },
    stdout: { destroy: vi.fn(), unref: vi.fn() },
    stderr: { destroy: vi.fn(), unref: vi.fn() },
    ...overrides,
  }) as FakeTransport;
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.spyOn(process, "platform", "get").mockReturnValue("linux");
  delete process.env.OPENCLAW_GATEWAY_HOST_LIFELINE;
});
afterEach(() => {
  vi.restoreAllMocks();
});

describe("Codex app-server transport close reaping", () => {
  it("retains observed descendant identities for adopted-zombie reaping", async () => {
    const descendants = [
      { pid: 401, pgid: 700, startedAt: "boot:100" },
      { pid: 402, pgid: 900, startedAt: "boot:101" },
    ];
    vi.mocked(terminateCodexAppServerDescendants).mockResolvedValue({
      root: {
        pid: 700,
        ppid: process.pid,
        pgid: 700,
        state: "T",
        startedAt: "boot:99",
      },
      resume: vi.fn(),
      descendants,
    });
    const child = fakeTransport();
    const closed = closeCodexAppServerTransportAndWait(child);
    await delay(5);
    child.emit("exit", 0, null);
    expect(await closed).toEqual({ exited: true, cleanup: "closed" });
    expect(scheduleAdoptedDescendantReapAfterRootExit).toHaveBeenCalledTimes(1);
    expect(scheduleAdoptedDescendantReapAfterRootExit).toHaveBeenCalledWith(child, descendants);
    expect(scheduleAdoptedChildZombieReapAfterExit).not.toHaveBeenCalled();
  });

  it("retains group-scoped cleanup when containment cannot prove identities", async () => {
    vi.mocked(terminateCodexAppServerDescendants).mockResolvedValue(undefined);
    const child = fakeTransport();
    const closed = closeCodexAppServerTransportAndWait(child);
    await delay(5);
    child.emit("exit", null, "SIGKILL");
    expect(await closed).toEqual({ exited: true, cleanup: "uncertain" });
    expect(scheduleAdoptedDescendantReapAfterRootExit).not.toHaveBeenCalled();
    expect(scheduleAdoptedChildZombieReapAfterExit).toHaveBeenCalledWith(child, true);
  });

  it("reports a detached-root group only outside hosted-gateway mode", async () => {
    process.env.OPENCLAW_GATEWAY_HOST_LIFELINE = "stdin";
    vi.mocked(terminateCodexAppServerDescendants).mockResolvedValue(undefined);
    const child = fakeTransport();
    const closed = closeCodexAppServerTransportAndWait(child);
    await delay(5);
    child.emit("exit", null, "SIGKILL");
    expect(await closed).toEqual({ exited: true, cleanup: "uncertain" });
    expect(scheduleAdoptedChildZombieReapAfterExit).toHaveBeenCalledWith(child, false);
  });

  it("schedules no reaping for a transport that already exited", async () => {
    const child = fakeTransport({ exitCode: 0 });
    const closed = closeCodexAppServerTransportAndWait(child);
    await delay(5);
    child.emit("exit", 0, null);
    expect(await closed).toEqual({ exited: true, cleanup: "uncertain" });
    expect(terminateCodexAppServerDescendants).not.toHaveBeenCalled();
    expect(scheduleAdoptedDescendantReapAfterRootExit).not.toHaveBeenCalled();
    expect(scheduleAdoptedChildZombieReapAfterExit).not.toHaveBeenCalled();
  });
});
