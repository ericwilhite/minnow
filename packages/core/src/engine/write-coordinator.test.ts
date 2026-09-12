import { afterEach, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { _resetWriteAdmissionForTests, coordinateWrite } from "./write-coordinator.js";

/** A LockManager whose grants the test controls; `never` refuses forever, like a paused holder. */
function fakeLocks(mode: "grant" | "never"): { locks: LockManager; requests: number } {
  const state = { requests: 0 };
  const request: LockManager["request"] = async (
    _name: string,
    optionsOrCallback: LockOptions | LockGrantedCallback<unknown>,
    maybeCallback?: LockGrantedCallback<unknown>,
  ) => {
    state.requests += 1;
    const options = typeof optionsOrCallback === "function" ? {} : optionsOrCallback;
    const callback = typeof optionsOrCallback === "function" ? optionsOrCallback : maybeCallback;
    if (mode === "grant") return callback?.({ name: "lock", mode: "exclusive" });
    return new Promise((_resolve, reject) => {
      options.signal?.addEventListener("abort", () => {
        const reason: unknown = options.signal?.reason;
        reject(reason instanceof Error ? reason : new Error("aborted", { cause: reason }));
      });
    });
  };
  return {
    locks: { request, query: async () => ({ held: [], pending: [] }) },
    requests: state.requests,
  };
}

const originalNavigator = globalThis.navigator;

afterEach(() => {
  _resetWriteAdmissionForTests();
  Object.defineProperty(globalThis, "navigator", {
    value: originalNavigator,
    configurable: true,
    writable: true,
  });
});

/**
 * A holder that never lets go, modelled faithfully: a plain request waits until its signal
 * aborts, and an `ifAvailable` request is answered at once with `null`. `release` frees the
 * lock, after which every request is granted.
 */
function frozenHolder(): {
  locks: LockManager;
  waits: () => number;
  probes: () => number;
  release: () => void;
} {
  let held = true;
  let waits = 0;
  let probes = 0;
  const request: LockManager["request"] = async (
    _name: string,
    optionsOrCallback: LockOptions | LockGrantedCallback<unknown>,
    maybeCallback?: LockGrantedCallback<unknown>,
  ) => {
    const options = typeof optionsOrCallback === "function" ? {} : optionsOrCallback;
    const callback = typeof optionsOrCallback === "function" ? optionsOrCallback : maybeCallback;
    if (!held) return callback?.({ name: "lock", mode: "exclusive" });
    if (options.ifAvailable === true) {
      probes += 1;
      return callback?.(null);
    }
    waits += 1;
    return new Promise((_resolve, reject) => {
      options.signal?.addEventListener("abort", () => {
        const reason: unknown = options.signal?.reason;
        reject(reason instanceof Error ? reason : new Error("aborted", { cause: reason }));
      });
    });
  };
  return {
    locks: { request, query: async () => ({ held: [], pending: [] }) },
    waits: () => waits,
    probes: () => probes,
    release: () => {
      held = false;
    },
  };
}

it("waits out a frozen holder once, then lets later writes go ahead at once until it lets go", async () => {
  const holder = frozenHolder();
  installLocks(holder.locks);
  const store = new MemoryBlockStore();
  Object.defineProperty(store, "liveQueryChannelName", { value: "minnowdb-live:frozen" });
  const exceeded = vi.fn();
  const write = (): Promise<number> =>
    coordinateWrite(store, async () => Date.now(), new AbortController().signal, {
      admissionWaitMs: 40,
      onAdmissionWaitExceeded: exceeded,
    });
  const started = Date.now();
  await write();
  const firstDone = Date.now() - started;
  expect(firstDone).toBeGreaterThanOrEqual(35);
  // Six more writes, queued together as a burst would be: none waits the admission wait
  // again — a serial 6 × 40 ms here stood for 6 × 10 s in a real tab.
  const burstStarted = Date.now();
  await Promise.all(Array.from({ length: 6 }, write));
  expect(Date.now() - burstStarted).toBeLessThan(35);
  expect(holder.waits()).toBe(1);
  expect(holder.probes()).toBe(6);
  expect(exceeded).toHaveBeenCalledTimes(1);
  // The holder lets go: the next probe is granted, coordination resumes, and a later write
  // takes the lock the ordinary way again.
  holder.release();
  await write();
  await write();
  expect(holder.probes()).toBe(6);
  expect(exceeded).toHaveBeenCalledTimes(1);
});

function installLocks(locks: LockManager): void {
  Object.defineProperty(globalThis, "navigator", {
    value: { locks },
    configurable: true,
    writable: true,
  });
}

it("goes ahead without the cross-tab lock once the wait runs out, and says so", async () => {
  installLocks(fakeLocks("never").locks);
  const store = new MemoryBlockStore();
  Object.defineProperty(store, "liveQueryChannelName", { value: "minnowdb-live:test" });
  const exceeded = vi.fn();
  const started = Date.now();
  const result = await coordinateWrite(store, async () => "wrote", new AbortController().signal, {
    admissionWaitMs: 30,
    onAdmissionWaitExceeded: exceeded,
  });
  expect(result).toBe("wrote");
  expect(Date.now() - started).toBeGreaterThanOrEqual(25);
  expect(exceeded).toHaveBeenCalledTimes(1);
  expect(exceeded.mock.calls[0]?.[0]).toBeGreaterThanOrEqual(25);
});

it("runs under the lock when it is granted, without reporting a wait", async () => {
  installLocks(fakeLocks("grant").locks);
  const store = new MemoryBlockStore();
  Object.defineProperty(store, "liveQueryChannelName", { value: "minnowdb-live:granted" });
  const exceeded = vi.fn();
  await expect(
    coordinateWrite(store, async () => 7, new AbortController().signal, {
      admissionWaitMs: 30,
      onAdmissionWaitExceeded: exceeded,
    }),
  ).resolves.toBe(7);
  expect(exceeded).not.toHaveBeenCalled();
});

it("still honors a close that arrives while the lock wait is running", async () => {
  installLocks(fakeLocks("never").locks);
  const store = new MemoryBlockStore();
  Object.defineProperty(store, "liveQueryChannelName", { value: "minnowdb-live:closing" });
  const shutdown = new AbortController();
  const pending = coordinateWrite(store, async () => "never", shutdown.signal, {
    admissionWaitMs: 10_000,
  });
  shutdown.abort(new Error("closing"));
  await expect(pending).rejects.toThrow("closing");
});
