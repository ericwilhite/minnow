import { afterEach, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { coordinateWrite } from "./write-coordinator.js";

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
  Object.defineProperty(globalThis, "navigator", {
    value: originalNavigator,
    configurable: true,
    writable: true,
  });
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
