import { afterEach, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import {
  admitWriter,
  markStoreUncoordinatedForTests,
  writeAdmissionState,
  writeCoordinationScope,
  type WriteAdmissionStall,
} from "./write-coordinator.js";

interface Grant {
  name: string;
  mode: "exclusive" | "shared";
}

/**
 * A lock manager the test controls. `held` names the client currently holding the lock (the
 * engine's own grants are recorded as "self"); requests wait until the holder lets go.
 */
function fakeLocks(): {
  locks: LockManager;
  requests: () => number;
  hold: (clientId: string) => () => void;
} {
  let holder: string | undefined;
  let requests = 0;
  const waiters: Array<() => void> = [];
  const wake = (): void => {
    const next = waiters.shift();
    next?.();
  };
  const request: LockManager["request"] = async (
    name: string,
    optionsOrCallback: LockOptions | LockGrantedCallback<unknown>,
    maybeCallback?: LockGrantedCallback<unknown>,
  ) => {
    requests += 1;
    const options = typeof optionsOrCallback === "function" ? {} : optionsOrCallback;
    const callback = typeof optionsOrCallback === "function" ? optionsOrCallback : maybeCallback;
    if (options.ifAvailable === true && holder !== undefined) return callback?.(null);
    while (holder !== undefined) {
      if (options.signal?.aborted === true) {
        const reason: unknown = options.signal.reason;
        throw reason instanceof Error ? reason : new Error("aborted", { cause: reason });
      }
      await new Promise<void>((resolve) => {
        const onAbort = (): void => {
          const index = waiters.indexOf(resolve);
          if (index !== -1) waiters.splice(index, 1);
          resolve();
        };
        options.signal?.addEventListener("abort", onAbort, { once: true });
        waiters.push(() => {
          options.signal?.removeEventListener("abort", onAbort);
          resolve();
        });
      });
    }
    holder = "self";
    try {
      return await callback?.({ name, mode: "exclusive" } satisfies Grant);
    } finally {
      holder = undefined;
      wake();
    }
  };
  const query: LockManager["query"] = async () => ({
    held:
      holder === undefined ? [] : [{ name: "minnowdb-write:minnowdb-live:test", clientId: holder }],
    pending: [],
  });
  return {
    locks: { request, query },
    requests: () => requests,
    hold: (clientId) => {
      if (holder !== undefined) throw new Error("already held");
      holder = clientId;
      return () => {
        holder = undefined;
        wake();
      };
    },
  };
}

const originalNavigator = globalThis.navigator;

function installLocks(locks: LockManager | undefined): void {
  Object.defineProperty(globalThis, "navigator", {
    value: locks === undefined ? undefined : { locks },
    configurable: true,
    writable: true,
  });
}

function namedStore(name = "minnowdb-live:test"): MemoryBlockStore {
  const store = new MemoryBlockStore();
  Object.defineProperty(store, "liveQueryChannelName", { value: name });
  return store;
}

afterEach(() => {
  vi.useRealTimers();
  Object.defineProperty(globalThis, "navigator", {
    value: originalNavigator,
    configurable: true,
    writable: true,
  });
});

it("runs writers one at a time in arrival order, whichever engine asked", async () => {
  installLocks(undefined);
  const store = namedStore();
  const order: string[] = [];
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const signal = new AbortController().signal;
  const first = admitWriter(store, { kind: "scope", signal }, async () => {
    order.push("first:start");
    await gate;
    order.push("first:end");
  });
  const second = admitWriter(store, { kind: "autocommit", signal }, async () => {
    order.push("second");
  });
  await new Promise((resolve) => setTimeout(resolve, 5));
  expect(order).toEqual(["first:start"]);
  expect(writeAdmissionState(store)).toMatchObject({ waiting: 1, holder: { kind: "scope" } });
  release();
  await Promise.all([first, second]);
  expect(order).toEqual(["first:start", "first:end", "second"]);
  expect(writeAdmissionState(store)).toEqual({ holder: undefined, waiting: 0 });
});

it("keeps different databases on different queues", async () => {
  installLocks(undefined);
  const blocked = namedStore("minnowdb-live:a");
  const other = namedStore("minnowdb-live:b");
  const signal = new AbortController().signal;
  const held = admitWriter(blocked, { kind: "scope", signal }, () => new Promise(() => undefined));
  void held;
  await expect(admitWriter(other, { kind: "scope", signal }, async () => "ran")).resolves.toBe(
    "ran",
  );
});

it("asks the lock manager only from the head of the local queue", async () => {
  const fake = fakeLocks();
  installLocks(fake.locks);
  const store = namedStore();
  const signal = new AbortController().signal;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const first = admitWriter(store, { kind: "scope", signal }, () => gate);
  const rest = Array.from({ length: 4 }, () =>
    admitWriter(store, { kind: "autocommit", signal }, async () => undefined),
  );
  await new Promise((resolve) => setTimeout(resolve, 5));
  expect(fake.requests()).toBe(1);
  release();
  await Promise.all([first, ...rest]);
  expect(fake.requests()).toBe(5);
});

it("cancels a queued writer at once without running it, and the queue moves on", async () => {
  installLocks(undefined);
  const store = namedStore();
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const holder = admitWriter(
    store,
    { kind: "scope", signal: new AbortController().signal },
    () => gate,
  );
  const controller = new AbortController();
  const callback = vi.fn(async () => undefined);
  const queued = admitWriter(store, { kind: "scope", signal: controller.signal }, callback);
  const after = admitWriter(
    store,
    { kind: "scope", signal: new AbortController().signal },
    async () => "after",
  );
  controller.abort(new Error("caller gave up"));
  await expect(queued).rejects.toThrow("caller gave up");
  release();
  await holder;
  await expect(after).resolves.toBe("after");
  expect(callback).not.toHaveBeenCalled();
});

it("cancels a writer waiting on another context's lock without taking the lock", async () => {
  const fake = fakeLocks();
  installLocks(fake.locks);
  const store = namedStore();
  const letGo = fake.hold("other-tab");
  const controller = new AbortController();
  const callback = vi.fn(async () => undefined);
  const waiting = admitWriter(store, { kind: "scope", signal: controller.signal }, callback);
  await new Promise((resolve) => setTimeout(resolve, 5));
  controller.abort(new Error("closed"));
  await expect(waiting).rejects.toThrow("closed");
  expect(callback).not.toHaveBeenCalled();
  letGo();
  // The lock is free and the local queue empty: the next writer runs under the lock.
  await expect(
    admitWriter(store, { kind: "scope", signal: new AbortController().signal }, async () => 1),
  ).resolves.toBe(1);
  expect(writeAdmissionState(store)).toEqual({ holder: undefined, waiting: 0 });
});

it("reports a holder that does not change once, keeps waiting, and never bypasses it", async () => {
  vi.useFakeTimers();
  const fake = fakeLocks();
  installLocks(fake.locks);
  const store = namedStore();
  const letGo = fake.hold("frozen-tab");
  const stalls: WriteAdmissionStall[] = [];
  let ran = false;
  const waiting = admitWriter(
    store,
    {
      kind: "autocommit",
      signal: new AbortController().signal,
      stallReportMs: 100,
      onStalled: (stall) => stalls.push(stall),
    },
    async () => {
      ran = true;
    },
  );
  await vi.advanceTimersByTimeAsync(1_000);
  expect(ran).toBe(false);
  expect(stalls).toHaveLength(1);
  expect(stalls[0]).toMatchObject({ holder: "other-context" });
  expect(stalls[0]?.waitedMs).toBeGreaterThanOrEqual(100);
  letGo();
  await vi.advanceTimersByTimeAsync(10);
  await waiting;
  expect(ran).toBe(true);
  expect(fake.requests()).toBe(1);
});

it("does not report a queue whose holders keep changing", async () => {
  vi.useFakeTimers();
  const fake = fakeLocks();
  installLocks(fake.locks);
  const store = namedStore();
  const stalls: WriteAdmissionStall[] = [];
  let letGo = fake.hold("tab-1");
  const waiting = admitWriter(
    store,
    {
      kind: "autocommit",
      signal: new AbortController().signal,
      stallReportMs: 100,
      onStalled: (stall) => stalls.push(stall),
    },
    async () => "ran",
  );
  for (let holder = 2; holder <= 5; holder += 1) {
    await vi.advanceTimersByTimeAsync(70);
    letGo();
    letGo = fake.hold(`tab-${String(holder)}`);
  }
  expect(stalls).toEqual([]);
  letGo();
  await vi.advanceTimersByTimeAsync(10);
  await expect(waiting).resolves.toBe("ran");
});

it("names a local holder in its stall report", async () => {
  vi.useFakeTimers();
  installLocks(undefined);
  const store = namedStore();
  const stalls: WriteAdmissionStall[] = [];
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const holder = admitWriter(
    store,
    { kind: "transaction", signal: new AbortController().signal },
    () => gate,
  );
  const waiting = admitWriter(
    store,
    {
      kind: "scope",
      signal: new AbortController().signal,
      stallReportMs: 100,
      onStalled: (stall) => stalls.push(stall),
    },
    async () => "ran",
  );
  await vi.advanceTimersByTimeAsync(500);
  expect(stalls).toHaveLength(1);
  expect(stalls[0]).toMatchObject({ holder: "this-context", holderKind: "transaction" });
  release();
  await holder;
  await expect(waiting).resolves.toBe("ran");
});

it("surfaces the callback's own failure and releases the turn", async () => {
  installLocks(undefined);
  const store = namedStore();
  const signal = new AbortController().signal;
  await expect(
    admitWriter(store, { kind: "scope", signal }, async () => {
      throw new Error("callback failed");
    }),
  ).rejects.toThrow("callback failed");
  await expect(admitWriter(store, { kind: "scope", signal }, async () => "next")).resolves.toBe(
    "next",
  );
});

it("lets a store marked uncoordinated publish without a turn", async () => {
  const fake = fakeLocks();
  installLocks(fake.locks);
  const store = namedStore();
  markStoreUncoordinatedForTests(store);
  const letGo = fake.hold("other-tab");
  await expect(
    admitWriter(
      store,
      { kind: "scope", signal: new AbortController().signal },
      async () => "rogue",
    ),
  ).resolves.toBe("rogue");
  expect(fake.requests()).toBe(0);
  letGo();
});

it("describes how far coordination reaches", () => {
  installLocks(fakeLocks().locks);
  expect(writeCoordinationScope(namedStore())).toBe("cross-context");
  expect(writeCoordinationScope(new MemoryBlockStore())).toBe("instance");
  installLocks(undefined);
  expect(writeCoordinationScope(namedStore())).toBe("context");
});

it("cancels only the wait on another context's lock, never the local queue", async () => {
  const fake = fakeLocks();
  installLocks(fake.locks);
  const store = namedStore();
  const crossContext = new AbortController();
  const signal = new AbortController().signal;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  // A local holder, then a local writer queued behind it: both are served after cancellation.
  const holder = admitWriter(
    store,
    { kind: "scope", signal, crossContextSignal: crossContext.signal },
    () => gate,
  );
  const queued = admitWriter(
    store,
    { kind: "autocommit", signal, crossContextSignal: crossContext.signal },
    async () => "served locally",
  );
  crossContext.abort(new Error("connection disposed"));
  release();
  await holder;
  await expect(queued).resolves.toBe("served locally");
  // A writer that must wait for another tab is refused, before and after it asks for the lock.
  const letGo = fake.hold("other-tab");
  await expect(
    admitWriter(
      store,
      { kind: "autocommit", signal, crossContextSignal: crossContext.signal },
      async () => "never",
    ),
  ).rejects.toThrow("connection disposed");
  const live = new AbortController();
  const waiting = admitWriter(
    store,
    { kind: "autocommit", signal, crossContextSignal: live.signal },
    async () => "never",
  );
  await new Promise((resolve) => setTimeout(resolve, 5));
  live.abort(new Error("disposed while waiting"));
  await expect(waiting).rejects.toThrow("disposed while waiting");
  letGo();
});
