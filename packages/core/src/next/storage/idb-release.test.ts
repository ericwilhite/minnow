/**
 * Review B1-p2h-1 on the IndexedDB lease release (indexeddb.ts, `TrackedConnection`):
 *
 * 1. A release whose wait runs out aborts the transactions still running, waits for their terminal
 *    events, and only then closes the connection and releases the lock: the next owner never
 *    starts while the old one has a transaction live. A strict commit it aborted rejects (not
 *    durable), queued transactions are refused before they begin, and the release reports how
 *    many drained and how many it aborted.
 * 2. Every acquisition has its own connection: releasing an obsolete (stolen) lease closes only
 *    its own, never its successor's.
 *
 * The reviewer's probes run over fake-indexeddb (real transactions, abort events and close); the
 * concurrent (unserialized) case over a double whose transactions never finish by themselves.
 *
 * Review B1-p2h-2 finding 2: an aborted transaction that never reports its end must not hold the
 * lock forever. After a second bound the release closes, releases the lock and rejects with
 * `ReleaseUnsettledError`; the transaction's operation rejects with it too (outcome unknown, never
 * acknowledged). A terminal event arriving later changes nothing, a repeated release rejects the
 * same way, and a successor owns the database afterwards, on the same connection object too.
 */
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import {
  OwnershipLostError,
  ReleaseUnsettledError,
  StoreClosedError,
  type OwnerLease,
  type ScratchSpace,
  type StorageBackend,
} from "./contract.js";
import {
  openIndexedDb,
  type IdbDatabaseLike,
  type IdbStoreLike,
  type IdbTransactionLike,
} from "./idb-like.js";
import { createIndexedDbBackend } from "./indexeddb.js";
import { SimDisk, SimLocks } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";

// idb-like.ts builds key ranges with the global, as in a browser.
(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

const tick = (ms = 0) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

/** The IndexedDB backend always has a spill store. */
function scratchOf(backend: StorageBackend, lease: OwnerLease): ScratchSpace {
  const scratch = backend.scratch(lease);
  if (scratch === null) throw new Error("no spill store");
  return scratch;
}

/** A factory that hands out the native connections it opens, so a test can instrument them. */
function instrumentedFactory() {
  const factory = new IDBFactory();
  const natives: IDBDatabase[] = [];
  const open = (name: string, version?: number) => {
    const request = factory.open(name, version);
    request.addEventListener("success", () => {
      natives.push(request.result);
    });
    return request;
  };
  return { factory: { open } as unknown as IDBFactory, natives };
}

describe("a release whose wait runs out aborts, settles, closes, then unlocks (finding 1)", () => {
  it("the reviewer's probe: no native transaction is live when the next owner gets the lock", async () => {
    const { factory, natives } = instrumentedFactory();
    const locks = new SimLocks();
    const events: string[] = [];
    const backend = createIndexedDbBackend({
      database: () => openIndexedDb(factory, "timeout"),
      locks,
      name: "timeout",
      releaseDrainMs: 50,
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    const native = natives.at(-1);
    if (native === undefined) throw new Error("no native connection");
    // From here every native transaction keeps itself alive (a request in each callback) until
    // `keepAlive` is cleared: one that would outlast any drain window.
    let keepAlive = true;
    let live = 0;
    let created = 0;
    const transaction = native.transaction.bind(native);
    native.transaction = (...args: Parameters<IDBDatabase["transaction"]>) => {
      const tx = transaction(...args);
      created += 1;
      live += 1;
      const spin = () => {
        if (keepAlive) tx.objectStore("meta").get("epoch").onsuccess = spin;
      };
      spin();
      const end = (kind: string) => () => {
        live -= 1;
        events.push(kind);
      };
      tx.addEventListener("complete", end("complete"));
      tx.addEventListener("abort", end("abort"));
      return tx;
    };
    const close = native.close.bind(native);
    native.close = () => {
      events.push("close");
      close();
    };

    const publishing = session.publish(new Uint8Array([1]));
    const publishOutcome = publishing.then(
      () => "committed",
      (error: unknown) => error,
    );
    await tick(10);
    // The spill's first write (it loads its record index) queues behind the publish on the
    // connection's chain: refused before it is created. A read queues on the session's own chain
    // and is refused by the closing connection once the publish fails.
    const spilled = scratchOf(backend, lease)
      .write(0, new Uint8Array(8))
      .then(
        () => "written",
        (error: unknown) => error,
      );
    const read = session.readRoot().then(
      () => "read",
      (error: unknown) => error,
    );
    await tick();
    // The next owner is already waiting for the lock while the release runs.
    let liveAtGrant = -1;
    const granted = locks.request("minnow:timeout", {}).then((held) => {
      events.push("granted");
      liveAtGrant = live;
      return held;
    });
    const report = await lease.release();
    const next = await granted;
    keepAlive = false;
    next.release();

    expect(liveAtGrant).toBe(0);
    expect(events).toEqual(["abort", "close", "granted"]);
    expect(report).toEqual({ drained: 0, aborted: 2 });
    expect(created).toBe(1);
    // The strict commit was aborted: it fails, and nothing of it is durable.
    expect(await publishOutcome).toBeInstanceOf(StoreClosedError);
    expect(await spilled).toBeInstanceOf(StoreClosedError);
    expect(await read).toBeInstanceOf(StoreClosedError);
    const after = await backend.open(await backend.acquire());
    expect(await after.readRoot()).toBe(null);
  });

  it("aborts every transaction of an unserialized connection and reports each", async () => {
    const disk = new SimDisk();
    const sim = simIndexedDb(disk);
    let hang = false;
    let closes = 0;
    const never: IdbStoreLike = {
      get: () => undefined,
      getAll: () => undefined,
      getAllKeys: () => undefined,
      last: () => undefined,
      put: () => undefined,
      delete: () => undefined,
      deleteRange: () => undefined,
      clear: () => undefined,
    };
    const database: IdbDatabaseLike = {
      transaction: (stores, mode, durability, body) =>
        hang
          ? // A transaction whose requests never answer: it ends only when aborted.
            new Promise<void>((_resolve, reject) => {
              body({ store: () => never, abort: reject });
            })
          : sim.transaction(stores, mode, durability, body),
      close: () => {
        closes += 1;
      },
    };
    const backend = createIndexedDbBackend({
      database: () => database,
      locks: new SimLocks(),
      name: "unserialized",
      serializeTransactions: false,
      releaseDrainMs: 20,
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    hang = true;
    const outcomes = [
      session.publish(new Uint8Array([1])),
      scratchOf(backend, lease).write(0, new Uint8Array(8)),
    ].map((run) =>
      run.then(
        () => "settled",
        (error: unknown) => error,
      ),
    );
    await tick();
    expect(await lease.release()).toEqual({ drained: 0, aborted: 2 });
    for (const outcome of await Promise.all(outcomes))
      expect(outcome).toBeInstanceOf(StoreClosedError);
    expect(closes).toBe(1);
  });

  it("a release within the window reports what drained and aborts nothing", async () => {
    const backend = createIndexedDbBackend({
      database: () => simIndexedDb(new SimDisk()),
      locks: new SimLocks(),
      name: "drained",
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    const publishing = session.publish(new Uint8Array([1]));
    await Promise.resolve();
    const [, report] = await Promise.all([publishing, lease.release()]);
    expect(report).toEqual({ drained: 1, aborted: 0 });
  });
});

describe("every acquisition has its own connection (finding 2)", () => {
  it("the reviewer's probe: releasing a stolen lease leaves its successor's connection open", async () => {
    const disk = new SimDisk();
    const closed: number[] = [];
    let opened = 0;
    const backend = createIndexedDbBackend({
      database: () => {
        const id = (opened += 1);
        const inner = simIndexedDb(disk);
        return {
          transaction: (stores, mode, durability, body) => {
            if (closed.includes(id)) throw new Error(`connection ${String(id)} is closed`);
            return inner.transaction(stores, mode, durability, body);
          },
          close: () => {
            closed.push(id);
          },
        };
      },
      locks: new SimLocks(),
      name: "steal",
    });
    const first = await backend.acquire();
    const second = await backend.acquire({ steal: true });
    const session = await backend.open(second);
    await first.release();
    expect(closed).toEqual([1]);
    // The reviewer's sequence: the successor's read after the obsolete lease's release.
    expect(await session.readRoot()).toBe(null);
    await session.publish(new Uint8Array([2]));
    expect(await session.readRoot()).toEqual(new Uint8Array([2]));
    await second.release();
    expect(closed).toEqual([1, 2]);
  });

  it("one database object handed to every acquisition closes only when the last releases", async () => {
    let closes = 0;
    const inner = simIndexedDb(new SimDisk());
    const shared: IdbDatabaseLike = {
      transaction: (stores, mode, durability, body) => {
        if (closes > 0) throw new Error("closed");
        return inner.transaction(stores, mode, durability, body);
      },
      close: () => {
        closes += 1;
      },
    };
    const backend = createIndexedDbBackend({
      database: () => shared,
      locks: new SimLocks(),
      name: "shared",
    });
    const first = await backend.acquire();
    const second = await backend.acquire({ steal: true });
    const session = await backend.open(second);
    await first.release();
    expect(closes).toBe(0);
    expect(await session.readRoot()).toBe(null);
    await second.release();
    expect(closes).toBe(1);
  });

  it("the reviewer's races: double release, release during open, session close during release", async () => {
    const inner = simIndexedDb(new SimDisk());
    const backend = createIndexedDbBackend({
      database: () => ({
        transaction: async (stores, mode, durability, body: (tx: IdbTransactionLike) => void) => {
          await inner.transaction(stores, mode, durability, body);
          await tick(5);
        },
        close: () => undefined,
      }),
      locks: new SimLocks(),
      name: "races",
    });
    const lease = await backend.acquire();
    const opening = backend.open(lease);
    const refused = expect(opening).rejects.toBeInstanceOf(StoreClosedError);
    const [one, two] = await Promise.all([lease.release(), lease.release(), refused]);
    expect(two).toEqual(one);
    const nextLease = await backend.acquire();
    const session = await backend.open(nextLease);
    await Promise.all([session.close(), nextLease.release()]);
    await expect(backend.open(nextLease)).rejects.toBeInstanceOf(OwnershipLostError);
  });
});

/** What `promise` settled to within `ms`: its value, its error, or "pending". */
async function outcomeWithin(promise: Promise<unknown>, ms: number): Promise<unknown> {
  return Promise.race([
    promise.then(
      (value) => ({ value }),
      (error: unknown) => ({ error }),
    ),
    tick(ms).then(() => "pending"),
  ]);
}

const inert: IdbStoreLike = {
  get: () => undefined,
  getAll: () => undefined,
  getAllKeys: () => undefined,
  last: () => undefined,
  put: () => undefined,
  delete: () => undefined,
  deleteRange: () => undefined,
  clear: () => undefined,
};

/**
 * One connection object over the simulated IndexedDB whose next transactions follow `plans`:
 * "withhold" accepts abort but never reports an end until the test calls its finisher; "honour"
 * ends with the abort reason when aborted. Neither answers requests. Others run normally.
 */
function faultyConnection() {
  const inner = simIndexedDb(new SimDisk());
  const state = {
    plans: [] as Array<"withhold" | "honour">,
    aborts: 0,
    closes: 0,
    finishers: [] as Array<() => void>,
  };
  const database: IdbDatabaseLike = {
    transaction(stores, mode, durability, body) {
      const plan = state.plans.shift();
      if (plan === undefined) return inner.transaction(stores, mode, durability, body);
      return new Promise<void>((resolve, reject) => {
        state.finishers.push(resolve);
        body({
          store: () => inert,
          abort: (reason) => {
            state.aborts += 1;
            if (plan === "honour") reject(reason);
          },
        });
      });
    },
    close: () => {
      state.closes += 1;
    },
  };
  return { state, database };
}

describe("a release gives up on aborted transactions that never end (review B1-p2h-2 finding 2)", () => {
  it("rejects with ReleaseUnsettledError after releasing the lock; late events change nothing", async () => {
    const { state, database } = faultyConnection();
    const locks = new SimLocks();
    const backend = createIndexedDbBackend({
      database: () => database,
      locks,
      name: "unsettled",
      releaseDrainMs: 5,
      releaseSettleMs: 30,
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    state.plans.push("withhold");
    const publishing = session.publish(new Uint8Array([1]));
    await tick();
    // The next owner is already waiting for the lock.
    const granted = locks.request("minnow:unsettled", {});
    const released = lease.release();

    const release = await outcomeWithin(released, 2_000);
    expect(release).not.toBe("pending");
    const error = (release as { error?: unknown }).error;
    expect(error).toBeInstanceOf(ReleaseUnsettledError);
    const unsettled = error as ReleaseUnsettledError;
    expect({
      drained: unsettled.drained,
      aborted: unsettled.aborted,
      unsettled: unsettled.unsettled,
    }).toEqual({ drained: 0, aborted: 0, unsettled: 1 });
    expect(unsettled.transactions).toEqual([
      expect.objectContaining({ mode: "readwrite", durability: "strict" }),
    ]);
    expect(unsettled.waitedMs).toBeGreaterThanOrEqual(30);
    // The strict commit is reported unknown, with the same error: never acknowledged.
    expect(await outcomeWithin(publishing, 100)).toEqual({ error: unsettled });
    expect(state.aborts).toBe(1);
    expect(state.closes).toBe(1);
    const next = await outcomeWithin(granted, 1_000);
    expect(next).not.toBe("pending");
    // Repeated releases reject the same way.
    await expect(lease.release()).rejects.toBe(unsettled);

    // A successor owns the database on the same connection object while the old transaction is
    // still unsettled: its chain is not held by it.
    (next as { value: { release(): void } }).value.release();
    const successor = await backend.acquire();
    const own = await backend.open(successor);
    await own.publish(new Uint8Array([2]));
    expect(await own.readRoot()).toEqual(new Uint8Array([2]));
    // The late terminal event: nothing changes, nothing throws.
    for (const finish of state.finishers) finish();
    await tick(5);
    expect(await outcomeWithin(publishing, 100)).toEqual({ error: unsettled });
    expect(await own.readRoot()).toEqual(new Uint8Array([2]));
    // The old session refuses (it is fenced and holds the unknown outcome as its failure).
    await expect(session.publish(new Uint8Array([3]))).rejects.toThrow();
    expect(await successor.release()).toEqual({ drained: 0, aborted: 0 });
  });

  it("the reviewer's probe: queued work behind the hung transaction is refused, not unknown", async () => {
    const { state, database } = faultyConnection();
    const backend = createIndexedDbBackend({
      database: () => database,
      locks: new SimLocks(),
      name: "queued",
      releaseDrainMs: 5,
      releaseSettleMs: 30,
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    state.plans.push("withhold");
    const publishing = session.publish(new Uint8Array([1])).catch((error: unknown) => error);
    await tick();
    // The spill's first write loads its record index: a transaction queued behind the publish.
    const spilling = scratchOf(backend, lease)
      .write(0, new Uint8Array(8))
      .catch((error: unknown) => error);
    await tick();
    const release = await outcomeWithin(lease.release(), 2_000);
    expect(release).not.toBe("pending");
    const error = (release as { error?: unknown }).error;
    expect(error).toBeInstanceOf(ReleaseUnsettledError);
    const unsettled = error as ReleaseUnsettledError;
    expect([unsettled.drained, unsettled.aborted, unsettled.unsettled]).toEqual([0, 1, 1]);
    expect(unsettled.transactions).toEqual([
      { stores: ["roots", "meta"], mode: "readwrite", durability: "strict" },
    ]);
    // The queued spill was refused (known); the strict publish is unknown.
    const spilled = await outcomeWithin(spilling, 100);
    expect((spilled as { value?: unknown }).value).toBeInstanceOf(StoreClosedError);
    expect(await outcomeWithin(publishing, 100)).toEqual({ value: unsettled });
    // Only the created transaction was asked to abort.
    expect(state.aborts).toBe(1);
    for (const finish of state.finishers) finish();
  });

  it("counts aborted and unsettled transactions apart (unserialized)", async () => {
    const { state, database } = faultyConnection();
    const backend = createIndexedDbBackend({
      database: () => database,
      locks: new SimLocks(),
      name: "mixed",
      serializeTransactions: false,
      releaseDrainMs: 5,
      releaseSettleMs: 30,
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    state.plans.push("honour", "withhold");
    const outcomes = [
      session.publish(new Uint8Array([1])),
      scratchOf(backend, lease).write(0, new Uint8Array(8)),
    ].map((run) =>
      run.then(
        () => "settled",
        (error: unknown) => error,
      ),
    );
    await tick();
    const release = await outcomeWithin(lease.release(), 2_000);
    expect(release).not.toBe("pending");
    const error = (release as { error?: unknown }).error;
    expect(error).toBeInstanceOf(ReleaseUnsettledError);
    const unsettled = error as ReleaseUnsettledError;
    expect([unsettled.drained, unsettled.aborted, unsettled.unsettled]).toEqual([0, 1, 1]);
    // Each is mapped to its value or error; a "pending" one fails both checks.
    const [first, second] = await Promise.all(
      outcomes.map(async (run) => {
        const outcome = await outcomeWithin(run, 1_000);
        return outcome === "pending" ? outcome : (outcome as { value: unknown }).value;
      }),
    );
    expect(first).toBeInstanceOf(StoreClosedError);
    expect(second).toBe(unsettled);
  });

  it("a successor that stole while the transaction hung starts once the old lease gives up", async () => {
    const { state, database } = faultyConnection();
    const backend = createIndexedDbBackend({
      database: () => database,
      locks: new SimLocks(),
      name: "steal-hung",
      releaseDrainMs: 5,
      releaseSettleMs: 30,
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    state.plans.push("withhold");
    const publishing = session.publish(new Uint8Array([1])).catch((error: unknown) => error);
    await tick();
    // The steal's fence queues behind the hung transaction on the shared connection object.
    const stealing = backend.acquire({ steal: true, timeoutMs: 3_000 });
    await tick(10);
    const released = await outcomeWithin(lease.release(), 1_000);
    expect((released as { error?: unknown }).error).toBeInstanceOf(ReleaseUnsettledError);
    const stolen = await outcomeWithin(stealing, 1_000);
    expect(stolen).not.toBe("pending");
    const successor = (stolen as { value: OwnerLease }).value;
    const own = await backend.open(successor);
    await own.publish(new Uint8Array([2]));
    expect(await own.readRoot()).toEqual(new Uint8Array([2]));
    expect(await publishing).toBeInstanceOf(ReleaseUnsettledError);
    for (const finish of state.finishers) finish();
    await successor.release();
  });
});
