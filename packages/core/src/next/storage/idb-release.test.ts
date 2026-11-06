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
 */
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import {
  OwnershipLostError,
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
