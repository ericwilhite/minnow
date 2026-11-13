/** Governor rule 1 and StoreSession.appendLog: retained copies stay charged until settlement. */
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createIndexedDbBackend, IDB_BATCH_BYTES } from "../storage/indexeddb.js";
import type { IdbDatabaseLike, IdbStoreLike } from "../storage/idb-like.js";
import { SimDisk, SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";

const reach = createReach("log-queue-memory", ["copied", "borrowed", "backpressure", "settled"]);
for (const seed of seedsFor("log-queue-memory", [571, 572, 573, 574]))
  it(`accounts live queued copies independently (seed ${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed);
    const governor = new Governor(32 << 20);
    const caller = governor.registerCache({ evictableBytes: 0, evict: () => 0 });
    const memory = governor.registerCache({ evictableBytes: 0, evict: () => 0 });
    const copies = new Map<number, number>(); // observed input.slice results, not backend counters
    let callerBytes = 0;
    let independentPeak = 0;
    let copyPeak = 0;
    const deficits: number[] = [];
    const sample = () => {
      const live = [...copies.values()].reduce((a, b) => a + b, 0);
      copyPeak = Math.max(copyPeak, live);
      independentPeak = Math.max(independentPeak, callerBytes + live);
      if (governor.usedBytes < callerBytes + live)
        deficits.push(callerBytes + live - governor.usedBytes);
    };
    const inner = simIndexedDb(new SimDisk());
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    const atTransaction = new Promise<void>((r) => {
      entered = r;
    });
    let armed = true;
    const database: IdbDatabaseLike = {
      close: () => undefined,
      async transaction(stores, mode, durability, body) {
        const written = new Set<number>();
        await inner.transaction(stores, mode, durability, (tx) =>
          body({
            abort: (reason) => tx.abort(reason),
            store: (name) => {
              const store = tx.store(name);
              if (name !== "log") return store;
              return new Proxy(store, {
                get(target, property) {
                  if (property === "put")
                    return (key: Parameters<IdbStoreLike["put"]>[0], value: unknown) => {
                      const record = value as { bytes?: ArrayBuffer };
                      if (record.bytes !== undefined)
                        written.add(new Uint8Array(record.bytes)[0] ?? 0);
                      store.put(key, value);
                    };
                  const value = Reflect.get(target, property) as unknown;
                  return typeof value === "function" ? (value.bind(target) as unknown) : value;
                },
              });
            },
          }),
        );
        if (armed && mode === "readwrite" && stores.includes("log")) {
          armed = false;
          entered();
          await gate;
        }
        for (const id of written) copies.delete(id);
        sample();
      },
    };
    const backend = createIndexedDbBackend({
      database: () => database,
      name: `queue-${String(seed)}`,
      locks: new SimLocks(),
      memory,
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    const promises: Array<Promise<number>> = [];
    const lengths: number[] = [];
    let lastResolved = false;
    try {
      for (let id = 1; id <= 10; id++) {
        const length = id <= 6 ? 1 << 20 : (1 + Math.floor(random() * 16)) * 65536;
        lengths.push(length);
        expect(caller.charge(length)).toBe(true);
        callerBytes += length;
        const input = new Uint8Array(length).fill(id);
        input.slice = (start, end) => {
          const copy = Uint8Array.prototype.slice.call(input, start, end);
          // Full-input snapshots are retained queue copies. ByteStore's bounded
          // slice(start,end) records cross the native clone boundary, not this queue.
          if (start === undefined && end === undefined) {
            copies.set(id, copy.byteLength);
            reach.reach("copied");
            sample();
          }
          return copy;
        };
        const pending = session.appendLog(input).finally(() => {
          callerBytes -= length;
          caller.release(length);
          input.fill(255);
          sample();
          if (id === 10) lastResolved = true;
        });
        promises.push(pending);
      }
      await atTransaction;
      await Promise.resolve();
      expect(lastResolved, "pressure keeps the caller's buffer charged and borrowed").toBe(false);
      reach.reach("backpressure");
      if (copies.size < 10) reach.reach("borrowed");
      release();
      const offsets = await Promise.all(promises);
      await session.flushLog();
      let end = 0;
      for (let index = 0; index < lengths.length; index++) {
        expect(offsets[index]).toBe(end);
        expect((await session.readLog(end, 1))[0]).toBe(index + 1);
        end += lengths[index] ?? 0;
      }
      expect(copies.size).toBe(0);
      expect(deficits, "every independently live copy is charged before allocation").toEqual([]);
      expect(governor.peakBytes).toBeGreaterThanOrEqual(independentPeak);
      expect(copyPeak).toBeLessThanOrEqual(IDB_BATCH_BYTES);
      expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
      reach.reach("settled");
    } finally {
      release();
      await Promise.allSettled(promises);
      await session.close();
      await lease.release();
    }
    expect(governor.usedBytes).toBe(0);
  });
it("reaches every log ownership state", () => reach.assertReached());

it("keeps replaced tails charged through settlement and closes only after draining", async () => {
  const governor = new Governor(8 << 20);
  const memory = governor.registerCache({ evictableBytes: 0, evict: () => 0 });
  const inner = simIndexedDb(new SimDisk());
  let release!: () => void;
  let entered!: () => void;
  const gate = new Promise<void>((r) => {
    release = r;
  });
  const atRoot = new Promise<void>((r) => {
    entered = r;
  });
  const database: IdbDatabaseLike = {
    close: () => undefined,
    async transaction(stores, mode, durability, body) {
      const publication = { writes: 0 };
      await inner.transaction(stores, mode, durability, (tx) =>
        body({
          abort: (error) => tx.abort(error),
          store: (name) => {
            const store = tx.store(name);
            return {
              ...store,
              put: (key, value) => {
                if (name === "roots") publication.writes++;
                store.put(key, value);
              },
            };
          },
        }),
      );
      // Acquisition reads roots to validate the pre-fence version barrier; only hold a publication.
      if (publication.writes > 0) {
        entered();
        await gate;
      }
    },
  };
  const backend = createIndexedDbBackend({
    database: () => database,
    name: "tails",
    locks: new SimLocks(),
    memory,
  });
  const lease = await backend.acquire();
  const session = await backend.open(lease);
  const publication = session.publish(new Uint8Array([1]));
  await atRoot;
  try {
    for (let i = 0; i < 6; i++) await session.appendLog(new Uint8Array(50_000));
    // Each 50k append needs a different 64k tail. None of their transactions has run.
    expect(governor.usedBytes).toBe(6 * 65536);
    const closing = session.close();
    let closedAgain = false;
    const again = session.close().then(() => {
      closedAgain = true;
    });
    await expect(session.appendLog(new Uint8Array(1))).rejects.toThrow();
    await Promise.resolve();
    expect(closedAgain).toBe(false);
    expect(governor.usedBytes).toBe(6 * 65536);
    release();
    await publication;
    await closing;
    await again;
    expect(governor.usedBytes).toBe(0);
  } finally {
    release();
    await session.close();
    await lease.release();
  }
});

it("borrows without copying on refused admission and preserves truncate/append order", async () => {
  const memory = {
    charge: () => false,
    release: () => {
      throw new Error("uncharged release");
    },
  };
  const backend = createIndexedDbBackend({
    database: () => simIndexedDb(new SimDisk()),
    name: "refused",
    locks: new SimLocks(),
    memory,
  });
  const lease = await backend.acquire();
  const session = await backend.open(lease);
  try {
    const input = new Uint8Array(70_000).fill(3);
    // ByteStore uses bounded slice(start,end) for native records, which is permitted.
    input.slice = (start, end) => {
      if (start === undefined && end === undefined) throw new Error("input copy after refusal");
      return Uint8Array.prototype.slice.call(input, start, end);
    };
    const first = session.appendLog(input);
    const truncating = session.truncateLog(10);
    const last = session.appendLog(new Uint8Array(20).fill(7));
    expect(await first).toBe(0);
    await truncating;
    expect(await last).toBe(10);
    expect(await session.readLog(0, 30)).toEqual(
      new Uint8Array([...new Uint8Array(10).fill(3), ...new Uint8Array(20).fill(7)]),
    );
  } finally {
    await session.close();
    await lease.release();
  }
});

it("releases copied inputs after a failed transaction and skips later failed batches", async () => {
  const governor = new Governor(16 << 20);
  const memory = governor.registerCache({ evictableBytes: 0, evict: () => 0 });
  const inner = simIndexedDb(new SimDisk());
  let fail = false;
  const fault = new Error("injected log abort");
  const database: IdbDatabaseLike = {
    close: () => undefined,
    transaction(stores, mode, durability, body) {
      return inner.transaction(stores, mode, durability, (tx) => {
        if (fail && mode === "readwrite" && stores.includes("log")) tx.abort(fault);
        else body(tx);
      });
    },
  };
  const backend = createIndexedDbBackend({
    database: () => database,
    name: "failure",
    locks: new SimLocks(),
    memory,
  });
  const lease = await backend.acquire();
  const session = await backend.open(lease);
  try {
    fail = true;
    const appends = Array.from({ length: 7 }, () => session.appendLog(new Uint8Array(1 << 20)));
    const outcomes = Promise.allSettled(appends);
    expect(governor.usedBytes).toBe(IDB_BATCH_BYTES);
    await expect(session.flushLog()).rejects.toBe(fault);
    expect((await outcomes).some((outcome) => outcome.status === "rejected")).toBe(true);
    await expect(session.appendLog(new Uint8Array(100))).rejects.toBe(fault);
  } finally {
    await session.close();
    await lease.release();
  }
  expect(governor.usedBytes).toBe(0);
});
