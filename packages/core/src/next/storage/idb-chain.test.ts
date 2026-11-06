/**
 * The IndexedDB backend's serialized chain (indexeddb.ts, `TrackedConnection`): by default every
 * transaction of a connection, from the data, log, spill and fence paths alike, waits for the
 * previous one to settle before it is created, so at most one is in flight (WebKit wedges
 * IndexedDB when a worker dies with two independent chains in flight: DECISIONS.md "Step-down").
 * A failed transaction does not stop the chain. `serializeTransactions: false` (benchmarks) lets
 * them overlap, which shows the probe can see an overlap.
 *
 * The chain belongs to the connection object, not to an acquisition (review B1-p2h-2 finding 1):
 * acquisitions, and backends, handed the same object share it, so a stealing acquisition's fence
 * waits for its predecessor's transaction. The reviewer's simulator and fake-indexeddb probes, and
 * seeded steal/release interleavings over two backends, check that.
 */
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { createReach, STORAGE_SHARED_CONNECTION_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import {
  OwnershipLostError,
  StoreClosedError,
  type OwnerLease,
  type StoreSession,
} from "./contract.js";
import { openIndexedDb, type IdbDatabaseLike } from "./idb-like.js";
import { createIndexedDbBackend } from "./indexeddb.js";
import { SimDisk, SimLocks } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";

// idb-like.ts builds key ranges with the global, as in a browser.
(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

const tick = (ms = 1) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

/**
 * A connection over the simulated IndexedDB whose transactions take a task before and after the
 * simulator runs them (as a platform transaction is in flight across tasks), counting how many
 * are in flight at once. While `failSpill` is set, the next transaction over the spill store fails
 * before it begins.
 */
function countingConnection() {
  const inner = simIndexedDb(new SimDisk());
  const probe = { live: 0, peak: 0, failSpill: false, failed: 0 };
  const database: IdbDatabaseLike = {
    transaction: async (stores, mode, durability, body) => {
      probe.live += 1;
      probe.peak = Math.max(probe.peak, probe.live);
      try {
        await tick();
        if (probe.failSpill && stores.includes("spill")) {
          probe.failSpill = false;
          probe.failed += 1;
          throw new Error("injected spill failure");
        }
        await inner.transaction(stores, mode, durability, body);
        await tick();
      } finally {
        probe.live -= 1;
      }
    },
    close: () => undefined,
  };
  return { probe, database };
}

/** Data, log, spill and root work issued at once, as an owner's lanes would. */
async function mixedWorkload(serializeTransactions: boolean) {
  const { probe, database } = countingConnection();
  const backend = createIndexedDbBackend({
    database: () => database,
    locks: new SimLocks(),
    name: "chain",
    serializeTransactions,
  });
  const lease = await backend.acquire();
  const session = await backend.open(lease);
  const scratch = backend.scratch(lease);
  if (scratch === null) throw new Error("no spill store");
  await scratch.write(0, new Uint8Array(4096).fill(1));
  probe.peak = 0;
  probe.failSpill = true;
  const settled = await Promise.allSettled([
    session.writeData(0, new Uint8Array(4096).fill(2)),
    session.appendLog(new Uint8Array(100).fill(3)).then(() => session.flushLog()),
    scratch.write(4096, new Uint8Array(4096).fill(4)),
    session.publish(new Uint8Array([5])),
    scratch.write(8192, new Uint8Array(4096).fill(6)),
    session.readRoot(),
  ]);
  const peak = probe.peak;
  // After the failure the chain still runs: everything else committed and reads back.
  const after = {
    root: await session.readRoot(),
    data: (await session.readData([{ offset: 0, length: 4096 }]))[0]?.[0],
    log: (await session.readLog(0, 100))[0],
  };
  await session.close();
  await lease.release();
  return { peak, failed: probe.failed, settled, after };
}

describe("every transaction of a connection runs through one serialized chain", () => {
  it("at most one transaction is in flight, and a failure does not stop the chain", async () => {
    const { peak, failed, settled, after } = await mixedWorkload(true);
    expect(peak).toBe(1);
    expect(failed).toBe(1);
    // Exactly the operation whose transaction failed rejects; the rest resolve.
    expect(settled.filter((outcome) => outcome.status === "rejected")).toHaveLength(1);
    expect(after).toEqual({ root: new Uint8Array([5]), data: 2, log: 3 });
  });

  it("without it, the same workload overlaps transactions (the probe sees overlap)", async () => {
    const { peak } = await mixedWorkload(false);
    expect(peak).toBeGreaterThan(1);
  });
});

describe("one chain per connection object, across acquisitions and backends (review B1-p2h-2 finding 1)", () => {
  it("the reviewer's simulator probe: a steal's fence waits for the predecessor's transaction", async () => {
    const inner = simIndexedDb(new SimDisk());
    let held = false;
    let live = 0;
    let peak = 0;
    let resume: () => void = () => undefined;
    const object: IdbDatabaseLike = {
      async transaction(stores, mode, durability, body) {
        live += 1;
        peak = Math.max(peak, live);
        try {
          await inner.transaction(stores, mode, durability, body);
          if (held) {
            held = false;
            await new Promise<void>((resolve) => {
              resume = resolve;
            });
          }
        } finally {
          live -= 1;
        }
      },
      close: () => undefined,
    };
    const locks = new SimLocks();
    // Two backends over the one object, as two owners in one process would be.
    const [first, second] = [0, 1].map(() =>
      createIndexedDbBackend({ database: () => object, locks, name: "shared" }),
    );
    if (first === undefined || second === undefined) throw new Error("backends");
    const a = await first.acquire();
    const session = await first.open(a);
    held = true;
    const publishing = session.publish(new Uint8Array([1]));
    await tick(0);
    const acquiring = second.acquire({ steal: true });
    await tick(10);
    const observed = peak;
    resume();
    await publishing;
    const b = await acquiring;
    await a.release();
    expect(await (await second.open(b)).readRoot()).toEqual(new Uint8Array([1]));
    await b.release();
    expect(observed).toBe(1);
  });

  it("the reviewer's fake-indexeddb probe: no second native transaction before the first ends", async () => {
    const factory = new IDBFactory();
    let native: IDBDatabase | null = null;
    const opening = {
      open(name: string, version?: number) {
        const request = factory.open(name, version);
        request.addEventListener("success", () => {
          native = request.result;
        });
        return request;
      },
    } as unknown as IDBFactory;
    const db = await openIndexedDb(opening, "native-shared");
    const backend = createIndexedDbBackend({
      database: () => db,
      locks: new SimLocks(),
      name: "native-shared",
    });
    const a = await backend.acquire();
    const session = await backend.open(a);
    const connection = native as IDBDatabase | null;
    if (connection === null) throw new Error("no native connection");
    const transaction = connection.transaction.bind(connection);
    let live = 0;
    let peak = 0;
    let spin = true;
    connection.transaction = (...args: Parameters<IDBDatabase["transaction"]>) => {
      const tx = transaction(...args);
      live += 1;
      peak = Math.max(peak, live);
      const end = () => {
        live -= 1;
      };
      tx.addEventListener("complete", end);
      tx.addEventListener("abort", end);
      // The first one stays alive (a request in each callback) until `spin` is cleared.
      if (live === 1) {
        const repeat = () => {
          if (spin) tx.objectStore("meta").get("epoch").onsuccess = repeat;
        };
        repeat();
      }
      return tx;
    };
    const publish = session.publish(new Uint8Array([1]));
    await tick(10);
    const taking = backend.acquire({ steal: true });
    await tick(20);
    const createdBeforeFirstEnded = peak;
    spin = false;
    await publish;
    const b = await taking;
    await a.release();
    await b.release();
    expect(createdBeforeFirstEnded).toBe(1);
  });

  const SEEDS = seedsFor("next-idb-shared-chain", [0x5c1, 0x5c2, 0x5c3, 0x5c4, 0x5c5, 0x5c6]);
  const reach = createReach("shared connection object", STORAGE_SHARED_CONNECTION_REACH, {
    minimum: 3,
  });

  it.each(SEEDS)(
    "seeded steals, releases and work from two backends over one object: one transaction at a time (seed %i)",
    { timeout: 60_000 },
    async (seed) => {
      const random = mulberry32(seed);
      reach.beginHistory();
      const { probe, database } = countingConnection();
      const locks = new SimLocks();
      const backends = [0, 1].map(() =>
        createIndexedDbBackend({
          database: () => database,
          locks,
          name: "generated",
          releaseDrainMs: 20,
        }),
      );
      const owners = new Set<number>();
      let acquiring = 0;
      const holders: Array<{ lease: OwnerLease; session: StoreSession | null }> = [];
      const work: Array<Promise<unknown>> = [];
      const unexpected: unknown[] = [];
      /** Fenced, released or closed work is expected to fail; nothing else may. */
      const track = (promise: Promise<unknown>) => {
        work.push(
          promise.catch((error: unknown) => {
            if (!(error instanceof OwnershipLostError || error instanceof StoreClosedError))
              unexpected.push(error);
          }),
        );
      };
      const pick = <T>(items: readonly T[]): T | undefined =>
        items[Math.floor(random() * items.length)];
      for (let step = 0; step < 40; step++) {
        const roll = random();
        if (acquiring === 0 && (roll < 0.2 || holders.length === 0)) {
          const index = random() < 0.5 ? 0 : 1;
          const backend = backends[index];
          if (backend === undefined) continue;
          if (probe.live > 0 && holders.length > 0) reach.reach("steal-while-another-in-flight");
          owners.add(index);
          if (owners.size === 2) reach.reach("both-backends-owned");
          acquiring += 1;
          track(
            backend
              .acquire({ steal: true })
              .finally(() => {
                acquiring -= 1;
              })
              .then(async (lease) => {
                const holder: { lease: OwnerLease; session: StoreSession | null } = {
                  lease,
                  session: null,
                };
                holders.push(holder);
                holder.session = await backend.open(lease);
              }),
          );
        } else if (roll < 0.65) {
          const session = pick(holders)?.session;
          if (session === undefined || session === null) continue;
          const kind = random();
          track(
            kind < 0.4
              ? session.publish(new Uint8Array([step]))
              : kind < 0.7
                ? session.writeData(4096 * step, new Uint8Array(4096).fill(step))
                : session.readRoot(),
          );
        } else if (roll < 0.75) {
          const holder = pick(holders);
          if (holder === undefined) continue;
          if (probe.live > 0) reach.reach("release-while-another-in-flight");
          track(holder.lease.release());
        } else await tick(Math.floor(random() * 3));
      }
      await Promise.all(work);
      await Promise.all(holders.map((holder) => holder.lease.release()));
      expect(unexpected).toEqual([]);
      expect(probe.peak).toBe(1);
      // The object still serves a new owner.
      const backend = backends[0];
      if (backend === undefined) throw new Error("backend");
      const lease = await backend.acquire({ steal: true });
      const session = await backend.open(lease);
      await session.publish(new Uint8Array([0xee]));
      expect(await session.readRoot()).toEqual(new Uint8Array([0xee]));
      await lease.release();
    },
  );

  it("the seeded interleavings reached steals and releases beside live transactions", () => {
    reach.assertReached();
  });
});
