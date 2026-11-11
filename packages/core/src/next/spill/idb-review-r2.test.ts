/**
 * Review B1-p2d re-review (B1-p2d-review-2.md) on the IndexedDB backend: release lists of any
 * length, the logical log end in call order, the spill index charged over the spill service's
 * lifetime, and log records bounded by log bytes rather than commits. The reviewer's probes are
 * here as tests (fake-indexeddb, the platform's semantics in Node), with the simulated IndexedDB
 * for bulk and crash cases.
 */
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { ScratchSpillService } from "./scratch-spill.js";
import { OwnershipLostError, type DataRange } from "../storage/contract.js";
import { ByteStore, IntervalIndex, type Interval } from "../storage/idb-bytes.js";
import { openIndexedDb, type IdbDatabaseLike } from "../storage/idb-like.js";
import { LOG_COALESCE_BYTES, createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimDisk, SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { openExtentSpace } from "../storage/space.js";
import {
  bytesOf,
  countingBudget,
  countingMemory,
  jsonReachable,
  rootOf,
} from "../storage/space-test-support.js";

(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

let serial = 0;
const fakeDb = (): Promise<IdbDatabaseLike> =>
  openIndexedDb(new IDBFactory(), `review-r2-${String(serial++)}`);
const filled = (length: number, byte: number) => new Uint8Array(length).fill(byte);

async function fakeSession(memory?: {
  charge(bytes: number): boolean;
  release(bytes: number): void;
}) {
  const db = await fakeDb();
  const backend = createIndexedDbBackend({
    database: () => db,
    name: `r2-${String(serial)}`,
    locks: new SimLocks(),
    ...(memory === undefined ? {} : { memory }),
  });
  const lease = await backend.acquire();
  const session = await backend.open(lease);
  return { db, backend, lease, session };
}

/** A database whose next read-write transaction over `store` aborts (once), when armed. */
function failingOnce(inner: IdbDatabaseLike, store: string) {
  const state = { armed: false };
  const db: IdbDatabaseLike = {
    transaction(stores, mode, durability, body) {
      if (state.armed && mode === "readwrite" && stores.includes(store)) {
        state.armed = false;
        return inner.transaction(stores, mode, durability, (tx) => {
          body(tx);
          tx.abort(new Error("injected abort"));
        });
      }
      return inner.transaction(stores, mode, durability, body);
    },
    close: () => {
      inner.close();
    },
  };
  return { db, state };
}

describe("finding 1: release lists of any length", () => {
  it("releases 12,000 ranges in one call (the reviewer's probe)", async () => {
    const { db, session } = await fakeSession();
    const ranges: DataRange[] = [];
    for (let index = 0; index < 12_000; index++) {
      await session.writeData(index * 4096, new Uint8Array([1]));
      ranges.push({ offset: index * 4096, length: 4096 });
    }
    await session.publish(new Uint8Array([1]));
    await session.releaseData(ranges);
    expect((await ByteStore.load(db, "data")).records).toBe(0);
    await session.close();
    db.close();
  }, 120_000);

  it("reclaims and aborts 12,000 extents through the extent space, and reopens after a failed release", async () => {
    const disk = new SimDisk();
    const failing = failingOnce(simIndexedDb(disk), "data");
    const backend = createIndexedDbBackend({
      database: () => failing.db,
      name: "space",
      locks: new SimLocks(),
    });
    const open = async () => {
      const session = await backend.open(await backend.acquire({ steal: true, timeoutMs: 2_000 }));
      const space = await openExtentSpace(session, {
        reachable: jsonReachable,
        budget: countingBudget(Infinity),
        registerCache: () => countingMemory(Infinity),
      });
      return space;
    };
    const space = await open();
    const settled = () => (space as unknown as { settled(): Promise<void> }).settled();
    // Writers with a budget stage each blob in its own buffer and write it alone at seal, so every
    // extent is exactly one record and each released range clears without a straddler (the
    // synchronous path a range list overflowed on).
    const own = { budget: countingBudget(Infinity) };
    // Abort: one writer holding 12,000 extents frees them in one release.
    const aborted = space.beginWrite("aborted", own);
    for (let index = 0; index < 12_000; index++) await aborted.write([bytesOf(1, index)]);
    await aborted.abort();
    await settled();
    expect(space.failed).toBeNull();
    expect(space.stats.dataBytes).toBe(0);
    // Reclaim: 12,000 published extents retired at once come back two publishes later.
    const writer = space.beginWrite("kept", own);
    const extents = [];
    const blobs = [];
    for (let index = 0; index < 12_000; index++) {
      const written = await writer.write([bytesOf(1, index)]);
      extents.push(written.extent);
      blobs.push(...written.blobs);
    }
    await space.publish(rootOf(blobs), { writers: [writer], retired: [] });
    await space.publish(rootOf([]), { writers: [], retired: extents });
    await space.publish(rootOf([]), { writers: [], retired: [] });
    await settled();
    expect(space.failed).toBeNull();
    await space.audit();
    // A release that fails (an aborted transaction) fails the space; a new owner opens cleanly
    // and the leaked space is free again after its walk.
    const again = space.beginWrite("again", own);
    const more = [];
    for (let index = 0; index < 3_000; index++)
      more.push((await again.write([bytesOf(1, index)])).extent);
    failing.state.armed = true;
    await again.abort();
    await settled();
    expect(space.failed).not.toBeNull();
    await space.close().catch(() => undefined);
    const reopened = await open();
    await reopened.audit();
    expect(reopened.failed).toBeNull();
    const after = reopened.beginWrite("after");
    await after.write([bytesOf(10, 1)]);
    await reopened.publish(rootOf([]), { writers: [after], retired: [] });
    await reopened.close();
  }, 300_000);

  it("closes a fragmented spill file (thousands of interleaved extents) and retries a failed release", async () => {
    const disk = new SimDisk();
    const failing = failingOnce(simIndexedDb(disk), "spill");
    const backend = createIndexedDbBackend({
      database: () => failing.db,
      name: "spill",
      locks: new SimLocks(),
    });
    const lease = await backend.acquire();
    const scratch = backend.scratch(lease);
    if (scratch === null) throw new Error("IndexedDB has a spill store");
    const service = new ScratchSpillService(scratch, { budget: countingBudget(Infinity) });
    await service.sweep();
    const unbuffered = { allocate: () => false, free: (size: number) => void size };
    const first = await service.create(1, unbuffered);
    const second = await service.create(2, unbuffered);
    // Alternating pages: every page of each file is its own extent.
    for (let index = 0; index < 8_000; index++) {
      await first.append(bytesOf(100, index));
      await second.append(bytesOf(100, index + 1));
    }
    const both = service.bytesInUse;
    await first.close(); // 8,000 ranges in one release
    expect(service.bytesInUse).toBe(both / 2);
    // A release that aborts can be retried: the records are still there, then they are gone.
    const ranges = Array.from({ length: 8_000 }, (_, index) => ({
      offset: (2 * index + 1) * 4096,
      length: 4096,
    }));
    failing.state.armed = true;
    await expect(scratch.release(ranges)).rejects.toThrow("injected abort");
    expect((await ByteStore.load(simIndexedDb(disk), "spill")).records).toBe(8_000);
    await scratch.release(ranges);
    expect((await ByteStore.load(simIndexedDb(disk), "spill")).records).toBe(0);
  }, 300_000);
});

describe("finding 2: the logical log end moves in call order", () => {
  it("truncate then append: the append lands at the new end (the reviewer's probe)", async () => {
    const { db, backend, lease, session } = await fakeSession();
    await session.appendLog(filled(100, 1));
    await session.flushLog();
    const truncating = session.truncateLog(50);
    const appending = session.appendLog(filled(10, 2));
    await truncating;
    expect(await appending).toBe(50);
    await session.flushLog();
    expect(session.logLength()).toBe(60);
    expect((await ByteStore.load(db, "log")).end).toBe(60);
    await session.close();
    const reopened = await backend.open(lease);
    expect(reopened.logLength()).toBe(60);
    expect(await reopened.readLog(0, 60)).toEqual(
      Uint8Array.from({ length: 60 }, (_, index) => (index < 50 ? 1 : 2)),
    );
    await reopened.close();
    db.close();
  });

  it("append then truncate: the truncate cuts the append too", async () => {
    const { db, session } = await fakeSession();
    await session.appendLog(filled(100, 1));
    const appending = session.appendLog(filled(10, 2));
    const truncating = session.truncateLog(50);
    expect(await appending).toBe(100);
    await truncating;
    await session.flushLog();
    expect(session.logLength()).toBe(50);
    expect((await ByteStore.load(db, "log")).end).toBe(50);
    await session.close();
    db.close();
  });

  it("reset then append, and append then reset", async () => {
    const { db, session } = await fakeSession();
    await session.appendLog(filled(100, 1));
    await session.flushLog();
    const resetting = session.resetLog();
    const appending = session.appendLog(filled(10, 2));
    await resetting;
    expect(await appending).toBe(0);
    await session.flushLog();
    expect(session.logLength()).toBe(10);
    expect(await session.readLog(0, 10)).toEqual(filled(10, 2));
    const late = session.appendLog(filled(5, 3));
    const again = session.resetLog();
    expect(await late).toBe(10);
    await again;
    await session.flushLog();
    expect(session.logLength()).toBe(0);
    expect((await ByteStore.load(db, "log")).end).toBe(0);
    await session.close();
    db.close();
  });

  it("a truncate or reset that fails fails the session", async () => {
    for (const operation of ["truncate", "reset"] as const) {
      const db = await fakeDb();
      const make = () =>
        createIndexedDbBackend({ database: () => db, name: "fail", locks: new SimLocks() });
      const older = make();
      const session = await older.open(await older.acquire());
      await session.appendLog(filled(100, 1));
      await session.flushLog();
      // A newer owner moves the epoch: the truncate's (or reset's) transaction aborts.
      const newer = make();
      await newer.acquire();
      const failing = operation === "truncate" ? session.truncateLog(50) : session.resetLog();
      const appending = session.appendLog(filled(10, 2));
      await expect(failing).rejects.toBeInstanceOf(OwnershipLostError);
      await appending.catch(() => undefined);
      await expect(session.appendLog(filled(1, 1))).rejects.toBeInstanceOf(OwnershipLostError);
      db.close();
    }
  });

  it("truncating inside a shared record, then appending, survives a reopen (the reviewer's probe)", async () => {
    const { db, backend, lease, session } = await fakeSession();
    for (let index = 0; index < 400; index++) await session.appendLog(filled(200, index % 256));
    await session.flushLog();
    await session.truncateLog(33_333);
    await session.appendLog(filled(17, 254));
    await session.flushLog();
    await session.close();
    const fresh = await backend.open(lease);
    expect(fresh.logLength()).toBe(33_350);
    const bytes = await fresh.readLog(0, 33_350);
    const differs = bytes
      .subarray(0, 33_333)
      .findIndex((byte, index) => byte !== Math.floor(index / 200) % 256);
    expect(differs).toBe(-1);
    expect(bytes.subarray(33_333)).toEqual(filled(17, 254));
    await fresh.close();
    db.close();
  });
});

describe("finding 3: scratch never keeps a resident record directory", () => {
  it("keeps the same 3,000-record workloads index-free through release, sweep and close", async () => {
    let used = 0;
    const memory = {
      charge(bytes: number) {
        used += bytes;
        return true;
      },
      release(bytes: number) {
        used -= bytes;
        expect(used).toBeGreaterThanOrEqual(0);
      },
    };
    const backend = createIndexedDbBackend({
      database: () => simIndexedDb(new SimDisk()),
      name: "spill-charge",
      locks: new SimLocks(),
    });
    const scratch = backend.scratch(await backend.acquire());
    if (scratch === null) throw new Error("IndexedDB has a spill store");
    const service = new ScratchSpillService(scratch, { memory, budget: countingBudget(Infinity) });
    expect(() => scratch.chargeTo?.(memory)).not.toThrow(); // before first use: allowed
    await service.sweep();
    expect(used).toBe(0);
    const unbuffered = { allocate: () => false, free: (size: number) => void size };
    const spill = async (query: number) => {
      const first = await service.create(query, unbuffered);
      const second = await service.create(query + 1, unbuffered);
      for (let index = 0; index < 1_500; index++) {
        await first.append(bytesOf(100, index));
        await second.append(bytesOf(100, index));
      }
      return [first, second] as const;
    };
    const [first, second] = await spill(1);
    // Native predecessor lookups replace the resident index, including when registration refuses.
    expect(used).toBe(0);
    await first.close();
    expect(used).toBe(0);
    await second.close();
    expect(used).toBe(0);
    await spill(10);
    expect(used).toBe(0);
    await service.sweep();
    expect(used).toBe(0);
    await spill(20);
    expect(used).toBe(0);
    await service.close();
    expect(used).toBe(0);
  }, 300_000);

  it("keeps working without an index when the registration refuses", async () => {
    const memory = { charge: () => false, release: () => undefined };
    const backend = createIndexedDbBackend({
      database: () => simIndexedDb(new SimDisk()),
      name: "spill-refused",
      locks: new SimLocks(),
    });
    const scratch = backend.scratch(await backend.acquire());
    if (scratch === null) throw new Error("IndexedDB has a spill store");
    const service = new ScratchSpillService(scratch, { memory, budget: countingBudget(Infinity) });
    await service.sweep();
    const file = await service.create(1, {
      allocate: () => false,
      free: (size: number) => void size,
    });
    const refs = [];
    for (let index = 0; index < 200; index++) refs.push(await file.append(bytesOf(100, index)));
    for (const [index, ref] of refs.entries())
      expect(await file.read(ref)).toEqual(bytesOf(100, index));
    await service.close();
  });

  it("refuses a registration after first use (the index is not re-homed)", async () => {
    const backend = createIndexedDbBackend({
      database: () => simIndexedDb(new SimDisk()),
      name: "spill-late",
      locks: new SimLocks(),
    });
    const scratch = backend.scratch(await backend.acquire());
    if (scratch === null) throw new Error("IndexedDB has a spill store");
    await scratch.write(0, new Uint8Array([1]));
    expect(() => scratch.chargeTo?.(countingMemory(Infinity))).toThrow(TypeError);
  });
});

describe("finding 4: log records scale with log bytes, not commits", () => {
  it("700 small flushed commits make about log bytes / 64 KiB records", async () => {
    const disk = new SimDisk();
    const backend = createIndexedDbBackend({
      database: () => simIndexedDb(disk),
      name: "log",
      locks: new SimLocks(),
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    for (let index = 0; index < 700; index++) {
      await session.appendLog(filled(200, index & 0xff));
      await session.flushLog();
    }
    const log = await ByteStore.load(simIndexedDb(disk), "log");
    expect(log.records).toBe(Math.ceil((700 * 200) / LOG_COALESCE_BYTES));
    const bytes = await session.readLog(0, 140_000);
    expect(bytes.findIndex((byte, index) => byte !== (Math.floor(index / 200) & 0xff))).toBe(-1);
    await session.close();
    const reopened = await backend.open(lease);
    expect(reopened.logLength()).toBe(140_000);
    await reopened.close();
  }, 120_000);

  it.each(seedsFor("next-idb-review-r2-crash", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]))(
    "power loss mid-rewrite keeps the previous complete tail (seed %i)",
    async (seed) => {
      const disk = new SimDisk({ seed });
      const make = () =>
        createIndexedDbBackend({
          database: () => simIndexedDb(disk),
          name: "crash",
          locks: new SimLocks(),
        });
      const backend = make();
      const session = await backend.open(await backend.acquire());
      // A flushed 20,000-byte tail, then unflushed rewrites that extend it (relaxed commits).
      for (let index = 0; index < 100; index++) await session.appendLog(filled(200, index));
      await session.flushLog();
      for (let index = 0; index < 10; index++) {
        await session.appendLog(filled(200, 201));
        await session.readLog(0, 1); // commits the queued rewrite (relaxed, not flushed)
      }
      disk.powerLoss();
      const reopened = make();
      const fresh = await reopened.open(await reopened.acquire());
      const length = fresh.logLength();
      expect(length).toBeGreaterThanOrEqual(20_000);
      expect((length - 20_000) % 200).toBe(0);
      const bytes = await fresh.readLog(0, length);
      const differs = bytes.findIndex(
        (byte, index) => byte !== (index < 20_000 ? Math.floor(index / 200) : 201),
      );
      expect(differs).toBe(-1);
      await fresh.close();
    },
  );
});

describe("the in-memory index (reviewer's extra probes)", () => {
  it("retries a refused charge per mutation and never releases more than it holds", () => {
    let attempts = 0;
    let held = 0;
    let allow = false;
    const index = new IntervalIndex({
      charge(bytes) {
        attempts += 1;
        if (!allow) return false;
        held += bytes;
        return true;
      },
      release(bytes) {
        held -= bytes;
        expect(held).toBeGreaterThanOrEqual(0);
      },
    });
    for (let at = 0; at < 10_000; at++) index.insert(at * 2, at * 2 + 1);
    expect(attempts).toBe(10_000);
    expect(index.charge.charged).toBe(0);
    allow = true;
    index.removeStarts(0, 0);
    expect(held).toBe(index.charge.charged);
    expect(index.charge.overdraft).toBe(0);
    index.release();
    index.release();
    expect(held).toBe(0);
  });

  it.each(seedsFor("next-idb-review-r2-index", [512, 513, 1024, 431]))(
    "matches a model across block-split boundaries (seed %i)",
    (seed) => {
      const random = mulberry32(seed);
      const index = new IntervalIndex();
      const model = new Map<number, number>();
      const sorted = (): Interval[] =>
        [...model].sort(([a], [b]) => a - b).map(([start, end]) => ({ start, end }));
      for (let at = 0; at < 1537; at++) {
        index.insert(at * 4, at * 4 + 3);
        model.set(at * 4, at * 4 + 3);
      }
      for (let step = 0; step < 4000; step++) {
        const at = Math.floor(random() * 2048) * 4;
        if (random() < 0.6) {
          if (!model.has(at)) {
            index.insert(at, at + 3);
            model.set(at, at + 3);
          }
        } else {
          const last = at + Math.floor(random() * 513) * 4;
          const expected = sorted().filter(({ start }) => start >= at && start <= last);
          expect(index.removeStarts(at, last)).toEqual(expected);
          for (const { start } of expected) model.delete(start);
        }
        index.check();
        expect(index.count).toBe(model.size);
        if (step % 100 === 0) expect(index.overlapping(0, Infinity)).toEqual(sorted());
      }
    },
  );

  it("a failed open returns the data index's charge", async () => {
    const db = simIndexedDb(new SimDisk());
    let held = 0;
    const backend = createIndexedDbBackend({
      database: () => db,
      name: "leak",
      locks: new SimLocks(),
      memory: {
        charge(bytes) {
          held += bytes;
          return true;
        },
        release(bytes) {
          held -= bytes;
        },
      },
    });
    const lease = await backend.acquire();
    await db.transaction(["data", "data-index", "log-index"], "readwrite", "strict", (tx) => {
      tx.store("data").put(0, { offset: 0, bytes: new ArrayBuffer(1) });
      tx.store("data-index").put(0, { offset: 0, length: 1 });
      tx.store("log-index").put(0, { offset: 0, length: -1 });
    });
    await expect(backend.open(lease)).rejects.toThrow();
    expect(held).toBe(0);
  });

  it("reads racing releases and publishes keep retained bytes (the reviewer's probe)", async () => {
    const { db, session } = await fakeSession();
    for (let round = 0; round < 30; round++) {
      await session.writeData(0, filled(8192, round + 1));
      await session.publish(new Uint8Array([1]));
      const results = await Promise.all([
        session.readData([{ offset: 4096, length: 4 }]),
        session.releaseData([{ offset: 0, length: 4096 }]),
        session.readData([{ offset: 4096, length: 4 }]),
        session.publish(new Uint8Array([2])),
      ]);
      expect(results[0][0]).toEqual(filled(4, round + 1));
      expect(results[2][0]).toEqual(filled(4, round + 1));
    }
    await session.close();
    db.close();
  }, 60_000);
});
