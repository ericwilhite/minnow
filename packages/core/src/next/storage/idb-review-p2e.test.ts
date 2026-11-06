/**
 * Review B1-p2e (B1-p2e-review-1.md): the spill service's terminal close drains what is in flight
 * before it returns the index charge; long releases are split into bounded transactions with a
 * task between them, which a crash between chunks may leave half done without touching live
 * space; the log tail's buffer is charged. The reviewer's other probes are kept as tests.
 */
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { ScratchSpillService } from "../spill/scratch-spill.js";
import { ByteStore, inOrder } from "./idb-bytes.js";
import { openIndexedDb, type IdbDatabaseLike } from "./idb-like.js";
import { LOG_COALESCE_BYTES, RELEASE_CHUNK, createIndexedDbBackend } from "./indexeddb.js";
import { SimDisk, SimLocks } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";
import { openExtentSpace } from "./space.js";
import {
  bytesOf,
  countingBudget,
  countingMemory,
  expectBytes,
  jsonReachable,
  rootOf,
} from "./space-test-support.js";

(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

/** A registration that never goes negative. */
function holding() {
  const memory = {
    held: 0,
    charge(bytes: number) {
      memory.held += bytes;
      return true;
    },
    release(bytes: number) {
      memory.held -= bytes;
      expect(memory.held).toBeGreaterThanOrEqual(0);
    },
  };
  return memory;
}

const unbuffered = { allocate: () => false, free: (size: number) => void size };

async function spillOver(db: IdbDatabaseLike) {
  const backend = createIndexedDbBackend({
    database: () => db,
    name: "spill",
    locks: new SimLocks(),
  });
  const scratch = backend.scratch(await backend.acquire());
  if (scratch === null) throw new Error("IndexedDB has a spill store");
  const memory = holding();
  const service = new ScratchSpillService(scratch, { memory });
  await service.sweep();
  const records = async () => (await ByteStore.load(db, "spill")).records;
  return { scratch, memory, service, records };
}

const ticks = async (count: number) => {
  for (let at = 0; at < count; at++) await Promise.resolve();
};

describe("finding 1: the spill service's close drains before it disposes", () => {
  for (const [name, open] of [
    ["simulated IndexedDB", async () => simIndexedDb(new SimDisk())],
    ["fake-indexeddb", async () => openIndexedDb(new IDBFactory(), "close-race")],
  ] as const)
    it(`closing during an unbuffered append leaves nothing charged or written (${name}, the reviewer's probe)`, async () => {
      const { memory, service, records } = await spillOver(await open());
      const file = await service.create(1, unbuffered);
      const append = file.append(new Uint8Array(100));
      await Promise.resolve(); // the append is past its live check, suspended at its empty flush
      const closing = service.close();
      await Promise.allSettled([append, closing]);
      expect(memory.held).toBe(0);
      expect(service.bytesInUse).toBe(0);
      expect(await records()).toBe(0);
      await service.close(); // idempotent
    });

  it("closing during a buffered flush leaves nothing charged or written", async () => {
    const db = simIndexedDb(new SimDisk());
    const { memory, service, records } = await spillOver(db);
    const file = await service.create(1, countingBudget(1 << 20));
    for (let page = 0; page < 8; page++) await file.append(bytesOf(30_000, page));
    // The next page does not fit: the append flushes the batch first.
    const append = file.append(bytesOf(30_000, 9));
    await ticks(3); // inside the flush, waiting on the scratch write
    const closing = service.close();
    await Promise.allSettled([append, closing]);
    expect(memory.held).toBe(0);
    expect(service.bytesInUse).toBe(0);
    expect(await records()).toBe(0);
  });

  it("closing during a file's release waits for it: nothing charged or written", async () => {
    const db = simIndexedDb(new SimDisk());
    const { memory, service, records } = await spillOver(db);
    const file = await service.create(1, unbuffered);
    for (let page = 0; page < 20; page++) await file.append(bytesOf(5_000, page));
    expect(memory.held).toBeGreaterThan(0);
    const releasing = file.close();
    const closing = service.close();
    await Promise.all([releasing, closing]);
    expect(memory.held).toBe(0);
    expect(await records()).toBe(0);
  });

  it("refuses new files and new operations after close, and sweep stays reusable", async () => {
    const db = simIndexedDb(new SimDisk());
    const { memory, service } = await spillOver(db);
    const file = await service.create(1, unbuffered);
    await file.append(bytesOf(100, 1));
    await service.sweep(); // not terminal: the service goes on
    const again = await service.create(2, unbuffered);
    await again.append(bytesOf(100, 2));
    await service.close();
    await expect(again.append(bytesOf(100, 3))).rejects.toThrow(TypeError);
    await expect(service.create(3, unbuffered)).rejects.toThrow(TypeError);
    expect(memory.held).toBe(0);
  });
});

describe("finding 2: long releases are bounded transactions with a task between them", () => {
  it(`releases 12,000 ranges in transactions of at most ${String(RELEASE_CHUNK)}, letting other tasks run`, async () => {
    const inner = simIndexedDb(new SimDisk());
    let releases = 0;
    const db: IdbDatabaseLike = {
      transaction(stores, mode, durability, body) {
        if (mode === "readwrite" && stores.includes("data-index")) releases += 1;
        return inner.transaction(stores, mode, durability, body);
      },
      close: () => undefined,
    };
    const backend = createIndexedDbBackend({
      database: () => db,
      name: "chunks",
      locks: new SimLocks(),
    });
    const session = await backend.open(await backend.acquire());
    const ranges = [];
    for (let index = 0; index < 12_000; index++) {
      await session.writeData(index * 4096, new Uint8Array([1]));
      ranges.push({ offset: index * 4096, length: 4096 });
    }
    await session.publish(new Uint8Array([1]));
    releases = 0;
    let ranMeanwhile = false;
    setTimeout(() => {
      ranMeanwhile = true;
    }, 0);
    let resolved = false;
    const releasing = session.releaseData(ranges).then(() => {
      resolved = true;
    });
    // A task scheduled after the release started runs before it finishes.
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
    expect(ranMeanwhile).toBe(true);
    expect(resolved).toBe(false);
    await releasing;
    expect(releases).toBe(Math.ceil(12_000 / RELEASE_CHUNK));
    expect((await ByteStore.load(inner, "data")).records).toBe(0);
    await session.close();
  }, 120_000);

  it("a crash between chunks of a reclaim leaves the live root intact and the space reusable", async () => {
    const disk = new SimDisk();
    let armed = false;
    let chunk = 0;
    const db: IdbDatabaseLike = {
      transaction(stores, mode, durability, body) {
        const inner = simIndexedDb(disk);
        // The third release transaction dies with the process (the first two committed).
        if (
          armed &&
          mode === "readwrite" &&
          stores.includes("data-index") &&
          durability === "relaxed"
        ) {
          chunk += 1;
          if (chunk === 3) {
            armed = false;
            return inner.transaction(stores, mode, durability, (tx) => {
              body(tx);
              tx.abort(new Error("killed between chunks"));
            });
          }
        }
        return inner.transaction(stores, mode, durability, body);
      },
      close: () => undefined,
    };
    const backend = createIndexedDbBackend({
      database: () => db,
      name: "crash",
      locks: new SimLocks(),
    });
    const open = async () =>
      openExtentSpace(
        await backend.open(await backend.acquire({ steal: true, timeoutMs: 2_000 })),
        {
          reachable: jsonReachable,
          budget: countingBudget(Infinity),
          registerCache: () => countingMemory(Infinity),
        },
      );
    const space = await open();
    const own = { budget: countingBudget(Infinity) };
    const kept = space.beginWrite("kept", own);
    const live = (await kept.write([bytesOf(5_000, 1)])).blobs;
    const doomed = space.beginWrite("doomed", own);
    const extents = [];
    const doomedBlobs = [];
    for (let index = 0; index < 4 * RELEASE_CHUNK; index++) {
      const written = await doomed.write([bytesOf(1, index)]);
      extents.push(written.extent);
      doomedBlobs.push(...written.blobs);
    }
    await space.publish(rootOf([...live, ...doomedBlobs]), {
      writers: [kept, doomed],
      retired: [],
    });
    await space.publish(rootOf(live), { writers: [], retired: extents });
    armed = true;
    chunk = 0;
    await space.publish(rootOf(live), { writers: [], retired: [] }); // frees them: chunked release
    await (space as unknown as { settled(): Promise<void> }).settled();
    expect(space.failed?.message).toContain("killed between chunks");
    await space.close().catch(() => undefined);
    disk.powerLoss();
    const reopened = await open();
    const lease = await reopened.read(live, { budget: countingBudget(Infinity) });
    expectBytes(lease.bytes[0], bytesOf(5_000, 1), "live blob");
    lease.release();
    await reopened.audit();
    // The leftover records lie in free space: new writes reuse it.
    const writer = reopened.beginWrite("after", own);
    const after = [];
    for (let index = 0; index < 2 * RELEASE_CHUNK; index++)
      after.push(...(await writer.write([bytesOf(1, index)])).blobs);
    await reopened.publish(rootOf([...live, ...after]), { writers: [writer], retired: [] });
    await reopened.audit();
    expect(reopened.stats.dataBytes).toBeLessThanOrEqual((4 * RELEASE_CHUNK + 2) * 4096);
    await reopened.close();
  }, 120_000);
});

describe("finding 3: the log tail's buffer is charged", () => {
  it("charges 64 KiB while the session holds a tail, and returns it at truncate, reset and close", async () => {
    const memory = holding();
    const backend = createIndexedDbBackend({
      database: () => simIndexedDb(new SimDisk()),
      name: "tail",
      locks: new SimLocks(),
      memory,
    });
    const session = await backend.open(await backend.acquire());
    const indexes = memory.held;
    await session.appendLog(new Uint8Array(200));
    await session.appendLog(new Uint8Array(200));
    await session.flushLog();
    expect(memory.held - indexes).toBeGreaterThanOrEqual(LOG_COALESCE_BYTES);
    const withTail = memory.held;
    await session.truncateLog(100);
    expect(withTail - memory.held).toBeGreaterThanOrEqual(LOG_COALESCE_BYTES);
    await session.appendLog(new Uint8Array(10));
    await session.resetLog();
    await session.appendLog(new Uint8Array(10));
    await session.close();
    expect(memory.held).toBe(0);
  });
});

describe("the reviewer's other probes", () => {
  it("reopening at exactly 64 KiB, truncating inside and appending keeps every byte", async () => {
    const backend = createIndexedDbBackend({
      database: () => simIndexedDb(new SimDisk()),
      name: "exact",
      locks: new SimLocks(),
    });
    const lease = await backend.acquire();
    let session = await backend.open(lease);
    await session.appendLog(new Uint8Array(LOG_COALESCE_BYTES).fill(7));
    await session.flushLog();
    await session.close();
    session = await backend.open(lease);
    await session.appendLog(new Uint8Array(11).fill(8));
    await session.flushLog();
    await session.truncateLog(LOG_COALESCE_BYTES - 3);
    await session.appendLog(new Uint8Array(13).fill(9));
    await session.flushLog();
    await session.close();
    session = await backend.open(lease);
    const bytes = await session.readLog(0, session.logLength());
    expect(bytes.length).toBe(LOG_COALESCE_BYTES + 10);
    expect(bytes.findIndex((byte, at) => byte !== (at < LOG_COALESCE_BYTES - 3 ? 7 : 9))).toBe(-1);
    await session.close();
  });

  it("inOrder stops at a synchronous error and runs nothing after it", () => {
    const seen: number[] = [];
    expect(() => {
      inOrder(
        12_000,
        (index, done) => {
          seen.push(index);
          if (index === 2_000) throw new Error("stop");
          done();
        },
        () => {
          throw new Error("finished");
        },
      );
    }).toThrow("stop");
    expect(seen.length).toBe(2_001);
  });

  it("dispose is idempotent and a later write charges again", async () => {
    const memory = holding();
    const db = simIndexedDb(new SimDisk());
    const backend = createIndexedDbBackend({
      database: () => db,
      name: "dispose",
      locks: new SimLocks(),
    });
    const scratch = backend.scratch(await backend.acquire());
    if (scratch === null) throw new Error("IndexedDB has a spill store");
    scratch.chargeTo?.(memory);
    await scratch.write(0, new Uint8Array(1));
    expect(memory.held).toBeGreaterThan(0);
    await Promise.all([scratch.dispose?.(), scratch.dispose?.()]);
    expect(memory.held).toBe(0);
    await scratch.write(4096, new Uint8Array(1));
    expect(memory.held).toBeGreaterThan(0);
    expect((await ByteStore.load(db, "spill")).records).toBe(2);
    await scratch.dispose?.();
    expect(memory.held).toBe(0);
  });

  it("a failed truncation fails the next barrier, though a queued append got its offset", async () => {
    const inner = simIndexedDb(new SimDisk());
    let armed = false;
    const db: IdbDatabaseLike = {
      close: () => undefined,
      transaction(stores, mode, durability, body) {
        return inner.transaction(stores, mode, durability, (tx) => {
          body(tx);
          if (armed && mode === "readwrite" && stores.includes("log")) {
            armed = false;
            tx.abort(new Error("injected"));
          }
        });
      },
    };
    const backend = createIndexedDbBackend({
      database: () => db,
      name: "failure",
      locks: new SimLocks(),
    });
    const session = await backend.open(await backend.acquire());
    await session.appendLog(new Uint8Array(100));
    await session.flushLog();
    armed = true;
    const truncating = session.truncateLog(50);
    const appended = session.appendLog(new Uint8Array(10));
    expect(await appended).toBe(50);
    await expect(truncating).rejects.toThrow("injected");
    await expect(session.flushLog()).rejects.toThrow("injected");
    expect((await ByteStore.load(inner, "log")).end).toBe(100);
    await session.close().catch(() => undefined);
  });
});
