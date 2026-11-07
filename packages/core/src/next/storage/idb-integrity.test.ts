/**
 * Review B1-p2d findings 1–3 on IndexedDB byte spaces (idb-bytes.ts, indexeddb.ts): reads select
 * their records inside their own fenced transaction, the stored index is validated at open and
 * each record at read (corruption is refused, never read as zeros), and clears are iterative.
 * Finding 4 (the index's structure and charge, log append runs) is idb-index.test.ts.
 *
 * The reviewer's probes run over fake-indexeddb (the platform's semantics in Node: real request
 * scheduling, real key ranges); the bulk cases over the simulated IndexedDB.
 */
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { expectBytes, countingBudget, countingMemory } from "./space-test-support.js";
import { openExtentSpace } from "./space.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { OwnershipLostError, StoreCorruptionError } from "./contract.js";
import { ByteStore, IDB_RECORD_BYTES } from "./idb-bytes.js";
import { openIndexedDb, type IdbDatabaseLike, type IdbTransactionLike } from "./idb-like.js";
import { createIndexedDbBackend } from "./indexeddb.js";
import { SimDisk, SimLocks } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";

// idb-like.ts builds key ranges with the global, as in a browser.
(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

const SEEDS = seedsFor("next-idb-integrity", [0x2d1, 77]);

let serial = 0;
const fakeDb = (): Promise<IdbDatabaseLike> =>
  openIndexedDb(new IDBFactory(), `integrity-${String(serial++)}`);

const backendOver = (db: IdbDatabaseLike, locks = new SimLocks()) =>
  createIndexedDbBackend({ database: () => db, name: "db", locks, batching: false });

const filled = (length: number, byte: number) => new Uint8Array(length).fill(byte);

/** Writes raw records and index entries, bypassing the byte space (forged fixtures). */
function forge(
  db: IdbDatabaseLike,
  space: string,
  records: ReadonlyArray<readonly [number, number]>,
  entries: ReadonlyArray<readonly [number, unknown]>,
): Promise<void> {
  return db.transaction([space, `${space}-index`], "readwrite", "strict", (tx) => {
    for (const [offset, length] of records)
      tx.store(space).put(offset, { offset, bytes: filled(length, 7).buffer });
    for (const [key, entry] of entries) tx.store(`${space}-index`).put(key, entry);
  });
}

describe("reads select inside their transaction (finding 1)", () => {
  it("a byte space's read selects from the stored index in its own transaction, with no epoch get before it", async () => {
    // Two byte spaces over one database: `stale` keeps the index it loaded, `fresh` rearranges
    // the records. A read through `stale`, in a bare transaction (no epoch get, nothing else
    // delaying it), is created before `fresh`'s rearranging transaction commits and must still
    // answer from what is stored when it runs, never from its own in-memory index.
    const db = await fakeDb();
    const fresh = await ByteStore.load(db, "data");
    await db.transaction(["data", "data-index"], "readwrite", "strict", (tx) => {
      fresh.write(tx, 0, filled(8192, 1), () => undefined);
    });
    fresh.commit();
    const stale = await ByteStore.load(db, "data");
    // `fresh` splits the record (releases the first half) and rewrites the second half with 2s.
    const rearranging = db.transaction(["data", "data-index"], "readwrite", "strict", (tx) => {
      fresh.clear(tx, 0, 4096, () => {
        fresh.write(tx, 4096, filled(4096, 2), () => undefined);
      });
    });
    const target = new Uint8Array(8192).fill(5);
    const reading = db.transaction(["data", "data-index"], "readonly", "relaxed", (tx) => {
      stale.read(tx, 0, target, () => undefined);
    });
    await Promise.all([rearranging, reading]);
    fresh.commit();
    // IndexedDB runs the read after the read-write transaction created before it.
    expectBytes(target.subarray(0, 4096), new Uint8Array(4096), "released half");
    expectBytes(target.subarray(4096), filled(4096, 2), "rewritten half");
    db.close();
  });

  it("a read racing a release on the same session returns the bytes it asked for", async () => {
    const db = await fakeDb();
    const backend = backendOver(db);
    const session = await backend.open(await backend.acquire());
    await session.writeData(0, filled(8192, 1));
    await session.publish(new Uint8Array([1]));
    const releasing = session.releaseData([{ offset: 0, length: 4096 }]);
    const reading = session.readData([{ offset: 4096, length: 4 }]);
    const [, result] = await Promise.all([releasing, reading]);
    expect(result[0]).toEqual(filled(4, 1));
    await session.close();
    db.close();
  });

  it.each(SEEDS)(
    "reads of kept pages never see a concurrent release of others (seed %i)",
    async (seed) => {
      const random = mulberry32(seed);
      const db = await fakeDb();
      const backend = backendOver(db);
      const session = await backend.open(await backend.acquire());
      const PAGES = 48;
      // Records span several pages (and straddle the released ones), so releases split them.
      for (let page = 0; page < PAGES; page += 6)
        await session.writeData(
          page * 4096,
          Uint8Array.from({ length: 6 * 4096 }, (_, index) => (page * 4096 + index) % 251),
        );
      await session.publish(new Uint8Array([1]));
      const released = new Set<number>();
      const pending: Array<Promise<unknown>> = [];
      const checks: Array<Promise<void>> = [];
      for (let step = 0; step < 40; step++) {
        const page = Math.floor(random() * PAGES);
        if (random() < 0.4 && !released.has(page)) {
          released.add(page);
          pending.push(session.releaseData([{ offset: page * 4096, length: 4096 }]));
        } else if (!released.has(page)) {
          // Not released before this call, and never released later (the set only grows and
          // is consulted at check time below).
          const at = page * 4096 + Math.floor(random() * 4000);
          const read = random() < 0.5;
          const result = read
            ? session.readData([{ offset: at, length: 64 }]).then((parts) => parts[0])
            : (async () => {
                const target = new Uint8Array(64);
                await session.readDataInto(at, target);
                return target;
              })();
          checks.push(
            result.then((bytes) => {
              if (released.has(page)) return; // released after the read was issued: unspecified
              const expected = Uint8Array.from({ length: 64 }, (_, index) => (at + index) % 251);
              expect(bytes, `step ${String(step)} page ${String(page)}`).toEqual(expected);
            }),
          );
        }
      }
      await Promise.all([...pending, ...checks]);
      await session.close();
      db.close();
    },
  );

  it("a fenced owner's reads reject with OwnershipLostError instead of reading a rearranged store", async () => {
    const db = await fakeDb();
    const older = backendOver(db);
    const stale = await older.open(await older.acquire());
    await stale.writeData(0, filled(8192, 1));
    await stale.publish(new Uint8Array([1]));
    // A newer owner (another tab: its own lock view) splits the record by releasing half.
    const newer = backendOver(db);
    const fresh = await newer.open(await newer.acquire());
    await fresh.releaseData([{ offset: 0, length: 4096 }]);
    await expect(stale.readData([{ offset: 4096, length: 4 }])).rejects.toBeInstanceOf(
      OwnershipLostError,
    );
    await expect(stale.readDataInto(4096, new Uint8Array(4))).rejects.toBeInstanceOf(
      OwnershipLostError,
    );
    await expect(stale.readRoot()).rejects.toBeInstanceOf(OwnershipLostError);
    expect((await fresh.readData([{ offset: 4096, length: 4 }]))[0]).toEqual(filled(4, 1));
    await stale.close();
    await fresh.close();
    db.close();
  });

  it("gaps that no entry covers read as zeros", async () => {
    const db = await fakeDb();
    const backend = backendOver(db);
    const session = await backend.open(await backend.acquire());
    await session.writeData(0, filled(100, 1));
    await session.writeData(3 * 4096, filled(100, 2));
    const [hole] = await session.readData([{ offset: 4096, length: 8192 }]);
    expectBytes(hole, new Uint8Array(8192), "hole");
    const [edge] = await session.readData([{ offset: 50, length: 100 }]);
    expect(edge).toEqual(Uint8Array.from({ length: 100 }, (_, index) => (index < 50 ? 1 : 0)));
    await session.close();
    db.close();
  });
});

describe("the stored index is validated (finding 2)", () => {
  const entry = (offset: number, length: number) => ({ offset, length });
  const FIXTURES: ReadonlyArray<{
    readonly name: string;
    readonly records: ReadonlyArray<readonly [number, number]>;
    readonly entries: ReadonlyArray<readonly [number, unknown]>;
  }> = [
    { name: "an entry without its record", records: [], entries: [[0, entry(0, 4)]] },
    { name: "an orphan record", records: [[0, 4]], entries: [] },
    {
      name: "an orphan record between entries",
      records: [
        [0, 4],
        [8, 4],
        [16, 4],
      ],
      entries: [
        [0, entry(0, 4)],
        [16, entry(16, 4)],
      ],
    },
    {
      name: "overlapping entries",
      records: [
        [0, 4],
        [2, 2],
      ],
      entries: [
        [0, entry(0, 4)],
        [2, entry(2, 2)],
      ],
    },
    {
      name: "a key that is not the entry's offset",
      records: [[0, 4]],
      entries: [[0, entry(8, 4)]],
    },
    { name: "a zero length", records: [[0, 4]], entries: [[0, entry(0, 0)]] },
    {
      name: "a length over the record cap",
      records: [[0, 4]],
      entries: [[0, entry(0, IDB_RECORD_BYTES + 1)]],
    },
    { name: "a fractional length", records: [[0, 4]], entries: [[0, entry(0, 3.5)]] },
    {
      name: "an unsafe offset",
      records: [[2 ** 53, 4]],
      entries: [[2 ** 53, entry(2 ** 53, 4)]],
    },
    {
      name: "an end past the safe integers",
      records: [[2 ** 53 - 2, 4]],
      entries: [[2 ** 53 - 2, entry(2 ** 53 - 2, 4)]],
    },
    { name: "a string offset", records: [[0, 4]], entries: [[0, { offset: "0", length: 4 }]] },
    { name: "a non-object entry", records: [[0, 4]], entries: [[0, 4]] },
  ];

  it.each(FIXTURES)(
    "full load, audit and test-build open refuse $name with StoreCorruptionError",
    async (fixture) => {
      for (const space of ["data", "log"] as const) {
        const db = await fakeDb();
        await forge(db, space, fixture.records, fixture.entries);
        await expect(ByteStore.load(db, space)).rejects.toBeInstanceOf(StoreCorruptionError);
        await expect(ByteStore.audit(db, space)).rejects.toBeInstanceOf(StoreCorruptionError);
        const backend = backendOver(db);
        const lease = await backend.acquire();
        try {
          // Lazy open may already reject a malformed last-entry boundary. Otherwise the full
          // checks are mandatory in both explicit audit and the test-build open (R25.5).
          const opened = await backend.open(lease).then(
            (session) => ({ session, error: null }),
            (error: unknown) => ({ session: null, error }),
          );
          if (opened.session === null) expect(opened.error).toBeInstanceOf(StoreCorruptionError);
          else {
            expect(typeof opened.session.audit).toBe("function");
            await expect(opened.session.audit?.()).rejects.toBeInstanceOf(StoreCorruptionError);
            await expect(
              openExtentSpace(opened.session, {
                reachable: function* () {
                  yield* [];
                },
                verifyAtOpen: true,
                budget: countingBudget(Infinity),
                registerCache: () => countingMemory(Infinity),
              }),
            ).rejects.toBeInstanceOf(StoreCorruptionError);
            await opened.session.close();
          }
        } finally {
          await lease.release();
        }
        db.close();
      }
    },
  );

  it("open accepts a valid index with holes, and reads it back", async () => {
    const db = await fakeDb();
    await forge(
      db,
      "data",
      [
        [0, 4],
        [4096, 10],
      ],
      [
        [0, entry(0, 4)],
        [4096, entry(4096, 10)],
      ],
    );
    const backend = backendOver(db);
    const session = await backend.open(await backend.acquire());
    const [bytes] = await session.readData([{ offset: 0, length: 4106 }]);
    expect(bytes?.subarray(0, 4)).toEqual(filled(4, 7));
    expectBytes(bytes?.subarray(4, 4096), new Uint8Array(4092), "gap");
    expect(bytes?.subarray(4096)).toEqual(filled(10, 7));
    await session.close();
    db.close();
  });

  it("a read refuses a record shorter than its entry", async () => {
    const db = await fakeDb();
    // Keys agree (open passes), but the record holds 4 bytes where the entry says 8.
    await forge(db, "data", [[0, 4]], [[0, entry(0, 8)]]);
    const backend = backendOver(db);
    const session = await backend.open(await backend.acquire());
    await expect(session.readData([{ offset: 0, length: 8 }])).rejects.toBeInstanceOf(
      StoreCorruptionError,
    );
    await session.close();
    db.close();
  });

  it("a read refuses a record deleted behind the session's back, rather than reading zeros", async () => {
    const db = await fakeDb();
    const backend = backendOver(db);
    const session = await backend.open(await backend.acquire());
    await session.writeData(0, filled(4096, 3));
    await session.publish(new Uint8Array([1]));
    await db.transaction(["data"], "readwrite", "strict", (tx) => {
      tx.store("data").delete(0);
    });
    await expect(session.readData([{ offset: 0, length: 16 }])).rejects.toBeInstanceOf(
      StoreCorruptionError,
    );
    const target = new Uint8Array(16);
    await expect(session.readDataInto(0, target)).rejects.toBeInstanceOf(StoreCorruptionError);
    await session.close();
    db.close();
  });
});

describe("clears are iterative (finding 3)", () => {
  it("the reviewer's 12,000-record clear completes on fake-indexeddb", async () => {
    const db = await fakeDb();
    await forge(
      db,
      "data",
      Array.from({ length: 12_000 }, (_, index) => [index * 4096, 1] as const),
      Array.from({ length: 12_000 }, (_, index) => [index * 4096, entry(index * 4096)] as const),
    );
    const space = await ByteStore.load(db, "data");
    await db.transaction(["data", "data-index"], "readwrite", "strict", (tx) => {
      space.clear(tx, 0, Infinity, () => undefined);
    });
    space.commit();
    expect(space.records).toBe(0);
    expect((await ByteStore.load(db, "data")).records).toBe(0);
    db.close();
  }, 60_000);

  function entry(offset: number, length = 1) {
    return { offset, length };
  }

  it("clears 100,000 records in one transaction, trims straddlers, and undoes on abort", async () => {
    const COUNT = 100_000;
    const db = simIndexedDb(new SimDisk());
    await forge(
      db,
      "data",
      Array.from({ length: COUNT }, (_, index) => [index * 16, 8] as const),
      Array.from({ length: COUNT }, (_, index) => [index * 16, entry(index * 16, 8)] as const),
    );
    const space = await ByteStore.load(db, "data");
    const run = (body: (tx: IdbTransactionLike) => void) =>
      db.transaction(["data", "data-index"], "readwrite", "strict", body).then(
        () => {
          space.commit();
        },
        (error: unknown) => {
          space.rollback();
          throw error;
        },
      );
    // An aborted clear of everything leaves every entry in place.
    await expect(
      run((tx) => {
        space.clear(tx, 0, Infinity, () => {
          tx.abort(new Error("stop"));
        });
      }),
    ).rejects.toThrow("stop");
    expect(space.records).toBe(COUNT);
    // A clear from inside the first record to inside the last: two trims, the rest deleted.
    await run((tx) => {
      space.clear(tx, 4, (COUNT - 1) * 16 + 4, () => undefined);
    });
    expect(space.records).toBe(2);
    const reloaded = await ByteStore.load(db, "data");
    expect(reloaded.records).toBe(2);
    expect(reloaded.end).toBe((COUNT - 1) * 16 + 8);
    const kept = new Uint8Array((COUNT - 1) * 16 + 8);
    await db.transaction(["data", "data-index"], "readonly", "relaxed", (tx) => {
      reloaded.read(tx, 0, kept, () => undefined);
    });
    expect(kept.subarray(0, 4)).toEqual(filled(4, 7));
    expect(kept.subarray(4, -4).every((byte) => byte === 0)).toBe(true);
    expect(kept.subarray(-4)).toEqual(filled(4, 7));
  }, 120_000);
});
