/** Ordinary native open/reopen, not a StorageCore/Harness/StandinEngine composition. */
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_BOOT_REACH } from "../../testing/reach.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { openIndexedDb } from "../storage/idb-like.js";
import { SimLocks } from "../storage/sim-disk.js";
import { ScratchSpillService } from "../spill/scratch-spill.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { MinnowDatabase } from "../owner/database.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { ProtocolClient } from "../owner/client.js";
import { openNativeOwner } from "../owner/native-boot.js";
(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;
for (const seed of seedsFor("next-public-boot", [0xb740, 0xb741]))
  it(`boots actual storage/executor/spill and reopens exact rows (${String(seed)})`, async () => {
    const reach = createReach("public native boot", PUBLIC_BOOT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.constantFrom(16, 32, 64),
        fc.bigInt({ min: -(1n << 60n), max: 1n << 60n }),
        fc.string({ maxLength: 32 }),
        async (idb, cap, value, text) => {
          reach.beginHistory();
          const factory = new IDBFactory();
          const backend = idb
            ? createIndexedDbBackend({
                name: "public-boot",
                locks: new SimLocks(),
                database: () => openIndexedDb(factory, "public-boot"),
              })
            : new MemoryBackend();
          const options = { backend, memoryLimitBytes: cap << 20 };
          if (cap === 16) {
            // Base WriteCredits' fixed owner minimum is 16,546,332 bytes, above the
            // 16 MiB cap's ordinary capacity. Public minimum/default belongs to B4b.
            await expect(MinnowDatabase.open(options)).rejects.toMatchObject({
              code: "MINNOW_MEMORY_ADMISSION",
              sqlState: "53200",
              label: "write owner minimum",
            });
            const lease = await backend.acquire();
            await lease.release();
            reach.reach("native fixed-minimum refusal releases fence");
            return;
          }
          const db = await MinnowDatabase.open(options);
          try {
            await db.execute("CREATE TABLE t(id INT PRIMARY KEY,n BIGINT,v TEXT)");
            await db.execute("INSERT INTO t VALUES(1,$1,$2)", [value, text]);
            expect((await db.query("SELECT n,v FROM t")).tuples()).toEqual([[value, text]]);
            await db.transaction(async (tx) => {
              await tx.execute("INSERT INTO t VALUES(2,$1,$2)", [value + 1n, text]);
            });
          } finally {
            await db.close();
          }
          // Same backend, new native owner, extent space, recovery and independent governor.
          const owner = await openNativeOwner(options),
            channel = new MessageChannel(),
            raw = new ProtocolClient();
          owner.accept(channel.port2);
          await raw.connect(channel.port1);
          const reopened = new MinnowDatabaseClient(raw, { ready: Promise.resolve() });
          try {
            expect(owner.spill).toBeInstanceOf(idb ? ScratchSpillService : MemorySpillService);
            expect((await reopened.query("SELECT n,v FROM t ORDER BY id")).tuples()).toEqual([
              [value, text],
              [value + 1n, text],
            ]);
            await reopened.execute("INSERT INTO t VALUES(3,$1,$2)", [value + 2n, text]);
            expect((await reopened.query("SELECT count(*) FROM t")).values(0)).toEqual([3]);
            expect(owner.governor.peakBytes).toBeLessThanOrEqual(cap << 20);
            reach.reach(idb ? "native IDB scratch service" : "memory backend spill exception");
            reach.reach("recovery and continued writes");
            reach.reach("exact native rows");
          } finally {
            await reopened.close();
            await owner.close();
            channel.port1.close();
            channel.port2.close();
            expect(owner.governor.usedBytes).toBe(0);
            expect(owner.spill.bytesInUse).toBe(0);
            reach.reach("all native charges closed");
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [false, 16, 9007199254740993n, "水"],
          [true, 16, -9007199254740993n, ""],
          [false, 32, 9007199254740993n, "水"],
          [true, 32, -9007199254740993n, ""],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
it.each([NaN, Infinity, -Infinity, 0, -1, 1.5])(
  "refuses invalid cap %s before acquisition",
  async (memoryLimitBytes) => {
    const backend = new MemoryBackend(),
      acquire = vi.spyOn(backend, "acquire");
    try {
      await expect(MinnowDatabase.open({ backend, memoryLimitBytes })).rejects.toThrow(
        "positive finite safe integer",
      );
      expect(acquire).not.toHaveBeenCalled();
    } finally {
      acquire.mockRestore();
    }
  },
);
it("honors an already-aborted open before acquiring a native fence", async () => {
  const backend = new MemoryBackend(),
    acquire = vi.spyOn(backend, "acquire");
  const abort = new AbortController(),
    reason = new Error("opening cancelled");
  abort.abort(reason);
  try {
    await expect(
      MinnowDatabase.open({ backend, memoryLimitBytes: 16 << 20, signal: abort.signal }),
    ).rejects.toBe(reason);
    expect(acquire).not.toHaveBeenCalled();
  } finally {
    acquire.mockRestore();
  }
});
it("captures the validated cap once before asynchronous acquisition", async () => {
  let reads = 0;
  const backend = new MemoryBackend();
  const db = await MinnowDatabase.open({
    backend,
    get memoryLimitBytes() {
      return ++reads === 1 ? 32 << 20 : Infinity;
    },
  });
  try {
    expect(reads).toBe(1);
    expect((await db.query("SELECT 1")).values(0)).toEqual([1]);
  } finally {
    await db.close();
  }
});
