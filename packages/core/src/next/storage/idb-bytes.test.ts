import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { ByteStore, IDB_RECORD_BYTES } from "./idb-bytes.js";
import { SimDisk } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";

const SEEDS = seedsFor("next-idb-bytes", [0xb17e, 5]);

describe("IndexedDB byte space (records + in-memory index)", () => {
  it.each(SEEDS)(
    "reads back a byte image under random writes and clears (seed %i)",
    async (seed) => {
      const random = mulberry32(seed);
      const db = simIndexedDb(new SimDisk());
      const space = await ByteStore.load(db, "data");
      const image = new Uint8Array(2 << 20);
      let length = 0;
      const run = (body: Parameters<typeof db.transaction>[3]) =>
        db.transaction([space.name, space.indexName], "readwrite", "relaxed", body).then(
          () => space.commit(),
          (error: unknown) => {
            space.rollback();
            throw error;
          },
        );
      // The simulated disk copies its log image per write: keep the space at ~1 MiB.
      for (let step = 0; step < 100; step++) {
        const offset = Math.floor(random() * 200) * 4096 + (random() < 0.3 ? 1000 : 0);
        const size = 1 + Math.floor(random() * (random() < 0.08 ? 2 * IDB_RECORD_BYTES : 20_000));
        if (random() < 0.7) {
          const bytes = Uint8Array.from({ length: size }, (_, index) => (step * 7 + index) & 0xff);
          await run((tx) => {
            space.write(tx, offset, bytes.slice(), () => undefined);
          });
          image.set(bytes, offset);
          length = Math.max(length, offset + size);
        } else {
          await run((tx) => {
            space.clear(tx, offset, offset + size, () => undefined);
          });
          image.fill(0, offset, offset + size);
        }
        if (step % 10 === 9) {
          const target = new Uint8Array(space.end);
          await db.transaction([space.name, space.indexName], "readonly", "relaxed", (tx) => {
            space.read(tx, 0, target, () => undefined);
          });
          const expected = image.subarray(0, space.end);
          const differs = target.findIndex((byte, index) => byte !== expected[index]);
          expect(differs, `step ${String(step)}: first differing byte`).toBe(-1);
          expect(space.end).toBeLessThanOrEqual(length);
        }
      }
      // A fresh load of the stored index agrees with the one kept in memory.
      const reloaded = await ByteStore.load(db, "data");
      expect(reloaded.end).toBe(space.end);
      expect(reloaded.records).toBe(space.records);
    },
  );

  it("undoes its in-memory index when the transaction aborts", async () => {
    const db = simIndexedDb(new SimDisk());
    const space = await ByteStore.load(db, "data");
    await db.transaction([space.name, space.indexName], "readwrite", "relaxed", (tx) => {
      space.write(tx, 0, new Uint8Array(5000).fill(1), () => undefined);
    });
    space.commit();
    await expect(
      db.transaction([space.name, space.indexName], "readwrite", "relaxed", (tx) => {
        space.write(tx, 8192, new Uint8Array(100).fill(2), () => {
          tx.abort(new Error("stop"));
        });
      }),
    ).rejects.toThrow("stop");
    space.rollback();
    expect(space.end).toBe(5000);
    expect((await ByteStore.load(db, "data")).end).toBe(5000);
  });
});
