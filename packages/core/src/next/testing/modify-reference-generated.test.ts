/** PG stored generated values are recomputed after assignments, including deferred IR. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { createReach, MODIFY_REFERENCE_GENERATED_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { oracleRows } from "./differential-adapter.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { StorageCore } from "./storage-core.js";

for (const seed of seedsFor("next-modify-reference-generated", [0xb5d5]))
  it(`recomputes deferred reference rows after UPDATE/upsert assignments seed ${String(seed)}`, async () => {
    const reach = createReach("deferred generated reference", MODIFY_REFERENCE_GENERATED_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 6 }),
        fc.integer({ min: 1, max: 20 }),
        fc.boolean(),
        async (n, delta, nullable) => {
          reach.beginHistory();
          const reference = new LoweredDatabase(),
            core = await new StorageCore().open(),
            shipped = new MinnowDatabase(new MemoryBlockStore()),
            pg = new PGlite();
          const ddl =
            "CREATE TABLE t(id INTEGER PRIMARY KEY,x INTEGER,g INTEGER GENERATED ALWAYS AS(x*2) STORED,h INTEGER GENERATED ALWAYS AS(x+3) STORED,CHECK(x>-10000))";
          try {
            const all = async (sql: string) => {
              const expected = (await pg.query(sql)).rows;
              expect(reference.execute(sql)).toEqual(expected);
              const native = await core.execute(sql),
                old = await shipped.execute(sql);
              if (native.kind === "rows" && "batches" in native)
                expect(oracleRows(native)).toEqual(expected);
              if (old.kind === "rows") expect(old.result.rows).toEqual(expected);
            };
            await all(ddl);
            const values = Array.from(
              { length: n },
              (_, i) => `(${String(i + 1)},${nullable && i === 0 ? "NULL" : String(i + 1)})`,
            ).join(",");
            await all(`INSERT INTO t(id,x) VALUES ${values} RETURNING *`);
            await all(`UPDATE t SET x=x+${String(delta)} RETURNING *`);
            reach.reach("UPDATE");
            await all(
              `INSERT INTO t(id,x) VALUES(2,${String(delta)}),(${String(n + 1)},7) ON CONFLICT(id) DO UPDATE SET x=t.x+EXCLUDED.x RETURNING *`,
            );
            reach.reach("SQL upsert update");
            reach.reach("upsert insert");
            const batch = [
              [1, nullable ? null : delta],
              [n + 2, delta + 1],
            ];
            reference.write({ table: "t", op: "upsert", names: ["id", "x"] }, batch);
            const sql = `INSERT INTO t(id,x) VALUES ${batch.map(([id, x]) => `(${String(id)},${x === null ? "NULL" : String(x)})`).join(",")} ON CONFLICT(id) DO UPDATE SET x=EXCLUDED.x`;
            await pg.exec(sql);
            await core.execute(sql);
            await shipped.execute(sql);
            reach.reach("batch upsert update");
            await all("SELECT * FROM t ORDER BY id");
            reach.reach("stored and RETURNING");
            reach.reach("multiple generated columns");
            reach.reach(nullable ? "NULL" : "non-NULL");
            expect(core.spill.bytesInUse).toBe(0);
          } finally {
            await core.close();
            await shipped.close();
            await pg.close();
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [2, 1, true],
          [3, 7, false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
