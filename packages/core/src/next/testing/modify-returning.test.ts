/** PostgreSQL RETURNING returns inserted/new UPDATE/deleted-old values after statement checks. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_RETURNING_REACH } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-modify-returning", [0xb5b5, 0xb5b6]))(
  "returns statement rows against PostgreSQL/shipped (seed %i)",
  async (seed) => {
    const core = await new StorageCore(64).open(),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    await pg.waitReady;
    const reach = createReach("native RETURNING", MODIFY_RETURNING_REACH);
    const all = async (sql: string, params: ReadonlyArray<string | number | null> = []) => {
      const actual = await core.execute(sql, params);
      const expected = await pg.query(sql, [...params]);
      const old = await shipped.run({ kind: "sql", sql, params });
      if ("returning" in actual) {
        const rows = oracleRows(actual.returning);
        expect(rows).toEqual(expected.rows);
        expect(rows).toEqual(old.rows);
        expect(core.spill.bytesInUse).toBe(0);
      }
      return actual;
    };
    try {
      await all("CREATE TABLE ret (id INTEGER PRIMARY KEY, v INTEGER, note TEXT)");
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 15 }),
          fc.integer({ min: 1, max: 8 }),
          fc.constantFrom("hello", "λ-text", ""),
          async (n, delta, note) => {
            reach.beginHistory();
            await all("DROP TABLE ret");
            await all("CREATE TABLE ret (id INTEGER PRIMARY KEY, v INTEGER, note TEXT)");
            await all(
              `INSERT INTO ret VALUES ${Array.from({ length: n }, (_, i) => `(${String(i + 1)},${String(i * 10)},'${note}')`).join(",")} RETURNING id,v,note,v+1 AS computed`,
            );
            reach.reach("inserted rows");
            await all(
              `UPDATE ret SET v = v + ${String(delta)} WHERE id % 2 = 0 RETURNING id,v,note,v*2 AS computed`,
            );
            reach.reach("new updated rows");
            await all(
              "UPDATE ret SET v=v+$1 WHERE id % 2=0 RETURNING id,v+$2::INTEGER AS bound,note",
              [delta, 3],
            );
            await all(
              "INSERT INTO ret VALUES ($1,$2,'parameter') RETURNING id,v+$3::INTEGER AS bound,note",
              [900 + delta, delta * 10, delta],
            );
            reach.reach("snapshotted parameters");
            await all("DELETE FROM ret WHERE id % 2 = 1 RETURNING id,v,note,v+2 AS computed");
            reach.reach("old deleted rows");
            const empty = await all("UPDATE ret SET v = v + 1 WHERE v < 0 RETURNING *");
            expect("returning" in empty ? empty.returning.batches.length : -1).toBe(0);
            reach.reach("empty");
            await all("INSERT INTO ret SELECT id+100,v+1000,note FROM ret RETURNING *");
            reach.reach("insert select");
          },
        ),
        { seed, numRuns: 12 },
      );
      reach.assertReached();
      await all("CREATE TABLE fixed_ret (id INTEGER PRIMARY KEY,v INTEGER)");
      await all("INSERT INTO fixed_ret VALUES (1,2),(2,3) RETURNING id,v+1 AS computed");
      await all("UPDATE fixed_ret SET v=v+1 RETURNING id,v");
      await all("DELETE FROM fixed_ret RETURNING id,v");
    } finally {
      await core.close();
      await pg.close();
      await shipped.close();
    }
  },
  60000,
);
