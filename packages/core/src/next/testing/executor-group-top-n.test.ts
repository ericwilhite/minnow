/** SQL ORDER BY/LIMIT over aggregate state, independently judged by PostgreSQL/current. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

it.each(seedsFor("next-executor-group-top-n", [0xe235, 0xe236]))(
  "selects only safe aggregate winners (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite(),
      current = await currentEngine("memory");
    const reach = createReach("aggregate output top-N", [
      "memory",
      "stored",
      "empty",
      "zero",
      "offset",
      "key-rank",
      "count-rank",
      "ascending",
      "descending",
      "nulls-first",
      "nulls-last",
      "reduced",
      "parameter-fallback",
      "finalizer-fallback",
      "all-input",
      "bound-fallback",
    ]);
    const rows = Array.from({ length: 4097 }, (_, id) => ({
      g: id % 31 === 0 ? null : ((id * 17) % 3001) - 1500,
      v: (id % 13) * 0.125,
    }));
    await pg.waitReady;
    try {
      for (const name of ["s", "m", "e"]) {
        const sql = `CREATE TABLE ${name}(g INTEGER,v DOUBLE PRECISION)`;
        await core.execute(sql);
        await pg.exec(sql);
        await current.run({ kind: "sql", sql });
        if (name === "e") continue;
        await current.run({ kind: "insertBatch", table: name, rows });
        for (let from = 0; from < rows.length; from += 128) {
          const part = rows.slice(from, from + 128);
          await pg.query(
            `INSERT INTO ${name} VALUES ${part.map((_, k) => `($${String(2 * k + 1)},$${String(2 * k + 2)})`).join(",")}`,
            part.flatMap((r) => [r.g, r.v]),
          );
        }
        await core.writeBatch("insertBatch", name, rows);
        if (name === "s") {
          await core.harness.path.recycleLog();
          await core.harness.path.quiesce();
        }
      }
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 2 }),
          fc.oneof(fc.integer({ min: 0, max: 64 }), fc.constantFrom(2048, 2049)),
          fc.integer({ min: 0, max: 64 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 0, max: 2 }),
          async (state, limit, offset, desc, nullsFirst, keyOnly, mode) => {
            reach.beginHistory();
            const name = ["m", "s", "e"][state];
            if (!name) throw new Error("table state");
            const sql = `SELECT g,COUNT(*) AS n,SUM(v) AS s${mode === 2 ? ",AVG(v) AS a" : ""} FROM ${name} GROUP BY g ORDER BY ${keyOnly ? "" : `n ${desc ? "DESC" : "ASC"},`}g ${desc ? "DESC" : "ASC"} NULLS ${nullsFirst ? "FIRST" : "LAST"} ${mode === 1 ? "LIMIT $1 OFFSET $2" : `LIMIT ${String(limit)} OFFSET ${String(offset)}`}`;
            const params = mode === 1 ? [limit, offset] : [];
            const expected = (await pg.query<Record<string, unknown>>(sql, params)).rows;
            const old = await current.run({ kind: "sql", sql, params });
            expect(old.kind).toBe("rows");
            expect(old.rows).toEqual(expected);
            const result = await core.execute(sql, params);
            if (!("batches" in result)) throw new Error("query rows");
            expect(oracleRows(result)).toEqual(expected);
            const aggregate = core.lastProfile?.operators.find((o) => o.op === "aggregate");
            const scan = core.lastProfile?.operators.find((o) => o.op === "scan");
            if (limit === 0) {
              expect(aggregate).toBeUndefined();
              reach.reach("zero");
            } else if (state === 2) {
              expect(aggregate?.rowsOut).toBe(0);
              reach.reach("empty");
            } else {
              expect(scan?.rowsOut).toBe(rows.length);
              reach.reach("all-input");
              if (mode === 0 && limit + offset <= 2048) {
                expect(aggregate?.rowsOut, "only winning aggregate groups reach the sorter").toBe(
                  limit + offset,
                );
                reach.reach("reduced");
              } else {
                expect(aggregate?.rowsOut).toBeGreaterThan(limit + offset);
                reach.reach(
                  mode === 0
                    ? "bound-fallback"
                    : mode === 1
                      ? "parameter-fallback"
                      : "finalizer-fallback",
                );
              }
            }
            if (state === 0) reach.reach("memory");
            if (state === 1) reach.reach("stored");
            if (offset > 0) reach.reach("offset");
            reach.reach(keyOnly ? "key-rank" : "count-rank");
            reach.reach(desc ? "descending" : "ascending");
            reach.reach(nullsFirst ? "nulls-first" : "nulls-last");
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            [0, 32, 17, true, true, false, 0],
            [0, 2048, 0, false, true, true, 0],
            [1, 2049, 0, true, false, false, 0],
            [1, 2048, 1, false, false, true, 0],
            [0, 32, 0, false, true, true, 0],
            [1, 32, 17, false, false, true, 0],
            [2, 32, 0, false, true, true, 0],
            [0, 0, 17, true, true, false, 0],
            [0, 32, 0, true, false, false, 1],
            [1, 32, 0, true, false, false, 2],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await current.close();
      await pg.close();
      await core.close();
      expect(core.harness.governor.usedBytes).toBe(0);
      expect(core.spill.bytesInUse).toBe(0);
    }
  },
  120000,
);
