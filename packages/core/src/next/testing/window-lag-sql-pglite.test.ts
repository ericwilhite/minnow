/** PG anycompatible lag/lead: typed, per-row offsets and lazily evaluated defaults. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, WINDOW_SHIFT_SQL_REACH } from "../../testing/reach.js";
import { batchesOf } from "../exec/join-fixtures.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
const code = (error: unknown): unknown => {
  if (typeof error === "object" && error !== null) {
    if ("sqlState" in error) return error.sqlState;
    if ("code" in error) return error.code;
  }
  throw error;
};
for (const seed of seedsFor("next-window-shift-sql", [0xb7a0, 0xb7a1]))
  it(`matches generated PostgreSQL shifts and common coercions (${String(seed)})`, async () => {
    const pg = new PGlite(),
      core = await new StorageCore().open();
    const reach = createReach(`window SQL shifts ${String(seed)}`, WINDOW_SHIFT_SQL_REACH);
    const random = mulberry32(seed);
    try {
      await pg.waitReady;
      const ddl =
        "CREATE TABLE w(id INTEGER PRIMARY KEY,p INTEGER,i INTEGER,b BIGINT,x DOUBLE PRECISION,t TEXT)";
      await pg.exec(ddl);
      await core.execute(ddl);
      const data = Array.from({ length: 24 }, (_, id) => ({
        id,
        p: id % 7 === 0 ? null : id % 3,
        i: id % 4 === 0 ? null : Math.floor(random() * 30) - 15,
        b: 9007199254740993n + BigInt(id),
        x: id % 4 === 0 ? null : (id - 8) / 4,
        t: id % 5 === 0 ? null : `v${String(id)}`,
      }));
      const table = core.harness.path.catalog.tableByName("w");
      if (table === undefined) throw Error("missing fixture table");
      for (const batch of batchesOf(
        table.columns.map((column) => column.type),
        data.map((row) => [row.id, row.p, row.i, row.b, row.x, row.t]),
        { int64: "int64" },
      ))
        await core.insertVectors(table, batch);
      for (const r of data)
        await pg.query("INSERT INTO w VALUES($1,$2,$3,$4,$5,$6)", [
          r.id,
          r.p,
          r.i,
          String(r.b),
          r.x,
          r.t,
        ]);
      const forms: Array<
        [(typeof WINDOW_SHIFT_SQL_REACH)[number], string, Array<number | null | string>?]
      > = [
        ["negative offset", "i,-2"],
        ["negative offset", "i,-2147483648,7"],
        ["negative offset", "i,2147483647,7"],
        ["unknown default", "i,'2','7'"],
        ["unknown default", "i,2::smallint,'7'"],
        ["numeric common type", "NULL,1,1.5"],
        ["numeric common type", "i,1,1.5::real"],
        ["unknown default", "'7',1,i"],
        ["text default", "NULL,1,'fallback'"],
        ["NULL offset", "i,NULL,100/(id-1)"],
        ["zero offset", "i,0"],
        ["NULL offset", "i,NULL,3"],
        ["row offset", "i,id%3-1,7"],
        ["row default", "i,99,id+10"],
        ["numeric common type", "i,1,1.5"],
        ["float common type", "i,1,x"],
        ["unknown default", "i,99,'7'"],
        ["NULL default", "i,99,NULL"],
        ["text default", "t,99,'fallback'"],
        ["int64 exact", "b,1,9007199254740997::bigint"],
        ["lazy default", "i,1,100/(id-1)"],
        ["parameter offset", "i,$1,$2", [-1, 7]],
        ["parameter offset", "i,$1,$2", [null, 7]],
        ["parameter default", "x,99,$1", [1.5]],
      ];
      for (const fn of ["lag", "lead"])
        for (const [dimension, args, params = []] of forms)
          for (const partition of ["", "PARTITION BY p "]) {
            reach.beginHistory();
            reach.reach(dimension);
            reach.reach(fn === "lag" ? "lag" : "lead");
            if (partition) reach.reach("partitions");
            // Row 1 is never out of range for lag(1), and lead's final default has a nonzero denominator.
            const call = `${fn}(${args}) OVER(${partition}ORDER BY id)`;
            const sql = `SELECT id,${call} v FROM w ORDER BY id`;
            let expected;
            try {
              expected = {
                rows: (
                  await pg.query<Record<string, unknown>>(
                    args.startsWith("b,") ? sql.replace(call, `CAST(${call} AS TEXT)`) : sql,
                    params,
                  )
                ).rows,
              };
            } catch (error) {
              expected = { error: code(error) };
            }
            let actual;
            try {
              const result = await core.execute(sql, params);
              if (!("batches" in result)) throw Error("query");
              actual = {
                rows: oracleRows(result).map((row) =>
                  args.startsWith("b,") ? { ...row, v: String(row.v) } : row,
                ),
              };
            } catch (error) {
              actual = { error: code(error) };
            }
            expect(actual, sql).toEqual(expected);
          }
      for (const fn of ["lag", "lead"]) {
        reach.reach("no ORDER BY");
        const sql = `SELECT id,${fn}(i,-1,7) OVER() v FROM w ORDER BY id`;
        const expected = (await pg.query(sql)).rows;
        const actual = await core.execute(sql);
        if (!("batches" in actual)) throw Error("query");
        expect(oracleRows(actual)).toEqual(expected);
      }
      for (const offset of [
        "1.5",
        "1.0",
        "-0.0",
        "1e0",
        "1::bigint",
        "1::float8",
        "TRUE",
        "1::text",
      ]) {
        reach.reach("invalid offset type");
        const sql = `SELECT lag(i,${offset}) OVER(ORDER BY id) FROM w`;
        await expect(pg.query(sql)).rejects.toMatchObject({ code: "42883" });
        await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: "42883" });
      }
      for (const fn of ["lag", "lead"])
        for (const offset of [-2, 1, 99])
          for (const tail of ["LIMIT 0", "LIMIT 1", "LIMIT 1 OFFSET 1", "LIMIT 24"]) {
            reach.reach("default demand");
            const sql = `SELECT ${fn}(i,${String(offset)},100/(id-1)) OVER(ORDER BY id ROWS 1 PRECEDING) v FROM w ${tail}`;
            let expected;
            try {
              expected = { rows: (await pg.query<Record<string, unknown>>(sql)).rows };
            } catch (error) {
              expected = { error: code(error) };
            }
            let actual;
            try {
              const result = await core.execute(sql);
              if (!("batches" in result)) throw Error("query");
              actual = { rows: oracleRows(result) };
            } catch (error) {
              actual = { error: code(error) };
            }
            expect(actual, sql).toEqual(expected);
          }
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 120000);
