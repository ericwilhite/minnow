/** A matching partition/order prefix can feed LIMIT without demanding later frames. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, WINDOW_PARTITION_DEMAND_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
const code = (error: unknown): unknown => {
  if (typeof error === "object" && error !== null) {
    if ("sqlState" in error) return error.sqlState;
    if ("code" in error) return error.code;
  }
  throw error;
};
for (const seed of seedsFor("next-window-partition-demand", [0xb7a8, 0xb7a9]))
  it(`matches PG partitioned ordering and partial frame demand (${String(seed)})`, async () => {
    const pg = new PGlite(),
      core = await new StorageCore().open();
    const random = mulberry32(seed),
      reach = createReach(`partition demand ${String(seed)}`, WINDOW_PARTITION_DEMAND_REACH);
    try {
      await pg.waitReady;
      const ddl =
        "CREATE TABLE w(id INTEGER PRIMARY KEY,p INTEGER,x DOUBLE PRECISION,v DOUBLE PRECISION)";
      await pg.exec(ddl);
      await core.execute(ddl);
      const rows = Array.from({ length: 12 + Math.floor(random() * 4) }, (_, id) => ({
        id,
        p: id % 5 === 0 ? null : (id + 1) % 3,
        x: 1e308,
        v: (id % 7) / 4,
      }));
      await core.writeBatch("insertBatch", "w", [...rows].reverse());
      for (const r of rows)
        await pg.query("INSERT INTO w VALUES($1,$2,$3,$4)", [r.id, r.p, r.x, r.v]);
      const compare = async (sql: string) => {
        let expected, actual;
        try {
          expected = { rows: (await pg.query(sql)).rows };
        } catch (error) {
          expected = { error: code(error) };
        }
        try {
          const result = await core.execute(sql);
          if (!("batches" in result)) throw Error("query");
          actual = { rows: oracleRows(result) };
        } catch (error) {
          actual = { error: code(error) };
        }
        expect(actual, sql).toEqual(expected);
      };
      for (const fn of ["sum", "avg"])
        for (const partition of ["id%1", "p", "p,id%2"])
          for (const direction of ["ASC", "DESC"])
            for (const output of ["matching", "prefix", "reverse partition", "wrong key"])
              for (const tail of ["LIMIT 0", "LIMIT 1", "LIMIT 2", "LIMIT 1 OFFSET 1"]) {
                reach.beginHistory();
                reach.reach(fn === "sum" ? "sum" : "avg");
                reach.reach(partition === "id%1" ? "computed partition" : "nullable partitions");
                if (partition.includes(",")) reach.reach("multiple partition keys");
                reach.reach(direction === "ASC" ? "ascending" : "descending");
                if (output === "matching") reach.reach("matching order");
                else if (output === "prefix") reach.reach("partition prefix");
                else reach.reach("mismatching order");
                const order =
                  output === "matching"
                    ? `${partition
                        .split(",")
                        .map((key) => `${key} ASC NULLS LAST`)
                        .join(",")},id ${direction}`
                    : output === "prefix"
                      ? `${partition.split(",")[0] ?? partition} ASC NULLS LAST`
                      : output === "reverse partition"
                        ? `${partition
                            .split(",")
                            .map((key) => `${key} DESC NULLS FIRST`)
                            .join(",")},id ${direction}`
                        : `id ${direction}`;
                const sql = `SELECT ${fn}(x) OVER(PARTITION BY ${partition} ORDER BY id ${direction} ROWS UNBOUNDED PRECEDING) s FROM w ORDER BY ${order} ${tail}`;
                await compare(sql);
              }
      for (const partition of ["p", "id%3", "p,id%2"])
        for (const direction of ["ASC", "DESC"])
          for (const frame of [
            "ROWS UNBOUNDED PRECEDING",
            "ROWS 1 PRECEDING",
            "RANGE CURRENT ROW",
          ]) {
            reach.reach("full ordered result");
            await compare(
              `SELECT id,${partition.split(",")[0] ?? partition} p,sum(v) OVER(PARTITION BY ${partition} ORDER BY id ${direction} ${frame}) s FROM w ORDER BY ${partition
                .split(",")
                .map((key) => `${key} ASC NULLS LAST`)
                .join(",")},id ${direction}`,
            );
          }
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 120000);
