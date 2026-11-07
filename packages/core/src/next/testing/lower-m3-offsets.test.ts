/** Offset evaluation timing: PostgreSQL nodeWindowAgg.c calculate_frame_offsets/in_range. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { createReach, LOWER_M3_OFFSETS_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const reach = createReach("M3 offset timing", LOWER_M3_OFFSETS_REACH, { minimum: 2 });
for (const seed of seedsFor("next-lower-m3-offsets", [0x7741, 0x7742]))
  it(`evaluates offsets at the PostgreSQL execution stage, seed ${String(seed)}`, async () => {
    const core = await new StorageCore().open();
    const pg = new PGlite();
    await pg.waitReady;
    const random = mulberry32(seed);
    reach.beginHistory();
    try {
      const check = async (sql: string, params: Array<number | null>) => {
        let authority;
        try {
          authority = { rows: (await pg.query(sql, params)).rows };
        } catch (error) {
          authority = { code: (error as { code: string }).code };
        }
        let actual;
        try {
          const result = await core.execute(sql, params);
          if (!("batches" in result)) throw new Error("rows expected");
          actual = { rows: oracleRows(result) };
        } catch (error) {
          const code = (error as { sqlState?: string }).sqlState;
          if (code === undefined) throw error;
          actual = { code };
        }
        const normalize = (value: unknown) =>
          JSON.parse(
            JSON.stringify(value, (_, v: unknown) => (typeof v === "bigint" ? Number(v) : v)),
          ) as unknown;
        expect(normalize(actual), `${sql}; params ${String(params)}`).toEqual(normalize(authority));
      };
      for (const state of ["empty", "NULL only", "mixed", "non-null"] as const) {
        const table = `t${String(LOWER_M3_OFFSETS_REACH.indexOf(state))}`;
        const ddl = `CREATE TABLE ${table}(id INTEGER PRIMARY KEY,x DOUBLE PRECISION)`;
        await core.execute(ddl);
        await pg.exec(ddl);
        if (state !== "empty") {
          const rows = Array.from({ length: 2 + Math.floor(random() * 4) }, (_, id) => ({
            id,
            x: state === "NULL only" || (state === "mixed" && id === 0) ? null : id + 1,
          }));
          await core.writeBatch("insertBatch", table, rows);
          for (const row of rows)
            await pg.query(`INSERT INTO ${table} VALUES($1,$2)`, [row.id, row.x]);
        }
        for (const unit of ["ROWS", "RANGE", "GROUPS"] as const)
          for (const fn of ["count(*)", "lag(x)", "row_number()", "ntile(2)"])
            for (const offset of unit === "RANGE"
              ? [-1 - Math.floor(random() * 3), null, NaN]
              : [-1, null])
              for (const parameter of Number.isNaN(offset) ? [true] : [true, false])
                for (const following of [false, true]) {
                  const expr = parameter ? "$1" : offset === null ? "NULL" : String(offset);
                  const bounds = following
                    ? `CURRENT ROW AND ${expr} FOLLOWING`
                    : `${expr} PRECEDING AND CURRENT ROW`;
                  const sql = `SELECT id,${fn}OVER(ORDER BY x ${unit} BETWEEN ${bounds})AS n FROM ${table} ORDER BY id`;
                  await check(sql, parameter ? [offset] : []);
                  await check(`${sql} LIMIT 0`, parameter ? [offset] : []);
                  reach.reach(unit);
                  reach.reach(parameter ? "parameter" : "literal");
                  reach.reach(following ? "FOLLOWING" : "PRECEDING");
                  reach.reach(
                    offset === null
                      ? "NULL offset"
                      : Number.isNaN(offset)
                        ? "NaN offset"
                        : "negative offset",
                  );
                  reach.reach(
                    fn === "count(*)"
                      ? "aggregate"
                      : fn === "lag(x)"
                        ? "navigation"
                        : "optimized ranking",
                  );
                  reach.reach("LIMIT 0");
                }
        reach.reach(state);
      }
    } finally {
      await core.close();
      await pg.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
    expect(core.spill.bytesInUse).toBe(0);
    reach.reach("released");
  }, 180_000);
it("reaches offset startup histories", () => reach.assertReached());
