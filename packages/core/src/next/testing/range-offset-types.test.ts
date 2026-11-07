/** PG btree support 3 has cross-width integer and float RANGE overloads. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { createReach, REFERENCE_RANGE_TYPES_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { LoweredDatabase } from "./lowering-harness.js";

const reach = createReach("reference RANGE overloads", REFERENCE_RANGE_TYPES_REACH, { minimum: 2 });
for (const seed of seedsFor("next-reference-range-types", [0x7751, 0x7752]))
  it(
    "checks cross-width RANGE offsets against PostgreSQL, seed " + String(seed),
    async () => {
      const reference = new LoweredDatabase(),
        pg = new PGlite();
      await pg.waitReady;
      const random = mulberry32(seed);
      reach.beginHistory();
      try {
        for (const kind of ["SMALLINT", "INTEGER", "REAL"] as const) {
          const table = kind.toLowerCase() + "_keys";
          const ddl = "CREATE TABLE " + table + "(id INTEGER PRIMARY KEY,k " + kind + ")";
          reference.execute(ddl);
          await pg.exec(ddl);
          const power = Math.floor(random() * 6),
            base = 2 ** power,
            step = 2 ** (power - 23);
          const keys =
            kind === "REAL"
              ? [null, base, base + step]
              : kind === "SMALLINT"
                ? [null, -32768, 0, 32767]
                : [null, -2147483648, 0, 2147483647];
          const offset =
            kind === "REAL" ? step * (1 - 2 ** -26) : 2147483648 + Math.floor(random() * 1024);
          for (const [id, key] of keys.entries()) {
            const insert = "INSERT INTO " + table + " VALUES($1,$2)";
            reference.execute(insert, [id, key]);
            await pg.query(insert, [id, key]);
          }
          for (const direction of ["ASC", "DESC"] as const)
            for (const nulls of ["FIRST", "LAST"] as const)
              for (const parameter of kind === "REAL" ? [false, true] : [false]) {
                const expr = parameter ? "$1" : String(offset),
                  params = parameter ? [offset] : [];
                const sql =
                  "SELECT id,COUNT(*)OVER(ORDER BY k " +
                  direction +
                  " NULLS " +
                  nulls +
                  " RANGE BETWEEN " +
                  expr +
                  " PRECEDING AND " +
                  expr +
                  " FOLLOWING)AS n FROM " +
                  table +
                  " ORDER BY id";
                const expected = (await pg.query(sql, params)).rows;
                expect(reference.execute(sql, params), sql).toEqual(expected);
                reach.reach(kind);
                reach.reach(direction);
                reach.reach(nulls);
                reach.reach(parameter ? "parameter" : "literal");
                reach.reach("NULL key");
                reach.reach("rows match");
              }
        }
      } finally {
        await pg.close();
      }
    },
    120_000,
  );
it("reaches every RANGE overload dimension", () => reach.assertReached());
