/** RANGE resolves the offset's type before converting its exact literal value. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { createReach, RANGE_LITERAL_WIDTH_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { LoweredDatabase } from "./lowering-harness.js";

const reach = createReach("RANGE literal width", RANGE_LITERAL_WIDTH_REACH, { minimum: 2 });
for (const seed of seedsFor("next-range-literal-width", [0x7761, 0x7762]))
  it(
    "preserves exact literals until RANGE type resolution, seed " + String(seed),
    async () => {
      const next = new LoweredDatabase(),
        pg = new PGlite();
      await pg.waitReady;
      const random = mulberry32(seed);
      const exponent = 400 + Math.floor(random() * 8);
      reach.beginHistory();
      const outcome = (sql: string) => {
        try {
          return { rows: next.execute(sql) };
        } catch (error) {
          const state = (error as { sqlState?: string }).sqlState;
          if (state === undefined) throw error;
          return { error: state };
        }
      };
      try {
        for (const type of ["NUMERIC", "NUMERIC(12,2)", "DOUBLE PRECISION"] as const)
          for (const state of ["empty", "NULL only", "values"] as const) {
            const table =
              "t" + String(type.length) + String(["empty", "NULL only", "values"].indexOf(state));
            const ddl = "CREATE TABLE " + table + "(id INTEGER,k " + type + ")";
            next.execute(ddl);
            await pg.exec(ddl);
            if (state !== "empty") {
              const insert =
                "INSERT INTO " +
                table +
                " VALUES(1," +
                (state === "NULL only" ? "NULL" : "0") +
                "),(2," +
                (state === "NULL only" ? "NULL" : "0.01") +
                ")";
              next.execute(insert);
              await pg.exec(insert);
            }
            for (const fn of ["count(*)", "row_number()"] as const)
              for (const literal of [
                "1e-" + String(exponent),
                "-1e-" + String(exponent),
                "1e" + String(exponent),
                "-1e" + String(exponent),
                "0.005",
              ])
                for (const limit of ["", " LIMIT 0"]) {
                  const sql =
                    "SELECT id," +
                    fn +
                    "OVER(ORDER BY k RANGE BETWEEN " +
                    literal +
                    " PRECEDING AND CURRENT ROW)AS n FROM " +
                    table +
                    " ORDER BY id" +
                    limit;
                  let expected;
                  try {
                    expected = { rows: (await pg.query(sql)).rows };
                  } catch (error) {
                    expected = { error: (error as { code: string }).code };
                  }
                  expect(outcome(sql), sql).toEqual(expected);
                  reach.reach(type);
                  reach.reach(state);
                  reach.reach(fn);
                  reach.reach(limit === "" ? "evaluated" : "LIMIT 0");
                  reach.reach(
                    literal === "0.005"
                      ? "typmod distance"
                      : literal.includes("-")
                        ? "tiny or negative"
                        : "large",
                  );
                }
          }
        // Distinct exact distances must survive both Number's 0/Infinity collisions and CTE copies.
        for (const sign of ["-", ""]) {
          const suffix = sign + String(exponent),
            table = sign === "" ? "big_distances" : "tiny_distances";
          const ddl = "CREATE TABLE " + table + "(id INTEGER,k NUMERIC)";
          next.execute(ddl);
          await pg.exec(ddl);
          const insert = "INSERT INTO " + table + " VALUES(1,0),(2,1.5e" + suffix + ")";
          next.execute(insert);
          await pg.exec(insert);
          const sql =
            "WITH f AS(SELECT id,COUNT(*)OVER(ORDER BY k RANGE 1e" +
            suffix +
            " PRECEDING)AS short,COUNT(*)OVER(ORDER BY k RANGE 2e" +
            suffix +
            " PRECEDING)AS long FROM " +
            table +
            ")SELECT id,short,long FROM f ORDER BY id";
          const expected = (await pg.query(sql)).rows;
          expect(outcome(sql), sql).toEqual({ rows: expected });
          reach.reach("adjacent frames");
          reach.reach("cloned frames");
        }
      } finally {
        await pg.close();
      }
    },
    180_000,
  );
it("reaches the literal width dimensions", () => reach.assertReached());
