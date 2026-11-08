/** Canonical DATE constants must reach execution without an unavailable text-input cast. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, DATE_LITERAL_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { DATE_MAX, DATE_MIN, daysFromCivil, formatDate, parseDate } from "../temporal.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite({ parsers: { 1082: (value) => value } });
  await pg.waitReady;
  core = await new StorageCore().open();
}, 60_000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
it.each(seedsFor("next-date-literals", [0xb611, 0xb612]))(
  "keeps canonical DATE literals exact through lowering and execution (seed %i)",
  async (seed) => {
    const reach = createReach<string>("DATE literals", DATE_LITERAL_REACH);
    const examples: Array<[number]> = [
      [daysFromCivil(1, 1, 1)],
      [daysFromCivil(9999, 12, 31)],
      [-1],
      [0],
      [1],
      [8825],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: daysFromCivil(1, 1, 1), max: daysFromCivil(9999, 12, 31) }),
        async (days) => {
          reach.beginHistory();
          const value = formatDate(days);
          const sql = `SELECT DATE '${value}' AS d, DATE '${value}' = $1::date AS same`;
          const expected = (await pg.query<Record<string, unknown>>(sql, [value])).rows;
          const result = await core.execute(sql, [value]);
          if (!("batches" in result)) throw new Error("Expected DATE rows");
          expect(oracleRows(result), sql).toEqual(expected);
          reach.reach("finite");
          if (days < 0) reach.reach("before epoch");
          if (days >= 0) reach.reach("at or after epoch");
          if (days === daysFromCivil(1, 1, 1)) reach.reach("minimum");
          if (days === daysFromCivil(9999, 12, 31)) reach.reach("maximum");
          if (days === 8825) reach.reach("leap day");
        },
      ),
      { seed, numRuns: 80, examples },
    );
    // The shared canonical parser/binder covers the physical range; SQL's shipped parser
    // still limits DATE literal syntax to AD years 0001..9999 (no new syntax claim).
    for (const value of [
      formatDate(DATE_MIN),
      formatDate(DATE_MAX),
      "0001-01-01 BC",
      "infinity",
      "-infinity",
    ]) {
      reach.beginHistory();
      const expected = (await pg.query<{ d: string }>("SELECT $1::date::text AS d", [value]))
        .rows[0]?.d;
      expect(formatDate(parseDate(value))).toBe(expected);
      reach.reach(
        value.endsWith(" BC")
          ? "BC parser"
          : value === "infinity" || value === "-infinity"
            ? value
            : "maximum parser",
      );
    }
    reach.assertReached();
  },
  120_000,
);
