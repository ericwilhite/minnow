/** JSON record labels are SQL identifiers, independent of unique result binding ids. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, JSON_RECORD_NAMES_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
const quote = (name: string) => '"' + name.replaceAll('"', '""') + '"';
const labels = [
  ["a", "a"],
  ["a.b", "Case"],
  ["Case", "case"],
  ['a"b', 'a"b'],
  ["a", "a", "a_2", "a.b", "Case"],
] as const;
const wrappers = [
  "derived",
  "CTE",
  "wildcard",
  "nested record",
  "hidden ORDER BY",
  "DISTINCT",
  "CTE column list",
  "derived column list",
  "window fields",
  "literal UNION ALL",
  "aliased wildcard",
] as const;
it.each(seedsFor("next-json-record-names", [0xb72a, 0xb72b]))(
  "preserves duplicate and quoted record fields through query shapes (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON record identifiers", JSON_RECORD_NAMES_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: labels.length - 1 }),
        fc.integer({ min: 0, max: wrappers.length - 1 }),
        fc.integer({ min: -1000, max: 1000 }),
        fc.boolean(),
        fc.integer({ min: 0, max: 2 }),
        async (shape, wrapper, value, pretty, expressionKind) => {
          reach.beginHistory();
          const names = labels[shape] ?? labels[0];
          const expression = (i: number): string => {
            // General computed UNION ALL arms remain a typed executor refusal.
            // This arm exercises supported literal UNIONs with the same data.
            if (wrapper === 9) return String(value + i);
            const base =
              expressionKind === 1
                ? `(${String(value + i)}+0)`
                : expressionKind === 2
                  ? `COALESCE(${String(value + i)}::int,NULL::int)`
                  : String(value + i);
            return wrapper === 8 ? `(${base}*ROW_NUMBER() OVER())` : base;
          };
          const aliasList = wrapper === 6 || wrapper === 7 || wrapper === 10;
          const inner =
            "SELECT " +
            (wrapper === 5 ? "DISTINCT " : "") +
            names
              .map(
                (name, i) => `${expression(i)} AS ${quote(aliasList ? `field${String(i)}` : name)}`,
              )
              .join(",") +
            (wrapper === 4 ? " ORDER BY 0+0" : "");
          const columns = names.map(quote).join(",");
          const source =
            wrapper === 1 || wrapper === 6
              ? `WITH q${wrapper === 6 ? `(${columns})` : ""} AS (${inner}) `
              : "";
          const from =
            wrapper === 1 || wrapper === 6
              ? "q"
              : wrapper === 2
                ? `(SELECT * FROM (${inner}) s) q`
                : wrapper === 9
                  ? `(${inner} UNION ALL ${inner}) q`
                  : wrapper === 10
                    ? `(SELECT * FROM (${inner}) s(${columns})) q`
                    : `(${inner}) q${wrapper === 7 ? `(${columns})` : ""}`;
          const rendered = wrapper === 3 ? "ROW(q,ROW(1,NULL::text))" : "q";
          const sql = `${source}SELECT row_to_json(${rendered},${pretty ? "true" : "false"})::text j,to_jsonb(${rendered})::text b FROM ${from}`;
          const expected = (await pg.query(sql)).rows;
          const actual = await core.execute(sql);
          if (!("batches" in actual)) throw new Error("Expected record rows");
          expect(oracleRows(actual), sql).toEqual(expected);
          reach.reach(
            ["duplicate", "dotted", "case-sensitive", "embedded quote", "mixed labels"][shape] ??
              "duplicate",
          );
          reach.reach(wrappers[wrapper] ?? "derived");
          if (expressionKind === 1 && wrapper !== 9) reach.reach("computed fields");
          if (expressionKind === 2 && wrapper !== 9) reach.reach("typed fields");
          reach.reach(pretty ? "pretty" : "compact");
          reach.reach("exact JSON and JSONB text");
          expect(core.spill.bytesInUse).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 72,
        examples: [
          [0, 0, 1, false, 0],
          [1, 0, 1, true, 0],
          [2, 1, 0, false, 0],
          [3, 2, -1, true, 0],
          [4, 3, 2, false, 0],
          [4, 4, 1, true, 1],
          [0, 5, 2, false, 2],
          [4, 6, 0, true, 0],
          [0, 7, 1, false, 0],
          [4, 8, 2, true, 1],
          [0, 9, 3, false, 2],
          [0, 10, 1, true, 0],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
