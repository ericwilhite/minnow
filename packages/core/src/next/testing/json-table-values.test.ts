/** A constant JSON_TABLE's UNION ALL desugar is a bag of literal rows, not a set executor. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import {
  createReach,
  JSON_TABLE_VALUES_REACH,
  JSON_TABLE_NULL_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
}, 60_000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
it.each(seedsFor("next-json-table-values", [0xb615, 0xb616]))(
  "executes constant JSON_TABLE bags like PG (seed %i)",
  async (seed) => {
    const reach = createReach<string>("constant JSON_TABLE", JSON_TABLE_VALUES_REACH);
    const item = fc.record({
      sku: fc.option(fc.string({ maxLength: 24 }), { nil: null }),
      qty: fc.option(fc.integer({ min: -999, max: 999 }), { nil: null }),
    });
    const examples: Array<[Array<{ sku: string | null; qty: number | null }>]> = [
      [[]],
      [[{ sku: "a", qty: 1 }]],
      [
        [
          { sku: "same", qty: null },
          { sku: "same", qty: null },
        ],
      ],
      [
        [
          { sku: null, qty: -1 },
          { sku: "'é\\", qty: 99 },
        ],
      ],
    ];
    await fc.assert(
      fc.asyncProperty(fc.array(item, { maxLength: 36 }), async (rows) => {
        reach.beginHistory();
        const json = JSON.stringify(rows).replaceAll("'", "''");
        const sql = `SELECT t.sku,t.qty FROM JSON_TABLE('${json}','$[*]' COLUMNS(sku TEXT PATH '$.sku',qty INTEGER PATH '$.qty')) AS t`;
        const authority = await pg.query<Record<string, unknown>>(sql);
        const expected = authority.rows;
        expect(authority.fields.map((f) => f.dataTypeID)).toEqual([25, 23]);
        const lowered = core.lowerer.lower(sql, core.context());
        if (lowered.kind !== "query") throw new Error("Expected JSON_TABLE query");
        expect(lowered.columns.map((c) => c.sqlType)).toEqual(["text", "integer"]);
        const result = await core.execute(sql);
        if (!("batches" in result)) throw new Error("Expected JSON_TABLE rows");
        expect(oracleRows(result), sql).toEqual(expected);
        reach.reach(rows.length === 0 ? "empty" : rows.length === 1 ? "one row" : "multiple rows");
        if (rows.some((r) => r.sku === null)) reach.reach("NULL text");
        if (rows.some((r) => r.qty === null)) reach.reach("NULL integer");
        if (new Set(rows.map((r) => JSON.stringify(r))).size < rows.length)
          reach.reach("duplicates");
        if (rows.some((r) => r.sku?.includes("'"))) reach.reach("escaped text");
        if (rows.some((r) => (r.qty ?? 0) < 0)) reach.reach("negative integer");
      }),
      { seed, numRuns: 80 + examples.length, examples },
    );
    // This rewrite never evaluates arbitrary scalar calls or changes set multiplicities.
    for (const sql of [
      "SELECT 1 AS n UNION SELECT 1",
      "SELECT 1 AS n EXCEPT SELECT 2",
      "SELECT random() AS n UNION ALL SELECT random()",
    ])
      expect(() => core.prepare(sql)).toThrow(
        expect.objectContaining({ sqlState: "0A000", stage: "execute" }),
      );
    reach.assertReached();
  },
  120_000,
);

const DECLARATIONS = [
  "smallint",
  "integer",
  "bigint",
  "real",
  "double precision",
  "boolean",
  "text",
  "numeric",
  "date",
  "timestamp without time zone",
  "timestamp with time zone",
] as const;
it.each(seedsFor("next-json-table-null-declarations", [0xb617, 0xb618]))(
  "keeps declared JSON_TABLE types on NULL-only and empty sources (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON_TABLE NULL declarations", JSON_TABLE_NULL_REACH);
    const examples: Array<[(typeof DECLARATIONS)[number], number]> = DECLARATIONS.flatMap(
      (type) =>
        [
          [type, 0],
          [type, 1],
          [type, 3],
        ] as Array<[(typeof DECLARATIONS)[number], number]>,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...DECLARATIONS),
        fc.integer({ min: 0, max: 12 }),
        async (type, count) => {
          reach.beginHistory();
          const source = (json: string) =>
            `JSON_TABLE('${json}','$[*]' COLUMNS(v ${type} PATH '$.v')) AS j`;
          const expected = (
            await pg.query<{ typ: string }>(
              `SELECT pg_typeof(v)::text AS typ FROM ${source('[{"v":null}]')}`,
            )
          ).rows[0]?.typ;
          const json = JSON.stringify(Array.from({ length: count }, () => ({ v: null })));
          const lowered = core.lowerer.lower(`SELECT v FROM ${source(json)}`, core.context());
          if (lowered.kind !== "query") throw new Error("Expected JSON_TABLE query");
          expect(lowered.columns[0]?.sqlType, `${type}/${json}`).toBe(expected);
          reach.reach(type);
          reach.reach(count === 0 ? "empty" : count === 1 ? "single NULL" : "NULL-only prefix");
        },
      ),
      { seed, numRuns: 80 + examples.length, examples },
    );
    reach.assertReached();
  },
  120_000,
);
