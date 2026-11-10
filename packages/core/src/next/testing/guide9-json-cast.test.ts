/** Authorized SQL correction only: JSON extraction executes through typed, charged native kernels. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import fixture from "./executor-catalogue.json" with { type: "json" };
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { createReach, GUIDE9_JSON_CAST_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
let pg: PGlite, current: MinnowDatabase, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  current = new MinnowDatabase(new MemoryBlockStore(), { autoCompact: false, autoCollect: false });
  core = await new StorageCore(32).open();
}, 60_000);
afterAll(async () => {
  await core.close();
  await current.close();
  await pg.close();
});
type Scalar = string | number | boolean | null;
interface Shape {
  form: number;
  value: Scalar;
}
it.each(seedsFor("next-guide9-json-cast", [0xb62b, 0xb62c]))(
  "resolves guide-9 ambiguity and keeps PG text extraction (seed %i)",
  async (seed) => {
    const reach = createReach<string>("guide-9 JSON cast", GUIDE9_JSON_CAST_REACH);
    const cell = fixture.queries.find((query) => query.id === "guide-9");
    if (cell === undefined) throw new Error("Missing frozen guide-9");
    const examples: Array<[Shape]> = [
      [{ form: 0, value: 30 }],
      [{ form: 0, value: -42 }],
      [{ form: 0, value: "a" }],
      [{ form: 0, value: "'é\\\"\n" }],
      [{ form: 0, value: true }],
      [{ form: 0, value: false }],
      [{ form: 0, value: null }],
      [{ form: 1, value: 1 }],
      [{ form: 2, value: 1 }],
      [{ form: 3, value: 1 }],
    ];
    const scalar = fc.oneof(
      fc.integer({ min: -1_000_000, max: 1_000_000 }),
      fc.boolean(),
      fc.constant(null),
      fc
        .array(fc.constantFrom("a", "é", "中", "'", "\\", '"', " ", "0", "\n", "\t"), {
          maxLength: 32,
        })
        .map((chars) => chars.join("")),
    );
    await fc.assert(
      fc.asyncProperty(
        fc.record({ form: fc.integer({ min: 0, max: 3 }), value: scalar }),
        async (shape) => {
          reach.beginHistory();
          const document =
            shape.form === 0
              ? { sku: "A-1", dims: { w: shape.value, h: 40 } }
              : shape.form === 1
                ? { dims: {} }
                : shape.form === 2
                  ? {}
                  : { dims: null };
          const source = JSON.stringify(document).replaceAll("'", "''");
          const original = `SELECT '${source}' -> 'dims' ->> 'w' AS width`;
          // Reuse the frozen cell's cast, path and operators with generated document values.
          const sql = cell.sql.replace(/^SELECT '(?:[^']|'')*'/u, `SELECT '${source}'`);
          await expect(pg.query(original)).rejects.toMatchObject({ code: "42725" });
          const expected = await pg.query(sql);
          expect(expected.fields.map((field) => field.dataTypeID)).toEqual([25]);
          expect(expected.rows).toEqual([
            {
              width: shape.form === 0 && shape.value !== null ? String(shape.value) : null,
            },
          ]);
          expect((await current.query(sql)).rows, sql).toEqual(expected.rows);
          const actual = await core.execute(sql);
          if (!("batches" in actual)) throw new Error("Expected JSON rows");
          expect(oracleRows(actual), sql).toEqual(expected.rows);
          reach.reach("explicit JSON");
          reach.reach("text result");
          reach.reach("original ambiguity");
          reach.reach("native execution");
          if (shape.form !== 0)
            reach.reach(["", "missing member", "missing parent", "null parent"][shape.form] ?? "");
          else if (shape.value === null) reach.reach("JSON null");
          else if (typeof shape.value === "number") {
            reach.reach("integer");
            if (shape.value < 0) reach.reach("negative");
          } else if (typeof shape.value === "boolean") reach.reach("boolean");
          else {
            reach.reach("string");
            if (/["'\\\n\t]/u.test(shape.value)) reach.reach("escaped string");
          }
        },
      ),
      { seed, numRuns: 80 + examples.length, examples },
    );
    reach.assertReached();
  },
  120_000,
);
