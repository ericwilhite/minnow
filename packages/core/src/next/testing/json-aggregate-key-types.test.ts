import { encodeArrayValue } from "../vector/array-value.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
/** Structural grouping equality must not silently degrade to equality of JSONB wire text. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_AGGREGATE_KEY_REACH } from "../../testing/reach.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
  await pg.exec("CREATE TABLE json_group_keys(n INTEGER,doc TEXT)");
  await core.execute("CREATE TABLE json_group_keys(n INTEGER,doc TEXT)");
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
it.each(seedsFor("next-json-aggregate-key-admission", [0xb718, 0xb719]))(
  "groups structural keys with PostgreSQL semantic equality (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "JSON aggregate structural key admission",
      JSON_AGGREGATE_KEY_REACH,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 5 }),
        fc.integer({ min: 1, max: 6 }),
        fc.constantFrom("scalar", "object", "array"),
        async (groups, scale, shape) => {
          reach.beginHistory();
          await pg.exec("TRUNCATE json_group_keys");
          await core.execute("DROP TABLE json_group_keys");
          await core.execute("CREATE TABLE json_group_keys(n INTEGER,doc TEXT)");
          const rows = Array.from({ length: groups * 2 }, (_, n) => {
            const number = String(Math.floor(n / 2) + 1) + (n % 2 ? "." + "0".repeat(scale) : "");
            return {
              n,
              doc:
                shape === "object"
                  ? '{"k":' + number + "}"
                  : shape === "array"
                    ? "[" + number + "]"
                    : number,
            };
          });
          await core.writeBatch("insertBatch", "json_group_keys", rows);
          for (const r of rows)
            await pg.query("INSERT INTO json_group_keys VALUES($1,$2)", [r.n, r.doc]);
          const keys = ["doc::jsonb"];
          for (const key of keys) {
            const sql = `SELECT json_agg(n ORDER BY n)::text j FROM json_group_keys GROUP BY ${key}`;
            const expected = await pg.query<{ j: string }>(sql);
            expect(expected.rows.length).toBe(groups);
            for (const row of expected.rows) expect(JSON.parse(row.j)).toHaveLength(2);
            const actual = await core.execute(sql);
            if (!("batches" in actual)) throw new Error("Expected grouped JSON rows");
            expect(
              oracleRows(actual)
                .map((r) => r.j)
                .sort(),
              sql,
            ).toEqual(expected.rows.map((r) => r.j).sort());
            reach.reach("JSONB key");
          }
          if (shape === "scalar") {
            // Read actual stored SQL arrays. ARRAY(...) construction is a separate
            // existing lowering refusal, and C9 excludes arrays of JSONB elements.
            await core.execute("DROP TABLE IF EXISTS numeric_array_keys");
            await core.createTable({
              name: "numeric_array_keys",
              columns: [
                { name: "n", type: "number" },
                { name: "k", type: "string", sqlDomain: { kind: "array", element: "numeric" } },
              ],
            });
            await pg.exec(
              "DROP TABLE IF EXISTS numeric_array_keys;CREATE TABLE numeric_array_keys(n DOUBLE PRECISION,k NUMERIC[])",
            );
            const element: PhysicalType = { kind: "decimal", precision: null, scale: null };
            const values = rows.map((r) => {
              const digits = r.doc.split(".");
              const arrayScale = digits[1]?.length ?? 0;
              return encodeArrayValue(element, {
                dimensions: [2],
                lowerBounds: [1],
                elements: [
                  { coefficient: BigInt(r.doc.replace(".", "")), scale: arrayScale },
                  null,
                ],
              });
            });
            const offsets = new Int32Array(values.length + 1);
            let total = 0;
            values.forEach((v, i) => {
              offsets[i] = total;
              total += v.length;
            });
            offsets[values.length] = total;
            const bytes = new Uint8Array(total);
            values.forEach((v, i) => bytes.set(v, offsets[i] ?? 0));
            const columns: FlatVector[] = [
              {
                form: "flat",
                type: { kind: "float64" },
                length: rows.length,
                validity: null,
                data: { layout: "f64", values: Float64Array.from(rows.map((r) => r.n)) },
              },
              {
                form: "flat",
                type: { kind: "array", element },
                length: rows.length,
                validity: null,
                data: { layout: "varlen", bytes, offsets },
              },
            ];
            const table = core.harness.path.catalog.tableByName("numeric_array_keys");
            if (!table) throw new Error("Missing array-key table");
            await core.insertVectors(table, { length: rows.length, columns, selection: null });
            for (const r of rows)
              await pg.query("INSERT INTO numeric_array_keys VALUES($1,$2::numeric[])", [
                r.n,
                `{${r.doc},NULL}`,
              ]);
            const sql = "SELECT json_agg(n ORDER BY n)::text j FROM numeric_array_keys GROUP BY k";
            const expected = await pg.query<{ j: string }>(sql),
              actual = await core.execute(sql);
            if (!("batches" in actual)) throw new Error("Expected array-key groups");
            expect(
              oracleRows(actual)
                .map((r) => r.j)
                .sort(),
              sql,
            ).toEqual(expected.rows.map((r) => r.j).sort());
            expect(expected.rows.length).toBe(groups);
            reach.reach("SQL array key");
            reach.reach("numeric array scale equality");
          }
          reach.reach(shape);
          reach.reach(scale === 1 ? "one fractional zero" : "several fractional zeros");
          reach.reach("PG semantic equal pairs");
          reach.reach("exact PostgreSQL groups");
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("zero spill files");
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [2, 1, "scalar"],
          [2, 2, "object"],
          [2, 3, "array"],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it("rejects plain JSON grouping with PostgreSQL's equality SQLSTATE", async () => {
  const sql = "SELECT json_agg(n)::text j FROM json_group_keys GROUP BY doc::json";
  await expect(pg.query(sql)).rejects.toMatchObject({ code: "42883" });
  await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: "42883" });
});
