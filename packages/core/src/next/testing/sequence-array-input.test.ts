/** array_agg(anyarray) adds a dimension and preserves shape, bounds and element NULLs. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SEQUENCE_ARRAY_INPUT_REACH } from "../../testing/reach.js";
import { oracleRows } from "./differential-adapter.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { decodeArrayValue, encodeArrayValue } from "../vector/array-value.js";
const INT = { kind: "text" } as const;
for (const seed of seedsFor("sequence-array-input", [47059, 47060]))
  it(`matches array aggregation shape errors and bounds seed${String(seed)}`, async () => {
    const core = await new StorageCore().open(),
      pg = new PGlite(),
      reach = createReach("array aggregate input", SEQUENCE_ARRAY_INPUT_REACH);
    try {
      await pg.waitReady;
      let history = 0;
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(
            "same",
            "null",
            "empty",
            "mismatch",
            "multidim",
            "numeric-scale",
            "derived-bound",
            "derived-null",
          ),
          fc.constantFrom(-2147483648, -2, -1, 0, 1, 2, 2147483644, 2147483645),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (kind, lower, filter, desc, distinct) => {
            reach.beginHistory();
            const name = `array_inputs_${String(history++)}`;
            const element =
              kind === "numeric-scale"
                ? { kind: "decimal" as const, precision: null, scale: null }
                : INT;
            const arrayType = { kind: "array" as const, element };
            await core.createTable({
              name,
              columns: [
                { name: "i", type: "number" },
                {
                  name: "a",
                  type: "string",
                  nullable: true,
                  sqlDomain: {
                    kind: "array",
                    element: kind === "numeric-scale" ? "numeric" : "text",
                  },
                },
              ],
            });
            await pg.exec(
              `CREATE TABLE ${name}(i double precision,a ${kind === "numeric-scale" ? "numeric" : "text"}[])`,
            );
            const derived = kind === "derived-bound" || kind === "derived-null";
            const a = derived
              ? "[2147483646:2147483646]={1}"
              : kind === "multidim"
                ? `[${String(lower)}:${String(lower + 1)}][1:2]={{1,NULL},{2,3}}`
                : `[${String(lower)}:${String(lower + 1)}]={${kind === "numeric-scale" ? "1.0" : "1"},NULL}`;
            const b =
              kind === "null"
                ? null
                : kind === "empty"
                  ? "{}"
                  : kind === "mismatch"
                    ? "{1,2,3}"
                    : kind === "numeric-scale"
                      ? a.replace("1.0,NULL", "1.00,NULL")
                      : a;
            const sql = `SELECT array_agg(${distinct ? "DISTINCT " : ""}a ORDER BY ${distinct ? "a" : "i"} ${desc ? "DESC" : "ASC"})${filter ? " FILTER(WHERE i=2 OR i=4)" : ""} a FROM ${name}`;
            const pgSql = sql;
            const utf8 = new TextEncoder();
            const native = (second: boolean) => {
              if (derived)
                return encodeArrayValue(element, {
                  dimensions: [2],
                  lowerBounds: [2147483646],
                  elements: [utf8.encode("2"), utf8.encode("1")],
                });
              if (second && kind === "null") return null;
              if (second && kind === "empty")
                return encodeArrayValue(element, { dimensions: [], lowerBounds: [], elements: [] });
              const dims = kind === "multidim" ? [2, 2] : second && kind === "mismatch" ? [3] : [2];
              const elems =
                dims.length === 2
                  ? ["1", null, "2", "3"]
                  : dims[0] === 3
                    ? ["1", "2", "3"]
                    : ["1", null];
              return encodeArrayValue(element, {
                dimensions: dims,
                lowerBounds: dims.map((_, i) =>
                  i ? 1 : second && kind === "mismatch" ? 1 : lower,
                ),
                elements: elems.map((e) =>
                  e === null
                    ? null
                    : kind === "numeric-scale"
                      ? {
                          coefficient: BigInt(e) * 10n ** BigInt(second ? 2 : 1),
                          scale: second ? 2 : 1,
                        }
                      : utf8.encode(e),
                ),
              });
            };
            const values = Array.from({ length: 4 }, (_, i) =>
                kind === "derived-null" && i === 3 ? null : native(i % 2 === 0),
              ),
              offsets = new Int32Array(5);
            let size = 0;
            values.forEach((v, i) => {
              offsets[i] = size;
              size += v?.length ?? 0;
            });
            offsets[4] = size;
            const bytes = new Uint8Array(size),
              validity = new Uint32Array(1);
            values.forEach((v, i) => {
              if (v) {
                bytes.set(v, offsets[i] ?? 0);
                validity[0] = (validity[0] ?? 0) | (1 << i);
              }
            });
            const table = core.harness.path.catalog.tableByName(name);
            if (!table) throw new Error("Missing table");
            await core.insertVectors(table, {
              length: 4,
              selection: null,
              columns: [
                {
                  form: "flat",
                  type: { kind: "float64" },
                  length: 4,
                  validity: null,
                  data: { layout: "f64", values: Float64Array.of(1, 2, 3, 4) },
                },
                {
                  form: "flat",
                  type: arrayType,
                  length: 4,
                  validity,
                  data: { layout: "varlen", bytes, offsets },
                },
              ],
            });
            for (let i = 1; i <= 4; i++) {
              if (derived)
                await pg.query(
                  `INSERT INTO ${name} SELECT $1,CASE WHEN $3::boolean THEN NULL ELSE array_prepend('2'::text,$2::text[]) END`,
                  [i, a, kind === "derived-null" && i === 4],
                );
              else
                await pg.query(
                  `INSERT INTO ${name} VALUES($1,$2::${kind === "numeric-scale" ? "numeric" : "text"}[])`,
                  [i, i % 2 === 0 ? a : b],
                );
            }
            let expected: Array<Record<string, unknown>> | undefined,
              error: string | undefined,
              dims: string | undefined;
            try {
              expected = (await pg.query<Record<string, unknown>>(pgSql)).rows;
              dims = (await pg.query<{ d: string }>(`SELECT array_dims(a) d FROM(${pgSql})q`))
                .rows[0]?.d;
            } catch (e) {
              error = (e as { code: string }).code;
            }
            const governor = new Governor(32 << 20),
              prepare = core.executor.prepare.bind(core.executor),
              spy = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                const p = prepare(plan);
                return { ...p, execute: (r) => p.execute({ ...r, governor }) };
              });
            try {
              if (error) {
                if ((kind === "same" || kind === "numeric-scale") && error !== "54000")
                  throw new Error(`Unexpected PG error ${error}`);
                await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: error });
                if (error === "54000") reach.reach("bounds 54000");
                else if (kind === "derived-null" && error === "22004")
                  reach.reach("bounds precedence");
                else if (kind === "null" || kind === "empty" || kind === "mismatch")
                  reach.reach(kind);
              } else {
                const result = await core.execute(sql);
                if (!("batches" in result)) throw new Error("Rows required");
                expect(oracleRows(result)).toEqual(expected);
                const col = result.batches[0]?.columns[0];
                if (col?.form !== "flat" || col.data.layout !== "varlen")
                  throw new Error("Serialized array required");
                const decoded = decodeArrayValue(
                  element,
                  col.data.bytes.subarray(col.data.offsets[0] ?? 0, col.data.offsets[1] ?? 0),
                );
                const actualDims = decoded.dimensions
                  .map(
                    (n, i) =>
                      `[${String(decoded.lowerBounds[i])}:${String((decoded.lowerBounds[i] ?? 1) + n - 1)}]`,
                  )
                  .join("");
                expect(actualDims).toBe(dims);
                reach.reach("PG exact");
                if (kind === "same") reach.reach("same");
                reach.reach("bounds");
                if (kind === "multidim") reach.reach("multidim");
                if (kind === "numeric-scale" && distinct) {
                  expect(decoded.dimensions[0]).toBe(1);
                  reach.reach("numeric scale equality");
                }
              }
              if (filter) reach.reach("FILTER");
              if (desc) reach.reach("DESC");
              if (distinct) reach.reach("DISTINCT");
              expect(core.spill.bytesInUse).toBe(0);
              expect(governor.usedBytes).toBe(0);
              reach.reach("zero");
            } finally {
              spy.mockRestore();
              // No query source or phase retains charged arrays between histories.
              expect(core.spill.bytesInUse).toBe(0);
            }
          },
        ),
        {
          seed,
          numRuns: 24,
          endOnFailure: true,
          examples: [
            ["same", -2, false, false, true],
            ["derived-bound", 1, false, false, false],
            ["derived-null", 1, false, false, false],
            ["numeric-scale", 1, false, false, true],
            ["null", 1, false, false, false],
            ["empty", 1, false, false, false],
            ["mismatch", 1, false, false, true],
            ["multidim", -1, true, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 120000);
