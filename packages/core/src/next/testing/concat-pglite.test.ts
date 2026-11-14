/** PostgreSQL array_cat/append/prepend: values, NULLs, dimensions and lower bounds. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ARRAY_CONCAT_REACH } from "../../testing/reach.js";
import { encodeArrayValue, decodeArrayValue, type ArrayValue } from "../vector/array-value.js";
import type { FlatVector, Vector } from "../vector/vector.js";
import { compileExpr } from "../exec/kernels/compile.js";
import { installSynchronousJson } from "../exec/kernels/test-support.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { oracleValue } from "./differential-adapter.js";
const INT = { kind: "int32", bits: 32 } as const,
  ARRAY = { kind: "array", element: INT } as const;
const arrays = fc
  .record({
    shape: fc.constantFrom<number[]>([], [1], [2], [2, 2], [2, 1, 2]),
    lower: fc.oneof(
      fc.integer({ min: -2, max: 2 }),
      fc.constantFrom(-2147483648, -2147483647, 2147483645, 2147483646, 2147483647),
    ),
    values: fc.array(fc.option(fc.integer({ min: -7, max: 7 }), { nil: null }), {
      minLength: 4,
      maxLength: 4,
    }),
  })
  .map(({ shape, lower, values }): ArrayValue => ({
    dimensions: shape,
    lowerBounds: shape.map((n) => Math.min(lower, 0x7fffffff - n)),
    elements: Array.from(
      { length: shape.length ? shape.reduce((n, d) => n * d, 1) : 0 },
      (_, i) => values[i % values.length] ?? null,
    ),
  }));
function pgArray(v: ArrayValue | null): string | null {
  if (!v) return null;
  if (!v.dimensions.length) return "{}";
  let at = 0;
  const nest = (d: number): string =>
    `{${Array.from({ length: v.dimensions[d] ?? 0 }, () => (d + 1 === v.dimensions.length ? String((v.elements[at++] as number | null) ?? "NULL") : nest(d + 1))).join(",")}}`;
  return `${v.dimensions.map((n, i) => `[${String(v.lowerBounds[i])}:${String((v.lowerBounds[i] ?? 1) + n - 1)}]`).join("")}=${nest(0)}`;
}
function vector(v: ArrayValue | null): FlatVector {
  const bytes = v ? encodeArrayValue(INT, v) : new Uint8Array(0);
  return {
    form: "flat",
    type: ARRAY,
    length: 1,
    validity: v ? null : new Uint32Array(1),
    data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
  };
}
for (const seed of seedsFor("array-concat", [47051, 47052]))
  it(`matches array shapes, NULLs and lower bounds seed${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("array concat", ARRAY_CONCAT_REACH);
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.option(arrays, { nil: null }),
          fc.option(arrays, { nil: null }),
          fc.constantFrom("cat", "append", "prepend"),
          fc.option(fc.integer({ min: -7, max: 7 }), { nil: null }),
          fc.boolean(),
          fc.boolean(),
          async (a, b, mode, element, constant, derive) => {
            reach.beginHistory();
            const derived =
              derive &&
              a !== null &&
              a.dimensions.length === 1 &&
              (a.lowerBounds[0] ?? 1) > -0x80000000;
            const actualA: ArrayValue | null = derived
              ? { ...a, dimensions: [(a.dimensions[0] ?? 0) + 1], elements: [2, ...a.elements] }
              : a;
            const scalar: FlatVector = {
              form: "flat",
              type: INT,
              length: 1,
              validity: element === null ? new Uint32Array(1) : null,
              data: { layout: "i32", values: Int32Array.of(element ?? 0) },
            };
            const inputs: Vector[] =
              mode === "cat"
                ? [vector(actualA), vector(b)]
                : mode === "append"
                  ? [vector(actualA), scalar]
                  : [scalar, vector(actualA)];
            const params = [pgArray(a), pgArray(b), element];
            const from = "FROM (SELECT $1::int[] aa,$2::int[] bb,$3::int ee)bound_args";
            const source = derived ? "array_prepend(2,aa)" : "aa";
            // Independently materialize the exact input expression before crediting
            // an operator error. A PG literal refusal is never operation coverage.
            try {
              await pg.query(`SELECT ${source} a,bb,ee ${from}`, params);
            } catch (error) {
              throw new Error("Harness bug: array input did not materialize", { cause: error });
            }
            const operator =
              mode === "cat"
                ? `${source}||bb`
                : mode === "append"
                  ? `${source}||ee`
                  : `ee||${source}`;
            let expected: Array<Record<string, unknown>> | null = null,
              error: string | null = null;
            try {
              expected = (
                await pg.query<Record<string, unknown>>(
                  `SELECT (${operator}) a,array_dims(${operator}) d ${from}`,
                  params,
                )
              ).rows;
            } catch (e) {
              error = (e as { code: string }).code;
            }
            let used = 0;
            const arena = new OutputArena(
              {
                allocate: (n) => {
                  used += n;
                  return true;
                },
                free: (n) => {
                  used -= n;
                },
              },
              1,
            );
            installSynchronousJson(arena);
            try {
              const expression = {
                op: "call" as const,
                fn:
                  mode === "cat"
                    ? "array_cat"
                    : mode === "append"
                      ? "array_append"
                      : "array_prepend",
                args: inputs.map((v, index) => ({ op: "column" as const, index, type: v.type })),
                type: ARRAY,
              };
              const compiled = compileExpr(expression, {
                arena,
                input: inputs.map((v) => v.type),
                params: { slots: [] },
              });
              const supplied = constant
                ? inputs.map((v) => ({
                    form: "constant" as const,
                    type: v.type,
                    length: 1,
                    value: v as FlatVector,
                  }))
                : inputs;
              const beforePayload = arena.bytes;
              let got: Vector | null = null,
                failure: unknown;
              try {
                got = compiled.evaluate({ length: 1, columns: supplied, selection: null }, null);
              } catch (e) {
                failure = e;
              }
              if (error) {
                expect(failure).toMatchObject({ sqlState: error });
                reach.reach("dimension error");
                if (error === "54000") reach.reach("bounds 54000");
                if (error === "22003") reach.reach("prepend 22003");
              } else {
                if (!got)
                  throw failure instanceof Error
                    ? failure
                    : new Error("Missing result", { cause: failure });
                if (got.form !== "flat" || got.data.layout !== "varlen")
                  throw new Error("Native array output required");
                expect(arena.bytes - beforePayload).toBe(got.data.bytes.byteLength);
                reach.reach("charged output");
                expect(oracleValue(got, 0)).toEqual(expected?.[0]?.a);
                let dims: string | null = null;
                if (got.validity?.[0] !== 0) {
                  const v = decodeArrayValue(
                    INT,
                    got.data.bytes.subarray(0, got.data.offsets[1] ?? 0),
                  );
                  dims = v.dimensions.length
                    ? v.dimensions
                        .map(
                          (n, i) =>
                            `[${String(v.lowerBounds[i])}:${String((v.lowerBounds[i] ?? 1) + n - 1)}]`,
                        )
                        .join("")
                    : null;
                }
                expect(dims).toBe(expected?.[0]?.d);
                reach.reach("PG exact");
              }
              reach.reach(mode);
              if (!a || !b || element === null) reach.reach("NULL");
              if (a && !a.dimensions.length) reach.reach("empty");
              if (a?.dimensions.length === 2) reach.reach("multidimensional");
              if (a?.lowerBounds.some((n) => n !== 1)) reach.reach("lower bound");
              if (constant) reach.reach("constant");
              if (
                derived &&
                actualA &&
                (actualA.lowerBounds[0] ?? 1) + (actualA.dimensions[0] ?? 0) > 0x7fffffff
              )
                reach.reach("derived boundary");
            } finally {
              arena.close();
              expect(used).toBe(0);
              reach.reach("zero");
            }
          },
        ),
        {
          seed,
          numRuns: 64,
          endOnFailure: true,
          examples: [
            [
              { dimensions: [1], lowerBounds: [-2147483648], elements: [1] },
              null,
              "prepend",
              2,
              false,
              false,
            ],
            [
              { dimensions: [1], lowerBounds: [2147483646], elements: [1] },
              null,
              "append",
              2,
              false,
              false,
            ],
            [
              { dimensions: [1], lowerBounds: [2147483646], elements: [1] },
              null,
              "prepend",
              2,
              false,
              false,
            ],
            [
              { dimensions: [1], lowerBounds: [2147483646], elements: [1] },
              null,
              "cat",
              null,
              false,
              true,
            ],
            [
              { dimensions: [1], lowerBounds: [2147483646], elements: [1] },
              null,
              "append",
              2,
              false,
              true,
            ],
            [
              { dimensions: [2], lowerBounds: [0], elements: [1, null] },
              { dimensions: [1], lowerBounds: [3], elements: [2] },
              "cat",
              null,
              false,
              false,
            ],
            [{ dimensions: [], lowerBounds: [], elements: [] }, null, "cat", 0, true, false],
            [null, null, "append", null, false, false],
            [
              { dimensions: [2, 2], lowerBounds: [1, 1], elements: [1, 2, 3, 4] },
              { dimensions: [2], lowerBounds: [1], elements: [5, 6] },
              "cat",
              0,
              false,
              false,
            ],
            [
              { dimensions: [2, 2], lowerBounds: [1, 1], elements: [1, 2, 3, 4] },
              null,
              "append",
              3,
              false,
              false,
            ],
            [
              { dimensions: [1], lowerBounds: [-1], elements: [7] },
              null,
              "prepend",
              null,
              true,
              false,
            ],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
