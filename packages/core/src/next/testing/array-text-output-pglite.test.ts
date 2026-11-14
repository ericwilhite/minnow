/** array_out quoting, element output functions and bounds have one implementation with FTSv2. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ARRAY_TEXT_OUTPUT_REACH } from "../../testing/reach.js";
import { encodeArrayValue, type ArrayElement, type ArrayValue } from "../vector/array-value.js";
import type { PhysicalType } from "../types.js";
import { compileExpr } from "../exec/kernels/compile.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { installSynchronousJson } from "../exec/kernels/test-support.js";
import { ftsValue } from "../exec/fts-value.js";
import { oracleValue } from "./differential-adapter.js";
import { outputCase, outputVector, arrayInput } from "./text-output-cases.test.fixture.js";
const enumLabels = ["", "NULL", "nUlL", "a,b", 'a"b', "a\\b", "汉"];
const ENUM = { kind: "enum", name: "out_enum", labels: enumLabels } as const;
const strings = [
  "",
  "NULL",
  "Null",
  "nUlL",
  "a,b",
  'a"b',
  "a\\b",
  "a{b",
  " a",
  "\t",
  "\n",
  "\r",
  "\v",
  "\f",
  "\u0085",
  "\u00a0",
  "\u2003",
  "汉😀",
  "\ufeff",
  "with'quote",
];
const base = {
  family: 13,
  shape: [2],
  lower: -2,
  other: 1,
  n: 0,
  scale: 3,
  bits: 0x8000000000000000n,
  nulls: true,
  nil: false,
};
for (const seed of seedsFor("array-text-output", [47067, 47068]))
  it(`matches PG array_out and FTSv2 seed${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("array text", ARRAY_TEXT_OUTPUT_REACH);
    try {
      await pg.waitReady;
      await pg.exec("SET TIME ZONE 'UTC'");
      await pg.exec(
        "CREATE TYPE out_enum AS ENUM(" +
          enumLabels.map((v) => "'" + v.replaceAll("'", "''") + "'").join(",") +
          ")",
      );
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            family: fc.integer({ min: 0, max: 15 }),
            shape: fc.constantFrom<number[]>([], [1], [2], [2, 2], [2, 1, 2]),
            other: fc.constantFrom(-2, 0, 1),
            lower: fc.constantFrom(-2147483648, -2, 0, 1, 2147483644),
            n: fc.integer({ min: -100, max: 100 }),
            scale: fc.integer({ min: 0, max: 12 }),
            bits: fc.bigInt({ min: 0n, max: 0xffffffffffffffffn }),
            nulls: fc.boolean(),
            nil: fc.boolean(),
          }),
          async (s) => {
            reach.beginHistory();
            const count = s.shape.length ? s.shape.reduce((a, b) => a * b, 1) : 0,
              values: Array<ArrayElement | null> = [],
              inputs: Array<string | null> = [];
            for (let i = 0; i < Math.max(1, count); i++) {
              if (s.family === 13) {
                const v = strings[(Math.abs(s.n) + i) % strings.length] ?? "v";
                values.push(new TextEncoder().encode(v));
                inputs.push(v);
              } else if (s.family === 14) {
                const v = Uint8Array.from(
                  { length: Math.abs(s.n) % 9 },
                  (_, k) => (k * 127 + 17 + i) & 255,
                );
                values.push(v);
                inputs.push("\\x" + Array.from(v, (b) => b.toString(16).padStart(2, "0")).join(""));
              } else if (s.family === 15) {
                const value = enumLabels[(Math.abs(s.n) + i) % enumLabels.length] ?? "";
                values.push(new TextEncoder().encode(value));
                inputs.push(value);
              } else {
                const c = outputCase({
                  ...s,
                  family: s.family,
                  n: s.n + i,
                  scope: 0,
                  sign: i % 2 === 0,
                  nil: false,
                });
                if (s.family === 3 && s.scale <= 2) {
                  const special = (["NaN", "Infinity", "-Infinity"] as const)[s.scale] ?? "NaN";
                  values.push({ coefficient: 0n, scale: 0, special });
                  inputs.push(special);
                } else {
                  values.push(c.value);
                  inputs.push(c.input);
                }
              }
              if (s.nulls && i % 3 === 1) {
                values[i] = null;
                inputs[i] = null;
              }
            }
            // All primitive families have at least one fixture even when the array itself is empty.
            const c0 = outputCase({ ...s, scope: 0, sign: true, nil: false }),
              type: PhysicalType = {
                kind: "array",
                element:
                  s.family === 13
                    ? { kind: "text" }
                    : s.family === 14
                      ? { kind: "bytes" }
                      : s.family === 15
                        ? ENUM
                        : c0.type,
              },
              pgType =
                s.family === 13
                  ? "text"
                  : s.family === 14
                    ? "bytea"
                    : s.family === 15
                      ? "out_enum"
                      : c0.sqlType;
            const array: ArrayValue = {
                dimensions: s.shape,
                lowerBounds: s.shape.map((_, i) => (i % 2 ? s.other : s.lower)),
                elements: values.slice(0, count),
              },
              native = s.nil ? null : encodeArrayValue(type.element, array),
              input = s.nil ? null : arrayInput(array, inputs.slice(0, count));
            try {
              await pg.query(`SELECT $1::${pgType}[]`, [input]);
            } catch (e) {
              throw new Error("Harness bug: array input cannot materialize", { cause: e });
            }
            const expected = (
              await pg.query<{ t: string | null }>(`SELECT $1::${pgType}[]::text t`, [input])
            ).rows[0]?.t;
            let used = 0;
            const arena = new OutputArena(
              {
                allocate(n) {
                  used += n;
                  return true;
                },
                free(n) {
                  used -= n;
                },
              },
              1,
            );
            installSynchronousJson(arena);
            const compiled = compileExpr(
              { op: "cast", input: { op: "column", index: 0, type }, type: { kind: "text" } },
              { arena, input: [type], params: { slots: [] } },
            );
            try {
              const vector = outputVector(type, native),
                before = arena.bytes,
                result = compiled.evaluate({ length: 1, columns: [vector], selection: null }, null);
              expect(oracleValue(result, 0)).toBe(expected);
              if (result.form !== "flat" || result.data.layout !== "varlen")
                throw new Error("Text bytes required");
              expect(arena.bytes - before).toBe(result.data.bytes.length);
              if (!s.nil) expect(ftsValue(vector, 0)).toBe(expected);
              reach.reach(s.family === 13 ? "text" : s.family === 14 ? "bytes" : "scalar elements");
              if (s.family === 3) reach.reach("numeric scale");
              if (s.family === 3 && s.scale < 3) reach.reach("numeric specials");
              if (s.family === 15) reach.reach("enum");
              if (s.family === 6) reach.reach("boolean t/f");
              if (s.family === 4 || s.family === 5) reach.reach("PG float");
              if (s.family >= 7 && s.family <= 11) reach.reach("temporal elements");
              if (s.shape.length > 1) reach.reach("multidimensional");
              if ((s.lower !== 1 || s.other !== 1) && count) reach.reach("bounds");
              if (s.lower === 1 && s.other !== 1 && s.shape.length > 1 && !s.nil)
                reach.reach("mixed lower bounds");
              if (s.nil) reach.reach("NULL array");
              if (s.nulls && count > 1 && !s.nil) reach.reach("NULL elements");
              if (!count && !s.nil) reach.reach("empty array");
              reach.reach("shared FTS");
              reach.reach("PG exact");
            } finally {
              arena.close();
              expect(used).toBe(0);
              reach.reach("zero");
            }
          },
        ),
        {
          seed,
          numRuns: 96,
          examples: [
            ...Array.from({ length: 16 }, (_, family) => [{ ...base, family }] as [typeof base]),
            [{ ...base, family: 3, scale: 0 }],
            [{ ...base, family: 3, scale: 1 }],
            [{ ...base, family: 3, scale: 2 }],
            [{ ...base, shape: [2, 2] }],
            [{ ...base, shape: [2, 2], lower: 1, other: -2 }],
            [{ ...base, shape: [], nil: false }],
            [{ ...base, nil: true }],
          ],
          endOnFailure: true,
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
