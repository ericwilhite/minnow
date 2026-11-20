/** The scan contract puts exactly the unpushable conjuncts in SplitFilter.residual.
 * Planning may inspect their forms, never convert/copy a literal's payload. The existing
 * splitter owns the classification contract; native PGlite differentials own SQL results.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUSHDOWN_ELIGIBILITY_REACH } from "../../testing/reach.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import { allocateFlat, type FlatVector } from "../vector/vector.js";
import { hasResidualFilter, splitFilter } from "./pushdown.js";
import {
  BOOL,
  DATE,
  F32,
  F64,
  I16,
  I32,
  I64,
  TEXT,
  TS,
  bigints,
  col,
  cmp,
  decimal,
  decimals,
  lit,
  numbers,
  texts,
} from "./kernels/test-support.js";

type EligibilityState = (typeof PUSHDOWN_ELIGIBILITY_REACH)[number];
interface Fixture {
  readonly name: EligibilityState;
  readonly type: PhysicalType;
  vector(salt: number): FlatVector;
}
const ordinary = (name: EligibilityState, type: PhysicalType): Fixture => ({
  name,
  type,
  vector: () => ({ ...allocateFlat(type, 1), length: 1 }),
});
const numberFixtures: ReadonlyArray<readonly [EligibilityState, PhysicalType]> = [
  ["int16", I16],
  ["int32", I32],
  ["int64 safe", I64],
  ["float32", F32],
  ["float64", F64],
  ["date", DATE],
  ["timestamp", TS],
  ["timestamptz", { kind: "timestamp", zoned: true }],
  ["time", { kind: "time" }],
  ["bool", BOOL],
];
const fixtures: readonly Fixture[] = [
  ...numberFixtures.map(([name, type]) => ({
    name,
    type,
    vector: (salt: number) => numbers(type, [(salt >>> 0) % 1000]),
  })),
  { name: "float32 from f64", type: F32, vector: (salt) => numbers(F64, [salt]) },
  { name: "float64 from f32", type: F64, vector: (salt) => numbers(F32, [salt]) },
  {
    name: "int64 wide",
    type: I64,
    vector: (salt) => bigints(I64, [BigInt(salt) * 1_000_000_000n]),
  },
  ...(["safe", "int64", "int128", "wide"] as const).map((tier): Fixture => ({
    name: `numeric ${tier}`,
    type: decimal(null, null),
    vector: (salt) =>
      decimals(
        decimal(null, null),
        [[(tier === "int128" ? 2n ** 110n : tier === "wide" ? 2n ** 200n : 0n) + BigInt(salt), 3]],
        tier,
      ),
  })),
  ...(["NaN", "Infinity", "-Infinity"] as const).map((special): Fixture => ({
    name: `numeric ${special}`,
    type: decimal(null, null),
    vector: () => decimals(decimal(null, null), [special]),
  })),
  { name: "text", type: TEXT, vector: (salt) => texts([`λ-${String(salt)}😀`]) },
  {
    name: "bytes",
    type: { kind: "bytes" },
    vector: (salt) => texts([String(salt)], { kind: "bytes" }),
  },
  ordinary("uuid", { kind: "uuid" }),
  ordinary("interval", { kind: "interval" }),
  ordinary("json", { kind: "json", binary: false }),
  ordinary("jsonb", { kind: "json", binary: true }),
  ordinary("array", { kind: "array", element: I32 }),
  ordinary("enum", { kind: "enum", name: "e", labels: ["a"] }),
  ordinary("locator", { kind: "locator", tableId: 1 }),
];
const shapes = [
  "compare",
  "flipped",
  "in",
  "negated in",
  "is null",
  "parameter",
  "and",
  "mixed and",
  "or",
  "empty and",
] as const;
type Shape = (typeof shapes)[number];
const collations = [null, "C", "default", "de-DE"] as const;
type History = [
  number,
  Shape,
  number,
  boolean,
  boolean,
  boolean,
  boolean,
  (typeof collations)[number],
];
const examples: History[] = fixtures.flatMap((_, index) =>
  shapes.flatMap((shape) =>
    [false, true].map((nullable): History => [index, shape, 1, nullable, false, false, false, "C"]),
  ),
);
const text = fixtures.findIndex((fixture) => fixture.name === "text");
examples.push(
  [text, "compare", 1, false, false, false, false, "de-DE"],
  [text, "in", 1, false, false, false, false, "de-DE"],
  [0, "compare", 1, false, true, false, false, "C"],
  [0, "compare", 1, false, false, true, false, "C"],
  [0, "compare", 1, false, false, false, true, "C"],
);

function filter(
  shape: Shape,
  column: ScalarExpr,
  value: ScalarExpr,
  collation: string | null,
): ScalarExpr {
  const compare: ScalarExpr = {
      op: "compare",
      cmp: "<",
      left: column,
      right: value,
      collation,
      type: BOOL,
    },
    residual = cmp(
      "=",
      { op: "call", fn: "add", args: [column, column], type: column.type },
      value,
    ),
    nulls: ScalarExpr = { op: "isNull", arg: column, negated: false, type: BOOL };
  switch (shape) {
    case "compare":
      return compare;
    case "flipped":
      return { op: "compare", cmp: ">", left: value, right: column, collation, type: BOOL };
    case "in":
    case "negated in":
      return {
        op: "inList",
        arg: column,
        list: [value],
        negated: shape === "negated in",
        collation,
        type: BOOL,
      };
    case "is null":
      return nulls;
    case "parameter":
      return cmp("=", column, { op: "param", slot: 0, type: column.type });
    case "and":
      return { op: "and", args: [compare, { op: "and", args: [nulls], type: BOOL }], type: BOOL };
    case "mixed and":
      return { op: "and", args: [compare, residual], type: BOOL };
    case "or":
      return { op: "or", args: [compare, nulls], type: BOOL };
    case "empty and":
      return { op: "and", args: [], type: BOOL };
  }
}
it.each(seedsFor("next-pushdown-page-eligibility", [0xb4ff, 0xb500]))(
  "classifies residuals without reading or copying literal payloads (seed %i)",
  (seed) => {
    const reach = createReach("page filter eligibility", PUSHDOWN_ELIGIBILITY_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: fixtures.length - 1 }),
        fc.constantFrom(...shapes),
        fc.integer(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.constantFrom(...collations),
        (index, shape, salt, nullable, empty, mismatch, missing, collation) => {
          reach.beginHistory();
          const fixture = fixtures[index];
          if (!fixture) throw new Error("Missing literal fixture");
          let value = fixture.vector(salt);
          if (nullable) value = { ...value, validity: new Uint32Array([0]) };
          if (empty) value = { ...value, length: 0 };
          if (mismatch)
            value = {
              ...value,
              data: numbers(fixture.type.kind === "bool" ? I32 : BOOL, [1]).data,
            };
          let reads = 0;
          for (const key of Object.keys(value.data)) {
            if (key === "layout") continue;
            const payload: unknown = Reflect.get(value.data, key);
            Object.defineProperty(value.data, key, {
              configurable: true,
              enumerable: true,
              get() {
                reads++;
                return payload;
              },
            });
          }
          const expr = filter(shape, col(missing ? 1 : 0, fixture.type), lit(value), collation),
            actual = hasResidualFilter(expr, [fixture.type]);
          expect(reads, "eligibility literal payload reads").toBe(0);
          expect(actual).toBe(splitFilter(expr, [fixture.type]).residual !== null);
          reach.reach(fixture.name);
          reach.reach(shape);
          reach.reach(nullable ? "null literal" : "nonnull literal");
          if (empty) reach.reach("empty literal");
          if (mismatch) reach.reach("mismatched layout");
          if (missing && shape !== "empty and") {
            expect(actual).toBe(true);
            reach.reach("missing column");
          }
          if (fixture.name === "text" && !empty && !mismatch && !missing) {
            if (collation === "de-DE" && (shape === "compare" || shape === "in")) {
              expect(actual).toBe(true);
              reach.reach(shape === "compare" ? "text compare nonC" : "text IN nonC");
            }
            if (nullable && shape === "in" && collation === "C") {
              expect(actual).toBe(false);
              reach.reach("null IN member accepted");
            }
          }
          if (nullable && shape === "compare") {
            expect(actual).toBe(true);
            reach.reach("null compare residual");
          }
          reach.reach(collation === "de-DE" ? "non C collation" : "C collation");
          reach.reach(actual ? "residual filter" : "accepted pushdown");
          reach.reach("no payload reads");
        },
      ),
      {
        seed,
        // Keep all 330 random histories when explicit reach examples grow.
        numRuns: examples.length + 330,
        examples,
      },
    );
    expect(hasResidualFilter(null, [])).toBe(false);
    reach.assertReached();
  },
);
