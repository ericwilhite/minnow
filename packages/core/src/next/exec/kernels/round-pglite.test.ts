/** numeric_round's value, display scale, limits and strictness, against PostgreSQL itself. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, NUMERIC_ROUND_REACH } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import { bigintBytes } from "../../memory/governor.js";
import type { Vector } from "../../vector/vector.js";
import { compileExpr } from "./compile.js";
import { OutputArena } from "./vectors.js";
import {
  I32,
  batchOf,
  call,
  col,
  constant,
  decimal,
  decimals,
  dictionary,
  numbers,
  valueAt,
  type DecimalValue,
} from "./test-support.js";

const TYPE = decimal(null, null);
const FORMS = ["flat", "dictionary", "constant"] as const;
const PRECISIONS = [15, 18, 38, 1000] as const;
const EXTREMES = [
  -2147483648, -131074, -131073, -131072, -1001, 1001, 16382, 16383, 16384, 2147483647,
];
let pg: PGlite;
let uniformExpected: Array<string | null>;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  uniformExpected = (
    await pg.query<{ v: string | null }>(
      "SELECT round(1.25::numeric,1)::text AS v UNION ALL SELECT round(-1.25::numeric,1)::text",
    )
  ).rows.map((row) => row.v);
}, 60_000);
afterAll(async () => {
  await pg.close();
});

interface Shape {
  precision: number;
  source: bigint;
  inputScale: number;
  scale: number;
  form: number;
  scaleForm: number;
  selected: boolean;
  negative: boolean;
  safeBoundary?: boolean;
}
function present<T>(value: T | undefined): T {
  if (value === undefined) throw new TypeError("Missing ROUND test dimension");
  return value;
}
function numericText(value: unknown): string | null {
  if (value === null || typeof value === "string") return value;
  if (!Array.isArray(value) || typeof value[0] !== "bigint" || typeof value[1] !== "number")
    throw new TypeError("Expected decimal tuple");
  const c = value[0],
    s = value[1];
  const digits = (c < 0n ? -c : c).toString().padStart(s + 1, "0");
  const point = digits.length - s;
  return (
    (c < 0n ? "-" : "") + (s === 0 ? digits : `${digits.slice(0, point)}.${digits.slice(point)}`)
  );
}
function valueVector(
  values: readonly DecimalValue[],
  tier: "safe" | "int64" | "int128" | "wide",
  form: number,
): Vector {
  if (form === 2) return constant(decimals(TYPE, [present(values[0])], tier), values.length);
  if (form === 1) {
    const entries = values.filter((value): value is Exclude<DecimalValue, null> => value !== null);
    let code = 0;
    const codes = values.map((value) => (value === null ? null : entries.length - 1 - code++));
    return dictionary(decimals(TYPE, entries.reverse(), tier), codes);
  }
  const vector = decimals(TYPE, values, tier);
  // Poison a NULL slot: validation and arithmetic must never read it.
  if (vector.data.layout === "decimal" && vector.data.scales && values[4] === null)
    vector.data.scales[4] = 65535;
  return vector;
}
function scaleVector(values: ReadonlyArray<number | null>, form: number): Vector {
  if (form === 2) return constant(numbers(I32, [values[0] ?? null]), values.length);
  if (form === 1) {
    const entries = values.filter((value): value is number => value !== null);
    let code = 0;
    return dictionary(
      numbers(I32, entries.reverse()),
      values.map((value) => (value === null ? null : entries.length - 1 - code++)),
    );
  }
  return numbers(I32, values);
}

async function check(shape: Shape, reach: ReturnType<typeof createReach<string>>): Promise<void> {
  const tier =
    shape.precision <= 15
      ? "safe"
      : shape.precision <= 18
        ? "int64"
        : shape.precision <= 38
          ? "int128"
          : "wide";
  const magnitude =
    shape.safeBoundary && tier === "safe"
      ? 9007199254740991n - (shape.source % 100n)
      : shape.source % 10n ** BigInt(shape.precision);
  const c = magnitude * (shape.negative ? -1n : 1n);
  const tieScale = shape.scale >= -5 && shape.scale <= 20 ? shape.scale : 0;
  const tie: readonly [bigint, number] =
    tieScale < 0 ? [25n * 10n ** BigInt(-tieScale - 1), 0] : [25n, tieScale + 1];
  const values: DecimalValue[] = [
    [c, shape.inputScale],
    [-c, shape.inputScale],
    tie,
    [-tie[0], tie[1]],
    null,
    "NaN",
    "Infinity",
    "-Infinity",
    [0n, 0],
    [0n, 0],
  ];
  const scales: Array<number | null> = values.map((_, index) =>
    index === 8 || (shape.negative && index === 5) ? null : shape.scale,
  );
  if (shape.selected) {
    values.push([1n, 65535]);
    scales.push(shape.scale);
  }
  const n = valueVector(values, tier, shape.form),
    s = scaleVector(scales, shape.scaleForm);
  const active = Array.from({ length: 10 }, (_, index) => index);
  const rows = active.map((row) => [numericText(valueAt(n, row)), valueAt(s, row)]);
  const sql = `SELECT round(n,s)::text AS v FROM (VALUES ${rows.map((_, index) => `($${String(2 * index + 1)}::numeric,$${String(2 * index + 2)}::int)`).join(",")}) AS t(n,s)`;
  let expected: Array<{ v: string | null }> | null = null,
    state: string | null = null;
  try {
    expected = (await pg.query<{ v: string | null }>(sql, rows.flat())).rows;
  } catch (error) {
    state = (error as { code: string }).code;
    expect(state).toBe("22003");
  }
  let used = 0;
  const arena = new OutputArena(
    {
      allocate: (bytes) => {
        used += bytes;
        return true;
      },
      free: (bytes) => {
        used -= bytes;
      },
    },
    values.length,
  );
  try {
    const compiled = compileExpr(call("round", TYPE, col(0, TYPE), col(1, I32)), {
      arena,
      input: [TYPE, I32],
      params: { slots: [] },
    });
    const baseline = arena.bytes;
    const batch = batchOf(
      [n, s],
      shape.selected ? { rows: Uint16Array.from(active), count: active.length } : null,
    );
    if (state !== null) {
      expect(() => compiled.evaluate(batch, batch.selection)).toThrow(
        expect.objectContaining({ sqlState: state }),
      );
      reach.reach("overflow");
    } else {
      const result = compiled.evaluate(batch, batch.selection);
      expect(active.map((row) => ({ v: numericText(valueAt(result, row)) }))).toEqual(expected);
      // Reuse the same kernel after a wide/high-scale batch: no stale values or retained charge.
      const small = batchOf([
        decimals(
          TYPE,
          [
            [125n, 2],
            [-125n, 2],
          ],
          "safe",
          2,
        ),
        numbers(I32, [1, 1]),
      ]);
      const reused = compiled.evaluate(small, null);
      expect([0, 1].map((row) => numericText(valueAt(reused, row)))).toEqual(uniformExpected);
      reach.reach("uniform input");
      expect(arena.bytes).toBe(baseline);
      reach.reach("reused output");
      if (shape.form !== 2) {
        for (const special of ["Infinity", "-Infinity"]) reach.reach(special);
        if (shape.negative && shape.scaleForm !== 2) reach.reach("special NULL wins");
        else reach.reach("NaN");
        reach.reach("null input");
        reach.reach("positive");
        reach.reach("negative");
        if (shape.form === 0) reach.reach("null garbage");
        if (shape.scale >= -5 && shape.scale <= 20) {
          reach.reach("positive tie");
          reach.reach("negative tie");
        }
      }
      if (shape.scaleForm !== 2) reach.reach("null scale");
      if (shape.selected && shape.form !== 2) reach.reach("unselected garbage");
    }
  } finally {
    arena.close();
    expect(used).toBe(0);
  }
  if (shape.safeBoundary && tier === "safe") {
    reach.reach("safe precision boundary");
    if (shape.inputScale - shape.scale === 16) reach.reach("sixteen-digit reduction");
  }
  reach.reach(tier);
  reach.reach(present(FORMS[shape.form]));
  reach.reach(shape.selected ? "selected" : "all rows");
  if (shape.scale >= -5 && shape.scale <= 20) reach.reach(`scale ${String(shape.scale)}`);
  if (shape.scale > 16383) reach.reach("high scale clamp");
  if (shape.scale < -131073) reach.reach("low scale clamp");
  if (shape.precision === 1000) reach.reach("1000-digit precision");
  if (shape.precision === 131072) reach.reach("largest integer precision");
}

it.each(seedsFor("next-numeric-round", [0xb605, 0xb606]))(
  "matches PostgreSQL numeric ROUND (seed %i)",
  async (seed) => {
    const reach = createReach<string>("numeric ROUND", NUMERIC_ROUND_REACH);
    const examples: Array<[Shape]> = [];
    for (let scale = -5; scale <= 20; scale++) {
      const i = scale + 5,
        precision = present(PRECISIONS[i % PRECISIONS.length]);
      examples.push([
        {
          precision,
          source: 10n ** BigInt(precision) - 1n,
          inputScale: i % 21,
          scale,
          form: i % 3,
          scaleForm: (i + 1) % 3,
          selected: i % 2 === 0,
          negative: i % 2 !== 0,
        },
      ]);
    }
    for (let scale = -5; scale <= 20; scale++)
      examples.push([
        {
          precision: 15,
          source: BigInt(scale + 5),
          inputScale: 2,
          scale,
          form: 0,
          scaleForm: 0,
          selected: true,
          negative: false,
          safeBoundary: true,
        },
      ]);
    for (const [inputScale, scale] of [
      [11, -5],
      [21, 5],
    ])
      examples.push([
        {
          precision: 15,
          source: 0n,
          inputScale: inputScale ?? 0,
          scale: scale ?? 0,
          form: 0,
          scaleForm: 0,
          selected: true,
          negative: false,
          safeBoundary: true,
        },
      ]);
    for (const scale of EXTREMES)
      examples.push([
        {
          precision: 38,
          source: 12345n,
          inputScale: 2,
          scale,
          form: 0,
          scaleForm: 0,
          selected: true,
          negative: false,
        },
      ]);
    for (const negative of [false, true])
      examples.push([
        {
          precision: 131072,
          source: 10n ** 131072n - 1n,
          inputScale: 0,
          scale: -1,
          form: 0,
          scaleForm: 0,
          selected: true,
          negative,
        },
      ]);
    examples.push([
      {
        precision: 131072,
        source: 10n ** 131071n,
        inputScale: 0,
        scale: -131073,
        form: 1,
        scaleForm: 0,
        selected: false,
        negative: false,
      },
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          precision: fc.constantFrom(...PRECISIONS),
          source: fc.bigInt({ min: 0n, max: 10n ** 1000n - 1n }),
          inputScale: fc.integer({ min: 0, max: 30 }),
          scale: fc.oneof(fc.integer({ min: -5, max: 20 }), fc.constantFrom(...EXTREMES)),
          form: fc.integer({ min: 0, max: 2 }),
          scaleForm: fc.integer({ min: 0, max: 2 }),
          selected: fc.boolean(),
          negative: fc.boolean(),
          safeBoundary: fc.boolean(),
        }),
        async (shape) => {
          reach.beginHistory();
          await check(shape, reach);
        },
      ),
      { seed, numRuns: 108, examples },
    );
    reach.assertReached();
  },
  300_000,
);

it("charges wide ROUND output beyond fixed scratch and releases it on reuse", async () => {
  const magnitude = 10n ** 20000n;
  const values: DecimalValue[] = Array.from(
    { length: 512 },
    (_, row) => [magnitude + BigInt(row * 10 + 5), 1] as const,
  );
  let used = 0;
  const arena = new OutputArena(
    {
      allocate: (bytes) => {
        if (used + bytes > 8 * 1024 * 1024) return false;
        used += bytes;
        return true;
      },
      free: (bytes) => {
        used -= bytes;
      },
    },
    values.length,
  );
  try {
    const compiled = compileExpr(call("round", TYPE, col(0, TYPE), col(1, I32)), {
      arena,
      input: [TYPE, I32],
      params: { slots: [] },
    });
    const fixed = arena.bytes;
    const batch = batchOf([
      decimals(TYPE, values, "wide", 1),
      constant(numbers(I32, [0]), values.length),
    ]);
    const result = compiled.evaluate(batch, null);
    const expected = await pg.query<{ v: string }>(
      "SELECT round($1::numeric,0)::text AS v UNION ALL SELECT round($2::numeric,0)::text",
      [numericText(values[0]), numericText(values[511])],
    );
    expect([0, 511].map((row) => numericText(valueAt(result, row)))).toEqual(
      expected.rows.map((row) => row.v),
    );
    if (
      result.form !== "flat" ||
      result.data.layout !== "decimal" ||
      result.data.coefficients.repr !== "wide"
    )
      throw new Error("Expected distinct wide coefficients");
    const live = result.data.coefficients.values.reduce(
      (sum, value) => sum + bigintBytes(value),
      0,
    );
    expect(live).toBeGreaterThan(fixed);
    expect(arena.bytes).toBeGreaterThanOrEqual(live);
    compiled.evaluate(batchOf([decimals(TYPE, [[1n, 0]]), numbers(I32, [0])]), null);
    expect(arena.bytes).toBe(fixed);
  } finally {
    arena.close();
    expect(used).toBe(0);
  }
}, 120_000);
