/**
 * The expression kernels against PostgreSQL itself (PGlite): generated cases are judged by
 * PostgreSQL's answer, never by this module. Each case is a small batch evaluated both ways:
 * here over typed vectors, there as `select (expr)::text from (values …)`.
 *
 * Generator dimensions (review B4-p2a-1, lead rulings):
 * - types: SMALLINT, INTEGER, BIGINT (incl. values past 2^53 in the int64 representation),
 *   DOUBLE PRECISION, REAL, NUMERIC (every coefficient tier, scales 0–20);
 * - operations: + − × ÷ %, unary minus, comparisons, casts (NUMERIC ↔ float, float/numeric →
 *   integer, NUMERIC(p, s) typmods including negative scales);
 * - representations: each operand flat, dictionary-encoded or constant;
 * - NULLs with garbage under them (NaN, huge values, zero divisors);
 * - errors: the SQLSTATE must match (the first failing row decides, as PostgreSQL's row order);
 * - batches of 40 NUMERIC rows over many (scale, scale) pairs (more than 32 per batch);
 * - CASE with up to 300 branches and COALESCE over text and INTERVAL results, intervals with
 *   microseconds past 2^53 (the int64 representation).
 * Declared reach (testing/reach.ts): each seed's run must enter the states its laws depend on.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { ScalarExpr } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import type { FlatVector, Vector } from "../../vector/vector.js";
import {
  F32,
  F64,
  I16,
  I32,
  I64,
  TEXT,
  batchOf,
  bigints,
  call,
  cmp,
  col,
  compileFor,
  decimal,
  decimals,
  dictionary,
  evaluate,
  lit,
  numbers,
  texts,
  valueAt,
  sqlLiteral,
} from "./test-support.js";

const INTERVAL: PhysicalType = { kind: "interval" };

const SEEDS = seedsFor("next-kernels-pglite", [0x9617, 0x2b]);
let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});

type Value = number | bigint | readonly [bigint, number] | "NaN" | "Infinity" | "-Infinity" | null;
type Kind = "int2" | "int4" | "int8" | "float8" | "float4" | "numeric";
const TYPES: Record<Kind, PhysicalType> = {
  int2: I16,
  int4: I32,
  int8: I64,
  float8: F64,
  float4: F32,
  numeric: decimal(null, null),
};

/** A SQL literal of `value` as PostgreSQL type `kind` (the shared, always-quoted form). */
function literal(kind: Kind, value: Value): string {
  return sqlLiteral(kind, value);
}

/** A vector of `values` as `kind`, with garbage under NULLs. */
function vectorOf(kind: Kind, values: readonly Value[], garbage: number): FlatVector {
  const type = TYPES[kind];
  if (kind === "numeric") {
    const fits = (tier: number) =>
      values.every(
        (v) =>
          !Array.isArray(v) ||
          ((v[0] as bigint) >= -(2n ** BigInt(tier - 1)) &&
            (v[0] as bigint) < 2n ** BigInt(tier - 1)),
      );
    const safe = values.every((v) => !Array.isArray(v) || Math.abs(Number(v[0])) <= 2 ** 53 - 1);
    const tier = safe ? "safe" : fits(64) ? "int64" : fits(128) ? "int128" : "wide";
    const vector = decimals(type, values as never, tier);
    const data = vector.data;
    if (data.layout === "decimal" && data.coefficients.repr === "safe")
      values.forEach((v, row) => {
        if (v === null) (data.coefficients as { values: Float64Array }).values[row] = garbage;
      });
    return vector;
  }
  if (
    kind === "int8" &&
    values.some((v) => typeof v === "bigint" && (v > 2n ** 53n - 1n || v < -(2n ** 53n - 1n)))
  )
    return bigints(
      type,
      values.map((v) => (v === null ? null : typeof v === "bigint" ? v : BigInt(v as number))),
    );
  const numberValues = values.map((v) =>
    v === null ? null : typeof v === "bigint" ? Number(v) : (v as number),
  );
  const isInteger = kind === "int2" || kind === "int4" || kind === "int8";
  return numbers(type, numberValues, isInteger && !Number.isFinite(garbage) ? 2147483647 : garbage);
}

/** One operand in one representation: flat, dictionary, or (all rows equal) constant. */
function represent(
  flat: FlatVector,
  values: readonly Value[],
  how: "flat" | "dictionary" | "constant",
): Vector {
  if (how === "constant")
    return { form: "constant", type: flat.type, length: values.length, value: flat };
  if (how === "flat") return flat;
  // Dictionary: entries are the distinct non-null values (first occurrence), codes per row.
  const key = (v: Value) =>
    Array.isArray(v)
      ? `${String(v[0])}e-${String(v[1])}`
      : typeof v === "number" && Object.is(v, -0)
        ? "-0"
        : String(v);
  const firsts: number[] = [];
  const seen = new Map<string, number>();
  const codes = values.map((v, row) => {
    if (v === null) return null;
    let code = seen.get(key(v));
    if (code === undefined) {
      code = firsts.length;
      seen.set(key(v), code);
      firsts.push(row);
    }
    return code;
  });
  const kind = (Object.entries(TYPES).find(([, t]) => t === flat.type)?.[0] ?? "int4") as Kind;
  const entries = vectorOf(
    kind,
    firsts.map((row) => values[row] ?? null),
    0,
  );
  return dictionary(entries, codes);
}

/** PostgreSQL's answer for each row, or the error's SQLSTATE. */
async function postgres(
  expr: string,
  columns: ReadonlyArray<readonly [Kind, readonly Value[]]>,
): Promise<string[] | { error: string }> {
  const rows = columns[0]?.[1].length ?? 0;
  const tuples = Array.from(
    { length: rows },
    (_, row) =>
      `(${String(row)}, ${columns.map(([kind, values]) => literal(kind, values[row] ?? null)).join(", ")})`,
  );
  const names = columns.map((_, c) => `c${String(c)}`).join(", ");
  try {
    const result = await pg.query<{ v: string | null }>(
      `select (${expr})::text as v from (values ${tuples.join(", ")}) t(r, ${names}) order by r`,
    );
    return result.rows.map((row) => row.v ?? "NULL");
  } catch (error) {
    return { error: (error as { code?: string }).code ?? String(error) };
  }
}

/** Our value as PostgreSQL text would read it: comparable strings per type. */
function normalize(kind: Kind | "bool", value: unknown): string {
  if (value === null) return "NULL";
  if (kind === "bool") return value === true ? "true" : "false";
  if (kind === "numeric") {
    if (typeof value === "string") return value;
    const [c, s] = value as [bigint, number];
    return `${c.toString()}/${String(s)}`;
  }
  if (kind === "float8" || kind === "float4") {
    const n = value as number;
    return Number.isNaN(n)
      ? "NaN"
      : Object.is(n, -0)
        ? "-0"
        : String(kind === "float4" ? Math.fround(n) : n);
  }
  return typeof value === "bigint" || typeof value === "number"
    ? String(value)
    : JSON.stringify(value);
}

/** PostgreSQL text into the same comparable string. */
function normalizePg(kind: Kind | "bool", text: string): string {
  if (text === "NULL") return text;
  if (kind === "bool") return text === "t" || text === "true" ? "true" : "false";
  if (kind === "numeric") {
    if (text === "NaN" || text === "Infinity" || text === "-Infinity") return text;
    const negative = text.startsWith("-");
    const [whole = "0", fraction = ""] = (negative ? text.slice(1) : text).split(".");
    const c = BigInt(whole + fraction);
    return `${(negative ? -c : c).toString()}/${String(fraction.length)}`;
  }
  if (kind === "float8" || kind === "float4") {
    if (text === "NaN") return "NaN";
    const n = Number(text);
    return Object.is(n, -0) ? "-0" : String(kind === "float4" ? Math.fround(n) : n);
  }
  return text;
}

async function judge(
  local: ScalarExpr,
  sql: string,
  resultKind: Kind | "bool",
  columns: ReadonlyArray<readonly [Kind, readonly Value[]]>,
  reps: ReadonlyArray<"flat" | "dictionary" | "constant">,
  garbage: number,
): Promise<void> {
  const vectors = columns.map(([kind, values], c) =>
    represent(vectorOf(kind, values, garbage), values, reps[c] ?? "flat"),
  );
  let ours: string[] | { error: string };
  try {
    ours = evaluate(local, batchOf(vectors)).map((v) => normalize(resultKind, v));
  } catch (error) {
    ours = { error: (error as { sqlState?: string }).sqlState ?? `local ${String(error)}` };
  }
  const theirs = await postgres(sql, columns);
  const expected = "error" in theirs ? theirs : theirs.map((text) => normalizePg(resultKind, text));
  expect(
    ours,
    `${sql} over ${JSON.stringify(columns, (_, v: unknown) => (typeof v === "bigint" ? `${v.toString()}n` : v))} as ${reps.join("/")}`,
  ).toEqual(expected);
}

const rep = fc.constantFrom("flat" as const, "dictionary" as const, "constant" as const);
const garbageArb = fc.constantFrom(0, NaN, 2 ** 52, -1, 2147483647, 1e300);

function withNulls<T>(value: fc.Arbitrary<T>): fc.Arbitrary<T | null> {
  return fc.oneof({ weight: 5, arbitrary: value }, { weight: 1, arbitrary: fc.constant(null) });
}

/** `rows` values of one arbitrary; a constant representation repeats one value. */
function columnArb(
  value: fc.Arbitrary<Value>,
  rows: number,
  how: "flat" | "dictionary" | "constant",
): fc.Arbitrary<Value[]> {
  return how === "constant"
    ? value.map((v) => Array.from({ length: rows }, () => v))
    : fc.array(value, { minLength: rows, maxLength: rows });
}

const intValue = (bits: 16 | 32 | 64): fc.Arbitrary<Value> => {
  const max = 2n ** BigInt(bits - 1) - 1n;
  const small = fc.integer({ min: -100, max: 100 });
  if (bits === 64)
    return withNulls(
      fc.oneof(
        small.map((v) => BigInt(v)),
        fc.bigInt({ min: -max - 1n, max }),
        fc.bigInt({ min: -(2n ** 53n), max: 2n ** 53n }),
        fc.constantFrom(-max - 1n, max, 2n ** 53n, -(2n ** 53n), 0n),
      ),
    );
  return withNulls(
    fc.oneof(
      small,
      fc.integer({ min: Number(-max - 1n), max: Number(max) }),
      fc.constantFrom(Number(max), Number(-max - 1n), 0, -1),
    ),
  );
};

const floatValue = (real: boolean): fc.Arbitrary<Value> =>
  withNulls(
    fc
      .oneof(
        fc.double({ noNaN: true, min: -1e6, max: 1e6 }),
        fc.constantFrom(
          0,
          -0,
          1,
          -1,
          1e-30,
          1e-20,
          1e30,
          3e38,
          1e300,
          1e-300,
          Infinity,
          -Infinity,
          NaN,
          0.1,
        ),
        fc.double(),
      )
      .map((v) => (real ? Math.fround(v) : v)),
  );

const numericValue: fc.Arbitrary<Value> = withNulls(
  fc.oneof(
    {
      weight: 6,
      arbitrary: fc.tuple(
        fc.bigInt({ min: -(10n ** 12n), max: 10n ** 12n }),
        fc.integer({ min: 0, max: 6 }),
      ),
    },
    {
      weight: 3,
      arbitrary: fc.tuple(
        fc.bigInt({ min: -(10n ** 38n), max: 10n ** 38n }),
        fc.integer({ min: 0, max: 20 }),
      ),
    },
    {
      weight: 1,
      arbitrary: fc.tuple(
        fc.bigInt({ min: -(10n ** 45n), max: 10n ** 45n }),
        fc.integer({ min: 0, max: 10 }),
      ),
    },
    {
      weight: 1,
      arbitrary: fc.constantFrom("NaN" as const, "Infinity" as const, "-Infinity" as const),
    },
  ),
);

const SYMBOLS = { add: "+", sub: "-", mul: "*", div: "/", mod: "%" } as const;

describe("kernels against PostgreSQL (PGlite, generated)", () => {
  for (const [kind, bits] of [
    ["int2", 16],
    ["int4", 32],
    ["int8", 64],
  ] as const)
    it.each(SEEDS)(
      `${kind} arithmetic, any representation, NULL garbage (seed %i)`,
      async (seed) => {
        await fc.assert(
          fc.asyncProperty(
            fc.constantFrom("add", "sub", "mul", "div", "mod", "neg"),
            fc
              .tuple(rep, rep, fc.integer({ min: 1, max: 4 }))
              .chain(([repA, repB, rows]) =>
                fc.tuple(
                  fc.constant([repA, repB] as const),
                  columnArb(intValue(bits), rows, repA),
                  columnArb(intValue(bits), rows, repB),
                ),
              ),
            garbageArb,
            async (op, [[repA, repB], a, b], garbage) => {
              const type = TYPES[kind];
              if (op === "neg")
                return judge(
                  call("neg", type, col(0, type)),
                  "-c0",
                  kind,
                  [[kind, a]],
                  [repA],
                  garbage,
                );
              return judge(
                call(op, type, col(0, type), col(1, type)),
                `c0 ${SYMBOLS[op]} c1`,
                kind,
                [
                  [kind, a],
                  [kind, b],
                ],
                [repA, repB],
                garbage,
              );
            },
          ),
          { seed, numRuns: 120 },
        );
      },
    );

  for (const kind of ["float8", "float4"] as const)
    it.each(SEEDS)(`${kind} arithmetic and comparison (seed %i)`, async (seed) => {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("add", "sub", "mul", "div", "neg", "<", "=", ">="),
          rep,
          rep,
          fc.tuple(
            columnArb(floatValue(kind === "float4"), 3, "flat"),
            columnArb(floatValue(kind === "float4"), 3, "flat"),
          ),
          garbageArb,
          async (op, repA, repB, [a0, b0], garbage) => {
            const a = repA === "constant" ? a0.map(() => a0[0] ?? null) : a0;
            const b = repB === "constant" ? b0.map(() => b0[0] ?? null) : b0;
            const type = TYPES[kind];
            if (op === "neg")
              return judge(
                call("neg", type, col(0, type)),
                "-c0",
                kind,
                [[kind, a]],
                [repA],
                garbage,
              );
            if (op === "<" || op === "=" || op === ">=")
              return judge(
                cmp(op, col(0, type), col(1, type)),
                `c0 ${op} c1`,
                "bool",
                [
                  [kind, a],
                  [kind, b],
                ],
                [repA, repB],
                garbage,
              );
            return judge(
              call(op, type, col(0, type), col(1, type)),
              `c0 ${SYMBOLS[op]} c1`,
              kind,
              [
                [kind, a],
                [kind, b],
              ],
              [repA, repB],
              garbage,
            );
          },
        ),
        { seed, numRuns: 150 },
      );
    });

  it.each(SEEDS)(
    "numeric arithmetic (incl. / and %), comparison and negation over every tier (seed %i)",
    async (seed) => {
      const D = TYPES.numeric;
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("add", "sub", "mul", "div", "mod", "neg", "<", "="),
          rep,
          rep,
          fc.tuple(
            fc.array(numericValue, { minLength: 3, maxLength: 3 }),
            fc.array(numericValue, { minLength: 3, maxLength: 3 }),
          ),
          garbageArb,
          async (op, repA, repB, [a0, b0], garbage) => {
            const a = repA === "constant" ? a0.map(() => a0[0] ?? null) : a0;
            const b = repB === "constant" ? b0.map(() => b0[0] ?? null) : b0;
            if (op === "neg")
              return judge(
                call("neg", D, col(0, D)),
                "-c0",
                "numeric",
                [["numeric", a]],
                [repA],
                garbage,
              );
            if (op === "<" || op === "=")
              return judge(
                cmp(op, col(0, D), col(1, D)),
                `c0 ${op} c1`,
                "bool",
                [
                  ["numeric", a],
                  ["numeric", b],
                ],
                [repA, repB],
                garbage,
              );
            return judge(
              call(op, D, col(0, D), col(1, D)),
              `c0 ${SYMBOLS[op]} c1`,
              "numeric",
              [
                ["numeric", a],
                ["numeric", b],
              ],
              [repA, repB],
              garbage,
            );
          },
        ),
        { seed, numRuns: 150 },
      );
    },
  );

  it.each(SEEDS)(
    "casts: numeric ↔ float, to integers, numeric typmods with negative scales (seed %i)",
    async (seed) => {
      const targets: ReadonlyArray<readonly [string, PhysicalType, Kind | "numeric"]> = [
        ["float8", F64, "float8"],
        ["real", F32, "float4"],
        ["int4", I32, "int4"],
        ["int8", I64, "int8"],
        ["numeric(10, 2)", decimal(10, 2), "numeric"],
        ["numeric(5, -2)", decimal(5, -2), "numeric"],
        ["numeric(3, 3)", decimal(3, 3), "numeric"],
        ["numeric(20, 0)", decimal(20, 0), "numeric"],
      ];
      await fc.assert(
        fc.asyncProperty(
          fc
            .tuple(
              fc.constantFrom(
                "numeric" as const,
                "float8" as const,
                "float4" as const,
                "int4" as const,
              ),
              rep,
            )
            .chain(([from, how]) =>
              fc.tuple(
                fc.constant([from, how] as const),
                columnArb(
                  from === "numeric"
                    ? numericValue
                    : from === "int4"
                      ? intValue(32)
                      : floatValue(from === "float4"),
                  3,
                  how,
                ),
              ),
            ),
          fc.constantFrom(...targets),
          garbageArb,
          async ([[from, how], values], [sqlType, type, resultKind], garbage) => {
            const source = TYPES[from];
            return judge(
              { op: "cast", input: col(0, source), type },
              `c0::${sqlType}`,
              resultKind,
              [[from, values]],
              [how],
              garbage,
            );
          },
        ),
        { seed, numRuns: 200 },
      );
    },
  );

  it("a CASE with 300 text branches", async () => {
    const n = 300;
    const branches = Array.from({ length: n }, (_, k) => ({
      when: cmp("=", col(0, I32), lit(numbers(I32, [k]))),
      then: lit(texts([`v${String(k)}`])),
    }));
    const values = Array.from({ length: n }, (_, k) => (k * 7) % (n + 5));
    const local = evaluate(
      { op: "case", type: TEXT, branches, otherwise: null },
      batchOf([numbers(I32, values)]),
    );
    const sql = `case ${Array.from({ length: n }, (_, k) => `when c0 = ${String(k)} then 'v${String(k)}'`).join(" ")} end`;
    const theirs = await postgres(sql, [["int4", values]]);
    expect(local.map((v) => (v === null ? "NULL" : v))).toEqual(theirs);
  });

  it.each(SEEDS)(
    "numeric arithmetic over 40-row batches with many scale pairs (seed %i)",
    async (seed) => {
      const D = TYPES.numeric;
      const reach = createReach("next-kernels-numeric-scale-pairs", [
        "more than 32 scale pairs",
        "int64 tier",
        "int128 tier",
      ] as const);
      // One coefficient bound per case (so every non-wide tier is reached), scales 0–12
      // independently per side and row.
      const column = (bound: bigint) =>
        fc.array(
          withNulls(
            fc.tuple(fc.bigInt({ min: -bound, max: bound }), fc.integer({ min: 0, max: 12 })),
          ),
          { minLength: 40, maxLength: 40 },
        );
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("add", "sub", "mul"),
          fc
            .constantFrom(10n ** 9n, 2n ** 62n, 2n ** 100n)
            .chain((bound) => fc.tuple(column(bound), column(bound))),
          async (op, [a, b]) => {
            reach.beginHistory();
            const pairs = new Set(
              a.map((x, k) => {
                const y = b[k];
                return `${Array.isArray(x) ? String(x[1]) : "-"}/${Array.isArray(y) ? String(y[1]) : "-"}`;
              }),
            );
            if (pairs.size > 32) reach.reach("more than 32 scale pairs");
            const width = (xs: readonly Value[]) =>
              xs.reduce<number>(
                (m, x) => (Array.isArray(x) ? Math.max(m, (x[0] as bigint).toString(2).length) : m),
                0,
              );
            const widest = Math.max(width(a), width(b));
            if (widest > 53 && widest <= 63) reach.reach("int64 tier");
            if (widest > 63) reach.reach("int128 tier");
            return judge(
              call(op, D, col(0, D), col(1, D)),
              `c0 ${SYMBOLS[op]} c1`,
              "numeric",
              [
                ["numeric", a],
                ["numeric", b],
              ],
              ["flat", "flat"],
              0,
            );
          },
        ),
        { seed, numRuns: 60 },
      );
      reach.assertReached();
    },
    120_000,
  );

  it.each(SEEDS)(
    "CASE (up to 300 branches) and COALESCE over text and wide intervals (seed %i)",
    async (seed) => {
      const reach = createReach("next-kernels-case-interval", [
        "more than 256 branches",
        "interval result",
        "interval microseconds past 2^53",
        "coalesce",
      ] as const);
      const microsArb = fc.oneof(
        fc.bigInt({ min: -(10n ** 9n), max: 10n ** 9n }),
        fc.bigInt({ min: -(2n ** 62n), max: 2n ** 62n }),
        fc.bigInt({ min: 2n ** 53n - 3n, max: 2n ** 53n + 3n }),
      );
      const intervalArb = withNulls(
        fc.tuple(
          fc.integer({ min: -1000, max: 1000 }),
          fc.integer({ min: -1000, max: 1000 }),
          microsArb,
        ),
      );
      type Interval = readonly [number, number, bigint] | null;
      const intervalSql = (v: Interval): string => {
        if (v === null) return "null::interval";
        const [months, days, micros] = v;
        const hours = micros / 3_600_000_000n;
        const rest = micros - hours * 3_600_000_000n;
        return `(interval '${String(months)} months' + interval '${String(days)} days' + interval '${hours.toString()} hours' + interval '${rest.toString()} microseconds')`;
      };
      const intervalVector = (values: readonly Interval[]): FlatVector => {
        const validity = new Uint32Array((values.length + 31) >>> 5);
        values.forEach((v, row) => {
          if (v !== null) validity[row >>> 5] = (validity[row >>> 5] ?? 0) | (1 << (row & 31));
        });
        return {
          form: "flat",
          type: INTERVAL,
          length: values.length,
          validity: values.some((v) => v === null) ? validity : null,
          data: {
            layout: "interval",
            months: Int32Array.from(values, (v) => v?.[0] ?? 0),
            days: Int32Array.from(values, (v) => v?.[1] ?? 0),
            micros: { repr: "int64", values: BigInt64Array.from(values, (v) => v?.[2] ?? 0n) },
          },
        };
      };
      /** Row `row` of an interval vector as "months/days/micros" (or NULL). */
      const intervalAt = (vector: Vector, row: number): string => {
        let flat: FlatVector;
        let slot = row;
        if (vector.form === "constant") {
          flat = vector.value;
          slot = 0;
        } else if (vector.form === "dictionary") {
          if (
            vector.validity !== null &&
            ((vector.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0
          )
            return "NULL";
          flat = vector.dictionary;
          slot = vector.codes[row] ?? 0;
        } else flat = vector;
        if (flat.validity !== null && ((flat.validity[slot >>> 5] ?? 0) & (1 << (slot & 31))) === 0)
          return "NULL";
        if (flat.data.layout !== "interval") throw new TypeError("interval expected");
        const micros = flat.data.micros;
        const u =
          micros.repr === "safe" ? BigInt(micros.values[slot] ?? 0) : (micros.values[slot] ?? 0n);
        return `${String(flat.data.months[slot] ?? 0)}/${String(flat.data.days[slot] ?? 0)}/${u.toString()}`;
      };
      await fc.assert(
        fc.asyncProperty(
          fc.oneof(fc.integer({ min: 1, max: 8 }), fc.integer({ min: 257, max: 300 })),
          fc.boolean(),
          fc.integer({ min: 1, max: 12 }),
          fc.boolean(),
          fc.infiniteStream(intervalArb),
          fc.array(fc.nat(320), { minLength: 12, maxLength: 12 }),
          async (n, interval, rows, coalesce, stream, keys) => {
            reach.beginHistory();
            if (n > 256) reach.reach("more than 256 branches");
            const take = (count: number): Interval[] =>
              Array.from({ length: count }, () => stream.next().value as Interval);
            const selector = keys.slice(0, rows);
            const otherwise = take(rows);
            const results = take(n);
            const wide = (v: Interval) =>
              v !== null && (v[2] > 2n ** 53n - 1n || v[2] < -(2n ** 53n - 1n));
            if (interval) {
              reach.reach("interval result");
              if (results.some(wide) || otherwise.some(wide))
                reach.reach("interval microseconds past 2^53");
            }
            if (coalesce) reach.reach("coalesce");
            const type = interval ? INTERVAL : TEXT;
            const resultLit = (k: number): ScalarExpr =>
              interval
                ? lit(intervalVector([results[k] ?? null]))
                : lit(texts([results[k] === null ? null : `v${String(k)}`]));
            const resultSql = (k: number): string =>
              interval
                ? intervalSql(results[k] ?? null)
                : results[k] === null
                  ? "null::text"
                  : `'v${String(k)}'`;
            const other: FlatVector = interval
              ? intervalVector(otherwise)
              : texts(otherwise.map((v, row) => (v === null ? null : `o${String(row)}`)));
            const caseExpr: ScalarExpr = {
              op: "case",
              type,
              branches: Array.from({ length: n }, (_, k) => ({
                when: cmp("=", col(0, I32), lit(numbers(I32, [k]))),
                then: resultLit(k),
              })),
              otherwise: col(1, type),
            };
            const local: ScalarExpr = coalesce
              ? call("coalesce", type, caseExpr, col(1, type))
              : caseExpr;
            const out = compileFor(local, [I32, type]).evaluate(
              batchOf([numbers(I32, selector), other]),
              null,
            );
            const ours = selector.map((_, row) =>
              interval ? intervalAt(out, row) : ((valueAt(out, row) as string | null) ?? "NULL"),
            );
            const otherSql = (row: number): string =>
              interval
                ? intervalSql(otherwise[row] ?? null)
                : otherwise[row] === null
                  ? "null::text"
                  : `'o${String(row)}'`;
            const caseSql = `case ${Array.from({ length: n }, (_, k) => `when c0 = ${String(k)} then ${resultSql(k)}`).join(" ")} else c1 end`;
            const expr = coalesce ? `coalesce(${caseSql}, c1)` : caseSql;
            const shown = interval
              ? `case when r is null then 'NULL' else (extract(year from r) * 12 + extract(month from r))::int8::text || '/' || extract(day from r)::int8::text || '/' || (extract(hour from r) * 3600000000 + extract(minute from r) * 60000000 + extract(microseconds from r))::int8::text end`
              : "coalesce(r, 'NULL')";
            const tuples = selector
              .map((key, row) => `(${String(row)}, ${String(key)}, ${otherSql(row)})`)
              .join(", ");
            const result = await pg.query<{ v: string }>(
              `select ${shown} as v from (select o, ${expr} as r from (values ${tuples}) t(o, c0, c1)) q order by o`,
            );
            expect(ours, `${String(n)} branches, ${interval ? "interval" : "text"}`).toEqual(
              result.rows.map((row) => row.v),
            );
          },
        ),
        { seed, numRuns: 40 },
      );
      reach.assertReached();
    },
    180_000,
  );
});
