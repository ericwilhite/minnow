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
 * - errors: the SQLSTATE must match (the first failing row decides, as PostgreSQL's row order).
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
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
  decimal,
  decimals,
  dictionary,
  evaluate,
  lit,
  numbers,
  texts,
} from "./test-support.js";

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

/** A SQL literal of `value` as PostgreSQL type `kind`. */
function literal(kind: Kind, value: Value): string {
  if (value === null) return `null::${kind}`;
  if (Array.isArray(value)) {
    const [c, s] = value as readonly [bigint, number];
    const negative = c < 0n;
    const digits = (negative ? -c : c).toString().padStart(s + 1, "0");
    const text =
      s === 0 ? digits : `${digits.slice(0, digits.length - s)}.${digits.slice(digits.length - s)}`;
    return `'${negative ? "-" : ""}${text}'::numeric`;
  }
  if (typeof value === "number" && Object.is(value, -0)) return `'-0'::${kind}`;
  return `'${String(value)}'::${kind}`;
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
    "numeric arithmetic, comparison and negation over every tier (seed %i)",
    async (seed) => {
      const D = TYPES.numeric;
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("add", "sub", "mul", "neg", "<", "="),
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
});
