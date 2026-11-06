/**
 * Arithmetic kernels against an exact BigInt reference (generated), plus PostgreSQL's float and
 * decimal rules. Laws: PostgreSQL's integer, float8/float4 and NUMERIC operators
 * (src/backend/utils/adt/int.c, int8.c, float.c, numeric.c): ranges and messages, truncating
 * division, dividend-signed modulo, display scales, specials.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import type { PhysicalType } from "../../types.js";
import { DivisionByZeroError, NumericOutOfRangeError, UnsupportedPlanError } from "../errors.js";
import {
  F32,
  F64,
  I16,
  I32,
  I64,
  batchOf,
  bigints,
  call,
  col,
  compileFor,
  constant,
  decimal,
  decimalValue,
  decimals,
  dictionary,
  evaluate,
  lit,
  numbers,
  type DecimalValue,
} from "./test-support.js";

const SEEDS = seedsFor("next-kernels-arith", [0xa217, 0x51]);
const OPS = ["add", "sub", "mul", "div", "mod"] as const;
type Op = (typeof OPS)[number];

function reference(op: Op, x: bigint, y: bigint): bigint | "div0" {
  switch (op) {
    case "add":
      return x + y;
    case "sub":
      return x - y;
    case "mul":
      return x * y;
    case "div":
      return y === 0n ? "div0" : x / y;
    case "mod":
      return y === 0n ? "div0" : x % y;
  }
}

/** Expected outcome of `op` over rows: values, or the first error a valid row raises. */
function expectedRows(
  op: Op,
  rows: ReadonlyArray<readonly [bigint | null, bigint | null]>,
  min: bigint,
  max: bigint,
): Array<bigint | null> | "div0" | "range" {
  const out: Array<bigint | null> = [];
  for (const [x, y] of rows) {
    if (x === null || y === null) {
      out.push(null);
      continue;
    }
    const v = reference(op, x, y);
    // The first failing row, in row order, decides the error.
    if (v === "div0") return "div0";
    if (v < min || v > max) return "range";
    out.push(v);
  }
  return out;
}

function run(
  op: Op,
  type: PhysicalType,
  left: Parameters<typeof batchOf>[0][0],
  right: Parameters<typeof batchOf>[0][0],
) {
  return () => evaluate(call(op, type, col(0, type), col(1, type)), batchOf([left, right]));
}

describe("int32 and int16 arithmetic (generated against BigInt)", () => {
  for (const [type, min, max, message] of [
    [I32, -(2n ** 31n), 2n ** 31n - 1n, "integer out of range"],
    [I16, -(2n ** 15n), 2n ** 15n - 1n, "smallint out of range"],
  ] as const)
    it.each(SEEDS)(
      `${type.kind}(${String(type.kind === "int32" ? type.bits : 0)}) (seed %i)`,
      (seed) => {
        const value = fc.oneof(
          fc.constant(null),
          fc.integer({ min: Number(min), max: Number(max) }),
          fc.constantFrom(Number(min), Number(max), 0, -1, 1),
        );
        fc.assert(
          fc.property(
            fc.constantFrom(...OPS),
            fc.array(fc.tuple(value, value), { minLength: 1, maxLength: 40 }),
            (op, pairs) => {
              const left = numbers(
                type,
                pairs.map(([x]) => x),
                0,
              );
              const right = numbers(
                type,
                pairs.map(([, y]) => y),
                0,
              ); // garbage under null: zero divisors
              const expected = expectedRows(
                op,
                pairs.map(([x, y]) => [
                  x === null ? null : BigInt(x),
                  y === null ? null : BigInt(y),
                ]),
                min,
                max,
              );
              const evaluateIt = run(op, type, left, right);
              if (expected === "div0") expect(evaluateIt).toThrow(DivisionByZeroError);
              else if (expected === "range")
                expect(evaluateIt).toThrow(new NumericOutOfRangeError(message));
              else
                expect(evaluateIt().map((v) => (v === null ? null : BigInt(v as number)))).toEqual(
                  expected,
                );
            },
          ),
          { seed, numRuns: 1500 },
        );
      },
    );
});

describe("int64 arithmetic: exact in both representations (generated)", () => {
  const MIN = -(2n ** 63n);
  const MAX = 2n ** 63n - 1n;
  const SAFE = 2n ** 53n - 1n;
  it.each(SEEDS)("safe and int64 inputs give the BigInt result (seed %i)", (seed) => {
    const big = fc.oneof(
      fc.constant(null),
      fc.bigInt({ min: -SAFE, max: SAFE }),
      fc.bigInt({ min: -(2n ** 31n), max: 2n ** 31n }),
      fc.bigInt({ min: MIN, max: MAX }),
      fc.constantFrom(MIN, MAX, 0n, -1n, 1n, SAFE, -SAFE, SAFE + 1n),
    );
    fc.assert(
      fc.property(
        fc.constantFrom(...OPS),
        fc.array(fc.tuple(big, big), { minLength: 1, maxLength: 30 }),
        fc.boolean(),
        (op, pairs, asSafe) => {
          const allSafe = pairs.every(
            ([x, y]) =>
              (x === null || (x >= -SAFE && x <= SAFE)) &&
              (y === null || (y >= -SAFE && y <= SAFE)),
          );
          const vector = (values: Array<bigint | null>) =>
            asSafe && allSafe
              ? numbers(
                  I64,
                  values.map((v) => (v === null ? null : Number(v))),
                )
              : bigints(I64, values);
          const left = vector(pairs.map(([x]) => x));
          const right = vector(pairs.map(([, y]) => y));
          const expected = expectedRows(op, pairs, MIN, MAX);
          const evaluateIt = run(op, I64, left, right);
          if (expected === "div0") expect(evaluateIt).toThrow(DivisionByZeroError);
          else if (expected === "range")
            expect(evaluateIt).toThrow(new NumericOutOfRangeError("bigint out of range"));
          else expect(evaluateIt()).toEqual(expected);
        },
      ),
      { seed, numRuns: 1500 },
    );
  });

  it("falls back from the safe representation exactly past 2^53", () => {
    const left = numbers(I64, [2 ** 53 - 1, 3, -(2 ** 52)]);
    const right = numbers(I64, [2, 2 ** 52, -(2 ** 52)]);
    expect(evaluate(call("add", I64, col(0, I64), col(1, I64)), batchOf([left, right]))).toEqual([
      2n ** 53n + 1n,
      2n ** 52n + 3n,
      -(2n ** 53n),
    ]);
    // Row 2's product is 2^104: past int64.
    expect(() =>
      evaluate(call("mul", I64, col(0, I64), col(1, I64)), batchOf([left, right])),
    ).toThrow("bigint out of range");
  });

  it("never leaves −0 in the safe representation", () => {
    const left = numbers(I64, [-5, 0, -1]);
    const right = numbers(I64, [0, -3, 0]);
    const out = compileFor(call("mul", I64, col(0, I64), col(1, I64)), [I64, I64]).evaluate(
      batchOf([left, right]),
      null,
    );
    const data = out.form === "flat" ? out.data : null;
    if (data?.layout !== "int64" || data.values.repr !== "safe")
      throw new Error("safe int64 expected");
    expect([...data.values.values].every((value) => !Object.is(value, -0))).toBe(true);
  });

  it("raises nothing for garbage under null, and errors only for active rows", () => {
    const left = numbers(I32, [1, null, 7], 2 ** 31 - 1);
    const right = numbers(I32, [1, 1, 0]);
    // Row 2 (division by zero) is outside the selection; row 1's garbage would overflow.
    const selection = { rows: Uint16Array.of(0, 1), count: 2 };
    expect(
      evaluate(call("add", I32, col(0, I32), col(1, I32)), batchOf([left, right], selection)),
    ).toEqual([2, null]);
    expect(
      evaluate(call("div", I32, col(0, I32), col(1, I32)), batchOf([left, right], selection)),
    ).toEqual([1, null]);
  });

  it("reads constants and dictionary operands", () => {
    const values = numbers(I32, [10, 20, 30]);
    const dict = dictionary(values, [2, 0, null, 1]);
    const out = evaluate(call("mul", I32, col(0, I32), lit(numbers(I32, [3]))), batchOf([dict]));
    expect(out).toEqual([90, 30, null, 60]);
    expect(
      evaluate(
        call("sub", I32, lit(numbers(I32, [1])), col(0, I32)),
        batchOf([constant(numbers(I32, [null]), 2)]),
      ),
    ).toEqual([null, null]);
    expect(
      evaluate(call("neg", I32, col(0, I32)), batchOf([numbers(I32, [5, -(2 ** 31) + 1])])),
    ).toEqual([-5, 2 ** 31 - 1]);
    expect(() =>
      evaluate(call("neg", I32, col(0, I32)), batchOf([numbers(I32, [-(2 ** 31)])])),
    ).toThrow("integer out of range");
  });
});

describe("float arithmetic (PostgreSQL float8 / float4)", () => {
  const f = (op: Op, type: PhysicalType, x: number, y: number) =>
    evaluate(
      call(op, type, col(0, type), col(1, type)),
      batchOf([numbers(type, [x]), numbers(type, [y])]),
    )[0];
  it("computes IEEE results, keeping NaN and infinities from their inputs", () => {
    expect(f("add", F64, 0.1, 0.2)).toBe(0.1 + 0.2);
    expect(f("mul", F64, Infinity, 2)).toBe(Infinity);
    expect(Number.isNaN(f("sub", F64, Infinity, Infinity))).toBe(true);
    expect(f("div", F64, 1, Infinity)).toBe(0);
  });
  it("raises overflow, underflow and division by zero", () => {
    expect(() => f("mul", F64, 1e308, 10)).toThrow("value out of range: overflow");
    expect(() => f("add", F64, 1.7e308, 1.7e308)).toThrow("value out of range: overflow");
    expect(() => f("mul", F64, 1e-308, 1e-308)).toThrow("value out of range: underflow");
    expect(() => f("div", F64, 1, 0)).toThrow(DivisionByZeroError);
    expect(() => f("div", F64, 0, 0)).toThrow(DivisionByZeroError);
  });
  it("rounds REAL with Math.fround and checks the float4 range", () => {
    expect(f("add", F32, Math.fround(0.1), Math.fround(0.2))).toBe(
      Math.fround(Math.fround(0.1) + Math.fround(0.2)),
    );
    expect(() => f("mul", F32, 3e38, 10)).toThrow("value out of range: overflow");
  });
  it("refuses float modulo at compile time (no such overload in ir/functions.ts)", () => {
    expect(() => f("mod", F64, 1, 2)).toThrow(UnsupportedPlanError);
  });
});

describe("decimal arithmetic: exact, per-slot scales, every tier (generated)", () => {
  const coefficient = fc.oneof(
    fc.bigInt({ min: -(10n ** 12n), max: 10n ** 12n }),
    fc.bigInt({ min: -(2n ** 53n - 1n), max: 2n ** 53n - 1n }),
    fc.bigInt({ min: -(10n ** 30n), max: 10n ** 30n }),
  );
  const value: fc.Arbitrary<DecimalValue> = fc.oneof(
    { weight: 8, arbitrary: fc.tuple(coefficient, fc.integer({ min: 0, max: 20 })) },
    { weight: 1, arbitrary: fc.constant(null) },
    {
      weight: 1,
      arbitrary: fc.constantFrom("NaN" as const, "Infinity" as const, "-Infinity" as const),
    },
  );
  const D = decimal(null, null);

  function referenceDecimal(
    op: "add" | "sub" | "mul",
    x: DecimalValue,
    y: DecimalValue,
  ): DecimalValue {
    if (x === null || y === null) return null;
    if (typeof x === "string" || typeof y === "string") {
      const sign = (v: DecimalValue) =>
        v === "Infinity"
          ? 1
          : v === "-Infinity"
            ? -1
            : v === "NaN"
              ? NaN
              : Array.isArray(v)
                ? Math.sign(Number(v[0]))
                : 0;
      if (x === "NaN" || y === "NaN") return "NaN";
      const sx = sign(x);
      const sy = (op === "sub" ? -1 : 1) * sign(y);
      if (op === "mul")
        return sx === 0 || sign(y) === 0 ? "NaN" : sx * sign(y) > 0 ? "Infinity" : "-Infinity";
      if (typeof x === "string" && typeof y === "string")
        return sx === sy ? (sx > 0 ? "Infinity" : "-Infinity") : "NaN";
      const s = typeof x === "string" ? sx : sy;
      return s > 0 ? "Infinity" : "-Infinity";
    }
    const [a, sa] = x;
    const [b, sb] = y;
    if (op === "mul") return [a * b, sa + sb];
    const s = Math.max(sa, sb);
    const aa = a * 10n ** BigInt(s - sa);
    const bb = b * 10n ** BigInt(s - sb);
    return [op === "add" ? aa + bb : aa - bb, s];
  }

  it.each(SEEDS)("add, sub and mul match exact rational arithmetic (seed %i)", (seed) => {
    fc.assert(
      fc.property(
        fc.constantFrom("add" as const, "sub" as const, "mul" as const),
        fc.array(fc.tuple(value, value), { minLength: 1, maxLength: 30 }),
        fc.constantFrom("safe" as const, "int64" as const, "wide" as const),
        (op, pairs, tier) => {
          const fits = (v: DecimalValue) =>
            !Array.isArray(v) ||
            (tier === "safe"
              ? Math.abs(Number(v[0])) <= 2 ** 53 - 1
              : tier === "int64"
                ? (v[0] as bigint) < 2n ** 63n && (v[0] as bigint) >= -(2n ** 63n)
                : true);
          const usable = pairs.filter(([x, y]) => fits(x) && fits(y));
          if (usable.length === 0) return;
          const left = decimals(
            D,
            usable.map(([x]) => x),
            tier,
          );
          const right = decimals(
            D,
            usable.map(([, y]) => y),
            tier,
          );
          const out = evaluate(call(op, D, col(0, D), col(1, D)), batchOf([left, right]));
          const expected = usable.map(([x, y]) => referenceDecimal(op, x, y));
          expect(out.map(decimalValue)).toEqual(expected.map(decimalValue));
          // Display scale: max(scales) for add/sub, the sum for mul (PostgreSQL).
          out.forEach((v, row) => {
            const e = expected[row];
            if (Array.isArray(v) && Array.isArray(e)) expect(v[1]).toBe(e[1]);
          });
        },
      ),
      { seed, numRuns: 1500 },
    );
  });

  it("keeps uniform scales uniform on the fast path", () => {
    const N = decimal(12, 2);
    const left = decimals(
      N,
      [
        [150n, 2],
        [-1n, 2],
      ],
      "safe",
      2,
    );
    const right = decimals(
      N,
      [
        [25n, 2],
        [1n, 2],
      ],
      "safe",
      2,
    );
    expect(
      evaluate(call("add", decimal(null, null), col(0, N), col(1, N)), batchOf([left, right])),
    ).toEqual([
      [175n, 2],
      [0n, 2],
    ]);
    expect(
      evaluate(call("mul", decimal(null, null), col(0, N), col(1, N)), batchOf([left, right])),
    ).toEqual([
      [3750n, 4],
      [-1n, 4],
    ]);
  });
});
