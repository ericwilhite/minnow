/**
 * Review B4-p2a-1's probes, kept as regression tests (one per finding). Expected values are
 * PostgreSQL's (checked against PGlite by the reviewer; pglite.test.ts generates around them).
 */
import { describe, expect, it } from "vitest";
import type { ScalarExpr } from "../../ir/ir.js";
import type { FlatVector } from "../../vector/vector.js";
import { NumericOutOfRangeError } from "../errors.js";
import { compileExpr } from "./compile.js";
import {
  BOOL,
  F32,
  F64,
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
  decimalValue,
  decimals,
  dictionary,
  evaluate,
  lit,
  numbers,
  texts,
  valueAt,
} from "./test-support.js";
import { OutputArena } from "./vectors.js";

const D = decimal(null, null);
const cast = (input: FlatVector, type: ScalarExpr["type"]): ScalarExpr => ({
  op: "cast",
  input: col(0, input.type),
  type,
});

describe("review B4-p2a-1 probes", () => {
  it("1. IN over a dictionary re-reads re-bound parameters (also NOT IN)", () => {
    const values = dictionary(numbers(I32, [1, 2]), [0, 1]);
    const params = { slots: [numbers(I32, [1])] };
    for (const negated of [false, true]) {
      const expr: ScalarExpr = {
        op: "inList",
        negated,
        arg: col(0, I32),
        list: [{ op: "param", slot: 0, type: I32 }],
        collation: null,
        type: BOOL,
      };
      const compiled = compileFor(expr, [I32], params);
      const read = () =>
        [0, 1].map((row) => valueAt(compiled.evaluate(batchOf([values]), null), row));
      params.slots = [numbers(I32, [1])];
      expect(read()).toEqual(negated ? [false, true] : [true, false]);
      params.slots = [numbers(I32, [2])];
      expect(read()).toEqual(negated ? [true, false] : [false, true]);
    }
  });

  it("2. REAL underflow is detected after rounding to float4", () => {
    const product = (x: number) =>
      evaluate(
        call("mul", F32, col(0, F32), col(1, F32)),
        batchOf([numbers(F32, [x]), numbers(F32, [1e-20])]),
      );
    expect(() => product(1e-30)).toThrow(
      new NumericOutOfRangeError("value out of range: underflow"),
    );
    expect(product(1e-10)[0]).toBe(Math.fround(Math.fround(1e-10) * Math.fround(1e-20)));
  });

  it("3. NUMERIC → float overflow and underflow are errors", () => {
    for (const [c, s, type, message] of [
      [10n ** 400n, 0, F64, "value out of range: overflow"],
      [1n, 400, F64, "value out of range: underflow"],
      [10n ** 400n, 0, F32, "value out of range: overflow"],
      [1n, 50, F32, "value out of range: underflow"],
    ] as const) {
      const input = decimals(D, [[c, s]], "wide");
      expect(() => evaluate(cast(input, type), batchOf([input]))).toThrow(
        new NumericOutOfRangeError(message),
      );
    }
  });

  it("4. REAL → NUMERIC goes through 6 significant digits", () => {
    const input = numbers(F32, [0.1, 1.234567, 1234567]);
    expect(evaluate(cast(input, D), batchOf([input])).map(decimalValue)).toEqual([
      "1e-1",
      "123457e-5",
      "1234570e-0",
    ]);
  });

  it("5. negative NUMERIC scales round to tens/hundreds and store scale 0", () => {
    const input = decimals(D, [
      [12345n, 0],
      [12355n, 1],
      [-99949n, 0],
    ]);
    expect(evaluate(cast(input, decimal(3, -2)), batchOf([input]))).toEqual([
      [12300n, 0],
      [1200n, 0],
      [-99900n, 0],
    ]);
    const big = decimals(D, [[99951n, 0]]);
    expect(() => evaluate(cast(big, decimal(3, -2)), batchOf([big]))).toThrow(
      "numeric field overflow",
    );
  });

  it("6. comparisons never read garbage under NULL (decimal and int64 exact paths)", () => {
    const a = decimals(D, [null, [1n, 0]]);
    (a.data as { coefficients: { values: Float64Array } }).coefficients.values[0] = NaN;
    const b = decimals(D, [
      [1n, 1],
      [2n, 1],
    ]);
    expect(evaluate(cmp("<", col(0, D), col(1, D)), batchOf([a, b]))).toEqual([null, false]);
    const x = numbers(I64, [null, 2], NaN);
    const y = bigints(I64, [1n, 3n]);
    expect(evaluate(cmp("<", col(0, I64), col(1, I64)), batchOf([x, y]))).toEqual([null, true]);
  });

  it("7. arithmetic over a dictionary of exact BIGINT values", () => {
    const values = dictionary(bigints(I64, [9007199254740992n, -(2n ** 62n)]), [0, 1, null]);
    expect(
      evaluate(call("add", I64, col(0, I64), lit(numbers(I64, [1]))), batchOf([values])),
    ).toEqual([9007199254740993n, -(2n ** 62n) + 1n, null]);
  });

  it("8. COALESCE and CASE over intervals with exact int64 microseconds", () => {
    const interval = { kind: "interval" } as const;
    const value: FlatVector = {
      form: "flat",
      type: interval,
      length: 2,
      validity: null,
      data: {
        layout: "interval",
        months: Int32Array.of(1, 2),
        days: Int32Array.of(3, 4),
        micros: { repr: "int64", values: BigInt64Array.of(2n ** 60n, -1n) },
      },
    };
    const out = compileFor(call("coalesce", interval, col(0, interval)), [interval]).evaluate(
      batchOf([value]),
      null,
    );
    if (out.form !== "flat" || out.data.layout !== "interval") throw new Error("interval expected");
    expect([out.data.months[0], out.data.days[1]]).toEqual([1, 4]);
    expect(out.data.micros.repr).toBe("int64");
    expect([...(out.data.micros.values as BigInt64Array).subarray(0, 2)]).toEqual([2n ** 60n, -1n]);
  });

  it("9. a text CASE with more than 256 populated branches", () => {
    const n = 300;
    const branches = Array.from({ length: n }, (_, k) => ({
      when: cmp("=", col(0, I32), lit(numbers(I32, [k]))),
      then: lit(texts([`v${String(k)}`])),
    }));
    const out = evaluate(
      { op: "case", type: TEXT, branches, otherwise: null },
      batchOf([
        numbers(
          I32,
          Array.from({ length: n }, (_, k) => k),
        ),
      ]),
    );
    expect(out).toEqual(Array.from({ length: n }, (_, k) => `v${String(k)}`));
  });

  it("10. text output growth keeps the grant's accounting exact", () => {
    let used = 0;
    const charge = {
      allocate: (bytes: number) => {
        used += bytes;
        return true;
      },
      free: (bytes: number) => {
        used -= bytes;
      },
    };
    const arena = new OutputArena(charge, 2048);
    const compiled = compileExpr(call("coalesce", TEXT, col(0, TEXT)), {
      arena,
      params: { slots: [] },
      input: [TEXT],
    });
    for (const value of ["a", "a much longer value", "x".repeat(5000)])
      compiled.evaluate(batchOf([texts([value])]), null);
    arena.close();
    expect(used).toBe(0);
  });

  it("11. a NUMERIC product past the maximum display scale rounds to it (16,383)", () => {
    const v = decimals(D, [[1n, 10_000]], "safe", 10_000);
    expect(evaluate(call("mul", D, col(0, D), col(0, D)), batchOf([v]))).toEqual([[0n, 16_383]]);
    const w = decimals(D, [[5n, 8192]]);
    // 25 × 10^−16384 rounds half away from zero to 3 × 10^−16383.
    expect(evaluate(call("mul", D, col(0, D), col(0, D)), batchOf([w]))).toEqual([[3n, 16_383]]);
  });

  it("12. NUMERIC past 2^53 stays in the int64 and int128 tiers (no wide fallback)", () => {
    const a = decimals(
      D,
      [
        [2n ** 60n, 2],
        [-(2n ** 62n), 0],
      ],
      "int64",
    );
    const b = decimals(
      D,
      [
        [3n, 0],
        [2n ** 62n, 0],
      ],
      "int64",
    );
    const add = compileFor(call("add", D, col(0, D), col(1, D)), [D, D]).evaluate(
      batchOf([a, b]),
      null,
    );
    const mul = compileFor(call("mul", D, col(0, D), col(1, D)), [D, D]).evaluate(
      batchOf([a, b]),
      null,
    );
    if (add.form !== "flat" || add.data.layout !== "decimal") throw new Error("decimal expected");
    if (mul.form !== "flat" || mul.data.layout !== "decimal") throw new Error("decimal expected");
    expect(add.data.coefficients.repr).toBe("int64");
    expect(mul.data.coefficients.repr).toBe("int128");
    expect([0, 1].map((row) => valueAt(add, row))).toEqual([
      [2n ** 60n + 300n, 2],
      [0n, 0],
    ]);
    expect([0, 1].map((row) => valueAt(mul, row))).toEqual([
      [3n * 2n ** 60n, 2],
      [-(2n ** 124n), 0],
    ]);
  });

  it("13. −(+0.0) is −0 for floats; integers never hold −0", () => {
    const out = evaluate(call("neg", F64, col(0, F64)), batchOf([numbers(F64, [0, -0, 1.5])]));
    expect(Object.is(out[0], -0)).toBe(true);
    expect(Object.is(out[1], 0)).toBe(true);
    expect(out[2]).toBe(-1.5);
    const ints = compileFor(call("neg", I64, col(0, I64)), [I64]).evaluate(
      batchOf([numbers(I64, [0, 5])]),
      null,
    );
    const data = ints.form === "flat" ? ints.data : null;
    if (data?.layout !== "int64" || data.values.repr !== "safe")
      throw new Error("safe int64 expected");
    expect(Object.is(data.values.values[0], -0)).toBe(false);
    const n = evaluate(
      call("neg", D, col(0, D)),
      batchOf([decimals(D, [[150n, 2], "Infinity", "NaN"])]),
    );
    expect(n).toEqual([[-150n, 2], "-Infinity", "NaN"]);
  });
});
