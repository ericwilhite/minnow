/**
 * Comparison, three-valued logic, CASE/COALESCE masking, IN lists and casts. Laws: PostgreSQL's
 * comparison operators (float NaN ordering, NUMERIC by value, C-collation text), SQL's
 * three-valued logic, CASE/COALESCE evaluation of only the needed branches, and PostgreSQL's cast
 * rounding rules (rint for float → int, half away from zero for numeric → int and typmods).
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import type { ScalarExpr } from "../../ir/ir.js";
import { DATE_POS_INFINITY, TIMESTAMP_POS_INFINITY } from "../../temporal.js";
import { DivisionByZeroError, NumericOutOfRangeError, UnsupportedPlanError } from "../errors.js";
import {
  BOOL,
  DATE,
  F64,
  I16,
  I32,
  I64,
  TEXT,
  TS,
  batchOf,
  call,
  cmp,
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
  texts,
  valueAt,
} from "./test-support.js";

const SEEDS = seedsFor("next-kernels-logic", [0x10c1, 0x7]);
const bools = (values: ReadonlyArray<boolean | null>) =>
  numbers(
    BOOL,
    values.map((v) => (v === null ? null : v ? 1 : 0)),
  );
const and = (...args: ScalarExpr[]): ScalarExpr => ({ op: "and", args, type: BOOL });
const or = (...args: ScalarExpr[]): ScalarExpr => ({ op: "or", args, type: BOOL });
const not = (arg: ScalarExpr): ScalarExpr => ({ op: "not", arg, type: BOOL });
const cast = (input: ScalarExpr, type: ScalarExpr["type"]): ScalarExpr => ({
  op: "cast",
  input,
  type,
});

describe("comparison", () => {
  it("orders floats like PostgreSQL: NaN equal to itself and greatest, −0 = 0", () => {
    const x = numbers(F64, [NaN, NaN, 1, -0, -Infinity]);
    const y = numbers(F64, [NaN, Infinity, NaN, 0, -Infinity]);
    const batch = batchOf([x, y]);
    expect(evaluate(cmp("=", col(0, F64), col(1, F64)), batch)).toEqual([
      true,
      false,
      false,
      true,
      true,
    ]);
    expect(evaluate(cmp(">", col(0, F64), col(1, F64)), batch)).toEqual([
      false,
      true,
      false,
      false,
      false,
    ]);
    expect(evaluate(cmp("<", col(0, F64), col(1, F64)), batch)).toEqual([
      false,
      false,
      true,
      false,
      false,
    ]);
  });

  it("keeps the NaN order on every loop shape: dense constant, dense columns, selected rows", () => {
    const x = numbers(F64, [NaN, 1, NaN, 2]);
    const y = numbers(F64, [1, NaN, NaN, 2]);
    const nan = lit(numbers(F64, [NaN]));
    expect(evaluate(cmp(">", col(0, F64), nan), batchOf([x]))).toEqual([
      false,
      false,
      false,
      false,
    ]);
    expect(evaluate(cmp("=", col(0, F64), nan), batchOf([x]))).toEqual([true, false, true, false]);
    expect(evaluate(cmp("<", col(0, F64), lit(numbers(F64, [5]))), batchOf([x]))).toEqual([
      false,
      true,
      false,
      true,
    ]);
    expect(evaluate(cmp(">", col(0, F64), col(1, F64)), batchOf([x, y]))).toEqual([
      true,
      false,
      false,
      false,
    ]);
    const selected = { rows: Uint16Array.of(0, 1, 2), count: 3 };
    expect(evaluate(cmp(">=", col(0, F64), col(1, F64)), batchOf([x, y], selected))).toEqual([
      true,
      false,
      true,
    ]);
  });

  it("compares decimals by value across scales, specials ordered −∞ < finite < ∞ < NaN", () => {
    const D = decimal(null, null);
    const x = decimals(
      D,
      [[150n, 2], [1n, 0], "NaN", "Infinity", [10n ** 30n, 2], "-Infinity"],
      "wide",
    );
    const y = decimals(
      D,
      [[15n, 1], [100n, 2], "NaN", "NaN", [10n ** 28n, 0], [-(10n ** 20n), 0]],
      "wide",
    );
    const batch = batchOf([x, y]);
    expect(evaluate(cmp("=", col(0, D), col(1, D)), batch)).toEqual([
      true,
      true,
      true,
      false,
      true,
      false,
    ]);
    expect(evaluate(cmp("<", col(0, D), col(1, D)), batch)).toEqual([
      false,
      false,
      false,
      true,
      false,
      true,
    ]);
  });

  it.each(SEEDS)("decimal comparison agrees with exact rationals (seed %i)", (seed) => {
    const D = decimal(null, null);
    const value = fc.tuple(
      fc.bigInt({ min: -(10n ** 20n), max: 10n ** 20n }),
      fc.integer({ min: 0, max: 18 }),
    );
    fc.assert(
      fc.property(fc.array(fc.tuple(value, value), { minLength: 1, maxLength: 20 }), (pairs) => {
        const safe = pairs.every(
          ([[a], [b]]) => Math.abs(Number(a)) < 2 ** 53 && Math.abs(Number(b)) < 2 ** 53,
        );
        const x = decimals(
          D,
          pairs.map(([a]) => a),
          safe ? "safe" : "wide",
        );
        const y = decimals(
          D,
          pairs.map(([, b]) => b),
          safe ? "safe" : "wide",
        );
        const out = evaluate(cmp("<=", col(0, D), col(1, D)), batchOf([x, y]));
        const expected = pairs.map(([[a, sa], [b, sb]]) => {
          const s = Math.max(sa, sb);
          return a * 10n ** BigInt(s - sa) <= b * 10n ** BigInt(s - sb);
        });
        expect(out).toEqual(expected);
      }),
      { seed, numRuns: 1000 },
    );
  });

  it("orders text by bytes (code points), not UTF-16", () => {
    const x = texts(["￿", "a", "ab", "", null]);
    const y = texts(["😀", "b", "a", "", "x"]);
    expect(evaluate(cmp("<", col(0, TEXT), col(1, TEXT)), batchOf([x, y]))).toEqual([
      true,
      true,
      false,
      false,
      null,
    ]);
  });

  it("compares a dictionary with a constant once per entry, and notices a new dictionary", () => {
    const expr = cmp("=", col(0, TEXT), lit(texts(["red"])));
    const compiled = compileFor(expr, [TEXT]);
    const first = dictionary(texts(["red", "blue"]), [0, 1, null, 0]);
    const out1 = compiled.evaluate(batchOf([first]), null);
    expect([0, 1, 2, 3].map((r) => valueAt(out1, r))).toEqual([true, false, null, true]);
    const second = dictionary(texts(["blue", "red"]), [0, 1]);
    const out2 = compiled.evaluate(batchOf([second]), null);
    expect([0, 1].map((r) => valueAt(out2, r))).toEqual([false, true]);
  });

  it("propagates null and refuses collations, enums and intervals not compiled yet", () => {
    expect(
      evaluate(cmp("=", col(0, I32), lit(numbers(I32, [null]))), batchOf([numbers(I32, [1, 2])])),
    ).toEqual([null, null]);
    expect(() =>
      compileFor(
        {
          op: "compare",
          cmp: "<",
          left: col(0, TEXT),
          right: col(0, TEXT),
          collation: "de-DE",
          type: BOOL,
        },
        [TEXT],
      ),
    ).toThrow(UnsupportedPlanError);
  });

  it("IS DISTINCT FROM treats nulls as values", () => {
    const x = numbers(I32, [1, null, null, 2]);
    const y = numbers(I32, [1, null, 3, 3]);
    const expr: ScalarExpr = {
      op: "distinctFrom",
      negated: false,
      left: col(0, I32),
      right: col(1, I32),
      type: BOOL,
    };
    expect(evaluate(expr, batchOf([x, y]))).toEqual([false, false, true, true]);
  });
});

describe("three-valued logic", () => {
  const values = [true, false, null] as const;
  const pairs = values.flatMap((a) => values.map((b) => [a, b] as const));
  const sqlAnd = (a: boolean | null, b: boolean | null) =>
    a === false || b === false ? false : a === null || b === null ? null : true;
  const sqlOr = (a: boolean | null, b: boolean | null) =>
    a === true || b === true ? true : a === null || b === null ? null : false;

  it("AND, OR and NOT follow SQL's truth tables", () => {
    const batch = batchOf([bools(pairs.map(([a]) => a)), bools(pairs.map(([, b]) => b))]);
    expect(evaluate(and(col(0, BOOL), col(1, BOOL)), batch)).toEqual(
      pairs.map(([a, b]) => sqlAnd(a, b)),
    );
    expect(evaluate(or(col(0, BOOL), col(1, BOOL)), batch)).toEqual(
      pairs.map(([a, b]) => sqlOr(a, b)),
    );
    expect(evaluate(not(col(0, BOOL)), batch)).toEqual(
      pairs.map(([a]) => (a === null ? null : !a)),
    );
  });

  it("AND does not evaluate later arguments on rows already false", () => {
    const x = numbers(I32, [0, 5, 2]);
    const expr = and(
      cmp("<>", col(0, I32), lit(numbers(I32, [0]))),
      cmp(">", call("div", I32, lit(numbers(I32, [10])), col(0, I32)), lit(numbers(I32, [1]))),
    );
    expect(evaluate(expr, batchOf([x]))).toEqual([false, true, true]);
    // Without the guard the same division raises.
    expect(() =>
      evaluate(call("div", I32, lit(numbers(I32, [10])), col(0, I32)), batchOf([x])),
    ).toThrow(DivisionByZeroError);
  });

  it("IS [NOT] NULL / TRUE / FALSE are never null", () => {
    const batch = batchOf([bools([true, false, null])]);
    expect(
      evaluate({ op: "isNull", negated: false, arg: col(0, BOOL), type: BOOL }, batch),
    ).toEqual([false, false, true]);
    // Lowering's IS TRUE / IS NOT FALSE: x IS NOT DISTINCT FROM true / x IS DISTINCT FROM false.
    const truth = (value: number): ScalarExpr => lit(numbers(BOOL, [value]));
    const is = (negated: boolean, value: number): ScalarExpr => ({
      op: "distinctFrom",
      negated,
      left: col(0, BOOL),
      right: truth(value),
      type: BOOL,
    });
    expect(evaluate(is(true, 1), batch)).toEqual([true, false, false]);
    expect(evaluate(is(false, 0), batch)).toEqual([true, false, true]);
  });

  it("IN and NOT IN are three-valued; dictionaries are decided per entry", () => {
    const list = [lit(numbers(I32, [1])), lit(numbers(I32, [3]))];
    const inList = (negated: boolean, items: ScalarExpr[]): ScalarExpr => ({
      op: "inList",
      negated,
      arg: col(0, I32),
      list: items,
      collation: null,
      type: BOOL,
    });
    const batch = batchOf([numbers(I32, [1, 2, null])]);
    expect(evaluate(inList(false, list), batch)).toEqual([true, false, null]);
    expect(evaluate(inList(true, list), batch)).toEqual([false, true, null]);
    const withNull = [...list, lit(numbers(I32, [null]))];
    expect(evaluate(inList(false, withNull), batch)).toEqual([true, null, null]);
    expect(evaluate(inList(true, withNull), batch)).toEqual([false, null, null]);
    const textIn: ScalarExpr = {
      op: "inList",
      negated: false,
      arg: col(0, TEXT),
      list: [lit(texts(["b"])), lit(texts(["c"]))],
      collation: null,
      type: BOOL,
    };
    expect(
      evaluate(textIn, batchOf([dictionary(texts(["a", "b", "c"]), [0, 1, 2, null])])),
    ).toEqual([false, true, true, null]);
  });
});

describe("CASE and COALESCE", () => {
  const caseExpr = (
    branches: Array<[ScalarExpr, ScalarExpr]>,
    otherwise: ScalarExpr | null,
    type: ScalarExpr["type"],
  ): ScalarExpr => ({
    op: "case",
    branches: branches.map(([when, then]) => ({ when, then })),
    otherwise,
    type,
  });

  it("evaluates a branch only on the rows it produces", () => {
    const x = numbers(I32, [0, 5, -2, 0]);
    const expr = caseExpr(
      [[cmp("=", col(0, I32), lit(numbers(I32, [0]))), lit(numbers(I32, [0]))]],
      call("div", I32, lit(numbers(I32, [10])), col(0, I32)),
      I32,
    );
    expect(evaluate(expr, batchOf([x]))).toEqual([0, 2, -5, 0]);
  });

  it("is null without ELSE, and treats a null WHEN as not taken", () => {
    const x = numbers(I32, [1, null, 3]);
    const expr = caseExpr(
      [[cmp(">", col(0, I32), lit(numbers(I32, [2]))), lit(numbers(I32, [100]))]],
      null,
      I32,
    );
    expect(evaluate(expr, batchOf([x]))).toEqual([null, null, 100]);
  });

  it("assembles text and decimal results in row order across branches", () => {
    const x = numbers(I32, [1, 2, 3, 4]);
    const text = caseExpr(
      [[cmp("<", col(0, I32), lit(numbers(I32, [3]))), lit(texts(["small"]))]],
      lit(texts(["large value"])),
      TEXT,
    );
    expect(evaluate(text, batchOf([x]))).toEqual(["small", "small", "large value", "large value"]);
    const D = decimal(null, null);
    const dec = caseExpr(
      [
        [
          cmp("=", col(0, I32), lit(numbers(I32, [2]))),
          lit(decimals(D, [[10n ** 30n, 3]], "wide")),
        ],
      ],
      lit(decimals(D, [[15n, 1]])),
      D,
    );
    expect(evaluate(dec, batchOf([x])).map(decimalValue)).toEqual(
      [
        [15n, 1],
        [10n ** 30n, 3],
        [15n, 1],
        [15n, 1],
      ].map(decimalValue),
    );
  });

  it("COALESCE evaluates later arguments only where earlier ones are null", () => {
    const x = numbers(I32, [null, 4, null]);
    const y = numbers(I32, [0, 0, 2]);
    const expr = call(
      "coalesce",
      I32,
      col(0, I32),
      call("div", I32, lit(numbers(I32, [10])), col(1, I32)),
    );
    // Row 0's fallback divides by zero and must raise; rows 1 and 2 alone do not.
    expect(() => evaluate(expr, batchOf([x, y]))).toThrow(DivisionByZeroError);
    const selection = { rows: Uint16Array.of(1, 2), count: 2 };
    expect(evaluate(expr, batchOf([x, y], selection))).toEqual([4, 5]);
    expect(
      evaluate(call("nullif", I32, col(1, I32), lit(numbers(I32, [0]))), batchOf([x, y])),
    ).toEqual([null, null, 2]);
  });
});

describe("casts", () => {
  const one = (input: ReturnType<typeof numbers>, type: ScalarExpr["type"]) =>
    evaluate(cast(col(0, input.type), type), batchOf([input]));
  it("float → integer rounds half to even and checks the range", () => {
    expect(one(numbers(F64, [2.5, 3.5, -2.5, 1.4999]), I32)).toEqual([2, 4, -2, 1]);
    expect(() => one(numbers(F64, [2 ** 31]), I32)).toThrow(
      new NumericOutOfRangeError("integer out of range"),
    );
    expect(() => one(numbers(I32, [40_000]), I16)).toThrow(
      new NumericOutOfRangeError("smallint out of range"),
    );
  });
  it("numeric → integer rounds half away from zero", () => {
    const D = decimal(null, null);
    expect(
      one(
        decimals(D, [
          [25n, 1],
          [-25n, 1],
          [-24n, 1],
        ]),
        I32,
      ),
    ).toEqual([3, -3, -2]);
    expect(one(decimals(D, [[123n, 0]]), I64)).toEqual([123n]);
  });
  it("numeric typmods round and refuse overflow; floats go through 15 digits", () => {
    const D = decimal(null, null);
    const N52 = decimal(5, 2);
    expect(
      one(
        decimals(D, [
          [12345n, 3],
          [-12355n, 4],
          [5n, 0],
        ]),
        N52,
      ).map(decimalValue),
    ).toEqual(["1235e-2", "-124e-2", "5e-0"]);
    expect(() => one(decimals(D, [[1000n, 0]]), N52)).toThrow(
      new NumericOutOfRangeError("numeric field overflow"),
    );
    expect(one(numbers(F64, [0.1, 2.5, 1e20]), D).map(decimalValue)).toEqual([
      "1e-1",
      "25e-1",
      "100000000000000000000e-0",
    ]);
    expect(one(decimals(D, [[1n, 1]]), F64)).toEqual([0.1]);
  });
  it("date ↔ timestamp keeps infinities and floors negative timestamps", () => {
    expect(one(numbers(DATE, [1, -1, DATE_POS_INFINITY]), TS)).toEqual([
      86_400_000_000n,
      -86_400_000_000n,
      TIMESTAMP_POS_INFINITY,
    ]);
    expect(one(numbers(TS, [-1, 86_400_000_000]), DATE)).toEqual([-1, 1]);
    expect(evaluate(cast(col(0, I32), I64), batchOf([constant(numbers(I32, [7]), 2)]))).toEqual([
      7n,
      7n,
    ]);
  });
});
