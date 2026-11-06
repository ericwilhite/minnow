/**
 * NUMERIC division and modulo: PostgreSQL's display scales and special values (outputs as
 * PostgreSQL 16 prints them; pglite.test.ts generates around these).
 */
import { describe, expect, it } from "vitest";
import { DivisionByZeroError } from "../errors.js";
import {
  batchOf,
  call,
  col,
  decimal,
  decimals,
  evaluate,
  type DecimalValue,
} from "./test-support.js";

const D = decimal(null, null);
const run = (
  op: "div" | "mod",
  a: DecimalValue,
  b: DecimalValue,
  tier: "safe" | "int64" | "int128" | "wide" = "safe",
) =>
  evaluate(
    call(op, D, col(0, D), col(1, D)),
    batchOf([decimals(D, [a], tier), decimals(D, [b], tier)]),
  )[0];

describe("NUMERIC / and %", () => {
  it("picks PostgreSQL's quotient scale", () => {
    expect(run("div", [1n, 0], [3n, 0])).toEqual([33333333333333333333n, 20]); // 0.33333333333333333333
    expect(run("div", [10n, 0], [2n, 0])).toEqual([50000000000000000n, 16]); // 5.0000000000000000
    expect(run("div", [1n, 0], [8n, 0])).toEqual([12500000000000000000n, 20]); // 0.12500000000000000000
    expect(run("div", [12345n, 2], [1n, 3])).toEqual([123450000000000000n, 12]); // 123450.000000000000
    expect(run("div", [2n, 0], [3n, 0])).toEqual([66666666666666666667n, 20]); // rounded half away
    expect(run("div", [-2n, 0], [3n, 0])).toEqual([-66666666666666666667n, 20]);
  });

  it("is the same in every coefficient tier", () => {
    for (const tier of ["safe", "int64", "int128", "wide"] as const)
      expect(run("div", [1n, 0], [3n, 0], tier)).toEqual([33333333333333333333n, 20]);
  });

  it("takes modulo's sign from the dividend, at the larger scale", () => {
    expect(run("mod", [-75n, 1], [2n, 0])).toEqual([-15n, 1]); // -7.5 % 2 = -1.5
    expect(run("mod", [75n, 1], [-2n, 0])).toEqual([15n, 1]);
    expect(run("mod", [10n, 0], [3n, 2])).toEqual([1n, 2]); // 10 % 0.03 = 0.01
  });

  it("follows PostgreSQL's specials and division by zero", () => {
    expect(run("div", "NaN", [1n, 0])).toBe("NaN");
    expect(run("div", "Infinity", [-2n, 0])).toBe("-Infinity");
    expect(run("div", "Infinity", "-Infinity")).toBe("NaN");
    expect(run("div", [5n, 0], "Infinity")).toEqual([0n, 0]);
    expect(run("mod", "Infinity", [2n, 0])).toBe("NaN");
    expect(run("mod", [5n, 1], "-Infinity")).toEqual([5n, 1]);
    expect(() => run("div", [1n, 0], [0n, 0])).toThrow(DivisionByZeroError);
    expect(() => run("div", "Infinity", [0n, 0])).toThrow(DivisionByZeroError);
    expect(() => run("mod", "Infinity", [0n, 0])).toThrow(DivisionByZeroError);
    expect(() => run("mod", [1n, 0], [0n, 3])).toThrow(DivisionByZeroError);
  });
});
