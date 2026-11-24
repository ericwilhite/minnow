/**
 * `jsonFloatText(value, true)` prints the binary32 a value is stored as, for every double, and
 * always returns.
 *
 * #58's limits (2026-11-23): in single-precision mode a double outside the binary32 range did
 * not terminate. Below the smallest subnormal (about 1.4e-45) the value's binary32 is zero, so
 * `exactFloatText`'s exact numerator is 0 and its decimal-exponent search (`comparePower < 0`,
 * then one power lower) never ends; the event loop stops. SQL never reaches it today (REAL
 * arithmetic raises 22003 on overflow and underflow, as PostgreSQL does), so it is the kernel's
 * contract that is fixed, before a caller relies on it.
 *
 * Law: the contract on `jsonFloatText` (kernels/json-float.ts): with `single`, the value printed
 * is the binary32 it is stored as (`Math.fround`), whose PostgreSQL float4 output
 * json-float-pglite.test.ts checks; every call returns.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { jsonFloatText } from "./json-float.js";

/** Doubles outside the binary32 range, at and around its edges. */
const EDGES = [
  1e-50,
  -1e-50,
  2 ** -150,
  2 ** -149 / 3,
  Number.MIN_VALUE,
  -Number.MIN_VALUE,
  1e39,
  -1e300,
  Number.MAX_VALUE,
  2 ** 128,
  3.4028236e38,
  3.4028235677973366e38,
];

it("prints a double outside the binary32 range as the binary32 it stores as", () => {
  for (const value of EDGES)
    expect(jsonFloatText(value, true), String(value)).toBe(jsonFloatText(Math.fround(value), true));
  expect(jsonFloatText(1e-50, true)).toBe("0");
  expect(jsonFloatText(-1e-50, true)).toBe("-0");
  expect(jsonFloatText(1e39, true)).toBe("Infinity");
});

it.each(seedsFor("next-json-float-single-domain", [0xb6f0, 0xb6f1]))(
  "single precision returns for every double, as its binary32 (seed %i)",
  (seed) => {
    fc.assert(
      fc.property(fc.double({ noNaN: true }), (value) => {
        expect(jsonFloatText(value, true)).toBe(jsonFloatText(Math.fround(value), true));
      }),
      { seed, numRuns: 2000, examples: EDGES.map((value) => [value]) },
    );
  },
);
