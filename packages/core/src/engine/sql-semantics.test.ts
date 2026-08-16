import { describe, expect, it } from "vitest";
import {
  compareSqlValues,
  compileLikePattern,
  encodeSqlEqualityValue,
  roundSqlNumber,
} from "./sql-semantics.js";

describe("shared SQL value semantics", () => {
  it("orders strings by codepoint independent of the host locale", () => {
    expect(compareSqlValues("a", "A")).toBeGreaterThan(0);
    expect(["a", "A", "z", "Z"].sort(compareSqlValues)).toEqual(["A", "Z", "a", "z"]);
  });

  it("rounds ties away from zero and bounds extreme precision", () => {
    expect(roundSqlNumber(-1.5)).toBe(-2);
    expect(roundSqlNumber(1.25, 1)).toBe(1.3);
    expect(roundSqlNumber(-1.25, 1)).toBe(-1.3);
    expect(roundSqlNumber(1.23, 1_000)).toBe(1.23);
    expect(roundSqlNumber(123.45, -1)).toBe(123);
    expect(roundSqlNumber(1.005, 2)).toBe(1);
    expect(roundSqlNumber(2.675, 2)).toBe(2.67);
  });

  it("matches LIKE underscore against one Unicode codepoint", () => {
    expect(compileLikePattern("_").test("😀")).toBe(true);
    expect(compileLikePattern("__").test("😀")).toBe(false);
    expect(compileLikePattern("😀_").test("😀x")).toBe(true);
  });

  it("normalizes signed zero in SQL equality keys", () => {
    expect(encodeSqlEqualityValue(-0)).toEqual(encodeSqlEqualityValue(0));
  });
});
