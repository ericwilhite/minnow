import { describe, expect, it } from "vitest";
import {
  AnswerCollector,
  INLINE_VALUES,
  checkFixtureRows,
  compareAnswers,
  normalize,
} from "./lib/answers.mts";

const collect = (rows: unknown[][], ordered: boolean) => {
  const collector = new AnswerCollector(ordered);
  for (const row of rows) collector.add(row);
  return collector.finish();
};

describe("head-to-head answer comparison", () => {
  it("compares floats with an explicit tolerance, across a rounding boundary", () => {
    // Ten-significant-digit hashing put these on different sides of a boundary.
    const a = collect([[1.00000000049, "x"]], true),
      b = collect([[1.00000000051, "x"]], true);
    expect(compareAnswers(a, b, true).same).toBe(true);
    expect(compareAnswers(a, collect([[1.0000001, "x"]], true), true).same).toBe(false);
  });

  it("normalizes representations without losing exact text", () => {
    expect(normalize("12.3400")).toBe(12.34);
    expect(normalize(12n)).toBe(12);
    expect(normalize(2n ** 63n - 1n)).toBe("b9223372036854775807");
    expect(normalize("00123")).toBe("s00123");
    expect(normalize(new Date(5))).toBe(5);
    expect(normalize("2024-01-01")).toBe(Date.parse("2024-01-01T00:00:00Z"));
    expect(normalize(true)).toBe(1);
  });

  it("orders unordered inline results before comparing", () => {
    expect(compareAnswers(collect([[1], [2]], false), collect([[2], [1]], false), false).same).toBe(
      true,
    );
    expect(compareAnswers(collect([[1], [2]], true), collect([[2], [1]], true), true).same).toBe(
      false,
    );
  });

  it("hashes every value of a large export, so one changed field differs", () => {
    // The reviewer's counterexample: x00 "good" -> "WRONG" and f03 12 -> 999 kept the old digest.
    const width = 200,
      rows = Math.ceil(INLINE_VALUES / width) + 5;
    const make = (mutate: boolean) => {
      const collector = new AnswerCollector(false);
      for (let r = 0; r < rows; r++) {
        const row: unknown[] = Array.from({ length: width }, (_, c) => (c === 0 ? r : c));
        if (mutate && r === 7) {
          row[1] = "WRONG";
          row[2] = 999;
        }
        collector.add(row);
      }
      return collector.finish();
    };
    const base = make(false),
      changed = make(true);
    expect(base.method).toBe("multiset-hash");
    expect(compareAnswers(base, make(false), false).same).toBe(true);
    expect(compareAnswers(base, changed, false).same).toBe(false);
  });

  it("checks unordered first-page rows against the fixture", () => {
    const expected = (id: number) => ({ id, f03: id * 2, x00: `v${String(id)}` });
    const good = [expected(3), expected(9)];
    expect(checkFixtureRows(good, ["id", "f03", "x00"], expected).mismatches).toEqual([]);
    const bad = [expected(3), { id: 9, f03: 999, x00: "v9" }];
    expect(checkFixtureRows(bad, ["id", "f03", "x00"], expected).mismatches).toEqual(["id 9 f03"]);
    expect(checkFixtureRows([expected(3), expected(3)], ["id"], expected).mismatches).toHaveLength(
      1,
    );
  });

  it("refuses different row counts and methods", () => {
    expect(compareAnswers(collect([[1]], true), collect([[1], [1]], true), true).same).toBe(false);
  });
});
