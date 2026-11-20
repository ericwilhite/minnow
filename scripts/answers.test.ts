import { describe, expect, it } from "vitest";
import {
  AnswerCollector,
  INLINE_VALUES,
  checkFixtureRows,
  columnKind,
  compareAnswers,
  normalize,
  type ColumnKind,
} from "./lib/answers.mts";

const collect = (rows: unknown[][], ordered: boolean, kinds?: readonly ColumnKind[]) => {
  const collector = new AnswerCollector(ordered, kinds);
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

  it("normalizes by column kind, exactly and without losing text", () => {
    expect(normalize("12.3400", "float")).toBe(12.34);
    expect(normalize("12.3400", "numeric")).toBe("n12.3400");
    expect(normalize("12.3400", "text")).toBe("s12.3400");
    expect(normalize(12n, "integer")).toBe("i12");
    expect(normalize(12, "integer")).toBe("i12");
    expect(normalize("00123", "text")).toBe("s00123");
    expect(normalize(new Date(5), "temporal")).toBe("t5");
    expect(normalize("2024-01-01", "temporal")).toBe(
      `t${String(Date.parse("2024-01-01T00:00:00Z"))}`,
    );
    expect(normalize(true, "bool")).toBe("b1");
    expect(columnKind("int64")).toBe("integer");
    expect(columnKind("decimal")).toBe("numeric");
    expect(columnKind("float64")).toBe("float");
  });

  // Review of #12 (P2): each pair was reported equal by the earlier value-guessing normalizer.
  it.each<[string, unknown, unknown, ColumnKind]>([
    ["NULL vs undefined", null, undefined, "value"],
    ["int8 max vs max - 1", "9223372036854775807", "9223372036854775806", "integer"],
    ["int8 bigint vs neighbour", 2n ** 63n - 1n, 2n ** 63n - 2n, "integer"],
    [
      "distinct high-precision NUMERIC",
      "1.000000000000000000001",
      "1.000000000000000000002",
      "numeric",
    ],
    ["text 1.0 vs 1.00", "1.0", "1.00", "text"],
    ["JSON objects", { a: 1 }, { a: 2 }, "json"],
    ["JSON objects of unknown kind", { a: 1 }, { a: 2 }, "value"],
    ["bytes ff vs fe", Buffer.from([0xff]), Buffer.from([0xfe]), "bytes"],
    ["bytes of unknown kind", Buffer.from([0xff]), Buffer.from([0xfe]), "value"],
    ["NUMERIC 1.0 vs text 1.0", "1.0", "1.0", "numeric"],
  ])("tells apart %s", (name, a, b, kind) => {
    const other: ColumnKind = name.startsWith("NUMERIC 1.0 vs text") ? "text" : kind;
    expect(
      compareAnswers(collect([[a]], true, [kind]), collect([[b]], true, [other]), true).same,
    ).toBe(false);
  });

  it.each<[string, unknown, unknown, ColumnKind]>([
    ["the same int8 as bigint and decimal text", 2n ** 63n - 1n, "9223372036854775807", "integer"],
    ["a safe integer as number and bigint", 42, 42n, "integer"],
    ["JSON with keys in another order", { a: 1, b: [2] }, { b: [2], a: 1 }, "json"],
    ["equal bytes", Buffer.from([1, 2]), new Uint8Array([1, 2]), "bytes"],
  ])("treats as equal %s", (_name, a, b, kind) => {
    expect(
      compareAnswers(collect([[a]], true, [kind]), collect([[b]], true, [kind]), true).same,
    ).toBe(true);
  });

  it("sorts unordered rows by an exact key before applying the tolerance", () => {
    // Review of #12 (P2): a six-digit sort key tied these, so a reversal compared unequal.
    const rows = [[1.0000001], [1.0000002]];
    expect(
      compareAnswers(
        collect(rows, false, ["float"]),
        collect([...rows].reverse(), false, ["float"]),
        false,
      ).same,
    ).toBe(true);
    expect(
      compareAnswers(
        collect([[1.0000001], [1.0000002]], false, ["float"]),
        collect([[1.0000001], [1.0000003]], false, ["float"]),
        false,
      ).same,
    ).toBe(false);
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
