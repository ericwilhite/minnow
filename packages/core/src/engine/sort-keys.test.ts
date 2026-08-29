import { describe, expect, it } from "vitest";
import { nullOrder } from "./query.js";
import { buildSortKeyColumn, sortKeyIndexes, type SortKeyTerm } from "./sort-keys.js";
import { compareSqlValues } from "./sql-semantics.js";
import { mulberry32 } from "../testing/seeds.js";

interface TermSpec {
  readonly values: readonly unknown[];
  readonly descending?: boolean;
  readonly nulls?: "first" | "last";
}

/** The order ORDER BY defines, spelled out with the engine's own value comparison. */
function referenceOrder(terms: readonly TermSpec[]): number[] {
  const count = terms[0]?.values.length ?? 0;
  const indexes = Array.from({ length: count }, (_, index) => index);
  indexes.sort((left, right) => {
    for (const term of terms) {
      const placed = nullOrder(
        term.values[left],
        term.values[right],
        term.nulls,
        term.descending === true ? "desc" : "asc",
      );
      if (placed !== undefined && placed !== 0) return placed;
      const comparison = compareSqlValues(term.values[left], term.values[right]);
      if (comparison !== 0) return term.descending === true ? -comparison : comparison;
    }
    return left - right;
  });
  return indexes;
}

function kernelOrder(terms: readonly TermSpec[]): number[] {
  const count = terms[0]?.values.length ?? 0;
  const prepared: SortKeyTerm[] = terms.map((term) => ({
    column: buildSortKeyColumn(count, (index) => term.values[index]),
    descending: term.descending === true,
    nulls: term.nulls,
  }));
  return [...sortKeyIndexes(count, prepared)];
}

/** Numbers with every awkward value the comparison has an opinion about, and plenty of ties. */
function awkwardNumbers(count: number, seed: number): unknown[] {
  const rng = mulberry32(seed);
  const specials = [null, NaN, -0, 0, Infinity, -Infinity, 1e-300, -1e-300, 2 ** 53, -(2 ** 53)];
  return Array.from({ length: count }, () => {
    const roll = rng();
    if (roll < 0.2) return specials[Math.floor(rng() * specials.length)];
    if (roll < 0.6) return Math.floor(rng() * 20) - 10;
    return (rng() - 0.5) * 1e6;
  });
}

const directions: ReadonlyArray<Pick<TermSpec, "descending" | "nulls">> = [
  {},
  { descending: true },
  { nulls: "first" },
  { nulls: "last" },
  { descending: true, nulls: "first" },
  { descending: true, nulls: "last" },
];

describe("sort key columns", () => {
  it("unboxes numbers, datetimes, and booleans, and ranks strings", () => {
    const numbers = buildSortKeyColumn(3, (index) => [3, null, 1][index]);
    expect(numbers.numbers).toBeInstanceOf(Float64Array);
    expect([...(numbers.nulls ?? [])]).toEqual([0, 1, 0]);
    expect(numbers.compare(0, 2)).toBeGreaterThan(0);
    expect(numbers.compare(1, 2)).toBeLessThan(0);

    const dates = buildSortKeyColumn(2, (index) => [new Date(5), new Date(2)][index]);
    expect([...(dates.numbers ?? [])]).toEqual([5, 2]);

    const booleans = buildSortKeyColumn(3, (index) => [true, false, null][index]);
    expect([...(booleans.numbers ?? [])]).toEqual([1, 0, 0]);
    expect(booleans.compare(0, 1)).toBeGreaterThan(0);
    expect(booleans.isNull(2)).toBe(true);

    // Ranks follow code-unit order, the engine's string collation: "Z" before "a", "é" after "z".
    const strings = buildSortKeyColumn(5, (index) => ["b", "Z", null, "é", "a"][index]);
    expect([...(strings.numbers ?? [])]).toEqual([2, 0, 0, 3, 1]);
    expect(strings.compare(1, 4)).toBeLessThan(0);
    expect(strings.compare(3, 0)).toBeGreaterThan(0);
    expect(strings.compare(0, 0)).toBe(0);
  });

  it("keeps the generic comparison, and its type error, for a column that mixes types", () => {
    const mixed = buildSortKeyColumn(2, (index) => [1, "a"][index]);
    expect(mixed.numbers).toBeUndefined();
    expect(() => mixed.compare(0, 1)).toThrow(TypeError);
    expect(() => kernelOrder([{ values: [1, "a", 2] }])).toThrow(
      "Values must have comparable SQL types",
    );
  });
});

describe("sortKeyIndexes", () => {
  it("is stable: equal keys keep their input order", () => {
    expect(kernelOrder([{ values: [2, 1, 2, 1, null, 2, null] }])).toEqual([1, 3, 0, 2, 5, 4, 6]);
    expect(kernelOrder([{ values: ["b", "a", "b", "a"], descending: true }])).toEqual([0, 2, 1, 3]);
  });

  it("treats -0 and +0 as one key and NaN as one key past infinity", () => {
    const values = [NaN, 0, Infinity, -0, NaN, -Infinity, 0];
    expect(kernelOrder([{ values }])).toEqual([5, 1, 3, 6, 2, 0, 4]);
    expect(kernelOrder([{ values, descending: true }])).toEqual([0, 4, 2, 1, 3, 6, 5]);
  });

  it("uses PostgreSQL NULL defaults or an explicit NULLS FIRST/LAST", () => {
    const values = [1, null, 0, null, 2];
    expect(kernelOrder([{ values }])).toEqual([2, 0, 4, 1, 3]);
    expect(kernelOrder([{ values, descending: true }])).toEqual([1, 3, 4, 0, 2]);
    expect(kernelOrder([{ values, nulls: "last" }])).toEqual([2, 0, 4, 1, 3]);
    expect(kernelOrder([{ values, descending: true, nulls: "first" }])).toEqual([1, 3, 4, 0, 2]);
    // NULL and NaN are different keys: NaN is a number that sorts last among numbers.
    expect(kernelOrder([{ values: [NaN, null, 1], nulls: "last" }])).toEqual([2, 0, 1]);
  });

  it("breaks ties on one term with the next, through every kernel", () => {
    const count = 12_000;
    const rng = mulberry32(11);
    // Two distinct leading values leave runs long enough to radix on the second term; the third
    // term is a string with few distinct values, the fourth a datetime with many.
    const first = Array.from({ length: count }, () => (rng() < 0.5 ? "west" : null));
    const second = awkwardNumbers(count, 12);
    const third = Array.from({ length: count }, () => ["a", "b", "c", null][Math.floor(rng() * 4)]);
    const fourth = Array.from({ length: count }, () =>
      rng() < 0.1 ? null : new Date(Math.floor(rng() * 1000)),
    );
    for (const [a, b, c, d] of [
      [{}, {}, {}, {}],
      [{ descending: true }, { nulls: "last" }, { descending: true, nulls: "first" }, {}],
      [{ nulls: "first" }, { descending: true }, {}, { descending: true, nulls: "last" }],
    ] as const) {
      const terms: TermSpec[] = [
        { values: first, ...a },
        { values: second, ...b },
        { values: third, ...c },
        { values: fourth, ...d },
      ];
      expect(kernelOrder(terms)).toEqual(referenceOrder(terms));
    }
  });

  it("matches the engine's comparison at every size, direction, and placement", () => {
    // 8 rows insertion-sort, 500 merge, 6000 radix — each with the awkward values and ties.
    for (const count of [8, 500, 6_000]) {
      for (const [seed, direction] of directions.entries()) {
        const numbers: TermSpec = { values: awkwardNumbers(count, seed + count), ...direction };
        expect(kernelOrder([numbers]), `numbers ${String(count)}`).toEqual(
          referenceOrder([numbers]),
        );
        const rng = mulberry32(seed * 7 + count);
        const strings: TermSpec = {
          values: Array.from({ length: count }, () =>
            rng() < 0.1 ? null : `k${String(Math.floor(rng() * 50))}`,
          ),
          ...direction,
        };
        expect(kernelOrder([strings]), `strings ${String(count)}`).toEqual(
          referenceOrder([strings]),
        );
        const booleans: TermSpec = {
          values: Array.from({ length: count }, () => (rng() < 0.1 ? null : rng() < 0.5)),
          ...direction,
        };
        expect(kernelOrder([booleans]), `booleans ${String(count)}`).toEqual(
          referenceOrder([booleans]),
        );
      }
    }
  });

  it("ranks strings of high cardinality the same as comparing them", () => {
    const rng = mulberry32(99);
    const values = Array.from({ length: 5_000 }, () =>
      rng() < 0.05 ? null : `s-${String(Math.floor(rng() * 1e9))}`,
    );
    for (const direction of directions) {
      const term: TermSpec = { values, ...direction };
      expect(kernelOrder([term])).toEqual(referenceOrder([term]));
    }
  });

  it("recognizes input that is already in order, including by direction", () => {
    const ascending = Array.from({ length: 5_000 }, (_, index) => Math.floor(index / 3));
    expect(kernelOrder([{ values: ascending }])).toEqual(ascending.map((_, index) => index));
    const descending = [...ascending].reverse();
    expect(kernelOrder([{ values: descending, descending: true }])).toEqual(
      descending.map((_, index) => index),
    );
    // Nearly sorted: one element out of place still sorts, and stably.
    const nearly = [...ascending];
    nearly[2_500] = -1;
    expect(kernelOrder([{ values: nearly }])).toEqual(referenceOrder([{ values: nearly }]));
  });

  it("handles empty input, one row, and no terms", () => {
    expect(kernelOrder([{ values: [] }])).toEqual([]);
    expect(kernelOrder([{ values: [42] }])).toEqual([0]);
    expect([...sortKeyIndexes(3, [])]).toEqual([0, 1, 2]);
  });
});
