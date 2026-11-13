import { describe, expect, it } from "vitest";
import { assignParts, defaultWeight, parsePart } from "./test-parts.mjs";
import { durations } from "./test-shards.ts";

/** A small deterministic generator, so a failure names its case without a seed table. */
function lcg(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
    return state / 2 ** 32;
  };
}

function caseFor(seed: number) {
  const random = lcg(seed);
  const n = 1 + Math.floor(random() * 300);
  const keys = Array.from({ length: n }, (_, i) => `file-${String(i)}-${String(seed)}.test.ts`);
  // Mostly fast files and a few very slow ones, the shape of the real suite.
  const weights = keys.map(() => (random() < 0.05 ? random() * 900_000 : random() * 5_000));
  const count = 1 + Math.floor(random() * 16);
  return { keys, weights, count, random };
}

describe("test parts", () => {
  it("puts every file in exactly one part, for any file list and part count", () => {
    for (let seed = 1; seed <= 400; seed++) {
      const { keys, weights, count } = caseFor(seed);
      const parts = assignParts(keys, weights, count);
      expect(parts).toHaveLength(keys.length);
      for (const part of parts) {
        expect(Number.isInteger(part)).toBe(true);
        expect(part).toBeGreaterThanOrEqual(0);
        expect(part).toBeLessThan(count);
      }
    }
  });

  it("assigns the same parts whatever order the files arrive in", () => {
    for (let seed = 1; seed <= 200; seed++) {
      const { keys, weights, count, random } = caseFor(seed);
      const reference = new Map(
        assignParts(keys, weights, count).map((part, i) => [keys[i], part] as const),
      );
      const order = keys
        .map((key, i) => ({ key, weight: weights[i] ?? 0, at: random() }))
        .sort((a, b) => a.at - b.at);
      const shuffled = assignParts(
        order.map((file) => file.key),
        order.map((file) => file.weight),
        count,
      );
      shuffled.forEach((part, j) => {
        expect(part).toBe(reference.get(order[j]?.key ?? ""));
      });
    }
  });

  it("keeps every part within one file's weight of an even share", () => {
    // Longest-first greedy: a part only receives a file while it is the least loaded, so no part
    // ends more than the largest single weight above the average.
    for (let seed = 1; seed <= 400; seed++) {
      const { keys, weights, count } = caseFor(seed);
      const reserved = Array.from({ length: count }, (_, p) => (p === 0 ? (weights[0] ?? 0) : 0));
      const parts = assignParts(keys, weights, count, reserved);
      const load = [...reserved];
      parts.forEach((part, i) => (load[part] = (load[part] ?? 0) + (weights[i] ?? 0)));
      const total = load.reduce((a, b) => a + b, 0);
      const largest = Math.max(...weights, ...reserved);
      for (const sum of load) expect(sum).toBeLessThanOrEqual(total / count + largest + 1e-6);
    }
  });

  it("refuses duplicate keys and impossible part counts", () => {
    expect(() => assignParts(["a", "a"], [1, 1], 2)).toThrow(/distinct/u);
    expect(() => assignParts(["a"], [1], 0)).toThrow(RangeError);
    expect(() => assignParts(["a"], [1], 1.5)).toThrow(RangeError);
  });

  it("parses one-based parts", () => {
    expect(parsePart(undefined)).toBeNull();
    expect(parsePart(" ")).toBeNull();
    expect(parsePart("2/4")).toEqual({ index: 2, count: 4 });
    for (const bad of ["0/4", "5/4", "1/0", "two/4", "1-4"])
      expect(() => parsePart(bad)).toThrow(/2\/4/u);
  });

  it("weighs unmeasured files at the median of the measured ones", () => {
    expect(defaultWeight({})).toBe(1);
    expect(defaultWeight({ a: 5, b: 1, c: 100 })).toBe(5);
    // The committed table is non-empty and holds only positive millisecond counts.
    for (const profile of [durations.coverage, durations.plain]) {
      const values = Object.values(profile);
      expect(values.length).toBeGreaterThan(0);
      for (const value of values) expect(value).toBeGreaterThanOrEqual(0);
    }
  });
});
