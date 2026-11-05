import { describe, expect, it } from "vitest";
import { dataBytes, type VectorData } from "../vector/vector.js";
import { DecimalSum } from "./decimal-sum.js";

type DecimalData = Extract<VectorData, { layout: "decimal" }>;

function mixed(values: ReadonlyArray<readonly [number, number]>): DecimalData {
  return {
    layout: "decimal",
    scale: null,
    scales: Uint16Array.from(values, ([, scale]) => scale),
    coefficients: { repr: "safe", values: Float64Array.from(values, ([c]) => c) },
    specials: null,
  };
}

/** Exact reference: every value aligned to the largest scale with BigInt, one by one. */
function reference(values: ReadonlyArray<readonly [number, number]>) {
  const scale = values.reduce((max, [, s]) => Math.max(max, s), 0);
  let sum = 0n;
  for (const [c, s] of values) sum += BigInt(c) * 10n ** BigInt(scale - s);
  return { kind: "finite", coefficient: sum, scale };
}

function best(run: () => void): number {
  let fastest = Infinity;
  for (let round = 0; round < 3; round++) {
    const start = performance.now();
    run();
    fastest = Math.min(fastest, performance.now() - start);
  }
  return fastest;
}

describe("mixed-scale NUMERIC (review A1-2 finding 9)", () => {
  it("sums heterogeneous scales exactly without rescaling values (benchmark)", () => {
    const rows = 200_000;
    // NUMERIC values at scale 2, plus one value with a 1,000-digit fraction (review probe).
    const uniform = Array.from(
      { length: rows },
      (_, i) => [((i * 7919) % 100_000) - 50_000, 2] as const,
    );
    const heterogeneous = [...uniform.slice(0, rows - 1), [1, 1000] as const];
    const sumOf = (values: ReadonlyArray<readonly [number, number]>) => {
      const sum = new DecimalSum();
      sum.add(mixed(values), values.length, null, null);
      return sum;
    };
    const sum = sumOf(heterogeneous);
    expect(sum.result()).toEqual(reference(heterogeneous));
    expect(sum.partitions).toBe(2);
    expect(sum.rescales).toBe(2);
    expect(sum.bigintValues).toBe(0);
    // The representation stays 10 bytes per value; one max scale per vector would have needed a
    // ~1,000-digit coefficient (hundreds of bytes as a BigInt) for every value.
    expect(dataBytes({ kind: "decimal", precision: null, scale: null }, rows)).toBe(rows * 10);
    const uniformMs = best(() => sumOf(uniform).result());
    const mixedMs = best(() => sumOf(heterogeneous).result());
    console.info(
      `decimal SUM over ${String(rows)} rows: uniform ${uniformMs.toFixed(1)} ms, one 1,000-digit scale ${mixedMs.toFixed(1)} ms`,
    );
    expect(mixedMs).toBeLessThan(uniformMs * 5 + 20);
  });

  it("widens a partition only when the f64 sum would stop being exact, and handles specials", () => {
    const big = Number.MAX_SAFE_INTEGER - 5;
    const values = [
      [big, 0],
      [big, 0],
      [3, 1],
      [-2, 1],
    ] as const;
    const sum = new DecimalSum();
    sum.add(mixed(values), values.length, null, null);
    expect(sum.result()).toEqual(reference(values));
    expect(sum.bigintValues).toBe(1);
    const special = new DecimalSum();
    special.add(
      {
        ...mixed([
          [1, 0],
          [0, 0],
          [0, 0],
        ]),
        specials: Uint8Array.from([0, 2, 3]),
      },
      3,
      null,
      null,
    );
    expect(special.result()).toEqual({ kind: "special", value: "NaN" });
    const selected = new DecimalSum();
    selected.add(mixed(values), 4, null, { rows: Uint16Array.from([2, 3]), count: 2 });
    expect(selected.result()).toEqual({ kind: "finite", coefficient: 1n, scale: 1 });
    expect(new DecimalSum().result()).toEqual({ kind: "empty" });
  });
});
