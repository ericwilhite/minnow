import { expect, it } from "vitest";
import { pairedCPU } from "./lib/paired-cpu.mts";

const deltas = (values: number[]) =>
  pairedCPU(
    Array<number>(values.length).fill(100),
    values.map((v) => 100 + v),
  );

it("cancels shared load before measuring paired IQR and full range", () => {
  const control = [100, 900, 200, 800, 300, 700, 400, 600, 500];
  const result = pairedCPU(
    control,
    control.map((value, i) => value - 50 + (i % 3)),
  );
  expect(result.medianDeltaMs).toBe(-49);
  expect(result.q1DeltaMs).toBe(-50);
  expect(result.q3DeltaMs).toBe(-48);
  expect(result.iqrDeltaMs).toBe(2);
  expect(result.minDeltaMs).toBe(-50);
  expect(result.maxDeltaMs).toBe(-48);
  expect(result.rangeDeltaMs).toBe(2);
  expect(result.spreadDeltaMs).toBe(result.rangeDeltaMs);
  expect(result.favoursCandidate).toBe(9);
  expect(result.requiredFavourable).toBe(7);
  expect(result.acceptedGain).toBe(true);
  expect(result.noRegression).toBe(true);
});

it("keeps outlying pairs while accepting a strict gain beyond IQR", () => {
  const result = deltas([-10, -10, -10, -10, -10, -10, -10, 0, 1000]);
  expect(result.pairs).toHaveLength(9);
  expect(result.medianDeltaMs).toBe(-10);
  expect(result.iqrDeltaMs).toBe(0);
  expect(result.rangeDeltaMs).toBe(1010);
  expect(result.favoursCandidate).toBe(7);
  expect(result.acceptedGain).toBe(true);
  expect(deltas([-20, -20, -20, -10, -10, -10, -10, -5, -5]).acceptedGain).toBe(false);
});

it("uses the same IQR and vote test in reverse for regressions", () => {
  const result = deltas([10, 10, 10, 10, 10, 10, 10, 0, -1000]);
  expect(result.significantRegression).toBe(true);
  expect(result.noRegression).toBe(false);
  expect(result.favoursControl).toBe(7);
  expect(result.iqrDeltaMs).toBe(0);
  expect(deltas(Array<number>(9).fill(0)).noRegression).toBe(true);
});

it("requires the favourable fraction even when the median beats IQR", () => {
  const result = deltas([-10, -10, -10, -10, -10, -10, -10, 0.1, 0.1, 0.1]);
  expect(-result.medianDeltaMs).toBeGreaterThan(result.iqrDeltaMs);
  expect(result.requiredFavourable).toBe(8);
  expect(result.favoursCandidate).toBe(7);
  expect(result.acceptedGain).toBe(false);
  const reverse = deltas([10, 10, 10, 10, 10, 10, 10, -0.1, -0.1, -0.1]);
  expect(reverse.medianDeltaMs).toBeGreaterThan(reverse.iqrDeltaMs);
  expect(reverse.significantRegression).toBe(false);
  expect(reverse.noRegression).toBe(true);
});

it("refuses incomplete and nonfinite pairs", () => {
  expect(() => pairedCPU(Array<number>(8).fill(1), Array<number>(8).fill(1))).toThrow(RangeError);
  expect(() => pairedCPU(Array<number>(9).fill(1), [1])).toThrow(RangeError);
  expect(() => pairedCPU(Array<number>(9).fill(1), Array<number>(9).fill(NaN))).toThrow(RangeError);
});

it("interpolates quartiles and the median for even sample counts", () => {
  const result = deltas([-10, -9, -8, -7, -6, -5, -4, -3, -2, -1]);
  expect(result.medianDeltaMs).toBe(-5.5);
  expect(result.q1DeltaMs).toBe(-7.75);
  expect(result.q3DeltaMs).toBe(-3.25);
  expect(result.iqrDeltaMs).toBe(4.5);
  expect(result.rangeDeltaMs).toBe(9);
  expect(result.acceptedGain).toBe(true);
});

it("replays every retained pair and the independently tabulated aggregation statistics", async () => {
  const { default: evidence } = await import(
    "./bench-evidence/aggregation-paired-2026-11-08.json",
    {
      with: { type: "json" },
    }
  );
  for (const [id, lane, median, iqr, range, wins, gain] of [
    ["agg_3dims_20aggs", "component", -76.731, 47.153, 140.913, 9, true],
    ["agg_3dims_20aggs", "whole", -31.575, 398.485, 589.357, 5, false],
    ["agg_highcard_int", "component", -55.578, 32.005, 51.571, 9, true],
    ["agg_highcard_int", "whole", -27.729, 159.009, 388.594, 8, false],
  ] as const) {
    const shape = evidence.shapes.find((item) => item.id === id);
    if (!shape) throw new Error("Missing paired shape");
    const stored = shape[lane].paired;
    const result = pairedCPU(
      stored.pairs.map((pair) => pair.control),
      stored.pairs.map((pair) => pair.candidate),
    );
    expect(result.pairs).toEqual(stored.pairs);
    expect(result.pairs).toHaveLength(9);
    expect(result.medianDeltaMs).toBeCloseTo(median, 6);
    expect(result.iqrDeltaMs).toBeCloseTo(iqr, 6);
    expect(result.rangeDeltaMs).toBeCloseTo(range, 6);
    expect(result.favoursCandidate).toBe(wins);
    expect(result.acceptedGain).toBe(gain);
    expect(result.noRegression).toBe(true);
    expect(stored).toEqual(result);
  }
});
