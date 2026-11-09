import { expect, it } from "vitest";
import { pairedCPU } from "./lib/paired-cpu.mts";

it("cancels shared load before measuring the spread", () => {
  const control = [100, 900, 200, 800, 300, 700, 400, 600, 500];
  const result = pairedCPU(
    control,
    control.map((value, i) => value - 50 + (i % 3)),
  );
  expect(result.medianDeltaMs).toBe(-49);
  expect(result.spreadDeltaMs).toBe(2);
  expect(result.favoursCandidate).toBe(9);
  expect(result.requiredFavourable).toBe(7);
  expect(result.acceptedGain).toBe(true);
  expect(result.noRegression).toBe(true);
});

it("requires a strict gain beyond the complete delta spread", () => {
  const control = Array<number>(9).fill(100);
  const result = pairedCPU(control, [90, 90, 90, 90, 90, 90, 90, 100, 100]);
  expect(result.medianDeltaMs).toBe(-10);
  expect(result.spreadDeltaMs).toBe(10);
  expect(result.favoursCandidate).toBe(7);
  expect(result.acceptedGain).toBe(false);
  expect(pairedCPU(control, Array<number>(9).fill(101)).noRegression).toBe(false);
  expect(pairedCPU(control, control).noRegression).toBe(true);
});

it("refuses incomplete and nonfinite pairs", () => {
  expect(() => pairedCPU(Array<number>(8).fill(1), Array<number>(8).fill(1))).toThrow(RangeError);
  expect(() => pairedCPU(Array<number>(9).fill(1), [1])).toThrow(RangeError);
  expect(() => pairedCPU(Array<number>(9).fill(1), Array<number>(9).fill(NaN))).toThrow(RangeError);
});

it("uses the two central deltas for an even number of pairs", () => {
  const result = pairedCPU(Array<number>(10).fill(100), [90, 91, 92, 93, 94, 95, 96, 97, 98, 99]);
  expect(result.medianDeltaMs).toBe(-5.5);
  expect(result.spreadDeltaMs).toBe(9);
  expect(result.requiredFavourable).toBe(8);
});
