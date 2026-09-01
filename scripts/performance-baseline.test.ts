import { describe, expect, it } from "vitest";
import {
  hasPerformanceRegression,
  parsePerformanceBaseline,
  performanceRatioRange,
  runtimePerformanceProfile,
  selectPerformanceThresholds,
  updatedPerformanceBaseline,
  type PerformanceBaselineFile,
} from "./lib/performance-baseline.mts";

const baseline: PerformanceBaselineFile = {
  schemaVersion: 1,
  rows: 200_000,
  profiles: {
    "darwin-arm64-node24": {
      thresholds: { scan: { sqlite: 0.5, pglite: 0.2 } },
    },
  },
};

describe("performance baseline profiles", () => {
  it("keys calibration by platform, architecture, and Node major", () => {
    expect(runtimePerformanceProfile("linux", "x64", "24.12.0")).toBe("linux-x64-node24");
    expect(() => runtimePerformanceProfile("linux", "x64", "next")).toThrow(
      "Cannot identify the Node major",
    );
  });

  it("validates the schema, row count, and finite positive thresholds", () => {
    expect(parsePerformanceBaseline(baseline, 200_000)).toEqual(baseline);
    expect(() => parsePerformanceBaseline(baseline, 10)).toThrow("this gate uses 10");
    expect(() =>
      parsePerformanceBaseline(
        {
          ...baseline,
          profiles: {
            broken: { thresholds: { scan: { sqlite: Number.NaN, pglite: 1 } } },
          },
        },
        200_000,
      ),
    ).toThrow("must be a positive finite number");
  });

  it("fails closed on missing runtime and workload calibration", () => {
    expect(() =>
      selectPerformanceThresholds(baseline, "linux-x64-node24", ["scan"], false),
    ).toThrow("No performance baseline");
    expect(() =>
      selectPerformanceThresholds(baseline, "darwin-arm64-node24", ["scan", "write"], false),
    ).toThrow("missing darwin-arm64-node24/write/sqlite");
    expect(
      selectPerformanceThresholds(baseline, "linux-x64-node24", ["scan"], true),
    ).toBeUndefined();
  });

  it("updates one profile without erasing another", () => {
    expect(
      updatedPerformanceBaseline(baseline, 200_000, "linux-x64-node24", {
        scan: { sqlite: 0.6, pglite: 0.3 },
      }).profiles,
    ).toEqual({
      ...baseline.profiles,
      "linux-x64-node24": {
        thresholds: { scan: { sqlite: 0.6, pglite: 0.3 } },
      },
    });
  });

  it("requires the full current timing range to cross a threshold", () => {
    const minnow = { median: 10, best: 8, worst: 12 };
    const engine = { median: 20, best: 18, worst: 24 };

    expect(performanceRatioRange(minnow, engine)).toEqual({
      typical: 0.5,
      lower: 1 / 3,
      upper: 2 / 3,
    });
    // The median moved beyond 0.4, but the samples still overlap the accepted range.
    expect(hasPerformanceRegression(minnow, engine, 0.4)).toBe(false);
    expect(hasPerformanceRegression(minnow, engine, 0.3)).toBe(true);
  });

  it("still catches a stable ratio beyond the threshold", () => {
    const minnow = { median: 10, best: 10, worst: 10 };
    const engine = { median: 20, best: 20, worst: 20 };

    expect(hasPerformanceRegression(minnow, engine, 0.49)).toBe(true);
    expect(hasPerformanceRegression(minnow, engine, 0.5)).toBe(false);
  });
});
