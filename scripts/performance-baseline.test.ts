import { describe, expect, it } from "vitest";
import {
  parsePerformanceBaseline,
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
});
