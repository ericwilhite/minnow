import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import {
  cpuPerformanceModel,
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
  it("keys calibration by platform, architecture, Node major and CPU model", () => {
    expect(
      runtimePerformanceProfile("linux", "x64", "24.12.0", "AMD EPYC 9V45 96-Core Processor"),
    ).toBe("linux-x64-node24-amd-epyc-9v45-96-core");
    expect(runtimePerformanceProfile("linux", "x64", "24.12.0", "Intel(R) Xeon(R) 6975P-C")).toBe(
      "linux-x64-node24-intel-xeon-6975p-c",
    );
    expect(cpuPerformanceModel("Intel(R) Core(TM) i5-8279U CPU @ 2.40GHz")).toBe(
      "intel-core-i5-8279u",
    );
    expect(() => runtimePerformanceProfile("linux", "x64", "next", "AMD EPYC 7763")).toThrow(
      "Cannot identify the Node major",
    );
  });

  it("refuses a host whose CPU cannot be identified", () => {
    for (const model of [undefined, "", "unknown", "(R)"])
      expect(() => cpuPerformanceModel(model)).toThrow("Cannot identify the CPU model");
    expect(() => runtimePerformanceProfile("linux", "x64", "24.12.0", "")).toThrow(
      "Cannot identify the CPU model",
    );
  });

  it("never compares one CPU model's run with another's thresholds", () => {
    const amd = runtimePerformanceProfile(
      "linux",
      "x64",
      "24.1.0",
      "AMD EPYC 9V45 96-Core Processor",
    );
    const intel = runtimePerformanceProfile("linux", "x64", "24.1.0", "Intel(R) Xeon(R) 6975P-C");
    const file = updatedPerformanceBaseline(undefined, 200_000, amd, {
      scan: { sqlite: 0.5, pglite: 0.2 },
    });
    expect(selectPerformanceThresholds(file, amd, ["scan"], false)).toEqual({
      scan: { sqlite: 0.5, pglite: 0.2 },
    });
    expect(() => selectPerformanceThresholds(file, intel, ["scan"], false)).toThrow(
      `No performance baseline for ${intel}`,
    );
  });

  it("names every checked-in profile by its CPU model", () => {
    const file = parsePerformanceBaseline(
      JSON.parse(
        readFileSync(new URL("../packages/core/perf-baseline.json", import.meta.url), "utf8"),
      ),
      200_000,
    );
    for (const name of Object.keys(file.profiles))
      expect(name).toMatch(/^(?:linux|darwin|win32)-[a-z0-9]+-node\d+-[a-z0-9-]+$/);
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
