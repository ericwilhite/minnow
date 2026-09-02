import { describe, expect, it } from "vitest";
import {
  parsePerformanceFailures,
  repeatedPerformanceFailures,
} from "./lib/performance-confirmation.mts";

describe("performance regression confirmation", () => {
  it("extracts completed workload comparisons without mistaking other failures for noise", () => {
    expect(
      parsePerformanceFailures(`
2 performance regression(s):
  bulk-ingest vs sqlite: ratio 5.36 (sampled lower bound 5.24) exceeds threshold 5.21
  distinct-aggregate vs pglite: ratio 0.20 exceeds threshold 0.181
Error: setup failed
`),
    ).toEqual([
      { workload: "bulk-ingest", engine: "sqlite" },
      { workload: "distinct-aggregate", engine: "pglite" },
    ]);
    expect(parsePerformanceFailures("Error: setup failed")).toEqual([]);
  });

  it("confirms a workload that fails twice even when a different engine flags it", () => {
    const first = [
      { workload: "bulk-ingest", engine: "sqlite" as const },
      { workload: "scan", engine: "pglite" as const },
    ];
    const second = [
      { workload: "bulk-ingest", engine: "pglite" as const },
      { workload: "scan", engine: "pglite" as const },
    ];
    expect(repeatedPerformanceFailures(first, second)).toEqual([
      { workload: "bulk-ingest", engine: "sqlite" },
      { workload: "scan", engine: "pglite" },
      { workload: "bulk-ingest", engine: "pglite" },
    ]);
  });

  it("reads two runs that flag different workloads as one-off noise", () => {
    expect(
      repeatedPerformanceFailures(
        [{ workload: "point-lookup", engine: "sqlite" }],
        [{ workload: "bulk-ingest", engine: "sqlite" }],
      ),
    ).toEqual([]);
  });
});
