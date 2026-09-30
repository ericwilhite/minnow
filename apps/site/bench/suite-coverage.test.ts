import { expect, it } from "vitest";
import { suiteCoverage, summarizeSamples } from "./worker/support";
it("requires exactly one result for every declared workload and engine", () => {
  const good = { engine: "minnow" as const, supported: true, verified: true };
  expect(suiteCoverage(["minnow"], ["a"], [{ id: "a", engines: [good] }]).passed).toBe(true);
  for (const reports of [
    [],
    [{ id: "other", engines: [good] }],
    [{ id: "a", engines: [] }],
    [{ id: "a", engines: [good, good] }],
    [{ id: "a", engines: [good, { ...good, engine: "sqlite" as const }] }],
    [
      { id: "a", engines: [good] },
      { id: "a", engines: [good] },
    ],
  ]) {
    expect(suiteCoverage(["minnow"], ["a"], reports).passed).toBe(false);
  }
  expect(suiteCoverage([], [], []).passed).toBe(false);
  expect(suiteCoverage(["minnow", "minnow"], ["a"], [{ id: "a", engines: [good] }]).passed).toBe(
    false,
  );
});
it("reports failed attempts separately from supported and verified measurements", () => {
  expect(
    suiteCoverage(
      ["minnow"],
      ["a", "b"],
      [
        { id: "a", engines: [{ engine: "minnow", supported: true, verified: true }] },
        { id: "b", engines: [{ engine: "minnow", supported: true, verified: false }] },
      ],
    ).coverageByEngine.minnow,
  ).toEqual({ expected: 2, attempted: 2, supported: 2, verified: 1, failed: 1 });
});
it("labels the small-sample percentile by its actual statistic", () => {
  expect(summarizeSamples([5, 1, 3, 2, 4])).toEqual({ medianMs: 3, p95Ms: 5 });
});
