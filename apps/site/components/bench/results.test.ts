import { expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import type { ReferenceSuiteResult, LiveSuiteResult } from "@/bench/protocol";
import { ReadResults, LiveResults } from "./results";

it("does not upgrade a legacy saved pass into a claim of complete read coverage", () => {
  const result: ReferenceSuiteResult = {
    datasetId: "legacy",
    scale: 1,
    secondaryIndexes: "none",
    sampleCount: 5,
    engines: ["minnow"],
    queries: [],
    totalMsByEngine: {},
    supportedByEngine: {},
    coverageByEngine: {},
    passed: true,
  };
  // Older saved JSON contains passed:true without the new coverage evidence.
  Reflect.deleteProperty(result, "coverageByEngine");
  const html = renderToStaticMarkup(createElement(ReadResults, { result, columns: [] }));
  expect(html).not.toContain("Every expected query");
  expect(html).toContain("unavailable in this saved run");
  expect(html).toContain("predates workload coverage counts");
});

it("does not upgrade a legacy saved pass into a claim of complete live coverage", () => {
  const result: LiveSuiteResult = {
    datasetId: "legacy",
    scale: 1,
    sampleCount: 7,
    engines: ["minnow"],
    cases: [],
    supportedByEngine: {},
    coverageByEngine: {},
    passed: true,
  };
  Reflect.deleteProperty(result, "coverageByEngine");
  const html = renderToStaticMarkup(createElement(LiveResults, { result, columns: [] }));
  expect(html).not.toContain("Every affected subscription");
  expect(html).toContain("unavailable in this saved run");
});
