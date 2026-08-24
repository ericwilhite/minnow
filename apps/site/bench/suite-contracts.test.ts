import { describe, expect, it } from "vitest";
import { engineIds, type WorkloadKind } from "./protocol";
import { liveCaseDefinitions } from "./worker/live-suite";
import { validateCreatePayload } from "./worker/datasets";
import { referenceQueryDefinitions } from "./worker/reference-suite";
import { validateDatasetSuitePayload } from "./worker/support";
import { writeCaseDefinitions } from "./worker/write-suite";

describe("benchmark suite contracts", () => {
  it("limits benchmark comparisons to the storage peers", () => {
    expect(engineIds).toEqual(["minnow", "minnow-opfs", "sqlite", "pglite"]);
  });

  it("scales the live-query cases from one subscription to a hundred", () => {
    const cases = liveCaseDefinitions();
    expect(cases.map(({ subscriptions, affected }) => [subscriptions, affected])).toEqual([
      [1, 1],
      [10, 10],
      [100, 100],
      [100, 1],
    ]);
    // Every case names how many of its subscriptions the commit touches, never more than exist.
    expect(
      cases.every(({ subscriptions, affected }) => affected >= 1 && affected <= subscriptions),
    ).toBe(true);
    expect(new Set(cases.map(({ id }) => id)).size).toBe(cases.length);
  });

  it("keeps OLTP and OLAP coverage explicit for reads and writes", () => {
    const reads = referenceQueryDefinitions(1_000);
    const writes = writeCaseDefinitions();
    const workloads = (values: ReadonlyArray<{ workload: WorkloadKind }>): WorkloadKind[] =>
      [...new Set(values.map(({ workload }) => workload))].sort();

    expect(workloads(reads)).toEqual(["olap", "oltp"]);
    expect(workloads(writes)).toEqual(["olap", "oltp"]);
    expect(writes.every(({ rows, workload }) => workload === (rows <= 100 ? "oltp" : "olap"))).toBe(
      true,
    );
  });

  it("validates dataset suite payloads once for every suite", () => {
    expect(
      validateDatasetSuitePayload({
        datasetId: "dataset-1",
        engines: ["minnow", "sqlite", "minnow"],
      }),
    ).toEqual({ datasetId: "dataset-1", engines: ["minnow", "sqlite"] });
    expect(() => validateDatasetSuitePayload({ datasetId: "", engines: ["minnow"] })).toThrow(
      "Dataset id must be a non-empty string",
    );
    expect(() =>
      validateDatasetSuitePayload({ datasetId: "dataset-1", engines: ["unknown"] }),
    ).toThrow("Select at least one valid engine");
  });

  it("requires every dataset to declare its secondary-index mode", () => {
    const base = {
      scale: 1,
      compression: "gzip",
      targetBlockBytes: 1_048_576,
      durability: "relaxed",
      engines: ["minnow", "sqlite"],
    };
    expect(validateCreatePayload({ ...base, secondaryIndexes: "none" })).toMatchObject({
      secondaryIndexes: "none",
    });
    expect(validateCreatePayload({ ...base, secondaryIndexes: "foreign-keys" })).toMatchObject({
      secondaryIndexes: "foreign-keys",
    });
    expect(() => validateCreatePayload(base)).toThrow("Invalid secondary-index mode");
  });
});
