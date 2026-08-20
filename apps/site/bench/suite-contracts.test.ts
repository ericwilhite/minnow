import { describe, expect, it } from "vitest";
import { engineIds, type WorkloadKind } from "./protocol";
import { referenceQueryDefinitions } from "./worker/reference-suite";
import { validateDatasetSuitePayload } from "./worker/support";
import { writeCaseDefinitions } from "./worker/write-suite";

describe("benchmark suite contracts", () => {
  it("limits benchmark comparisons to the storage peers", () => {
    expect(engineIds).toEqual(["minnow", "minnow-opfs", "sqlite", "pglite"]);
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
});
