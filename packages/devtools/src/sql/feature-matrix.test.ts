import matrix from "@minnowdb/core/sql-feature-matrix.json" with { type: "json" };
import { describe, expect, it } from "vitest";
import {
  buildFailureIndex,
  describeUnsupported,
  lookupFailure,
  type MatrixFeature,
} from "./feature-matrix.js";

const features: MatrixFeature[] = [
  { id: "a.one", status: "unsupported", error: "Only mine", notes: "Use the other thing." },
  { id: "a.two", status: "unsupported", error: "No note here" },
  { id: "b.one", status: "unsupported", error: "Shared" },
  { id: "b.two", status: "unsupported", error: "Shared", notes: "Never seen." },
  { id: "c.one", status: "supported", error: "Supported things are skipped" },
];

describe("buildFailureIndex", () => {
  it("indexes only unsupported features", () => {
    const index = buildFailureIndex(features);
    expect(index.has("Supported things are skipped")).toBe(false);
  });

  it("drops a fragment two features share, rather than guessing between them", () => {
    // `Expected SELECT` is the real case: DDL and transactions both report it, and so does a
    // perfectly supported DELETE sent through the read-only path.
    expect(buildFailureIndex(features).has("Shared")).toBe(false);
  });

  it("keeps a fragment that identifies one feature", () => {
    expect(buildFailureIndex(features).get("Only mine")).toEqual({
      id: "a.one",
      notes: "Use the other thing.",
    });
  });
});

describe("lookupFailure", () => {
  const index = buildFailureIndex(features);

  it("matches a fragment inside a longer message", () => {
    expect(lookupFailure(index, "boom: Only mine, sorry")?.id).toBe("a.one");
  });

  it("finds nothing for an unrelated failure", () => {
    expect(lookupFailure(index, "Ambiguous or missing column: nope")).toBeUndefined();
  });
});

describe("describeUnsupported", () => {
  it("reads as a sentence with or without notes", () => {
    expect(describeUnsupported({ id: "a.one", notes: "Use the other thing." })).toBe(
      "a.one is not supported: Use the other thing.",
    );
    expect(describeUnsupported({ id: "a.two" })).toBe("a.two is a known unsupported feature.");
  });
});

/** The shipped matrix is the source of truth, so the shape this relies on is worth asserting. */
describe("the shipped matrix", () => {
  it("explains the failures that a devtools user is most likely to hit", () => {
    const index = buildFailureIndex(matrix.features);
    for (const message of [
      "UPDATE requires a table with a unique key",
      // SET is a statement now; SERIALIZABLE is what a user still cannot ask for.
      "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE is not supported: the engine has one isolation level",
      "Expected SELECT, found GRANT",
    ]) {
      expect(lookupFailure(index, message)?.notes).toBeDefined();
    }
  });

  it("refuses to explain Expected SELECT, which several features share", () => {
    // A supported DELETE run through query() reports this; attaching a DDL note would mislead.
    expect(lookupFailure(buildFailureIndex(matrix.features), "Expected SELECT")).toBeUndefined();
  });
});
