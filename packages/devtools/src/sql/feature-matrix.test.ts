import matrix from "@minnowdb/core/sql-feature-matrix.json" with { type: "json" };
import { describe, expect, it } from "vitest";
import {
  buildFailureIndex,
  describeUnsupported,
  lookupFailure,
  type UnsupportedFeatureRecord,
} from "./feature-matrix.js";
import { unsupportedFeatures } from "./unsupported-features.js";

const features: UnsupportedFeatureRecord[] = [
  { id: "a.one", error: "Only mine", notes: "Use the other thing." },
  { id: "a.two", error: "No note here" },
  { id: "b.one", error: "Shared" },
  { id: "b.two", error: "Shared", notes: "Never seen." },
];

describe("buildFailureIndex", () => {
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

/**
 * The shipped module is a generated slice of core's matrix — the unsupported features that carry
 * an error fragment — so it has to say exactly what the matrix says. This is the check that keeps
 * it current: when the matrix changes, `npm run devtools:matrix` rewrites the module.
 */
describe("the generated unsupported-features module", () => {
  it("matches the shipped matrix", () => {
    const expected = matrix.features
      .filter((feature) => feature.status === "unsupported" && feature.error !== undefined)
      .map((feature) => ({
        id: feature.id,
        error: feature.error ?? "",
        ...(feature.notes === undefined ? {} : { notes: feature.notes }),
      }))
      .sort((left, right) => left.id.localeCompare(right.id));
    expect(unsupportedFeatures).toEqual(expected);
  });

  it("explains the failures that a devtools user is most likely to hit", () => {
    const index = buildFailureIndex(unsupportedFeatures);
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
    expect(
      lookupFailure(buildFailureIndex(unsupportedFeatures), "Expected SELECT"),
    ).toBeUndefined();
  });
});
