/** Frozen data/query fingerprints from the reviewed Gate B catalogue workload. */
import { createHash } from "node:crypto";
import { expect, it } from "vitest";
import { wideSpec, wideBatch, dimTables } from "./gen.mjs";
import * as queries from "./queries.mjs";

const hash = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
it.each([
  [200, "752c2c8268e21c94d9d66f746a033dafcf0ebc8d0d04da66bd88bef32566ca3e"],
  [500, "964ab50db39d8670823824346c80014605a653b35f1466bfc2a9432e16e0ac7d"],
] as const)("keeps the reviewed %i-column generated workload", (width, expected) => {
  const spec = wideSpec(width);
  expect(
    hash([spec, ...[0, 12345, 999992].map((start) => wideBatch(spec, start, 8, 1000000))]),
  ).toBe(expected);
});
it("keeps the dimension rows and SQL catalogue", () => {
  expect(hash(dimTables())).toBe(
    "9a65d5fcfe0538b0ed2352049803399ae536011916a3f64f7b0b03581b646c2c",
  );
  expect(hash([queries.W_QUERIES, queries.S_QUERIES, queries.S_TABLES])).toBe(
    "8bca028db9e008c0d6f67128614d1948c02500f71db90b32d3d7711bef311a8b",
  );
});
