/** PostgreSQL array equality includes dimensions, bounds, NULLs and element identity. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, JSON_BYTE_ARRAY_GROUP_REACH } from "../../../testing/reach.js";
import type { PhysicalType } from "../../types.js";
import type { Vector } from "../../vector/vector.js";
import { encodeArrayValue, type ArrayValue } from "../../vector/array-value.js";
import { equalByteArrayKeys } from "./json-array-key.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("CREATE TYPE b6_key_enum AS ENUM ('a', '汉', 'NULL')");
}, 60000);
afterAll(async () => {
  await pg.close();
});
const labels = ["a", "汉", "NULL"] as const;
const utf8 = new TextEncoder();
function wire(element: PhysicalType, value: ArrayValue, bitmap: boolean, isNull: boolean): Vector {
  let bytes = encodeArrayValue(element, value);
  // A redundant all-valid bitmap is a legal representation of the same array.
  if (bitmap && bytes[3] === 0) {
    const at = 4 + 8 * value.dimensions.length;
    const count = Math.ceil(value.elements.length / 8);
    const widened = new Uint8Array(bytes.length + count);
    widened.set(bytes.subarray(0, at));
    widened[3] = 1;
    widened.set(bytes.subarray(at), at + count);
    bytes = widened;
  }
  return {
    form: "flat",
    type: { kind: "array", element },
    length: 1,
    validity: isNull ? Uint32Array.of(0) : null,
    data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
  };
}
function spelling(value: ArrayValue, binary: boolean): string {
  if (!value.dimensions.length) return "{}";
  let at = 0;
  const contents = (dimension: number): string =>
    "{" +
    Array.from({ length: value.dimensions[dimension] ?? 0 }, () => {
      if (dimension + 1 < value.dimensions.length) return contents(dimension + 1);
      const item = value.elements[at++];
      if (item === null) return "NULL";
      if (!(item instanceof Uint8Array)) throw new Error("Expected byte array element");
      const text = binary
        ? "\\x" + Array.from(item, (b) => b.toString(16).padStart(2, "0")).join("")
        : new TextDecoder().decode(item);
      return '"' + text.replaceAll("\\", "\\\\").replaceAll('"', '\\"') + '"';
    }).join(",") +
    "}";
  return (
    value.dimensions
      .map(
        (length, d) =>
          `[${String(value.lowerBounds[d])}:${String((value.lowerBounds[d] ?? 1) + length - 1)}]`,
      )
      .join("") +
    "=" +
    contents(0)
  );
}
it.each(seedsFor("next-json-byte-array-groups", [0xb73e, 0xb73f]))(
  "compares encoded array element views exactly (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "streamed JSON aggregate array keys",
      JSON_BYTE_ARRAY_GROUP_REACH,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("text", "bytes", "enum"),
        fc.constantFrom(0, 1, 2),
        fc.constantFrom("same", "value", "bounds", "shape", "null"),
        fc.integer({ min: -2, max: 2 }),
        fc.boolean(),
        fc.boolean(),
        async (kind, dimensions, change, lower, nullElement, outerNull) => {
          reach.beginHistory();
          const element: PhysicalType =
            kind === "enum"
              ? { kind: "enum", name: "b6_key_enum", labels: [...labels] }
              : kind === "bytes"
                ? { kind: "bytes" }
                : { kind: "text" };
          const shape = dimensions === 0 ? [] : dimensions === 1 ? [4] : [2, 2];
          const a: ArrayValue = {
            dimensions: shape,
            lowerBounds: shape.map(() => lower),
            elements:
              dimensions === 0
                ? []
                : [
                    utf8.encode("a"),
                    nullElement ? null : utf8.encode("汉"),
                    utf8.encode("NULL"),
                    utf8.encode("汉"),
                  ],
          };
          const b = {
            dimensions: [...a.dimensions],
            lowerBounds: [...a.lowerBounds],
            elements: [...a.elements],
          };
          if (dimensions !== 0) {
            if (change === "value") b.elements[0] = utf8.encode("汉");
            if (change === "bounds") b.lowerBounds[0] = lower + 1;
            if (change === "shape") {
              b.dimensions = dimensions === 1 ? [2, 2] : [4];
              b.lowerBounds = b.dimensions.map(() => lower);
            }
            if (change === "null") b.elements[0] = null;
          }
          const type = kind === "enum" ? "b6_key_enum" : kind === "bytes" ? "bytea" : "text";
          const expected = (
            await pg.query<{ v: boolean }>(
              `SELECT $1::${type}[] IS NOT DISTINCT FROM $2::${type}[] v`,
              [
                outerNull ? null : spelling(a, kind === "bytes"),
                outerNull && change === "same" ? null : spelling(b, kind === "bytes"),
              ],
            )
          ).rows[0]?.v;
          const actual = await equalByteArrayKeys(
            wire(element, a, false, outerNull),
            0,
            wire(element, b, true, outerNull && change === "same"),
            0,
            async () => undefined,
          );
          expect(actual).toBe(expected);
          reach.reach(kind);
          reach.reach(String(dimensions) + " dimensions");
          reach.reach(change);
          if (!nullElement && !outerNull && change === "same")
            reach.reach("redundant bitmap equality");
          if (nullElement) reach.reach("NULL elements");
          if (outerNull) reach.reach("NULL arrays");
          reach.reach("exact PG equality");
        },
      ),
      {
        seed,
        numRuns: 40,
        examples: [
          ["text", 1, "same", 1, false, false],
          ["bytes", 2, "bounds", 0, true, false],
          ["enum", 0, "same", 1, false, false],
          ["text", 2, "shape", -1, false, false],
          ["text", 1, "value", 1, true, false],
          ["bytes", 1, "null", 1, false, false],
          ["enum", 1, "same", 1, true, true],
          ["enum", 1, "null", 1, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
