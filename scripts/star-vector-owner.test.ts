/** vector.ts's dictionary identity names an immutable prefix, not a fixed entry count. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createStarVectorOwner } from "./lib/star-fixture.mts";
import {
  newDictionaryId,
  type Vector,
  type FlatVector,
} from "../packages/core/dist/next/vector/vector.js";
import { valueAt } from "../packages/core/dist/next/exec/kernels/test-support.js";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, STAR_FIXTURE_PREFIX_REACH } from "../packages/core/src/testing/reach.js";

function dictionary(
  values: readonly string[],
  id: number,
): Extract<Vector, { form: "dictionary" }> {
  const chunks = values.map((v) => new TextEncoder().encode(v)),
    offsets = new Int32Array(values.length + 1);
  for (let i = 0; i < values.length; i++)
    offsets[i + 1] = (offsets[i] ?? 0) + (chunks[i]?.length ?? 0);
  const bytes = new Uint8Array(offsets[values.length] ?? 0);
  for (let i = 0; i < values.length; i++)
    bytes.set(chunks[i] ?? new Uint8Array(0), offsets[i] ?? 0);
  const flat: FlatVector = {
    form: "flat",
    type: { kind: "text" },
    length: values.length,
    validity: null,
    data: { layout: "varlen", bytes, offsets },
  };
  return {
    form: "dictionary",
    type: flat.type,
    length: values.length,
    validity: null,
    dictionaryId: id,
    dictionary: flat,
    codes: Int32Array.from(values, (_, i) => i),
  };
}
it.each(seedsFor("next-star-fixture-prefix", [0xb767, 0xb768]))(
  "captures owned growing dictionary prefixes (seed %i)",
  (seed) => {
    const reach = createReach("star fixture prefixes", STAR_FIXTURE_PREFIX_REACH);
    fc.assert(
      fc.property(
        fc.array(fc.constantFrom("", "a", "汉", "😀"), { minLength: 1, maxLength: 12 }),
        fc.constantFrom("new", "界", "🚀"),
        (prefix, tail) => {
          reach.beginHistory();
          const own = createStarVectorOwner(),
            id = newDictionaryId(),
            a = dictionary(prefix, id),
            b = dictionary([...prefix, tail], id),
            first = own(a),
            grown = own(b);
          if (first.form !== "dictionary" || grown.form !== "dictionary")
            throw new Error("Missing captured dictionary");
          expect(first.dictionary.length).toBe(prefix.length);
          expect(grown.dictionary.length).toBe(prefix.length + 1);
          expect(grown.dictionary).not.toBe(first.dictionary);
          const fixed = own(b);
          if (fixed.form !== "dictionary") throw new Error("Missing stable prefix");
          expect(fixed.dictionary).toBe(grown.dictionary);
          for (const source of [a, b]) {
            source.codes.fill(0);
            if (source.dictionary.data.layout !== "varlen")
              throw new Error("Unexpected fixture layout");
            source.dictionary.data.bytes.fill(0x71);
            source.dictionary.data.offsets.fill(0);
          }
          expect(prefix.map((_, i) => valueAt(first, i))).toEqual(prefix);
          expect([...prefix, tail].map((_, i) => valueAt(grown, i))).toEqual([...prefix, tail]);
          reach.reach("growing identity");
          reach.reach("old prefix intact");
          reach.reach("owned backing");
          reach.reach("stable prefix reused");
        },
      ),
      { seed, numRuns: 20, examples: [[["", "汉"], "🚀"]] },
    );
    reach.assertReached();
  },
);
