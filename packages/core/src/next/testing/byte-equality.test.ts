import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { byteSliceEquals } from "./byte-equality.test.fixture.js";

it.each(seedsFor("next-oracle-byte-equality", [0xb4e1, 0xb4e2]))(
  "compares every word and trailing byte of unaligned views (seed %i)",
  (seed) => {
    const reach = createReach("portable byte equality", [
      "empty",
      "unaligned",
      "tail",
      "equal",
      "first difference",
      "last difference",
      "middle difference",
      "outside ignored",
    ]);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 7 }),
        fc.integer({ min: 0, max: 65 }),
        (from, length) => {
          reach.beginHistory();
          const storage = new Uint8Array(3 + from + length + 7).fill(77);
          const actual = storage.subarray(3);
          const expectedStorage = new Uint8Array(length + 2);
          const expected = expectedStorage.subarray(2);
          for (let i = 0; i < length; i++) expected[i] = (i * 73 + 57) & 255;
          actual.set(expected, from);
          expect(byteSliceEquals(actual, from, expected)).toBe(true);
          reach.reach("equal");
          if (length === 0) reach.reach("empty");
          if ((actual.byteOffset + from) % 4 !== 0) reach.reach("unaligned");
          if (length % 4) reach.reach("tail");
          for (let at = 0; at < length; at++) {
            const original = actual[from + at] ?? 0;
            for (let bit = 0; bit < 8; bit++) {
              actual[from + at] = original ^ (1 << bit);
              const reference = actual
                .subarray(from, from + length)
                .every((b, i) => b === expected[i]);
              expect(byteSliceEquals(actual, from, expected)).toBe(reference);
              expect(reference).toBe(false);
              if (at === 0) reach.reach("first difference");
              if (at === length - 1) reach.reach("last difference");
              if (at > 0 && at < length - 1) reach.reach("middle difference");
              actual[from + at] = original;
            }
          }
          actual[from + length] = 21;
          expect(byteSliceEquals(actual, from, expected)).toBe(true);
          reach.reach("outside ignored");
          expect(byteSliceEquals(actual, -1, expected)).toBe(false);
          expect(byteSliceEquals(actual, actual.length + 1, expected)).toBe(false);
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [0, 0],
          [0, 5],
          [2, 7],
          [1, 8],
        ],
      },
    );
    reach.assertReached();
  },
);
