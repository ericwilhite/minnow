import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach } from "../../../testing/reach.js";
import { Slice, runToEnd, runSliced } from "../slices.js";
import { selectNumber } from "./selection.js";

it.each(seedsFor("next-percentile-selection", [0xe151, 0xe152]))(
  "selects exact numeric ranks (seed %i)",
  (seed) => {
    const reach = createReach("percentile selection", [
      "empty guards",
      "ascending",
      "descending",
      "NaN",
      "infinities",
      "signed zero",
      "duplicates",
      "prefix untouched",
      "suffix untouched",
      "heap fallback",
      "partitions",
    ]);
    const value = fc.oneof(
      fc.integer({ min: -1000, max: 1000 }),
      fc.constantFrom(NaN, Infinity, -Infinity, -0, 0),
    );
    fc.assert(
      fc.property(
        fc.array(value, { minLength: 1, maxLength: 2048 }),
        fc.boolean(),
        fc.nat(),
        fc.boolean(),
        (data, descending, which, heap) => {
          reach.beginHistory();
          const rank = which % data.length;
          const sorted = data.slice().sort((a, b) => {
            const order = Number.isNaN(a)
              ? Number.isNaN(b)
                ? 0
                : 1
              : Number.isNaN(b)
                ? -1
                : a < b
                  ? -1
                  : a > b
                    ? 1
                    : 0;
            return descending ? -order : order;
          });
          const array = Float64Array.from([123456, ...data, 654321]);
          const result = runToEnd(
            selectNumber(
              array,
              1,
              array.length - 1,
              rank + 1,
              descending,
              new Slice(Infinity),
              heap ? 0 : undefined,
            ),
          );
          // SQL ORDER BY ties between signed zeros do not identify an input tuple.
          const expected = sorted[rank] ?? NaN;
          if (Number.isNaN(expected)) expect(Number.isNaN(result)).toBe(true);
          else if (expected === 0) expect(result === 0).toBe(true);
          else expect(result).toBe(expected);
          expect(array[0]).toBe(123456);
          expect(array[array.length - 1]).toBe(654321);
          reach.reach(heap ? "heap fallback" : "partitions");
          reach.reach(descending ? "descending" : "ascending");
          reach.reach("prefix untouched");
          reach.reach("suffix untouched");
          if (data.some(Number.isNaN)) reach.reach("NaN");
          if (data.some((x) => !Number.isNaN(x) && !Number.isFinite(x))) reach.reach("infinities");
          if (data.some((x) => Object.is(x, -0))) reach.reach("signed zero");
          if (new Set(data).size < data.length) reach.reach("duplicates");
        },
      ),
      {
        seed,
        numRuns: 66,
        examples: [
          [[NaN, Infinity, -Infinity, -0, 0, 1, 1], false, 0, false],
          [[NaN, Infinity, -Infinity, -0, 0, 1, 1], true, 3, true],
          [[1, 0, -1], true, 1, false],
          [[1, 0, -1], false, 1, true],
        ],
      },
    );
    expect(() =>
      runToEnd(selectNumber(new Float64Array(), 0, 0, 0, false, new Slice(Infinity))),
    ).toThrow(RangeError);
    reach.reach("empty guards");
    reach.assertReached();
  },
);

it.each(seedsFor("next-percentile-selection-cancel", [0xe155, 0xe156]))(
  "selection yields and observes cancellation (seed %i)",
  async (seed) => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 8192, max: 16384 }), fc.boolean(), async (n, heap) => {
        const values = Float64Array.from({ length: n }, (_, i) => i % 17),
          signal = new AbortController(),
          slice = new Slice(0);
        const work = runSliced(
          selectNumber(values, 0, n, n >>> 1, false, slice, heap ? 0 : undefined),
          slice,
          () => signal.signal.throwIfAborted(),
        );
        expect(work).toBeInstanceOf(Promise);
        signal.abort(new Error("cancel selection"));
        await expect(work).rejects.toThrow("cancel selection");
      }),
      {
        seed,
        numRuns: 4,
        examples: [
          [8192, false],
          [8192, true],
        ],
      },
    );
  },
);
