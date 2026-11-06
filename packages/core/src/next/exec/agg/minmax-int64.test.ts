/** MIN/MAX compare int64 values exactly, independently of the vector's physical tier. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import {
  I64,
  TS,
  UNCHARGED,
  bigints,
  dictionary,
  numbers,
  valueAt,
} from "../kernels/test-support.js";
import { IDENTITY } from "../kernels/vectors.js";
import { accumulatorFor } from "./states.js";

const EDGE = 2n ** 53n;
const values = fc.constantFrom(
  -EDGE - 1n,
  -EDGE,
  -EDGE + 1n,
  -1n,
  0n,
  1n,
  EDGE - 1n,
  EDGE,
  EDGE + 1n,
);
it.each(seedsFor("next-agg-minmax-int64", [0x164, 0x165]))(
  "int64 MIN/MAX preserve unsafe ties and transitions (seed %i)",
  (seed) => {
    const reach = createReach("next-agg-minmax-int64", [
      "safe",
      "unsafe",
      "dictionary",
      "wide tie",
      "grouped",
      "scalar",
    ] as const);
    fc.assert(
      fc.property(
        fc.array(values, { minLength: 4, maxLength: 64 }),
        fc.boolean(),
        (input, encoded) => {
          reach.beginHistory();
          for (const type of [I64, TS])
            for (const fn of ["min", "max"] as const)
              for (const scalar of [true, false]) {
                reach.reach(scalar ? "scalar" : "grouped");
                const state = accumulatorFor(fn, type, type, UNCHARGED);
                const groups = scalar ? 1 : 2;
                state.grow(groups);
                const expected: Array<bigint | null> = Array.from({ length: groups }, () => null);
                // Safe prefix followed by mixed values, unsafe adjacent values, then a safe suffix.
                for (const part of [
                  [1n, 0n],
                  input,
                  [EDGE, EDGE + 1n],
                  [-EDGE, -EDGE - 1n],
                  [-1n],
                ]) {
                  const safe = part.every((v) => v > -EDGE && v < EDGE);
                  reach.reach(safe ? "safe" : "unsafe");
                  if (part.includes(EDGE) && part.includes(EDGE + 1n)) reach.reach("wide tie");
                  const flat = safe ? numbers(type, part.map(Number)) : bigints(type, part);
                  const vector = encoded
                    ? dictionary(
                        flat,
                        part.map((_, i) => i),
                      )
                    : flat;
                  if (encoded) reach.reach("dictionary");
                  const gids = Int32Array.from(part, (_, i) => i % groups);
                  state.update(vector, gids, IDENTITY, part.length, scalar);
                  for (let i = 0; i < part.length; i++) {
                    const v = part[i] ?? 0n;
                    const g = i % groups;
                    const old = expected[g] ?? null;
                    if (old === null || (fn === "min" ? v <= old : v >= old)) expected[g] = v;
                  }
                  const out = state.emit(0, groups);
                  for (let g = 0; g < groups; g++) {
                    const actual = valueAt(out, g);
                    expect(actual === null ? null : BigInt(actual as bigint | number)).toBe(
                      expected[g],
                    );
                  }
                }
                state.close();
              }
        },
      ),
      { seed, numRuns: 100 },
    );
    reach.assertReached();
  },
);
