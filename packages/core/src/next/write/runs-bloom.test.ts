/**
 * The Bloom filter encoder (perf step 2) sets exactly the bits of the stored formula, runs.ts
 * "Stored form": probe p of a key sets bit (h1 + p·h2) mod bits. Generated (fast-check,
 * `seedsFor`): partitions of up to 600 keys with arbitrary 32-bit hash pairs.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { encodeBloom } from "./runs.js";
import { createReach, RUN_BLOOM_STEP_STATES } from "../../testing/reach.js";

const reach = createReach("stored run Bloom probes", RUN_BLOOM_STEP_STATES);

function reference(hashes: Uint32Array, count: number): Uint8Array {
  const keys = count / 2;
  const bits = Math.max(64, Math.ceil((keys * 10) / 8) * 8);
  const out = new Uint8Array(5 + bits / 8);
  new DataView(out.buffer).setUint32(0, bits, true);
  out[4] = 7;
  for (let k = 0; k < count; k += 2) {
    const h1 = hashes[k] ?? 0;
    const h2 = hashes[k + 1] ?? 0;
    for (let p = 0; p < 7; p++) {
      const bit = (h1 + p * h2) % bits;
      out[5 + (bit >>> 3)] = (out[5 + (bit >>> 3)] ?? 0) | (1 << (bit & 7));
    }
  }
  return out;
}

describe("encodeBloom", () => {
  for (const seed of seedsFor("next-write-run-bloom", [0x5b1f]))
    it(`sets the stored formula's bits (seed ${String(seed)})`, () => {
      fc.assert(
        fc.property(
          fc.array(fc.tuple(fc.nat({ max: 2 ** 32 - 1 }), fc.nat({ max: 2 ** 32 - 1 })), {
            maxLength: 600,
          }),
          (pairs) => {
            reach.beginHistory();
            const bits = Math.max(64, Math.ceil((pairs.length * 10) / 8) * 8);
            reach.reach(bits === 64 ? "minimum bitmap" : "rounded bitmap");
            for (const [h1, h2] of pairs) {
              if (h1 >= 2 ** 31) reach.reach("unsigned high hash");
              if ((h1 % bits) + (((h2 | 1) >>> 0) % bits) >= bits) reach.reach("wrapped step");
            }
            const hashes = Uint32Array.from(pairs.flatMap(([h1, h2]) => [h1, (h2 | 1) >>> 0]));
            expect(encodeBloom(hashes, hashes.length)).toEqual(reference(hashes, hashes.length));
          },
        ),
        { seed, numRuns: 1000 },
      );
    });
  it("reaches the stored-probe boundary states", () => reach.assertReached());
});
