/** The sealed disposition is durable, canonical, and fixed-width at the body's end. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_CLAIM_CODEC_REACH } from "../../testing/reach.js";
import { encodeBody, decodeBody } from "../write/body.js";
import { LogCorruptionError } from "../format/log-frame.js";
for (const seed of seedsFor("next-fts-claim-codec", [0xb881, 0xb882]))
  it(`preserves the canonical disposition and rejects malformed flags seed ${String(seed)}`, () => {
    const reach = createReach("FTS claim codec", FTS_CLAIM_CODEC_REACH);
    fc.assert(
      fc.property(
        fc.constantFrom(1, 127, 128, 16384, Number.MAX_SAFE_INTEGER),
        fc.boolean(),
        (transaction, invalidated) => {
          reach.beginHistory();
          const body = {
              request: null,
              effects: [
                { kind: "fts" as const, transaction, rootSequence: transaction, invalidated },
              ],
            },
            bytes = encodeBody(body);
          expect(bytes.at(-1)).toBe(invalidated ? 1 : 0);
          expect(decodeBody(bytes)).toEqual(body);
          reach.reach(invalidated ? "invalidated flag" : "valid flag");
          if (transaction > 127) reach.reach("wide identity");
          const other = encodeBody({
            ...body,
            effects: [
              { kind: "fts", transaction, rootSequence: transaction, invalidated: !invalidated },
            ],
          });
          expect(other.length).toBe(bytes.length);
          expect(other.subarray(0, -1)).toEqual(bytes.subarray(0, -1));
          reach.reach("fixed final byte");
          for (const flag of [2, 127, 128, 255]) {
            const corrupt = bytes.slice();
            corrupt[corrupt.length - 1] = flag;
            expect(() => decodeBody(corrupt)).toThrow(LogCorruptionError);
          }
          reach.reach("reserved flags refused");
          expect(() => decodeBody(bytes.subarray(0, -1))).toThrow(LogCorruptionError);
          reach.reach("missing flag refused");
          const old = bytes.slice();
          old[0] = 7;
          expect(() => decodeBody(old)).toThrow(LogCorruptionError);
          reach.reach("development predecessor refused");
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [1, false],
          [Number.MAX_SAFE_INTEGER, true],
        ],
      },
    );
    reach.assertReached();
  });
