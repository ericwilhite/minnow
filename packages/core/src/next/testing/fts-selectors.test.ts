/** Independent golden selectors allocated by the lead: reader4/manifest5/commitBody3.
 * These are development bytes, not a supported predecessor or an indexed-query claim.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_SELECTOR_REACH } from "../../testing/reach.js";
import { encodeRootSlot, decodeRootSlot } from "../storage/root-slot.js";
import { encodeBody, decodeBody } from "../write/body.js";

for (const seed of seedsFor("next-fts-selectors", [0xb817, 0xb818]))
  it(`emits the independently allocated FTS barriers (${String(seed)})`, () => {
    const reach = createReach("FTS selectors", FTS_SELECTOR_REACH);
    fc.assert(
      fc.property(
        fc.oneof(
          fc.integer({ min: 1, max: 0xffffffff }),
          fc.integer({ min: 0, max: 4096 }).map((n) => Number.MAX_SAFE_INTEGER - n),
        ),
        fc.uint8Array({ maxLength: 32 }),
        (sequence, payload) => {
          reach.beginHistory();
          const checkBody = () => {
            const encoded = encodeBody({ request: null, effects: [] });
            expect([...encoded], "commit-body3 ABI").toEqual([3, 0, 0]);
            expect(decodeBody(encoded)).toEqual({ request: null, effects: [] });
            reach.reach("commit body3");
          };
          if (seed & 1) checkBody();
          const encoded = encodeRootSlot(sequence, payload);
          expect((encoded[10] ?? 0) >>> 5, "minimum-reader4 ABI").toBe(3);
          reach.reach("reader4 barrier");
          expect(encoded[11], "manifest5 ABI").toBe(3);
          reach.reach("manifest5 barrier");
          const decoded = decodeRootSlot(encoded);
          expect(decoded?.sequence).toBe(sequence);
          expect(decoded?.root).toEqual(payload);
          if (sequence > 2 ** 32) reach.reach("wide sequence preserved");
          reach.reach("checksummed roundtrip");
          if (!(seed & 1)) checkBody();
        },
      ),
      { seed, numRuns: 200 },
    );
    reach.assertReached();
  });
