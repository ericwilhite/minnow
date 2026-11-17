/** Independent golden selectors: Stable-ID ROW reader10/writer10, manifest13/commitBody9.
 * These are development bytes, not a supported predecessor or an indexed-query claim.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_SELECTOR_REACH } from "../../testing/reach.js";
import { encodeRootSlot, decodeRootSlot } from "../storage/root-slot.js";
import { encodeBody, decodeBody } from "../write/body.js";

for (const seed of seedsFor("next-fts-selectors", [0xb817, 0xb818]))
  it(`emits the independently allocated catalog barriers (${String(seed)})`, () => {
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
            expect([...encoded], "commit-body9 ABI").toEqual([9, 0, 0]);
            expect(decodeBody(encoded)).toEqual({ request: null, effects: [] });
            reach.reach("commit body9");
          };
          if (seed & 1) checkBody();
          const encoded = encodeRootSlot(sequence, payload);
          expect((encoded[10] ?? 0) >>> 5, "u32 capability escape ABI").toBe(7);
          const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
          expect(view.getUint32(16, true), "reader10 ABI").toBe(10);
          reach.reach("reader10 barrier");
          expect(view.getUint32(20, true), "writer10 ABI").toBe(10);
          reach.reach("writer10 barrier");
          expect(encoded[11], "manifest13 ABI").toBe(11);
          reach.reach("manifest13 barrier");
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
