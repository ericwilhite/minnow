/** The write-key work contract: at most one canonical traversal per ingress key.
 * This test uses only the parent's API, so its RED is an actual duplicate hash, not a missing API. */
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { createReach, WRITE_KEY_HASH_ONCE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import * as canonical from "../hash/xxhash32.js";
import { KeyDelta } from "./key-hash.js";

const reach = createReach("single write-key traversal", WRITE_KEY_HASH_ONCE_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("write-key-hash-once", [0x5eed, 0x91])) {
  it(`does not hash in both find and insert (${String(seed)})`, () => {
    const spy = vi.spyOn(canonical, "xxHash32");
    try {
      fc.assert(
        fc.property(
          fc.integer({ min: 1, max: 64 }),
          fc.uint8Array({ maxLength: 64 }),
          fc.nat({ max: 15 }),
          fc.boolean(),
          (count, suffix, lead, repeated) => {
            reach.beginHistory();
            reach.reach(count === 1 ? "one key" : "many keys");
            if (lead > 0) reach.reach("shifted range");
            if (repeated && count > 1) reach.reach("duplicate keys");
            const backing = new Uint8Array(lead + suffix.length + 8);
            const key = backing.subarray(lead);
            key.set(suffix, 4);
            const delta = new KeyDelta();
            spy.mockClear();
            try {
              for (let n = 0; n < count; n++) {
                key[3] = repeated ? 0 : n;
                delta.add(key, 3, 4 + suffix.length, n, 1, false);
              }
              expect(
                spy.mock.calls.length,
                "find and insert must share the ingress hash",
              ).toBeLessThanOrEqual(count);
            } finally {
              delta.releaseCharge();
            }
          },
        ),
        { seed, numRuns: 300 },
      );
    } finally {
      spy.mockRestore();
    }
  });
}
