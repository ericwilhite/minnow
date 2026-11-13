import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, ENCODING_VERSION_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { decodeManifestRoot, encodeManifestRoot } from "./manifest-codec.js";
import type { ManifestRoot } from "./manifest.js";
const reach = createReach("unpublished format refusal", ENCODING_VERSION_STATES);
for (const seed of seedsFor("next-encoding-versions", [0xb502]))
  it(`typed refusal before use (${String(seed)})`, () => {
    fc.assert(
      fc.property(fc.constantFrom(0, 1, 2, 4, 255), (version) => {
        reach.beginHistory();
        let paid = 0;
        const budget = {
          allocate(n: number) {
            paid += n;
            return true;
          },
          free(n: number) {
            paid -= n;
          },
        };
        const root: ManifestRoot = {
          formatVersion: version,
          sequence: 1,
          epoch: 1,
          log: { salt: 1, firstLsn: 1, rootSequence: 1 },
          globalLsn: 0,
          catalogVersion: 1,
          catalog: { extent: { offset: 0, length: 4096 }, offset: 0, length: 1, checksum: 0 },
          nextTableId: 1,
          sequences: [],
          tables: [],
          clients: [],
          extentList: [],
        };
        let error: unknown;
        try {
          const decoded = decodeManifestRoot(encodeManifestRoot(root), budget);
          budget.free(decoded.charge);
        } catch (e) {
          error = e;
        }
        expect(error).toMatchObject({
          name: "UnsupportedEncodingError",
          code: "MINNOW_UNSUPPORTED_ENCODING",
          format: "manifest",
          version,
        });
        expect(paid).toBe(0);
        reach.reach(version < 3 ? "old development" : "newer");
        if (version === 2) reach.reach("previous root");
      }),
      { seed, numRuns: 40 },
    );
    reach.assertReached();
  });
