/** Body7 tag9 is a reference-free positive table/schema identity, not an optional claim. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, FTS_INLINE_CODEC_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { encodeBody, decodeBody } from "../write/body.js";
import { LogCorruptionError } from "../format/log-frame.js";
import { DURABLE_CODECS } from "../durable-format.js";
for (const seed of seedsFor("next-fts-inline-codec", [0xb877, 0xb878]))
  it(`decodes exact inline invalidations and refuses malformed identities ${String(seed)}`, () => {
    const reach = createReach("FTS inline codec", FTS_INLINE_CODEC_REACH);
    fc.assert(
      fc.property(
        fc.constantFrom(1, 127, 128, 16384, 2 ** 32 - 1),
        fc.constantFrom(1, 128, 2 ** 32 - 1),
        (tableId, schemaVersion) => {
          reach.beginHistory();
          const effect = { kind: "ftsMutation" as const, tableId, schemaVersion };
          const body = { request: null, effects: [effect] };
          const bytes = encodeBody(body);
          expect(bytes[3]).toBe(DURABLE_CODECS.effect.ftsMutation);
          expect(decodeBody(bytes)).toEqual(body);
          reach.reach("registered tag");
          if (tableId > 127 || schemaVersion > 127) reach.reach("wide identifiers");
          for (const field of ["tableId", "schemaVersion"] as const) {
            const invalid = encodeBody({
              request: null,
              effects: [{ ...effect, [field]: 0 }],
            });
            expect(() => decodeBody(invalid)).toThrow(LogCorruptionError);
            reach.reach(field === "tableId" ? "zero table refused" : "zero schema refused");
          }
          for (let end = 0; end < bytes.length; end++)
            expect(() => decodeBody(bytes.subarray(0, end))).toThrow(LogCorruptionError);
          reach.reach("truncation refused");
          const trailing = new Uint8Array(bytes.length + 1);
          trailing.set(bytes);
          expect(() => decodeBody(trailing)).toThrow(LogCorruptionError);
          reach.reach("trailing bytes refused");
        },
      ),
      { seed, numRuns: 32, examples: [[2 ** 32 - 1, 2 ** 32 - 1]] },
    );
    reach.assertReached();
  });
