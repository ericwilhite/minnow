/** PC2: nested codec extensions need a new outer reader profile before storage can mutate. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, NATIVE_PROFILE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  DURABLE_READER_VERSION,
  MANIFEST_FORMAT_VERSION,
  NATIVE_READER_PROFILES,
  nativeVocabularyFingerprint,
  requireNativeReaderProfile,
} from "../durable-format.js";

// Independent ledger pin: changing the grammar's fingerprint on an existing profile must not
// make a new nested tag look compatible. Before 1.0 these are development bytes; first frozen
// fixtures will pin the agreed released profiles and all later entries are append-only.
const known = { 7: { manifest: 8, vocabulary: 2414547993 } };
for (const seed of seedsFor("next-native-profile", [0xb2a1, 0xb2a2]))
  it(`refuses an unassigned native vocabulary before acquisition seed ${String(seed)}`, () => {
    const reach = createReach("outer native profile binding", NATIVE_PROFILE_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 31 }),
        fc.integer({ min: 9, max: 255 }),
        (bit, unsupported) => {
          reach.beginHistory();
          expect(NATIVE_READER_PROFILES).toEqual(known);
          expect(Object.isFrozen(NATIVE_READER_PROFILES)).toBe(true);
          expect(Object.isFrozen(NATIVE_READER_PROFILES[DURABLE_READER_VERSION])).toBe(true);
          const vocabulary = nativeVocabularyFingerprint();
          expect(vocabulary).toBe(known[7].vocabulary);
          expect(() =>
            requireNativeReaderProfile(DURABLE_READER_VERSION, MANIFEST_FORMAT_VERSION, vocabulary),
          ).not.toThrow();
          reach.reach("assigned vocabulary");
          expect(() =>
            requireNativeReaderProfile(
              DURABLE_READER_VERSION,
              MANIFEST_FORMAT_VERSION,
              (vocabulary ^ (1 << bit)) >>> 0,
            ),
          ).toThrow(expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }));
          reach.reach("unassigned nested vocabulary");
          expect(() =>
            requireNativeReaderProfile(unsupported, MANIFEST_FORMAT_VERSION, vocabulary),
          ).toThrow(expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }));
          reach.reach("unassigned reader");
          expect(() =>
            requireNativeReaderProfile(DURABLE_READER_VERSION, unsupported, vocabulary),
          ).toThrow(expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }));
          reach.reach("manifest mismatch");
        },
      ),
      { seed, numRuns: 32 },
    );
    reach.assertReached();
  });
