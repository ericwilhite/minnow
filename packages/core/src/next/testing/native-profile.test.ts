/** PC2: nested codec extensions need a new outer reader profile before storage can mutate. */
import fc from "fast-check";
import { expect, it } from "vitest";
import {
  createReach,
  NATIVE_PROFILE_REACH,
  NATIVE_WRITER_PROFILE_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  CATALOG_RECORD_SCHEMAS,
  CATALOG_REQUIRED_FEATURES,
  DURABLE_READER_VERSION,
  DURABLE_WRITER_VERSION,
  NATIVE_WRITER_PROFILES,
  requireNativeWriterProfile,
  MANIFEST_FORMAT_VERSION,
  NATIVE_READER_PROFILES,
  nativeVocabularyFingerprint,
  requireNativeReaderProfile,
} from "../durable-format.js";

// Independent ledger pin: changing the grammar's fingerprint on an existing profile must not
// make a new nested tag look compatible. Before 1.0 these are development bytes; first frozen
// fixtures will pin the agreed released profiles and all later entries are append-only.
const known = {
  7: { manifest: 10, vocabulary: 3713904595 },
  8: { manifest: 11, vocabulary: 1164132746 },
  9: { manifest: 12, vocabulary: 131312130 },
  10: { manifest: 13, vocabulary: 3997453788 },
  11: { manifest: 14, vocabulary: 3266998012 },
  12: { manifest: 15, vocabulary: 1157799046 },
};
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
          expect(DURABLE_READER_VERSION).toBe(12);
          expect(MANIFEST_FORMAT_VERSION).toBe(15);
          expect(vocabulary).toBe(known[12].vocabulary);
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
          // Preserve9..255:12 is now assigned, so assert its acceptance rather than removing it
          // from the generator. Every history also exercises mismatched older selectors.
          for (const selector of [unsupported, 7, 8, 10, 11, 12, 255]) {
            const check = () =>
              requireNativeReaderProfile(selector, MANIFEST_FORMAT_VERSION, vocabulary);
            if (selector === 12) {
              expect(check).not.toThrow();
              reach.reach("assigned selector in preserved population");
            } else {
              expect(check).toThrow(expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }));
              reach.reach("unassigned reader");
            }
          }
          // Keep the original generated population; the current known manifest15 is not
          // an unsupported selector. Also witness every superseded pinned development layout.
          for (const manifest of [unsupported, 8, 9, 10, 11, 12, 13, 14, 255]) {
            if (manifest === MANIFEST_FORMAT_VERSION) continue;
            expect(() =>
              requireNativeReaderProfile(DURABLE_READER_VERSION, manifest, vocabulary),
            ).toThrow(expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }));
          }
          reach.reach("manifest mismatch");
          const required = {
              ...CATALOG_RECORD_SCHEMAS,
              table: [...CATALOG_RECORD_SCHEMAS.table, "future:u~!"],
            },
            optional = {
              ...CATALOG_RECORD_SCHEMAS,
              table: [...CATALOG_RECORD_SCHEMAS.table, "future:u~"],
            },
            changedWire = {
              ...CATALOG_RECORD_SCHEMAS,
              table: CATALOG_RECORD_SCHEMAS.table.map((field, at) => (at === 0 ? "id:s" : field)),
            };
          for (const changed of [
            nativeVocabularyFingerprint(required),
            nativeVocabularyFingerprint(changedWire),
            nativeVocabularyFingerprint(CATALOG_RECORD_SCHEMAS, [
              ...CATALOG_REQUIRED_FEATURES,
              { id: unsupported, section: 3 },
            ]),
          ]) {
            expect(changed).not.toBe(vocabulary);
            expect(() =>
              requireNativeReaderProfile(DURABLE_READER_VERSION, MANIFEST_FORMAT_VERSION, changed),
            ).toThrow(expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }));
          }
          expect(nativeVocabularyFingerprint(optional)).toBe(vocabulary);
          expect(
            nativeVocabularyFingerprint({
              ...CATALOG_RECORD_SCHEMAS,
              table: CATALOG_RECORD_SCHEMAS.table.map((field, at) =>
                at === 0 ? "renamedId:u" : field,
              ),
            }),
          ).toBe(vocabulary);
          reach.reach("required catalog field");
          reach.reach("read-optional field");
          reach.reach("catalog wire type");
          reach.reach("required feature");
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [0, 9],
          [31, 255],
        ],
      },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-native-writer-profile", [0xb2c5, 0xb2c6]))
  it(`requires a new writer gate for read-optional fields seed ${String(seed)}`, () => {
    const reach = createReach("outer rewrite profile", NATIVE_WRITER_PROFILE_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 31 }),
        fc.integer({ min: 9, max: 255 }),
        (bit, future) => {
          reach.beginHistory();
          const knownWriter = {
            8: { manifest: 11, vocabulary: 1164132746 },
            9: { manifest: 12, vocabulary: 131312130 },
            10: { manifest: 13, vocabulary: 3997453788 },
            11: { manifest: 14, vocabulary: 3266998012 },
            12: { manifest: 15, vocabulary: 1157799046 },
          };
          expect(DURABLE_WRITER_VERSION).toBe(12);
          expect(NATIVE_WRITER_PROFILES).toEqual(knownWriter);
          const current = nativeVocabularyFingerprint(
            CATALOG_RECORD_SCHEMAS,
            CATALOG_REQUIRED_FEATURES,
            "write",
          );
          expect(MANIFEST_FORMAT_VERSION).toBe(15);
          expect(current).toBe(knownWriter[12].vocabulary);
          requireNativeWriterProfile(DURABLE_WRITER_VERSION, MANIFEST_FORMAT_VERSION, current);
          reach.reach("assigned rewrite vocabulary");
          for (const selector of [future, 8, 10, 11, 12, 255]) {
            const check = () =>
              requireNativeWriterProfile(selector, MANIFEST_FORMAT_VERSION, current);
            if (selector === 12) {
              expect(check).not.toThrow();
              reach.reach("assigned selector in preserved population");
            } else
              expect(check).toThrow(expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }));
          }
          expect(() =>
            requireNativeWriterProfile(
              DURABLE_WRITER_VERSION,
              MANIFEST_FORMAT_VERSION,
              (current ^ (1 << bit)) >>> 0,
            ),
          ).toThrow(expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }));
          reach.reach("unassigned writer");
          reach.reach("unassigned rewrite vocabulary");
          // All record kinds, not only table fields, carry rewrite obligations.
          for (const name of Object.keys(CATALOG_RECORD_SCHEMAS)) {
            const extended = {
              ...CATALOG_RECORD_SCHEMAS,
              [name]: [
                ...CATALOG_RECORD_SCHEMAS[name as keyof typeof CATALOG_RECORD_SCHEMAS],
                "future:u~",
              ],
            };
            expect(nativeVocabularyFingerprint(extended)).toBe(nativeVocabularyFingerprint());
            const changed = nativeVocabularyFingerprint(
              extended,
              CATALOG_REQUIRED_FEATURES,
              "write",
            );
            expect(changed).not.toBe(current);
            expect(() =>
              requireNativeWriterProfile(DURABLE_WRITER_VERSION, MANIFEST_FORMAT_VERSION, changed),
            ).toThrow(expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }));
          }
          reach.reach("read optional field requires writer gate");
          reach.reach("all record kinds");
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [0, 9],
          [31, 255],
        ],
      },
    );
    reach.assertReached();
  });
