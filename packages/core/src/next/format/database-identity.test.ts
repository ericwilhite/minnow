import { ReadDeniedError } from "../storage/extents.js";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DATABASE_IDENTITY_REACH } from "../../testing/reach.js";
import { ByteReader, ByteWriter, ManifestCorruptionError } from "./bytes.js";
import {
  MAX_DATABASE_ID,
  readDatabaseIdentity,
  writeDatabaseIdentity,
  nextDatabaseIdentity,
  freshDatabaseIdentity,
  DatabaseIdentityExhaustedError,
} from "./database-identity.js";

for (const seed of seedsFor("next-database-identity", [0xb2db, 0xb2dc]))
  it(`canonical nonzero u128 identities and non-repeating restore successor ${String(seed)}`, () => {
    const reach = createReach("database identity", DATABASE_IDENTITY_REACH);
    fc.assert(
      fc.property(fc.bigInt({ min: 1n, max: MAX_DATABASE_ID }), (id) => {
        reach.beginHistory();
        const writer = new ByteWriter();
        writeDatabaseIdentity(writer, id);
        const bytes = writer.finish();
        expect(bytes.length).toBe(16);
        const reader = new ByteReader(bytes);
        expect(readDatabaseIdentity(reader)).toBe(id);
        reader.end();
        let reference = id;
        for (const byte of bytes) {
          expect(byte).toBe(Number(reference & 255n));
          reference >>= 8n;
        }
        reach.reach(id <= 0xffffffffn ? "small" : "wide");
        if (id === MAX_DATABASE_ID) {
          expect(() => nextDatabaseIdentity(id)).toThrow(DatabaseIdentityExhaustedError);
          reach.reach("exhaustion");
        } else {
          const next = nextDatabaseIdentity(id);
          expect(next).toBe(id + 1n);
          expect(next).not.toBe(id);
          reach.reach("successor");
        }
        expect(() => readDatabaseIdentity(new ByteReader(new Uint8Array(16)))).toThrow(
          ManifestCorruptionError,
        );
        expect(() =>
          readDatabaseIdentity(new ByteReader(bytes.subarray(0, Number(id % 16n)))),
        ).toThrow(ManifestCorruptionError);
        expect(() => writeDatabaseIdentity(new ByteWriter(), 0n)).toThrow(ManifestCorruptionError);
        expect(() => writeDatabaseIdentity(new ByteWriter(), MAX_DATABASE_ID + 1n)).toThrow(
          ManifestCorruptionError,
        );
        reach.reach("zero refused");
        reach.reach("truncation refused");
        let charged = 0,
          peak = 0,
          constructed = 0;
        const original = globalThis.Uint32Array;
        globalThis.Uint32Array = new Proxy(original, {
          construct(target, args, newTarget) {
            constructed++;
            return Reflect.construct(target, args, newTarget) as Uint32Array;
          },
        });
        try {
          const fresh = freshDatabaseIdentity({
            allocate(n) {
              expect(constructed, "seed words allocated before funding").toBe(0);
              charged += n;
              peak = Math.max(peak, charged);
              return true;
            },
            free(n) {
              charged -= n;
            },
          });
          expect(fresh).toBeGreaterThan(0n);
          expect(fresh).toBeLessThanOrEqual(MAX_DATABASE_ID);
          expect(charged).toBe(0);
          expect(peak).toBeGreaterThanOrEqual(16);
          expect(constructed).toBe(1);
          reach.reach("funded native seed");
          constructed = 0;
          expect(() =>
            freshDatabaseIdentity({
              allocate() {
                expect(constructed).toBe(0);
                return false;
              },
              free() {
                throw new Error("Unallocated seed was freed");
              },
            }),
          ).toThrow(ReadDeniedError);
          expect(constructed).toBe(0);
          reach.reach("denied seed");
        } finally {
          globalThis.Uint32Array = original;
        }
      }),
      { seed, numRuns: 32, examples: [[1n], [1n << 64n], [MAX_DATABASE_ID]] },
    );
    reach.assertReached();
  });
