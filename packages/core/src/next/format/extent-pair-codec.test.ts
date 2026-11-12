/** Extent identity is the full safe-integer (offset, length) pair, not either word alone. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, EXTENT_PAIR_CODEC_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ByteReader, ByteWriter } from "./bytes.js";
import {
  decodeGroupRoot,
  encodeGroupRoot,
  parseCharge,
  ParseBudgetError,
} from "./manifest-codec.js";
import type { GroupRoot } from "./manifest.js";
import type { ByteBudget } from "../storage/extents.js";

const shapes = ["distinct offsets", "shared offset", "mixed offsets"] as const;
const bases = [0, 2 ** 31, 2 ** 32, 2 ** 40, Number.MAX_SAFE_INTEGER - 1024] as const;

function budget(): ByteBudget & { held: number } {
  return {
    held: 0,
    allocate(bytes) {
      this.held += bytes;
      return true;
    },
    free(bytes) {
      this.held -= bytes;
    },
  };
}

function duplicate(bytes: Uint8Array, from: number, to: number): Uint8Array {
  const reader = new ByteReader(bytes),
    head = new ByteWriter();
  head.u32(reader.u32());
  head.u8(reader.u8());
  const count = reader.uint(),
    pairs = Array.from({ length: count }, () => [reader.uint(), reader.uint()] as const);
  const pair = pairs[from];
  if (pair === undefined) throw new Error("Missing source extent");
  pairs[to] = pair;
  head.uint(count);
  for (const [offset, length] of pairs) {
    head.uint(offset);
    head.uint(length);
  }
  return Uint8Array.from([...head.finish(), ...bytes.subarray(reader.at)]);
}

for (const seed of seedsFor("next-extent-pair-codecs", [0xb2e1, 0xb2e2]))
  it(`preserves exact extent pairs and canonical refusal (${String(seed)})`, () => {
    const reach = createReach("numeric extent-pair validation", EXTENT_PAIR_CODEC_REACH);
    fc.assert(
      fc.property(
        fc.constantFrom(...shapes),
        fc.constantFrom(...bases),
        fc.integer({ min: 5, max: 12 }),
        fc.boolean(),
        (shape, base, width, zeroLength) => {
          reach.beginHistory();
          const root: GroupRoot = {
            rowCount: 7,
            rowIds: { kind: "dense" },
            columns: Array.from({ length: width }, (_, k) => {
              const extent = {
                offset:
                  base +
                  (shape === "shared offset" ? 0 : shape === "mixed offsets" ? k % 3 : k * 64),
                length: zeroLength ? k : k + 1,
              };
              return {
                columnId: k + 1,
                type: { kind: "float64" },
                data: { extent, offset: 0, length: 0 },
                directory: { extent, offset: 0, length: 0, checksum: k },
                statistics: { nullCount: 0, min: null, max: null, dictionarySize: null },
                distinctSketch: null,
              };
            }),
          };
          const bytes = encodeGroupRoot(root),
            paid = budget(),
            decoded = decodeGroupRoot(bytes, paid);
          expect(decoded.value).toEqual(root);
          expect(encodeGroupRoot(decoded.value)).toEqual(bytes);
          expect(decoded.charge).toBe(parseCharge(bytes.length));
          expect(paid.held).toBe(decoded.charge);
          for (const column of decoded.value.columns)
            expect(column.data.extent).toBe(column.directory.extent);
          paid.free(decoded.charge);
          expect(paid.held).toBe(0);
          reach.reach(shape);
          reach.reach(base >= 2 ** 32 ? "wide safe integers" : "small integers");
          if (zeroLength) reach.reach("zero length");
          for (const [from, to] of [
            [0, 2],
            [1, 4],
            [2, 4],
          ] as const) {
            const refused = budget();
            expect(() => decodeGroupRoot(duplicate(bytes, from, to), refused)).toThrow(
              /appears twice/u,
            );
            expect(refused.held).toBe(0);
          }
          if (shape === "shared offset") reach.reach("promoted set remembers every length");
          reach.reach("exact duplicate refusal");
          expect(() =>
            decodeGroupRoot(bytes, {
              allocate: () => false,
              free: () => {
                throw new Error("Unacquired charge");
              },
            }),
          ).toThrow(ParseBudgetError);
          reach.reach("charge refusal before parse");
          reach.reach("zero charges after failure");
        },
      ),
      { seed, numRuns: 80 },
    );
    reach.assertReached();
  });
