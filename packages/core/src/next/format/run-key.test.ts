import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, RUN_KEY_CODEC_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { readRunEncoding, UnsupportedEncodingError } from "./encoding.js";
import { ByteReader, ByteWriter, ManifestCorruptionError } from "./bytes.js";
import { readRunKey, writeRunKey, type RunKey } from "./run-key.js";

const reach = createReach("overflow key codec", RUN_KEY_CODEC_STATES);
for (const seed of seedsFor("next-run-key-codec", [0xb501]))
  it(`round trips bounded descriptors and refuses corrupt references (${String(seed)})`, () => {
    fc.assert(
      fc.property(
        fc.constantFrom(0, 2048, 2049, 49155, 65536, 131073),
        fc.integer({ min: 0, max: 255 }),
        (length, fill) => {
          reach.beginHistory();
          expect(readRunEncoding(1)).toBe(1);
          expect(readRunEncoding(2)).toBe(2);
          reach.reach("run encodings");
          expect(() => readRunEncoding(fill + 3)).toThrow(UnsupportedEncodingError);
          reach.reach("unknown run encoding");
          const key: RunKey =
            length <= 2048
              ? { kind: "inline", bytes: new Uint8Array(length).fill(fill) }
              : {
                  kind: "overflow",
                  length,
                  prefix: new Uint8Array(32).fill(fill),
                  chunks: Array.from({ length: Math.ceil(length / 65536) }, (_, i) => ({
                    offset: 100 + i * 65536,
                    length: Math.min(65536, length - i * 65536),
                    checksum: fill,
                  })),
                };
          const bound = 100 + length,
            out = new ByteWriter();
          writeRunKey(out, key, bound);
          const bytes = out.finish();
          const input = new ByteReader(bytes);
          expect(readRunKey(input, bound)).toEqual(key);
          input.end();
          reach.reach(length <= 2048 ? "inline" : "overflow");
          if (length === 49155) reach.reach("shipped text maximum");
          if (length > 65536) reach.reach("multiple chunks");
          const bad = bytes.slice();
          bad[0] = 255;
          expect(() => readRunKey(new ByteReader(bad), bound)).toThrow(ManifestCorruptionError);
          reach.reach("unknown tag");
          for (const cut of [0, Math.floor(bytes.length / 2), bytes.length - 1])
            expect(() => readRunKey(new ByteReader(bytes.subarray(0, cut)), bound)).toThrow(
              ManifestCorruptionError,
            );
          reach.reach("truncation");
          if (key.kind === "overflow") {
            expect(() => readRunKey(new ByteReader(bytes), bound - 1)).toThrow(/outside payload/);
            reach.reach("extent bounds");
            for (const wrong of [
              { ...key, length: key.length + 1 },
              { ...key, prefix: key.prefix.subarray(1) },
              { ...key, chunks: [] },
              { ...key, chunks: key.chunks.map((x, i) => (i === 0 ? { ...x, checksum: -1 } : x)) },
            ])
              expect(() => writeRunKey(new ByteWriter(), wrong, bound)).toThrow(
                ManifestCorruptionError,
              );
            reach.reach("length prefix count checksum");
            if (key.chunks.length > 1) {
              const wrong = {
                ...key,
                chunks: key.chunks.map((x, i) => (i === 1 ? { ...x, offset: 100 } : x)),
              };
              expect(() => writeRunKey(new ByteWriter(), wrong, bound)).toThrow(/Overlapping/);
              reach.reach("overlap");
            }
          }
        },
      ),
      { seed, numRuns: 60 },
    );
    reach.assertReached();
  });
