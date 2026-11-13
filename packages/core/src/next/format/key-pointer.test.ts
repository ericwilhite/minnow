import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { createReach, KEY_POINTER_CODEC_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ByteReader, ByteWriter, ManifestCorruptionError } from "./bytes.js";
import { writeKeyPointer, readKeyPointer, type KeyPointer } from "./key-pointer.js";
const reach = createReach("key pointer codec", KEY_POINTER_CODEC_STATES);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-key-pointer", [0xb514]))
  it(`bounded relative pointers reject invalid references (${String(seed)})`, () => {
    fc.assert(
      fc.property(
        fc.constantFrom(2049, 49155, 65537),
        fc.oneof(fc.constant(0), fc.integer({ min: 1, max: 4096 })),
        fc.uint8Array({ minLength: 32, maxLength: 32 }),
        (length, padding, prefix) => {
          reach.beginHistory();
          reach.reach(length === 49155 ? "shipped maximum" : "other lengths");
          reach.reach(padding === 0 ? "adjacent directory" : "directory padding");
          const pointer: KeyPointer = {
              kind: "overflow",
              length,
              prefix,
              directory: { offset: length + padding, length: 128, checksum: 0xffffffff },
            },
            end = length + padding + 128,
            out = new ByteWriter();
          writeKeyPointer(out, pointer, end);
          const bytes = out.finish(),
            input = new ByteReader(bytes);
          expect(readKeyPointer(input, end)).toEqual(pointer);
          input.end();
          reach.reach("round trip");
          expect(() => readKeyPointer(new ByteReader(bytes), end - 1)).toThrow(/precede block/);
          for (const bad of [
            { ...pointer, length: 2048 },
            { ...pointer, prefix: prefix.subarray(1) },
            { ...pointer, directory: { ...pointer.directory, offset: length - 1 } },
            { ...pointer, directory: { ...pointer.directory, length: 0 } },
            { ...pointer, directory: { ...pointer.directory, length: 1048577 } },
            { ...pointer, directory: { ...pointer.directory, checksum: -1 } },
          ])
            expect(() => writeKeyPointer(new ByteWriter(), bad, end)).toThrow(
              ManifestCorruptionError,
            );
          reach.reach("invalid references");
          const unknown = bytes.slice();
          unknown[0] = 2;
          expect(() => readKeyPointer(new ByteReader(unknown), end)).toThrow(/Unknown/);
          reach.reach("unknown tag");
          reach.reach("truncation");
          for (const cut of [0, 1, bytes.length - 1])
            expect(() => readKeyPointer(new ByteReader(bytes.subarray(0, cut)), end)).toThrow(
              ManifestCorruptionError,
            );
        },
      ),
      { seed, numRuns: 40 },
    );
  });
