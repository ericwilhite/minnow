/** MNRT2: independent read/rewrite requirements precede every ownership mutation. */
import {
  DURABLE_READER_VERSION,
  DURABLE_WRITER_VERSION,
  MANIFEST_FORMAT_VERSION,
} from "../durable-format.js";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ROOT_DUAL_GATE_REACH } from "../../testing/reach.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { decodeRootSlot, encodeRootSlot } from "./root-slot.js";

for (const seed of seedsFor("next-root-dual-codec", [0xb2c1, 0xb2c2]))
  it(`publishes both gates independently in the checksummed root seed ${String(seed)}`, () => {
    const reach = createReach("dual root codec", ROOT_DUAL_GATE_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: 0x1fffff }),
        fc.uint8Array({ minLength: 32, maxLength: 128 }),
        (high, root) => {
          reach.beginHistory();
          const sequence = high * 2 ** 32 + 17,
            bytes = encodeRootSlot(sequence, root),
            view = new DataView(bytes.buffer);
          expect(view.getUint8(10) >>> 5).toBe(7);
          expect(view.getUint8(11)).toBe(MANIFEST_FORMAT_VERSION - 2); // manifest-minus2; recognizable old-reader refusal
          expect(view.getUint32(12, true)).toBe(root.length + 8);
          expect(view.getUint32(16, true)).toBe(DURABLE_READER_VERSION);
          expect(view.getUint32(20, true)).toBe(DURABLE_WRITER_VERSION);
          expect(decodeRootSlot(bytes)).toEqual({ sequence, root });
          expect(decodeRootSlot(bytes, true)?.root.buffer).toBe(bytes.buffer);
          for (let length = 0; length < 8; length++) {
            const short = bytes.slice(0, 16 + length + 4),
              header = new DataView(short.buffer);
            header.setUint32(12, length, true);
            header.setUint32(16 + length, xxHash32(short, 0, 0, 16 + length), true);
            expect(() => decodeRootSlot(short)).toThrow(
              expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
            );
          }
          reach.reach("short capability header corrupt");
          reach.reach("u32 reader and writer");
          reach.reach("high sequence independent");
          reach.reach("borrowed manifest only");
        },
      ),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  });
