/** GroupRef.deletions law: exact word length, zero unused bits, owned decoded words. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DELETION_BITMAP_REACH } from "../../testing/reach.js";
import { decodeDeletionBitmap } from "../format/deletion-bitmap.js";
import { Harness } from "../write/test-support.js";

function bitmap(rows: number, salt: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(Math.ceil(rows / 32) * 4),
    view = new DataView(bytes.buffer);
  for (let word = 0; word < bytes.length / 4; word++)
    view.setUint32(word * 4, (salt ^ Math.imul(word + 1, 0x9e3779b1)) >>> 0, true);
  if ((rows & 31) !== 0)
    view.setUint32(
      bytes.length - 4,
      view.getUint32(bytes.length - 4, true) & (2 ** (rows & 31) - 1),
      true,
    );
  return bytes;
}
function assertRefusal(run: () => unknown): void {
  expect(run).toThrow(expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }));
}
for (const seed of seedsFor("next-deletion-bitmap", [0xb241, 0xb242]))
  it(`checks exact lengths and every unused tail bit seed ${String(seed)}`, async () => {
    const reach = createReach("deletion bitmap", DELETION_BITMAP_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 2048 }),
        fc.integer({ min: 0, max: 0xffffffff }),
        async (words, salt) => {
          reach.beginHistory();
          for (const rows of new Set([0, 1, 31, 32, 33, words * 32, words * 32 - 1])) {
            const source = bitmap(rows, salt),
              before = source.slice();
            reach.reach(rows === 0 ? "empty" : (rows & 31) === 0 ? "whole word" : "partial word");
            if (rows > 32) reach.reach("multiple words");
            for (const offset of [0, 1]) {
              const backing = new Uint8Array(source.length + offset);
              backing.set(source, offset);
              const input = backing.subarray(offset),
                decoded = decodeDeletionBitmap(input, rows);
              expect(decoded.length).toBe(Math.ceil(rows / 32));
              expect(decoded.buffer).not.toBe(input.buffer);
              expect(decoded.byteOffset).toBe(0);
              expect(decoded.byteLength).toBe(decoded.buffer.byteLength);
              const ref = new DataView(source.buffer);
              expect([...decoded]).toEqual(
                Array.from({ length: source.length / 4 }, (_, i) => ref.getUint32(i * 4, true)),
              );
              expect(input).toEqual(before);
              if (offset !== 0) reach.reach("unaligned input");
            }
            for (const length of new Set([
              source.length + 4,
              source.length + 1,
              ...(source.length === 0 ? [] : [source.length - 4, source.length - 1]),
            ])) {
              const bad = new Uint8Array(length);
              bad.set(source.subarray(0, length));
              const image = bad.slice();
              assertRefusal(() => decodeDeletionBitmap(bad, rows));
              expect(bad).toEqual(image);
              reach.reach(length < source.length ? "short" : "long");
              if ((length & 3) !== 0) reach.reach("nonword length");
            }
            if ((rows & 31) !== 0)
              for (let bit = rows & 31; bit < 32; bit++) {
                const bad = source.slice(),
                  view = new DataView(bad.buffer);
                view.setUint32(
                  bad.length - 4,
                  view.getUint32(bad.length - 4, true) | (1 << bit),
                  true,
                );
                const image = bad.slice();
                assertRefusal(() => decodeDeletionBitmap(bad, rows));
                expect(bad).toEqual(image);
                reach.reach("tail bit");
              }
            expect(source).toEqual(before);
            reach.reach("unchanged input");
          }
          // Exercise the actual engine decoder, using checksummed sealed blobs and both branches.
          const harness = new Harness("strict", "memory"),
            path = await harness.open();
          const writer = harness.space.beginWrite("bitmap codec law", {
            budget: path.shared.budget,
          });
          const rows = words * 32 - 1,
            valid = bitmap(rows, salt),
            tail = valid.slice();
          new DataView(tail.buffer).setUint32(tail.length - 4, 0x80000000, true);
          const payloads = [
            valid,
            tail,
            new Uint8Array(valid.length - 4),
            new Uint8Array(valid.length + 4),
            new Uint8Array(5),
          ];
          const peek = harness.space.peek.bind(harness.space);
          try {
            const refs = (await writer.write(payloads)).blobs;
            const lease = await harness.space.read(refs, { budget: path.shared.budget });
            lease.release();
            for (const asynchronous of [false, true]) {
              harness.space.peek = (ref) => (asynchronous ? null : peek(ref));
              for (let i = 0; i < refs.length; i++) {
                const ref = refs[i];
                if (ref === undefined) throw new Error("Missing sealed bitmap");
                if (i === 0) {
                  const decoded = path.shared.bitmap(ref, rows);
                  expect(decoded instanceof Promise).toBe(asynchronous);
                  expect(await decoded).toEqual(decodeDeletionBitmap(valid, rows));
                } else {
                  let failure: unknown;
                  try {
                    await path.shared.bitmap(ref, rows);
                  } catch (error) {
                    failure = error;
                  }
                  expect(failure).toMatchObject({ code: "MINNOW_STORE_CORRUPT" });
                }
              }
              reach.reach(asynchronous ? "async read" : "cache hit");
            }
          } finally {
            harness.space.peek = (ref) => peek(ref);
            await writer.abort();
            await harness.close();
          }
          expect(harness.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  });
