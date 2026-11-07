/** Existing run bytes, streamed in both directions across real Bloom-partition boundaries. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_RANGE_DIRECTORY_STATES } from "../../testing/reach.js";
import { inKeyRange, type KeyRange } from "../access/key-index.js";
import { need } from "./bytes.js";
import { Harness } from "./test-support.js";
import { RunBuilder, runRef, PARTITION_BLOCKS } from "./runs.js";
import { RunRangeInput, rangeRunBytes } from "./range-input.js";

const reach = createReach("streamed stored run directory", WRITE_RANGE_DIRECTORY_STATES);
for (const seed of seedsFor("next-write-stream-directory", [0xb3ce]))
  it(`agrees with the stored entry model across partitions (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(64, 256, 1024),
        fc.boolean(),
        fc.boolean(),
        async (width, hot, cold) => {
          reach.beginHistory();
          const count = PARTITION_BLOCKS * 3 * Math.floor((4096 - 2) / (width + 11)) + 19;
          const h = new Harness("strict", "memory");
          await h.open();
          const writer = h.space.beginWrite("range directory input"),
            builder = new RunBuilder();
          const key = new Uint8Array(width).fill(0x42),
            view = new DataView(key.buffer);
          const keyOf = (i: number) => {
            const b = new Uint8Array(width).fill(0x42);
            new DataView(b.buffer).setUint32(0, hot ? 0 : i, false);
            return b;
          };
          try {
            for (let i = 0; i < count; i++) {
              view.setUint32(0, hot ? 0 : i, false);
              builder.add(key, i + 1, i % 7 === 0);
            }
            const encoded = need(builder.finish()),
              ref = runRef(encoded, (await writer.write(encoded.parts)).blobs);
            const ranges: KeyRange[] = [
              { lower: null, upper: null },
              {
                lower: { kind: "atLeast", key: keyOf(Math.floor(count / 4)) },
                upper: { kind: "through", key: keyOf(Math.floor((count * 3) / 4)) },
              },
              {
                lower: { kind: "after", key: keyOf(0).slice(0, 3) },
                upper: { kind: "before", key: keyOf(count - 1) },
              },
            ];
            const savedPeek = h.space.peek.bind(h.space);
            if (cold) h.space.peek = () => null;
            reach.reach(cold ? "asynchronous metadata" : "cached metadata");
            reach.reach(hot ? "one hot key" : "distinct keys");
            for (const range of ranges)
              for (const direction of [1, -1]) {
                const scratchBytes = Math.max(
                  ref.fences.length,
                  ref.entries.length,
                  ref.bloom.length,
                  4096,
                );
                const resident = rangeRunBytes(ref, direction) + scratchBytes;
                const grant = await h.governor.admit({
                  label: "directory fixture reader",
                  minimumBytes: resident + 2 * scratchBytes + 128 * 1024,
                  spillable: false,
                });
                expect(grant.allocate(resident)).toBe(true);
                const input = new RunRangeInput(
                  h.space,
                  grant,
                  ref,
                  range,
                  direction,
                  Infinity,
                  new AbortController().signal,
                  new Uint8Array(scratchBytes),
                );
                try {
                  await input.open();
                  const expected: number[] = [];
                  for (let i = 0; i < count; i++)
                    if (inKeyRange(range, keyOf(i))) expected.push(i + 1);
                  if (direction < 0) expected.reverse();
                  let n = 0;
                  while (await input.advance()) {
                    expect(input.rowId).toBe(expected[n++]);
                    expect(input.tombstone).toBe((input.rowId - 1) % 7 === 0);
                  }
                  expect(n).toBe(expected.length);
                  reach.reach(direction > 0 ? "ascending" : "descending");
                  if (range.lower === null) {
                    expect(n).toBe(count);
                    reach.reach("crossed Bloom boundaries");
                  } else reach.reach("bounded prefix seek");
                } finally {
                  input.close();
                  grant.close();
                }
              }
            h.space.peek = savedPeek;
          } finally {
            await writer.abort();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 12 },
    );
  }, 120_000);
it("reaches reverse-directory states", () => reach.assertReached());
