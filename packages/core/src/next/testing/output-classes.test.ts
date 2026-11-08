/** Logical rows stay exact while full dedicated capacities are charged and explicitly returned. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, OUTPUT_BUCKET_POOL_REACH } from "../../testing/reach.js";
import { OBJECT_OVERHEAD_BYTES, Governor } from "../memory/governor.js";
import { outputCapacity, ReturnPool } from "../exec/return-pool.js";
import { recyclingFrame } from "../owner/recycling-frame.js";
import { frameTransferList, FrameBufferError } from "../exec/result.js";
import { referenceCapacity } from "../exec/output-class-test-support.js";

for (const seed of seedsFor("next-output-buckets", [0xb2f9, 0xb2fa]))
  it(`neighboring lengths share charged exclusive output seed ${String(seed)}`, async () => {
    const reach = createReach("output buckets", OUTPUT_BUCKET_POOL_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 8192 }), fc.boolean(), async (n, closeEarly) => {
        reach.beginHistory();
        const a = 8 * n - 1,
          b = 8 * n,
          size = outputCapacity(a);
        expect(size).toBe(referenceCapacity(a));
        expect(outputCapacity(b)).toBe(size);
        expect(outputCapacity(size)).toBe(size);
        expect(size % 8).toBe(0);
        const governor = new Governor(8 * 1048576),
          pool = new ReturnPool(governor, 4 * size + 1024);
        const grant = await governor.admit({
          label: "bucketed output",
          minimumBytes: 2 * size,
          spillable: true,
        });
        try {
          expect(grant.allocate(size)).toBe(true);
          const sender = new Uint8Array(size);
          sender.fill(17, 0, a);
          const ids = pool.issue(0, [sender.buffer]);
          const receiver = structuredClone(sender, { transfer: [sender.buffer] });
          const alias = new DataView(receiver.buffer);
          const payload = {
            sequence: 0,
            rowCount: a,
            columns: [
              {
                form: "flat" as const,
                length: a,
                validity: null,
                data: { layout: "bool" as const, values: receiver },
              },
            ],
            dictionaries: [],
          };
          const column = payload.columns[0];
          if (!column) throw new Error("Missing test column");
          expect(frameTransferList(payload)).toEqual([receiver.buffer]);
          expect(() =>
            frameTransferList({
              ...payload,
              columns: [
                {
                  ...column,
                  data: { layout: "bool", values: receiver.subarray(0, a) },
                },
              ],
            }),
          ).toThrow(FrameBufferError);
          const frame = recyclingFrame(payload, ids, (returns, transfer) =>
            pool.release(0, structuredClone(returns, { transfer })),
          );
          grant.free(size); // Ordinary credit never relinquishes the consumer's buffer.
          expect(pool.take(outputCapacity(b))).toBeNull();
          expect(receiver[0]).toBe(17);
          reach.reach("held is never reused");
          if (closeEarly) pool.close();
          frame.release();
          frame.release();
          expect(receiver.length).toBe(0);
          expect(() => alias.getUint8(0)).toThrow(TypeError);
          if (closeEarly) {
            expect(pool.take(size)).toBeNull();
            reach.reach("release after close");
          } else {
            expect(pool.evictableBytes).toBe(size + 2 * OBJECT_OVERHEAD_BYTES);
            expect(grant.allocate(size)).toBe(true);
            const reused = pool.take(outputCapacity(b));
            expect(reused?.byteLength).toBe(size);
            if (!reused) throw new Error("Missing class reuse");
            expect(new Uint8Array(reused).every((byte) => byte === 0)).toBe(true);
            new Uint8Array(reused).fill(99);
            grant.free(size);
            expect(receiver[0]).toBeUndefined();
            expect(() => alias.getUint8(0)).toThrow(TypeError);
            reach.reach("neighbor size reused");
            reach.reach("slack charged");
          }
          expect(pool.evictableBytes).toBeLessThanOrEqual(pool.capBytes);
          reach.reach("tight views only");
        } finally {
          pool.close();
          grant.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("all charges released");
      }),
      { seed, numRuns: 64 },
    );
    reach.assertReached();
  });

it("refuses invalid or overflowing class sizes", () => {
  for (const bytes of [-1, NaN, Infinity, 1.5, Number.MAX_SAFE_INTEGER])
    expect(() => outputCapacity(bytes)).toThrow(RangeError);
  expect(outputCapacity(0)).toBe(0);
});
