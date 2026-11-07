import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  OUTPUT_RETURN_REACH,
  OUTPUT_RETURN_PRESSURE_REACH,
} from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { ReturnPool } from "../exec/return-pool.js";
import { recyclingFrame } from "../owner/recycling-frame.js";
for (const seed of seedsFor("result-return-pool", [0xb2c0, 0xb2c1]))
  it(`exclusive return and bounded reuse (${String(seed)})`, () => {
    const reach = createReach("output return pool", OUTPUT_RETURN_REACH);
    fc.assert(
      fc.property(fc.integer({ min: 8, max: 2048 }), fc.boolean(), (n, late) => {
        reach.beginHistory();
        const governor = new Governor(8 * 1048576),
          pool = new ReturnPool(governor, 8192);
        const source = new Int32Array(n);
        source.fill(17);
        const ids = pool.issue(1, [source.buffer]),
          received = structuredClone(source, { transfer: [source.buffer] });
        const payload = {
          sequence: 1,
          rowCount: n,
          columns: [
            {
              form: "flat" as const,
              length: n,
              validity: null,
              data: { layout: "i32" as const, values: received },
            },
          ],
          dictionaries: [],
        };
        const handle = recyclingFrame(payload, ids, (returns, transfer) => {
          const moved = structuredClone(returns, { transfer });
          pool.release(1, moved);
        });
        pool.release(1, []); // credit timing does not relinquish a consumer-held buffer
        expect(pool.take(n * 4)).toBeNull();
        expect(received[0]).toBe(17);
        reach.reach("held");
        handle.release();
        handle.release();
        expect(received.length).toBe(0);
        reach.reach("released");
        const reused = pool.take(n * 4);
        if (n * 4 + 128 <= 8192) {
          expect(reused).not.toBeNull();
          if (!reused) throw new Error("missing reuse");
          new Int32Array(reused).fill(99);
          expect(received.length).toBe(0);
          reach.reach("reused");
        }
        pool.release(99, [{ id: 999999, buffer: new ArrayBuffer(8) }]);
        reach.reach("foreign");
        const invalid = new ArrayBuffer(n),
          notSent = pool.issue(3, [invalid]);
        const rejectedBefore = pool.rejected;
        pool.release(3, [{ id: notSent[0] ?? -1, buffer: invalid }]);
        expect(pool.rejected).toBe(rejectedBefore + 1);
        expect(invalid.byteLength).toBe(n);
        reach.reach("not detached");
        const wrong = new ArrayBuffer(n),
          wrongIds = pool.issue(4, [wrong]);
        structuredClone(wrong, { transfer: [wrong] });
        pool.release(4, [{ id: wrongIds[0] ?? -1, buffer: new ArrayBuffer(n + 1) }]);
        expect(pool.rejected).toBe(rejectedBefore + 2);
        reach.reach("wrong size");
        const replay = new ArrayBuffer(16),
          replayIds = pool.issue(5, [replay]);
        const back = structuredClone(replay, { transfer: [replay] });
        const item = { id: replayIds[0] ?? -1, buffer: back };
        pool.release(5, [item]);
        const heldBytes = pool.evictableBytes;
        pool.release(5, [item]);
        pool.release(5, [{ id: item.id, buffer: new ArrayBuffer(16) }]);
        expect(pool.evictableBytes).toBe(heldBytes);
        reach.reach("replay");
        structuredClone(back, { transfer: [back] }); // hostile host retains a worker-side wrapper
        expect(() => pool.take(16)).toThrow("Detached or resized returned buffer");
        reach.reach("detached reuse refused");
        const pending = pool.issue(2, [new ArrayBuffer(16)]);
        pool.evict(Infinity);
        if (late) {
          pool.release(99, [{ id: pending[0] ?? -1, buffer: new ArrayBuffer(16) }]);
          reach.reach("late");
        }
        expect(pool.evictableBytes).toBeLessThanOrEqual(8192);
        reach.reach("cap");
        pool.close();
        pool.close();
        expect(governor.usedBytes).toBe(0);
        reach.reach("closed");
      }),
      { seed, numRuns: 64 },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("result-return-pool-pressure", [0xb2c2, 0xb2c3]))
  it(`makes room for the current output size (${String(seed)})`, () => {
    const reach = createReach("return pool pressure", OUTPUT_RETURN_PRESSURE_REACH);
    fc.assert(
      fc.property(fc.integer({ min: 256, max: 512 }), (unit) => {
        reach.beginHistory();
        const governor = new Governor(8 * 1048576),
          pool = new ReturnPool(governor, 8 * unit);
        const sendBack = (size: number) => {
          const sender = new ArrayBuffer(size),
            ids = pool.issue(0, [sender]);
          const receiver = structuredClone(sender, { transfer: [sender] });
          pool.release(0, [{ id: ids[0] ?? -1, buffer: receiver }]);
        };
        for (let i = 0; i < 3; i++) sendBack(unit);
        sendBack(6 * unit);
        expect(pool.take(6 * unit)?.byteLength).toBe(6 * unit);
        reach.reach(OUTPUT_RETURN_PRESSURE_REACH[0]);
        expect(pool.evictableBytes).toBeLessThanOrEqual(8 * unit);
        pool.close();
        expect(governor.usedBytes).toBe(0);
        reach.reach(OUTPUT_RETURN_PRESSURE_REACH[1]);
      }),
      { seed, numRuns: 64 },
    );
    reach.assertReached();
  });
