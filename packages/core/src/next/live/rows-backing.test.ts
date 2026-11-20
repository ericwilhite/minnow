import { afterAll, expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, LIVE_BACKING_REACH } from "../../testing/reach.js";
import { LiveRows } from "./rows.js";
const reach = createReach("next-live-row-backing", LIVE_BACKING_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-live-row-backing", [8870, 8871]))
  for (const view of ["bytes", "words", "data", "buffer"] as const)
    for (const shadow of ["none", "view", "capacity", "both"] as const) {
      it(`funds complete ${view}/${shadow} backing and previous diff rows seed=${String(seed)}`, () => {
        reach.beginHistory();
        const random = mulberry32(seed),
          capacity = (1 + Math.floor(random() * 4)) * 1024 * 1024,
          offset = 4 * Math.floor(random() * 1024);
        const buffer = new ArrayBuffer(capacity);
        const payload =
          view === "bytes"
            ? new Uint8Array(buffer, offset, 1)
            : view === "words"
              ? new Uint32Array(buffer, offset, 1)
              : view === "data"
                ? new DataView(buffer, offset, 1)
                : buffer;
        if ((shadow === "view" || shadow === "both") && ArrayBuffer.isView(payload)) {
          const shadowBuffer = new ArrayBuffer(offset + 8);
          new Uint8Array(shadowBuffer).fill(7);
          Object.defineProperty(payload, "buffer", { value: shadowBuffer });
          reach.reach("shadowed buffer");
        }
        if (shadow === "capacity" || shadow === "both") {
          Object.defineProperty(buffer, "byteLength", { value: 1 });
          reach.reach("shadowed capacity");
        }
        let bytes = 0,
          cap = 64 * 1024;
        const rows = new LiveRows<{ id: number; payload: ArrayBuffer | ArrayBufferView }>(
          {
            allocate(n) {
              if (bytes + n > cap) return false;
              bytes += n;
              return true;
            },
            free(n) {
              bytes -= n;
              expect(bytes).toBeGreaterThanOrEqual(0);
            },
          },
          "id",
        );
        try {
          const before = rows.apply([{ id: 1, payload: new Uint8Array([7]) }], 1);
          expect(() => rows.apply([{ id: 1, payload }], 2)).toThrow("live row reconciliation");
          expect(rows.getSnapshot()).toBe(before);
          expect(bytes).toBeLessThanOrEqual(cap);
          reach.reach("refused before clone");
          cap = 64 * 1024 * 1024;
          const large = rows.apply([{ id: 1, payload }], 3);
          const retained = large.rows[0]?.payload;
          if (!retained) throw new Error("Missing copied view");
          const retainedCapacity = ArrayBuffer.isView(retained)
            ? retained.buffer.byteLength
            : retained.byteLength;
          expect(retainedCapacity).toBe(capacity);
          expect(bytes).toBeGreaterThanOrEqual(retainedCapacity);
          reach.reach("whole backing charged");
          const next = rows.apply([{ id: 1, payload: new Uint8Array([9]) }], 4);
          expect(next.changes[0]).toMatchObject({ type: "update", previous: large.rows[0] });
          expect(bytes).toBeGreaterThanOrEqual(retainedCapacity);
          reach.reach("previous row charged");
          const small = rows.apply([{ id: 1, payload: new Uint8Array([10]) }], 5);
          expect(small.rows).toHaveLength(1);
          expect(bytes).toBeLessThan(64 * 1024);
          reach.reach("old diff released");
          reach.reach(view);
        } finally {
          rows.close();
        }
        expect(bytes).toBe(0);
        reach.reach("zero charges");
      });
    }
