/** Stable live rows compare binary values by visible bytes, not enumerable JS properties. */
import { afterAll, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, LIVE_BINARY_VALUE_REACH } from "../../testing/reach.js";
import { LiveRows } from "./rows.js";

type Binary = ArrayBuffer | ArrayBufferView;
type Kind = "buffer" | "bytes" | "words" | "data";
function visible(value: Binary): Uint8Array {
  return value instanceof ArrayBuffer
    ? new Uint8Array(value)
    : new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
}
function payload(kind: Kind, value: Uint8Array, offset: number): Binary {
  if (kind === "buffer") return value.slice().buffer;
  const backing = new Uint8Array(value.length + 2 * offset).fill(91);
  backing.set(value, offset);
  if (kind === "bytes") return backing.subarray(offset, offset + value.length);
  if (kind === "words") return new Uint16Array(backing.buffer, offset, value.length / 2);
  return new DataView(backing.buffer, offset, value.length);
}
const reach = createReach("next-live-row-binary", LIVE_BINARY_VALUE_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-live-row-binary", [8872, 8873])) {
  for (const kind of ["buffer", "bytes", "words", "data"] as const) {
    for (const update of ["equal", "different", "grow", "shrink"] as const) {
      it(`compares ${kind} ${update} visible bytes seed=${String(seed)}`, () => {
        reach.beginHistory();
        const random = mulberry32(seed);
        const length = 4 + 2 * Math.floor(random() * 20);
        const original = Uint8Array.from({ length }, () => Math.floor(random() * 256));
        const next =
          update === "grow"
            ? Uint8Array.from([...original, 7, 239])
            : update === "shrink"
              ? original.slice(0, -2)
              : original.slice();
        if (update === "different") {
          const at = Math.floor(random() * length);
          next[at] = (next[at] ?? 0) ^ 255;
        }
        let held = 0;
        const cap = 64 * 1024;
        const rows = new LiveRows<{ id: number; value: Binary }>(
          {
            allocate(bytes) {
              if (held + bytes > cap) return false;
              held += bytes;
              return true;
            },
            free(bytes) {
              held -= bytes;
              expect(held).toBeGreaterThanOrEqual(0);
            },
          },
          "id",
        );
        try {
          const first = rows.apply(
            [{ id: 1, value: payload(kind, original, 2 + 2 * Math.floor(random() * 8)) }],
            1,
          );
          const input = payload(kind, next, 24 + 2 * Math.floor(random() * 8));
          const result = rows.apply([{ id: 1, value: input }], 2);
          const current = result.rows[0];
          if (current === undefined) throw new Error("Missing reconciled row");
          expect(Array.from(visible(current.value))).toEqual(Array.from(next));
          if (update === "equal") {
            expect(result).toBe(first);
            expect(result.rows[0]).toBe(first.rows[0]);
          } else {
            expect(result).not.toBe(first);
            expect(result.rows[0]).not.toBe(first.rows[0]);
            expect(result.changes).toEqual([
              { type: "update", index: 0, previous: first.rows[0], row: result.rows[0] },
            ]);
            expect(result.patch.type).toBe("patch");
            if (result.patch.type !== "patch") throw new Error("Missing update patch");
            expect(Array.from(result.patch.retained)).toEqual([-1]);
            expect(result.patch.changedRows).toEqual([{ index: 0, row: result.rows[0] }]);
            reach.reach("updated patch");
          }
          // Engine snapshots never borrow the caller's mutable backing store.
          const inputBytes = visible(input);
          inputBytes[0] = (inputBytes[0] ?? 0) ^ 127;
          expect(Array.from(visible(current.value))).toEqual(Array.from(next));
          expect(held).toBeLessThanOrEqual(cap);
          reach.reach(kind);
          reach.reach(update);
          if (kind !== "buffer") reach.reach("offset view");
          reach.reach("private copy");
        } finally {
          rows.close();
        }
        expect(held).toBe(0);
        reach.reach("zero charges");
      });
    }
  }
}
