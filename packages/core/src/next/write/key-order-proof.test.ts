/** Sorted slots are an order proof, never a replacement for byte comparison on prefix ties. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  WRITE_KEY_ORDER_PROOF_STATES,
  WRITE_KEY_BATCH_PROOF_STATES,
} from "../../testing/reach.js";
import * as ordering from "../vector/varlen.js";
import { KeyDelta } from "./key-hash.js";
const key = (n: number) => Uint8Array.of(1, n >>> 24, n >>> 16, n >>> 8, n);
const scalar = (a: Uint8Array, b: Uint8Array) => {
  const at = Array.from(a.subarray(0, Math.min(a.length, b.length))).findIndex(
    (v, i) => v !== b[i],
  );
  return at < 0 ? a.length - b.length : (a[at] ?? 0) - (b[at] ?? 0);
};

for (const seed of seedsFor("next-write-key-order-proof", [0xb50001]))
  it(`keeps order, ranges and bounded work over generated histories (${String(seed)})`, async () => {
    const reach = createReach("key order proof", WRITE_KEY_ORDER_PROOF_STATES);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(
          "ascending",
          "descending",
          "late disorder",
          "prefix ties",
          "empty and zero extension",
          "rollback",
          "duplicates",
        ),
        fc.integer({ min: 8, max: 128 }),
        fc.nat({ max: 127 }),
        async (shape, count, offset) => {
          reach.beginHistory();
          reach.reach(shape);
          let keys = Array.from({ length: count }, (_, i) => key(i + offset));
          if (shape === "descending") keys.reverse();
          if (shape === "late disorder") keys.push(key(0));
          if (shape === "prefix ties")
            keys = Array.from({ length: count }, (_, i) =>
              Uint8Array.of(1, 2, 3, 4, 5, 6, (count - i) & 255),
            );
          if (shape === "empty and zero extension")
            keys = [new Uint8Array(0), new Uint8Array(2), new Uint8Array(1), ...keys];
          if (shape === "duplicates") keys = keys.flatMap((k) => [k, k]);
          const history = [...keys];
          const delta = new KeyDelta();
          const spy = vi.spyOn(ordering, "compareBytes");
          try {
            keys.forEach((k, i) => delta.add(k, 0, k.length, i + 1, 1, false));
            if (shape === "rollback") {
              for (let i = 0; i < 3; i++) {
                delta.pop(delta.entryCount - 1);
                keys.pop();
              }
              const added = key(offset);
              keys.push(added);
              history.push(added);
              delta.add(added, 0, added.length, keys.length, 1, false);
            }
            const range = delta.keyRange();
            const sorted = [...history].sort(scalar);
            expect(range).toEqual({ min: sorted[0], max: sorted[sorted.length - 1] });
            const expected = keys
              .map((k, i) => ({ key: Array.from(k), row: i + 1 }))
              .sort(
                (a, b) => scalar(Uint8Array.from(a.key), Uint8Array.from(b.key)) || a.row - b.row,
              );
            const actual: typeof expected = [];
            await delta.forEachSorted(1, (a, s, e, row) =>
              actual.push({ key: Array.from(a.subarray(s, e)), row }),
            );
            expect(actual).toEqual(expected);
            if (shape === "ascending") {
              expect(spy).not.toHaveBeenCalled();
              reach.reach("zero workspace");
            }
            if (shape === "prefix ties") {
              expect(spy).toHaveBeenCalled();
              reach.reach("byte fallback");
            }
            const ordered = delta.ordered();
            const rows = [];
            if (ordered !== null)
              for (let at = ordered.first(1), step = 0; at !== -1 && step <= keys.length; step++) {
                const entry = ordered.at(at);
                if (entry < 0) break;
                rows.push(delta.rowId(entry));
                at = ordered.step(at, 1);
              }
            expect(rows).toEqual(expected.map((x) => x.row));
          } finally {
            spy.mockRestore();
            delta.releaseCharge();
          }
        },
      ),
      { seed, numRuns: 300 },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-write-ordered-batch", [0xb50002]))
  it(`admits only proved new ordered batches, without key lookups (${String(seed)})`, async () => {
    const reach = createReach("ordered batch", WRITE_KEY_BATCH_PROOF_STATES);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("ascending", "duplicate", "disorder", "overlap", "mixed tag"),
        fc.integer({ min: 32, max: 128 }),
        fc.oneof(
          fc.integer({ min: 0, max: 0xfffffe00 }),
          fc.constantFrom(0x7ffffff0, 0x80000000, 0xfffff000),
        ),
        async (shape, count, offset) => {
          reach.beginHistory();
          const delta = new KeyDelta();
          const encode = (values: Uint8Array[]) => {
            const bytes = new Uint8Array(values.length * 5);
            const offsets = new Int32Array(values.length + 1);
            values.forEach((v, i) => {
              bytes.set(v, i * 5);
              offsets[i + 1] = (i + 1) * 5;
            });
            return { bytes, offsets };
          };
          const first = Array.from({ length: count }, (_, i) => key(offset + i));
          const initial = encode(first);
          const find = vi.spyOn(delta, "find");
          const compare = vi.spyOn(ordering, "compareBytes");
          try {
            expect(delta.addOrderedBatch(initial.bytes, initial.offsets, count, 1, 1)).toBe(true);
            expect(compare).not.toHaveBeenCalled();
            expect(find).not.toHaveBeenCalled();
            reach.reach("ordered batch");
            if (offset >= 0x80000000) reach.reach("unsigned payload");
            if (offset < 0x80000000 && offset + count >= 0x80000000) reach.reach("signed boundary");
            const incoming = Array.from({ length: count }, (_, i) => key(offset + count + i));
            if (shape === "duplicate") incoming[1] = key(offset + count);
            if (shape === "disorder") incoming.reverse();
            if (shape === "overlap") incoming[0] = key(offset + count - 1);
            if (shape === "mixed tag") incoming[1] = Uint8Array.of(2, 0, 0, 0, 0);
            const next = encode(incoming);
            compare.mockClear();
            const appended = delta.addOrderedBatch(next.bytes, next.offsets, count, count + 1, 1);
            expect(appended).toBe(shape === "ascending");
            if (appended) {
              expect(compare).toHaveBeenCalledTimes(1);
              reach.reach("one boundary");
            } else {
              expect(delta.entryCount).toBe(count);
              reach.reach("batch fallback");
              if (shape === "overlap") reach.reach("boundary rejection");
              incoming.forEach((v, i) => delta.add(v, 0, v.length, count + i + 1, 1, false));
            }
            const expected = [...first, ...incoming]
              .map((k, i) => ({ key: Array.from(k), row: i + 1 }))
              .sort(
                (a, b) => scalar(Uint8Array.from(a.key), Uint8Array.from(b.key)) || a.row - b.row,
              );
            const actual: typeof expected = [];
            await delta.forEachSorted(1, (a, s, e, row) =>
              actual.push({ key: Array.from(a.subarray(s, e)), row }),
            );
            expect(actual).toEqual(expected);
          } finally {
            find.mockRestore();
            compare.mockRestore();
            delta.releaseCharge();
          }
        },
      ),
      { seed, numRuns: 300 },
    );
    reach.assertReached();
  });
