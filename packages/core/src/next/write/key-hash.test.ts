/**
 * KeyDelta.forEachSorted (perf step 1): the stream a run is encoded from yields
 * exactly what `sorted` lists, filtered by the same bound and predicate, in the same order.
 * Generated (fast-check, `seedsFor`): keys of 1-6 bytes from a small alphabet (so keys repeat),
 * puts and tombstones, row ids, seqs on both sides of the bound, pops of the newest entries.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { KeyDelta } from "./key-hash.js";

describe("KeyDelta.forEachSorted", () => {
  for (const seed of seedsFor("next-write-key-delta-stream", [0x4b44]))
    it(`streams what sorted() lists (seed ${String(seed)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.array(
            fc.record({
              key: fc.uint8Array({ minLength: 1, maxLength: 6, min: 0, max: 3 }),
              rowId: fc.integer({ min: 1, max: 40 }),
              seq: fc.integer({ min: 1, max: 20 }),
              tombstone: fc.boolean(),
            }),
            { maxLength: 200 },
          ),
          fc.integer({ min: 0, max: 30 }),
          fc.integer({ min: 0, max: 21 }),
          fc.integer({ min: 0, max: 3 }),
          async (entries, pops, bound, filter) => {
            const delta = new KeyDelta();
            for (const entry of entries)
              delta.add(entry.key, 0, entry.key.length, entry.rowId, entry.seq, entry.tombstone);
            for (let k = 0; k < Math.min(pops, delta.entryCount); k++)
              delta.pop(delta.entryCount - 1);
            const keep =
              filter === 0
                ? null
                : (rowId: number, tombstone: boolean) =>
                    filter === 1 ? !tombstone : filter === 2 ? rowId % 2 === 0 : rowId < 20;
            const expected: string[] = [];
            for (const { key, entries: listed } of delta.sorted(bound))
              for (const entry of listed)
                if (keep === null || keep(entry.rowId, entry.tombstone))
                  expected.push(
                    `${key.join(".")}:${String(entry.rowId)}:${String(entry.tombstone)}`,
                  );
            const actual: string[] = [];
            await delta.forEachSorted(
              bound,
              (arena, start, end, rowId, tombstone) => {
                actual.push(
                  `${arena.subarray(start, end).join(".")}:${String(rowId)}:${String(tombstone)}`,
                );
              },
              keep,
            );
            expect(actual).toEqual(expected);
          },
        ),
        { seed, numRuns: 2000 },
      );
    });
});
