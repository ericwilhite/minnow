/**
 * Per-group dictionaries and their translation maps (PROPOSAL "Efficiency": maps are built once
 * per group and query, at size O(dictionary), not O(rows); a group whose dictionary would not
 * pay is stored plain). `ChunkReader.translationMap` maps each group's dictionary codes into one
 * id space, so text GROUP BY and join keys agree across groups without materializing strings.
 */
import fc from "fast-check";
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { PageEncoding } from "../format/manifest.js";
import type { ByteBudget } from "../storage/extents.js";
import { MemoryExtentSpace, UNLIMITED } from "./memory-extents.js";
import { ChunkReader } from "./reader.js";
import { defined, targetFor, valueAt, writeGroup, type TestValue } from "./test-support.js";

const SEEDS = seedsFor("next-dictionary-translation", [0x7a5, 0x1d]);
const encoder = new TextEncoder();
const decoder = new TextDecoder();

const reach = createReach("next-dictionary-translation", [
  "groups with different code orders",
  "a group stored plain beside dictionary groups",
  "a value in one group only",
  "nulls in a dictionary group",
]);
afterAll(() => {
  reach.assertReached();
});

/** The query's interner: one id per distinct stored value, across every group. */
function interner() {
  const ids = new Map<string, number>();
  let calls = 0;
  const intern = (bytes: Uint8Array, start: number, end: number): number => {
    calls++;
    const key = decoder.decode(bytes.subarray(start, end));
    let id = ids.get(key);
    if (id === undefined) {
      id = ids.size;
      ids.set(key, id);
    }
    return id;
  };
  return { intern, ids, calls: () => calls };
}

describe.each(SEEDS)("dictionary translation across groups (seed %i)", (seed) => {
  it("maps every group's codes into one id space, at O(dictionary) cost, and stores plain what would not pay", async () => {
    const pool = Array.from({ length: 400 }, (_, k) => `value-${String(k)}`);
    const group = fc.record({
      rows: fc.integer({ min: 1, max: 3000 }),
      distinct: fc.oneof(fc.integer({ min: 1, max: 12 }), fc.integer({ min: 13, max: 400 })),
      offset: fc.nat({ max: 399 }),
      nulls: fc.constantFrom(0, 0.2),
      shuffle: fc.nat(),
    });
    await fc.assert(
      fc.asyncProperty(fc.array(group, { minLength: 2, maxLength: 4 }), async (groups) => {
        const shared = interner();
        const space = new MemoryExtentSpace();
        const counts = new Map<string, number>();
        const viaIds = new Map<number, number>();
        reach.beginHistory();
        const orders: string[] = [];
        let plainGroups = 0;
        let dictionaryGroups = 0;
        const perGroupValues: Array<Set<string>> = [];
        for (const shape of groups) {
          // Values of this group: `distinct` of the pool from `offset`, in a shuffled order.
          let state = shape.shuffle | 1;
          const pick = (): number => {
            state ^= state << 13;
            state ^= state >>> 17;
            state ^= state << 5;
            return (state >>> 0) % shape.distinct;
          };
          const values: TestValue[] = Array.from({ length: shape.rows }, () =>
            (state >>> 0) / 2 ** 32 < shape.nulls && pick() >= 0 && (state & 7) === 0
              ? null
              : encoder.encode(pool[(shape.offset + pick()) % pool.length] ?? ""),
          );
          const { group: written } = await writeGroup([{ type: { kind: "text" }, values }], {
            space,
          });
          const chunk = defined(written.columns[0]);
          let held = 0;
          const budget: ByteBudget = {
            allocate: (bytes) => ((held += bytes), true),
            free: (bytes) => void (held -= bytes),
          };
          const reader = await ChunkReader.open(space, chunk, { budget });
          await reader.load(reader.directory.pages.map((_, page) => page));
          const rows = reader.rowCount;
          const valid = values.filter((value) => value !== null) as Uint8Array[];
          for (const value of valid) {
            const key = decoder.decode(value);
            counts.set(key, (counts.get(key) ?? 0) + 1);
          }
          perGroupValues.push(new Set(valid.map((value) => decoder.decode(value))));
          const dictionary = reader.directory.dictionary;
          const dictionaryRows = reader.directory.pages
            .filter((page) => (page.encoding & 0x7f) === PageEncoding.dictionary)
            .reduce((sum, page) => sum + page.rowCount - page.nullCount, 0);
          if (dictionary === null) {
            plainGroups++;
            expect(reader.translationMap(shared.intern)).toBeNull();
            // Decoded plain: count straight from the values.
            const target = targetFor(reader, rows, reader.varlenBytes(0, rows));
            reader.decodeRange(0, rows, target, 0);
            for (let row = 0; row < rows; row++) {
              const value = valueAt(target, row);
              if (value instanceof Uint8Array) {
                const id = shared.intern(value, 0, value.length);
                viaIds.set(id, (viaIds.get(id) ?? 0) + 1);
              }
            }
          } else {
            dictionaryGroups++;
            // Never more entries than the rows they code: the map costs at most what the rows do.
            expect(dictionary.rowCount).toBeLessThanOrEqual(dictionaryRows);
            const before = shared.calls();
            const map = defined(reader.translationMap(shared.intern) ?? undefined);
            expect(shared.calls() - before).toBe(dictionary.rowCount); // O(dictionary), not O(rows)
            expect(held).toBeGreaterThanOrEqual(map.byteLength);
            expect(reader.translationMap(shared.intern)).toBe(map); // once per group and query
            expect(shared.calls() - before).toBe(dictionary.rowCount);
            orders.push(Array.from(map.subarray(0, Math.min(3, map.length))).join(","));
            if (reader.directory.pages.some((page) => page.nullCount > 0))
              reach.reach("nulls in a dictionary group");
            // GROUP BY over translated codes (dictionary pages) or decoded values (the rest).
            const [first, end] = reader.pagesInRange(0, rows);
            for (let page = first; page < end; page++) {
              const entry = defined(reader.directory.pages[page]);
              const from = entry.firstRow;
              const to = entry.firstRow + entry.rowCount;
              if ((entry.encoding & 0x7f) === PageEncoding.dictionary) {
                const codes = new Int32Array(to - from);
                const validity = new Uint32Array(Math.ceil((to - from) / 32) + 1);
                reader.decodeCodes(from, to, codes, validity);
                for (let k = 0; k < to - from; k++) {
                  if (entry.nullCount > 0 && (((validity[k >>> 5] ?? 0) >>> (k & 31)) & 1) === 0)
                    continue;
                  const id = map[codes[k] ?? 0] ?? -1;
                  viaIds.set(id, (viaIds.get(id) ?? 0) + 1);
                }
              } else {
                const target = targetFor(reader, to - from, reader.varlenBytes(from, to));
                reader.decodeRange(from, to, target, 0);
                for (let k = 0; k < to - from; k++) {
                  const value = valueAt(target, k);
                  if (value instanceof Uint8Array) {
                    const id = shared.intern(value, 0, value.length);
                    viaIds.set(id, (viaIds.get(id) ?? 0) + 1);
                  }
                }
              }
            }
          }
          reader.close();
          expect(held).toBe(0);
        }
        // The same groups and counts either way: one id per distinct value across groups.
        const byValue = new Map([...shared.ids].map(([value, id]) => [id, value]));
        const translated = new Map(
          [...viaIds].map(([id, count]) => [byValue.get(id) ?? "?", count]),
        );
        expect(translated).toEqual(counts);
        if (new Set(orders).size > 1) reach.reach("groups with different code orders");
        if (plainGroups > 0 && dictionaryGroups > 0)
          reach.reach("a group stored plain beside dictionary groups");
        const all = perGroupValues.flatMap((set) => [...set]);
        if (all.some((value) => perGroupValues.filter((set) => set.has(value)).length === 1))
          reach.reach("a value in one group only");
      }),
      { seed, numRuns: 40 },
    );
  }, 240_000);
});

describe("dictionary translation", () => {
  it("stores a group plain when its dictionary would not pay (every value distinct)", async () => {
    const values = Array.from({ length: 5000 }, (_, k) =>
      encoder.encode(`unique-${String(k)}-value`),
    );
    const { space, group } = await writeGroup([{ type: { kind: "text" }, values }]);
    const reader = await ChunkReader.open(space, defined(group.columns[0]), { budget: UNLIMITED });
    expect(
      reader.directory.pages.some((page) => (page.encoding & 0x7f) === PageEncoding.dictionary),
    ).toBe(false);
    expect(reader.translationMap(() => 0)).toBeNull();
    reader.close();
  });

  it("needs the dictionary loaded, and refuses a closed reader", async () => {
    const values = Array.from({ length: 2000 }, (_, k) => encoder.encode(`v${String(k % 5)}`));
    const { space, group } = await writeGroup([{ type: { kind: "text" }, values }]);
    const reader = await ChunkReader.open(space, defined(group.columns[0]), { budget: UNLIMITED });
    expect(() => reader.translationMap(() => 0)).toThrow(/not loaded/u);
    await reader.loadDictionary();
    expect(reader.translationMap((bytes, start) => bytes[start + 1] ?? 0)?.length).toBe(5);
    reader.close();
    expect(() => reader.translationMap(() => 0)).toThrow(/closed/u);
  });
});
