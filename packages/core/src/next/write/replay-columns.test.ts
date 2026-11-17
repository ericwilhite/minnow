/** Cached identity metadata can be retired without retiring a memtable's descriptor array. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, REPLAY_MAPPING_MEMORY_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Catalog } from "./catalog.js";
import { need } from "./bytes.js";
import { Memtable } from "./memtable.js";
import { ReplayColumnMappings } from "./replay-columns.js";
import { column, INT, tableInput } from "./test-support.js";

for (const seed of seedsFor("replay-mapping-memory", [47407, 47408]))
  it(`charges, retires and retries identity metadata seed ${String(seed)}`, () => {
    const reach = createReach("replay mapping ownership", REPLAY_MAPPING_MEMORY_REACH);
    fc.assert(
      fc.property(fc.integer({ min: 8, max: 80 }), fc.boolean(), (width, wideIds) => {
        reach.beginHistory();
        reach.reach(wideIds ? "wide IDs" : "small IDs");
        const base = need(
          Catalog.empty().change(
            {
              kind: "createTable",
              table: tableInput(
                "t",
                Array.from({ length: width }, (_, c) => column(`c${String(c)}`, INT)),
                { primaryKey: null },
              ),
            },
            1,
          ).catalog.tables[0],
        );
        const offset = wideIds ? 2 ** 32 : 0;
        const old = base.columns.map((c) => ({ ...c, id: c.id + offset }));
        const ids = old.map((c) => c.id);
        const current = {
          ...base,
          schemaVersion: 2,
          columns: [...old.slice(1), { ...need(old[0]), id: offset + width + 1, name: "extra" }],
        };
        let used = 0,
          limit = Infinity,
          refusals = 0;
        const budget = {
          allocate(bytes: number) {
            if (used + bytes > limit) {
              refusals++;
              return false;
            }
            used += bytes;
            return true;
          },
          free(bytes: number) {
            used -= bytes;
            expect(used).toBeGreaterThanOrEqual(0);
          },
        };
        const cache = new ReplayColumnMappings(budget);
        let memtable: Memtable | null = null;
        try {
          const mapping = cache.get(current, 1, ids);
          expect(cache.get(current, 1, [...ids])).toBe(mapping);
          expect(cache.builds).toBe(1);
          expect(cache.hits).toBe(1);
          expect([...mapping.sourcePositions].slice(0, width - 1)).toEqual(
            Array.from({ length: width - 1 }, (_, c) => c + 1),
          );
          expect(mapping.indexPositions[width - 1]).toBe(-1);
          reach.reach("stable identity reuse");
          const reordered = [...ids].reverse();
          expect(cache.get(current, 1, reordered)).toBe(mapping);
          expect(cache.builds).toBe(1);
          expect([...mapping.sourcePositions].slice(0, width - 1)).toEqual(
            mapping.definition.columns.map((c) => reordered.indexOf(c.id)),
          );
          reach.reach("permuted layout reuse");
          const changed = [...ids];
          changed[0] = offset + width + 100;
          expect(() => cache.get(current, 1, changed)).toThrow(
            expect.objectContaining({ code: "MINNOW_LOG_CORRUPT" }),
          );
          reach.reach("same version identity refusal");
          memtable = new Memtable(
            1,
            1,
            1,
            mapping.definition.columns,
            [],
            budget,
            8,
            undefined,
            true,
          );
          expect(memtable.columns).not.toBe(mapping.definition.columns);
          expect(memtable.bytes).toBeGreaterThan(0);
          reach.reach("independent memtable ownership");
          // Each mapping fits, both together do not. Discard old memoization, not the row owner.
          limit = Math.floor(used * 1.5);
          const second = {
            ...current,
            id: 2,
            columns: current.columns.map((c) => ({ ...c, id: c.id + 1000 })),
          };
          const next = cache.get(
            second,
            1,
            ids.map((id) => id + 1000),
          );
          expect(next.definition.columns.map((c) => c.id)).toEqual(
            old.slice(1).map((c) => c.id + 1000),
          );
          expect(refusals).toBeGreaterThan(0);
          expect(cache.evictions).toBeGreaterThan(0);
          expect(used).toBeLessThanOrEqual(limit);
          reach.reach("pressure discards inactive cache");
          cache.synchronize(Catalog.empty());
          expect(cache.bytes + memtable.bytes).toBe(used);
          reach.reach("catalog invalidation");
          cache.close();
          expect(used).toBe(memtable.bytes);
          expect(memtable.columns.map((c) => c.id)).toEqual(old.slice(1).map((c) => c.id));
          memtable.release();
          expect(used).toBe(memtable.bytes);
          memtable.releaseAll();
          memtable.releaseAll();
          reach.reach("row ownership outlives cache");
        } finally {
          cache.close();
          memtable?.releaseAll();
        }
        expect(used).toBe(0);
        reach.reach("zero retained");
      }),
      {
        seed,
        numRuns: 24,
        examples: [
          [8, false],
          [24, true],
        ],
      },
    );
    reach.assertReached();
  });
