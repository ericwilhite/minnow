/** Hot-key ordering work, independent of JIT, GC, CPU accounting and runner load. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { createReach, HOT_KEY_SORT_WORK_STATES } from "../packages/core/src/testing/reach.js";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { KeyDelta } from "../packages/core/src/next/write/key-hash.js";

const reach = createReach("hot-key sort work", HOT_KEY_SORT_WORK_STATES);
afterAll(() => reach.assertReached());
const rows = 16384;
for (const seed of seedsFor("next-write-hot-key-cpu", [0xb3b2]))
  it(`does not multiply the legacy sort work on generated hot keys (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.uint8Array({ minLength: 1, maxLength: 12 }), async (key) => {
        reach.beginHistory();
        const Native = Float64Array;
        const reads = { stream: 0, legacy: 0 };
        let measuring: keyof typeof reads | null = null;
        let delta: KeyDelta;
        // Only arrays owned by this delta are wrapped. Restore the constructor before
        // traversal so unrelated tasks and runner allocations cannot affect the metric.
        // Native methods keep their real receiver; indexed reads count row/sequence work.
        globalThis.Float64Array = new Proxy(Native, {
          construct(target, args) {
            const array = Reflect.construct(target, args) as Float64Array;
            return new Proxy(array, {
              get(target, property) {
                if (
                  measuring !== null &&
                  typeof property === "string" &&
                  /^(0|[1-9]\d*)$/.test(property)
                )
                  reads[measuring]++;
                const value: unknown = Reflect.get(target, property, target);
                return typeof value === "function" ? value.bind(target) : value;
              },
            });
          },
        });
        try {
          delta = new KeyDelta();
          for (let k = 0; k < rows; k++) delta.add(key, 0, key.length, k + 1, 1, false);
        } finally {
          globalThis.Float64Array = Native;
        }
        try {
          expect(delta.keyCount).toBe(1);
          let count = 0;
          measuring = "stream";
          await delta.forEachSorted(Infinity, (_arena, _start, _end, rowId, tombstone) => {
            if (rowId !== ++count || tombstone) throw new Error("Hot-key rows lost their order");
          });
          measuring = null;
          expect(count).toBe(rows);
          reach.reach("exact ordered rows");
          measuring = "legacy";
          const legacy = delta.sorted(Infinity);
          measuring = null;
          expect(legacy[0]?.entries.length).toBe(rows);
          expect(legacy[0]?.key).toEqual(key);
          // The legacy traversal reads each row and sequence once. Stream also reads
          // adjacent row ids to prove monotonicity and row ids during output. Six reads
          // per row leave slack over those linear passes, but reject a heapsort/re-scan.
          expect(reads.legacy, "legacy input reads must be instrumented").toBeGreaterThanOrEqual(
            2 * rows,
          );
          expect(reads.stream, JSON.stringify(reads)).toBeLessThanOrEqual(6 * rows);
          expect(reads.stream, JSON.stringify(reads)).toBeLessThanOrEqual(3 * reads.legacy);
          reach.reach("linear stream work");
          reach.reach("bounded legacy ratio");
        } finally {
          measuring = null;
          delta.releaseCharge();
        }
      }),
      { seed, numRuns: 3, endOnFailure: true },
    );
  }, 120_000);
