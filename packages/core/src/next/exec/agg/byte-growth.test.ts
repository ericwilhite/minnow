/** A denied speculative key resize must split before any aggregate transition. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, BYTE_GROWTH_REACH } from "../../../testing/reach.js";
import { Governor } from "../../memory/governor.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import { ArraySource, CollectSink } from "../join-fixtures.js";
import { OutputArena } from "../kernels/vectors.js";
import { drive } from "../pipeline.js";
import { HybridAggregateOperator } from "./hybrid.js";
import { TEXT, I64, texts, dictionary } from "../kernels/test-support.js";
import type { Batch } from "../../vector/vector.js";

it.each(seedsFor("hash-byte-transition", [0xb7bd, 0xb7be]))(
  "retires incomplete keys and splits before transitions seed%i",
  async (seed) => {
    const reach = createReach("byte transition peak", BYTE_GROWTH_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 4095, max: 810000 }),
        fc.integer({ min: 1, max: 16 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (width, columns, encoded, selected, reverse) => {
          reach.beginHistory();
          const governor = new Governor(256 << 20),
            hashMinimum = 1 << 20;
          // Native fixture's complete I/O envelope: one wide tuple plus bounded
          // decode/writer/lookup copies. Production derives it from source bounds.
          const ioMinimum = Math.max(2 << 20, 8 * columns * (width + 32) + (1 << 20));
          const grant = await governor.admit({
            label: "hybrid",
            minimumBytes: hashMinimum + ioMinimum,
            spillable: true,
          });
          const extras = await governor.admitGroup({
            label: "growth",
            grants: [0, 1].map((i) => ({ label: String(i), minimumBytes: 0, spillable: true })),
          });
          const spill = new MemorySpillService(),
            sink = new CollectSink(),
            arena = new OutputArena(grant, 2048);
          const types = Array.from({ length: columns }, () => TEXT);
          const operator = new HybridAggregateOperator(
            {
              op: "aggregate",
              id: 2,
              input: { op: "values", id: 1, output: [], rows: [] },
              output: [],
              groupBy: types.map((_, i) => i),
              groupingSets: null,
              aggregates: [
                {
                  fn: "count_star",
                  args: [],
                  type: I64,
                  filter: null,
                  distinct: false,
                  orderBy: [],
                },
              ],
            },
            { arena, input: types, params: { slots: [] } },
            { grant, extras, hashMinimum, ioMinimum, spill, queryId: 1 },
            sink,
          );
          try {
            const batches: Batch[] = [];
            // The first speculative step has no committed state. Later small input
            // proves its failed IDs/partial arrays were discarded before retrying.
            for (const prefix of [false, true]) {
              const keys = types.map((_, c) => {
                const values = prefix
                  ? ["p" + String(c), null]
                  : ["x".repeat(width) + String(c), "z" + String(c)];
                const entries = texts(prefix ? ["p" + String(c), "q" + String(c)] : values);
                return encoded
                  ? dictionary(
                      entries,
                      selected ? [0, prefix ? null : 1, 0, 1] : [0, prefix ? null : 1],
                    )
                  : texts(selected ? [values[0] ?? null, values[1] ?? null, null, null] : values);
              });
              batches.push({
                columns: keys,
                length: selected ? 4 : 2,
                selection: selected
                  ? { rows: Uint16Array.from(reverse ? [1, 0] : [0, 1]), count: 2 }
                  : null,
              });
            }
            await drive(new ArraySource(batches), operator);
            expect(sink.rows).toHaveLength(4);
            expect(sink.rows.map((r) => Number(r[columns]))).toEqual([1, 1, 1, 1]);
            if (operator.keyRetries > 0) reach.reach("key denial split");
            if (width === 800000 && columns === 1 && !reverse)
              expect(operator.keyRetries).toBeGreaterThan(0);
            reach.reach("funded transition");
            reach.reach(encoded ? "dictionary" : "flat");
            reach.reach(columns === 1 ? "single key" : "compound keys");
            if (selected) reach.reach("selected");
            if (!reverse) reach.reach("large then small");
          } finally {
            operator.abort(new Error("cleanup"));
            await operator.closed;
            arena.close();
            for (const extra of extras) extra.close();
            grant.close();
            expect(governor.usedBytes).toBe(0);
            expect(spill.bytesInUse).toBe(0);
            reach.reach("zero");
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        endOnFailure: true,
        examples: [
          [800000, 1, false, false, false],
          [800000, 3, true, false, false],
          [131071, 16, false, true, false],
          [65535, 3, true, true, true],
          [4095, 1, false, false, false],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
