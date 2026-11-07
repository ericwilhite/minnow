/** PostgreSQL ORDER BY/LIMIT: retain exactly the best groups, including NULL ordering. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach } from "../../../testing/reach.js";
import { I32, I64, numbers } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import { AggregateOperator } from "./aggregate.js";
import { yieldTask, type Operator } from "../pipeline.js";
import { GroupTopN } from "./group-top-n.js";

it.each(seedsFor("next-group-top-n", [0xe231, 0xe232]))(
  "selects the SQL order across batches (seed %i)",
  async (seed) => {
    const reach = createReach("group top-N", [
      "empty",
      "multiple-batches",
      "ascending",
      "descending",
      "nulls-first",
      "nulls-last",
      "bounded",
      "one-key",
      "count-ascending",
      "count-descending",
      "refused",
      "winning-null",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 8193 }),
        fc.integer({ min: 1, max: 100 }),
        fc.boolean(),
        fc.boolean(),
        fc.constantFrom("key", "count-asc", "count-desc"),
        async (n, limit, desc, nullsFirst, mode) => {
          reach.beginHistory();
          let deniedUsed = 0;
          const denied = {
            allocate(bytes: number) {
              if (deniedUsed + bytes > 256 + 4 * limit) return false;
              deniedUsed += bytes;
              return true;
            },
            free(bytes: number) {
              deniedUsed -= bytes;
            },
          };
          expect(
            () =>
              new GroupTopN(
                {
                  count: limit,
                  keys: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
                },
                denied,
              ),
          ).toThrow(RangeError);
          expect(deniedUsed).toBe(0);
          reach.reach("refused");
          let used = 0,
            peak = 0;
          const charge = {
            allocate(bytes: number) {
              used += bytes;
              peak = Math.max(peak, used);
              return true;
            },
            free(bytes: number) {
              used -= bytes;
            },
          };
          const key = {
            column: 0,
            direction: desc ? ("desc" as const) : ("asc" as const),
            nulls: nullsFirst ? ("first" as const) : ("last" as const),
            collation: null,
          };
          const count = {
            column: 1,
            direction: mode === "count-asc" ? ("asc" as const) : ("desc" as const),
            nulls: "last" as const,
            collation: null,
          };
          const top = new GroupTopN(
            { count: limit, keys: mode === "key" ? [key] : [count, key] },
            charge,
          );
          const rows = Array.from({ length: n }, (_, id) => ({
            id,
            key: id === 17 ? null : (Math.imul(id, 7919) | 0) ^ -2147483648,
            count: (Math.imul(id, 101) >>> 0) % 11,
          }));
          try {
            for (let first = 0; first < n; first += 128) {
              const part = rows.slice(first, first + 128);
              const keyVector = numbers(
                I32,
                part.map((r) => r.key),
              );
              top.offer(
                mode === "key"
                  ? [keyVector]
                  : [
                      numbers(
                        I64,
                        part.map((r) => r.count),
                      ),
                      keyVector,
                    ],
                first,
                part.length,
              );
            }
            const order = (a: (typeof rows)[number], b: (typeof rows)[number]) => {
              if (mode !== "key" && a.count !== b.count)
                return mode === "count-asc" ? a.count - b.count : b.count - a.count;
              if (a.key === null || b.key === null)
                return a.key === b.key ? 0 : (a.key === null) == nullsFirst ? -1 : 1;
              return desc ? b.key - a.key : a.key - b.key;
            };
            const actual = Array.from({ length: top.size }, (_, i) => rows[top.group(i)])
              .map((row) => {
                if (!row) throw new Error("group id");
                return row;
              })
              .sort(order);
            if (actual.some((r) => r.key === null)) reach.reach("winning-null");
            expect(actual).toEqual([...rows].sort(order).slice(0, limit));
            expect(peak).toBe(256 + limit * (4 + (mode === "key" ? 1 : 2) * 8));
            reach.reach(
              mode === "key"
                ? "one-key"
                : mode === "count-asc"
                  ? "count-ascending"
                  : "count-descending",
            );
            reach.reach("bounded");
            if (n === 0) reach.reach("empty");
            if (n > 128) reach.reach("multiple-batches");
            reach.reach(desc ? "descending" : "ascending");
            reach.reach(nullsFirst ? "nulls-first" : "nulls-last");
          } finally {
            top.close();
            top.close();
            expect(used).toBe(0);
            reach.reach("released");
          }
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [0, 1, false, false, "key"],
          [8193, 100, true, true, "count-desc"],
          [33, 32, false, true, "count-asc"],
          [33, 32, true, false, "key"],
        ],
      },
    );
    reach.assertReached();
  },
);

it.each(seedsFor("next-group-top-n-cancel", [0xe237, 0xe238]))(
  "releases ranking and winner state on cancellation (seed %i)",
  async (seed) => {
    const reach = createReach("group top-N cancellation", [
      "before",
      "ranking",
      "backpressure",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(8193, 16385, 32768),
        fc.integer({ min: 0, max: 2 }),
        async (n, mode) => {
          reach.beginHistory();
          let bytes = 0;
          const charge = {
            allocate(n: number) {
              bytes += n;
              return true;
            },
            free(n: number) {
              bytes -= n;
            },
          };
          const arena = new OutputArena(charge, 2048),
            controller = new AbortController(),
            reason = new Error("cancel top-N");
          let release: () => void = () => undefined,
            pushes = 0,
            tick = 0;
          const sink: Operator = {
            push() {
              pushes++;
              return mode === 2 ? "blocked" : "ready";
            },
            ready: () =>
              new Promise<void>((resolve) => {
                release = resolve;
              }),
            finish: () => undefined,
            abort: () => undefined,
          };
          const op = new AggregateOperator(
            {
              id: 1,
              op: "aggregate",
              output: [],
              input: { id: 0, op: "values", output: [], rows: [] },
              groupBy: [0],
              groupingSets: null,
              aggregates: [
                {
                  fn: "count_star",
                  type: I64,
                  args: [],
                  filter: null,
                  orderBy: [],
                  distinct: false,
                },
              ],
            },
            { arena, input: [I32], params: { slots: [] } },
            charge,
            sink,
            {
              signal: controller.signal,
              now: mode === 1 ? () => tick++ : () => 0,
              outputTopN: {
                count: 17,
                keys: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
              },
            },
          );
          let cancelTask: Promise<void> | null = null;
          try {
            for (let from = 0; from < n; from += 2048) {
              const count = Math.min(2048, n - from);
              op.push({
                length: count,
                selection: null,
                columns: [
                  numbers(
                    I32,
                    Array.from({ length: count }, (_, i) => from + i),
                  ),
                ],
              });
            }
            if (mode === 0) controller.abort(reason);
            if (mode === 1) cancelTask = yieldTask().then(() => controller.abort(reason));
            const done = Promise.resolve().then(() => op.finish());
            if (mode === 2) {
              await Promise.resolve();
              expect(pushes).toBe(1);
              controller.abort(reason);
              release();
            }
            await expect(done).rejects.toBe(reason);
            expect(pushes).toBe(mode === 2 ? 1 : 0);
            reach.reach((["before", "ranking", "backpressure"] as const)[mode] ?? "before");
          } finally {
            if (cancelTask !== null) await cancelTask;
            release();
            op.abort(reason);
            arena.close();
            expect(bytes).toBe(0);
            reach.reach("released");
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [8193, 0],
          [16385, 1],
          [32768, 2],
        ],
      },
    );
    reach.assertReached();
  },
);
