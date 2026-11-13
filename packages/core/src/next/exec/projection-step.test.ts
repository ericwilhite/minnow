/** Splitting happens BEFORE evaluation: no volatile work or data error is retried. */
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PROJECTION_SPLIT_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { Batch } from "../vector/vector.js";
import * as compiler from "./kernels/compile.js";
import { OutputArena } from "./kernels/vectors.js";
import { BOOL, TEXT, col, texts, numbers, valueAt } from "./kernels/test-support.js";
import { ProjectOperator } from "./filter.js";
import type { Operator } from "./pipeline.js";

const compile = compiler.compileExpr;
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("projection-split", [0xb7b3, 0xb7b4]))(
  "splits without replay and retains borrows seed%i",
  async (seed) => {
    const reach = createReach("projection split", PROJECTION_SPLIT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1024, max: 80000 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (width, sparse, cancel, error) => {
          reach.beginHistory();
          const governor = new Governor(16 << 20);
          const grant = await governor.admit({
            label: "projection",
            minimumBytes: 0,
            spillable: true,
          });
          const competitor = await governor.admit({
            label: "pinned competitor",
            minimumBytes: 0,
            spillable: true,
          });
          const arena = new OutputArena(grant, 16);
          const abort = new AbortController();
          const counts = new Uint8Array(8);
          const values = Array.from({ length: 8 }, (_, i) => `${String(i)}${"x".repeat(width)}`);
          const batch: Batch = {
            length: 8,
            columns: [numbers(BOOL, [1, 0, 1, 0, 1, 0, 1, 0]), texts(values)],
            selection: sparse ? { rows: Uint16Array.of(1, 3, 4, 7), count: 4 } : null,
          };
          const expr: ScalarExpr = {
            op: "case",
            type: TEXT,
            branches: [{ when: col(0, BOOL), then: col(1, TEXT) }],
            otherwise: col(1, TEXT),
          };
          const mock = vi.spyOn(compiler, "compileExpr").mockImplementation((ir, context) => {
            const node = compile(ir, context);
            return {
              ...node,
              type: node.type,
              evaluate: (b, rows) => {
                const n = rows?.count ?? b.length;
                for (let k = 0; k < n; k++) {
                  const row = rows?.rows[k] ?? k;
                  counts[row] = (counts[row] ?? 0) + 1;
                  if (error && row === (sparse ? 3 : 1)) throw new Error("one data error");
                }
                return node.evaluate(b, rows);
              },
            };
          });
          let released = 0,
            pushes = 0;
          const actual: unknown[] = [];
          let fail: (reason: unknown) => void = () => undefined;
          let operator: ProjectOperator | undefined;
          const sink: Operator = {
            push: (b) => {
              pushes++;
              const first = b.selection?.rows[0] ?? 0;
              const vector = b.columns[0];
              if (!vector) throw new Error("Missing borrowed output");
              const before = valueAt(vector, first);
              actual.push(before);
              expect(released).toBe(0);
              const wait = new Promise<void>((resolve, reject) => {
                fail = reject;
                queueMicrotask(() => {
                  try {
                    expect(valueAt(vector, first)).toEqual(before);
                    if (cancel) abort.abort(new Error("cancel wait"));
                    else resolve();
                  } catch (e) {
                    reject(e instanceof Error ? e : new Error("Borrow check failed", { cause: e }));
                  }
                });
              });
              ready = wait;
              return "blocked";
            },
            ready: () => ready,
            finish: () => undefined,
            abort: (reason) => fail(reason),
          };
          let ready = Promise.resolve();
          const onAbort = () => operator?.abort(abort.signal.reason);
          abort.signal.addEventListener("abort", onAbort, { once: true });
          try {
            operator = new ProjectOperator(
              [expr],
              { arena, params: { slots: [] }, input: [BOOL, TEXT] },
              sink,
              { available: () => 0, signal: abort.signal },
            );
            mock.mockRestore();
            grant.release(grant.held - grant.allocated);
            const fixed = grant.allocated;
            expect(
              competitor.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
            ).toBe(true);
            expect(operator.push(batch, { release: () => released++ })).toBe("blocked");
            if (cancel || error)
              await expect(operator.ready()).rejects.toThrow(
                cancel ? "cancel wait" : "one data error",
              );
            else {
              await operator.ready();
              const rows = batch.selection ? [...batch.selection.rows] : [0, 1, 2, 3, 4, 5, 6, 7];
              expect(actual).toEqual(rows.map((r) => values[r]));
              expect(pushes).toBe(rows.length);
              reach.reach("exact values");
            }
            expect([...counts].every((n) => n <= 1)).toBe(true);
            expect(released).toBe(1);
            expect(grant.allocated).toBe(fixed);
            reach.reach("one evaluation");
            reach.reach("no payload allocation");
            reach.reach("held borrow");
            reach.reach(sparse ? "sparse" : "dense");
            if (cancel) reach.reach("cancel wait");
            else if (error) reach.reach("error once");
          } finally {
            mock.mockRestore();
            abort.signal.removeEventListener("abort", onAbort);
            operator?.abort(new Error("test cleanup"));
            await operator?.closed;
            arena.close();
            grant.close();
            competitor.close();
            expect(governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        endOnFailure: true,
        examples: [
          [80000, false, false, false],
          [80000, true, false, false],
          [80000, false, true, false],
          [80000, true, false, true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
