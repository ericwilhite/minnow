/** A fitting batch followed by ordinary execution must not retain a redundant resize overlap. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { OutputArena } from "./kernels/vectors.js";
import { BOOL, TEXT, col, numbers, texts } from "./kernels/test-support.js";
import { ProjectOperator } from "./filter.js";
import { createReach, PROJECTION_PEAK_REACH } from "../../testing/reach.js";
import type { Batch } from "../vector/vector.js";
import type { ScalarExpr } from "../ir/ir.js";

it.each(seedsFor("projection-step-peak", [0xb7b5, 0xb7b6]))(
  "keeps fitting two-batch peaks seed%i",
  async (seed) => {
    const reach = createReach("projection step peaks", PROJECTION_PEAK_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(8, 16, 32),
        fc.integer({ min: 1024, max: 4096 }),
        async (rows, width) => {
          reach.beginHistory();
          const batch: Batch = {
            length: rows,
            selection: null,
            columns: [
              numbers(
                BOOL,
                Array.from({ length: rows }, (_, i) => i % 2),
              ),
              texts(Array.from({ length: rows }, () => "x".repeat(width))),
            ],
          };
          const expr: ScalarExpr = {
            op: "case",
            type: TEXT,
            branches: [{ when: col(0, BOOL), then: col(1, TEXT) }],
            otherwise: col(1, TEXT),
          };
          async function run(adaptive: boolean) {
            const governor = new Governor(16 << 20),
              grant = await governor.admit({
                label: "projection",
                minimumBytes: 0,
                spillable: true,
              }),
              competitor = await governor.admit({
                label: "temporary competitor",
                minimumBytes: 0,
                spillable: true,
              });
            let peak = 0,
              pushes = 0,
              total = 0;
            const arena = new OutputArena(
              {
                allocate: (bytes) => {
                  const ok = grant.allocate(bytes);
                  peak = Math.max(peak, grant.held);
                  return ok;
                },
                free: (bytes) => grant.free(bytes),
              },
              rows,
            );
            const operator = new ProjectOperator(
              [expr],
              { arena, params: { slots: [] }, input: [BOOL, TEXT] },
              {
                push: (b) => {
                  pushes++;
                  total += b.selection?.count ?? b.length;
                  return "ready";
                },
                ready: () => Promise.resolve(),
                finish: () => undefined,
                abort: () => undefined,
              },
              adaptive
                ? {
                    available: () =>
                      grant.held -
                      grant.allocated +
                      Math.max(0, governor.capBytes - governor.reserveBytes - governor.usedBytes),
                    signal: new AbortController().signal,
                  }
                : undefined,
            );
            const fixed = arena.bytes;
            try {
              expect(
                competitor.allocate(
                  governor.capBytes -
                    governor.reserveBytes -
                    governor.usedBytes -
                    (rows * width + 1024),
                ),
              ).toBe(true);
              operator.push(batch);
              await operator.ready();
              const first = pushes;
              competitor.close();
              operator.push(batch);
              await operator.ready();
              expect(pushes - first).toBe(1);
              expect(total).toBe(2 * rows);
              if (adaptive) {
                expect(first).toBe(1);
                reach.reach("fitting first");
                reach.reach("ordinary second");
              } else expect(peak).toBe(fixed + rows * width);
              return peak;
            } finally {
              await operator.closed;
              arena.close();
              grant.close();
              competitor.close();
              expect(governor.usedBytes).toBe(0);
            }
          }
          const normal = await run(false),
            adaptive = await run(true);
          expect(adaptive).toBeLessThanOrEqual(normal);
          reach.reach("no peak increase");
        },
      ),
      { seed, numRuns: 12, endOnFailure: true },
    );
    reach.assertReached();
  },
  60000,
);
