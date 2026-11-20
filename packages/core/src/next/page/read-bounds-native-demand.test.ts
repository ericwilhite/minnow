/** Complete hint unions cover every subinterval; partial source demand retains its target floor. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, READ_DEMAND_COVERAGE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { boundSource } from "./read-bounds.js";
it.each(seedsFor("next-read-demand-coverage", [0xb9e21, 0xb9e22]))(
  "uses complete hint unions and preserves partial source coverage seed %i",
  async (seed) => {
    const reach = createReach("read demand coverage", READ_DEMAND_COVERAGE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: 0, max: 2048 }), { minLength: 3, maxLength: 80 }),
        fc.boolean(),
        async (widths, prefix) => {
          reach.beginHistory();
          const total = widths.reduce((sum, width) => sum + width + 32, 0),
            target = total + 8192,
            governor = new Governor(8 << 20),
            signal = new AbortController().signal;
          const view = {
            pages: {
              peek: () => null,
              read: () => {
                throw Error("resident sizing must not read storage");
              },
            },
            table: () => ({
              columns: [{ id: 1 }],
              segments: [
                {
                  kind: "memory" as const,
                  rowCount: widths.length,
                  liveRowCount: widths.length,
                  readHint: (request: {
                    startRow: number;
                    maxRows: number;
                    targetBytes: number;
                  }) => {
                    const count = Math.min(request.maxRows, widths.length - request.startRow);
                    return {
                      rowCount: count,
                      outputBytes: widths
                        .slice(request.startRow, request.startRow + count)
                        .reduce((sum, width) => sum + width + 32, 0),
                      residentBytes: 0,
                      scratchBytes: 512 + 8 * count,
                      ioBytes: 0,
                    };
                  },
                },
              ],
            }),
          };
          const source = {
            id: 1,
            tableId: 1,
            columns: [1],
            types: [{ kind: "text" as const }],
            windowBytes: 0,
            stagingBytes: 0,
            rows: prefix ? 2 : null,
          };
          const bound = await boundSource(view, source, governor, signal, target);
          if (prefix) {
            expect(bound.progress).toBeGreaterThanOrEqual(4 * target);
            reach.reach("physical prefix is not complete chunk");
          } else {
            expect(bound.valueBytes).toBe(total);
            expect(bound.progress).toBe(Math.max(65536, 4 * total));
            for (let from = 0; from < widths.length; from++)
              for (let end = from + 1; end <= widths.length; end++) {
                const part = widths.slice(from, end).reduce((sum, width) => sum + width + 32, 0);
                expect(part).toBeLessThanOrEqual(bound.valueBytes ?? 0);
              }
            reach.reach("whole chunk covers every subinterval");
            reach.reach("unused byte target not retained");
          }
          const partial = await boundSource(
            view,
            { ...source, rows: null, batchRows: 1 },
            governor,
            signal,
            target,
            1,
          );
          // The independently summed complete source bounds every shifted interval,
          // even when each sizing request only visits one row. No target floor is
          // needed once the union of all row hints covers the physical source.
          // ReadBound.valueBytes is a logical value bound, not the sum of the
          // source. Each one-row hint has exactly the independent width +32.
          expect(partial.valueBytes).toBe(Math.max(...widths.map((width) => width + 32)));
          expect(partial.batchRows).toBe(1);
          expect(partial.progress).toBe(Math.max(65536, 4 * total));
          const whole = prefix
            ? await boundSource(view, { ...source, rows: null }, governor, signal, target)
            : bound;
          expect(partial.bytes).toBeLessThanOrEqual(whole.bytes);
          for (let from = 0; from < widths.length; from++)
            for (let end = from + 1; end <= widths.length; end++) {
              const part = widths.slice(from, end).reduce((sum, width) => sum + width + 32, 0);
              expect(part).toBeLessThanOrEqual(whole.valueBytes ?? 0);
            }
          reach.reach("one-row hints cover whole source");
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero sizing credits");
        },
      ),
      { seed, numRuns: 24 },
    );
    reach.assertReached();
  },
);
