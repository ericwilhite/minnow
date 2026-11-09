/** PostgreSQL's maximum numeric coefficient fits the external scalar workspace even when
 * all shared credit is pinned. Overflow is credited only after PG materializes the inputs.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import { Governor } from "../../memory/governor.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import { decimal, decimals, F64, col, numbers, valueAt } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import { ExternalCall, externalCallMinimum } from "./external-call.js";
import { SortGrants } from "./sort-grants.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const D = decimal(null, null);
it.each(seedsFor("aggregate-external-wide-numeric", [0xb4a1, 0xb4a2]))(
  "keeps wide numeric scratch private through cancelling terms and final overflow (seed %i)",
  async (seed) => {
    const reach = createReach("external numeric workspace", [
      "maximum coefficient",
      "coefficient cancellation",
      "PG input materialized",
      "overflow",
      "no headroom",
      "returned all credit",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 131040, max: 131071 }), async (exponent) => {
        reach.beginHistory();
        for (const overflow of [false, true]) {
          const coefficient = overflow ? 10n ** 131072n - 1n : 10n ** BigInt(exponent);
          const values = overflow ? [coefficient, coefficient] : [coefficient, -coefficient, 1n];
          const call = {
            fn: "sum" as const,
            args: [col(0, D)],
            type: D,
            distinct: false,
            filter: null,
            orderBy: [],
          };
          const minimum = externalCallMinimum(call),
            governor = new Governor(16 << 20);
          const parts = await governor.admitGroup({
            label: "numeric workspace",
            includesProgress: true,
            grants: [
              { label: "fixed", minimumBytes: minimum, spillable: false },
              { label: "growth", minimumBytes: 0, spillable: true },
            ],
          });
          const fixed = parts[0],
            extra = parts[1];
          if (!fixed || !extra) throw new Error("Missing numeric workspace");
          const others = await governor.admitGroup({
            label: "pinned other execution",
            includesProgress: true,
            grants: [
              { label: "pinned", minimumBytes: governor.capBytes - minimum, spillable: false },
            ],
          });
          const grants = new SortGrants(fixed, [extra]),
            arena = new OutputArena(fixed, 2048),
            spill = new MemorySpillService();
          const state = new ExternalCall(
            call,
            {
              grants,
              context: { arena, input: [D], params: { slots: [] } },
              check: () => {
                expect(governor.usedBytes).toBe(governor.capBytes);
              },
              pace: () => Promise.resolve(),
              sortContext: (grant) => ({ grant, spill, queryId: 1 }),
            },
            0,
          );
          try {
            const text = values.map(String),
              input = text.map((_, i) => `($${String(i + 1)}::numeric)`).join(",");
            expect((await pg.query(`SELECT v FROM (VALUES ${input}) t(v)`, text)).rows.length).toBe(
              values.length,
            );
            reach.reach("PG input materialized");
            for (let row = 0; row < values.length; row++) {
              const coefficient = values[row];
              if (coefficient === undefined) throw new Error("Missing numeric input");
              await state.add(
                { length: 1, columns: [decimals(D, [[coefficient, 0]], "wide")], selection: null },
                0,
                1,
                numbers(F64, [row]),
              );
            }
            if (overflow) {
              await expect(
                pg.query(`SELECT sum(v) FROM (VALUES ${input}) t(v)`, text),
              ).rejects.toMatchObject({ code: "22003" });
              await expect(state.emit()).rejects.toMatchObject({ sqlState: "22003" });
              reach.reach("overflow");
              reach.reach("maximum coefficient");
            } else {
              expect(
                (
                  await pg.query<{ v: string }>(
                    `SELECT sum(v)::text v FROM (VALUES ${input}) t(v)`,
                    text,
                  )
                ).rows[0]?.v,
              ).toBe("1");
              expect(valueAt(await state.emit(), 0)).toEqual([1n, 0]);
              reach.reach("coefficient cancellation");
            }
            reach.reach("no headroom");
          } finally {
            state.abort(new Error("test cleanup"));
            await state.close();
            arena.close();
            expect(parts.reduce((n, p) => n + p.allocated, 0)).toBe(0);
            grants.close();
            for (const p of parts) p.close();
            for (const p of others) p.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("returned all credit");
        }
      }),
      { seed, numRuns: 2, examples: [[131071]] },
    );
    reach.assertReached();
  },
  60000,
);
