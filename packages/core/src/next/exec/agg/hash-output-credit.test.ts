/** Governor rule 2: funded hash transitions retain their charge across task yields. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import {
  createReach,
  HASH_OUTPUT_CREDIT_REACH,
  HASH_PREPARED_CREDIT_REACH,
} from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { Rel } from "../../ir/ir.js";
import { Governor } from "../../memory/governor.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import { ArraySource, CollectSink, batchesOf, TEXT } from "../join-fixtures.js";
import { I32, I64, col } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import { drive } from "../pipeline.js";
import { AggregateOperator } from "./aggregate.js";
import { HybridAggregateOperator } from "./hybrid.js";
import { HashWorkspace } from "./hash-workspace.js";

const reach = createReach("hash output credit", HASH_OUTPUT_CREDIT_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
for (const seed of seedsFor("next-hash-output-credit", [0xb4e1, 0xb4e2]))
  it(`retains complete output funding through reclaim and cancel (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          rows: fc.integer({ min: 8192, max: 8448 }),
          groups: fc.constantFrom(2, 4, 8),
          width: fc.integer({ min: 200, max: 248 }),
          batchRows: fc.constantFrom(128, 512),
          nullable: fc.boolean(),
        }),
        async ({ rows, groups, width, batchRows, nullable }) => {
          reach.beginHistory();
          const expected = (
            await pg.query<{ g: number; n: number }>(
              `SELECT (i % ${String(groups)})::int AS g, count(DISTINCT ${nullable ? "CASE WHEN i%17=0 THEN NULL ELSE " : ""}i::text || repeat('x',${String(width)})${nullable ? " END" : ""})::int AS n FROM generate_series(0,${String(rows - 1)}) i GROUP BY g ORDER BY g`,
            )
          ).rows;
          for (const cancel of [false, true]) {
            const gov = new Governor(32 << 20),
              hashMinimum = 512 << 10,
              ioMinimum = 2 << 20;
            const grant = await gov.admit({
              label: "hybrid",
              minimumBytes: hashMinimum + ioMinimum,
              spillable: true,
            });
            const extras = await gov.admitGroup({
              label: "extras",
              grants: ["hash", "io"].map((label) => ({ label, minimumBytes: 0, spillable: true })),
            });
            const filler = await gov.admit({
              label: "competitor",
              minimumBytes: 0,
              spillable: true,
            });
            const arena = new OutputArena(grant, 2048),
              sink = new CollectSink(),
              spill = new MemorySpillService();
            const input = [I32, TEXT];
            const rel: Extract<Rel, { op: "aggregate" }> = {
              op: "aggregate",
              id: 2,
              input: { op: "values", id: 1, output: [], rows: [] },
              output: [],
              groupBy: [0],
              groupingSets: null,
              aggregates: [
                {
                  fn: "count",
                  args: [col(1, TEXT)],
                  type: I64,
                  filter: null,
                  distinct: true,
                  orderBy: [],
                },
              ],
            };
            const controller = new AbortController(),
              reason = new Error("cancel funded hash output");
            const op = new HybridAggregateOperator(
              rel,
              { arena, input, params: { slots: [] } },
              {
                grant,
                extras,
                hashMinimum,
                ioMinimum,
                spill,
                queryId: 1,
                signal: controller.signal,
              },
              sink,
            );
            let reclaimed = false;
            // eslint-disable-next-line @typescript-eslint/unbound-method -- called with original receiver.
            const finish = AggregateOperator.prototype.finish;
            const spy = vi
              .spyOn(AggregateOperator.prototype, "finish")
              .mockImplementation(async function (this: AggregateOperator) {
                if (!reclaimed) {
                  reclaimed = true;
                  expect(filler.allocate(gov.capBytes - gov.reserveBytes - gov.usedBytes)).toBe(
                    true,
                  );
                  const extra = extras[0];
                  if (!extra) throw Error("Missing hash extra");
                  const before = extra.held;
                  if (filler.reclaimRoom === undefined)
                    throw new Error("Native reclaim hook missing");
                  expect(await filler.reclaimRoom(1)).toBe(true);
                  expect(extra.held).toBeLessThan(before);
                  reach.reach("reclaim after prepare");
                  reach.reach("unused capacity reclaimed");
                  if (cancel) controller.abort(reason);
                }
                return finish.call(this);
              });
            try {
              const values = Array.from({ length: rows }, (_, i) => [
                i % groups,
                nullable && i % 17 === 0 ? null : String(i) + "x".repeat(width),
              ]);
              const result = drive(new ArraySource(batchesOf(input, values, { batchRows })), op);
              if (cancel) {
                await expect(result).rejects.toBe(reason);
                reach.reach("cancel funded output");
              } else {
                await result;
                expect(
                  sink.rows
                    .map((row) => ({ g: Number(row[0]), n: Number(row[1]) }))
                    .sort((a, b) => a.g - b.g),
                ).toEqual(expected);
                reach.reach("PG exact output");
              }
              expect(reclaimed).toBe(true);
              if (nullable) reach.reach("nullable membership");
            } finally {
              spy.mockRestore();
              op.abort(new Error("test cleanup"));
              await op.closed;
              arena.close();
              for (const g of extras) g.close();
              grant.close();
              filler.close();
              expect(gov.usedBytes).toBe(0);
              expect(spill.bytesInUse).toBe(0);
              reach.reach("all charges returned");
            }
          }
        },
      ),
      {
        seed,
        numRuns: 5,
        examples: [
          [{ rows: 8192, groups: 4, width: 240, batchRows: 512, nullable: false }],
          [{ rows: 8192, groups: 8, width: 208, batchRows: 128, nullable: true }],
        ],
      },
    );
  }, 120000);

for (const seed of seedsFor("next-hash-prepared-charge", [0xb4e3, 0xb4e4]))
  it(`transfers prepared credit once and releases unused credit before waiting (${String(seed)})`, async () => {
    const chargeReach = createReach("hash prepared charge", HASH_PREPARED_CREDIT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 16 }), async (units) => {
        chargeReach.beginHistory();
        const size = 4096 * units,
          governor = new Governor(8 << 20);
        const grant = await governor.admit({
          label: "hash",
          minimumBytes: 4 * size,
          spillable: true,
        });
        const workspace = new HashWorkspace(grant, 4 * size);
        try {
          workspace.allocate(size);
          expect(await workspace.prepare(3 * size)).toBe(true);
          expect(workspace.used).toBe(size);
          expect(grant.allocated).toBe(3 * size);
          expect(() => workspace.free(size + 1)).toThrow(RangeError);
          chargeReach.reach("reserved credit is not a live buffer");
          workspace.allocate(size);
          expect(workspace.used).toBe(2 * size);
          expect(grant.allocated).toBe(3 * size);
          workspace.free(size);
          chargeReach.reach("credit consumed once");
          const pinned = await governor.admit({
            label: "pinned",
            minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
            spillable: false,
          });
          try {
            expect(await workspace.prepare(8 * size)).toBe(false);
            expect(grant.allocated).toBe(size);
            chargeReach.reach("unused released before wait");
          } finally {
            pinned.close();
          }
          expect(await workspace.prepare(3 * size)).toBe(true);
          workspace.free(size);
          expect(workspace.used).toBe(0);
          workspace.close();
          expect(grant.allocated).toBe(0);
        } finally {
          grant.close();
        }
        expect(governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 10 },
    );
    chargeReach.assertReached();
  });
