/** Authority: IndexRangePlan's complete-minimum and caller-owned-grant contract. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_RANGE_ADMISSION_STATES } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { need } from "./bytes.js";
import { autocommit, batch, column, Harness, INT, ints, tableInput } from "./test-support.js";

const ALL = { lower: null, upper: null };
for (const seed of seedsFor("next-range-admission", [0xb381, 0xb382]))
  it(`admits complete cursor minima across publication and cancellation (${String(seed)})`, async () => {
    const reach = createReach("admitted range", WRITE_RANGE_ADMISSION_STATES);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 3, max: 600 }),
        fc.boolean(),
        fc.boolean(),
        async (count, desc, cancel) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory"),
            p = await h.open();
          let table = 0;
          try {
            await autocommit(p, async (s) => {
              table =
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false)]),
                  })
                ).id ?? 0;
            });
            await autocommit(p, (s) =>
              s.table(table).insert(batch(ints(Array.from({ length: count }, (_, i) => i))), null),
            );
            const pin = p.pin(),
              index = pin.index(table, need(p.catalog.table(table)?.primaryKey));
            const direction = desc ? "desc" : "asc";
            reach.reach(direction);
            const before = h.governor.usedBytes;
            const plan = index.planRange(ALL, direction);
            expect(h.governor.usedBytes).toBe(before);
            // Force the publication transition after planning, before query admission/open.
            await p.recycleLog();
            await p.quiesce();
            expect(index.table.tableView.memtables.some((m) => m.published !== null)).toBe(true);
            reach.reach("publication");
            const minimum = plan.minimumBytes;
            const governor = new Governor(minimum + 1024 * 1024);
            const grant = await governor.admit({
              label: "first query",
              minimumBytes: minimum,
              spillable: false,
            });
            const controller = new AbortController();
            const cursor = plan.open({ budget: grant, signal: controller.signal });
            expect(() => plan.open({ budget: grant, signal: controller.signal })).toThrow(
              "already opened",
            );
            const admit = vi.spyOn(h.governor, "admit").mockImplementation(() => {
              throw new Error("cursor self-admitted");
            });
            const tryAdmit = vi.spyOn(h.governor, "tryAdmit").mockImplementation(() => {
              throw new Error("cursor self-admitted");
            });
            try {
              // Each fits alone; one FIFO query admission must complete after the first releases.
              const secondPlan = index.planRange(ALL, direction);
              let secondAdmitted = false;
              const second = governor
                .admit({
                  label: "second query",
                  minimumBytes: secondPlan.minimumBytes,
                  spillable: false,
                })
                .then((g) => {
                  secondAdmitted = true;
                  return g;
                });
              const out = new Float64Array(1);
              expect(await cursor.next(out)).toBe(1);
              reach.reach("next");
              expect(await cursor.skip(count - 2)).toBe(count - 2);
              reach.reach("skip");
              if (cancel) {
                controller.abort();
                expect(() => cursor.next(out)).toThrow();
                reach.reach("cancel");
              } else {
                cursor.close();
                reach.reach("close");
              }
              expect(secondAdmitted).toBe(false);
              cursor.close();
              expect(grant.allocated).toBe(0);
              expect(grant.held).toBe(minimum);
              grant.close();
              const other = await second;
              const next = secondPlan.open({ budget: other, signal: new AbortController().signal });
              expect(await next.skip(count + 1)).toBe(count);
              next.close();
              expect(other.allocated).toBe(0);
              other.close();
              expect(governor.usedBytes).toBe(0);
              reach.reach("concurrent");
            } finally {
              cursor.close();
              grant.close();
              admit.mockRestore();
              tryAdmit.mockRestore();
              pin.release();
            }
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  }, 60000);
