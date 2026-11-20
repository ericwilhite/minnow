/** ResidentIndexRangePlan owns its captured, already charged input through replacement.
 * Memtable.replaceDeltas is the production staging handoff; no data semantics change. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, RESIDENT_RANGE_REACH } from "../../testing/reach.js";
import { taskClock } from "../task.js";
import { Governor } from "../memory/governor.js";
import { packLocator } from "../vector/locator.js";
import { need } from "./bytes.js";
import { Harness, autocommit, batch, column, ints, INT, tableInput } from "./test-support.js";
it.each(seedsFor("next-resident-range", [0xb9e41, 0xb9e42]))(
  "captured delta stays exact through handoff and publication seed %i",
  async (seed) => {
    const reach = createReach("resident range input", RESIDENT_RANGE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 80 }),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 0, max: 3 }),
        async (count, descending, publish, cancel) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory"),
            p = await h.open();
          let pin: ReturnType<typeof p.pin> | null = null;
          try {
            let table = 0;
            await autocommit(p, async (s) => {
              table =
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("resident_range", [column("id", INT, false)]),
                  })
                ).id ?? 0;
            });
            const ids = Array.from({ length: count }, (_, i) => count - i);
            await autocommit(p, (s) => s.table(table).insert(batch(ints(ids)), null));
            pin = p.pin();
            const primary = need(need(p.catalog.table(table)).primaryKey),
              index = pin.index(table, primary),
              plan = index.prepareResidentRange(
                { lower: null, upper: null },
                descending ? "desc" : "asc",
              );
            if (!plan) throw Error("resident input capability");
            try {
              expect(plan.minimumBytes).toBeLessThan(16 << 10);
              reach.reach("bounded resident minimum");
              const memtable = need(index.table.tableView.memtables[0]),
                old = need(memtable.deltas.get(primary)),
                next = memtable.newDelta(),
                arena = old.keyArena();
              for (let entry = 0; entry < old.entryCount; entry++) {
                const key = old.keyOf(entry);
                next.add(
                  arena,
                  old.keyStart(key),
                  old.keyEnd(key),
                  old.rowId(entry),
                  old.seq(entry),
                  old.isTombstone(entry),
                );
              }
              memtable.replaceDeltas(new Map([[primary, next]]));
              expect(old.entryCount).toBe(count);
              expect(old.chargedBytes).toBeGreaterThan(0);
              reach.reach("real delta replacement keeps captured arrays");
              if (publish) {
                await p.recycleLog();
                await p.quiesce();
                reach.reach("publication before open");
              }
              const governor = new Governor(8 << 20),
                grant = await governor.admit({
                  label: "complete resident cursor",
                  minimumBytes: plan.minimumBytes,
                  spillable: false,
                });
              if (cancel === 1) {
                try {
                  plan.close();
                  expect(() =>
                    plan.open({ budget: grant, signal: new AbortController().signal }),
                  ).toThrow("closed");
                  reach.reach("cancel before open");
                } finally {
                  grant.close();
                }
                expect(governor.usedBytes).toBe(0);
                return;
              }
              const competitor = await governor.admit({
                label: "pinned competitor",
                minimumBytes: 0,
                spillable: true,
              });
              expect(
                competitor.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
              ).toBe(true);
              const cursor = plan.open({ budget: grant, signal: new AbortController().signal }),
                out = new Float64Array(3),
                got: number[] = [];
              try {
                if (cancel === 2) {
                  plan.close();
                  expect(old.chargedBytes).toBe(0);
                  reach.reach("cancel transferred input before next");
                  return;
                }
                if (cancel === 3) {
                  const spy = vi.spyOn(taskClock, "due").mockReturnValue(true);
                  const pending = cursor.next(out);
                  try {
                    expect(pending).toBeInstanceOf(Promise);
                    plan.close();
                    expect(old.chargedBytes).toBeGreaterThan(0);
                    await pending;
                    expect(old.chargedBytes).toBe(0);
                    reach.reach("cancel pending input keeps charge until settle");
                  } finally {
                    cursor.close();
                    await pending;
                    spy.mockRestore();
                  }
                  return;
                }
                for (;;) {
                  const n = await cursor.next(out);
                  if (!n) break;
                  got.push(...out.subarray(0, n));
                }
                const ordered = [...ids].sort((a, b) => (descending ? b - a : a - b));
                expect(got).toEqual(ordered.map((id) => packLocator(0, ids.indexOf(id))));
                reach.reach(descending ? "descending exact locators" : "ascending exact locators");
                reach.reach("pinned optional growth denied");
              } finally {
                cursor.close();
                plan.close();
                competitor.close();
                grant.close();
              }
              expect(governor.usedBytes).toBe(0);
              reach.reach("zero cursor credits");
            } finally {
              plan.close();
            }
          } finally {
            pin?.release();
            await h.close();
            expect(h.governor.usedBytes).toBe(0);
          }
        },
      ),
      { seed, numRuns: 24 },
    );
    reach.assertReached();
  },
  120000,
);
