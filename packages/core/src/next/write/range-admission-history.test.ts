/** Regression histories from the admission review: large keys and publication during traversal.
 * Law: IndexRangePlan preserves its pinned input/order within the advertised minimum.
 * Legacy traversal is a differential check, not an independent SQL visibility authority.
 */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_RANGE_ADMISSION_HISTORY_STATES } from "../../testing/reach.js";
import { need } from "./bytes.js";
import { Governor } from "../memory/governor.js";
import { autocommit, batch, column, Harness, TEXT, texts, tableInput } from "./test-support.js";
const ALL = { lower: null, upper: null };
for (const seed of seedsFor("next-range-admission-history", [0x1108, 0xb4]))
  it(`keeps large-key admission through publication (${String(seed)})`, async () => {
    const reach = createReach(
      "admitted range publication histories",
      WRITE_RANGE_ADMISSION_HISTORY_STATES,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 4, max: 12 }),
        fc.integer({ min: 100, max: 1900 }),
        fc.boolean(),
        async (rounds, width, desc) => {
          reach.beginHistory();
          reach.reach(desc ? "desc" : "asc");
          if (width >= 1000) reach.reach("large text keys");
          const h = new Harness("strict", "memory", 8),
            p = await h.open();
          let t = 0;
          try {
            await autocommit(p, async (s) => {
              t = need(
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", TEXT, false)]),
                  })
                ).id,
              );
            });
            for (let r = 0; r < rounds; r++) {
              await autocommit(p, (s) =>
                s
                  .table(t)
                  .insert(
                    batch(
                      texts(
                        Array.from(
                          { length: 8 },
                          (_, i) => String(r * 8 + i).padStart(6, "0") + "x".repeat(width),
                        ),
                      ),
                    ),
                    null,
                  ),
              );
              if (r % 3 === 0) {
                await p.recycleLog();
                await p.quiesce();
              }
            }
            const pin = p.pin(),
              idx = pin.index(t, need(need(p.catalog.table(t)).primaryKey)),
              dir = desc ? "desc" : "asc";
            if (idx.table.tableView.memtables.length > 1) reach.reach("multiple memtables");
            const unpublished = idx.table.tableView.memtables.filter((m) => m.published === null);
            const before = h.governor.usedBytes,
              plan = idx.planRange(ALL, dir);
            expect(h.governor.usedBytes).toBe(before);
            if (desc) {
              await p.recycleLog();
              await p.quiesce();
              if (unpublished.some((m) => m.published !== null))
                reach.reach("publication before open");
            }
            const g = new Governor(plan.minimumBytes + 1048576),
              grant = await g.admit({
                label: "admitted history",
                minimumBytes: plan.minimumBytes,
                spillable: false,
              });
            const c = plan.open({ budget: grant, signal: new AbortController().signal }),
              legacy = idx.range(ALL, dir);
            // Materialize the legacy oracle before forbidding shared admission.
            const expected: number[] = [],
              buf = new Float64Array(3);
            for (;;) {
              const n = await legacy.next(buf);
              if (!n) break;
              expected.push(...buf.subarray(0, n));
            }
            legacy.close();
            let a = vi.spyOn(h.governor, "admit").mockImplementation(() => {
                throw Error("self admission");
              }),
              b = vi.spyOn(h.governor, "tryAdmit").mockImplementation(() => {
                throw Error("self admission");
              });
            try {
              let at = 0,
                published = desc;
              while (at < expected.length) {
                const n = await c.next(buf);
                expect([...buf.subarray(0, n)]).toEqual(expected.slice(at, at + n));
                expect(n).toBeGreaterThan(0);
                at += n;
                reach.reach("next");
                if (!published) {
                  const pendingPublication = idx.table.tableView.memtables.filter(
                    (m) => m.published === null,
                  );
                  a.mockRestore();
                  b.mockRestore();
                  await p.recycleLog();
                  await p.quiesce();
                  published = true;
                  if (pendingPublication.some((m) => m.published !== null)) {
                    reach.reach("publication during iteration");
                    if (width >= 1000) reach.reach("large text publication during iteration");
                  }
                  a = vi.spyOn(h.governor, "admit").mockImplementation(() => {
                    throw Error("self admission");
                  });
                  b = vi.spyOn(h.governor, "tryAdmit").mockImplementation(() => {
                    throw Error("self admission");
                  });
                }
                const skip = Math.min((at % 5) + 1, expected.length - at);
                expect(await c.skip(skip)).toBe(skip);
                at += skip;
                reach.reach("skip after next");
              }
              expect(await c.next(buf)).toBe(0);
            } finally {
              a.mockRestore();
              b.mockRestore();
              c.close();
              expect(grant.allocated).toBe(0);
              expect(grant.held).toBe(plan.minimumBytes);
              grant.close();
              expect(g.usedBytes).toBe(0);
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
  }, 120000);
