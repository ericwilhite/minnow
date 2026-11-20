/** Online query dedup keeps a fixed charged bitmap cache and spills sparse locators. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, QUERY_INDEX_SEEN_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { QueryCancelledError } from "../exec/errors.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { operators } from "./query-index.test-support.js";

for (const seed of seedsFor("next-query-index-seen", [47813, 47814]))
  it(`spills sparse IN locators and fences cancellation seed${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("query index online seen", QUERY_INDEX_SEEN_REACH);
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 32, max: 40 }), fc.boolean(), async (n, reverse) => {
          reach.beginHistory();
          const core = new StorageCore(1),
            governor = new Governor(32 << 20);
          Object.assign(core.harness, { governor });
          await core.open();
          let created = 0,
            files = 0,
            appended = 0,
            armed = false;
          const controller = new AbortController();
          const create = core.spill.create.bind(core.spill);
          const spy = vi.spyOn(core.spill, "create").mockImplementation(async (id) => {
            const file = await create(id);
            created++;
            files++;
            const append = file.append.bind(file),
              close = file.close.bind(file);
            let closed = false;
            file.append = async (bytes) => {
              const ref = await append(bytes);
              appended++;
              if (armed) {
                armed = false;
                controller.abort(new Error("cancel after bitmap append"));
              }
              return ref;
            };
            file.close = async () => {
              await close();
              if (!closed) {
                closed = true;
                files--;
              }
            };
            return file;
          });
          try {
            await pg.exec("DROP TABLE IF EXISTS qseen");
            const ddl = "CREATE TABLE qseen(id INT PRIMARY KEY,v INT)";
            await pg.exec(ddl);
            await core.execute(ddl);
            for (let id = 1; id <= n; id++) {
              const sql = `INSERT INTO qseen VALUES(${String(id)},${String(id * 7)})`;
              await pg.exec(sql);
              await core.execute(sql);
              if (id % 8 === 0) await core.harness.path.runMaintenance();
            }
            await core.harness.path.quiesce();
            await core.harness.path.recycleLog();
            await core.reopen();
            const snapshot = core.harness.path.pin();
            const budget = await governor.admit({
              label: "seen shape",
              minimumBytes: 1 << 20,
              spillable: true,
            });
            try {
              const table = snapshot.catalog.tableByName("qseen");
              if (!table) throw Error("table");
              const view = snapshot.table(table.id, { budget });
              expect(view.segments.filter((s) => s.liveRowCount > 0).length).toBeGreaterThan(8);
            } finally {
              snapshot.release();
              budget.close();
            }
            reach.reach("sparse locator blocks");
            created = 0;
            appended = 0;
            const ids = Array.from({ length: n + 8 }, (_, i) => (i % 7 === 0 ? null : 1 + (i % n)));
            if (reverse) ids.reverse();
            const sql = `SELECT id,v FROM qseen WHERE id IN(${ids.map((_, i) => `$${String(i + 1)}`).join(",")})`;
            const lowered = core.lowerer.lower(sql, core.context());
            if (lowered.kind !== "query") throw Error("query");
            expect(operators(lowered.plan.root)).toContain("keyLookup");
            expect(operators(lowered.plan.root)).not.toContain("sort");
            const bag = (rows: readonly unknown[]) =>
              [...rows].sort(
                (a, b) =>
                  Number((a as Record<string, unknown>).id) -
                  Number((b as Record<string, unknown>).id),
              );
            const result = await core.execute(sql, ids, AbortSignal.timeout(30000));
            if (!("batches" in result)) throw Error("rows");
            expect(bag(oracleRows(result))).toEqual(bag((await pg.query(sql, ids)).rows));
            expect(created, "bitmap overflow must spill").toBe(1);
            expect(appended).toBeGreaterThan(0);
            expect(files).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("bitmap spill");
            reach.reach("one file");
            reach.reach("NULL and repeated keys");
            reach.reach("PGlite");
            reach.reach(reverse ? "reverse keys" : "forward keys");
            armed = true;
            created = 0;
            await expect(core.execute(sql, ids, controller.signal)).rejects.toBeInstanceOf(
              QueryCancelledError,
            );
            expect(controller.signal.aborted).toBe(true);
            expect(created).toBe(1);
            expect(files).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            expect(governor.loanedBytes).toBe(0);
            reach.reach("cancel after spill");
            reach.reach("fenced cleanup");
            const fresh = await core.execute("SELECT id FROM qseen WHERE id=$1", [1]);
            if (!("batches" in fresh)) throw Error("rows");
            expect(oracleRows(fresh)).toEqual([{ id: 1 }]);
            reach.reach("read after cancel");
          } finally {
            spy.mockRestore();
            await core.close();
            expect(files).toBe(0);
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero ownership");
          }
        }),
        {
          seed,
          numRuns: 4,
          endOnFailure: true,
          examples: [
            [32, false],
            [40, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 180000);
