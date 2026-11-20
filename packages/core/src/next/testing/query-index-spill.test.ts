/** Indexed sources must continue through a spilling parent at the same small cap. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, QUERY_INDEX_SPILL_REACH } from "../../testing/reach.js";
import { rules } from "../write/test-support.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
for (const seed of seedsFor("next-query-index-spill", [47807, 47808]))
  it(`streams indexed ranges into an exact spilling sort at32MiB seed${String(seed)}`, async () => {
    const reach = createReach("query index spill", QUERY_INDEX_SPILL_REACH);
    const pg = new PGlite();
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 512, max: 640 }),
          fc.boolean(),
          async (n, descending) => {
            reach.beginHistory();
            const core = new StorageCore(256),
              governor = new Governor(32 << 20);
            Object.assign(core.harness, { governor });
            Object.assign(core.harness.tuning, {
              merges: { mode: "manual", fanIn: 2, sliceEntries: 3 },
            });
            await core.open();
            let files = 0,
              created = 0;
            let releaseStep: (() => void) | undefined;
            let admission: { mockRestore(): void } | undefined;
            const create = core.spill.create.bind(core.spill);
            const tracked = vi.spyOn(core.spill, "create").mockImplementation(async (id) => {
              const file = await create(id);
              files++;
              created++;
              const close = file.close.bind(file);
              let closed = false;
              file.close = async () => {
                await close();
                if (!closed) {
                  closed = true;
                  files--;
                }
              };
              return file;
            });
            const ctx = core.context.bind(core);
            core.context = () => ({
              ...ctx(),
              statistics: { table: () => ({ rowCount: 1000000, column: () => null }) },
            });
            try {
              await pg.exec(
                "DROP TABLE IF EXISTS spill_t; CREATE TABLE spill_t(id INT PRIMARY KEY,v TEXT)",
              );
              await core.execute("CREATE TABLE spill_t(id INT PRIMARY KEY,v TEXT)");
              for (let start = 1; start <= n; start += 16) {
                const rows = Array.from({ length: Math.min(16, n - start + 1) }, (_, i) => ({
                  id: start + i,
                  v: String(n - start - i).padStart(6, "0") + "雪".repeat(5500),
                }));
                await core.writeBatch("insertBatch", "spill_t", rows);
                const values = rows.flatMap((r) => [r.id, r.v]);
                await pg.query(
                  `INSERT INTO spill_t VALUES ${rows.map((_, i) => `($${String(2 * i + 1)},$${String(2 * i + 2)})`).join(",")}`,
                  values,
                );
              }
              await core.harness.path.quiesce();
              await core.harness.path.recycleLog();
              const tx = await core.harness.path.begin();
              const statement = tx.statement({ rules });
              releaseStep = () => {
                statement.rollback();
                tx.rollback();
              };
              const preparation = await statement.prepareNativeStep(
                0,
                new AbortController().signal,
                { includesProgress: true },
              );
              releaseStep = () => {
                preparation.release();
                statement.rollback();
                tx.rollback();
              };
              const admit = governor.tryAdmit.bind(governor);
              admission = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
                if (options.label.endsWith("/fixed-buffers"))
                  expect(
                    options.minimumBytes + preparation.protectedWriteBytes,
                    "selected-row query plus native protected floor",
                  ).toBeLessThanOrEqual(governor.capBytes);
                return admit(options);
              });
              const sql = `SELECT id,v FROM spill_t WHERE id >= $1 AND id < $2 ORDER BY v ${descending ? "DESC" : "ASC"},id`;
              const plan = core.lowerer.lower(sql, core.context());
              if (plan.kind !== "query") throw Error("query");
              const ops = (r: typeof plan.plan.root): string[] => [
                r.op,
                ...("input" in r ? ops(r.input) : []),
              ];
              expect(ops(plan.plan.root)).toContain("indexRange");
              const result = await core.execute(sql, [1, n + 1], AbortSignal.timeout(60000));
              if (!("batches" in result)) throw Error("rows");
              expect(oracleRows(result)).toEqual((await pg.query(sql, [1, n + 1])).rows);
              expect(
                core.lastProfile?.operators
                  .filter((op) => op.op === "sort")
                  .reduce((sum, op) => sum + op.spilledBytes, 0),
              ).toBeGreaterThan(0);
              expect(created).toBeGreaterThan(0);
              expect(files).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              reach.reach("complete minimum");
              reach.reach("stored range");
              reach.reach("spilled sort");
              reach.reach("full payload exact");
              reach.reach(descending ? "descending" : "ascending");
              reach.reach("files retired");
            } finally {
              admission?.mockRestore();
              releaseStep?.();
              tracked.mockRestore();
              await core.close();
              expect(files).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              expect(governor.usedBytes).toBe(0);
              reach.reach("zero ownership");
            }
          },
        ),
        {
          seed,
          numRuns: 2,
          endOnFailure: true,
          examples: [
            [512, false],
            [640, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 180000);
