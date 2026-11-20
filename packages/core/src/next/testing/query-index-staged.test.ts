/** Native transaction staged rows are visible to a later statement's indexed SELECT. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, QUERY_INDEX_STAGED_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { batch, ints, rules } from "../write/test-support.js";
import { Governor } from "../memory/governor.js";
for (const seed of seedsFor("next-query-index-staged", [47803, 47804]))
  it(`reads exact staged rowsets through native query cursors seed${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("staged query indexes", QUERY_INDEX_STAGED_REACH);
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 17, max: 35 }), fc.boolean(), async (n, rollback) => {
          reach.beginHistory();
          const core = new StorageCore(4);
          Object.assign(core.harness, { governor: new Governor(32 << 20) });
          await core.open();
          const ctx = core.context.bind(core);
          core.context = () => ({
            ...ctx(),
            statistics: { table: () => ({ rowCount: 1000000, column: () => null }) },
          });
          let ended = false;
          let tx: Awaited<ReturnType<typeof core.harness.path.begin>> | null = null;
          try {
            await pg.exec(
              "DROP TABLE IF EXISTS stage_t; CREATE TABLE stage_t(id INT PRIMARY KEY,v INT); CREATE INDEX stage_v ON stage_t(v)",
            );
            await core.execute("CREATE TABLE stage_t(id INT PRIMARY KEY,v INT)");
            await core.execute("CREATE INDEX stage_v ON stage_t(v)");
            const table = core.harness.path.catalog.tableByName("stage_t");
            if (!table) throw Error("missing table");
            tx = await core.harness.path.begin();
            const insert = tx.statement({ rules });
            for (let at = 1; at <= n; at += 4) {
              const ids = Array.from({ length: Math.min(4, n - at + 1) }, (_, i) => at + i);
              await insert
                .table(table.id)
                .insert(batch(ints(ids), ints(ids.map((i) => i % 4))), null);
            }
            await insert.end();
            const probe = core.harness.path.pin();
            try {
              expect(
                probe
                  .internalTable(table.id)
                  .tableView.memtables.some((m) => (m.staged?.runs.size ?? 0) > 0),
              ).toBe(true);
            } finally {
              probe.release();
            }
            reach.reach("staged native input");
            await pg.exec("BEGIN");
            await pg.exec(
              `INSERT INTO stage_t VALUES ${Array.from({ length: n }, (_, i) => `(${String(i + 1)},${String((i + 1) % 4)})`).join(",")}`,
            );
            const read = tx.statement({ rules });
            const prepare = core.executor.prepare.bind(core.executor);
            const within = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
              const compiled = prepare(plan);
              return {
                ...compiled,
                execute: (request) =>
                  compiled.execute({ ...request, snapshot: read.view, statement: null }),
              };
            });
            try {
              for (const [sql, params] of [
                ["SELECT id,v FROM stage_t WHERE id IN($1,$2,$3,$4) ORDER BY id", [1, n, null, 1]],
                ["SELECT id,v FROM stage_t WHERE v >= $1 AND v < $2 ORDER BY id", [1, 3]],
              ] as const) {
                const lowered = core.lowerer.lower(sql, core.context());
                if (lowered.kind !== "query") throw Error("query");
                const result = await core.execute(sql, params);
                if (!("batches" in result)) throw Error("rows");
                expect(oracleRows(result)).toEqual((await pg.query(sql, [...params])).rows);
              }
            } finally {
              within.mockRestore();
              await read.end();
            }
            reach.reach("point and range");
            reach.reach("NULL and duplicates");
            if (rollback) {
              tx.rollback();
              await pg.exec("ROLLBACK");
              reach.reach("rollback");
            } else {
              await (
                await tx.commit(null)
              ).durable;
              await pg.exec("COMMIT");
              reach.reach("commit");
            }
            ended = true;
            await core.reopen();
            const sql = "SELECT id,v FROM stage_t WHERE id IN($1,$2) ORDER BY id";
            const actual = await core.execute(sql, [1, n]);
            if (!("batches" in actual)) throw Error("rows");
            expect(oracleRows(actual)).toEqual((await pg.query(sql, [1, n])).rows);
            reach.reach("reopen");
          } finally {
            if (tx && !ended) tx.rollback();
            await core.close();
            expect(core.harness.governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero ownership");
          }
        }),
        {
          seed,
          numRuns: 6,
          endOnFailure: true,
          examples: [
            [20, false],
            [24, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
