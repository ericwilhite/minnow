/** A native SQL writer cannot choose the direct-native legacy repeat Set. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_TRACKING_CAPABILITY_REACH } from "../../testing/reach.js";
import { StatementImpl } from "../write/transaction.js";
import { ChunkReader } from "../page/reader.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

for (const seed of seedsFor("next-modify-tracking-capability", [0xb5d0]))
  it(`requires admitted tracking even for raw SQL execution seed ${String(seed)}`, async () => {
    const reach = createReach("SQL repeat capability", MODIFY_TRACKING_CAPABILITY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("update", "delete", "upsert"),
        fc.boolean(),
        fc.boolean(),
        async (kind, returning, cached) => {
          reach.beginHistory();
          const core = await new StorageCore().open();
          const touched = vi
            .spyOn(StatementImpl.prototype, "touched", "get")
            .mockImplementation(() => {
              throw new Error("uncharged repeat Set reached");
            });
          try {
            await core.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)");
            await core.execute("CREATE TABLE s(sid INTEGER PRIMARY KEY,tid INTEGER,v INTEGER)");
            await core.execute("INSERT INTO t VALUES(1,10),(2,20)");
            await core.execute("INSERT INTO s VALUES(1,1,101),(2,1,999),(3,2,202)");
            const sql =
              (kind === "update"
                ? "UPDATE t SET v=s.v FROM s WHERE t.id=s.tid"
                : kind === "delete"
                  ? "DELETE FROM t USING s WHERE t.id=s.tid"
                  : "INSERT INTO t SELECT tid,v FROM s WHERE sid<>2 ON CONFLICT(id) DO UPDATE SET v=EXCLUDED.v") +
              (returning ? " RETURNING t.id,t.v" : "");
            const lowered = core.lowerer.lower(sql, core.context());
            if (lowered.kind !== "modify" || lowered.plan.root.op !== "modify")
              throw new Error("modify");
            const tx = await core.harness.path.begin(),
              statement = tx.statement({
                rules: () => ({ check: () => null, generate: () => undefined }),
              });
            const root = lowered.plan.root;
            if (cached)
              statement.table(root.tableId, {
                assigned: root.assigned,
                onRepeat: root.onRepeat,
                uniqueTargets: root.uniqueTargets === true,
              });
            const execution = core.executor.prepare(lowered.plan).execute({
              snapshot: null,
              statement,
              parameters: [],
              statementTime: 0n,
              governor: core.harness.governor,
              spill: core.spill,
              queryId: 1,
              reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
              sequences: null,
              sequenceSession: null,
              returning: "atExecuted",
            });
            try {
              await expect(execution.executed).rejects.toThrow(/requires admitted target tracking/);
            } finally {
              execution.cancel();
              await Promise.allSettled([execution.executed, execution.delivered]);
              tx.rollback();
            }
            expect(touched).not.toHaveBeenCalled();
            reach.reach("raw caller refused");
            reach.reach(cached ? "cached writer" : "fresh writer");
            const before = await core.execute("SELECT * FROM t ORDER BY id");
            if (before.kind !== "rows" || !("batches" in before)) throw new Error("rows");
            expect(oracleRows(before)).toEqual([
              { id: 1, v: 10 },
              { id: 2, v: 20 },
            ]);
            await core.execute(sql);
            const after = await core.execute("SELECT * FROM t ORDER BY id");
            if (after.kind !== "rows" || !("batches" in after)) throw new Error("rows");
            expect(oracleRows(after)).toEqual(
              kind === "delete"
                ? []
                : [
                    { id: 1, v: 101 },
                    { id: 2, v: 202 },
                  ],
            );
            expect(touched).not.toHaveBeenCalled();
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach(kind);
            reach.reach(returning ? "RETURNING" : "plain");
            reach.reach("owned SQL tracked");
          } finally {
            touched.mockRestore();
            await core.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          ["update", false, false],
          ["delete", true, true],
          ["upsert", true, true],
          ["upsert", false, false],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
