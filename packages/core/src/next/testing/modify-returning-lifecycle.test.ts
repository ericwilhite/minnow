/** exec/executor.ts R2: executed, committed and delivered are separate completions. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_RETURNING_LIFECYCLE_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { startModification } from "./modify-native-test-support.js";
import { yieldTask } from "../exec/pipeline.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-returning-completions", [0xb5b7, 0xb5b8]))(
  "gates and releases buffered RETURNING (seed %i)",
  async (seed) => {
    const core = await new StorageCore(64).open();
    const reach = createReach("RETURNING completions", MODIFY_RETURNING_LIFECYCLE_REACH);
    try {
      await core.execute("CREATE TABLE gate (id INTEGER PRIMARY KEY,v INTEGER,note TEXT)");
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("insert" as const, "update" as const, "delete" as const),
          fc.constantFrom("atCommit" as const, "atExecuted" as const),
          fc.boolean(),
          fc.integer({ min: 2, max: 40 }),
          async (kind, gate, cancel, n) => {
            reach.beginHistory();
            await core.execute("DELETE FROM gate");
            const values = Array.from(
              { length: n },
              (_, i) => `(${String(i)},${String(i + 1)},'${"x".repeat(1000)}')`,
            ).join(",");
            if (kind !== "insert") await core.execute(`INSERT INTO gate VALUES ${values}`);
            const sql =
              kind === "insert"
                ? `INSERT INTO gate VALUES ${values} RETURNING id,v,note`
                : kind === "update"
                  ? "UPDATE gate SET v=v+1 RETURNING id,v,note"
                  : "DELETE FROM gate RETURNING id,v,note";
            const { tx, execution } = await startModification(core, sql, gate);
            try {
              const summary = await execution.executed;
              expect(summary.rowCount).toBe(n);
              reach.reach(`gate ${kind}`);
              await yieldTask();
              if (gate === "atCommit") {
                const premature = execution.results.poll();
                premature?.release();
                expect(premature).toBeUndefined();
                expect(core.spill.bytesInUse).toBeGreaterThan(0);
                reach.reach("atCommit hidden");
              } else reach.reach("atExecuted");
              const receipt = await tx.commit(null);
              await receipt.durable;
              if (cancel) {
                execution.cancel("discard buffered rows");
                await expect(execution.delivered).rejects.toMatchObject({ sqlState: "57014" });
                reach.reach("cancel gate");
              } else {
                execution.acknowledgeCommit();
                execution.acknowledgeCommit();
                let rows = 0;
                for (;;) {
                  const frame = await execution.results.next();
                  if (frame === null) break;
                  rows += frame.payload.rowCount;
                  frame.release();
                }
                expect(rows).toBe(n);
                await execution.delivered;
              }
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("delivery cleanup");
            } finally {
              execution.cancel("fixture cleanup");
              await Promise.allSettled([execution.executed, execution.delivered]);
              tx.rollback();
            }
          },
        ),
        {
          seed,
          numRuns: 20,
          examples: [
            ["insert", "atCommit", false, 3],
            ["update", "atCommit", true, 3],
            ["delete", "atExecuted", false, 3],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  },
  60000,
);

it.each([
  ["INSERT INTO atomic_ret VALUES (1,1),(2,0) RETURNING id,10/v AS result", "22012"],
  ["INSERT INTO atomic_ret VALUES (1,1),(1,2) RETURNING id,v", "23505"],
])("hides all rows and preserves the transaction on late failure: %s", async (sql, sqlState) => {
  const core = await new StorageCore().open();
  try {
    await core.execute("CREATE TABLE atomic_ret (id INTEGER PRIMARY KEY,v INTEGER)");
    const { tx, execution } = await startModification(core, sql, "atExecuted");
    try {
      await expect(execution.executed).rejects.toMatchObject({ sqlState });
      await expect(execution.results.next()).rejects.toMatchObject({ sqlState });
      await execution.delivered.catch(() => undefined);
      const next = tx.statement({
        rules: () => ({ check: () => null, generate: () => undefined }),
      });
      await next.end();
      const receipt = await tx.commit(null);
      await receipt.durable;
    } finally {
      tx.rollback();
    }
    const rows = await core.execute("SELECT * FROM atomic_ret ORDER BY id");
    if (rows.kind !== "rows" || !("batches" in rows)) throw new Error("rows");
    expect(oracleRows(rows)).toEqual([]);
    expect(core.spill.bytesInUse).toBe(0);
  } finally {
    await core.close();
  }
});
