/** UPDATE/DELETE score their statement snapshot before filtering or changing any document. */
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, FTS_WRITE_REACH } from "../../testing/reach.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import type { ParameterValue } from "../exec/executor.js";
import { StorageCore } from "./storage-core.js";

/** This base does not execute `modify` yet. Execute its real lowered source through
 * the ordinary query executor; the write operation itself remains stage 4's contract. */
async function selectedIds(core: StorageCore, sql: string, parameters: readonly ParameterValue[]) {
  const lowered = core.lowerer.lower(sql, core.context());
  if (lowered.kind !== "modify" || lowered.plan.root.op !== "modify") throw Error("modify plan");
  const input = lowered.plan.root.input;
  if (input.op !== "project") throw Error("modify source projection");
  const source = input.input;
  const index = source.output.findIndex((column) => column.name === "id");
  const output = source.output[index];
  if (!output) throw Error("source id");
  const prepared = core.executor.prepare({
    ...lowered.plan,
    root: {
      op: "project",
      id: 1_000_000,
      input: source,
      exprs: [{ op: "column", index, type: output.type }],
      output: [output],
    },
    columns: [output],
  });
  const run = core.start(prepared, parameters);
  const ids: number[] = [];
  try {
    for (;;) {
      const frame = await run.execution.results.next();
      if (!frame) break;
      try {
        const column = frame.payload.columns[0];
        if (column?.form !== "flat" || column.data.layout !== "i32") throw Error("source int4");
        ids.push(...column.data.values.subarray(0, frame.payload.rowCount));
      } finally {
        frame.release();
      }
    }
    await run.execution.executed;
    await run.execution.delivered;
    return ids.sort((a, b) => a - b);
  } catch (error) {
    run.execution.cancel(error);
    await Promise.allSettled([run.execution.executed, run.execution.delivered]);
    throw error;
  } finally {
    run.release();
  }
}
for (const seed of seedsFor("next-fts-write", [0xb801, 0xb802]))
  it(`keeps UPDATE/DELETE source scoring exact before predicates (${String(seed)})`, async () => {
    const core = await new StorageCore(16).open(),
      db = new MinnowDatabase(new MemoryBlockStore(), { ftsAutoIndexRows: 1_000_000 }),
      random = mulberry32(seed);
    const reach = createReach(`FTS write ${String(seed)}`, FTS_WRITE_REACH);
    try {
      const ddl = "CREATE TABLE docs(id INTEGER PRIMARY KEY,a TEXT)";

      const rows = Array.from({ length: 16 }, (_, id) => ({
        id,
        a: id === 0 ? "alpha" : id % 7 === 0 ? null : random() < 0.5 ? "alpha alpha" : "beta",
      }));

      for (let round = 0; round < 4; round++) {
        reach.beginHistory();
        if (round) {
          await core.execute("DROP TABLE docs");
          await db.execute("DROP TABLE docs");
        }
        await core.execute(ddl);
        await db.execute(ddl);
        await core.writeBatch("insertBatch", "docs", rows);
        await db.insertBatch("docs", rows);
        const fresh = [{ id: 32 + round, a: "alpha beta" }];
        await core.writeBatch("insertBatch", "docs", fresh);
        await db.insertBatch("docs", fresh);
        const query = round % 2 ? "al*" : "alpha";
        const scores = (
          await db.query("SELECT BM25(a) AGAINST $1 s FROM docs", {
            params: [query],
            memoize: false,
          })
        ).rows
          .map((row) => row.s)
          .filter((s): s is number => typeof s === "number" && s > 0);
        const threshold = Math.min(...scores) * 0.95;
        const update = round % 2 === 0;
        const sql = `${update ? "UPDATE docs SET a='beta'" : "DELETE FROM docs"} WHERE id%2=0 AND BM25(a) AGAINST $1>$2 RETURNING id`;
        const expected = await db.execute(sql, [query, threshold]);
        if (expected.kind !== "update" && expected.kind !== "delete") throw Error("shipped modify");
        const actual = await selectedIds(core, sql, [query, threshold]);
        expect(actual).toEqual(
          (expected.returnedRows ?? []).map((row) => Number(row.id)).sort((a, b) => a - b),
        );
        expect(actual.length).toBe(expected.rowCount);
        reach.reach(update ? "UPDATE" : "DELETE");
        reach.reach("whole statement corpus");
        reach.reach("parameters and threshold");
        expect(core.spill.bytesInUse).toBe(0);
        reach.reach("fresh inserted corpus");
      }
      reach.assertReached();
    } finally {
      await core.close();
      await db.close();
    }
  }, 60000);
