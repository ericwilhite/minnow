/**
 * A read by key retains its selected memtable chunk from sizing to close.
 *
 * Law (access/snapshot.ts "Borrow", write/view.ts `prepareRead({ locator })`): a retained chunk
 * stays readable after its memtable publishes; publication lends its columns, charged to the write
 * owner (`WritePathStats.borrowedChunkBytes`), until the reader releases them. A read sized over a
 * retained chunk funds no publication it will not read (write/memory-read.ts: a retained hint has
 * no resident envelope), so the retention must hold from sizing until the query ends, and
 * release on success and on cancellation alike.
 *
 * `QueryRuntime.afterReservation` publishes the memtable after the read is sized and before it
 * executes: the read then returns its exact row from the retained chunk, the lent chunk is charged
 * while it runs, and nothing stays lent or held once the query ends, while its snapshot is still
 * pinned (a transaction's snapshot outlives its statements).
 */
import { afterEach, expect, it } from "vitest";
import { QueryRuntime } from "../exec/run.js";
import { Governor, type Grant } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";

const WIDTH = 1 << 20;

afterEach(() => {
  QueryRuntime.afterReservation = null;
  Governor.observeHoldings = null;
});

it.each([{ cancel: false }, { cancel: true }])(
  "a key read over a memtable published after sizing (cancel $cancel)",
  async ({ cancel }) => {
    // Memtables of two rows: the second and third inserts freeze and publish the read's chunk.
    const core = await new StorageCore(2).open();
    const recipes = new Map<Grant, number>();
    Governor.observeHoldings = (grant, delta) => {
      if (grant.label.endsWith("/source-recipe"))
        recipes.set(grant, (recipes.get(grant) ?? 0) + delta);
    };
    try {
      await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
      const value = "a".repeat(WIDTH);
      await core.writeBatch("insertBatch", "t", [{ id: 1, v: value }]);
      expect(core.harness.path.stats().borrowedChunkBytes).toBe(0);
      const controller = new AbortController();
      let lent = -1;
      QueryRuntime.afterReservation = async () => {
        QueryRuntime.afterReservation = null;
        expect(
          [...recipes.values()].some((held) => held > 0),
          "retained at sizing",
        ).toBe(true);
        await core.writeBatch("insertBatch", "t", [
          { id: 2, v: "b" },
          { id: 3, v: "c" },
        ]);
        await core.harness.path.quiesce();
        expect(core.harness.path.stats().frozenMemtables, "published").toBe(0);
        lent = core.harness.path.stats().borrowedChunkBytes;
        if (cancel) controller.abort("cancel the retained read");
      };
      const run = core.start(core.prepare("SELECT v FROM t WHERE id = 1"), [], controller.signal);
      const values: string[] = [];
      try {
        const read = (async () => {
          for (let frame = await run.execution.results.next(); frame !== null;) {
            try {
              const column = frame.payload.columns[0];
              if (column?.form !== "flat" || column.data.layout !== "varlen")
                throw new TypeError("text column expected");
              for (let row = 0; row < frame.payload.rowCount; row++)
                values.push(
                  new TextDecoder().decode(
                    column.data.bytes.subarray(
                      column.data.offsets[row],
                      column.data.offsets[row + 1],
                    ),
                  ),
                );
            } finally {
              frame.release();
            }
            frame = await run.execution.results.next();
          }
          await run.execution.executed;
          await run.execution.delivered;
        })();
        if (cancel) await expect(read).rejects.toMatchObject({ sqlState: "57014" });
        else {
          await read;
          expect(values.length).toBe(1);
          expect(values[0] === value, "the row read from the retained chunk").toBe(true);
        }
        await Promise.allSettled([run.execution.executed, run.execution.delivered]);
        // Released at query end, while the snapshot is still pinned.
        expect(core.harness.path.stats().borrowedChunkBytes, "nothing stays lent").toBe(0);
      } finally {
        run.release();
      }
      // The publication lent the retained chunk's columns, charged to the write owner: at least
      // the selected value, at most the memtable chunk that held it.
      expect(lent, "the retained chunk is lent while the read runs").toBeGreaterThanOrEqual(WIDTH);
      expect(lent).toBeLessThan(4 * WIDTH);
      expect(recipes.size, "one retained source").toBe(1);
      expect([...recipes.values()], "the recipe's control is released").toEqual([0]);
    } finally {
      await core.close();
    }
  },
);
