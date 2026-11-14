/**
 * Ruling R25 / R8: an extent is released by exactly one publication. A rolled-back transaction's
 * staging has two retirement routes - the rollback's own deferred retirement (`#dropStaging`) and,
 * when its table was dropped, the next root's walk of everything the durable table root reaches
 * (`#nextRoot`). Which of them runs first depends on who holds the publish lock when the rollback
 * happens. This file forces the order the random campaign (`extent-deltas.test.ts`, seed 701)
 * reached only by timing: the walk first, then the deferred retirement.
 *
 * The order, by the lock queue (FIFO):
 *   1. a staged transaction on table T is durable in the root;
 *   2. a merge holds the lock (held in the sink's `prepare`);
 *   3. a flush asks for the lock behind it;
 *   4. the transaction rolls back: its deferred retirement queues behind the flush;
 *   5. DROP TABLE T commits;
 *   6. the merge finishes, then the flush publishes a root without T, whose walk retires T's
 *      staging; then the deferred retirement runs.
 * Whatever publishes next must not release the staging a second time.
 */
import { describe, expect, it } from "vitest";
import type { ExtentDeltaSink, PublishDelta } from "../access/write-path.js";
import { reachableExtents } from "../format/reachable.js";
import type { ExtentSpace } from "../storage/extents.js";
import { need } from "./bytes.js";
import { yieldTask } from "./merge.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rules,
  tableInput,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };

/** Every extent the space's current root reaches, by offset → length. */
async function walk(space: ExtentSpace): Promise<Map<number, number>> {
  const out = new Map<number, number>();
  for await (const reached of reachableExtents()(need(space.root), space, FREE)) {
    const extent = "extent" in reached ? reached.extent : reached;
    if (extent.length === 0) continue;
    out.set(extent.offset, extent.length);
  }
  return out;
}

/** Task turns enough for every continuation already runnable to run. */
async function settle(): Promise<void> {
  for (let turn = 0; turn < 8; turn++) await yieldTask();
}

function gate(): { promise: Promise<void>; open: () => void } {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { promise, open };
}

describe("an extent is released by one publication (ruling R25)", () => {
  it("a rolled-back staging whose table was dropped is not released twice", async () => {
    const WIDTH = 6;
    const flushHeld = gate();
    const flushAtHook = gate();
    const mergeHeld = gate();
    const mergeAtSink = gate();
    let holdFlush = false;
    let holdMerge = false;
    const harness = new Harness("strict", "sim", WIDTH, undefined, {
      merges: { mode: "manual", fanIn: 2 },
      beforeFlushPublish: async () => {
        if (!holdFlush) return;
        holdFlush = false;
        flushAtHook.open();
        await flushHeld.promise;
      },
    });
    // The committed list, replayed from the deltas: what the publications named released must be
    // in it (the keeper checks the same), and the final walk must equal it.
    const set = new Map<number, number>();
    let pending: Pick<PublishDelta, "added" | "released"> | null = null;
    const sink: ExtentDeltaSink = {
      prepare: async (_attempt, delta) => {
        pending = { added: [...delta.added], released: [...delta.released] };
        if (holdMerge) {
          holdMerge = false;
          mergeAtSink.open();
          await mergeHeld.promise;
        }
        return null;
      },
      commit: () => {
        const { added, released } = need(pending);
        for (const extent of released) set.delete(extent.offset);
        for (const extent of added) set.set(extent.offset, extent.length);
        pending = null;
      },
      abort: () => {
        pending = null;
      },
    };
    harness.extentDeltas = sink;
    const path = await harness.open();
    try {
      const create = async (name: string) => {
        let id = 0;
        await autocommit(path, async (s) => {
          id =
            (
              await s.changeCatalog({
                kind: "createTable",
                table: tableInput(name, [column("id", INT, false)]),
              })
            ).id ?? 0;
        });
        return id;
      };
      const target = await create("target");
      const other = await create("other");
      const rowsFor = (from: number) => Array.from({ length: WIDTH }, (_, k) => from + k);
      const fill = async (from: number) => {
        await autocommit(path, (s) => s.table(other).insert(batch(ints(rowsFor(from))), null));
        await autocommit(path, (s) => s.table(other).insert(batch(ints([from + WIDTH])), null));
      };
      // Two level-0 runs of one index: a merge is due (manual mode: only when asked).
      await fill(0);
      await path.quiesce();
      await fill(100);
      await path.quiesce();

      // A committed, frozen memtable whose flush stops before it takes the lock.
      holdFlush = true;
      await fill(200);
      await flushAtHook.promise;

      // 1. The staged transaction on `target`, durable in the root.
      const tx = await path.begin();
      const s = tx.statement({ rules });
      for (let call = 0; call < 3; call++) {
        const ids = rowsFor(1000 + call * 100);
        await s.table(target).insert(batch(ints(ids)), null);
      }
      await s.end();
      expect(path.maintenance.stages, "the transaction staged rows").toBeGreaterThan(0);
      const staged = new Map(set);

      // 2. A merge holds the lock.
      holdMerge = true;
      const merging = path.runMaintenance();
      await mergeAtSink.promise;

      // 3. The flush asks for the lock behind it.
      flushHeld.open();
      await settle();

      // 4. The rollback: its deferred retirement queues behind the flush.
      tx.rollback();
      await settle();

      // 5. DROP TABLE commits; the flush's root will not carry `target`.
      await autocommit(path, (statement) =>
        statement.changeCatalog({ kind: "dropTable", tableId: target }),
      );

      // 6. The merge finishes, the flush publishes (its walk releases the staging), then the
      //    rollback's retirement runs; the next publication drains what it left.
      mergeHeld.open();
      await merging;
      await path.quiesce();
      let released = 0;
      for (const [offset] of staged) if (!set.has(offset)) released++;
      expect(released, "the dropped table's walk released the staging").toBeGreaterThan(0);
      await fill(300);
      await path.quiesce();
      await fill(400);
      await path.quiesce();

      expect(path.failure).toBeNull();
      await harness.space.audit();
      expect(set).toEqual(await walk(harness.space));
    } finally {
      flushHeld.open();
      mergeHeld.open();
      await harness.close();
    }
  }, 60_000);
});
