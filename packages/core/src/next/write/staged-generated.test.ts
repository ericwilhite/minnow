/**
 * Generated histories of large transactions against a model (format/replay.ts "Large
 * transactions"; access/write-path.ts "Commit"; access/snapshot.ts visibility). Small memtables
 * make most transactions stage. Each transaction inserts in many calls, deletes and updates rows
 * (committed ones and its own, staged or not), takes savepoints and rolls back to them, fails
 * statements on duplicate keys, then commits, rolls back, or the process crashes. After each one
 * the visible rows and every key probe equal the model's committed rows; the space audit runs
 * often, and crashes and reopens replay the log.
 */
import { describe, expect, it } from "vitest";
import { UniqueViolationError } from "../access/modify.js";
import type { Snapshot } from "../access/snapshot.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { need } from "./bytes.js";
import type { WritePathImpl } from "./engine.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  rowsOf,
  rules,
  tableInput,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };

const REACH = [
  "staged",
  "folded",
  "truncated-tail",
  "dropped-staged-memtable",
  "rollback-drop",
  "crash-unclaimed",
  "replayed-commit",
  "residual-tombstones",
  "unique-violation",
  "savepoint-rollback",
] as const;
const reach = createReach("next-write-staged", REACH);

/** Locators of `ids` in `view` (each id present exactly once). */
async function locate(view: Snapshot, t: number, ids: readonly number[]): Promise<number[]> {
  const keys = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints(ids), direction: "asc", nulls: "first" }],
    { length: ids.length, selection: null },
    keys,
  );
  const out = {
    probes: new Int32Array(ids.length + 1),
    locators: new Float64Array(ids.length + 1),
    count: 0,
  };
  await view.index(t, need(view.catalog.table(t)?.primaryKey)).probe(keys, out);
  keys.close();
  expect(out.count).toBe(ids.length);
  return Array.from(out.locators.subarray(0, out.count));
}

/** id → v of every row `view` sees. */
async function contents(view: Snapshot, t: number): Promise<Map<number, number>> {
  const out = new Map<number, number>();
  for (const row of await rowsOf(view, t)) {
    const id = Number(row[0]);
    expect(out.has(id), `row ${String(id)} is seen once`).toBe(false);
    out.set(id, Number(row[1]));
  }
  return out;
}

async function check(path: WritePathImpl, t: number, model: Map<number, number>, label: string) {
  const view = path.pin();
  try {
    const seen = await contents(view, t);
    expect(
      [...seen].sort((a, b) => a[0] - b[0]),
      label,
    ).toEqual([...model].sort((a, b) => a[0] - b[0]));
    // Every key resolves through the index layers (staged runs included) to exactly its row.
    const present = [...model.keys()];
    if (present.length > 0) await locate(view, t, present);
    const keys = new KeyBatch(FREE);
    const absent = Array.from({ length: 20 }, (_, k) => 5000 + k);
    encodeKeyBatch(
      "equality",
      [{ vector: ints(absent), direction: "asc", nulls: "first" }],
      { length: absent.length, selection: null },
      keys,
    );
    const out = { probes: new Int32Array(32), locators: new Float64Array(32), count: 0 };
    await view.index(t, need(view.catalog.table(t)?.primaryKey)).probe(keys, out);
    keys.close();
    expect(out.count, label).toBe(0);
  } finally {
    view.release();
  }
}

describe("staged large transactions, generated", () => {
  const SEEDS = seedsFor("next-write-staged", [901, 902, 903, 904]);
  for (const seed of SEEDS)
    it(`histories of large transactions equal the model (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const int = (n: number) => Math.floor(random() * n);
      // Flushes can be held before they publish: a crash right after a commit then finds its
      // staging unfolded, and the open replays its publishedRows.
      // The held generation's flushes never publish: a crashed process writes nothing more.
      let generation = 0;
      let held = -1;
      const harness = new Harness("strict", "sim", 6, undefined, {
        // Merges off: a crashed process must publish nothing more, and merges run on their own.
        merges: { mode: "off" },
        beforeFlushPublish: async () => {
          if (held === generation) await new Promise<never>(() => undefined);
        },
      });
      reach.beginHistory();
      let path = await harness.open();
      let t = 0;
      await autocommit(path, async (s) => {
        t =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput("t", [column("id", INT, false), column("v", INT)]),
            })
          ).id ?? 0;
      });
      const crash = () => {
        held = generation;
        generation += 1;
        return harness.crash();
      };
      const model = new Map<number, number>();
      let next = 1;
      const observe = () => {
        const m = path.maintenance;
        if (m.stages > 0) reach.reach("staged");
        if (m.stagedFolds > 0) reach.reach("folded");
        if (m.stagedTruncations > 0) reach.reach("truncated-tail");
        if (m.stagedMemtablesDropped > 0) reach.reach("dropped-staged-memtable");
        if (m.stagingRollbacks > 0) reach.reach("rollback-drop");
        if (m.stagingDropped > 0) reach.reach("crash-unclaimed");
        if (m.publishedReplays > 0) reach.reach("replayed-commit");
        if (m.stagedResidualEntries > 0) reach.reach("residual-tombstones");
      };
      for (let round = 0; round < 20; round++) {
        const tx = await path.begin();
        // The transaction's own view of the rows: the model plus its changes, per savepoint.
        let mine = new Map(model);
        const savepoints: Array<{
          point: ReturnType<typeof tx.savepoint>;
          rows: Map<number, number>;
        }> = [];
        const steps = 3 + int(10);
        for (let step = 0; step < steps; step++) {
          const roll = int(100);
          const s = tx.statement({ rules });
          try {
            if (roll < 45) {
              // Many calls of fresh rows: memtables fill and stage between calls.
              const calls = 1 + int(5);
              for (let call = 0; call < calls; call++) {
                const ids = Array.from({ length: 1 + int(7) }, () => next++);
                const values = ids.map(() => int(1000));
                await s.table(t).insert(batch(ints(ids), ints(values)), null);
                ids.forEach((id, k) => mine.set(id, values[k] ?? 0));
              }
              await s.end();
            } else if (roll < 60 && mine.size > 0) {
              const ids = [
                ...new Set(
                  Array.from({ length: 1 + int(4) }, () => need([...mine.keys()][int(mine.size)])),
                ),
              ];
              await s.table(t).delete(batch(locators(await locate(s.view, t, ids), t)), null);
              await s.end();
              for (const id of ids) mine.delete(id);
            } else if (roll < 72 && mine.size > 0) {
              const ids = [
                ...new Set(
                  Array.from({ length: 1 + int(4) }, () => need([...mine.keys()][int(mine.size)])),
                ),
              ];
              const values = ids.map(() => 1000 + int(1000));
              const v = need(path.catalog.table(t)?.columns[1]).id;
              await s
                .table(t, { assigned: [v] })
                .update(batch(locators(await locate(s.view, t, ids), t), ints(values)), null);
              await s.end();
              ids.forEach((id, k) => mine.set(id, values[k] ?? 0));
            } else if (roll < 80 && mine.size > 0) {
              // A duplicate key fails the statement: all of it rolls back.
              const ids = [next++, next++];
              await s.table(t).insert(batch(ints(ids), ints([1, 2])), null);
              const dup = need([...mine.keys()][int(mine.size)]);
              await s.table(t).insert(batch(ints([dup]), ints([3])), null);
              await expect(s.end()).rejects.toBeInstanceOf(UniqueViolationError);
              reach.reach("unique-violation");
              s.rollback();
              continue;
            } else if (roll < 90) {
              s.rollback();
              savepoints.push({ point: tx.savepoint(), rows: new Map(mine) });
              continue;
            } else {
              s.rollback();
              const saved = savepoints.pop();
              if (saved !== undefined) {
                tx.rollbackTo(saved.point);
                mine = saved.rows;
                reach.reach("savepoint-rollback");
              }
              continue;
            }
          } catch (error) {
            s.rollback();
            throw error;
          }
          observe();
        }
        const end = int(100);
        if (end < 70) {
          const unfolded = int(100) < 25;
          if (unfolded) held = generation;
          await (
            await tx.commit(null)
          ).durable;
          model.clear();
          for (const [id, v] of mine) model.set(id, v);
          if (unfolded) {
            observe();
            path = await crash();
            observe();
            await check(path, t, model, `seed ${String(seed)} round ${String(round)} unfolded`);
            continue;
          }
        } else if (end < 88) tx.rollback();
        else {
          // The process dies mid-transaction: nothing of it may survive.
          observe();
          path = await crash();
          observe();
          await check(path, t, model, `seed ${String(seed)} round ${String(round)} after a crash`);
          continue;
        }
        observe();
        await check(path, t, model, `seed ${String(seed)} round ${String(round)}`);
        const after = int(100);
        if (after < 15) {
          path = await crash();
          observe();
          await check(path, t, model, `seed ${String(seed)} round ${String(round)} crash`);
        } else if (after < 25) {
          await path.quiesce();
          observe();
          await harness.space.audit();
          path = await harness.reopen();
          observe();
          await check(path, t, model, `seed ${String(seed)} round ${String(round)} reopen`);
        } else if (after < 50) {
          await path.quiesce();
          observe();
          await harness.space.audit();
        }
      }
      await path.quiesce();
      // An open drops staging no frame claimed at its first publication: make one.
      await path.recycleLog();
      observe();
      const final = path.state().root.tables.find((table) => table.tableId === t);
      expect(final?.staged ?? []).toEqual([]);
      await harness.space.audit();
      await check(path, t, model, `seed ${String(seed)} end`);
      await harness.close();
    }, 300_000);

  it("reached every declared state", () => {
    reach.assertReached();
  });
});
