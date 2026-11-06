/**
 * Roots whose LSN an index build holds back (found by the R25 delta test, review B3-p2b-3 C2).
 * A root claims an LSN no later than the first index build no table checkpoint covers, so its
 * catalog is the catalog at that LSN. Three things follow:
 * - a build on a table with no unpublished data has no entries; a root settles it (an empty index
 *   root, the checkpoint moved), or it would hold every later root and log recycle back;
 * - a table created, altered or indexed after that LSN cannot be in the root: its flush waits,
 *   and the build's table is frozen so its flush (which settles the build) runs;
 * - a table dropped after that LSN stays in the root as it was (replay starts before the drop).
 */
import { describe, expect, it } from "vitest";
import { need } from "./bytes.js";
import type { WritePathImpl } from "./engine.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  tableInput,
} from "./test-support.js";

async function createTable(path: WritePathImpl, name: string): Promise<number> {
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput(name, [column("id", INT, false), column("v", INT)]),
        })
      ).id ?? 0;
  });
  return t;
}

async function createIndex(path: WritePathImpl, t: number): Promise<number> {
  const lsn = path.appliedLsn;
  await autocommit(path, (s) =>
    s.changeCatalog({
      kind: "createIndex",
      index: {
        tableId: t,
        name: `t${String(t)}_v`,
        columnIds: [need(path.catalog.table(t)?.columns[1]).id],
        unique: false,
        role: "secondary",
      },
    }),
  );
  expect(path.appliedLsn).toBeGreaterThan(lsn);
  return path.appliedLsn;
}

function insert(path: WritePathImpl, t: number, ids: number[]): Promise<unknown> {
  return autocommit(path, (s) => s.table(t).insert(batch(ints(ids), ints(ids)), null));
}

async function count(path: WritePathImpl, t: number): Promise<number> {
  const view = path.pin();
  try {
    return (await rowsOf(view, t)).length;
  } finally {
    view.release();
  }
}

const tableRoot = (path: WritePathImpl, t: number) =>
  path.state().root.tables.find((table) => table.tableId === t);

describe("roots an index build holds back", () => {
  it("a build on an empty table is settled by the next root, and the log can recycle", async () => {
    const harness = new Harness("strict", "memory", 4, undefined, { merges: { mode: "off" } });
    const path = await harness.open();
    const a = await createTable(path, "a");
    const b = await createTable(path, "b");
    const built = await createIndex(path, a);
    for (let from = 0; from < 16; from += 4)
      await insert(path, b, [from, from + 1, from + 2, from + 3]);
    await path.quiesce();
    expect(path.state().root.globalLsn).toBeGreaterThanOrEqual(built);
    expect(path.maintenance.buildsSettled).toBe(1);
    const index = need(
      path.catalog.table(a)?.indexes.find((candidate) => candidate.name === "t1_v"),
    );
    expect(
      tableRoot(path, a)?.secondaryIndexes.map((root) => [root.indexId, root.runs.length]),
    ).toEqual([[index.id, 0]]);
    await path.recycleLog();
    expect(path.maintenance.recycles).toBe(1);
    expect(path.maintenance.recyclesAbandoned).toBe(0);
    await harness.space.audit();
    const reopened = await harness.reopen();
    await insert(reopened, a, [1, 2, 3, 4, 5]);
    await reopened.quiesce();
    expect(await count(reopened, a)).toBe(5);
    expect(await count(reopened, b)).toBe(16);
    await harness.space.audit();
    await harness.close();
  });

  it("a table newer than the held root waits; the build's table flushes first", async () => {
    const harness = new Harness("strict", "memory", 4, undefined, { merges: { mode: "off" } });
    const path = await harness.open();
    const x = await createTable(path, "x");
    await insert(path, x, [1, 2]); // live, committed: the build's entries go with them
    const built = await createIndex(path, x);
    const y = await createTable(path, "y");
    for (let from = 0; from < 12; from += 4)
      await insert(path, y, [from, from + 1, from + 2, from + 3]);
    await path.quiesce();
    expect(path.stats().frozenMemtables).toBe(0);
    expect(path.state().root.globalLsn).toBeGreaterThanOrEqual(built);
    expect(tableRoot(path, y)?.groupCount).toBeGreaterThan(0);
    // Every allocated extent is reachable from a root (or retired): nothing written for a root
    // that left its table out.
    await harness.space.audit();
    expect(path.maintenance.flushesHeld).toBeGreaterThan(0);
    const reopened = await harness.reopen();
    expect(await count(reopened, x)).toBe(2);
    expect(await count(reopened, y)).toBe(12);
    await harness.space.audit();
    await harness.close();
  });

  it("an index dropped after the held LSN holds its table's flush until a root covers the drop", async () => {
    const harness = new Harness("strict", "memory", 4, undefined, { merges: { mode: "off" } });
    const path = await harness.open();
    const y = await createTable(path, "y");
    const x = await createTable(path, "x");
    await createIndex(path, y);
    await insert(path, y, [1, 2, 3, 4]);
    await insert(path, y, [5]);
    await path.quiesce();
    const index = need(tableRoot(path, y)?.secondaryIndexes[0]);
    expect(index.runs.length).toBe(1);
    await insert(path, x, [1, 2]);
    await createIndex(path, x);
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropIndex", indexId: index.indexId }));
    const dropped = path.appliedLsn;
    await insert(path, y, [6, 7, 8]);
    await insert(path, y, [9]);
    await path.quiesce();
    // Whenever y's root has lost the index, the root's catalog has too.
    expect(tableRoot(path, y)?.secondaryIndexes).toEqual([]);
    expect(path.state().root.globalLsn).toBeGreaterThanOrEqual(dropped);
    expect(tableRoot(path, y)?.groupCount).toBe(2);
    await harness.space.audit();
    const reopened = await harness.reopen();
    expect(await count(reopened, y)).toBe(9);
    expect(await count(reopened, x)).toBe(2);
    await harness.space.audit();
    await harness.close();
  });

  it("a table dropped after the held root's LSN stays in it, and the database reopens", async () => {
    const harness = new Harness("strict", "memory", 4, undefined, { merges: { mode: "off" } });
    const path = await harness.open();
    const w = await createTable(path, "w");
    const z = await createTable(path, "z");
    const x = await createTable(path, "x");
    await insert(path, z, [1, 2, 3, 4]);
    await insert(path, z, [5]); // freezes the full memtable
    await path.quiesce();
    expect(tableRoot(path, z)?.groupCount).toBe(1);
    await insert(path, x, [1, 2]);
    await createIndex(path, x);
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: z }));
    const dropped = path.appliedLsn;
    // w predates the build: it flushes under the held LSN.
    await insert(path, w, [1, 2, 3, 4]);
    await insert(path, w, [5]);
    await path.quiesce();
    expect(tableRoot(path, w)?.groupCount).toBe(1);
    expect(path.state().root.globalLsn).toBeLessThan(dropped);
    expect(tableRoot(path, z)?.groupCount).toBe(1);
    await harness.space.audit();
    const reopened = await harness.reopen();
    expect(reopened.catalog.table(z)).toBeUndefined();
    expect(await count(reopened, w)).toBe(5);
    expect(await count(reopened, x)).toBe(2);
    // Once a root covers the drop, the table and its extents go.
    await reopened.recycleLog();
    expect(reopened.state().root.tables.some((table) => table.tableId === z)).toBe(false);
    await harness.space.audit();
    await harness.close();
  });
});
