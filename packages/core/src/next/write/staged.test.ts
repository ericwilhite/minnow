/**
 * Staged large transactions (format/replay.ts "Large transactions", access/modify.ts "Memory and
 * async"): a transaction's full memtables are written as staged groups and runs and freed while
 * it runs; a root lists them as staged (present, invisible); the commit frame's `publishedRows`
 * makes them visible; a later flush folds them into the table's groups.
 */
import { describe, expect, it } from "vitest";
import { UniqueViolationError } from "../access/modify.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import type { Snapshot } from "../access/snapshot.js";
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
  rules,
  tableInput,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };

async function createTable(path: WritePathImpl, name = "t"): Promise<number> {
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

const range = (from: number, count: number) => Array.from({ length: count }, (_, k) => from + k);

/** Inserts `ids` in calls of `step` rows (staging happens at a call's start, as for batches). */
async function insertIn(
  writer: { insert: (b: ReturnType<typeof batch>, w: null) => unknown },
  ids: readonly number[],
  step = 8,
): Promise<void> {
  for (let k = 0; k < ids.length; k += step) {
    const part = ids.slice(k, k + step);
    await writer.insert(batch(ints(part), ints(part)), null);
  }
}

/** Row ids the primary key holds for `ids` in `view` (one per found key). */
async function probe(view: Snapshot, t: number, ids: readonly number[]): Promise<number> {
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
  return out.count;
}

async function ids(view: Snapshot, t: number): Promise<number[]> {
  return (await rowsOf(view, t)).map((row) => Number(row[0])).sort((a, b) => a - b);
}

async function seen(path: WritePathImpl, t: number): Promise<number[]> {
  const view = path.pin();
  try {
    return await ids(view, t);
  } finally {
    view.release();
  }
}

const tableRoot = (path: WritePathImpl, t: number) =>
  path.state().root.tables.find((table) => table.tableId === t);

describe("staged large transactions", () => {
  it("stages full memtables while the transaction runs, commits them at once, folds them", async () => {
    const harness = new Harness("strict", "sim", 8);
    const path = await harness.open();
    const t = await createTable(path);
    const tx = await path.begin();
    for (let from = 0; from < 64; from += 8) {
      const s = tx.statement({ rules });
      await s.table(t).insert(batch(ints(range(from, 8)), ints(range(from, 8))), null);
      await s.end();
    }
    expect(path.maintenance.stages).toBeGreaterThan(0);
    expect(path.maintenance.stagedRows).toBeGreaterThanOrEqual(48);
    // Listed as staged in the durable root; no group, nothing visible to any reader.
    expect(tableRoot(path, t)?.staged.length).toBe(1);
    expect(tableRoot(path, t)?.groupCount).toBe(0);
    expect(await seen(path, t)).toEqual([]);
    const reader = path.pin();
    expect(await probe(reader, t, range(0, 64))).toBe(0);
    // The transaction's next statement sees every earlier row, staged or not, and its keys.
    const s = tx.statement({ rules });
    expect(await ids(s.view, t)).toEqual(range(0, 64));
    expect(await probe(s.view, t, range(0, 64))).toBe(64);
    // A duplicate of a staged key is caught.
    await expect(
      (async () => {
        await s.table(t).insert(batch(ints([3]), ints([0])), null);
        await s.end();
      })(),
    ).rejects.toBeInstanceOf(UniqueViolationError);
    s.rollback();
    await (
      await tx.commit(null)
    ).durable;
    // The old reader still sees nothing; a new one sees all of it.
    expect(await ids(reader, t)).toEqual([]);
    reader.release();
    expect(await seen(path, t)).toEqual(range(0, 64));
    await path.quiesce();
    expect(path.maintenance.stagedFolds).toBeGreaterThan(0);
    expect(tableRoot(path, t)?.staged).toEqual([]);
    expect(tableRoot(path, t)?.liveRowCount).toBe(64);
    const view = path.pin();
    expect(await probe(view, t, range(0, 64))).toBe(64);
    view.release();
    await harness.space.audit();
    const reopened = await harness.reopen();
    expect(await seen(reopened, t)).toEqual(range(0, 64));
    await harness.space.audit();
    await harness.close();
  });

  it("a transaction still writing never has its rows flushed (C1's forced freeze included)", async () => {
    // A 1-row memtable: every row of the open transaction freezes one; C1's tail freeze and every
    // flush must leave them alone until the commit.
    const harness = new Harness("strict", "sim", 1);
    const path = await harness.open();
    const t = await createTable(path);
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1000]), ints([0])), null));
    await path.quiesce();
    const before = tableRoot(path, t) ?? { groupCount: 0, liveRowCount: 0 };
    const tx = await path.begin();
    const s = tx.statement({ rules });
    await insertIn(s.table(t), range(0, 40), 3);
    await s.end();
    expect(path.maintenance.stages).toBeGreaterThan(0);
    // Maintenance runs meanwhile; nothing of the open transaction reaches the table's groups.
    await path.runMaintenance();
    await new Promise((resolve) => setTimeout(resolve, 50));
    // The committed row's memtable froze when the transaction's first row came (it may flush);
    // the transaction's own rows reach no group before the commit.
    const during = tableRoot(path, t) ?? { groupCount: 0, liveRowCount: 0 };
    expect(during.groupCount).toBeLessThanOrEqual(before.groupCount + 1);
    expect(during.liveRowCount).toBeLessThanOrEqual(1);
    expect(await seen(path, t)).toEqual([1000]);
    await (
      await tx.commit(null)
    ).durable;
    await path.quiesce();
    expect(await seen(path, t)).toEqual([...range(0, 40), 1000]);
    expect(tableRoot(path, t)?.staged).toEqual([]);
    await harness.space.audit();
    await harness.close();
  });

  it("a rollback drops the staging; its extents are retired", async () => {
    const harness = new Harness("strict", "sim", 8);
    const path = await harness.open();
    const t = await createTable(path);
    const tx = await path.begin();
    const s = tx.statement({ rules });
    await insertIn(s.table(t), range(0, 40));
    await s.end();
    expect(path.maintenance.stages).toBeGreaterThan(0);
    tx.rollback();
    await path.quiesce();
    expect(path.state().root.tables.find((table) => table.tableId === t)?.staged.length).toBe(1);
    // The next publication leaves the staging out and retires it.
    // Two commits: the first memtable holds the first one whole, so it flushes.
    await autocommit(path, (s2) =>
      s2.table(t).insert(batch(ints(range(100, 8)), ints(range(100, 8))), null),
    );
    await autocommit(path, (s2) => s2.table(t).insert(batch(ints([108]), ints([108])), null));
    await path.quiesce();
    expect(tableRoot(path, t)?.staged).toEqual([]);
    expect(await seen(path, t)).toEqual(range(100, 9));
    await harness.space.audit();
    const reopened = await harness.reopen();
    expect(await seen(reopened, t)).toEqual(range(100, 9));
    await harness.close();
  });

  it("rolling back to a savepoint drops staged rows after it, mid-memtable too", async () => {
    const harness = new Harness("strict", "sim", 8);
    const path = await harness.open();
    const t = await createTable(path);
    const tx = await path.begin();
    const insert = async (from: number, count: number) => {
      const s = tx.statement({ rules });
      await insertIn(s.table(t), range(from, count), 5);
      await s.end();
    };
    await insert(0, 13); // a memtable and a part
    const savepoint = tx.savepoint();
    await insert(13, 30); // the part fills; more memtables stage
    expect(path.maintenance.stages).toBeGreaterThan(0);
    tx.rollbackTo(savepoint);
    await insert(500, 3);
    await (
      await tx.commit(null)
    ).durable;
    const kept = [...range(0, 13), ...range(500, 3)];
    expect(await seen(path, t)).toEqual(kept);
    const view = path.pin();
    expect(await probe(view, t, range(13, 30))).toBe(0);
    expect(await probe(view, t, kept)).toBe(kept.length);
    view.release();
    await path.quiesce();
    expect(await seen(path, t)).toEqual(kept);
    await harness.space.audit();
    const crashed = await harness.crash();
    expect(await seen(crashed, t)).toEqual(kept);
    const again = crashed.pin();
    expect(await probe(again, t, range(13, 30))).toBe(0);
    again.release();
    await harness.space.audit();
    await harness.close();
  });

  it("a crash before the commit frame: the open drops the staging", async () => {
    const harness = new Harness("strict", "sim", 8);
    const path = await harness.open();
    const t = await createTable(path);
    const tx = await path.begin();
    const s = tx.statement({ rules });
    await insertIn(s.table(t), range(0, 40));
    await s.end();
    expect(tableRoot(path, t)?.staged.length).toBe(1);
    const crashed = await harness.crash();
    expect(crashed.maintenance.stagingDropped).toBe(1);
    expect(await seen(crashed, t)).toEqual([]);
    await autocommit(crashed, (s2) =>
      s2.table(t).insert(batch(ints(range(0, 8)), ints(range(0, 8))), null),
    );
    await autocommit(crashed, (s2) => s2.table(t).insert(batch(ints([8]), ints([8])), null));
    await crashed.quiesce();
    expect(tableRoot(crashed, t)?.staged).toEqual([]);
    expect(await seen(crashed, t)).toEqual(range(0, 9));
    await harness.space.audit();
    await harness.close();
  });

  it("a crash after the commit, before the fold: replay makes the staged rows visible", async () => {
    const harness = new Harness("strict", "sim", 8, undefined, { merges: { mode: "off" } });
    const path = await harness.open();
    const t = await createTable(path);
    const tx = await path.begin();
    const s = tx.statement({ rules });
    await insertIn(s.table(t), range(0, 43));
    await s.end();
    expect(path.maintenance.stages).toBeGreaterThan(0);
    const d = tx.statement({ rules });
    // A later statement deletes a staged row and a resident one.
    const view = d.view;
    const located: number[] = [];
    for (const id of [5, 42]) {
      const keys = new KeyBatch(FREE);
      encodeKeyBatch(
        "equality",
        [{ vector: ints([id]), direction: "asc", nulls: "first" }],
        { length: 1, selection: null },
        keys,
      );
      const out = { probes: new Int32Array(2), locators: new Float64Array(2), count: 0 };
      await view.index(t, need(view.catalog.table(t)?.primaryKey)).probe(keys, out);
      keys.close();
      located.push(need(out.locators[0]));
    }
    const { locators } = await import("./test-support.js");
    await d.table(t).delete(batch(locators(located, t)), null);
    await d.end();
    const receipt = await tx.commit(null);
    await receipt.durable;
    const expected = range(0, 43).filter((id) => id !== 5 && id !== 42);
    expect(await seen(path, t)).toEqual(expected);
    const crashed = await harness.crash();
    expect(await seen(crashed, t)).toEqual(expected);
    const reader = crashed.pin();
    expect(await probe(reader, t, range(0, 43))).toBe(41);
    reader.release();
    await crashed.quiesce();
    expect(tableRoot(crashed, t)?.staged).toEqual([]);
    expect(await seen(crashed, t)).toEqual(expected);
    await harness.space.audit();
    const reopened = await harness.reopen();
    expect(await seen(reopened, t)).toEqual(expected);
    await harness.close();
  });
});
