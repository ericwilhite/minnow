/**
 * Review B3-p2b-3 C3: a DROP INDEX leaves every root published after it, whichever table's
 * publication carries it (another table's flush, a log recycle), and its runs are retired there.
 * An older reader pinned before the drop still reads the index; once it closes and the space
 * advances past the retirement, the runs' extents are free.
 */
import { describe, expect, it } from "vitest";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { reachableExtents } from "../format/reachable.js";
import type { RunRef } from "../format/manifest.js";
import { need } from "./bytes.js";
import type { WritePathImpl } from "./engine.js";
import { autocommit, batch, column, Harness, INT, ints, tableInput } from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };

async function setUp(harness: Harness) {
  const path = await harness.open();
  let t = 0;
  let u = 0;
  let indexId = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false), column("v", INT)]),
        })
      ).id ?? 0;
    u =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("u", [column("id", INT, false)]),
        })
      ).id ?? 0;
  });
  await autocommit(path, async (s) => {
    indexId =
      (
        await s.changeCatalog({
          kind: "createIndex",
          index: {
            tableId: t,
            name: "t_v",
            columnIds: [need(path.catalog.table(t)?.columns[1]).id],
            unique: false,
            role: "secondary",
          },
        })
      ).id ?? 0;
  });
  for (const ids of [[1, 2, 3, 4], [5]])
    await autocommit(path, (s) => s.table(t).insert(batch(ints(ids), ints(ids)), null));
  await path.quiesce();
  const runs = indexRuns(path, t, indexId);
  expect(runs.length).toBeGreaterThan(0);
  return { path, t, u, indexId, runs };
}

function indexRuns(path: WritePathImpl, t: number, indexId: number): readonly RunRef[] {
  const table = need(path.state().root.tables.find((candidate) => candidate.tableId === t));
  return table.secondaryIndexes.find((index) => index.indexId === indexId)?.runs ?? [];
}

async function reached(harness: Harness): Promise<Set<number>> {
  const out = new Set<number>();
  for await (const item of reachableExtents()(need(harness.space.root), harness.space, FREE))
    out.add(("extent" in item ? item.extent : item).offset);
  return out;
}

async function insertU(path: WritePathImpl, u: number, from: number): Promise<void> {
  for (const ids of [[from, from + 1, from + 2, from + 3], [from + 4]])
    await autocommit(path, (s) => s.table(u).insert(batch(ints(ids)), null));
  await path.quiesce();
}

describe("DROP INDEX published by another publication", () => {
  it("another table's flush publishes the drop and retires the runs (the reviewer's probe)", async () => {
    const harness = new Harness("strict", "memory", 4, undefined, { merges: { mode: "off" } });
    const { path, t, u, indexId, runs } = await setUp(harness);
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropIndex", indexId }));
    const dropLsn = path.appliedLsn;
    await insertU(path, u, 1);
    const root = path.state().root;
    expect(root.globalLsn).toBeGreaterThanOrEqual(dropLsn);
    expect(indexRuns(path, t, indexId)).toEqual([]);
    const live = await reached(harness);
    for (const run of runs) expect(live.has(run.entries.extent.offset)).toBe(false);
    await harness.space.audit();
    await harness.close();
  });

  it("a log recycle publishes the drop too", async () => {
    const harness = new Harness("strict", "memory", 4, undefined, { merges: { mode: "off" } });
    const { path, t, indexId } = await setUp(harness);
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropIndex", indexId }));
    await path.recycleLog();
    expect(indexRuns(path, t, indexId)).toEqual([]);
    await harness.space.audit();
    const reopened = await harness.reopen();
    expect(indexRuns(reopened, t, indexId)).toEqual([]);
    await harness.close();
  });

  it("an older reader keeps reading the dropped index; after it closes the runs are freed", async () => {
    const harness = new Harness("strict", "memory", 4, undefined, { merges: { mode: "off" } });
    const { path, t, u, indexId, runs } = await setUp(harness);
    const old = path.pin();
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropIndex", indexId }));
    await insertU(path, u, 1);
    expect(indexRuns(path, t, indexId)).toEqual([]);
    const live = await reached(harness);
    for (const run of runs) expect(live.has(run.entries.extent.offset)).toBe(false);
    // The old snapshot's catalog still has the index; its pinned root still reaches the runs.
    const keys = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: ints([1, 2, 3, 4, 5]), direction: "asc", nulls: "first" }],
      { length: 5, selection: null },
      keys,
    );
    const out = { probes: new Int32Array(16), locators: new Float64Array(16), count: 0 };
    await old.index(t, indexId).probe(keys, out);
    keys.close();
    expect(out.count).toBe(5);
    old.release();
    // Two more publications carry the retirement past both generations (the space's audit checks
    // every allocated extent against both roots and its retirement ledger).
    await insertU(path, u, 10);
    await insertU(path, u, 20);
    // (Their offsets may hold new blobs now: the space reuses freed extents.)
    await harness.space.audit();
    await harness.close();
  });

  it("a later merge of the table's other index keeps the dropped one out", async () => {
    // Merges by hand: two flushes of t leave two primary-key runs; the drop is published through
    // u; then the primary key merges. Its root is built from t's root, which must be the published
    // one (without the dropped index), or the retired runs would come back.
    const harness = new Harness("strict", "memory", 4, undefined, {
      merges: { mode: "manual", fanIn: 2 },
    });
    const { path, t, u, indexId } = await setUp(harness);
    // A second primary-key run on t.
    for (const ids of [[6, 7, 8], [9]])
      await autocommit(path, (s) => s.table(t).insert(batch(ints(ids), ints(ids)), null));
    await path.quiesce();
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropIndex", indexId }));
    await insertU(path, u, 1);
    expect(indexRuns(path, t, indexId)).toEqual([]);
    await path.runMaintenance();
    expect(path.maintenance.merges).toBeGreaterThan(0);
    expect(indexRuns(path, t, indexId)).toEqual([]);
    await harness.space.audit();
    const reopened = await harness.reopen();
    expect(indexRuns(reopened, t, indexId)).toEqual([]);
    await harness.space.audit();
    await harness.close();
  });
});
