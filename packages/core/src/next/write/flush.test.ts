/**
 * Flush and publish: memtables become column groups and index runs in a durable root; readers that
 * pinned the memtable are re-pointed to the published group (ruling R16.3); deletions fold into
 * bitmaps; recovery reopens groups, runs and the log tail.
 */
import { describe, expect, it } from "vitest";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { MEMTABLE_ROWS } from "./memtable.js";
import { MEMORY_CHUNK_ROWS } from "./columns.js";
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
  TEXT,
  texts,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };

async function load(path: WritePathImpl, t: number, from: number, count: number): Promise<void> {
  for (let start = from; start < from + count; start += MEMORY_CHUNK_ROWS) {
    const n = Math.min(MEMORY_CHUNK_ROWS, from + count - start);
    const ids = Array.from({ length: n }, (_, k) => start + k);
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints(ids), texts(ids.map((id) => `v${String(id)}`))), null),
    );
  }
}

async function probe(path: WritePathImpl, t: number, keys: readonly number[]): Promise<number> {
  const view = path.pin();
  try {
    const def = path.catalog.table(t);
    const batchKeys = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: ints(keys), direction: "asc", nulls: "first" }],
      { length: keys.length, selection: null },
      batchKeys,
    );
    const out = {
      probes: new Int32Array(keys.length),
      locators: new Float64Array(keys.length),
      count: 0,
    };
    await view.index(t, def?.primaryKey ?? 0).probe(batchKeys, out);
    return out.count;
  } finally {
    view.release();
  }
}

async function setup(): Promise<{ harness: Harness; path: WritePathImpl; t: number }> {
  const harness = new Harness();
  const path = await harness.open();
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("big", [column("id", INT, false), column("v", TEXT)]),
        })
      ).id ?? 0;
  });
  return { harness, path, t };
}

describe("flush and publish", () => {
  it("finishes a flush decision while the next memtable holds only pending rows (seq past 2^52)", async () => {
    // Found by the generated differential (seed 4242 step 126): minSeq binary-searched the seq
    // space up to 2^53, and with a pending seq (PENDING_BASE + statement) the midpoint of two safe
    // integers rounded up to `hi`, so the search never ended, synchronously, inside #nextFlush.
    const harness = new Harness("strict", "sim", 16);
    const path = await harness.open();
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("p", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints(Array.from({ length: 10 }, (_, k) => k))), null),
    );
    const tx = await path.begin();
    for (let k = 0; k < 2; k++) await tx.statement({ rules }).end();
    // Statement 3: the batch does not fit, so the committed memtable freezes and a flush is
    // scheduled while the new memtable holds only this statement's pending rows.
    const third = tx.statement({ rules });
    await third.table(t).insert(batch(ints(Array.from({ length: 10 }, (_, k) => 100 + k))), null);
    await third.end();
    await (
      await tx.commit(null)
    ).durable;
    await path.quiesce();
    const reopened = await harness.reopen();
    const view = reopened.pin();
    expect((await rowsOf(view, t)).length).toBe(20);
    view.release();
    await harness.close();
  });

  it("publishes full memtables as groups and runs; probes and scans read them; a reopen keeps them", async () => {
    const { harness, path, t } = await setup();
    const total = MEMTABLE_ROWS * 2 + 1000;
    await load(path, t, 0, total);
    await path.quiesce();
    await harness.space.audit();
    expect(path.publications).toBe(2);
    const view = path.pin();
    const table = view.table(t, { budget: FREE });
    expect(table.segments.map((segment) => segment.kind)).toEqual(["group", "group", "memory"]);
    expect(table.liveRowCount).toBe(total);
    view.release();
    expect(await probe(path, t, [0, 65_535, 65_536, 131_071, total - 1, total])).toBe(5);
    const reopened = await harness.reopen();
    expect(await probe(reopened, t, [0, 70_000, total - 1])).toBe(3);
    const rows = await rowsOf(reopened.pin(), t);
    expect(rows.length).toBe(total);
    expect(rows[70_000]).toEqual([70_000, "v70000"]);
    await harness.close();
  });

  it("re-points a reader that pinned a memtable before its flush published", async () => {
    const { harness, path, t } = await setup();
    await load(path, t, 0, MEMTABLE_ROWS);
    const early = path.pin();
    const before = early.table(t, { budget: FREE });
    expect(before.segments.at(-1)?.kind).toBe("memory");
    await load(path, t, MEMTABLE_ROWS, 10); // freezes the full memtable
    await path.quiesce();
    await harness.space.audit();
    expect(path.publications).toBe(1);
    expect(path.stats().repointPins).toBe(1);
    // The early reader still sees its memtable segment, now read from the published group.
    const rows = await rowsOf(early, t);
    expect(rows.length).toBe(MEMTABLE_ROWS);
    expect(rows[12_345]).toEqual([12_345, "v12345"]);
    const keys = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: ints([42]), direction: "asc", nulls: "first" }],
      { length: 1, selection: null },
      keys,
    );
    const out = { probes: new Int32Array(1), locators: new Float64Array(1), count: 0 };
    await early.index(t, path.catalog.table(t)?.primaryKey ?? 0).probe(keys, out);
    expect(out.count).toBe(1);
    early.release();
    expect(path.stats().repointPins).toBe(0);
    await harness.close();
  });

  it("folds deletions of published rows into bitmaps at the next publish; a reopen keeps them", async () => {
    const { harness, path, t } = await setup();
    await load(path, t, 0, MEMTABLE_ROWS + 10);
    await path.quiesce();
    await harness.space.audit();
    const view = path.pin();
    const def = path.catalog.table(t);
    const keys = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: ints([3, 4]), direction: "asc", nulls: "first" }],
      { length: 2, selection: null },
      keys,
    );
    const out = { probes: new Int32Array(2), locators: new Float64Array(2), count: 0 };
    await view.index(t, def?.primaryKey ?? 0).probe(keys, out);
    view.release();
    await autocommit(path, (s) =>
      s.table(t).delete(batch(locators(Array.from(out.locators), t)), null),
    );
    expect(await probe(path, t, [3, 4, 5])).toBe(1);
    await load(path, t, MEMTABLE_ROWS + 10, MEMTABLE_ROWS); // second flush folds the deletions
    await path.quiesce();
    await harness.space.audit();
    expect(path.publications).toBe(2);
    // Folded deletions count once: the bitmap's, not the delta's again.
    const folded = path.pin();
    const table = folded.table(t, { budget: FREE });
    expect(table.segments[0]?.liveRowCount).toBe(MEMTABLE_ROWS - 2);
    expect(table.liveRowCount).toBe(2 * MEMTABLE_ROWS + 10 - 2);
    folded.release();
    const reopened = await harness.reopen();
    expect(await probe(reopened, t, [3, 4, 5])).toBe(1);
    const rows = await rowsOf(reopened.pin(), t);
    expect(rows.length).toBe(2 * MEMTABLE_ROWS + 10 - 2);
    await harness.close();
  });

  it("keeps rows, memtables and deletions that arrive while a flush is publishing", async () => {
    const { harness, path, t } = await setup();
    // Hold the next publish open until the test releases it.
    const space = (
      path as unknown as { shared: { space: { publish: (...args: unknown[]) => Promise<number> } } }
    ).shared.space;
    const publish = space.publish.bind(space);
    let open!: () => void;
    const gate = new Promise<void>((resolve) => {
      open = resolve;
    });
    let held = false;
    space.publish = async (...args: unknown[]) => {
      if (!held) {
        held = true;
        await gate;
      }
      return publish(...args);
    };
    await load(path, t, 0, MEMTABLE_ROWS + 10); // freezes the first memtable; its flush now waits
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(held).toBe(true);
    // While the publish is held: a second memtable fills and freezes, and rows are deleted.
    await load(path, t, MEMTABLE_ROWS + 10, MEMTABLE_ROWS);
    const doomed = [5, MEMTABLE_ROWS + 20];
    const view = path.pin();
    const keys = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: ints(doomed), direction: "asc", nulls: "first" }],
      { length: 2, selection: null },
      keys,
    );
    const out = { probes: new Int32Array(2), locators: new Float64Array(2), count: 0 };
    await view.index(t, path.catalog.table(t)?.primaryKey ?? 0).probe(keys, out);
    view.release();
    await autocommit(path, (s) =>
      s.table(t).delete(batch(locators(Array.from(out.locators), t)), null),
    );
    open();
    await path.quiesce();
    await harness.space.audit();
    const total = 2 * MEMTABLE_ROWS + 10 - doomed.length;
    expect((await rowsOf(path.pin(), t)).length).toBe(total);
    expect(await probe(path, t, [...doomed, 6, MEMTABLE_ROWS + 21])).toBe(2);
    const reopened = await harness.reopen();
    expect((await rowsOf(reopened.pin(), t)).length).toBe(total);
    expect(await probe(reopened, t, doomed)).toBe(0);
    await harness.close();
  });

  it("keeps a deletion committed while a later flush publishes (folded positions are pruned, it is not)", async () => {
    const { harness, path, t } = await setup();
    await load(path, t, 0, MEMTABLE_ROWS + 10); // the first group publishes
    await path.quiesce();
    expect(path.publications).toBe(1);
    const space = (
      path as unknown as { shared: { space: { publish: (...args: unknown[]) => Promise<number> } } }
    ).shared.space;
    const publish = space.publish.bind(space);
    let open!: () => void;
    const gate = new Promise<void>((resolve) => {
      open = resolve;
    });
    let held = false;
    space.publish = async (...args: unknown[]) => {
      if (!held) {
        held = true;
        await gate;
      }
      return publish(...args);
    };
    await load(path, t, MEMTABLE_ROWS + 10, MEMTABLE_ROWS); // freezes the next; its flush waits
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(held).toBe(true);
    // A row of the published group is deleted after the held flush took its bound.
    const view = path.pin();
    const keys = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: ints([5]), direction: "asc", nulls: "first" }],
      { length: 1, selection: null },
      keys,
    );
    const out = { probes: new Int32Array(1), locators: new Float64Array(1), count: 0 };
    await view.index(t, path.catalog.table(t)?.primaryKey ?? 0).probe(keys, out);
    view.release();
    await autocommit(path, (s) =>
      s.table(t).delete(batch(locators(Array.from(out.locators), t)), null),
    );
    open();
    await path.quiesce();
    expect(path.publications).toBe(2);
    // The publish pruned what it folded; the later deletion stays (before any reopen).
    expect(await probe(path, t, [5, 6])).toBe(1);
    const rows = await rowsOf(path.pin(), t);
    expect(rows.length).toBe(2 * MEMTABLE_ROWS + 10 - 1);
    await harness.close();
  });

  it("keeps folded deletions for a reader older than the publish that folded them", async () => {
    const { harness, path, t } = await setup();
    await load(path, t, 0, MEMTABLE_ROWS + 10);
    await path.quiesce();
    const view = path.pin();
    const keys = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: ints([5]), direction: "asc", nulls: "first" }],
      { length: 1, selection: null },
      keys,
    );
    const out = { probes: new Int32Array(1), locators: new Float64Array(1), count: 0 };
    await view.index(t, path.catalog.table(t)?.primaryKey ?? 0).probe(keys, out);
    view.release();
    await autocommit(path, (s) =>
      s.table(t).delete(batch(locators(Array.from(out.locators), t)), null),
    );
    // A reader from before the fold: its group has the old bitmap, without row 5.
    const old = path.pin();
    await load(path, t, MEMTABLE_ROWS + 10, MEMTABLE_ROWS); // the next flush folds the deletion
    await path.quiesce();
    expect(path.publications).toBe(2);
    expect((await rowsOf(old, t)).some((row) => row[0] === 5)).toBe(false);
    old.release();
    expect(await probe(path, t, [5])).toBe(0);
    await harness.close();
  });

  it("retires every extent of a dropped table at the next publish", async () => {
    const { harness, path, t } = await setup();
    await load(path, t, 0, MEMTABLE_ROWS + 10);
    await path.quiesce();
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: t }));
    let other = 0;
    await autocommit(path, async (s) => {
      other =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("other", [column("id", INT, false), column("v", TEXT)]),
          })
        ).id ?? 0;
    });
    await load(path, other, 0, MEMTABLE_ROWS + 10); // its flush publishes the root without `t`
    await path.quiesce();
    await harness.space.audit();
    expect(harness.space.stats.waitingBytes).toBeGreaterThan(0);
    const reopened = await harness.reopen();
    expect(reopened.catalog.table(t)).toBeUndefined();
    expect(await probe(reopened, other, [0, MEMTABLE_ROWS + 9])).toBe(2);
    await harness.space.audit();
    await harness.close();
  });

  it("recovers after a crash without a clean close", async () => {
    const { harness, path, t } = await setup();
    await load(path, t, 0, MEMTABLE_ROWS + 100);
    const recovered = await harness.crash();
    expect(await probe(recovered, t, [0, MEMTABLE_ROWS + 99])).toBe(2);
    expect((await rowsOf(recovered.pin(), t)).length).toBe(MEMTABLE_ROWS + 100);
    await harness.close();
  });
});
