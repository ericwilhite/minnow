/**
 * Review B3-p2a-1, findings 1–6: each reviewer probe as a test (rulings 1–6).
 */
import { describe, expect, it } from "vitest";
import { ForeignKeyViolationError } from "../access/modify.js";
import type { Snapshot } from "../access/snapshot.js";
import { Governor } from "../memory/governor.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { MEMORY_CHUNK_ROWS } from "./columns.js";
import type { WritePathImpl } from "./engine.js";
import { MEMTABLE_ROWS } from "./memtable.js";
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

async function create(path: WritePathImpl, name: string, extra = {}): Promise<number> {
  let id = 0;
  await autocommit(path, async (s) => {
    id =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput(name, [column("id", INT, false), column("v", INT)], extra),
        })
      ).id ?? 0;
  });
  return id;
}

async function load(path: WritePathImpl, t: number, from: number, to: number): Promise<void> {
  for (let k = from; k < to; k += MEMORY_CHUNK_ROWS) {
    const ids = Array.from({ length: Math.min(MEMORY_CHUNK_ROWS, to - k) }, (_, j) => k + j);
    await autocommit(path, (s) => s.table(t).insert(batch(ints(ids), ints(ids)), null));
  }
}

async function probe(view: Snapshot, t: number, index: number, keys: number[]): Promise<number[]> {
  const encoded = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints(keys), direction: "asc", nulls: "first" }],
    { length: keys.length, selection: null },
    encoded,
  );
  const out = { probes: new Int32Array(16), locators: new Float64Array(16), count: 0 };
  await view.index(t, index).probe(encoded, out);
  encoded.close();
  return Array.from(out.probes.subarray(0, out.count));
}

async function createIndex(path: WritePathImpl, t: number): Promise<number> {
  let id = 0;
  const v = path.catalog.table(t)?.columns[1]?.id ?? 0;
  await autocommit(path, async (s) => {
    id =
      (
        await s.changeCatalog({
          kind: "createIndex",
          index: { tableId: t, name: "v_idx", columnIds: [v], unique: false, role: "secondary" },
        })
      ).id ?? 0;
  });
  return id;
}

describe("review B3-p2a-1", () => {
  it("1: a checkpoint of another table never covers an unpublished CREATE INDEX build", async () => {
    const harness = new Harness();
    let path = await harness.open();
    const t = await create(path, "t");
    await load(path, t, 0, MEMTABLE_ROWS + 1);
    await path.quiesce();
    const index = await createIndex(path, t);
    const u = await create(path, "u");
    await load(path, u, 0, MEMTABLE_ROWS + 1);
    await path.quiesce();
    path = await harness.reopen();
    const view = path.pin();
    expect(await probe(view, t, index, [0, 70_000])).toEqual([0]);
    view.release();
    await harness.close();
  });

  it("2: a snapshot older than a publication keeps its own index answers", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await create(path, "t");
    await load(path, t, 0, 1);
    const old = path.pin();
    const primary = path.catalog.table(t)?.primaryKey ?? 0;
    await load(path, t, 1, MEMTABLE_ROWS);
    await autocommit(path, (s) => s.table(t).delete(batch(locators([0], t)), null));
    await load(path, t, MEMTABLE_ROWS, MEMTABLE_ROWS + 1);
    await path.quiesce();
    expect(path.publications).toBe(1);
    expect(await probe(old, t, primary, [0, 1])).toEqual([0]);
    expect(await rowsOf(old, t)).toEqual([[0, 0]]);
    old.release();
    const now = path.pin();
    expect(await probe(now, t, primary, [0, 1])).toEqual([1]);
    now.release();
    await harness.close();
  });

  it("3: replay applies a DELETE before a later CREATE INDEX of the same transaction", async () => {
    const harness = new Harness();
    let path = await harness.open();
    const t = await create(path, "t");
    await load(path, t, 0, 2);
    const v = path.catalog.table(t)?.columns[1]?.id ?? 0;
    const tx = await path.begin();
    let s = tx.statement({ rules });
    await s.table(t).delete(batch(locators([0], t)), null);
    await s.end();
    s = tx.statement({ rules });
    const index =
      (
        await s.changeCatalog({
          kind: "createIndex",
          index: { tableId: t, name: "v_idx", columnIds: [v], unique: false, role: "secondary" },
        })
      ).id ?? 0;
    await s.end();
    await (
      await tx.commit(null)
    ).durable;
    path = await harness.reopen();
    const view = path.pin();
    expect(await probe(view, t, index, [0, 1])).toEqual([1]);
    view.release();
    await harness.close();
  });

  it("4: a composite supporting index answers a foreign key by prefix", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await create(path, "t");
    await load(path, t, 0, 1);
    let child = 0;
    await autocommit(path, async (s) => {
      child =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput(
              "child",
              [column("parent", INT, false), column("other", INT, false)],
              {
                primaryKey: [0, 1],
                foreignKeys: [
                  {
                    name: "fk",
                    columns: [0],
                    parentTableId: t,
                    parentIndexId: "primary",
                    onDelete: "restrict",
                    enforced: true,
                  },
                ],
              },
            ),
          })
        ).id ?? 0;
    });
    await autocommit(path, (s) => s.table(child).insert(batch(ints([0]), ints([7])), null));
    await expect(
      autocommit(path, (s) => s.table(t).delete(batch(locators([0], t)), null)),
    ).rejects.toThrow(ForeignKeyViolationError);
    await harness.close();
  });

  it("5: a strict reader keeps a dropped table until the drop is durable", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await create(path, "t");
    await load(path, t, 0, 1);
    const tx = await path.begin();
    const s = tx.statement({ rules });
    await s.changeCatalog({ kind: "dropTable", tableId: t });
    await s.end();
    const receipt = await tx.commit(null);
    const view = path.pin();
    expect(view.bound).toBeLessThan(receipt.lsn);
    expect(await rowsOf(view, t)).toEqual([[0, 0]]);
    view.release();
    await receipt.durable;
    await Promise.resolve();
    const after = path.pin();
    expect(() => after.table(t, { budget: FREE })).toThrow();
    after.release();
    await harness.close();
  });

  it("6: memtable memory is charged, bounded across tables, and borrowed chunks counted", async () => {
    const harness = new Harness("strict", "memory");
    // A small cap: the memtable cap (a quarter, at least 16 MiB) binds before the governor's.
    Object.assign(harness, { governor: new Governor(64 * 1024 * 1024) });
    const path = await harness.open();
    const before = harness.governor.usedBytes;
    const tables: number[] = [];
    for (let k = 0; k < 6; k++) tables.push(await create(path, `t${String(k)}`));
    for (const t of tables) await load(path, t, 0, 60_000);
    await path.quiesce();
    expect(harness.governor.usedBytes).toBeGreaterThan(before);
    expect(path.stats().memtableBytes).toBeLessThanOrEqual(16 * 1024 * 1024 + 8 * 1024 * 1024);
    expect(path.publications).toBeGreaterThan(0);
    // A reader borrowing a memtable chunk across its publication keeps that chunk charged.
    const t = await create(path, "borrow");
    await load(path, t, 0, MEMTABLE_ROWS);
    const old = path.pin();
    const table = old.table(t, { budget: FREE });
    const segment = table.segments.at(-1);
    if (segment?.kind !== "memory") throw new Error("expected a memory segment");
    await segment.chunk(table.columns[0]?.id ?? 0, 0, {
      flat: ints([0]),
      codes: new Int32Array(0),
      varlen: null,
    });
    await load(path, t, MEMTABLE_ROWS, MEMTABLE_ROWS + 1);
    await path.quiesce();
    expect(path.stats().borrowedChunkBytes).toBeGreaterThan(0);
    old.release();
    expect(path.stats().borrowedChunkBytes).toBe(0);
    await harness.close();
  });
});
