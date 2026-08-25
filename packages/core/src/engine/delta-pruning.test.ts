/**
 * Zone-map pruning must survive a mutation history.
 *
 * A keyed point lookup reads the handful of blocks its predicate cannot eliminate. That held on
 * a freshly loaded table and stopped holding the moment anything in the table was updated: an
 * update segment stores the key column — that is how the replay addresses the rows it patches —
 * and the pruning gate read the key's presence as "an update may have moved a value into this
 * predicate's range". For `WHERE id = ?` the predicate *is* on the key, so pruning switched off
 * and stayed off until compaction folded the delta away. Every later lookup scanned every row.
 *
 * Updating a unique key is refused outright, so that presence was never a hazard. These tests
 * pin the distinction by counting the blocks a lookup actually reads, which is the contract the
 * timing followed from: a lookup that reads a bounded number of blocks cannot be scanning a
 * table that grows, however fast the machine is.
 *
 * Scope: this covers the streamed scan, which is what a small block size selects. The same
 * conflation existed on the materialized overlay, and that one is what the *write* path pays --
 * a SQL UPDATE resolves the rows it will touch by reading them back first. It is not tested here
 * because at the default block size a whole row group is one block, so block counts move by one
 * whether the scan prunes or not and the real difference is in decode work no counter here can
 * see. The performance gate pins it instead: `point-update`, `range-update` and
 * `filtered-update` in scripts/perf-gate.mts fail loudly (3.7x over threshold, measured) if that
 * pruning regresses.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { allSegmentRecords } from "./storage-test-helpers.js";

/** Counts the blocks a read pulls, which is what pruning changes and wall-clock only reflects. */
class CountingStore extends MemoryBlockStore {
  reads = 0;
  bytes = 0;
  blockIds: string[] = [];

  override async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    this.reads += ids.length;
    this.blockIds.push(...ids);
    const blocks = await super.getBlocks(ids);
    for (const block of blocks) if (block !== undefined) this.bytes += block.byteLength;
    return blocks;
  }
}

const ROWS = 40_000;
/** Small enough that the table spans many blocks, so "pruned" and "not pruned" differ loudly. */
const ROWS_PER_BLOCK = 2_048;

async function seeded(): Promise<{ database: MinnowDatabase; store: CountingStore }> {
  const store = new CountingStore();
  const database = new MinnowDatabase(store, { rowsPerBlock: ROWS_PER_BLOCK, autoCompact: false });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "amount", type: "number" },
      { name: "label", type: "string" },
    ],
  });
  await database.insertBatch(
    "items",
    Array.from({ length: ROWS }, (_, index) => ({
      id: index + 1,
      amount: index % 500,
      label: `row-${String(index % 7)}`,
    })),
  );
  return { database, store };
}

/**
 * Blocks read while running `sql`, counted cold.
 *
 * Decoded blocks are cached, so running the same query twice and measuring the second run counts
 * cache hits rather than pruning and reports a healthy number either way — which is exactly how
 * an earlier version of this test passed with the fix reverted. Each measurement gets its own
 * database, and the query runs once.
 */
async function coldReads(
  build: (database: MinnowDatabase) => Promise<void>,
  sql: string,
  params: readonly number[],
): Promise<{ reads: number; bytes: number }> {
  const { database, store } = await seeded();
  await build(database);
  store.reads = 0;
  store.bytes = 0;
  await database.query(sql, { params: [...params], memoize: false });
  return { reads: store.reads, bytes: store.bytes };
}

describe("zone-map pruning across a mutation history", () => {
  it("keeps a keyed lookup pruned after a row is updated", async () => {
    const lookup = "SELECT id, amount FROM items WHERE id = ?";
    const clean = await coldReads(async () => undefined, lookup, [31_337]);
    expect(clean.reads).toBeGreaterThan(0);
    const afterUpdate = await coldReads(
      (database) =>
        database
          .execute("UPDATE items SET amount = ? WHERE id = ?", [999, 7])
          .then(() => undefined),
      lookup,
      [31_337],
    );

    // The replay adds a small, bounded overhead: the update segment's own blocks. What it must
    // not add is the rest of the table. Measured on this fixture, a pruned lookup pulls 23 blocks
    // and 50 KB against the clean 21 and 50 KB; with pruning disabled it pulls 61 and 115 KB.
    // Halfway between is the line, stated relative to the clean scan so the fixture can grow.
    expect(afterUpdate.reads).toBeLessThan(clean.reads * 1.5);
    expect(afterUpdate.bytes).toBeLessThan(clean.bytes * 1.5);
  });

  it("still refuses to prune on a column an update rewrites", async () => {
    const { database, store } = await seeded();
    await database.execute("UPDATE items SET amount = ? WHERE id = ?", [4_242, 9]);
    store.reads = 0;

    // `amount` is the column the update rewrote, so the base blocks' zone maps cannot be trusted
    // for it: the row group holding id 9 records amount < 500, and the update moved it past that.
    // Pruning here would drop the row, so the scan has to read broadly -- and find it.
    const result = await database.query("SELECT id FROM items WHERE amount = ?", {
      params: [4_242],
      memoize: false,
    });
    expect(result.rows).toEqual([{ id: 9 }]);
    expect(store.reads).toBeGreaterThan(ROWS / ROWS_PER_BLOCK);
  });

  it("answers the same rows pruned or not, across a mixed history", async () => {
    const { database } = await seeded();
    await database.execute("UPDATE items SET amount = ? WHERE id = ?", [1, 100]);
    await database.execute("DELETE FROM items WHERE id = ?", [200]);
    await database.execute("UPDATE items SET label = ? WHERE id = ?", ["patched", 300]);

    // Keyed lookups take the pruned path; the aggregate does not. Both must agree with the
    // history that produced them.
    expect(
      (await database.query("SELECT amount FROM items WHERE id = ?", { params: [100] })).rows,
    ).toEqual([{ amount: 1 }]);
    expect(
      (await database.query("SELECT id FROM items WHERE id = ?", { params: [200] })).rows,
    ).toEqual([]);
    expect(
      (await database.query("SELECT label FROM items WHERE id = ?", { params: [300] })).rows,
    ).toEqual([{ label: "patched" }]);
    expect(
      (await database.query("SELECT COUNT(*) AS n FROM items", { memoize: false })).rows,
    ).toEqual([{ n: ROWS - 1 }]);
  });

  it("does not replay patched columns from deltas whose immutable keys cannot match", async () => {
    const { database, store } = await seeded();
    for (let id = 1; id <= 64; id += 1) {
      await database.execute("UPDATE items SET amount = amount + 1 WHERE id = ?", [id]);
    }
    const table = await store.getTableByName("items");
    if (table === undefined) throw new Error("items table is missing");
    const amountColumn = table.columns.find((column) => column.name === "amount");
    if (amountColumn === undefined) throw new Error("amount column is missing");
    const irrelevantPatchBlockIds = new Set(
      (await allSegmentRecords(store, table.id))
        .filter((segment) => segment.kind === "update")
        .flatMap((segment) => segment.columnBlockIds[amountColumn.id] ?? []),
    );

    // A second database has a cold decoded-block cache but sees the same committed history.
    // It must inspect delta key headers, while loading none of their irrelevant patch payloads.
    const reader = new MinnowDatabase(store, {
      rowsPerBlock: ROWS_PER_BLOCK,
      autoCompact: false,
    });
    store.reads = 0;
    store.bytes = 0;
    store.blockIds = [];
    expect(
      (
        await reader.query("SELECT amount FROM items WHERE id = ?", {
          params: [31_337],
          memoize: false,
        })
      ).rows,
    ).toEqual([{ amount: 336 }]);
    expect(store.blockIds.some((id) => irrelevantPatchBlockIds.has(id))).toBe(false);
  });
});
