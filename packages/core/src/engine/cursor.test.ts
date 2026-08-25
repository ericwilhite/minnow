import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { allSegmentRecords } from "./storage-test-helpers.js";

class CountingCursorStore extends MemoryBlockStore {
  blockIdsRead: string[] = [];

  override async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    this.blockIdsRead.push(...ids);
    return super.getBlocks(ids);
  }
}

async function populatedDatabase(rowCount = 5_000): Promise<{
  database: MinnowDatabase;
  store: CountingCursorStore;
}> {
  const store = new CountingCursorStore();
  const writer = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 32 });
  await writer.createTable({
    name: "events",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number", integer: true },
      { name: "label", type: "string" },
    ],
  });
  await writer.insertBatch("events", {
    columns: {
      id: Array.from({ length: rowCount }, (_, index) => index),
      label: Array.from({ length: rowCount }, (_, index) => `event-${String(index)}`),
    },
  });
  // A second database starts with a cold buffer pool over the same durable store.
  return { database: new MinnowDatabase(store), store };
}

describe("queryCursor", () => {
  it("streams cursor-safe scans in bounded pages with global OFFSET/LIMIT semantics", async () => {
    const { database } = await populatedDatabase();
    const batches = database.queryCursor(
      "SELECT id, label FROM events WHERE id >= 100 LIMIT 301 OFFSET 7",
      { batchRows: 64, memoize: false },
    );
    const pages = [];
    for await (const batch of batches) pages.push(batch);

    expect(pages.length).toBeGreaterThan(1);
    expect(pages.every(({ rows }) => rows.length > 0 && rows.length <= 64)).toBe(true);
    expect(pages.flatMap(({ rows }) => rows).map(({ id }) => id)).toEqual(
      Array.from({ length: 301 }, (_, index) => index + 107),
    );
    await database.close();
  });

  it("keeps scan reads and modeled result memory bounded by cursor backpressure", async () => {
    const { database, store } = await populatedDatabase(8_192);
    const allBlocks = (await allSegmentRecords(store)).flatMap((segment) =>
      Object.values(segment.columnBlockIds).flat(),
    ).length;
    let cursorPeak = 0;
    const cursor = database.queryCursor("SELECT id FROM events", {
      batchRows: 32,
      memoize: false,
      onStats: ({ peakMemoryBytes }) => {
        cursorPeak = peakMemoryBytes;
      },
    });
    const first = await cursor.next();
    expect(first.done).toBe(false);
    expect(first.value?.rows).toHaveLength(32);
    // One pending page may be prepared, but the cold cursor must not read the whole table.
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(store.blockIdsRead.length).toBeLessThan(allBlocks / 4);

    let rowCount = first.value?.rows.length ?? 0;
    for await (const batch of cursor) rowCount += batch.rows.length;
    expect(rowCount).toBe(8_192);

    let materializedPeak = 0;
    await database.query("SELECT id FROM events", {
      memoize: false,
      onStats: ({ peakMemoryBytes }) => {
        materializedPeak = peakMemoryBytes;
      },
    });
    expect(cursorPeak).toBeGreaterThan(0);
    expect(cursorPeak).toBeLessThan(materializedPeak);
    await database.close();
  });

  it("pages blocking plans correctly and preserves an empty result's columns", async () => {
    const { database } = await populatedDatabase(19);
    const ordered = [];
    for await (const batch of database.queryCursor(
      "SELECT id FROM events ORDER BY id DESC LIMIT 7",
      { batchRows: 3 },
    )) {
      ordered.push(batch);
    }
    expect(ordered.map(({ rows }) => rows.length)).toEqual([3, 3, 1]);
    expect(ordered.flatMap(({ rows }) => rows).map(({ id }) => id)).toEqual([
      18, 17, 16, 15, 14, 13, 12,
    ]);

    const empty = [];
    for await (const batch of database.queryCursor("SELECT id FROM events WHERE id < 0")) {
      empty.push(batch);
    }
    expect(empty).toEqual([{ columns: ["id"], rows: [] }]);
    await database.close();
  });

  it("cancels a parked cursor and releases its producer", async () => {
    const { database } = await populatedDatabase();
    const controller = new AbortController();
    const cursor = database.queryCursor("SELECT id FROM events", {
      batchRows: 16,
      signal: controller.signal,
    });
    expect((await cursor.next()).done).toBe(false);
    const reason = new Error("stop cursor");
    controller.abort(reason);
    await expect(cursor.next()).rejects.toBe(reason);
    await database.close();
  });

  it("does not begin execution when its signal is already aborted", async () => {
    const { database, store } = await populatedDatabase();
    store.blockIdsRead = [];
    const controller = new AbortController();
    const reason = new Error("already stopped");
    controller.abort(reason);
    const cursor = database.queryCursor("SELECT id FROM events", {
      signal: controller.signal,
    });
    await expect(cursor.next()).rejects.toBe(reason);
    expect(store.blockIdsRead).toEqual([]);
    await database.close();
  });
});
