/**
 * The folded unique-key base is what rejects a duplicate key, so a fold that loses or invents
 * a token is a correctness bug, not a slow path. These tests drive real folds through the
 * public write API and check the resulting membership against an independent JavaScript set,
 * across the cases where the incremental fold and the full rewrite have to agree: plain
 * growth, removals, a token removed and re-added, a partition count that has to change, and a
 * base written before the token count was recorded.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { IndexedDbBlockStore } from "./indexeddb.js";
import { MinnowDatabase, UniqueConstraintError } from "../engine/database.js";

/**
 * The tail holds 16 chunks, so a fold lands on every 17th commit. Two folds are the minimum
 * that proves anything: the first has no recorded size and rewrites the base in full, and only
 * the second can take the incremental path.
 */
const FOLD_BATCHES = 40;

interface Harness {
  store: IndexedDbBlockStore;
  database: MinnowDatabase;
  indexedDB: IDBFactory;
  name: string;
}

async function open(): Promise<Harness> {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  const store = await IndexedDbBlockStore.open({ name, indexedDB });
  const database = new MinnowDatabase(store, { rowsPerBlock: 512 });
  await database.createTable({
    name: "t",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "v", type: "number" },
    ],
  });
  return { store, database, indexedDB, name };
}

/**
 * Edits one catalog record through a second connection, so a test can present the shape an
 * older writer would have left behind without adding a seam to the store itself.
 */
async function editCatalogRecord(
  harness: Harness,
  key: IDBValidKey,
  edit: (value: Record<string, unknown>) => Record<string, unknown>,
): Promise<void> {
  const connection = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = harness.indexedDB.open(harness.name);
    request.onsuccess = () => {
      resolve(request.result);
    };
    request.onerror = () => {
      reject(request.error ?? new Error("open failed"));
    };
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const transaction = connection.transaction("catalog", "readwrite");
      const catalog = transaction.objectStore("catalog");
      const read = catalog.get(key);
      read.onsuccess = () => {
        const current = read.result as Record<string, unknown> | undefined;
        if (current === undefined) {
          reject(new Error("catalog record is missing"));
          return;
        }
        catalog.put(edit(current), key);
      };
      transaction.oncomplete = () => {
        resolve();
      };
      transaction.onerror = () => {
        reject(transaction.error ?? new Error("edit failed"));
      };
    });
  } finally {
    connection.close();
  }
}

const insert = (database: MinnowDatabase, ids: readonly number[]): Promise<unknown> =>
  database.insertBatch("t", { columns: { id: [...ids], v: ids.map((id) => id * 2) } });

/** Reads membership back the way a write does, one probe per token. */
async function present(
  store: IndexedDbBlockStore,
  database: MinnowDatabase,
  ids: readonly number[],
): Promise<Set<number>> {
  const tables = await store.listTables();
  const tableId = tables.find((table) => table.name === "t")?.id ?? "";
  const tokens = ids.map((id) => `number:${String(id)}`);
  const found = new Set(await store.getExistingUniqueKeys(tableId, tokens));
  void database;
  return new Set(ids.filter((id) => found.has(`number:${String(id)}`)));
}

/** The engine's own view, which must agree with the index. */
async function storedIds(database: MinnowDatabase): Promise<number[]> {
  const result = await database.query("SELECT id FROM t ORDER BY id", { memoize: false });
  return result.rows.map((row) => Number(row.id));
}

/**
 * The size the folded base claims. An incremental fold derives it from the partitions it
 * touched rather than by counting, so a wrong derivation shows up here and nowhere else until
 * the count next decides the partition layout.
 */
async function foldState(
  harness: Harness,
): Promise<{ tokenCount: number | undefined; tailChunks: number }> {
  const tables = await harness.store.listTables();
  const tableId = tables.find((table) => table.name === "t")?.id ?? "";
  let state = { tokenCount: undefined as number | undefined, tailChunks: 0 };
  await editCatalogRecord(harness, ["unique-key-chunk-index", tableId], (current) => {
    state = {
      tokenCount: typeof current.tokenCount === "number" ? current.tokenCount : undefined,
      tailChunks: Array.isArray(current.versions) ? current.versions.length : 0,
    };
    return current;
  });
  return state;
}

describe("unique-key base fold", () => {
  it("keeps every token across repeated folds", async () => {
    const harness = await open();
    const { store, database } = harness;
    const expected = new Set<number>();
    for (let batch = 0; batch < FOLD_BATCHES; batch += 1) {
      const ids = Array.from({ length: 25 }, (_, i) => batch * 25 + i + 1);
      await insert(database, ids);
      for (const id of ids) expected.add(id);
    }
    const all = [...expected];
    expect(await present(store, database, all)).toEqual(expected);
    expect(await storedIds(database)).toEqual([...expected].sort((a, b) => a - b));
    // Absent neighbours must stay absent — a fold that widened a partition would show here.
    expect(await present(store, database, [0, 99_999, 100_000])).toEqual(new Set());
    // The base was written once, by the first fold. Every later tail was small enough to
    // collapse into a single chunk instead, so the base still describes the same 425 tokens
    // and the tail is back to one record. A fold that rewrote the base each time — the cost
    // this avoids — would show a larger count here.
    const state = await foldState(harness);
    expect(state.tokenCount).toBe(17 * 25);
    expect(state.tailChunks).toBeLessThanOrEqual(16);
    store.close();
  });

  it("rewrites the base once the tail outgrows a partition", async () => {
    const harness = await open();
    const { store, database } = harness;
    // Batches this size push the tail past the partition target, so it has to fold for real
    // rather than merge, and the recorded size has to follow.
    const perBatch = 2_000;
    const expected = new Set<number>();
    for (let batch = 0; batch < 34; batch += 1) {
      const ids = Array.from({ length: perBatch }, (_, i) => batch * perBatch + i + 1);
      await insert(database, ids);
      for (const id of ids) expected.add(id);
    }
    const state = await foldState(harness);
    expect(state.tokenCount).toBeGreaterThan(17 * perBatch);
    expect(await present(store, database, [1, 34 * perBatch])).toEqual(new Set([1, 34 * perBatch]));
    expect(await present(store, database, [34 * perBatch + 1])).toEqual(new Set());
    store.close();
  });

  it("drops deleted tokens and keeps the rest", async () => {
    const harness = await open();
    const { store, database } = harness;
    const expected = new Set<number>();
    for (let batch = 0; batch < FOLD_BATCHES; batch += 1) {
      const ids = Array.from({ length: 20 }, (_, i) => batch * 20 + i + 1);
      await insert(database, ids);
      for (const id of ids) expected.add(id);
      // Delete a slice of an earlier batch, so removals land in the same tail as the adds.
      if (batch >= 2) {
        const doomed = Array.from({ length: 5 }, (_, i) => (batch - 2) * 20 + i + 1);
        await database.execute(`DELETE FROM t WHERE id IN (${doomed.join(", ")})`);
        for (const id of doomed) expected.delete(id);
      }
    }
    const probed = Array.from({ length: FOLD_BATCHES * 20 }, (_, i) => i + 1);
    expect(await present(store, database, probed)).toEqual(expected);
    expect(await storedIds(database)).toEqual([...expected].sort((a, b) => a - b));
    // The base only ever holds what a real fold put there, so it cannot exceed live membership.
    const state = await foldState(harness);
    expect(state.tokenCount).toBeDefined();
    expect(state.tokenCount).toBeLessThanOrEqual(expected.size);
    store.close();
  });

  it("ends with the token present when it is removed and re-added", async () => {
    const { store, database } = await open();
    await insert(database, [1, 2, 3]);
    for (let batch = 0; batch < FOLD_BATCHES; batch += 1) {
      await database.execute("DELETE FROM t WHERE id = 2");
      await insert(database, [2]);
      await insert(database, [100 + batch]);
    }
    expect(await present(store, database, [1, 2, 3])).toEqual(new Set([1, 2, 3]));
    // And the reverse order must end absent.
    await insert(database, [500]);
    await database.execute("DELETE FROM t WHERE id = 500");
    for (let batch = 0; batch < FOLD_BATCHES; batch += 1) await insert(database, [200 + batch]);
    expect(await present(store, database, [500])).toEqual(new Set());
    store.close();
  });

  it("still rejects a duplicate key after folding", async () => {
    const { store, database } = await open();
    for (let batch = 0; batch < FOLD_BATCHES; batch += 1) {
      await insert(database, [batch + 1]);
    }
    await expect(insert(database, [3])).rejects.toBeInstanceOf(UniqueConstraintError);
    await expect(insert(database, [FOLD_BATCHES + 1, 3])).rejects.toBeInstanceOf(
      UniqueConstraintError,
    );
    expect(await present(store, database, [3])).toEqual(new Set([3]));
    store.close();
  });

  it("stays correct when the base has to grow past one partition", async () => {
    // The partition count is derived from the token count, so crossing the threshold moves
    // every token to a new partition and the incremental path must hand back to the rewrite.
    const { store, database } = await open();
    const total = 20_000;
    const batchSize = 1_000;
    for (let start = 0; start < total; start += batchSize) {
      await insert(
        database,
        Array.from({ length: batchSize }, (_, i) => start + i + 1),
      );
    }
    const sample = [1, 2, 16_383, 16_384, 16_385, 19_999, 20_000];
    expect(await present(store, database, sample)).toEqual(new Set(sample));
    expect(await present(store, database, [20_001, 30_000])).toEqual(new Set());
    const counted = await database.query("SELECT COUNT(*) AS n FROM t", { memoize: false });
    expect(counted.rows[0]?.n).toBe(total);
    store.close();
  });

  it("folds a base that carries no recorded token count", async () => {
    const harness = await open();
    const { store, database } = harness;
    for (let batch = 0; batch < FOLD_BATCHES; batch += 1) await insert(database, [batch + 1]);

    // Strip the recorded count, the way a base written before it existed would read. The next
    // fold has to notice, rewrite in full, and record the count again.
    const tables = await store.listTables();
    const tableId = tables.find((table) => table.name === "t")?.id ?? "";
    let sawCount = false;
    await editCatalogRecord(harness, ["unique-key-chunk-index", tableId], (current) => {
      sawCount = typeof current.tokenCount === "number";
      const { tokenCount, ...withoutCount } = current;
      void tokenCount;
      return withoutCount;
    });
    expect(sawCount).toBe(true);

    const expected = new Set<number>();
    for (let batch = 0; batch < FOLD_BATCHES; batch += 1) expected.add(batch + 1);
    for (let batch = 0; batch < FOLD_BATCHES; batch += 1) {
      await insert(database, [1_000 + batch]);
      expected.add(1_000 + batch);
    }
    expect(await present(store, database, [...expected])).toEqual(expected);
    expect(await storedIds(database)).toEqual([...expected].sort((a, b) => a - b));
    store.close();
  });
});
