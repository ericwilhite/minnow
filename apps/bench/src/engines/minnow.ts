import { MinnowDatabase } from "@minnowdb/core";
import { IndexedDbBlockStore } from "@minnowdb/core/storage";
import { generateEntityBatch, getScenario } from "../benchmark.js";
import type { DatasetRecord, EngineMaterialization } from "../protocol.js";
import { canonicalizeRow, normalizeRows } from "./shared.js";
import type { EngineDriver, EngineSession, LoadContext } from "./session.js";

const BATCH_ROWS = 50_000;

function storageName(record: DatasetRecord): string {
  return `mdb-dataset-${record.id}`;
}

function databaseOptions(record: DatasetRecord): {
  compression: DatasetRecord["compression"];
  rowsPerBlock: number;
} {
  return {
    compression: record.compression,
    rowsPerBlock: Math.max(1, Math.min(100_000, Math.floor(record.targetBlockBytes / 8.125))),
  };
}

export const minnowDriver: EngineDriver = {
  id: "minnow",

  async loadDataset(context: LoadContext): Promise<EngineMaterialization> {
    const { record } = context;
    const name = storageName(record);
    const started = performance.now();
    const store = await IndexedDbBlockStore.open({ name, durability: record.durability });
    const database = new MinnowDatabase(store, databaseOptions(record));
    let insertMs = 0;
    try {
      const entities = getScenario("commerce").entities;
      for (const entity of entities) {
        await database.createTable({
          name: entity.name,
          ...(entity.primaryKey === undefined ? {} : { uniqueKey: entity.primaryKey }),
          columns: entity.columns.map((column) => ({ name: column.name, type: column.type })),
        });
      }
      let completedRows = 0;
      for (const entity of entities) {
        const entityRows = entity.rows(record.scale);
        for (let start = 0; start < entityRows; start += BATCH_ROWS) {
          context.checkCancelled();
          const rowCount = Math.min(BATCH_ROWS, entityRows - start);
          const columns = generateEntityBatch(entity, start, rowCount, entityRows, record.scale);
          const insertStarted = performance.now();
          await database.insertBatch(entity.name, { columns });
          insertMs += performance.now() - insertStarted;
          completedRows += rowCount;
          context.report(`MinnowDatabase · ${entity.name}`, completedRows);
        }
      }
      const orderRows =
        entities.find((entity) => entity.name === "orders")?.rows(record.scale) ?? 0;
      const counted = await database.query("SELECT COUNT(*) AS row_count FROM orders");
      if (counted.rows[0]?.row_count !== orderRows) {
        throw new Error(
          `MinnowDatabase verification failed: expected ${String(orderRows)} orders, found ${String(counted.rows[0]?.row_count)}`,
        );
      }
      return {
        engine: "minnow",
        status: "ready",
        storageName: name,
        version: "workspace",
        persistence: "IndexedDB · immutable compressed column blocks",
        storedBytes: await store.getLogicalStorageBytes(),
        buildMs: performance.now() - started,
        insertMs,
      };
    } finally {
      store.close();
    }
  },

  async openSession(record: DatasetRecord): Promise<EngineSession> {
    const store = await IndexedDbBlockStore.open({
      name: storageName(record),
      durability: record.durability,
    });
    const database = new MinnowDatabase(store, databaseOptions(record));
    return {
      engine: "minnow",
      async prepare(sql) {
        // Prepare compiles only; every execute() is a fresh statement over current data.
        const plan = await database.explain(sql);
        return {
          plan,
          // memoize: false measures execution, not the probe-validated result memo. The suite
          // replays one statement over unchanging data, so the default (memo on) would answer
          // every sample from cache — a constant-time lookup against the other engines' real
          // execution, which is not a comparison. Applications keep the default and get the
          // cache; this column is what the engine costs when it actually runs the query.
          execute: async () =>
            normalizeRows((await database.query(sql, { memoize: false })).rows).map(
              canonicalizeRow,
            ),
          // The same statement with the memo left on, reported beside the execution number
          // rather than instead of it.
          executeCached: async () =>
            normalizeRows((await database.query(sql)).rows).map(canonicalizeRow),
          close: () => undefined,
        };
      },
      close() {
        store.close();
        return Promise.resolve();
      },
    };
  },

  async deleteDataset(materialization: EngineMaterialization): Promise<void> {
    await new Promise<void>((resolve) => {
      const request = indexedDB.deleteDatabase(materialization.storageName);
      request.onsuccess = () => {
        resolve();
      };
      request.onerror = () => {
        resolve();
      };
      request.onblocked = () => {
        resolve();
      };
    });
  },
};
