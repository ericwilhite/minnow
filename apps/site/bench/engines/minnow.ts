import { MinnowDatabase, exposeDatabase } from "@minnowdb/core";
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { IndexedDbBlockStore, type BlockStore } from "@minnowdb/core/storage";
import { generateEntityBatch, getScenario } from "../benchmark";
import type { DatasetRecord, EngineId, EngineMaterialization } from "../protocol";
import { canonicalizeRow, createSecondaryIndexes, normalizeRows } from "./shared";
import type {
  EngineDriver,
  EngineSession,
  LiveSession,
  LoadContext,
  WriteSession,
  WriteTableSchema,
} from "./session";

const BATCH_ROWS = 50_000;

/**
 * The client an application holds, wired to this database over a message channel inside the
 * bench worker. Every call crosses the channel exactly as it would cross a worker boundary —
 * RPC frames out, results back in their columnar wire form, rows rebuilt on the receiving
 * side — so what it measures is the client layer, not the engine again. Close it and the
 * channel goes with it; the database and store are the caller's.
 */
export function connectClient(database: MinnowDatabase): {
  client: MinnowDatabaseClient;
  close(): Promise<void>;
} {
  const channel = new MessageChannel();
  exposeDatabase(database, channel.port1);
  channel.port1.start();
  channel.port2.start();
  const client = new MinnowDatabaseClient(channel.port2, { store: { kind: "memory" } });
  return {
    client,
    async close() {
      await client.close();
      channel.port1.close();
      channel.port2.close();
    },
  };
}

/** One name for a dataset's storage whichever store holds it; the namespaces never collide. */
export function datasetStorageName(record: DatasetRecord): string {
  return `mdb-dataset-${record.id}`;
}

/** The store a bench driver opens: the storage contract plus the two extras the bench reads. */
export interface BenchBlockStore extends BlockStore {
  getLogicalStorageBytes(): Promise<number>;
}

/**
 * Both Minnow columns run the identical engine over the identical schema and batches; the only
 * differences are which block store holds the bytes and how a dataset is deleted. One factory
 * keeps the workload byte-for-byte shared so the columns compare storage, not workload drift.
 */
export interface MinnowStorageBackend {
  id: EngineId;
  persistence: string;
  openStore(record: DatasetRecord): Promise<BenchBlockStore>;
  deleteDataset(materialization: EngineMaterialization): Promise<void>;
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

export function createMinnowDriver(backend: MinnowStorageBackend): EngineDriver {
  return {
    id: backend.id,

    async loadDataset(context: LoadContext): Promise<EngineMaterialization> {
      const { record } = context;
      const name = datasetStorageName(record);
      const started = performance.now();
      const store = await backend.openStore(record);
      const database = new MinnowDatabase(store, databaseOptions(record));
      let insertMs = 0;
      let indexMs: number;
      let dataStoredBytes: number;
      let indexStoredBytes: number;
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
        dataStoredBytes = await store.getLogicalStorageBytes();
        indexMs =
          record.secondaryIndexes === "foreign-keys"
            ? await createSecondaryIndexes(entities, (sql) => database.execute(sql))
            : 0;
        indexStoredBytes = Math.max(0, (await store.getLogicalStorageBytes()) - dataStoredBytes);
        const orderRows =
          entities.find((entity) => entity.name === "orders")?.rows(record.scale) ?? 0;
        const counted = await database.query("SELECT COUNT(*) AS row_count FROM orders");
        if (counted.rows[0]?.row_count !== orderRows) {
          throw new Error(
            `MinnowDatabase verification failed: expected ${String(orderRows)} orders, found ${String(counted.rows[0]?.row_count)}`,
          );
        }
        const storedBytes = await store.getLogicalStorageBytes();
        return {
          engine: backend.id,
          status: "ready",
          storageName: name,
          version: "workspace",
          persistence: backend.persistence,
          dataStoredBytes,
          indexStoredBytes,
          storedBytes,
          buildMs: performance.now() - started,
          insertMs,
          indexMs,
        };
      } finally {
        store.close();
      }
    },

    async openSession(record: DatasetRecord): Promise<EngineSession> {
      const store = await backend.openStore(record);
      const database = new MinnowDatabase(store, databaseOptions(record));
      return {
        engine: backend.id,
        async prepare(sql) {
          // Prepare compiles only; every execute() is a fresh statement over current data.
          const plan = await database.explain(sql);
          const statement: {
            plan: string;
            peakMemoryBytes?: number;
            execute: () => Promise<Array<Record<string, unknown>>>;
            executeCached: () => Promise<Array<Record<string, unknown>>>;
            close: () => void;
          } = {
            plan,
            // memoize: false measures execution, not the probe-validated result memo. The suite
            // replays one statement over unchanging data, so the default (memo on) would answer
            // every sample from cache — a constant-time lookup against the other engines' real
            // execution, which is not a comparison. Applications keep the default and get the
            // cache; this column is what the engine costs when it actually runs the query.
            execute: async () => {
              const result = await database.query(sql, {
                memoize: false,
                onStats: (stats) => {
                  statement.peakMemoryBytes = stats.peakMemoryBytes;
                },
              });
              return normalizeRows(result.rows).map(canonicalizeRow);
            },
            // The same statement with the memo left on, reported beside the execution number
            // rather than instead of it.
            executeCached: async () =>
              normalizeRows((await database.query(sql)).rows).map(canonicalizeRow),
            close: () => undefined,
          };
          return statement;
        },
        async close() {
          await database.close();
          store.close();
        },
      };
    },

    async openLiveSession(record: DatasetRecord): Promise<LiveSession> {
      const store = await backend.openStore(record);
      const database = new MinnowDatabase(store, databaseOptions(record));
      const connection = connectClient(database);
      // Subscriptions and writes share one client, as they do in an application: the commit the
      // client issues hints the worker-side live set, whose change events come back over the
      // same channel the subscription was registered on.
      const live = connection.client.liveQueries();
      return {
        engine: backend.id,
        async createTable(schema: WriteTableSchema) {
          await database.createTable({
            name: schema.name,
            uniqueKey: schema.primaryKey,
            columns: schema.columns.map((column) => ({ name: column.name, type: column.type })),
          });
        },
        async insert(table, batch) {
          await connection.client.insertBatch(table, {
            columns: batch.columns,
            rowCount: batch.rowCount,
          });
        },
        async subscribe(sql, onChange) {
          const subscription = await live.subscribe(sql, {
            onChange: (result) => {
              onChange(normalizeRows(result.rows));
            },
          });
          return { close: () => subscription.close() };
        },
        async close() {
          await live.close();
          await connection.close();
          store.close();
        },
      };
    },

    async openWriteSession(record: DatasetRecord): Promise<WriteSession> {
      const store = await backend.openStore(record);
      const database = new MinnowDatabase(store, databaseOptions(record));
      return {
        engine: backend.id,
        async createTable(schema: WriteTableSchema) {
          await database.createTable({
            name: schema.name,
            uniqueKey: schema.primaryKey,
            columns: schema.columns.map((column) => ({ name: column.name, type: column.type })),
          });
          const projection = schema.columns.map((column) => column.name).join(", ");
          // Nothing to convert: the batch write API takes the columnar batch as it stands, so
          // the timed call is the whole cost of the write.
          return {
            prepareInsert: (batch) => async () => {
              await database.insertBatch(schema.name, {
                columns: batch.columns,
                rowCount: batch.rowCount,
              });
            },
            prepareUpsert: (batch) => async () => {
              await database.upsertBatch(schema.name, {
                columns: batch.columns,
                rowCount: batch.rowCount,
              });
            },
            prepareUpdate: (keys, column, values) => async () => {
              await database.updateBatch(schema.name, { keys, changes: { [column]: values } });
            },
            // memoize: false for the same reason the reference suite uses it — a verification
            // read must observe the table, not a memo of an identical earlier statement.
            readAll: async () =>
              normalizeRows(
                (
                  await database.query(
                    `SELECT ${projection} FROM ${schema.name} ORDER BY ${schema.primaryKey}`,
                    { memoize: false },
                  )
                ).rows,
              ).map(canonicalizeRow),
            // MinnowDatabase has no DROP TABLE; these tables leave with the dataset's database.
            drop: () => Promise.resolve(),
          };
        },
        close() {
          store.close();
          return Promise.resolve();
        },
      };
    },

    async deleteDataset(materialization: EngineMaterialization): Promise<void> {
      await backend.deleteDataset(materialization);
    },
  };
}

export const minnowDriver: EngineDriver = createMinnowDriver({
  id: "minnow",
  persistence: "IndexedDB · immutable compressed column blocks",
  openStore: (record) =>
    IndexedDbBlockStore.open({ name: datasetStorageName(record), durability: record.durability }),
  deleteDataset: (materialization) =>
    new Promise<void>((resolve) => {
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
    }),
});
