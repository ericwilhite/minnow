import sqlite3InitModule, {
  type Database,
  type SAHPoolUtil,
  type Sqlite3Static,
} from "@sqlite.org/sqlite-wasm";
import { generateEntityBatch, getScenario } from "../benchmark.js";
import type { DatasetRecord, EngineMaterialization } from "../protocol.js";
import {
  canonicalizeRow,
  columnList,
  createTableSql,
  normalizeRows,
  quoteIdentifier,
  rowsFromColumns,
  secondaryIndexSql,
} from "./shared.js";
import type {
  EngineDriver,
  EngineSession,
  LoadContext,
  WriteSession,
  WriteTableSchema,
} from "./session.js";

const BATCH_ROWS = 50_000;
const SAH_POOL_NAME = "mdb-datasets";
/**
 * SQLite runs on its shipped defaults: no journal, cache, locking, or temp-store pragmas.
 * Every engine in this comparison is measured as it comes out of the box, so no engine's
 * numbers depend on tuning knowledge the next person may not apply.
 */
const PRAGMAS = "";

interface SqliteContext {
  sqlite3: Sqlite3Static;
  vfs: "opfs" | "sah-pool";
  pool?: SAHPoolUtil;
}

let contextPromise: Promise<SqliteContext> | undefined;

/**
 * One sqlite module and one persistent VFS per worker. The plain OPFS VFS is preferred;
 * WebKit lacks it, so a fixed-name SAH pool is installed there instead — fixed-name
 * because datasets written through a pool are reachable only through that same pool.
 */
function sqliteContext(): Promise<SqliteContext> {
  contextPromise ??= (async () => {
    const sqlite3 = await sqlite3InitModule();
    if ("opfs" in sqlite3 && typeof sqlite3.oo1.OpfsDb === "function") {
      return { sqlite3, vfs: "opfs" as const };
    }
    const pool = await sqlite3.installOpfsSAHPoolVfs({
      name: SAH_POOL_NAME,
      directory: `/${SAH_POOL_NAME}`,
      initialCapacity: 6,
    });
    return { sqlite3, vfs: "sah-pool" as const, pool };
  })();
  return contextPromise;
}

function databasePath(record: DatasetRecord): string {
  return `/mdb-dataset-${record.id}.sqlite3`;
}

async function openDatabase(context: SqliteContext, path: string): Promise<Database> {
  if (context.vfs === "opfs") {
    const database = new context.sqlite3.oo1.OpfsDb(path, "c");
    if (PRAGMAS.length > 0) database.exec(PRAGMAS);
    return database;
  }
  const pool = context.pool;
  if (pool === undefined) throw new Error("SQLite SAH pool is not installed");
  // Each database file consumes one preallocated sync access handle; grow the pool
  // before it runs out so additional datasets keep fitting.
  if (pool.getCapacity() - pool.getFileCount() < 3) await pool.addCapacity(4);
  const database = new pool.OpfsSAHPoolDb(path);
  if (PRAGMAS.length > 0) database.exec(PRAGMAS);
  return database;
}

/** Opens a dataset's file, refusing when it was written through a VFS this browser lacks. */
async function openDatasetDatabase(record: DatasetRecord): Promise<Database> {
  const sqlite = await sqliteContext();
  const materialization = record.engines.sqlite;
  if (materialization?.vfs !== undefined && materialization.vfs !== sqlite.vfs) {
    throw new Error(
      `This dataset's SQLite copy was written through the ${materialization.vfs} VFS, which is not available in this browser session`,
    );
  }
  return openDatabase(sqlite, databasePath(record));
}

export const sqliteDriver: EngineDriver = {
  id: "sqlite",

  async loadDataset(context: LoadContext): Promise<EngineMaterialization> {
    const { record } = context;
    const started = performance.now();
    const sqlite = await sqliteContext();
    const path = databasePath(record);
    let database = await openDatabase(sqlite, path);
    let insertMs = 0;
    try {
      const entities = getScenario("commerce").entities;
      for (const entity of entities) database.exec(createTableSql(entity));
      let completedRows = 0;
      for (const entity of entities) {
        const entityRows = entity.rows(record.scale);
        const placeholders = entity.columns.map(() => "?").join(", ");
        const insertSql = `INSERT INTO ${quoteIdentifier(entity.name)} (${entity.columns.map((column) => quoteIdentifier(column.name)).join(", ")}) VALUES (${placeholders})`;
        const statement = database.prepare(insertSql);
        try {
          for (let start = 0; start < entityRows; start += BATCH_ROWS) {
            context.checkCancelled();
            const rowCount = Math.min(BATCH_ROWS, entityRows - start);
            const columns = generateEntityBatch(entity, start, rowCount, entityRows, record.scale);
            const rows = rowsFromColumns(entity, columns);
            const insertStarted = performance.now();
            database.transaction("IMMEDIATE", () => {
              for (const row of rows) statement.bind(row).stepReset();
            });
            insertMs += performance.now() - insertStarted;
            completedRows += rowCount;
            context.report(`SQLite Wasm · ${entity.name}`, completedRows);
          }
        } finally {
          statement.finalize();
        }
      }
      for (const sql of secondaryIndexSql(entities)) database.exec(sql);
      database.exec("PRAGMA optimize");
      // Close and reopen so what the record marks "ready" is what actually persisted.
      database.close();
      database = await openDatabase(sqlite, path);
      const orderRows =
        entities.find((entity) => entity.name === "orders")?.rows(record.scale) ?? 0;
      const counted = Number(database.selectValue("SELECT COUNT(*) FROM orders") ?? -1);
      if (counted !== orderRows) {
        throw new Error(
          `SQLite verification failed after reopen: expected ${String(orderRows)} orders, found ${String(counted)}`,
        );
      }
      const pageCount = Number(database.selectValue("PRAGMA page_count") ?? 0);
      const pageSize = Number(database.selectValue("PRAGMA page_size") ?? 0);
      return {
        engine: "sqlite",
        status: "ready",
        storageName: path,
        vfs: sqlite.vfs,
        version: sqlite.sqlite3.version.libVersion,
        persistence:
          sqlite.vfs === "opfs"
            ? "OPFS VFS · persistent SQLite file"
            : "OPFS SAH-pool VFS · persistent SQLite file",
        storedBytes: pageCount * pageSize,
        buildMs: performance.now() - started,
        insertMs,
      };
    } finally {
      if (database.isOpen()) database.close();
    }
  },

  async openSession(record: DatasetRecord): Promise<EngineSession> {
    const database = await openDatasetDatabase(record);
    return {
      engine: "sqlite",
      prepare(sql) {
        // A genuine prepared statement: compiled once, reset and re-stepped per execution,
        // so prepareMs means the same thing here as in the other engines.
        const statement = database.prepare(sql);
        return Promise.resolve({
          execute: () => {
            const rows: Array<Record<string, unknown>> = [];
            statement.reset();
            while (statement.step()) rows.push(statement.get({}));
            return Promise.resolve(normalizeRows(rows).map(canonicalizeRow));
          },
          close: () => {
            statement.finalize();
          },
        });
      },
      close() {
        if (database.isOpen()) database.close();
        return Promise.resolve();
      },
    };
  },

  async openWriteSession(record: DatasetRecord): Promise<WriteSession> {
    const database = await openDatasetDatabase(record);
    return {
      engine: "sqlite",
      createTable(schema: WriteTableSchema) {
        database.exec(createTableSql(schema));
        const table = quoteIdentifier(schema.name);
        const key = quoteIdentifier(schema.primaryKey);
        const columns = columnList(schema);
        const placeholders = schema.columns.map(() => "?").join(", ");
        const assignments = schema.columns
          .filter((column) => column.name !== schema.primaryKey)
          .map(
            (column) =>
              `${quoteIdentifier(column.name)} = excluded.${quoteIdentifier(column.name)}`,
          )
          .join(", ");
        /**
         * One prepared statement stepped per row inside a single transaction — the way
         * sqlite is meant to be written in bulk, and what the dataset loader already does.
         * Compiling the statement is engine work and stays inside the timed call; pivoting
         * the batch into bindable tuples is not, and happens before the call is returned.
         */
        const rowRunner =
          (sql: string, rows: Array<Array<boolean | number | string | null>>) =>
          (): Promise<void> => {
            const statement = database.prepare(sql);
            try {
              database.transaction("IMMEDIATE", () => {
                for (const row of rows) statement.bind(row).stepReset();
              });
            } finally {
              statement.finalize();
            }
            return Promise.resolve();
          };
        return Promise.resolve({
          prepareInsert: (batch) =>
            rowRunner(
              `INSERT INTO ${table} (${columns}) VALUES (${placeholders})`,
              rowsFromColumns(schema, batch.columns),
            ),
          prepareUpsert: (batch) =>
            rowRunner(
              `INSERT INTO ${table} (${columns}) VALUES (${placeholders}) ON CONFLICT(${key}) DO UPDATE SET ${assignments}`,
              rowsFromColumns(schema, batch.columns),
            ),
          prepareUpdate: (keys, column, values) =>
            rowRunner(
              `UPDATE ${table} SET ${quoteIdentifier(column)} = ? WHERE ${key} = ?`,
              keys.map((keyValue, index) => [values[index] ?? null, keyValue]),
            ),
          readAll: () => {
            const statement = database.prepare(`SELECT ${columns} FROM ${table} ORDER BY ${key}`);
            try {
              const rows: Array<Record<string, unknown>> = [];
              while (statement.step()) rows.push(statement.get({}));
              return Promise.resolve(normalizeRows(rows).map(canonicalizeRow));
            } finally {
              statement.finalize();
            }
          },
          drop: () => {
            database.exec(`DROP TABLE IF EXISTS ${table}`);
            return Promise.resolve();
          },
        });
      },
      close() {
        if (database.isOpen()) database.close();
        return Promise.resolve();
      },
    };
  },

  async deleteDataset(materialization: EngineMaterialization): Promise<void> {
    const sqlite = await sqliteContext();
    const path = materialization.storageName;
    if (materialization.vfs === "sah-pool") {
      sqlite.pool?.unlink(path);
      return;
    }
    const opfs = (
      sqlite.sqlite3 as Sqlite3Static & {
        opfs?: { unlink?: (path: string) => Promise<unknown> };
      }
    ).opfs;
    await opfs?.unlink?.(path);
    await opfs?.unlink?.(`${path}-wal`);
    await opfs?.unlink?.(`${path}-shm`);
  },
};
