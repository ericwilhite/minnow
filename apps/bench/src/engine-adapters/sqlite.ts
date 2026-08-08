import sqlite3InitModule, { type Database, type SAHPoolUtil } from "@sqlite.org/sqlite-wasm";
import {
  createTableSql,
  estimateBatchBytes,
  heapBytes,
  maxOptional,
  measureSqlQueries,
  normalizeRows,
  optionalDifference,
  quoteIdentifier,
  resourceBytes,
  rowsFromColumns,
  secondaryIndexSql,
  type AdapterContext,
  type AdapterMeasurement,
} from "../engine-comparison.js";
import { generateEntityBatch } from "../benchmark.js";

export async function runSqlite(context: AdapterContext): Promise<AdapterMeasurement> {
  const started = performance.now();
  const resourceBefore = resourceBytes();
  const heapBefore = heapBytes();
  let peakHeapBytes = heapBefore;
  const databasePath = `/browserdatabase-compare-${crypto.randomUUID()}.sqlite3`;
  const coldStarted = performance.now();
  const sqlite3 = await sqlite3InitModule();
  let sahPool: SAHPoolUtil | undefined;
  let persistenceMode: "OPFS VFS" | "OPFS SAH-pool VFS";
  let openDatabase: () => Database;
  if ("opfs" in sqlite3 && typeof sqlite3.oo1.OpfsDb === "function") {
    persistenceMode = "OPFS VFS";
    openDatabase = () => new sqlite3.oo1.OpfsDb(databasePath, "c");
  } else {
    try {
      const poolName = `sah-${crypto.randomUUID()}`;
      const installedPool = await sqlite3.installOpfsSAHPoolVfs({
        name: poolName,
        directory: `/${poolName}`,
        initialCapacity: 8,
      });
      sahPool = installedPool;
      persistenceMode = "OPFS SAH-pool VFS";
      openDatabase = () => new installedPool.OpfsSAHPoolDb(databasePath);
    } catch (error) {
      throw new Error(
        `SQLite persistent OPFS storage is unavailable; the benchmark refuses an in-memory fallback. ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  let database = openDatabase();
  const coldStartMs = performance.now() - coldStarted;
  let generateMs = 0;
  let insertMs = 0;
  let maxGeneratedBatchBytes = 0;
  try {
    database.exec("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;");
    const schemaStarted = performance.now();
    for (const entity of context.entities) database.exec(createTableSql(entity));
    const schemaMs = performance.now() - schemaStarted;
    let completedRows = 0;
    for (const entity of context.entities) {
      const entityRows = entity.rows(context.config.scale);
      const placeholders = entity.columns.map(() => "?").join(", ");
      const insertSql = `INSERT INTO ${quoteIdentifier(entity.name)} (${entity.columns.map((column) => quoteIdentifier(column.name)).join(", ")}) VALUES (${placeholders})`;
      const statement = database.prepare(insertSql);
      try {
        for (let start = 0; start < entityRows; start += context.config.batchRows) {
          const rowCount = Math.min(context.config.batchRows, entityRows - start);
          const generateStarted = performance.now();
          const columns = generateEntityBatch(
            entity,
            start,
            rowCount,
            entityRows,
            context.config.scale,
          );
          generateMs += performance.now() - generateStarted;
          maxGeneratedBatchBytes = Math.max(maxGeneratedBatchBytes, estimateBatchBytes(columns));
          const rows = rowsFromColumns(entity, columns);
          const insertStarted = performance.now();
          database.transaction("IMMEDIATE", () => {
            for (const row of rows) statement.bind(row).stepReset();
          });
          insertMs += performance.now() - insertStarted;
          completedRows += rowCount;
          peakHeapBytes = maxOptional(peakHeapBytes, heapBytes());
          context.report(
            `SQLite Wasm · ${entity.name} · ${completedRows.toLocaleString()} rows`,
            completedRows,
            context.totalRows,
          );
        }
      } finally {
        statement.finalize();
      }
    }
    const indexStarted = performance.now();
    for (const sql of secondaryIndexSql(context.entities)) database.exec(sql);
    database.exec("PRAGMA optimize");
    const indexMs = performance.now() - indexStarted;
    database.close();
    const persistenceReopenStarted = performance.now();
    database = openDatabase();
    database.exec("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;");
    const persistenceReopenMs = performance.now() - persistenceReopenStarted;
    const pageCount = Number(database.selectValue("PRAGMA page_count") ?? 0);
    const pageSize = Number(database.selectValue("PRAGMA page_size") ?? 0);
    const queries = await measureSqlQueries(context.queries, async (sql) =>
      normalizeRows(database.selectObjects(sql)),
    );
    const orderRows =
      context.entities.find((entity) => entity.name === "orders")?.rows(context.config.scale) ?? 0;
    const verified =
      Number(database.selectValue("SELECT COUNT(*) FROM orders") ?? -1) === orderRows &&
      queries.length === context.queries.length;
    peakHeapBytes = maxOptional(peakHeapBytes, heapBytes());
    return {
      version: sqlite3.version.libVersion,
      recommendedSettings: [
        `dedicated worker + required ${persistenceMode}`,
        "WAL journal",
        "synchronous=NORMAL",
        "foreign_keys=ON",
        "prepared inserts inside IMMEDIATE transactions",
      ],
      persistence: `${persistenceMode} · persistent SQLite file`,
      coldStartMs,
      schemaMs,
      generateMs,
      insertMs,
      indexMs,
      publicReadMs: null,
      persistenceReopenMs,
      persistenceVerified: verified,
      totalMs: performance.now() - started,
      storedBytes: pageCount * pageSize,
      dataPayloadBytes: null,
      storageSizeKind: "complete SQLite page allocation",
      peakHeapBytes,
      peakHeapDeltaBytes: optionalDifference(peakHeapBytes, heapBefore),
      maxGeneratedBatchBytes,
      loadedResourceBytes: optionalDifference(resourceBytes(), resourceBefore),
      queries,
      verified,
      disclosure:
        "Uses SQLite's documented OO1 public API in the benchmark worker. The OPFS database is closed and reopened before verification. Storage is page_count × page_size after indexes; query timing is warm plus three measured executions.",
    };
  } finally {
    if (database.isOpen()) database.close();
    if (sahPool === undefined) {
      const opfs = (
        sqlite3 as typeof sqlite3 & {
          opfs?: { unlink?: (path: string) => Promise<unknown> };
        }
      ).opfs;
      await opfs?.unlink?.(databasePath);
      await opfs?.unlink?.(`${databasePath}-wal`);
      await opfs?.unlink?.(`${databasePath}-shm`);
    } else {
      sahPool.unlink(databasePath);
      await sahPool.removeVfs();
    }
  }
}
