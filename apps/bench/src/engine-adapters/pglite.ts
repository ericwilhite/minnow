import { PGlite } from "@electric-sql/pglite";
import { generateEntityBatch } from "../benchmark.js";
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

export async function runPGlite(context: AdapterContext): Promise<AdapterMeasurement> {
  const started = performance.now();
  const resourceBefore = resourceBytes();
  const heapBefore = heapBytes();
  let peakHeapBytes = heapBefore;
  const databaseName = `browserdatabase-compare-${crypto.randomUUID()}`;
  const coldStarted = performance.now();
  let database = await PGlite.create(`idb://${databaseName}`);
  const coldStartMs = performance.now() - coldStarted;
  let generateMs = 0;
  let insertMs = 0;
  let maxGeneratedBatchBytes = 0;
  try {
    const schemaStarted = performance.now();
    for (const entity of context.entities) await database.exec(createTableSql(entity));
    const schemaMs = performance.now() - schemaStarted;
    let completedRows = 0;
    for (const entity of context.entities) {
      const entityRows = entity.rows(context.config.scale);
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
        const maxRowsPerStatement = Math.max(1, Math.floor(10_000 / entity.columns.length));
        const insertStarted = performance.now();
        await database.transaction(async (transaction) => {
          for (let offset = 0; offset < rows.length; offset += maxRowsPerStatement) {
            const statementRows = rows.slice(offset, offset + maxRowsPerStatement);
            const values: unknown[] = [];
            const tuples = statementRows.map((row) => {
              const placeholders = row.map((value) => {
                values.push(value);
                return `$${String(values.length)}`;
              });
              return `(${placeholders.join(", ")})`;
            });
            await transaction.query(
              `INSERT INTO ${quoteIdentifier(entity.name)} (${entity.columns.map((column) => quoteIdentifier(column.name)).join(", ")}) VALUES ${tuples.join(", ")}`,
              values,
            );
          }
        });
        insertMs += performance.now() - insertStarted;
        completedRows += rowCount;
        peakHeapBytes = maxOptional(peakHeapBytes, heapBytes());
        context.report(
          `PGlite · ${entity.name} · ${completedRows.toLocaleString()} rows`,
          completedRows,
          context.totalRows,
        );
      }
    }
    const orderRows =
      context.entities.find((entity) => entity.name === "orders")?.rows(context.config.scale) ?? 0;
    const preIndexCount = await database.query<{ row_count: number | bigint }>(
      "SELECT COUNT(*) AS row_count FROM orders",
    );
    const loadedRowsBeforeIndexes = Number(preIndexCount.rows[0]?.row_count ?? -1);
    const indexStarted = performance.now();
    for (const sql of secondaryIndexSql(context.entities)) await database.exec(sql);
    await database.exec("ANALYZE");
    const indexMs = performance.now() - indexStarted;
    await database.syncToFs();
    await database.close();
    const persistenceReopenStarted = performance.now();
    database = await PGlite.create(`idb://${databaseName}`);
    const persistenceReopenMs = performance.now() - persistenceReopenStarted;
    const countResult = await database.query<{ row_count: number | bigint }>(
      "SELECT COUNT(*) AS row_count FROM orders",
    );
    const loadedRowsAfterIndexes = Number(countResult.rows[0]?.row_count ?? -1);
    const integrityPassed =
      loadedRowsBeforeIndexes === orderRows && loadedRowsAfterIndexes === orderRows;
    const queries = integrityPassed
      ? await measureSqlQueries(context.queries, async (sql) =>
          normalizeRows((await database.query(sql)).rows),
        )
      : [];
    const sizeResult = await database.query<{ bytes: number | bigint }>(
      "SELECT pg_database_size(current_database()) AS bytes",
    );
    const verified = integrityPassed && queries.length === context.queries.length;
    peakHeapBytes = maxOptional(peakHeapBytes, heapBytes());
    return {
      version: "0.5.4",
      recommendedSettings: [
        "benchmark runs inside an existing dedicated worker",
        "IndexedDB filesystem",
        "default durable filesystem syncing",
        "parameterized multi-row inserts capped at 10,000 bind values",
        "ANALYZE after index creation",
      ],
      persistence: "IndexedDB VFS · persistent PostgreSQL data directory",
      coldStartMs,
      schemaMs,
      generateMs,
      insertMs,
      indexMs,
      publicReadMs: null,
      persistenceReopenMs,
      persistenceVerified: integrityPassed,
      totalMs: performance.now() - started,
      storedBytes:
        sizeResult.rows[0]?.bytes === undefined ? null : Number(sizeResult.rows[0].bytes),
      dataPayloadBytes: null,
      storageSizeKind: "complete PostgreSQL database size",
      peakHeapBytes,
      peakHeapDeltaBytes: optionalDifference(peakHeapBytes, heapBefore),
      maxGeneratedBatchBytes,
      loadedResourceBytes: optionalDifference(resourceBytes(), resourceBefore),
      queries,
      verified,
      disclosure: integrityPassed
        ? "Uses PGlite's public create(), transaction(), query(), and exec() APIs. The IndexedDB-backed instance is closed and reopened before verification. The existing benchmark worker provides worker isolation."
        : `Public COUNT(*) verification failed before SQL timing: expected ${String(orderRows)} orders, observed ${String(loadedRowsBeforeIndexes)} before indexes and ${String(loadedRowsAfterIndexes)} after indexes. Query latency is intentionally withheld.`,
    };
  } finally {
    if (!database.closed) await database.close();
    await deleteDatabase(databaseName);
  }
}

async function deleteDatabase(name: string): Promise<void> {
  await new Promise<void>((resolve) => {
    const request = indexedDB.deleteDatabase(name);
    request.onsuccess = () => resolve();
    request.onerror = () => resolve();
    request.onblocked = () => resolve();
  });
}
