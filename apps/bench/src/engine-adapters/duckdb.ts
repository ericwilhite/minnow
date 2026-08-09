import * as duckdb from "@duckdb/duckdb-wasm";
import { tableFromArrays } from "apache-arrow";
import duckdbMvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import duckdbMvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import { generateEntityBatch } from "../benchmark.js";
import {
  createTableSql,
  estimateBatchBytes,
  heapBytes,
  maxOptional,
  measureSqlQueries,
  normalizeRows,
  optionalDifference,
  resourceBytes,
  secondaryIndexSql,
  type AdapterContext,
  type AdapterMeasurement,
} from "../engine-comparison.js";

export async function runDuckDb(context: AdapterContext): Promise<AdapterMeasurement> {
  const started = performance.now();
  const resourceBefore = resourceBytes();
  const heapBefore = heapBytes();
  let peakHeapBytes = heapBefore;
  const coldStarted = performance.now();
  context.report("DuckDB-Wasm · starting portable runtime", 0, context.totalRows);
  const worker = new Worker(duckdbMvpWorker);
  const database = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
  let connection: duckdb.AsyncDuckDBConnection | undefined;
  let generateMs = 0;
  let insertMs = 0;
  let maxGeneratedBatchBytes = 0;
  try {
    await withTimeout(
      database.instantiate(duckdbMvpWasm),
      20_000,
      "DuckDB-Wasm worker startup timed out after 20 seconds",
    );
    context.report("DuckDB-Wasm · opening in-memory database", 0, context.totalRows);
    await withTimeout(
      database.open({ path: ":memory:" }),
      10_000,
      "DuckDB-Wasm database open timed out after 10 seconds",
    );
    const activeConnection = await withTimeout(
      database.connect(),
      10_000,
      "DuckDB-Wasm connection timed out after 10 seconds",
    );
    connection = activeConnection;
    const coldStartMs = performance.now() - coldStarted;
    const schemaStarted = performance.now();
    for (const entity of context.entities) await activeConnection.query(createTableSql(entity));
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
        const arrowColumns = Object.fromEntries(
          Object.entries(columns).map(([name, values]) => [
            name,
            values.map((value) => (value instanceof Date ? value : value)),
          ]),
        );
        const table = tableFromArrays(arrowColumns);
        const insertStarted = performance.now();
        await activeConnection.insertArrowTable(table, { name: entity.name, create: false });
        insertMs += performance.now() - insertStarted;
        completedRows += rowCount;
        peakHeapBytes = maxOptional(peakHeapBytes, heapBytes());
        context.report(
          `DuckDB-Wasm · ${entity.name} · ${completedRows.toLocaleString()} rows`,
          completedRows,
          context.totalRows,
        );
      }
    }
    const indexStarted = performance.now();
    for (const sql of secondaryIndexSql(context.entities)) await activeConnection.query(sql);
    const indexMs = performance.now() - indexStarted;
    const queries = await measureSqlQueries(context.queries, async (sql) => {
      const table = await activeConnection.query(sql);
      return normalizeRows(table.toArray());
    });
    const countTable = await activeConnection.query("SELECT COUNT(*) AS row_count FROM orders");
    const countRow = normalizeRows(countTable.toArray())[0] as { row_count?: unknown } | undefined;
    const orderRows =
      context.entities.find((entity) => entity.name === "orders")?.rows(context.config.scale) ?? 0;
    const sizeTable = await activeConnection.query("PRAGMA database_size");
    const sizeRow = normalizeRows(sizeTable.toArray())[0] as
      { memory_usage?: unknown; database_size?: unknown } | undefined;
    const storedBytes = parseDuckDbBytes(sizeRow?.memory_usage ?? sizeRow?.database_size);
    const verified = Number(countRow?.row_count ?? -1) === orderRows;
    peakHeapBytes = maxOptional(peakHeapBytes, heapBytes());
    return {
      version: await database.getVersion(),
      recommendedSettings: [
        "AsyncDuckDB worker",
        "portable MVP Wasm bundle",
        "Arrow record-batch ingestion",
        "in-memory analytical database",
      ],
      persistence:
        "in-memory · OPFS reopen was tested but the installed build reopened an empty catalog",
      coldStartMs,
      schemaMs,
      generateMs,
      insertMs,
      indexMs,
      publicReadMs: null,
      persistenceReopenMs: null,
      persistenceVerified: null,
      totalMs: performance.now() - started,
      storedBytes,
      dataPayloadBytes: null,
      storageSizeKind: "DuckDB-reported database memory",
      peakHeapBytes,
      peakHeapDeltaBytes: optionalDifference(peakHeapBytes, heapBefore),
      maxGeneratedBatchBytes,
      loadedResourceBytes: optionalDifference(resourceBytes(), resourceBefore),
      queries,
      verified,
      disclosure:
        "Uses AsyncDuckDB and insertArrowTable() through the public API with the portable MVP bundle for consistent worker startup across browser shells. DuckDB-Wasm's OPFS path was exercised separately but failed close/recreate/reopen verification in this build, so this adapter remains transparently in-memory. Size is DuckDB's reported database memory.",
    };
  } finally {
    await connection?.close().catch(() => undefined);
    worker.terminate();
  }
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timer: number | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = self.setTimeout(() => reject(new Error(message)), timeoutMs);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer !== undefined) self.clearTimeout(timer);
  }
}

function parseDuckDbBytes(value: unknown): number | null {
  if (typeof value === "number") return value;
  if (typeof value === "bigint") return Number(value);
  if (typeof value !== "string") return null;
  const match = /^([\d.]+)\s*(bytes|kib|mib|gib|kb|mb|gb)?$/i.exec(value.trim());
  if (match === null) return null;
  const amount = Number(match[1]);
  const unit = (match[2] ?? "bytes").toLowerCase();
  const multiplier =
    unit === "kib" || unit === "kb"
      ? 1024
      : unit === "mib" || unit === "mb"
        ? 1024 ** 2
        : unit === "gib" || unit === "gb"
          ? 1024 ** 3
          : 1;
  return amount * multiplier;
}
