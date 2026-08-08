import * as duckdb from "@duckdb/duckdb-wasm";
import { tableFromArrays } from "apache-arrow";
import duckdbEhWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbMvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import duckdbEhWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
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
  const bundle = await duckdb.selectBundle({
    mvp: { mainModule: duckdbMvpWasm, mainWorker: duckdbMvpWorker },
    eh: { mainModule: duckdbEhWasm, mainWorker: duckdbEhWorker },
  });
  if (bundle.mainWorker === null) throw new Error("DuckDB-Wasm did not select a worker bundle");
  const worker = new Worker(bundle.mainWorker);
  const database = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
  await database.instantiate(bundle.mainModule, bundle.pthreadWorker);
  await database.open({ path: ":memory:" });
  const connection = await database.connect();
  const coldStartMs = performance.now() - coldStarted;
  let generateMs = 0;
  let insertMs = 0;
  let maxGeneratedBatchBytes = 0;
  try {
    const schemaStarted = performance.now();
    for (const entity of context.entities) await connection.query(createTableSql(entity));
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
        await connection.insertArrowTable(table, { name: entity.name, create: false });
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
    for (const sql of secondaryIndexSql(context.entities)) await connection.query(sql);
    const indexMs = performance.now() - indexStarted;
    const queries = await measureSqlQueries(context.queries, async (sql) => {
      const table = await connection.query(sql);
      return normalizeRows(table.toArray());
    });
    const countTable = await connection.query("SELECT COUNT(*) AS row_count FROM orders");
    const countRow = normalizeRows(countTable.toArray())[0] as { row_count?: unknown } | undefined;
    const orderRows =
      context.entities.find((entity) => entity.name === "orders")?.rows(context.config.scale) ?? 0;
    const sizeTable = await connection.query("PRAGMA database_size");
    const sizeRow = normalizeRows(sizeTable.toArray())[0] as
      { memory_usage?: unknown; database_size?: unknown } | undefined;
    const storedBytes = parseDuckDbBytes(sizeRow?.memory_usage ?? sizeRow?.database_size);
    const verified = Number(countRow?.row_count ?? -1) === orderRows;
    peakHeapBytes = maxOptional(peakHeapBytes, heapBytes());
    return {
      version: await database.getVersion(),
      recommendedSettings: [
        "AsyncDuckDB worker",
        "selectBundle() feature detection",
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
        "Uses AsyncDuckDB and insertArrowTable() through the public API. DuckDB-Wasm's OPFS path was exercised separately but failed close/recreate/reopen verification in this build, so this adapter remains transparently in-memory. Size is DuckDB's reported database memory.",
    };
  } finally {
    await connection.close().catch(() => undefined);
    await database.terminate();
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
