import type { LogicalType } from "@browserdatabase/block-format";
import { BrowserDatabase, type DatabaseRow } from "@browserdatabase/engine";
import { IndexedDbBlockStore } from "@browserdatabase/storage-idb";
import {
  generateEntityBatch,
  getScenario,
  relationalTotalRows,
  type EntityDefinition,
  type GeneratedBatchColumns,
} from "./benchmark.js";

export type ComparisonEngineId = "browserdatabase" | "sqlite-wasm" | "duckdb-wasm" | "pglite";

export interface EngineComparisonConfig {
  scale: number;
  compression: "raw" | "rle" | "gzip";
  targetBlockBytes: number;
  durability: "relaxed" | "strict";
  batchRows?: number;
}

export interface SqlQueryDefinition {
  id: string;
  name: string;
  complexity: "simple" | "moderate" | "complex";
  sql: string;
}

export interface SqlQueryMeasurement {
  id: string;
  name: string;
  complexity: SqlQueryDefinition["complexity"];
  medianMs: number;
  p95Ms: number;
  resultRows: number;
  checksum: number;
}

export interface EngineRunMeasurement {
  id: ComparisonEngineId;
  name: string;
  version: string;
  status: "passed" | "failed";
  recommendedSettings: string[];
  persistence: string;
  coldStartMs: number;
  schemaMs: number;
  generateMs: number;
  insertMs: number;
  indexMs: number;
  publicReadMs: number | null;
  persistenceReopenMs: number | null;
  persistenceVerified: boolean | null;
  totalMs: number;
  rows: number;
  rowsPerSecond: number;
  storedBytes: number | null;
  dataPayloadBytes: number | null;
  storageSizeKind: string;
  peakHeapBytes: number | null;
  peakHeapDeltaBytes: number | null;
  maxGeneratedBatchBytes: number;
  loadedResourceBytes: number | null;
  queries: SqlQueryMeasurement[];
  verified: boolean;
  disclosure: string;
  error?: string;
}

export interface EngineComparisonResult {
  recordedAt: string;
  config: Required<EngineComparisonConfig>;
  dataset: {
    name: string;
    tables: number;
    rows: number;
    batchRows: number;
  };
  queries: SqlQueryDefinition[];
  engines: EngineRunMeasurement[];
  sqlChecksumsMatch: boolean;
  note: string;
}

export interface AdapterContext {
  config: Required<EngineComparisonConfig>;
  entities: readonly EntityDefinition[];
  queries: readonly SqlQueryDefinition[];
  totalRows: number;
  report(message: string, completed: number, total: number): void;
}

export interface AdapterMeasurement {
  version: string;
  recommendedSettings: string[];
  persistence: string;
  coldStartMs: number;
  schemaMs: number;
  generateMs: number;
  insertMs: number;
  indexMs: number;
  publicReadMs: number | null;
  persistenceReopenMs: number | null;
  persistenceVerified: boolean | null;
  totalMs: number;
  storedBytes: number | null;
  dataPayloadBytes: number | null;
  storageSizeKind: string;
  peakHeapBytes: number | null;
  peakHeapDeltaBytes: number | null;
  maxGeneratedBatchBytes: number;
  loadedResourceBytes: number | null;
  queries: SqlQueryMeasurement[];
  verified: boolean;
  disclosure: string;
}

export const comparisonQueries: SqlQueryDefinition[] = [
  {
    id: "q1",
    name: "Order count",
    complexity: "simple",
    sql: "SELECT COUNT(*) AS row_count FROM orders",
  },
  {
    id: "q2",
    name: "Revenue by order status",
    complexity: "simple",
    sql: "SELECT status, COUNT(*) AS order_count, ROUND(SUM(total) * 100) / 100 AS revenue FROM orders GROUP BY status ORDER BY status",
  },
  {
    id: "q3",
    name: "Customer segment revenue",
    complexity: "moderate",
    sql: "SELECT c.segment, COUNT(*) AS order_count, ROUND(SUM(o.total) * 100) / 100 AS revenue FROM customers c JOIN orders o ON o.customer_id = c.customer_id GROUP BY c.segment ORDER BY c.segment",
  },
  {
    id: "q4",
    name: "Category line-item revenue",
    complexity: "complex",
    sql: "SELECT p.category, ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct)) * 100) / 100 AS revenue FROM order_items oi JOIN products p ON p.product_id = oi.product_id GROUP BY p.category ORDER BY p.category",
  },
  {
    id: "q5",
    name: "Tax by jurisdiction",
    complexity: "complex",
    sql: "SELECT tj.name, ROUND(SUM(ot.tax_amount) * 100) / 100 AS tax_total FROM order_taxes ot JOIN tax_rates tr ON tr.tax_rate_id = ot.tax_rate_id JOIN tax_jurisdictions tj ON tj.jurisdiction_id = tr.jurisdiction_id GROUP BY tj.name ORDER BY tj.name",
  },
  {
    id: "q6",
    name: "Payment funnel",
    complexity: "complex",
    sql: "SELECT p.status AS payment_status, pt.status AS transaction_status, COUNT(*) AS event_count, ROUND(SUM(pt.amount) * 100) / 100 AS amount FROM payments p JOIN payment_transactions pt ON pt.payment_id = p.payment_id GROUP BY p.status, pt.status ORDER BY p.status, pt.status",
  },
];

export async function runEngineComparison(
  input: EngineComparisonConfig,
  report: AdapterContext["report"],
): Promise<EngineComparisonResult> {
  const config: Required<EngineComparisonConfig> = {
    ...input,
    batchRows: Math.max(1_000, Math.min(100_000, input.batchRows ?? 50_000)),
  };
  const entities = getScenario("commerce").entities;
  const context: AdapterContext = {
    config,
    entities,
    queries: comparisonQueries,
    totalRows: relationalTotalRows(config.scale),
    report,
  };
  const runners: Array<{
    id: ComparisonEngineId;
    name: string;
    load: () => Promise<(context: AdapterContext) => Promise<AdapterMeasurement>>;
  }> = [
    { id: "browserdatabase", name: "BrowserDatabase", load: async () => runBrowserDatabase },
    {
      id: "sqlite-wasm",
      name: "SQLite Wasm",
      load: async () => (await import("./engine-adapters/sqlite.js")).runSqlite,
    },
    {
      id: "duckdb-wasm",
      name: "DuckDB-Wasm",
      load: async () => (await import("./engine-adapters/duckdb.js")).runDuckDb,
    },
    {
      id: "pglite",
      name: "PGlite",
      load: async () => (await import("./engine-adapters/pglite.js")).runPGlite,
    },
  ];
  const engines: EngineRunMeasurement[] = [];
  for (const [index, runner] of runners.entries()) {
    report(`Loading ${runner.name}`, index, runners.length);
    const started = performance.now();
    try {
      const run = await runner.load();
      const measurement = await run({
        ...context,
        report(message, completed, total) {
          const engineProgress = completed / Math.max(1, total);
          report(
            message,
            (index + engineProgress) * context.totalRows,
            runners.length * context.totalRows,
          );
        },
      });
      engines.push({
        id: runner.id,
        name: runner.name,
        status: measurement.verified ? "passed" : "failed",
        rows: context.totalRows,
        rowsPerSecond: context.totalRows / Math.max(measurement.insertMs / 1_000, 0.001),
        ...measurement,
      });
    } catch (error) {
      engines.push({
        id: runner.id,
        name: runner.name,
        version: "unavailable",
        status: "failed",
        recommendedSettings: [],
        persistence: "not initialized",
        coldStartMs: 0,
        schemaMs: 0,
        generateMs: 0,
        insertMs: 0,
        indexMs: 0,
        publicReadMs: null,
        persistenceReopenMs: null,
        persistenceVerified: false,
        totalMs: performance.now() - started,
        rows: context.totalRows,
        rowsPerSecond: 0,
        storedBytes: null,
        dataPayloadBytes: null,
        storageSizeKind: "unavailable",
        peakHeapBytes: null,
        peakHeapDeltaBytes: null,
        maxGeneratedBatchBytes: 0,
        loadedResourceBytes: null,
        queries: [],
        verified: false,
        disclosure: "The adapter failed before verification completed.",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
  const sqlEngines = engines.filter((engine) => engine.verified);
  const sqlChecksumsMatch =
    sqlEngines.length === 4 &&
    comparisonQueries.every((query) => {
      const checksums = sqlEngines.map(
        (engine) => engine.queries.find((measurement) => measurement.id === query.id)?.checksum,
      );
      return checksums.every((checksum) => checksum !== undefined && checksum === checksums[0]);
    });
  report("Engine comparison complete", runners.length, runners.length);
  return {
    recordedAt: new Date().toISOString(),
    config,
    dataset: {
      name: "Relational commerce",
      tables: entities.length,
      rows: context.totalRows,
      batchRows: config.batchRows,
    },
    queries: comparisonQueries,
    engines,
    sqlChecksumsMatch,
    note: "All four engines receive the same deterministic batches and execute the same SQL through documented public APIs. BrowserDatabase prepares one immutable projected snapshot per query during the untimed warm-up, retains only that query's referenced columns, and reports execution separately from preparation. Checksums must agree across all four engines.",
  };
}

async function runBrowserDatabase(context: AdapterContext): Promise<AdapterMeasurement> {
  const started = performance.now();
  const resourceBefore = resourceBytes();
  const heapBefore = heapBytes();
  let peakHeapBytes = heapBefore;
  const databaseName = `browserdatabase-engine-compare-${crypto.randomUUID()}`;
  const coldStarted = performance.now();
  let store = await IndexedDbBlockStore.open({
    name: databaseName,
    durability: context.config.durability,
  });
  const databaseOptions = {
    compression: context.config.compression,
    rowsPerBlock: Math.max(
      1,
      Math.min(100_000, Math.floor(context.config.targetBlockBytes / 8.125)),
    ),
  } as const;
  let database = new BrowserDatabase(store, databaseOptions);
  const coldStartMs = performance.now() - coldStarted;
  let generateMs = 0;
  let insertMs = 0;
  let storedBytes = 0;
  let maxGeneratedBatchBytes = 0;
  try {
    const schemaStarted = performance.now();
    for (const entity of context.entities) {
      await database.createTable({
        name: entity.name,
        ...(entity.primaryKey === undefined ? {} : { uniqueKey: entity.primaryKey }),
        columns: entity.columns.map((column) => ({ name: column.name, type: column.type })),
      });
    }
    const schemaMs = performance.now() - schemaStarted;
    let completedRows = 0;
    for (const entity of context.entities) {
      const entityRows = entity.rows(context.config.scale);
      for (let start = 0; start < entityRows; start += context.config.batchRows) {
        const rowCount = Math.min(context.config.batchRows, entityRows - start);
        const generatedStarted = performance.now();
        const columns = generateEntityBatch(
          entity,
          start,
          rowCount,
          entityRows,
          context.config.scale,
        );
        generateMs += performance.now() - generatedStarted;
        maxGeneratedBatchBytes = Math.max(maxGeneratedBatchBytes, estimateBatchBytes(columns));
        const insertStarted = performance.now();
        const result = await database.insertBatch(entity.name, { columns });
        insertMs += performance.now() - insertStarted;
        storedBytes += result.storedBytes;
        completedRows += rowCount;
        peakHeapBytes = maxOptional(peakHeapBytes, heapBytes());
        context.report(
          `BrowserDatabase · ${entity.name} · ${completedRows.toLocaleString()} rows`,
          completedRows,
          context.totalRows,
        );
      }
    }
    store.close();
    const persistenceReopenStarted = performance.now();
    store = await IndexedDbBlockStore.open({
      name: databaseName,
      durability: context.config.durability,
    });
    database = new BrowserDatabase(store, databaseOptions);
    const persistenceReopenMs = performance.now() - persistenceReopenStarted;
    let publicReadMs = 0;
    let activeSql: string | undefined;
    let activePrepared: Awaited<ReturnType<BrowserDatabase["prepareQuery"]>> | undefined;
    const queries = await measureSqlQueries(context.queries, async (sql) => {
      if (activeSql !== sql) {
        activePrepared?.close();
        const prepareStarted = performance.now();
        activePrepared = await database.prepareQuery(sql);
        publicReadMs += performance.now() - prepareStarted;
        activeSql = sql;
        peakHeapBytes = maxOptional(peakHeapBytes, heapBytes());
      }
      if (activePrepared === undefined) throw new Error("Query preparation did not complete");
      return normalizeRows(activePrepared.execute().rows);
    });
    activePrepared?.close();
    const expectedOrders =
      context.entities.find((entity) => entity.name === "orders")?.rows(context.config.scale) ?? 0;
    const orderCount = queries.find((query) => query.id === "q1");
    const verified =
      orderCount?.resultRows === 1 &&
      orderCount.checksum !== 0 &&
      (await database.query("SELECT COUNT(*) AS row_count FROM orders")).rows[0]?.row_count ===
        expectedOrders &&
      (await database.listTables()).length === context.entities.length;
    peakHeapBytes = maxOptional(peakHeapBytes, heapBytes());
    return {
      version: "workspace",
      recommendedSettings: [
        `${context.config.compression} compression`,
        `${context.config.targetBlockBytes.toLocaleString()}-byte target blocks`,
        `${context.config.durability} IndexedDB durability`,
        `${context.config.batchRows.toLocaleString()}-row bounded batches`,
      ],
      persistence: "IndexedDB · persistent immutable compressed blocks",
      coldStartMs,
      schemaMs,
      generateMs,
      insertMs,
      indexMs: 0,
      publicReadMs,
      persistenceReopenMs,
      persistenceVerified: verified,
      totalMs: performance.now() - started,
      storedBytes: await store.getLogicalStorageBytes(),
      dataPayloadBytes: storedBytes,
      storageSizeKind: "complete logical IndexedDB payload",
      peakHeapBytes,
      peakHeapDeltaBytes: optionalDifference(peakHeapBytes, heapBefore),
      maxGeneratedBatchBytes,
      loadedResourceBytes: optionalDifference(resourceBytes(), resourceBefore),
      queries,
      verified,
      disclosure:
        "Measured through createTable(), insertBatch(), prepareQuery(), query(), and listTables(). The IndexedDB connection is closed and reopened before verification. Each SQL query is prepared during its untimed warm-up against one immutable snapshot; the reported SQL samples measure public PreparedQuery.execute(), while Public query prep is the sum of storage reads and decoding for all six projected query snapshots. Only one prepared query is retained at a time. Database size includes blocks, keys, manifests, segments, transactions, and catalog records as complete logical IndexedDB payload; browser-specific page overhead is not exposed.",
    };
  } finally {
    store.close();
    await deleteIndexedDatabase(databaseName);
  }
}

export function sqlType(type: LogicalType): string {
  switch (type) {
    case "boolean":
      return "BOOLEAN";
    case "number":
      return "DOUBLE PRECISION";
    case "string":
      return "TEXT";
    case "datetime":
      return "TIMESTAMP";
  }
}

export function createTableSql(entity: EntityDefinition): string {
  const columns = entity.columns.map((column) => {
    const primary = column.name === entity.primaryKey ? " PRIMARY KEY" : "";
    return `${quoteIdentifier(column.name)} ${sqlType(column.type)}${primary}`;
  });
  return `CREATE TABLE ${quoteIdentifier(entity.name)} (${columns.join(", ")})`;
}

export function secondaryIndexSql(entities: readonly EntityDefinition[]): string[] {
  return entities.flatMap((entity) =>
    entity.columns
      .filter((column) => column.name.endsWith("_id") && column.name !== entity.primaryKey)
      .map(
        (column) =>
          `CREATE INDEX ${quoteIdentifier(`idx_${entity.name}_${column.name}`)} ON ${quoteIdentifier(entity.name)} (${quoteIdentifier(column.name)})`,
      ),
  );
}

export function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

export function rowsFromColumns(
  entity: EntityDefinition,
  columns: GeneratedBatchColumns,
): Array<Array<boolean | number | string | null>> {
  const rowCount = columns[entity.columns[0]?.name ?? ""]?.length ?? 0;
  return Array.from({ length: rowCount }, (_, rowIndex) =>
    entity.columns.map((column) => {
      const value = columns[column.name]?.[rowIndex] ?? null;
      return value instanceof Date ? value.toISOString() : value;
    }),
  );
}

export async function measureSqlQueries(
  definitions: readonly SqlQueryDefinition[],
  query: (sql: string) => Promise<unknown[]>,
): Promise<SqlQueryMeasurement[]> {
  const measurements: SqlQueryMeasurement[] = [];
  for (const definition of definitions) {
    await query(definition.sql);
    const samples: number[] = [];
    let rows: unknown[] = [];
    for (let sample = 0; sample < 3; sample += 1) {
      const started = performance.now();
      rows = await query(definition.sql);
      samples.push(performance.now() - started);
    }
    samples.sort((left, right) => left - right);
    measurements.push({
      id: definition.id,
      name: definition.name,
      complexity: definition.complexity,
      medianMs: samples[1] ?? samples[0] ?? 0,
      p95Ms: samples[2] ?? samples[0] ?? 0,
      resultRows: rows.length,
      checksum: resultChecksum(rows),
    });
  }
  return measurements;
}

export function estimateBatchBytes(columns: GeneratedBatchColumns): number {
  let total = 0;
  for (const values of Object.values(columns)) {
    for (const value of values) {
      if (value === null) total += 1;
      else if (typeof value === "boolean") total += 1;
      else if (typeof value === "number" || value instanceof Date) total += 8;
      else total += new TextEncoder().encode(value).byteLength;
    }
  }
  return total;
}

export function heapBytes(): number | null {
  const value = (performance as Performance & { memory?: { usedJSHeapSize?: number } }).memory
    ?.usedJSHeapSize;
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

export function resourceBytes(): number | null {
  const resources = performance.getEntriesByType("resource") as PerformanceResourceTiming[];
  if (resources.length === 0) return null;
  const bytes = resources.reduce(
    (total, resource) => total + (resource.encodedBodySize || resource.transferSize || 0),
    0,
  );
  return bytes > 0 ? bytes : null;
}

export function maxOptional(left: number | null, right: number | null): number | null {
  if (left === null) return right;
  if (right === null) return left;
  return Math.max(left, right);
}

export function optionalDifference(after: number | null, before: number | null): number | null {
  return after === null || before === null ? null : Math.max(0, after - before);
}

export function normalizeRows(rows: unknown[]): unknown[] {
  return rows.map((row) => {
    if (row !== null && typeof row === "object" && "toJSON" in row) {
      const toJSON = (row as { toJSON?: () => unknown }).toJSON;
      if (typeof toJSON === "function") return toJSON.call(row);
    }
    return row;
  });
}

function resultChecksum(rows: unknown[]): number {
  const normalized = JSON.stringify(rows, (_key, value: unknown) => {
    if (typeof value === "bigint") return Number(value);
    if (typeof value === "number" && Number.isFinite(value)) {
      return Math.round(value * 1_000_000) / 1_000_000;
    }
    if (value !== null && typeof value === "object" && !Array.isArray(value)) {
      return Object.fromEntries(
        Object.entries(value as Record<string, unknown>).sort(([left], [right]) =>
          left.localeCompare(right),
        ),
      );
    }
    return value;
  });
  let hash = 2_166_136_261;
  for (let index = 0; index < normalized.length; index += 1) {
    hash ^= normalized.charCodeAt(index);
    hash = Math.imul(hash, 16_777_619);
  }
  return hash >>> 0;
}

async function deleteIndexedDatabase(name: string): Promise<void> {
  await new Promise<void>((resolve) => {
    const request = indexedDB.deleteDatabase(name);
    request.onsuccess = () => resolve();
    request.onerror = () => resolve();
    request.onblocked = () => resolve();
  });
}

export function databaseRowsChecksum(rows: readonly DatabaseRow[]): number {
  return resultChecksum([...rows]);
}
