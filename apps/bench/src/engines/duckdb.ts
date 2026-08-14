/**
 * DuckDB (wasm) driver — an executor reference, not a storage peer. duckdb-wasm holds its
 * database in memory: default storage is a transient in-memory catalog, and its OPFS
 * persistence remains experimental and is not exercised here. Every number this engine posts
 * therefore excludes all storage I/O; the materialization records that in `persistence`, and
 * the benchmarks page renders the disclosure from it. Because nothing persists, a session on
 * a fresh worker rebuilds the dataset from the deterministic generator before serving queries.
 */
import * as duckdb from "@duckdb/duckdb-wasm";
import duckdbWasmMvp from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import duckdbWasmEh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbWorkerMvp from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbWorkerEh from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import { generateEntityBatch, getScenario } from "../benchmark.js";
import type { DatasetRecord, EngineMaterialization } from "../protocol.js";
import {
  canonicalizeRow,
  createTableSql,
  quoteIdentifier,
  rowsFromColumns,
  secondaryIndexSql,
} from "./shared.js";
import type { EngineDriver, EngineSession, LoadContext } from "./session.js";

const BATCH_ROWS = 50_000;
/** Rows per INSERT statement; values are rendered as SQL literals. */
const ROWS_PER_STATEMENT = 2_000;

const BUNDLES: duckdb.DuckDBBundles = {
  mvp: { mainModule: duckdbWasmMvp, mainWorker: duckdbWorkerMvp },
  eh: { mainModule: duckdbWasmEh, mainWorker: duckdbWorkerEh },
};

interface DuckDbInstance {
  db: duckdb.AsyncDuckDB;
  version: string;
}

let instancePromise: Promise<DuckDbInstance> | undefined;

function duckdbInstance(): Promise<DuckDbInstance> {
  instancePromise ??= (async () => {
    const bundle = await duckdb.selectBundle(BUNDLES);
    const worker = new Worker(bundle.mainWorker ?? duckdbWorkerEh);
    const logger = new duckdb.VoidLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    return { db, version: await db.getVersion() };
  })();
  return instancePromise;
}

function literal(value: boolean | number | string | null, type: string): string {
  if (value === null) return "NULL";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "NULL";
  const escaped = value.replaceAll("'", "''");
  return type === "TIMESTAMP" ? `TIMESTAMP '${escaped}'` : `'${escaped}'`;
}

/** Arrow's Timestamp type id; arrow-js hands values back as epoch milliseconds. */
const ARROW_TIMESTAMP_TYPE = 10;

function arrowTimestampToIso(value: unknown): unknown {
  if (value === null || value === undefined) return null;
  const millis = typeof value === "bigint" ? Number(value) : Number(value);
  if (!Number.isFinite(millis)) return value;
  return new Date(millis).toISOString();
}

/** Datasets are memory-resident, so a loaded copy lives exactly as long as this worker. */
const loadedDatasets = new Set<string>();

async function buildDataset(
  record: DatasetRecord,
  report?: (message: string, completedRows: number) => void,
  checkCancelled?: () => void,
): Promise<{ insertMs: number }> {
  const { db } = await duckdbInstance();
  const connection = await db.connect();
  let insertMs = 0;
  try {
    const entities = getScenario("commerce").entities;
    for (const entity of entities) {
      await connection.query(`DROP TABLE IF EXISTS ${quoteIdentifier(entity.name)}`);
      await connection.query(createTableSql(entity));
    }
    let completedRows = 0;
    for (const entity of entities) {
      const entityRows = entity.rows(record.scale);
      const types = entity.columns.map((column) =>
        column.type === "datetime" ? "TIMESTAMP" : "OTHER",
      );
      const columnList = entity.columns.map((column) => quoteIdentifier(column.name)).join(", ");
      for (let start = 0; start < entityRows; start += BATCH_ROWS) {
        checkCancelled?.();
        const rowCount = Math.min(BATCH_ROWS, entityRows - start);
        const columns = generateEntityBatch(entity, start, rowCount, entityRows, record.scale);
        const rows = rowsFromColumns(entity, columns);
        const insertStarted = performance.now();
        for (let offset = 0; offset < rows.length; offset += ROWS_PER_STATEMENT) {
          const tuples = rows
            .slice(offset, offset + ROWS_PER_STATEMENT)
            .map(
              (row) =>
                `(${row.map((value, index) => literal(value, types[index] ?? "OTHER")).join(", ")})`,
            );
          await connection.query(
            `INSERT INTO ${quoteIdentifier(entity.name)} (${columnList}) VALUES ${tuples.join(", ")}`,
          );
        }
        insertMs += performance.now() - insertStarted;
        completedRows += rowCount;
        report?.(`DuckDB · ${entity.name}`, completedRows);
      }
    }
    for (const statement of secondaryIndexSql(entities)) await connection.query(statement);
    loadedDatasets.add(record.id);
    return { insertMs };
  } finally {
    await connection.close();
  }
}

/** A raw connection for suites that manage their own fixtures (e.g. the feature matrix). */
export async function duckdbConnection(): Promise<duckdb.AsyncDuckDBConnection> {
  const { db } = await duckdbInstance();
  return db.connect();
}

export const duckdbDriver: EngineDriver = {
  id: "duckdb",

  async loadDataset(context: LoadContext): Promise<EngineMaterialization> {
    const { record } = context;
    const started = performance.now();
    const { version } = await duckdbInstance();
    const { insertMs } = await buildDataset(
      record,
      (message, completedRows) => {
        context.report(message, completedRows);
      },
      () => {
        context.checkCancelled();
      },
    );
    return {
      engine: "duckdb",
      status: "ready",
      storageName: `memory:mdb-dataset-${record.id}`,
      version,
      persistence:
        "in-memory only — no disk-backed storage; all numbers exclude storage I/O entirely",
      storedBytes: null,
      buildMs: performance.now() - started,
      insertMs,
    };
  },

  async openSession(record: DatasetRecord): Promise<EngineSession> {
    const { db } = await duckdbInstance();
    // Memory-resident data dies with the worker; rebuild deterministically when absent.
    if (!loadedDatasets.has(record.id)) await buildDataset(record);
    const connection = await db.connect();
    return {
      engine: "duckdb",
      async prepare(sql) {
        const statement = await connection.prepare(sql);
        return {
          execute: async () => {
            const table = await statement.query();
            // Arrow hands timestamps back as epoch milliseconds; normalize them to the ISO
            // strings the cross-engine checksum expects.
            const timestampColumns: string[] = [];
            for (const field of table.schema.fields) {
              const type = field.type as { typeId?: number };
              if (type.typeId === ARROW_TIMESTAMP_TYPE) timestampColumns.push(field.name);
            }
            return table.toArray().map((entry) => {
              const row = (entry as { toJSON(): Record<string, unknown> }).toJSON();
              for (const name of timestampColumns) row[name] = arrowTimestampToIso(row[name]);
              return canonicalizeRow(row);
            });
          },
          close: () => {
            void statement.close();
          },
        };
      },
      async close() {
        await connection.close();
      },
    };
  },

  async deleteDataset(materialization: EngineMaterialization): Promise<void> {
    const id = materialization.storageName.replace(/^memory:mdb-dataset-/, "");
    loadedDatasets.delete(id);
    const { db } = await duckdbInstance();
    const connection = await db.connect();
    try {
      const entities = getScenario("commerce").entities;
      for (const entity of entities) {
        await connection.query(`DROP TABLE IF EXISTS ${quoteIdentifier(entity.name)}`);
      }
    } finally {
      await connection.close();
    }
  },
};
