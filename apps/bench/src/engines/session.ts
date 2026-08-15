/**
 * The common driver contract the worker uses to materialize, open, and delete one
 * engine's copy of a dataset. Each driver lives in its own module so an engine's wasm
 * loads only when that engine is actually used.
 */
import type { LogicalType } from "@minnowdb/core/block-format";
import type { DatasetRecord, EngineId, EngineMaterialization } from "../protocol.js";

export interface PreparedStatement {
  /** Rows with canonicalized values (see engines/shared.ts). */
  execute(): Promise<Array<Record<string, unknown>>>;
  /**
   * minnow only: the same statement run the way an application runs it, with the
   * probe-validated result memo left on. Repeating a statement over data that has not changed
   * is answered from cache, so this measures the cache, not execution — it is reported beside
   * `execute()` rather than in place of it, because the other engines have no equivalent and
   * would be re-executing.
   */
  executeCached?(): Promise<Array<Record<string, unknown>>>;
  /**
   * Peak modeled execution memory, in bytes, for the most recent `execute()`. Reported by
   * engines that can measure their own execution; the browser's cross-engine memory API
   * (performance.measureUserAgentSpecificMemory) is unavailable in this harness, so this is
   * self-reported rather than observed from outside, and only Minnow reports it.
   */
  peakMemoryBytes?: number;
  /** minnow only: the optimized plan. */
  plan?: string;
  close(): void;
}

export interface EngineSession {
  engine: EngineId;
  prepare(sql: string): Promise<PreparedStatement>;
  close(): Promise<void>;
}

/**
 * The dedicated table the write suite creates; never one of the dataset's own tables.
 * Structurally a `TableShape` (engines/shared.ts), so the portable DDL builds it directly —
 * the key is a real PRIMARY KEY on the SQL engines and the unique key on minnow, which is
 * what update and upsert address rows through.
 */
export interface WriteTableSchema {
  name: string;
  primaryKey: string;
  columns: ReadonlyArray<{ name: string; type: LogicalType }>;
}

/** One batch in the columnar form every engine's bulk path ultimately wants. */
export interface WriteBatch {
  rowCount: number;
  columns: Record<string, Array<boolean | number | string | Date | null>>;
}

/**
 * One created table, driven through its engine's own bulk write paths.
 *
 * Each `prepare*` converts the batch into the form its engine's API takes — row tuples to
 * bind for sqlite, statement text and flat parameters for PGlite, an Arrow table for duckdb,
 * the columnar batch itself for minnow — and returns the call that applies it. Only that
 * returned call is timed. The split exists because reshaping JavaScript is the harness's
 * cost, not the engine's: pivoting 100,000 columnar rows into tuples takes ~60ms, which
 * would otherwise be charged to the engines whose APIs want rows and to no others.
 * Everything the engine itself does — compiling the statement, encoding, staging, the
 * transaction, the commit — stays inside the timed call.
 */
export interface WriteTarget {
  /** Fresh rows through the engine's bulk insert path. */
  prepareInsert(batch: WriteBatch): () => Promise<void>;
  /** Rows keyed like an insert, where an existing key overwrites the row in place. */
  prepareUpsert(batch: WriteBatch): () => Promise<void>;
  /** One column changed on existing rows, addressed by key. */
  prepareUpdate(
    keys: readonly number[],
    column: string,
    values: readonly number[],
  ): () => Promise<void>;
  /** The whole table, canonicalized, with every result cache bypassed. */
  readAll(): Promise<Array<Record<string, unknown>>>;
  /** Best effort: an engine with no DROP TABLE leaves the table behind with the dataset. */
  drop(): Promise<void>;
}

export interface WriteSession {
  engine: EngineId;
  /** Creates one empty table. The suite calls this per sample so no sample inherits rows. */
  createTable(schema: WriteTableSchema): Promise<WriteTarget>;
  close(): Promise<void>;
}

export interface LoadContext {
  record: DatasetRecord;
  /** Rows loaded so far into this engine; the caller scales to overall progress. */
  report(message: string, completedRows: number): void;
  /** Throws AbortError when the user cancelled the run. */
  checkCancelled(): void;
}

export interface EngineDriver {
  id: EngineId;
  loadDataset(context: LoadContext): Promise<EngineMaterialization>;
  openSession(record: DatasetRecord): Promise<EngineSession>;
  /**
   * A connection for the write suite, on the same storage the dataset lives in — writing
   * into a database that already holds data is the case worth measuring, and it keeps every
   * engine's persistence settings identical to the read comparison.
   */
  openWriteSession(record: DatasetRecord): Promise<WriteSession>;
  deleteDataset(materialization: EngineMaterialization): Promise<void>;
}

export function requireMaterialization(
  record: DatasetRecord,
  engine: EngineId,
): EngineMaterialization {
  const materialization = record.engines[engine];
  if (materialization?.status !== "ready") {
    throw new Error(`Dataset ${record.id} has no ready ${engine} copy`);
  }
  return materialization;
}

export async function loadDriver(engine: EngineId): Promise<EngineDriver> {
  switch (engine) {
    case "minnow":
      return (await import("./minnow.js")).minnowDriver;
    case "sqlite":
      return (await import("./sqlite.js")).sqliteDriver;
    case "pglite":
      return (await import("./pglite.js")).pgliteDriver;
    case "duckdb":
      return (await import("./duckdb.js")).duckdbDriver;
  }
}
