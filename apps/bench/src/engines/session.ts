/**
 * The common driver contract the worker uses to materialize, open, and delete one
 * engine's copy of a dataset. Each driver lives in its own module so an engine's wasm
 * loads only when that engine is actually used.
 */
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
  /** minnow only: the optimized plan. */
  plan?: string;
  close(): void;
}

export interface EngineSession {
  engine: EngineId;
  prepare(sql: string): Promise<PreparedStatement>;
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
