/**
 * The calls the Node driver makes into the page (`window.BrowserPerf`), and what they return.
 *
 * Every call resolves; an engine failure comes back as `{ ok: false, failure }` so the driver can
 * record it and carry on. Only a crashed tab or a driver-side deadline rejects.
 */
import type { EngineId } from "./catalogue.ts";

export interface PageFailure {
  /** The error's class (`name`), as the engine reported it. */
  readonly name: string;
  readonly message: string;
  /** The page's own per-run deadline fired and the engine worker was terminated. */
  readonly timeout: boolean;
}

export type PageResult<T> =
  ({ readonly ok: true } & T) | { readonly ok: false; readonly failure: PageFailure };

/** SQLite pragma sets. `strict` and `relaxed` ask for WAL, which needs EXCLUSIVE locking here. */
export type SqlitePragmaSet = "default" | "strict" | "relaxed";

export interface OpenRequest {
  readonly engine: EngineId;
  /** The database name: a Minnow OPFS store, `<db>.duckdb`, or `/<db>.sqlite3` in the SAH pool. */
  readonly db: string;
  /** Minnow store durability. Every Minnow cell names it. */
  readonly durability?: "strict" | "relaxed";
  /** Minnow database options (for example the execution memory budget). */
  readonly minnowOptions?: {
    readonly executionMemoryBudgetBytes?: number | null;
    readonly autoCompact?: boolean;
    readonly autoCollect?: boolean;
  };
  /** Minnow worker connections to open in this page; one leads, the rest follow. */
  readonly connections?: number;
  readonly duckdbMemoryLimitBytes?: number;
  readonly sqlitePragmas?: SqlitePragmaSet;
  /** Delete the database first, so a load starts empty. */
  readonly fresh?: boolean;
}

export interface OpenResult {
  readonly openMs: number;
  /** What was actually in effect, read back from the engine: versions, pragmas, settings. */
  readonly config: Record<string, unknown>;
}

export interface QueryRequest {
  readonly sql: string;
  readonly ordered: boolean;
  readonly kind?: "export";
  readonly check?: "count";
  readonly reps: number;
  /** Digest the first run's result (outside its timer). */
  readonly digest: boolean;
  readonly timeoutMs: number;
}

export interface QueryResult {
  readonly times: number[];
  readonly rows?: number;
  readonly columns?: number;
  readonly hash?: string;
  readonly dump?: Array<Array<number | string>>;
}

export interface LoadResult {
  readonly genMs: number;
  readonly convertMs: number;
  readonly engineMs: number;
  readonly totalMs: number;
  readonly rows: Record<string, number>;
}

/** A timed point operation: batch means (ms per operation) plus every operation's own latency. */
export interface PointSamples {
  /** One entry per batch: the batch's wall time divided by its operations. */
  readonly samples: number[];
  /** Individual operation latencies, where each is long enough to time on its own. */
  readonly latencies?: number[];
  /** Operations per sample. */
  readonly operations: number;
  readonly note?: string;
}
