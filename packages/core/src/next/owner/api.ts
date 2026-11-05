/**
 * The public client API of the new core: what an app, the Kysely dialect, the React hooks and the
 * devtools panel call. Every context uses it, the owner's own tab included; a context that is not
 * the owner loads this and the protocol only (no engine code).
 *
 * ## Results are columnar first
 * - A `Result` holds the transferred frames (frame.ts) as they arrived. `column()` and
 *   `columnChunks()` give typed views over those buffers without copying (except `column()` on a
 *   multi-frame result, which concatenates once and caches).
 * - Row objects, tuples and decoded JS values are built only when asked for, and cached.
 * - JS values follow DECISIONS.md: BIGINT → `bigint` (D2); COUNT and SUM(int2/int4) → `number`
 *   (`WireColumnMeta.render`); NUMERIC, DATE, TIME, TIMESTAMP(TZ) (ISO, six fraction digits, D1),
 *   INTERVAL, UUID, JSON/JSONB → `string`; REAL/DOUBLE → `number`; BYTEA → `Uint8Array`; arrays
 *   → JS arrays of these.
 *
 * ## Cancellation
 * Every call takes `signal`. Aborting sends `cancel`; the promise settles with the request's
 * terminal reply: rejected with `signal.reason` if it stopped with no effect, or resolved if it
 * finished first (a write then committed).
 *
 * ## Connection changes are mostly invisible
 * - When the owner changes, the client reconnects and resends unanswered requests; the ledger
 *   makes writes exactly-once. Prepared statements are re-prepared and subscriptions resubscribed
 *   transparently.
 * - Open transactions and cursors cannot move: they fail with `SessionLostError` (nothing took
 *   effect). A retried write whose RETURNING rows were lost fails with
 *   `CommittedWithoutResultError` (it did commit).
 * - A frozen owner fails reads with `OwnerUnresponsiveError` after `unresponsiveAfterMs`, and
 *   writes in flight with `OutcomeUnknownError { reason: "unresponsive" }`: they may still commit.
 */
import type { PhysicalType } from "../types.js";
import type { Int128Values, Int64Values, WideCoefficients } from "../vector/vector.js";
import type { Broker } from "./election.js";
import type { WireColumnMeta, WireValue } from "./protocol.js";

/** A value as row access returns it. */
export type Value = null | boolean | number | bigint | string | Uint8Array | readonly Value[];
/** A value a parameter or input row may hold (protocol.ts `WireValue`). */
export type Param = WireValue;

export type Row = Record<string, Value>;

/** A validity bitmap: bit `i % 8` of byte `i >> 3`, set = valid (the `Uint32Array` words' bytes). */
export type ValidityBytes = Uint8Array;

/**
 * A zero-copy typed view of one column (DECISIONS.md D1's descriptor). Never a bare typed-array
 * union: `kind` and `repr` say how to read the values. `validity` is null when every row is valid.
 * Views alias the result's buffers: they stay valid as long as the `Result` is reachable.
 */
export type ColumnView =
  | { readonly kind: "bool"; readonly values: Uint8Array; readonly validity: ValidityBytes | null }
  | {
      readonly kind: "int32";
      readonly bits: 16 | 32;
      readonly values: Int32Array;
      readonly validity: ValidityBytes | null;
    }
  | ({ readonly kind: "int64"; readonly validity: ValidityBytes | null } & Int64Values)
  | {
      readonly kind: "float64";
      readonly values: Float64Array;
      readonly validity: ValidityBytes | null;
    }
  | {
      readonly kind: "float32";
      readonly values: Float32Array;
      readonly validity: ValidityBytes | null;
    }
  | {
      readonly kind: "decimal";
      /** value = coefficient × 10^−scale; `scale` null means per-row `scales`. */
      readonly scale: number | null;
      readonly scales: Uint16Array | null;
      readonly coefficients: Int64Values | Int128Values | WideCoefficients;
      /** vector/vector.ts `DecimalSpecial` per row, or null when all are finite. */
      readonly specials: Uint8Array | null;
      readonly validity: ValidityBytes | null;
    }
  | {
      readonly kind: "date";
      readonly unit: "days";
      readonly epoch: "2000-01-01";
      readonly values: Int32Array;
      readonly validity: ValidityBytes | null;
    }
  | ({
      readonly kind: "time";
      readonly unit: "microseconds";
      readonly validity: ValidityBytes | null;
    } & Int64Values)
  | ({
      readonly kind: "timestamp";
      readonly zoned: boolean;
      readonly unit: "microseconds";
      readonly epoch: "2000-01-01T00:00:00Z";
      readonly validity: ValidityBytes | null;
    } & Int64Values)
  | {
      readonly kind: "interval";
      readonly months: Int32Array;
      readonly days: Int32Array;
      readonly micros: Int64Values;
      readonly validity: ValidityBytes | null;
    }
  | {
      /** UTF-8 text-like values: TEXT, JSON/JSONB (canonical text), enum labels, BYTEA, arrays (C9 binary). */
      readonly kind: "text" | "json" | "enum" | "bytes" | "array";
      readonly encoding: "plain";
      /** Value i is `bytes[offsets[i] … offsets[i + 1])`. */
      readonly bytes: Uint8Array;
      readonly offsets: Int32Array;
      readonly validity: ValidityBytes | null;
    }
  | {
      readonly kind: "text" | "json" | "enum" | "bytes" | "array";
      readonly encoding: "dictionary";
      readonly codes: Int32Array;
      /**
       * The stream's dictionary as of this chunk (frame.ts `StreamDictionaries`): a later chunk of
       * the same result may see more entries, never different ones.
       */
      readonly dictionary: { readonly bytes: Uint8Array; readonly offsets: Int32Array };
      readonly validity: ValidityBytes | null;
    }
  | { readonly kind: "uuid"; readonly bytes: Uint8Array; readonly validity: ValidityBytes | null };

export interface ResultColumn {
  readonly name: string;
  /** PostgreSQL's type name: `integer`, `numeric(10,2)`, `timestamp with time zone`, … */
  readonly sqlType: string;
  readonly nullable: boolean;
  readonly type: PhysicalType;
}

/** The outcome of one statement. `TRow` types `rows()`; it is not checked at run time. */
export interface Result<TRow = Row> {
  /** PostgreSQL command tag word: `SELECT`, `INSERT`, `UPDATE`, `CREATE TABLE`, … */
  readonly command: string;
  readonly columns: readonly ResultColumn[];
  /** Rows held (SELECT or RETURNING). */
  readonly rowCount: number;
  /** Rows written by INSERT/UPDATE/DELETE/MERGE, else null. */
  readonly affectedRows: number | null;
  /** This answer came from the request ledger after a reconnect: the write had committed. */
  readonly replayed: boolean;
  /** One contiguous view (concatenated once if the result has several frames). */
  column(column: string | number): ColumnView;
  /** One view per frame, no copy. */
  columnChunks(column: string | number): readonly ColumnView[];
  /** The column's values decoded to JS values. */
  values(column: string | number): Value[];
  /** Row objects. Duplicate column names: the last one wins; use `tuples()` instead. */
  rows(): TRow[];
  row(index: number): TRow;
  tuples(): Value[][];
  [Symbol.iterator](): Iterator<TRow>;
}

export interface CallOptions {
  readonly signal?: AbortSignal;
}

export interface CursorOptions extends CallOptions {
  /** At most this many rows per yielded result; defaults to the byte-bounded batch. */
  readonly frameRows?: number;
  /**
   * Result bytes the owner may send ahead of the consumer (the stream's window). Defaults to
   * `DEFAULT_RESULT_WINDOW_BYTES` (two ~1 MiB frames). A frame is released to the owner when the
   * iterator hands it out, so this bounds both the owner's charge and this client's backlog.
   */
  readonly prefetchBytes?: number;
}

/**
 * Rows streamed one frame at a time, with backpressure: the owner sends a frame only when the
 * consumer's window has room for it. Holds a snapshot pin until it ends; `return()` (a `break` in
 * `for await`) or `close()` releases it.
 */
export interface Cursor<TRow = Row> extends AsyncIterableIterator<Result<TRow>> {
  readonly columns: Promise<readonly ResultColumn[]>;
  close(): Promise<void>;
}

/** SQL execution. A transaction and the client both implement it. */
export interface SqlExecutor {
  /**
   * Runs one statement. Any statement: SELECT, DML (with RETURNING), DDL, and on the client's
   * default session also `BEGIN`/`COMMIT`/`ROLLBACK` (as one PostgreSQL connection would).
   */
  query<TRow = Row>(
    sql: string,
    params?: readonly Param[],
    options?: CallOptions,
  ): Promise<Result<TRow>>;
  cursor<TRow = Row>(sql: string, params?: readonly Param[], options?: CursorOptions): Cursor<TRow>;
  /** Plans once on the owner; executions send an id and parameters only. */
  prepare(sql: string, options?: CallOptions): Promise<PreparedStatement>;
  insert(table: string, rows: WriteInput, options?: InsertOptions): Promise<Result>;
  upsert(table: string, rows: WriteInput, options?: WriteOptions): Promise<Result>;
  /** Rows carry the primary key plus the columns to set. */
  update(table: string, rows: WriteInput, options?: WriteOptions): Promise<Result>;
  /** Rows carry the primary key columns only. */
  delete(table: string, keys: WriteInput, options?: WriteOptions): Promise<Result>;
}

export interface PreparedStatement {
  readonly sql: string;
  readonly columns: readonly ResultColumn[];
  readonly parameters: readonly PhysicalType[];
  query<TRow = Row>(params?: readonly Param[], options?: CallOptions): Promise<Result<TRow>>;
  cursor<TRow = Row>(params?: readonly Param[], options?: CursorOptions): Cursor<TRow>;
  /** Frees the plan on the owner. Idempotent. */
  close(): void;
}

/** Rows to write: objects, or columns of equal length. */
export type WriteInput = ReadonlyArray<Readonly<Record<string, Param>>> | ColumnarInput;

/**
 * Columns by name. Typed arrays are sent without encoding, plain arrays are encoded by the
 * client. Input is structured-cloned, never transferred: the caller's arrays stay usable, and the
 * client keeps the clone source until the reply so it can resend the write after a reconnect
 * (ruling R9). Do not mutate them until the call settles.
 */
export interface ColumnarInput {
  readonly columns: Readonly<
    Record<
      string,
      | readonly Param[]
      | Float64Array
      | Int32Array
      | BigInt64Array
      | {
          readonly values: Float64Array | Int32Array | BigInt64Array;
          readonly validity: ValidityBytes;
        }
    >
  >;
}

export interface WriteOptions extends CallOptions {
  /** Columns to return per written row; the result holds them. */
  readonly returning?: readonly string[];
}

export interface InsertOptions extends WriteOptions {
  /** `ON CONFLICT DO NOTHING`. */
  readonly ignoreConflicts?: boolean;
}

export interface TransactionOptions extends CallOptions {
  readonly readOnly?: boolean;
}

/** Statements of one transaction. Ends when the callback settles; using it later throws. */
export type Transaction = SqlExecutor;

/** A query kept current: re-run on the owner after each commit that may change it. */
export interface LiveQuery<TRow = Row> {
  /** The latest result, or null before the first. */
  readonly current: Result<TRow> | null;
  /** Called with each new result, in order. Returns an unsubscribe function. */
  subscribe(listener: (result: Result<TRow>) => void, onError?: (error: Error) => void): () => void;
  close(): void;
}

export type ClientStatus =
  | "connecting"
  | "ready"
  /** The owner changed or stepped down; requests are queued and will be resent. */
  | "reconnecting"
  /** No answer within `unresponsiveAfterMs`; calls fail with `OwnerUnresponsiveError`. */
  | "unresponsive"
  /** A newer build owns the database; see `whenSuperseded`. */
  | "superseded"
  | "closed";

export interface ClientOptions {
  /** The database name: one owner per name per origin. */
  readonly database: string;
  /**
   * In a page: starts this page's owner worker when it wins the election
   * (`@minnowdb/core/owner-worker`). Without it the page never hosts.
   */
  readonly ownerWorker?: () => Worker;
  /**
   * In a dedicated worker: run the owner in this worker when it wins the election (ruling R9).
   * The page that created the worker should relay its lifecycle (`relayLifecycle(worker)`, which
   * posts election.ts `LifecycleRelay` messages); without it this worker keeps the store until it
   * ends, even while its page sits in bfcache.
   */
  readonly hostInPlace?: boolean;
  /** Defaults to the SharedWorker broker shipped with the package. */
  readonly broker?: Broker;
  /** Owner settings, applied when this page hosts. */
  readonly durability?: "strict" | "relaxed";
  /** The one optional memory cap of the owner, in bytes. */
  readonly memoryLimitBytes?: number;
  /** Silence before calls fail with `OwnerUnresponsiveError`. */
  readonly unresponsiveAfterMs?: number;
  /** As in 0.19: reload the page (at most once a minute) or only report. */
  readonly whenSuperseded?: "reload" | "prompt";
  readonly onSuperseded?: (event: { readonly reloading: boolean }) => void;
  readonly onStatus?: (status: ClientStatus) => void;
  /** Owner failures that belong to no call (protocol.ts `Diagnostic`). Default: console.error. */
  readonly onOwnerError?: (event: {
    readonly kind: string;
    readonly context: string;
    readonly error: Error;
  }) => void;
}

export interface MinnowClient extends SqlExecutor {
  readonly status: ClientStatus;
  /** Resolves when connected to an owner (electing this page if needed). */
  ready(): Promise<void>;
  /** Commits when `body` resolves, rolls back when it rejects. Pins one snapshot throughout. */
  transaction<T>(body: (tx: Transaction) => Promise<T>, options?: TransactionOptions): Promise<T>;
  live<TRow = Row>(sql: string, params?: readonly Param[]): LiveQuery<TRow>;
  /** The catalog (tables, columns, indexes, views) for Kysely introspection and devtools. */
  catalog(options?: CallOptions): Promise<unknown>;
  /** Waits for in-flight calls, then says `bye`; steps this page down if it hosts. */
  close(): Promise<void>;
}

/**
 * Relays the page's lifecycle events to a dedicated worker created with `hostInPlace`
 * (election.ts `LifecycleRelay`). Returns a function that stops relaying. Implemented in Phase 2.
 */
export type RelayLifecycle = (worker: Worker) => () => void;

/** Column metadata as the client keeps it (protocol.ts `WireColumnMeta` → `ResultColumn`). */
export const resultColumn = (meta: WireColumnMeta): ResultColumn => ({
  name: meta.name,
  sqlType: meta.sqlType,
  nullable: meta.nullable,
  type: meta.type,
});
