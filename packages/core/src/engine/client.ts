import {
  CompactionJobConflictError,
  GarbageCollectionJobConflictError,
  LeaseConflictError,
  SnapshotManifestMissingError,
  TableRecordConflictError,
  TempOwnerConflictError,
  TransactionRecordConflictError,
  UniqueKeyConflictError,
  WriteConflictError,
  type CompactionJobRecord,
  type GarbageCollectionJobRecord,
  type SnapshotLoadProgress,
} from "../storage/index.js";
import {
  parseRpcResponse,
  protocolVersion,
  type SerializedError,
} from "../worker-protocol/index.js";
import { toColumnarBatch, type BatchRow, type InsertBatchInput } from "./batch.js";
import type { Catalog } from "./catalog.js";
import type {
  BatchValue,
  BufferPoolStats,
  StagedWriteResult,
  BufferedFlushResult,
  BufferedWriterOptions,
  CancelCompactionJobResult,
  CollectGarbageOptions,
  CollectGarbageStepOptions,
  CompactTableOptions,
  CompactTableResult,
  CompactTableStepOptions,
  CompactionJobProgress,
  CreateTableInput,
  DatabaseRow,
  MigrateOptions,
  DeleteBatchInput,
  DeleteBatchResult,
  ExecuteResult,
  GarbageCollectionProgress,
  GarbageCollectionResult,
  InsertBatchResult,
  QueryOptions,
  QuerySpillCleanupOptions,
  QuerySpillCleanupResult,
  ReadTableOptions,
  RunStatementOptions,
  SnapshotExportOptions,
  SnapshotImportOptions,
  TableDefinition,
  UpdateBatchInput,
  UpdateBatchResult,
  UpsertBatchResult,
  VisibleSegment,
} from "./database.js";
import {
  CompactionJobCancelledError,
  CompactionMemoryBudgetError,
  CompactionWriteAmplificationError,
  MissingKeyError,
  SqlCompileError,
  UniqueConstraintError,
} from "./errors.js";
import type { LiveQueryInput, LiveQueryStats, LiveQuerySubscribeOptions } from "./live.js";
import { QueryMemoryBudgetError } from "./memory.js";
import type { CompiledQuery, CompiledStatement, QueryResult, QueryValue } from "./query.js";
import { decodeQueryResult } from "./result-wire.js";
import type { AnyTable, SchemaDefinition } from "./schema.js";
import { serializeSchema, type WireMigrationStep } from "./schema-wire.js";
import type { DatabaseInitPayload, StoreDescriptor, WireDatabaseOptions } from "./worker-host.js";

/**
 * Main-thread async proxy of the full database API, talking to a worker that runs
 * `@minnowdb/core/worker` (or a custom entry built on exposeDatabase()). Construction
 * sends the init frame immediately; because the channel is ordered, calls may be issued without
 * awaiting ready(). Everything here stays deliberately light — the heavy engine modules are
 * imported as types only, so a main-thread bundle carries the proxy, not the executor.
 *
 * Two deliberate deviations from the in-worker API, both forced by the boundary:
 * - Every sync member becomes async (`PreparedQuery.execute()`, stats getters, `close()`).
 * - `migrate()` takes the same schema DSL but returns wire-format steps, and typed errors are
 *   rehydrated copies — `instanceof` works, stacks point at the worker.
 */

/** The slice of Worker (or MessagePort) the client needs. */
export interface ClientTransport {
  postMessage(message: unknown): void;
  addEventListener(type: "message", listener: (event: MessageEvent<unknown>) => void): void;
  addEventListener(type: "error" | "messageerror", listener: () => void): void;
  terminate?(): void;
}

export interface MinnowDatabaseClientOptions {
  /**
   * Defaults to `{ kind: "indexeddb", name: "minnow" }`. The `opfs` kind selects
   * `OpfsBlockStore`, which needs the worker to be a dedicated worker (it always is with
   * `@minnowdb/core/worker`) and OPFS to exist — notably absent in Safari private browsing.
   */
  store?: StoreDescriptor;
  /** Cloneable database options applied when the worker constructs the database. */
  databaseOptions?: WireDatabaseOptions;
}

export interface ClientLiveQueryOptions {
  /** BroadcastChannel name the worker uses to exchange cross-tab commit hints. */
  channelName?: string;
  pollIntervalMs?: number;
}

export interface CloseClientOptions {
  /** Also terminate the worker after disposing; only meaningful when the transport can. */
  terminateWorker?: boolean;
}

export interface ClientMigrationResult {
  createdTables: string[];
  alteredTables: string[];
  droppedTables: string[];
  replacedViews: string[];
  droppedViews: string[];
  steps: WireMigrationStep[];
}

interface EventRoute {
  onChange?: (result: QueryResult) => void;
  onError?: (error: unknown) => void;
  onComplete?: () => void;
  /** Payload shape is the handle's own; only snapshot loads emit these today. */
  onProgress?: (progress: unknown) => void;
}

/**
 * Bytes per slice when a snapshot crosses the channel. Large enough that a hundred-megabyte
 * database is tens of round trips, small enough that no single structured clone is long enough
 * to be felt as a dropped frame.
 */
const SNAPSHOT_CHUNK_BYTES = 4 * 1024 * 1024;

interface PendingCall {
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
}

const errorRegistry = new Map<string, new (...args: never[]) => Error>(
  [
    UniqueConstraintError,
    MissingKeyError,
    CompactionMemoryBudgetError,
    CompactionWriteAmplificationError,
    CompactionJobCancelledError,
    SqlCompileError,
    QueryMemoryBudgetError,
    WriteConflictError,
    UniqueKeyConflictError,
    TableRecordConflictError,
    CompactionJobConflictError,
    GarbageCollectionJobConflictError,
    SnapshotManifestMissingError,
    TransactionRecordConflictError,
    LeaseConflictError,
    TempOwnerConflictError,
  ].map((constructor) => [constructor.name, constructor]),
);

function rehydrateError(serialized: SerializedError): Error {
  const constructor = errorRegistry.get(serialized.name);
  const error: Error =
    constructor === undefined
      ? new Error(serialized.message)
      : (Object.create(constructor.prototype as object) as Error);
  Object.defineProperty(error, "message", {
    value: serialized.message,
    writable: true,
    configurable: true,
  });
  Object.defineProperty(error, "name", {
    value: serialized.name,
    writable: true,
    configurable: true,
  });
  if (serialized.stack !== undefined) {
    Object.defineProperty(error, "stack", {
      value: serialized.stack,
      writable: true,
      configurable: true,
    });
  }
  if (serialized.props !== undefined) Object.assign(error, serialized.props);
  return error;
}

export class MinnowDatabaseClient {
  readonly #transport: ClientTransport;
  readonly #pending = new Map<string, PendingCall>();
  readonly #events = new Map<string, EventRoute>();
  readonly #ready: Promise<void>;
  #fatal: Error | undefined;
  #closed = false;
  #onVisibilityChange: (() => void) | undefined;

  constructor(transport: ClientTransport, options: MinnowDatabaseClientOptions = {}) {
    this.#transport = transport;
    transport.addEventListener("message", (event: MessageEvent<unknown>) => {
      this.#receive(event.data);
    });
    transport.addEventListener("error", () => {
      this.#fail(new Error("The database worker failed; see the worker's own error output"));
    });
    transport.addEventListener("messageerror", () => {
      this.#fail(new Error("A database worker message could not be deserialized"));
    });
    const payload: DatabaseInitPayload = {
      store: options.store ?? { kind: "indexeddb", name: "minnow" },
      ...(options.databaseOptions === undefined ? {} : { options: options.databaseOptions }),
    };
    this.#ready = this.#post("rpc-init", null, "init", [payload]).then(() => undefined);
    // Callers may rely on call ordering instead of awaiting ready(); keep its rejection observed.
    this.#ready.catch(() => undefined);

    // Report page visibility so a store that cares (the OPFS store's leadership preference)
    // can follow the tab the user is looking at. Best-effort: a worker-side host that predates
    // the frame rejects it, and nothing here depends on the answer.
    if (typeof document !== "undefined" && typeof document.addEventListener === "function") {
      const report = (): void => {
        if (this.#closed) return;
        this.#post("rpc-call", null, "setVisibility", [
          document.visibilityState === "visible",
        ]).catch(() => undefined);
      };
      this.#onVisibilityChange = report;
      document.addEventListener("visibilitychange", report);
      report();
    }
  }

  /** Resolves once the worker has opened the store and constructed the database. */
  async ready(): Promise<void> {
    return this.#ready;
  }

  // --- Catalog and writes -----------------------------------------------------------------------

  async createTable(input: CreateTableInput): Promise<void> {
    await this.#call("createTable", [input]);
  }

  /** The published catalog; see `MinnowDatabase.introspect()`. */
  async introspect(): Promise<Catalog> {
    return (await this.#call("introspect", [])) as Catalog;
  }

  async listTables(): Promise<TableDefinition[]> {
    return (await this.#call("listTables", [])) as TableDefinition[];
  }

  async migrate(
    definition: SchemaDefinition<readonly AnyTable[]>,
    options: MigrateOptions = {},
  ): Promise<ClientMigrationResult> {
    return (await this.#call("migrate", [
      serializeSchema(definition),
      options,
    ])) as ClientMigrationResult;
  }

  async insertBatch(tableName: string, input: InsertBatchInput): Promise<InsertBatchResult> {
    // Pivoted here rather than in the worker: the columnar form is the cheaper structured clone,
    // and it keeps the worker protocol's payload shape the same whichever form the caller used.
    const batch = toColumnarBatch(input);
    return (await this.#call("insertBatch", [tableName, batch])) as InsertBatchResult;
  }

  async insert(tableName: string, row: BatchRow): Promise<InsertBatchResult> {
    return (await this.#call("insert", [tableName, row])) as InsertBatchResult;
  }

  async upsertBatch(tableName: string, input: InsertBatchInput): Promise<UpsertBatchResult> {
    const batch = toColumnarBatch(input);
    return (await this.#call("upsertBatch", [tableName, batch])) as UpsertBatchResult;
  }

  async upsert(tableName: string, row: BatchRow): Promise<UpsertBatchResult> {
    return (await this.#call("upsert", [tableName, row])) as UpsertBatchResult;
  }

  async updateBatch(tableName: string, input: UpdateBatchInput): Promise<UpdateBatchResult> {
    return (await this.#call("updateBatch", [tableName, input])) as UpdateBatchResult;
  }

  async update(
    tableName: string,
    key: Exclude<BatchValue, null>,
    changes: Readonly<Record<string, BatchValue>>,
  ): Promise<UpdateBatchResult> {
    return (await this.#call("update", [tableName, key, changes])) as UpdateBatchResult;
  }

  async deleteBatch(tableName: string, input: DeleteBatchInput): Promise<DeleteBatchResult> {
    return (await this.#call("deleteBatch", [tableName, input])) as DeleteBatchResult;
  }

  bufferedWriter(tableName: string, options: BufferedWriterOptions = {}): ClientBufferedWriter {
    const handleId = crypto.randomUUID();
    const { onError, ...wireOptions } = options;
    this.#events.set(handleId, {
      ...(onError === undefined ? {} : { onError }),
    });
    const created = this.#call("bufferedWriter", [handleId, tableName, wireOptions]);
    return new ClientBufferedWriter(this, handleId, created);
  }

  // --- Reads ------------------------------------------------------------------------------------

  async readTable(
    tableName: string,
    versionOrOptions?: number | ReadTableOptions,
  ): Promise<DatabaseRow[]> {
    return decodeQueryResult(
      await this.#call(
        "readTable",
        versionOrOptions === undefined ? [tableName] : [tableName, versionOrOptions],
      ),
    ).rows;
  }

  /**
   * Results cross the channel as one array per column (typed arrays for numbers, booleans, and
   * datetimes) and are rebuilt into row objects here; see `result-wire.ts`.
   */
  async query(sql: string, options?: QueryOptions): Promise<QueryResult> {
    return decodeQueryResult(
      await this.#call("query", options === undefined ? [sql] : [sql, options]),
    );
  }

  async run<TRow>(query: {
    kind: "typed-query";
    plan: CompiledQuery;
    __row?: TRow;
  }): Promise<TRow[]> {
    return decodeQueryResult(await this.#call("run", [query])).rows as TRow[];
  }

  async explain(sql: string): Promise<string> {
    return (await this.#call("explain", [sql])) as string;
  }

  async execute(sql: string, params?: readonly QueryValue[]): Promise<ExecuteResult> {
    return (await this.#call(
      "execute",
      params === undefined ? [sql] : [sql, params],
    )) as ExecuteResult;
  }

  /** Executes a compiled statement from the typed mutation builders in the worker. */
  async runStatement(
    statement: CompiledStatement,
    options?: RunStatementOptions,
  ): Promise<ExecuteResult> {
    return (await this.#call(
      "runStatement",
      options === undefined ? [statement] : [statement, options],
    )) as ExecuteResult;
  }

  /**
   * Runs the callback against one pinned manifest version, exactly like the in-worker
   * `MinnowDatabase.snapshot()`: the worker holds the version's reader lease until the
   * callback settles, and every session query crosses the channel pinned to that version.
   */
  async snapshot<T>(action: (session: ClientSnapshotSession) => Promise<T>): Promise<T> {
    const opened = (await this.#call("snapshotOpen", [])) as {
      handleId: string;
      version: number | null;
    };
    try {
      const session: ClientSnapshotSession = {
        version: opened.version,
        query: (sql: string, options: QueryOptions = {}) =>
          this.query(sql, {
            ...options,
            ...(opened.version === null ? {} : { version: opened.version }),
          }),
      };
      return await action(session);
    } finally {
      await this._invoke(opened.handleId, "close", []);
    }
  }

  /**
   * Runs the callback against one worker-side write transaction, exactly like the in-worker
   * `MinnowDatabase.write()`: every staged mutation crosses the channel into the shared
   * transaction and publishes as one atomic commit when the callback returns; an error
   * aborts the scope with nothing published.
   */
  async write<T>(
    action: (session: ClientWriteSession) => Promise<T>,
  ): Promise<{ result: T; version: number | null }> {
    const opened = (await this.#call("writeOpen", [])) as { handleId: string };
    const stage = (
      op: "insertBatch" | "upsertBatch" | "updateBatch" | "deleteBatch",
      tableName: string,
      input: unknown,
    ): Promise<StagedWriteResult> =>
      this._invoke(opened.handleId, "stage", [op, tableName, input]) as Promise<StagedWriteResult>;
    const session: ClientWriteSession = {
      query: async (sql, options) =>
        decodeQueryResult(await this._invoke(opened.handleId, "query", [sql, options])),
      execute: (sql, params) =>
        this._invoke(
          opened.handleId,
          "execute",
          params === undefined ? [sql] : [sql, params],
        ) as Promise<ExecuteResult>,
      insertBatch: (tableName, input) => stage("insertBatch", tableName, input),
      upsertBatch: (tableName, input) => stage("upsertBatch", tableName, input),
      updateBatch: (tableName, input) => stage("updateBatch", tableName, input),
      deleteBatch: (tableName, input) => stage("deleteBatch", tableName, input),
    };
    try {
      const result = await action(session);
      const committed = (await this._invoke(opened.handleId, "commit", [])) as {
        version: number | null;
      };
      return { result, version: committed.version };
    } catch (error) {
      await this._invoke(opened.handleId, "abort", []).catch(() => undefined);
      throw error;
    }
  }

  /**
   * Copies the database out as a portable snapshot file, exactly like the in-worker
   * `MinnowDatabase.exportSnapshot()`. The worker encodes it and hands it back in slices, so the
   * main thread copies a few megabytes at a time instead of stalling on one clone of the whole
   * database; `onProgress` reports each slice as it lands.
   */
  async exportSnapshot(options: SnapshotExportOptions = {}): Promise<Uint8Array> {
    options.onProgress?.({ phase: "reading", transferredBytes: 0, totalBytes: 0 });
    const opened = (await this.#call("exportSnapshotOpen", [])) as {
      handleId: string;
      byteLength: number;
    };
    const totalBytes = opened.byteLength;
    const bytes = new Uint8Array(totalBytes);
    try {
      let offset = 0;
      while (offset < totalBytes) {
        const chunk = (await this._invoke(opened.handleId, "read", [
          offset,
          Math.min(SNAPSHOT_CHUNK_BYTES, totalBytes - offset),
        ])) as Uint8Array;
        // A slice that came back empty would loop forever; the export is over either way.
        if (chunk.byteLength === 0) throw new Error("Snapshot transfer returned no bytes");
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
        options.onProgress?.({ phase: "transfer", transferredBytes: offset, totalBytes });
      }
    } finally {
      await this._invoke(opened.handleId, "close", []).catch(() => undefined);
    }
    options.onProgress?.({ phase: "done", transferredBytes: totalBytes, totalBytes });
    return bytes;
  }

  /**
   * Loads a snapshot file into the worker's store, which must be empty. The bytes go over in the
   * same slices the export comes back in, and the worker's load progress arrives as events, so a
   * multi-megabyte restore can be shown moving rather than as a frozen tab.
   */
  async importSnapshot(bytes: Uint8Array, options: SnapshotImportOptions = {}): Promise<void> {
    const handleId = crypto.randomUUID();
    const { onProgress } = options;
    this.#events.set(handleId, {
      ...(onProgress === undefined
        ? {}
        : {
            onProgress: (progress: unknown) => {
              onProgress(progress as SnapshotLoadProgress);
            },
          }),
    });
    try {
      await this.#call("importSnapshotOpen", [handleId]);
      for (let offset = 0; offset < bytes.byteLength; offset += SNAPSHOT_CHUNK_BYTES) {
        // Copied, not viewed: structured clone copies the buffer behind a typed array, so a
        // subarray would post the whole file on every write.
        await this._invoke(handleId, "write", [bytes.slice(offset, offset + SNAPSHOT_CHUNK_BYTES)]);
      }
      await this._invoke(handleId, "finish", []);
    } catch (error) {
      // The handle survives a failed write; drop it rather than leaving the chunks in the worker.
      await this._invoke(handleId, "close", []).catch(() => undefined);
      throw error;
    } finally {
      this.#events.delete(handleId);
    }
  }

  /** The worker-side buffer pool's byte budget, residency, and lifetime counters. */
  async bufferPoolStats(): Promise<BufferPoolStats> {
    return (await this.#call("bufferPoolStats", [])) as BufferPoolStats;
  }

  liveQueries(options: ClientLiveQueryOptions = {}): ClientLiveQuerySet {
    const handleId = crypto.randomUUID();
    const created = this.#call("liveQueries", [handleId, options]);
    return new ClientLiveQuerySet(this, handleId, created);
  }

  // --- Maintenance ------------------------------------------------------------------------------

  async listVisibleSegments(tableName: string, version?: number): Promise<VisibleSegment[]> {
    return (await this.#call(
      "listVisibleSegments",
      version === undefined ? [tableName] : [tableName, version],
    )) as VisibleSegment[];
  }

  async cleanupQuerySpill(options?: QuerySpillCleanupOptions): Promise<QuerySpillCleanupResult> {
    return (await this.#call(
      "cleanupQuerySpill",
      options === undefined ? [] : [options],
    )) as QuerySpillCleanupResult;
  }

  async compactTable(
    tableName: string,
    options?: CompactTableOptions,
  ): Promise<CompactTableResult> {
    return (await this.#call(
      "compactTable",
      options === undefined ? [tableName] : [tableName, options],
    )) as CompactTableResult;
  }

  async compactTableStep(
    tableName: string,
    options?: CompactTableStepOptions,
  ): Promise<CompactionJobProgress> {
    return (await this.#call(
      "compactTableStep",
      options === undefined ? [tableName] : [tableName, options],
    )) as CompactionJobProgress;
  }

  async resumeCompactionJob(
    jobId: string,
    options?: { maxBlocks?: number },
  ): Promise<CompactionJobProgress> {
    return (await this.#call(
      "resumeCompactionJob",
      options === undefined ? [jobId] : [jobId, options],
    )) as CompactionJobProgress;
  }

  async listCompactionJobs(tableName?: string): Promise<CompactionJobRecord[]> {
    return (await this.#call(
      "listCompactionJobs",
      tableName === undefined ? [] : [tableName],
    )) as CompactionJobRecord[];
  }

  async cancelCompactionJob(jobId: string): Promise<CancelCompactionJobResult> {
    return (await this.#call("cancelCompactionJob", [jobId])) as CancelCompactionJobResult;
  }

  async collectGarbage(options?: CollectGarbageOptions): Promise<GarbageCollectionResult> {
    return (await this.#call(
      "collectGarbage",
      options === undefined ? [] : [options],
    )) as GarbageCollectionResult;
  }

  async collectGarbageStep(
    options?: CollectGarbageStepOptions,
  ): Promise<GarbageCollectionProgress> {
    return (await this.#call(
      "collectGarbageStep",
      options === undefined ? [] : [options],
    )) as GarbageCollectionProgress;
  }

  async resumeGarbageCollectionJob(
    jobId: string,
    options?: { maxItems?: number },
  ): Promise<GarbageCollectionProgress> {
    return (await this.#call(
      "resumeGarbageCollectionJob",
      options === undefined ? [jobId] : [jobId, options],
    )) as GarbageCollectionProgress;
  }

  async listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    return (await this.#call("listGarbageCollectionJobs", [])) as GarbageCollectionJobRecord[];
  }

  // --- Lifecycle --------------------------------------------------------------------------------

  /** Disposes every worker-side handle, closes the store, and optionally terminates the worker. */
  async close(options: CloseClientOptions = {}): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#onVisibilityChange !== undefined && typeof document !== "undefined") {
      document.removeEventListener("visibilitychange", this.#onVisibilityChange);
    }
    try {
      await this.#post("rpc-call", null, "dispose", []);
    } finally {
      this.#events.clear();
      if (options.terminateWorker === true) this.#transport.terminate?.();
    }
  }

  // --- Channel internals ------------------------------------------------------------------------

  /** @internal */
  async _invoke(handleId: string, method: string, args: unknown[]): Promise<unknown> {
    return this.#post("rpc-call", handleId, method, args);
  }

  /** @internal */
  _routeEvents(handleId: string, route: EventRoute): void {
    this.#events.set(handleId, route);
  }

  /** @internal */
  _unrouteEvents(handleId: string): void {
    this.#events.delete(handleId);
  }

  async #call(method: string, args: unknown[]): Promise<unknown> {
    if (this.#closed) throw new Error("Database client is closed");
    return this.#post("rpc-call", null, method, args);
  }

  async #post(
    kind: "rpc-init" | "rpc-call",
    handleId: string | null,
    method: string,
    args: unknown[],
  ): Promise<unknown> {
    if (this.#fatal !== undefined) throw this.#fatal;
    const requestId = crypto.randomUUID();
    return new Promise((resolve, reject) => {
      this.#pending.set(requestId, { resolve, reject });
      this.#transport.postMessage(
        kind === "rpc-init"
          ? { version: protocolVersion, requestId, kind, payload: args[0] }
          : { version: protocolVersion, requestId, kind, handleId, method, args },
      );
    });
  }

  #receive(message: unknown): void {
    const response = parseRpcResponse(message);
    if (response === null) return;
    if (response.kind === "rpc-event") {
      const route = this.#events.get(response.handleId);
      if (route === undefined) return;
      if (response.event === "change") route.onChange?.(decodeQueryResult(response.payload));
      else if (response.event === "error") {
        route.onError?.(rehydrateError(response.payload as SerializedError));
      } else if (response.event === "progress") route.onProgress?.(response.payload);
      else if (response.event === "complete") {
        this.#events.delete(response.handleId);
        route.onComplete?.();
      }
      return;
    }
    const pending = this.#pending.get(response.requestId);
    if (pending === undefined) return;
    this.#pending.delete(response.requestId);
    if (response.kind === "rpc-result") pending.resolve(response.result);
    else pending.reject(rehydrateError(response.error));
  }

  #fail(error: Error): void {
    this.#fatal = error;
    const pending = [...this.#pending.values()];
    this.#pending.clear();
    for (const call of pending) call.reject(error);
  }
}

/**
 * The scope handed to the client `snapshot()`: queries pinned to one manifest version,
 * consistent with each other for the lifetime of the callback.
 */
/** The scope handed to the client `write()`; mirrors the in-worker WriteSession. */
export interface ClientWriteSession {
  /** Read-your-writes: observes the pre-scope snapshot plus everything staged so far. */
  query(sql: string, options?: { params?: QueryValue[] }): Promise<QueryResult>;
  execute(sql: string, params?: readonly QueryValue[]): Promise<ExecuteResult>;
  insertBatch(tableName: string, input: InsertBatchInput): Promise<StagedWriteResult>;
  upsertBatch(tableName: string, input: InsertBatchInput): Promise<StagedWriteResult>;
  updateBatch(tableName: string, input: UpdateBatchInput): Promise<StagedWriteResult>;
  deleteBatch(tableName: string, input: DeleteBatchInput): Promise<StagedWriteResult>;
}

export interface ClientSnapshotSession {
  /** The pinned manifest version; null only on a database with no commits yet. */
  readonly version: number | null;
  query(sql: string, options?: QueryOptions): Promise<QueryResult>;
}

/**
 * Proxy of a worker-side BufferedTableWriter. The age timer runs on the worker's clock, and
 * onError fires for background flush failures exactly as in-worker — delivered as an event.
 */
export class ClientBufferedWriter {
  constructor(
    private readonly client: MinnowDatabaseClient,
    private readonly handleId: string,
    created: Promise<unknown>,
  ) {
    this.#created = created;
    // The creation result also arrives through the first chained call; keep it observed.
    created.catch(() => undefined);
  }

  readonly #created: Promise<unknown>;

  async add(row: Readonly<Record<string, BatchValue>>): Promise<BufferedFlushResult | undefined> {
    await this.#created;
    return (await this.client._invoke(this.handleId, "add", [row])) as
      BufferedFlushResult | undefined;
  }

  async flush(): Promise<BufferedFlushResult | undefined> {
    await this.#created;
    return (await this.client._invoke(this.handleId, "flush", [])) as
      BufferedFlushResult | undefined;
  }

  /** Fire-and-forget: flush failures surface through onError, matching the in-worker contract. */
  requestFlush(): void {
    void this.#created
      .then(() => this.client._invoke(this.handleId, "requestFlush", []))
      .catch(() => undefined);
  }

  async stats(): Promise<{ pendingRowCount: number; estimatedBytes: number }> {
    await this.#created;
    return (await this.client._invoke(this.handleId, "stats", [])) as {
      pendingRowCount: number;
      estimatedBytes: number;
    };
  }

  async discard(): Promise<number> {
    await this.#created;
    return (await this.client._invoke(this.handleId, "discard", [])) as number;
  }

  async close(): Promise<BufferedFlushResult | undefined> {
    await this.#created;
    try {
      return (await this.client._invoke(this.handleId, "close", [])) as
        BufferedFlushResult | undefined;
    } finally {
      this.client._unrouteEvents(this.handleId);
    }
  }
}

/** Proxy of a worker-side LiveQuerySet; onChange/onError arrive as events from the worker. */
export class ClientLiveQuerySet {
  readonly #subscriptionIds = new Set<string>();

  constructor(
    private readonly client: MinnowDatabaseClient,
    private readonly handleId: string,
    created: Promise<unknown>,
  ) {
    this.#created = created;
    created.catch(() => undefined);
  }

  readonly #created: Promise<unknown>;

  /** Registers a query (SQL or a compiled-plan envelope) and re-runs it on relevant changes. */
  async subscribe(
    query: LiveQueryInput,
    options: LiveQuerySubscribeOptions,
  ): Promise<ClientLiveSubscription> {
    await this.#created;
    const subscriptionId = crypto.randomUUID();
    // The worker tears down its handle when the subscription completes (set closed there);
    // closing the client wrapper afterwards must not call into the vanished handle.
    const state = { completed: false };
    this.client._routeEvents(subscriptionId, {
      onChange: options.onChange.bind(options),
      ...(options.onError === undefined ? {} : { onError: options.onError.bind(options) }),
      onComplete: () => {
        state.completed = true;
        this.#subscriptionIds.delete(subscriptionId);
        options.onComplete?.();
      },
    });
    try {
      const created = (await this.client._invoke(this.handleId, "subscribe", [
        subscriptionId,
        query,
      ])) as { dependencyTableIds: string[] };
      this.#subscriptionIds.add(subscriptionId);
      return new ClientLiveSubscription(
        this.client,
        subscriptionId,
        created.dependencyTableIds,
        () => this.#subscriptionIds.delete(subscriptionId),
        state,
      );
    } catch (error) {
      this.client._unrouteEvents(subscriptionId);
      throw error;
    }
  }

  async stats(): Promise<LiveQueryStats> {
    await this.#created;
    return (await this.client._invoke(this.handleId, "stats", [])) as LiveQueryStats;
  }

  /** Hints the worker that this tab committed elsewhere (e.g. through another client). */
  notifyLocalCommit(): void {
    void this.#created
      .then(() => this.client._invoke(this.handleId, "notifyLocalCommit", []))
      .catch(() => undefined);
  }

  /** Runs one authoritative version check and selective re-execution sweep. */
  async refresh(): Promise<void> {
    await this.#created;
    await this.client._invoke(this.handleId, "refresh", []);
  }

  async close(): Promise<void> {
    await this.#created;
    try {
      await this.client._invoke(this.handleId, "close", []);
    } finally {
      for (const subscriptionId of this.#subscriptionIds) {
        this.client._unrouteEvents(subscriptionId);
      }
      this.#subscriptionIds.clear();
    }
  }
}

export class ClientLiveSubscription {
  constructor(
    private readonly client: MinnowDatabaseClient,
    private readonly handleId: string,
    readonly dependencyTableIds: readonly string[],
    private readonly onClosed: () => void,
    private readonly state: { completed: boolean } = { completed: false },
  ) {}

  async close(): Promise<void> {
    try {
      // A completed subscription no longer exists worker-side; only clean up locally.
      if (!this.state.completed) await this.client._invoke(this.handleId, "close", []);
    } finally {
      this.client._unrouteEvents(this.handleId);
      this.onClosed();
    }
  }
}
