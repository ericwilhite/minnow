import {
  BlockReadBatchTooLargeError,
  CompactionBacklogError,
  CompactionJobConflictError,
  GarbageCollectionJobConflictError,
  IndexedDbSchemaUpgradeBlockedError,
  LeaseConflictError,
  LeaseExpiredError,
  LeaseOwnerConflictError,
  PostingBuildConflictError,
  SnapshotManifestMissingError,
  SnapshotImportConflictError,
  SchemaConflictError,
  StorageResourceLimitError,
  TableInUseError,
  TableRecordConflictError,
  TempOwnerConflictError,
  TransactionRecordConflictError,
  UniqueKeyBuildConflictError,
  UniqueKeyConflictError,
  UniqueIndexCoverageError,
  WriteConflictError,
  StorageCorruptionError,
  StorageFormatVersionError,
  OpfsUncertainOutcomeError,
  type CompactionJobRecord,
  type GarbageCollectionJobRecord,
  type StorageIntegrityMode,
  type StorageIntegrityReport,
  type StorageStats,
  type InterruptedSnapshotImport,
  type InterruptedSnapshotImportAbortResult,
} from "../storage/types.js";
import { MAX_SNAPSHOT_STREAM_CHUNK_BYTES, type SnapshotLoadProgress } from "../storage/snapshot.js";
import {
  parseRpcResponse,
  MAX_DATABASE_RPC_IN_FLIGHT,
  protocolVersion,
  type RpcResponse,
  type SerializedError,
} from "../worker-protocol/index.js";
import {
  definedVectors,
  toColumnarBatch,
  type BatchRow,
  type InsertBatchInputLike,
} from "./batch.js";
import type { Catalog } from "./catalog.js";
import type {
  BatchValue,
  BufferPoolStats,
  StagedWriteResult,
  StagedUpsertResult,
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
  ExecuteOptions,
  ExecuteResult,
  GarbageCollectionProgress,
  GarbageCollectionResult,
  MaintenanceStatus,
  InsertBatchResult,
  QueryOptions,
  QueryExecutionStats,
  QueryCursorOptions,
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
  UpsertOptions,
  VisibleSegmentPage,
  VisibleSegmentPageOptions,
} from "./database.js";
import {
  CompactionJobCancelledError,
  CompactionMemoryBudgetError,
  CompactionWriteAmplificationError,
  MaintenanceBacklogError,
  DatabaseReadBacklogError,
  LiveQueryLimitError,
  MissingKeyError,
  SqlCompileError,
  UnknownTableError,
  UniqueConstraintError,
  VisibleSegmentCursorStaleError,
} from "./errors.js";
import type {
  LiveQueryDelivery,
  LiveQueryInput,
  LiveQueryInvalidation,
  LiveQueryObserveOptions,
  LiveQueryStats,
  LiveQuerySubscribeOptions,
} from "./live.js";
import { QueryMemoryBudgetError } from "./memory.js";
import type { CompiledQuery, CompiledStatement, QueryResult, QueryValue } from "./query.js";
import { decodeQueryResult } from "./result-wire.js";
import type {
  AnySchema,
  UntypedSchema,
  AnyTable,
  BatchColumnName,
  BatchDeleteInput,
  BatchInsertInput,
  BatchInsertRow,
  BatchKeyValue,
  BatchReadOptions,
  BatchReadRow,
  BatchUpdateChanges,
  BatchUpdateInput,
  BatchUpsertOptions,
  SchemaDefinition,
  TableName,
} from "./schema.js";
import { serializeSchema, type WireMigrationStep } from "./schema-wire.js";
import type { DatabaseInitPayload, StoreDescriptor, WireDatabaseOptions } from "./worker-host.js";

/**
 * Main-thread async proxy of the full database API, talking to a worker that runs
 * `@minnowdb/core/worker` (or a custom entry built on
 * `exposeDatabase()` from `@minnowdb/core/worker-host`). Construction
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
  postMessage(message: unknown, options?: { transfer: ArrayBuffer[] }): void;
  addEventListener(type: "message", listener: (event: MessageEvent<unknown>) => void): void;
  addEventListener(type: "error" | "messageerror", listener: () => void): void;
  removeEventListener?(type: "message", listener: (event: MessageEvent<unknown>) => void): void;
  removeEventListener?(type: "error" | "messageerror", listener: () => void): void;
  terminate?(): void;
}

export interface MinnowDatabaseClientOptions<TSchema extends AnySchema = UntypedSchema> {
  /**
   * The schema this database is declared against. It types every batch method by table name and
   * is what a bare `migrate()` applies. It stays on the main thread: the worker learns the
   * schema from `migrate()`, not from construction.
   */
  schema?: TSchema;
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
  onChange?: (result: QueryResult, delivery: LiveQueryDelivery) => void;
  onInvalidate?: (invalidation: LiveQueryInvalidation) => void;
  onError?: (error: unknown) => void;
  onComplete?: () => void;
  /** Payload shape is the handle's own; only snapshot loads emit these today. */
  onProgress?: (progress: unknown) => void;
  onStats?: (stats: QueryExecutionStats) => void;
}

/**
 * Bytes per slice when a snapshot crosses the channel. Large enough that a hundred-megabyte
 * database is tens of round trips, small enough that no single structured clone is long enough
 * to be felt as a dropped frame.
 */
const SNAPSHOT_CHUNK_BYTES = MAX_SNAPSHOT_STREAM_CHUNK_BYTES;
const MAX_SNAPSHOT_BYTE_ARRAY_LENGTH = 0xffffffff;

async function* clientSnapshotChunks(bytes: Uint8Array): AsyncGenerator<Uint8Array> {
  for (let offset = 0; offset < bytes.byteLength; offset += SNAPSHOT_CHUNK_BYTES) {
    yield bytes.subarray(offset, offset + SNAPSHOT_CHUNK_BYTES);
  }
}

function throwIfClientSnapshotAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted !== true) return;
  if (signal.reason instanceof Error) throw signal.reason;
  const error = new Error("Snapshot operation was aborted", { cause: signal.reason });
  error.name = "AbortError";
  throw error;
}

interface PendingCall {
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
  cleanup?: () => void;
}

interface RpcCallControls {
  signal?: AbortSignal | undefined;
  onStats?: ((stats: QueryExecutionStats) => void) | undefined;
}

const errorRegistry = new Map<string, new (...args: never[]) => Error>(
  [
    UniqueConstraintError,
    MissingKeyError,
    UnknownTableError,
    CompactionBacklogError,
    CompactionMemoryBudgetError,
    CompactionWriteAmplificationError,
    CompactionJobCancelledError,
    MaintenanceBacklogError,
    DatabaseReadBacklogError,
    LiveQueryLimitError,
    SqlCompileError,
    QueryMemoryBudgetError,
    VisibleSegmentCursorStaleError,
    BlockReadBatchTooLargeError,
    WriteConflictError,
    SchemaConflictError,
    UniqueKeyConflictError,
    UniqueIndexCoverageError,
    TableRecordConflictError,
    CompactionJobConflictError,
    GarbageCollectionJobConflictError,
    IndexedDbSchemaUpgradeBlockedError,
    SnapshotManifestMissingError,
    SnapshotImportConflictError,
    TableInUseError,
    TransactionRecordConflictError,
    LeaseConflictError,
    LeaseExpiredError,
    LeaseOwnerConflictError,
    StorageResourceLimitError,
    TempOwnerConflictError,
    UniqueKeyBuildConflictError,
    PostingBuildConflictError,
    StorageCorruptionError,
    StorageFormatVersionError,
    OpfsUncertainOutcomeError,
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

/**
 * A failure frame whose payload is not a serialized error is a protocol violation, and one that
 * must still settle the call it answers: rehydrating it blindly would throw inside the message
 * listener and leave that call pending forever.
 */
function rehydrateResponseError(payload: unknown): Error {
  const candidate = payload as Partial<SerializedError> | null;
  if (
    typeof candidate !== "object" ||
    candidate === null ||
    typeof candidate.name !== "string" ||
    typeof candidate.message !== "string"
  ) {
    return new Error("The database worker reported a failure without a readable error payload");
  }
  return rehydrateError(candidate as SerializedError);
}

export class MinnowDatabaseClient<TSchema extends AnySchema = UntypedSchema> {
  readonly #schema: TSchema | undefined;
  /** This client with the batch API erased to plain strings, for the handles it hands itself to. */
  // eslint-disable-next-line @typescript-eslint/prefer-return-this-type -- the erasure is the point
  get #erased(): MinnowDatabaseClient {
    return this;
  }
  readonly #transport: ClientTransport;
  readonly #pending = new Map<string, PendingCall>();
  readonly #events = new Map<string, EventRoute>();
  readonly #ready: Promise<void>;
  #fatal: Error | undefined;
  #closed = false;
  #onVisibilityChange: (() => void) | undefined;
  readonly #onMessage = (event: MessageEvent<unknown>): void => {
    this.#receive(event.data);
  };
  readonly #onError = (): void => {
    this.#fail(new Error("The database worker failed; see the worker's own error output"));
  };
  readonly #onMessageError = (): void => {
    this.#fail(new Error("A database worker message could not be deserialized"));
  };

  constructor(transport: ClientTransport, options: MinnowDatabaseClientOptions<TSchema> = {}) {
    this.#schema = options.schema;
    this.#transport = transport;
    transport.addEventListener("message", this.#onMessage);
    transport.addEventListener("error", this.#onError);
    transport.addEventListener("messageerror", this.#onMessageError);
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

  async createView(
    name: string,
    sql: string,
    options: { orReplace?: boolean; managed?: boolean } = {},
  ): Promise<void> {
    await this.#call("createView", [name, sql, options]);
  }

  async dropView(name: string, options: { ifExists?: boolean } = {}): Promise<boolean> {
    return (await this.#call("dropView", [name, options])) as boolean;
  }

  async dropColumn(
    tableName: string,
    columnName: string,
    options: { ifExists?: boolean } = {},
  ): Promise<boolean> {
    return (await this.#call("dropColumn", [tableName, columnName, options])) as boolean;
  }

  async dropTable(tableName: string, options: { ifExists?: boolean } = {}): Promise<boolean> {
    return (await this.#call("dropTable", [tableName, options])) as boolean;
  }

  async createIndex(
    indexName: string,
    tableName: string,
    requestedColumns: string | ReadonlyArray<{ name: string; direction: "asc" | "desc" }>,
    options: { unique?: boolean } = {},
  ): Promise<void> {
    await this.#call("createIndex", [indexName, tableName, requestedColumns, options]);
  }

  async dropIndex(indexName: string, options: { ifExists?: boolean } = {}): Promise<boolean> {
    return (await this.#call("dropIndex", [indexName, options])) as boolean;
  }

  async buildFtsIndex(tableName: string, columnName: string): Promise<void> {
    await this.#call("buildFtsIndex", [tableName, columnName]);
  }

  /** The published catalog; see `MinnowDatabase.introspect()`. */
  async introspect(): Promise<Catalog> {
    return (await this.#call("introspect", [])) as Catalog;
  }

  async listTables(): Promise<TableDefinition[]> {
    return (await this.#call("listTables", [])) as TableDefinition[];
  }

  /**
   * Brings storage in line with a schema declaration. With no argument it applies the schema the
   * client was constructed with.
   */
  async migrate(
    definition: SchemaDefinition<readonly AnyTable[]> | undefined = this.#schema,
    options: MigrateOptions = {},
  ): Promise<ClientMigrationResult> {
    if (definition === undefined) {
      throw new TypeError(
        "migrate() needs a schema: pass a definition, or construct the client with { schema }",
      );
    }
    return (await this.#call("migrate", [
      serializeSchema(definition),
      options,
    ])) as ClientMigrationResult;
  }

  insertBatch<TName extends TableName<TSchema>>(
    tableName: TName,
    input: BatchInsertInput<TSchema, TName>,
  ): Promise<InsertBatchResult>;
  async insertBatch(tableName: string, input: InsertBatchInputLike): Promise<InsertBatchResult> {
    // Pivoted here rather than in the worker: the columnar form is the cheaper structured clone,
    // and it keeps the worker protocol's payload shape the same whichever form the caller used.
    const batch = toColumnarBatch(input);
    return (await this.#call("insertBatch", [tableName, batch])) as InsertBatchResult;
  }

  insert<TName extends TableName<TSchema>>(
    tableName: TName,
    row: BatchInsertRow<TSchema, TName>,
  ): Promise<InsertBatchResult>;
  async insert(tableName: string, row: BatchRow): Promise<InsertBatchResult> {
    return (await this.#call("insert", [tableName, row])) as InsertBatchResult;
  }

  upsertBatch<TName extends TableName<TSchema>>(
    tableName: TName,
    input: BatchInsertInput<TSchema, TName>,
    options?: BatchUpsertOptions<TSchema, TName>,
  ): Promise<UpsertBatchResult>;
  async upsertBatch(
    tableName: string,
    input: InsertBatchInputLike,
    options: UpsertOptions = {},
  ): Promise<UpsertBatchResult> {
    const batch = toColumnarBatch(input);
    return (await this.#call("upsertBatch", [tableName, batch, options])) as UpsertBatchResult;
  }

  upsert<TName extends TableName<TSchema>>(
    tableName: TName,
    row: BatchInsertRow<TSchema, TName>,
    options?: BatchUpsertOptions<TSchema, TName>,
  ): Promise<UpsertBatchResult>;
  async upsert(
    tableName: string,
    row: BatchRow,
    options: UpsertOptions = {},
  ): Promise<UpsertBatchResult> {
    return (await this.#call("upsert", [tableName, row, options])) as UpsertBatchResult;
  }

  updateBatch<TName extends TableName<TSchema>>(
    tableName: TName,
    input: BatchUpdateInput<TSchema, TName>,
  ): Promise<UpdateBatchResult>;
  async updateBatch(
    tableName: string,
    input: {
      readonly keys: readonly BatchValue[];
      readonly changes: Readonly<Record<string, readonly BatchValue[] | undefined>>;
    },
  ): Promise<UpdateBatchResult> {
    const wire: UpdateBatchInput = { keys: input.keys, changes: definedVectors(input.changes) };
    return (await this.#call("updateBatch", [tableName, wire])) as UpdateBatchResult;
  }

  /**
   * Changes one row by the table's unique key. An explicitly `undefined` change leaves that
   * column untouched, so a patch spread from optional fields needs no filtering first.
   */
  update<TName extends TableName<TSchema>>(
    tableName: TName,
    key: BatchKeyValue<TSchema, TName>,
    changes: BatchUpdateChanges<TSchema, TName>,
  ): Promise<UpdateBatchResult>;
  async update(
    tableName: string,
    key: Exclude<BatchValue, null>,
    changes: Readonly<Record<string, BatchValue | undefined>>,
  ): Promise<UpdateBatchResult> {
    // Structured clone keeps an `undefined` property; strip it here so the wire carries only
    // real changes and the worker's own filtering never sees a shape the direct engine would not.
    const present = Object.fromEntries(
      Object.entries(changes).filter(([, value]) => value !== undefined),
    );
    return (await this.#call("update", [tableName, key, present])) as UpdateBatchResult;
  }

  deleteBatch<TName extends TableName<TSchema>>(
    tableName: TName,
    input: BatchDeleteInput<TSchema, TName>,
  ): Promise<DeleteBatchResult>;
  async deleteBatch(tableName: string, input: DeleteBatchInput): Promise<DeleteBatchResult> {
    return (await this.#call("deleteBatch", [tableName, input])) as DeleteBatchResult;
  }

  delete<TName extends TableName<TSchema>>(
    tableName: TName,
    key: BatchKeyValue<TSchema, TName>,
  ): Promise<DeleteBatchResult>;
  async delete(tableName: string, key: Exclude<BatchValue, null>): Promise<DeleteBatchResult> {
    return (await this.#call("delete", [tableName, key])) as DeleteBatchResult;
  }

  bufferedWriter<TName extends TableName<TSchema>>(
    tableName: TName,
    options: BufferedWriterOptions = {},
  ): ClientBufferedWriter<BatchInsertRow<TSchema, TName>> {
    const handleId = crypto.randomUUID();
    const { onError, ...wireOptions } = options;
    this.#events.set(handleId, {
      ...(onError === undefined ? {} : { onError }),
    });
    const created = this.#call("bufferedWriter", [handleId, tableName, wireOptions]);
    return new ClientBufferedWriter<BatchInsertRow<TSchema, TName>>(
      this.#erased,
      handleId,
      created,
    );
  }

  // --- Reads ------------------------------------------------------------------------------------

  readTable<
    TName extends TableName<TSchema>,
    const TColumns extends ReadonlyArray<BatchColumnName<TSchema, TName>>,
  >(
    tableName: TName,
    options: BatchReadOptions<TSchema, TName, TColumns> & { readonly columns: TColumns },
  ): Promise<Array<Pick<BatchReadRow<TSchema, TName>, TColumns[number]>>>;
  readTable<TName extends TableName<TSchema>>(
    tableName: TName,
    versionOrOptions?: number | BatchReadOptions<TSchema, TName>,
  ): Promise<Array<BatchReadRow<TSchema, TName>>>;
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
    const { signal, onStats, ...wireOptions } = options ?? {};
    return decodeQueryResult(
      await this.#call("query", [sql, wireOptions, onStats !== undefined], { signal, onStats }),
    );
  }

  /** Pulls one transferred columnar page at a time, preserving worker backpressure. */
  queryCursor(
    sql: string,
    options: QueryCursorOptions = {},
  ): AsyncIterableIterator<QueryResult, undefined> {
    const call = this.#call.bind(this);
    const invoke = this._invoke.bind(this);
    const routeEvents = this._routeEvents.bind(this);
    const unrouteEvents = this._unrouteEvents.bind(this);
    const handleId = crypto.randomUUID();
    const { signal, onStats, ...wireOptions } = options;
    async function* batches(): AsyncGenerator<QueryResult, undefined> {
      let opened = false;
      let closing: Promise<unknown> | undefined;
      const close = (): Promise<unknown> => {
        if (!opened) return Promise.resolve();
        closing ??= invoke(handleId, "close", []).catch(() => undefined);
        return closing;
      };
      const onAbort = (): void => {
        void close();
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      if (onStats !== undefined) routeEvents(handleId, { onStats });
      try {
        signal?.throwIfAborted();
        await call("queryCursorOpen", [handleId, sql, wireOptions, onStats !== undefined]);
        opened = true;
        if (signal?.aborted === true) {
          await close();
          signal.throwIfAborted();
        }
        for (;;) {
          const next = (await invoke(handleId, "next", [])) as
            { done: true } | { done: false; batch: unknown };
          if (next.done) return undefined;
          yield decodeQueryResult(next.batch);
        }
      } finally {
        signal?.removeEventListener("abort", onAbort);
        await close();
        unrouteEvents(handleId);
      }
    }
    return batches();
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

  async execute(
    sql: string,
    params?: readonly QueryValue[],
    options: ExecuteOptions = {},
  ): Promise<ExecuteResult> {
    const { signal, onStats, ...wireOptions } = options;
    return (await this.#call("execute", [sql, params, wireOptions, onStats !== undefined], {
      signal,
      onStats,
    })) as ExecuteResult;
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
    action: (session: ClientWriteSession<TSchema>) => Promise<T>,
  ): Promise<{ result: T; version: number | null }> {
    const opened = (await this.#call("writeOpen", [])) as { handleId: string };
    const stage = (
      op: "insertBatch" | "upsertBatch" | "updateBatch" | "deleteBatch",
      tableName: string,
      input: unknown,
      options?: UpsertOptions,
    ): Promise<StagedWriteResult> =>
      this._invoke(opened.handleId, "stage", [
        op,
        tableName,
        input,
        options,
      ]) as Promise<StagedWriteResult>;
    const session: ClientWriteSession = {
      query: async (sql, options = {}) => {
        const { signal, onStats, ...wireOptions } = options;
        return decodeQueryResult(
          await this._invokeControlled(
            opened.handleId,
            "query",
            [sql, wireOptions, onStats !== undefined],
            { signal, onStats },
          ),
        );
      },
      execute: (sql, params) =>
        this._invoke(
          opened.handleId,
          "execute",
          params === undefined ? [sql] : [sql, params],
        ) as Promise<ExecuteResult>,
      insertBatch: (tableName, input) => stage("insertBatch", tableName, input),
      upsertBatch: (tableName, input, options) =>
        stage("upsertBatch", tableName, input, options) as Promise<StagedUpsertResult>,
      updateBatch: (tableName, input) => stage("updateBatch", tableName, input),
      deleteBatch: (tableName, input) => stage("deleteBatch", tableName, input),
    };
    try {
      // The scope stages by runtime table name; the declaration only types the caller's view.
      const result = await action(session as ClientWriteSession<TSchema>);
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
    const chunks: Uint8Array[] = [];
    let byteLength = 0;
    for await (const chunk of this.exportSnapshotStream(options)) {
      byteLength += chunk.byteLength;
      if (!Number.isSafeInteger(byteLength) || byteLength > MAX_SNAPSHOT_BYTE_ARRAY_LENGTH) {
        throw new RangeError("Snapshot exceeds the byte-array API's 4 GiB limit");
      }
      chunks.push(chunk);
    }
    const bytes = new Uint8Array(byteLength);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return bytes;
  }

  /** Pulls at most one bounded worker chunk at a time; closing releases the worker-side pin. */
  async *exportSnapshotStream(
    options: SnapshotExportOptions = {},
  ): AsyncGenerator<Uint8Array, void, void> {
    throwIfClientSnapshotAborted(options.signal);
    options.onProgress?.({ phase: "reading", transferredBytes: 0, totalBytes: 0 });
    const opened = (await this.#call("exportSnapshotOpen", [])) as { handleId: string };
    let transferredBytes = 0;
    let succeeded = false;
    try {
      for (;;) {
        throwIfClientSnapshotAborted(options.signal);
        const next = (await this._invoke(opened.handleId, "read", [])) as
          { done: true } | { done: false; chunk: Uint8Array };
        if (next.done) break;
        throwIfClientSnapshotAborted(options.signal);
        if (!(next.chunk instanceof Uint8Array) || next.chunk.byteLength === 0) {
          throw new Error("Snapshot transfer returned an invalid chunk");
        }
        yield next.chunk;
        transferredBytes += next.chunk.byteLength;
        options.onProgress?.({ phase: "transfer", transferredBytes, totalBytes: 0 });
      }
      succeeded = true;
    } finally {
      await this._invoke(opened.handleId, "close", []).catch(() => undefined);
      if (succeeded) {
        options.onProgress?.({
          phase: "done",
          transferredBytes,
          totalBytes: transferredBytes,
        });
      }
    }
  }

  /**
   * Loads a snapshot file into the worker's store, which must be empty. The bytes go over in the
   * same slices the export comes back in, and the worker's load progress arrives as events, so a
   * multi-megabyte restore can be shown moving rather than as a frozen tab.
   */
  async importSnapshot(bytes: Uint8Array, options: SnapshotImportOptions = {}): Promise<void> {
    await this.importSnapshotStream(clientSnapshotChunks(bytes), options);
  }

  /** Uploads with one acknowledged worker chunk in flight; cancellation leaves resumable state. */
  async importSnapshotStream(
    source: AsyncIterable<Uint8Array>,
    options: SnapshotImportOptions = {},
  ): Promise<void> {
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
      throwIfClientSnapshotAborted(options.signal);
      await this.#call("importSnapshotOpen", [handleId]);
      for await (const input of source) {
        throwIfClientSnapshotAborted(options.signal);
        if (!(input instanceof Uint8Array)) {
          throw new TypeError("Snapshot stream chunks must be Uint8Array values");
        }
        for (let offset = 0; offset < input.byteLength; offset += SNAPSHOT_CHUNK_BYTES) {
          // Copy the bounded window: cloning a subarray would otherwise transfer its whole backing
          // file on every request.
          const chunk = input.slice(offset, offset + SNAPSHOT_CHUNK_BYTES);
          await this.#post("rpc-call", handleId, "write", [chunk], [chunk.buffer]);
          throwIfClientSnapshotAborted(options.signal);
        }
      }
      await this._invoke(handleId, "finish", []);
    } catch (error) {
      await this._invoke(handleId, options.signal?.aborted === true ? "cancel" : "close", []).catch(
        () => undefined,
      );
      throw error;
    } finally {
      this.#events.delete(handleId);
    }
  }

  /** The worker-side buffer pool's byte budget, residency, and lifetime counters. */
  async bufferPoolStats(): Promise<BufferPoolStats> {
    return (await this.#call("bufferPoolStats", [])) as BufferPoolStats;
  }

  async maintenanceStatus(): Promise<MaintenanceStatus> {
    return (await this.#call("maintenanceStatus", [])) as MaintenanceStatus;
  }

  async checkIntegrity(
    options: {
      mode?: StorageIntegrityMode;
      maxIssues?: number;
    } = {},
  ): Promise<StorageIntegrityReport> {
    return (await this.#call("checkIntegrity", [options])) as StorageIntegrityReport;
  }

  async storageStats(): Promise<StorageStats> {
    return (await this.#call("storageStats", [])) as StorageStats;
  }

  async inspectInterruptedImport(): Promise<InterruptedSnapshotImport | null> {
    return (await this.#call("inspectInterruptedImport", [])) as InterruptedSnapshotImport | null;
  }

  async abortInterruptedImport(identity: string): Promise<InterruptedSnapshotImportAbortResult> {
    return (await this.#call("abortInterruptedImport", [
      identity,
    ])) as InterruptedSnapshotImportAbortResult;
  }

  liveQueries(options: ClientLiveQueryOptions = {}): ClientLiveQuerySet {
    const handleId = crypto.randomUUID();
    const created = this.#call("liveQueries", [handleId, options]);
    return new ClientLiveQuerySet(this.#erased, handleId, created);
  }

  // --- Maintenance ------------------------------------------------------------------------------

  async listVisibleSegmentPage(
    tableName: string,
    options?: VisibleSegmentPageOptions,
  ): Promise<VisibleSegmentPage> {
    return (await this.#call(
      "listVisibleSegmentPage",
      options === undefined ? [tableName] : [tableName, options],
    )) as VisibleSegmentPage;
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
      this.#transport.removeEventListener?.("message", this.#onMessage);
      this.#transport.removeEventListener?.("error", this.#onError);
      this.#transport.removeEventListener?.("messageerror", this.#onMessageError);
      if (options.terminateWorker === true) this.#transport.terminate?.();
    }
  }

  // --- Channel internals ------------------------------------------------------------------------

  /** @internal */
  async _invoke(handleId: string, method: string, args: unknown[]): Promise<unknown> {
    if (this.#closed) throw new Error("Database client is closed");
    return this.#post("rpc-call", handleId, method, args);
  }

  /** @internal */
  async _invokeControlled(
    handleId: string,
    method: string,
    args: unknown[],
    controls: RpcCallControls,
  ): Promise<unknown> {
    if (this.#closed) throw new Error("Database client is closed");
    return this.#post("rpc-call", handleId, method, args, undefined, false, controls);
  }

  /** @internal */
  _routeEvents(handleId: string, route: EventRoute): void {
    this.#events.set(handleId, route);
  }

  /** @internal */
  _unrouteEvents(handleId: string): void {
    this.#events.delete(handleId);
  }

  async #call(method: string, args: unknown[], controls: RpcCallControls = {}): Promise<unknown> {
    if (this.#closed) throw new Error("Database client is closed");
    return this.#post("rpc-call", null, method, args, undefined, false, controls);
  }

  async #post(
    kind: "rpc-init" | "rpc-call",
    handleId: string | null,
    method: string,
    args: unknown[],
    transfer?: ArrayBuffer[],
    bypassLimit = kind === "rpc-init" || method === "dispose",
    controls: RpcCallControls = {},
  ): Promise<unknown> {
    if (this.#fatal !== undefined) throw this.#fatal;
    controls.signal?.throwIfAborted();
    if (!bypassLimit && this.#pending.size >= MAX_DATABASE_RPC_IN_FLIGHT) {
      throw new RangeError(
        `A database worker connection cannot hold more than ${String(MAX_DATABASE_RPC_IN_FLIGHT)} in-flight requests`,
      );
    }
    const requestId = crypto.randomUUID();
    return new Promise((resolve, reject) => {
      const onAbort = (): void => {
        try {
          this.#transport.postMessage({ version: protocolVersion, requestId, kind: "rpc-cancel" });
        } catch {
          // The original RPC still owns completion. A transport failure also reaches #fail via
          // its error event; throwing from an AbortSignal listener would only be unhandled noise.
        }
      };
      const cleanup = (): void => {
        controls.signal?.removeEventListener("abort", onAbort);
        if (controls.onStats !== undefined) this.#events.delete(requestId);
      };
      controls.signal?.addEventListener("abort", onAbort, { once: true });
      if (controls.onStats !== undefined) {
        this.#events.set(requestId, { onStats: controls.onStats });
      }
      this.#pending.set(requestId, { resolve, reject, cleanup });
      try {
        this.#transport.postMessage(
          kind === "rpc-init"
            ? { version: protocolVersion, requestId, kind, payload: args[0] }
            : { version: protocolVersion, requestId, kind, handleId, method, args },
          transfer === undefined ? undefined : { transfer },
        );
      } catch (error) {
        this.#pending.delete(requestId);
        cleanup();
        reject(
          error instanceof Error ? error : new Error("Database request failed", { cause: error }),
        );
      }
    });
  }

  #receive(message: unknown): void {
    let response: RpcResponse | null;
    try {
      response = parseRpcResponse(message);
    } catch (cause) {
      this.#rejectUnreadable(message, cause);
      return;
    }
    if (response === null) return;
    if (response.kind === "rpc-event") {
      const route = this.#events.get(response.handleId);
      if (route === undefined) return;
      if (response.event === "change") {
        const { result, delivery } = response.payload as {
          result: unknown;
          delivery: LiveQueryDelivery;
        };
        route.onChange?.(decodeQueryResult(result), delivery);
      } else if (response.event === "invalidate") {
        route.onInvalidate?.(response.payload as LiveQueryInvalidation);
      } else if (response.event === "error") {
        route.onError?.(rehydrateResponseError(response.payload));
      } else if (response.event === "progress") route.onProgress?.(response.payload);
      else if (response.event === "stats") {
        route.onStats?.(response.payload as QueryExecutionStats);
      } else if (response.event === "complete") {
        this.#events.delete(response.handleId);
        route.onComplete?.();
      }
      return;
    }
    const pending = this.#pending.get(response.requestId);
    if (pending === undefined) return;
    this.#pending.delete(response.requestId);
    pending.cleanup?.();
    if (response.kind === "rpc-result") pending.resolve(response.result);
    else pending.reject(rehydrateResponseError(response.error));
  }

  /**
   * A frame the protocol module refused: an RPC response at another protocol version, from a
   * worker built against a different release than this client. When the frame names a request
   * still in flight, that call alone fails and the connection stays usable. Otherwise nothing
   * this worker sends can be trusted, so every pending call fails the way a transport error does.
   */
  #rejectUnreadable(message: unknown, cause: unknown): void {
    const reason = cause instanceof Error ? cause.message : String(cause);
    const error = new Error(`The database worker sent a frame this client cannot read: ${reason}`, {
      cause,
    });
    const requestId = (message as { requestId?: unknown }).requestId;
    const pending = typeof requestId === "string" ? this.#pending.get(requestId) : undefined;
    if (pending === undefined || typeof requestId !== "string") {
      this.#fail(error);
      return;
    }
    this.#pending.delete(requestId);
    pending.cleanup?.();
    pending.reject(error);
  }

  #fail(error: Error): void {
    this.#fatal = error;
    const pending = [...this.#pending.values()];
    this.#pending.clear();
    this.#events.clear();
    for (const call of pending) {
      call.cleanup?.();
      call.reject(error);
    }
  }
}

/**
 * The scope handed to the client `snapshot()`: queries pinned to one manifest version,
 * consistent with each other for the lifetime of the callback.
 */
/** The scope handed to the client `write()`; mirrors the in-worker WriteSession. */
export interface ClientWriteSession<TSchema extends AnySchema = UntypedSchema> {
  /** Read-your-writes: observes the pre-scope snapshot plus everything staged so far. */
  query(sql: string, options?: QueryOptions): Promise<QueryResult>;
  execute(sql: string, params?: readonly QueryValue[]): Promise<ExecuteResult>;
  insertBatch<TName extends TableName<TSchema>>(
    tableName: TName,
    input: BatchInsertInput<TSchema, TName>,
  ): Promise<StagedWriteResult>;
  upsertBatch<TName extends TableName<TSchema>>(
    tableName: TName,
    input: BatchInsertInput<TSchema, TName>,
    options?: BatchUpsertOptions<TSchema, TName>,
  ): Promise<StagedUpsertResult>;
  updateBatch<TName extends TableName<TSchema>>(
    tableName: TName,
    input: BatchUpdateInput<TSchema, TName>,
  ): Promise<StagedWriteResult>;
  deleteBatch<TName extends TableName<TSchema>>(
    tableName: TName,
    input: BatchDeleteInput<TSchema, TName>,
  ): Promise<StagedWriteResult>;
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
export class ClientBufferedWriter<TRow extends BatchRow = BatchRow> {
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

  async add(row: TRow): Promise<BufferedFlushResult | undefined> {
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

  /** Registers dependency observation while leaving execution/result mapping to an adapter. */
  async observe(
    query: LiveQueryInput,
    options: LiveQueryObserveOptions,
  ): Promise<ClientLiveSubscription> {
    await this.#created;
    const subscriptionId = crypto.randomUUID();
    const state = { completed: false };
    this.client._routeEvents(subscriptionId, {
      onInvalidate: options.onInvalidate.bind(options),
      ...(options.onError === undefined ? {} : { onError: options.onError.bind(options) }),
      onComplete: () => {
        state.completed = true;
        this.#subscriptionIds.delete(subscriptionId);
        options.onComplete?.();
      },
    });
    try {
      const created = (await this.client._invoke(this.handleId, "observe", [
        subscriptionId,
        query,
        { suppressUnchanged: options.suppressUnchanged === true },
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
