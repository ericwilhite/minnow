import type { BlockStore } from "../storage/types.js";
import { MAX_SNAPSHOT_STREAM_CHUNK_BYTES } from "../storage/snapshot.js";
import {
  parseRpcRequest,
  MAX_DATABASE_RPC_IN_FLIGHT,
  rpcEvent,
  rpcFailure,
  rpcResult,
  serializeError,
  type RpcRequest,
} from "../worker-protocol/index.js";
import {
  type WriteSession,
  MinnowDatabase,
  type MinnowDatabaseOptions,
  type BufferedTableWriter,
  type BatchValue,
  type BufferedWriterOptions,
  type QueryCursorOptions,
  type QueryOptions,
  type ReadTableOptions,
} from "./database.js";
import { LiveQuerySet, type LiveQueryInput, type LiveQuerySubscription } from "./live.js";
import type { CompiledQuery, QueryRow, QueryValue } from "./query.js";
import { encodeQueryResult, encodeQueryRows, type EncodedQueryResult } from "./result-wire.js";
import { deserializeSchema, serializeMigrationSteps, type WireSchema } from "./schema-wire.js";

/**
 * Worker-side host for the main-thread client. exposeDatabase() answers the client's RPC frames
 * against a database you construct yourself; attachDatabaseWorker() additionally owns
 * construction, building the store from the cloneable descriptor in the client's init frame. The
 * ready-made worker entry (`@minnowdb/core/worker`) is one line over the latter.
 *
 * Dispatch is a whitelist per target: the root database and each issued handle accept only the
 * methods listed here, so arbitrary property access never crosses the channel.
 */

export type StoreDescriptor =
  | { kind: "memory" }
  | {
      kind: "indexeddb";
      name: string;
      durability?: IDBTransactionDurability;
      uniqueKeyCacheBytes?: number;
    }
  | { kind: "opfs"; name: string; durability?: "relaxed" | "strict" };

/** The cloneable subset of MinnowDatabaseOptions; function-valued seams stay worker-side. */
export type WireDatabaseOptions = Pick<
  MinnowDatabaseOptions,
  | "compression"
  | "targetBlockBytes"
  | "rowsPerBlock"
  | "maxCommitRetries"
  | "spillOwnerLeaseMs"
  | "transactionOwnerLeaseMs"
  | "transactionIdleTimeoutMs"
  | "bufferPoolBytes"
  | "executionMemoryBudgetBytes"
  | "autoCollectDebtLimitCommits"
  | "autoCollect"
  | "autoCompact"
>;

export interface DatabaseInitPayload {
  store: StoreDescriptor;
  options?: WireDatabaseOptions;
}

interface RpcCallContext {
  readonly requestId: string;
  readonly signal: AbortSignal;
}

/**
 * The staged-mutation ops a client may name inside a "stage" call. The op arrives as wire data,
 * so it must be checked against this list before indexing into the session — otherwise any
 * property name (including Object.prototype members like "constructor") would be reachable.
 */
const stageOps = ["insertBatch", "upsertBatch", "updateBatch", "deleteBatch"] as const;
type StageOp = (typeof stageOps)[number];

function isStageOp(value: unknown): value is StageOp {
  return typeof value === "string" && (stageOps as readonly string[]).includes(value);
}

/**
 * Root methods whose arguments and results are already structured-clone-safe. `satisfies` makes
 * a removed or renamed database method a compile error, while the runtime guard below fails
 * closed if a caller supplies a malformed database object through the manual host seam.
 */
const directRootMethods = [
  "createTable",
  "createView",
  "dropView",
  "dropColumn",
  "dropTable",
  "createIndex",
  "dropIndex",
  "buildFtsIndex",
  "introspect",
  "listTables",
  "insertBatch",
  "insert",
  "upsertBatch",
  "upsert",
  "updateBatch",
  "update",
  "deleteBatch",
  "delete",
  "runStatement",
  "explain",
  "execute",
  "listVisibleSegmentPage",
  "cleanupQuerySpill",
  "compactTable",
  "compactTableStep",
  "resumeCompactionJob",
  "listCompactionJobs",
  "cancelCompactionJob",
  "collectGarbage",
  "collectGarbageStep",
  "resumeGarbageCollectionJob",
  "bufferPoolStats",
  "maintenanceStatus",
  "checkIntegrity",
  "storageStats",
  "inspectInterruptedImport",
  "abortInterruptedImport",
  "listGarbageCollectionJobs",
] as const satisfies ReadonlyArray<keyof MinnowDatabase>;
type DirectRootMethod = (typeof directRootMethods)[number];

function isDirectRootMethod(value: string): value is DirectRootMethod {
  return (directRootMethods as readonly string[]).includes(value);
}

/**
 * A call result that travels as a columnar frame with its buffers transferred. Only query
 * results take this path: rows of objects are what structured clone is slowest at, so they are
 * pivoted into per-column arrays here and rebuilt by the client.
 */
class ColumnarResult {
  constructor(readonly encoded: EncodedQueryResult) {}
}

class CursorBatchResult {
  constructor(readonly encoded: EncodedQueryResult | undefined) {}
}

class SnapshotChunkResult {
  constructor(readonly chunk: Uint8Array<ArrayBuffer>) {}
}

class WriteScopeAbortedError extends Error {
  override readonly name = "WriteScopeAbortedError";

  constructor() {
    super("Write scope aborted");
  }
}

/** One-chunk rendezvous. The sender is acknowledged only when the decoder asks for more. */
class SnapshotImportChunkQueue implements AsyncIterable<Uint8Array>, AsyncIterator<Uint8Array> {
  #queued:
    | {
        chunk: Uint8Array;
        resolve: () => void;
        reject: (error: unknown) => void;
      }
    | undefined;
  #consumed:
    | {
        resolve: () => void;
        reject: (error: unknown) => void;
      }
    | undefined;
  #reader:
    | {
        resolve: (result: IteratorResult<Uint8Array>) => void;
        reject: (error: unknown) => void;
      }
    | undefined;
  #finished = false;
  #failure: unknown;

  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    return this;
  }

  push(chunk: Uint8Array): Promise<void> {
    if (chunk.byteLength > MAX_SNAPSHOT_STREAM_CHUNK_BYTES) {
      throw new RangeError("Snapshot transfer chunk exceeds the 1 MiB limit");
    }
    if (this.#failure !== undefined) return Promise.reject(snapshotQueueError(this.#failure));
    if (this.#finished) return Promise.reject(new Error("Snapshot import transfer is closed"));
    if (this.#queued !== undefined) {
      return Promise.reject(new Error("Snapshot import transfer already has a queued chunk"));
    }
    return new Promise<void>((resolve, reject) => {
      const reader = this.#reader;
      if (reader === undefined) {
        this.#queued = { chunk, resolve, reject };
        return;
      }
      this.#reader = undefined;
      this.#consumed = { resolve, reject };
      reader.resolve({ done: false, value: chunk });
    });
  }

  async next(): Promise<IteratorResult<Uint8Array>> {
    this.#consumed?.resolve();
    this.#consumed = undefined;
    if (this.#failure !== undefined) throw snapshotQueueError(this.#failure);
    const queued = this.#queued;
    if (queued !== undefined) {
      this.#queued = undefined;
      this.#consumed = { resolve: queued.resolve, reject: queued.reject };
      return { done: false, value: queued.chunk };
    }
    if (this.#finished) return { done: true, value: undefined };
    return new Promise<IteratorResult<Uint8Array>>((resolve, reject) => {
      this.#reader = { resolve, reject };
    });
  }

  async return(): Promise<IteratorResult<Uint8Array>> {
    this.finish();
    return { done: true, value: undefined };
  }

  finish(): void {
    if (this.#finished) return;
    this.#finished = true;
    if (this.#queued === undefined) {
      const reader = this.#reader;
      this.#reader = undefined;
      reader?.resolve({ done: true, value: undefined });
    }
  }

  fail(error: unknown): void {
    if (this.#failure !== undefined) return;
    this.#failure = error;
    this.#finished = true;
    this.#queued?.reject(error);
    this.#queued = undefined;
    this.#consumed?.reject(error);
    this.#consumed = undefined;
    this.#reader?.reject(error);
    this.#reader = undefined;
  }
}

function snapshotQueueError(value: unknown): Error {
  return value instanceof Error ? value : new Error("Snapshot transfer failed", { cause: value });
}

function abortError(message: string): Error {
  const error = new Error(message);
  error.name = "AbortError";
  return error;
}

/** The slice of DedicatedWorkerGlobalScope (or MessagePort) the host needs. */
export interface RpcScope {
  addEventListener(type: "message", listener: (event: MessageEvent<unknown>) => void): void;
  /**
   * Query results arrive with their typed-array buffers listed for transfer; a scope that
   * ignores the second argument still works, it just clones them.
   */
  postMessage(message: unknown, options?: { transfer: ArrayBuffer[] }): void;
}

export interface ExposeDatabaseOptions {
  /** Called when the client sends its dispose frame, after every handle has been closed. */
  onDispose?: () => void | Promise<void>;
  /**
   * Rolls back a worker write handle after this long without a handle RPC. Match the constructed
   * database's `transactionIdleTimeoutMs` when it is customized. Defaults to 30 seconds.
   */
  writeHandleIdleTimeoutMs?: number;
  /**
   * Called when the main thread reports its page visibility. The stock entry forwards this to
   * the store's `setForeground` when it has one — the OPFS store uses it to keep leadership
   * on the tab the user is looking at.
   */
  onVisibility?: (visible: boolean) => void;
}

export interface AttachDatabaseWorkerOptions {
  /** Test seam for environments without a global IndexedDB (e.g. fake-indexeddb). */
  indexedDB?: IDBFactory;
}

type Handle =
  | { type: "snapshot"; release: () => void; done: Promise<void> }
  | {
      type: "snapshot-export";
      iterator: AsyncIterator<Uint8Array>;
      reading: boolean;
      done: boolean;
    }
  | {
      type: "snapshot-import";
      queue: SnapshotImportChunkQueue;
      task: Promise<void>;
      abort: AbortController;
    }
  | {
      type: "write";
      session: WriteSession;
      finish: (commit: boolean) => void;
      done: Promise<{ version: number | null }>;
      activeCalls: number;
      activeCallDone: Promise<void> | undefined;
      finishActiveCall: (() => void) | undefined;
      open: boolean;
      idleTimer: ReturnType<typeof setTimeout> | undefined;
    }
  | { type: "writer"; writer: BufferedTableWriter }
  | { type: "query-cursor"; iterator: AsyncIterator<import("./query.js").QueryResult, undefined> }
  | { type: "live-set"; set: LiveQuerySet; subscriptionIds: Set<string> }
  | { type: "live-subscription"; subscription: LiveQuerySubscription; setId: string };

/** Hard per-connection ceiling for resident worker resources abandoned by a client. */
export const MAX_WORKER_HANDLES_PER_CONNECTION = 256;
const DEFAULT_WRITE_HANDLE_IDLE_TIMEOUT_MS = 30_000;
const MAX_TIMER_DELAY_MS = 2_147_483_647;
/** Shared by RPC methods that do not expose cancellation, avoiding one controller per call. */
const passiveRpcSignal = new AbortController().signal;

class DatabaseRpcServer {
  readonly #handles = new Map<string, Handle>();
  readonly #reservedHandleIds = new Set<string>();
  readonly #openingHandlePromises = new Set<Promise<unknown>>();
  readonly #settlingWritePromises = new Set<Promise<unknown>>();
  readonly #requestAborts = new Map<string, AbortController>();
  #disposed = false;
  #inFlightRpcCount = 0;
  #inFlightRpcDrain: Promise<void> | undefined;
  #resolveInFlightRpcDrain: (() => void) | undefined;
  readonly #writeHandleIdleTimeoutMs: number;

  constructor(
    private readonly database: MinnowDatabase,
    private readonly scope: RpcScope,
    private readonly options: ExposeDatabaseOptions,
  ) {
    this.#writeHandleIdleTimeoutMs =
      options.writeHandleIdleTimeoutMs ?? DEFAULT_WRITE_HANDLE_IDLE_TIMEOUT_MS;
    if (
      !Number.isSafeInteger(this.#writeHandleIdleTimeoutMs) ||
      this.#writeHandleIdleTimeoutMs <= 0 ||
      this.#writeHandleIdleTimeoutMs > MAX_TIMER_DELAY_MS
    ) {
      throw new RangeError(
        `Worker write-handle idle timeout must be a positive whole number no greater than ${String(MAX_TIMER_DELAY_MS)}`,
      );
    }
  }

  async handle(request: RpcRequest): Promise<void> {
    if (request.kind === "rpc-init") {
      this.scope.postMessage(rpcResult(request.requestId, { ready: true }));
      return;
    }
    if (request.kind === "rpc-cancel") {
      this.#requestAborts
        .get(request.requestId)
        ?.abort(abortError("Database request was cancelled"));
      return;
    }
    const bypassLimit = request.method === "dispose";
    if (!bypassLimit && this.#inFlightRpcCount >= MAX_DATABASE_RPC_IN_FLIGHT) {
      this.scope.postMessage(
        rpcFailure(
          request.requestId,
          new RangeError(
            `A database worker connection cannot hold more than ${String(MAX_DATABASE_RPC_IN_FLIGHT)} in-flight requests`,
          ),
        ),
      );
      return;
    }
    if (!bypassLimit) this.#inFlightRpcCount += 1;
    const abort = request.method === "query" ? new AbortController() : undefined;
    if (abort !== undefined) this.#requestAborts.set(request.requestId, abort);
    const context: RpcCallContext = {
      requestId: request.requestId,
      signal: abort?.signal ?? passiveRpcSignal,
    };
    try {
      if (this.#disposed) throw new Error("Database connection is disposed");
      const result =
        request.handleId === null
          ? await this.#callRoot(request.method, request.args, context)
          : await this.#callHandle(request.handleId, request.method, request.args, context);
      if (result instanceof ColumnarResult) {
        this.scope.postMessage(rpcResult(request.requestId, result.encoded.payload), {
          transfer: result.encoded.transfer,
        });
      } else if (result instanceof CursorBatchResult) {
        this.scope.postMessage(
          rpcResult(
            request.requestId,
            result.encoded === undefined
              ? { done: true }
              : { done: false, batch: result.encoded.payload },
          ),
          result.encoded === undefined ? undefined : { transfer: result.encoded.transfer },
        );
      } else if (result instanceof SnapshotChunkResult) {
        this.scope.postMessage(rpcResult(request.requestId, { done: false, chunk: result.chunk }), {
          transfer: [result.chunk.buffer],
        });
      } else {
        this.scope.postMessage(rpcResult(request.requestId, result));
      }
    } catch (error) {
      this.scope.postMessage(rpcFailure(request.requestId, error));
    } finally {
      if (abort !== undefined) this.#requestAborts.delete(request.requestId);
      if (!bypassLimit) this.#inFlightRpcCount -= 1;
      if (this.#inFlightRpcCount === 0 && this.#resolveInFlightRpcDrain !== undefined) {
        const resolve = this.#resolveInFlightRpcDrain;
        this.#inFlightRpcDrain = undefined;
        this.#resolveInFlightRpcDrain = undefined;
        resolve();
      }
    }
  }

  async #callRoot(method: string, args: unknown[], context: RpcCallContext): Promise<unknown> {
    const database = this.database as unknown as Record<
      string,
      (...forwarded: unknown[]) => Promise<unknown>
    >;
    if (isDirectRootMethod(method)) {
      const operation = database[method];
      if (operation === undefined) {
        throw new Error(`Database root method is unavailable: ${method}`);
      }
      return operation.apply(this.database, args);
    }
    switch (method) {
      case "query": {
        const [sql, options, reportStats = false] = args as [
          string,
          QueryOptions | undefined,
          boolean | undefined,
        ];
        return new ColumnarResult(
          encodeQueryResult(
            await this.database.query(sql, {
              ...options,
              signal: context.signal,
              ...(reportStats
                ? {
                    onStats: (stats) => {
                      this.scope.postMessage(rpcEvent(context.requestId, "stats", stats));
                    },
                  }
                : {}),
            }),
          ),
        );
      }
      case "queryCursorOpen": {
        const handleId = this.#claimHandleId(args[0]);
        const [sql, options, reportStats = false] = args.slice(1) as [
          string,
          QueryCursorOptions | undefined,
          boolean | undefined,
        ];
        try {
          const iterator = this.database.queryCursor(sql, {
            ...options,
            ...(reportStats
              ? {
                  onStats: (stats) => {
                    this.scope.postMessage(rpcEvent(handleId, "stats", stats));
                  },
                }
              : {}),
          });
          this.#publishHandle(handleId, { type: "query-cursor", iterator });
        } catch (error) {
          this.#releaseHandleId(handleId);
          throw error;
        }
        return { handleId };
      }
      case "run": {
        const query = args[0] as { kind: "typed-query"; plan: CompiledQuery; __row?: QueryRow };
        return new ColumnarResult(encodeQueryRows(await this.database.run(query)));
      }
      case "readTable": {
        // A table's rows are uniform — every row carries every read column — so they take the
        // same columnar frame a query result does.
        const [tableName, versionOrOptions] = args as [
          string,
          number | ReadTableOptions | undefined,
        ];
        const rows = await this.database.readTable(tableName, versionOrOptions);
        return new ColumnarResult(encodeQueryRows(rows));
      }
      case "migrate": {
        const result = await this.database.migrate(
          deserializeSchema(args[0] as WireSchema),
          args[1] ?? {},
        );
        return {
          createdTables: result.createdTables,
          alteredTables: result.alteredTables,
          droppedTables: result.droppedTables,
          replacedViews: result.replacedViews,
          droppedViews: result.droppedViews,
          steps: serializeMigrationSteps(result.steps),
        };
      }
      case "writeOpen":
        return this.#trackOpeningHandle(this.#openWriteHandle());
      case "snapshotOpen":
        return this.#trackOpeningHandle(this.#openSnapshotHandle());
      case "exportSnapshotOpen": {
        const handleId = this.#claimGeneratedHandleId();
        const iterator = this.database.exportSnapshotStream()[Symbol.asyncIterator]();
        try {
          this.#publishHandle(handleId, {
            type: "snapshot-export",
            iterator,
            reading: false,
            done: false,
          });
        } catch (error) {
          await iterator.return(undefined);
          this.#releaseHandleId(handleId);
          throw error;
        }
        return { handleId };
      }
      case "importSnapshotOpen": {
        // Named by the client, like the other event-producing handles, so its progress route
        // exists before the first frame the load can emit.
        const handleId = this.#claimHandleId(args[0]);
        const queue = new SnapshotImportChunkQueue();
        const abort = new AbortController();
        const task = this.database.importSnapshotStream(queue, {
          signal: abort.signal,
          onProgress: (progress) => {
            this.scope.postMessage(rpcEvent(handleId, "progress", progress));
          },
        });
        void task.catch((error: unknown) => queue.fail(error));
        try {
          this.#publishHandle(handleId, { type: "snapshot-import", queue, task, abort });
        } catch (error) {
          abort.abort(error);
          queue.fail(error);
          await task.catch(() => undefined);
          this.#releaseHandleId(handleId);
          throw error;
        }
        return { handleId };
      }
      case "bufferedWriter": {
        // Event-producing handles are named by the client so its event routes exist before the
        // first frame the handle can emit.
        const handleId = this.#claimHandleId(args[0]);
        let writer: BufferedTableWriter | undefined;
        try {
          writer = this.database.bufferedWriter(args[1] as string, {
            ...(args[2] as Omit<BufferedWriterOptions, "onError">),
            onError: (error) => {
              this.scope.postMessage(rpcEvent(handleId, "error", serializeError(error)));
            },
          });
          this.#publishHandle(handleId, { type: "writer", writer });
        } catch (error) {
          await writer?.close().catch(() => undefined);
          this.#releaseHandleId(handleId);
          throw error;
        }
        return { handleId };
      }
      case "liveQueries": {
        const handleId = this.#claimHandleId(args[0]);
        const { pollIntervalMs, channelName } = (args[1] ?? {}) as {
          pollIntervalMs?: number;
          channelName?: string;
        };
        // The database owns channelName resolution (and closes the channel with the set).
        let set: LiveQuerySet | undefined;
        try {
          set = this.database.liveQueries({
            ...(channelName === undefined ? {} : { channelName }),
            ...(pollIntervalMs === undefined ? {} : { pollIntervalMs }),
          });
          this.#publishHandle(handleId, { type: "live-set", set, subscriptionIds: new Set() });
        } catch (error) {
          set?.close();
          this.#releaseHandleId(handleId);
          throw error;
        }
        return { handleId };
      }
      case "dispose": {
        await this.#dispose();
        return { disposed: true };
      }
      case "setVisibility": {
        this.options.onVisibility?.(args[0] === true);
        return { acknowledged: true };
      }
      default:
        throw new Error(`Unsupported database method: ${method}`);
    }
  }

  async #openWriteHandle(): Promise<{ handleId: string }> {
    // Same deferred-callback shape as snapshotOpen: the scoped write() stays pending until the
    // client commits or aborts, so its staged transaction spans the RPC session.
    const handleId = this.#claimGeneratedHandleId();
    let sessionResolve!: (session: WriteSession) => void;
    let sessionReject!: (error: unknown) => void;
    const sessionReady = new Promise<WriteSession>((resolveSession, rejectSession) => {
      sessionResolve = resolveSession;
      sessionReject = rejectSession;
    });
    let finish: ((commit: boolean) => void) | undefined;
    const done = this.database
      .write(async (session) => {
        sessionResolve(session);
        await new Promise<void>((resolveCommit, rejectAbort) => {
          finish = (commit) => {
            if (commit) resolveCommit();
            else rejectAbort(new WriteScopeAbortedError());
          };
        });
      })
      .then((outcome) => ({ version: outcome.version }));
    // beginDeferred can fail before the callback starts (for example at the durable reader-lease
    // ceiling). Reject the open rendezvous too and observe `done` immediately.
    void done.catch((error: unknown) => sessionReject(error));
    try {
      const session = await sessionReady;
      if (finish === undefined) throw new Error("Write scope did not install its settlement");
      this.#publishHandle(handleId, {
        type: "write",
        session,
        finish,
        done,
        activeCalls: 0,
        activeCallDone: undefined,
        finishActiveCall: undefined,
        open: true,
        idleTimer: undefined,
      });
      return { handleId };
    } catch (error) {
      // Publication can lose a race with connection disposal. If the callback started, abort and
      // join it before the opening task releases its lifecycle slot.
      finish?.(false);
      await done.catch(() => undefined);
      this.#releaseHandleId(handleId);
      throw error;
    }
  }

  async #openSnapshotHandle(): Promise<{ handleId: string; version: number | null }> {
    // The scoped snapshot() holds its lease until the callback settles; the handle keeps the
    // callback pending until close, so the pin spans the RPC session.
    const handleId = this.#claimGeneratedHandleId();
    let release: (() => void) | undefined;
    let resolveVersion!: (version: number | null) => void;
    let rejectVersion!: (error: unknown) => void;
    const versionReady = new Promise<number | null>((resolve, reject) => {
      resolveVersion = resolve;
      rejectVersion = reject;
    });
    const done = this.database.snapshot(async (session) => {
      resolveVersion(session.version);
      await new Promise<void>((resolveRelease) => {
        release = resolveRelease;
      });
    });
    // Lease admission may fail before the callback installs `release`. Preserve that typed error
    // through the rendezvous and keep the task observed from creation.
    void done.catch((error: unknown) => rejectVersion(error));
    try {
      const version = await versionReady;
      if (release === undefined) throw new Error("Snapshot scope did not install its release");
      this.#publishHandle(handleId, { type: "snapshot", release, done });
      return { handleId, version };
    } catch (error) {
      release?.();
      await done.catch(() => undefined);
      this.#releaseHandleId(handleId);
      throw error;
    }
  }

  #trackOpeningHandle<T>(opening: Promise<T>): Promise<T> {
    this.#openingHandlePromises.add(opening);
    void opening.then(
      () => this.#openingHandlePromises.delete(opening),
      () => this.#openingHandlePromises.delete(opening),
    );
    return opening;
  }

  async #callHandle(
    handleId: string,
    method: string,
    args: unknown[],
    context: RpcCallContext,
  ): Promise<unknown> {
    const handle = this.#handles.get(handleId);
    if (handle === undefined) throw new Error(`Unknown handle: ${handleId}`);
    if (handle.type === "write") {
      this.#beginWriteHandleCall(handle);
      try {
        return await this.#callWriteHandle(handleId, handle, method, args, context);
      } finally {
        this.#endWriteHandleCall(handleId, handle);
      }
    }
    switch (handle.type) {
      case "snapshot": {
        if (method !== "close") throw new Error(`Unsupported snapshot method: ${method}`);
        handle.release();
        this.#handles.delete(handleId);
        await handle.done;
        return undefined;
      }
      case "snapshot-export":
        return this.#callSnapshotExport(handleId, handle, method, args);
      case "snapshot-import":
        return this.#callSnapshotImport(handleId, handle, method, args);
      case "writer":
        return this.#callWriter(handleId, handle.writer, method, args);
      case "query-cursor": {
        if (method === "next") {
          const next = await handle.iterator.next();
          if (next.done) {
            this.#handles.delete(handleId);
            return new CursorBatchResult(undefined);
          }
          return new CursorBatchResult(encodeQueryResult(next.value));
        }
        if (method === "close") {
          await handle.iterator.return?.();
          this.#handles.delete(handleId);
          return undefined;
        }
        throw new Error(`Unsupported query cursor method: ${method}`);
      }
      case "live-set":
        return this.#callLiveSet(handleId, handle, method, args);
      case "live-subscription": {
        if (method !== "close") throw new Error(`Unsupported subscription method: ${method}`);
        handle.subscription.close();
        this.#handles.delete(handleId);
        const parent = this.#handles.get(handle.setId);
        if (parent?.type === "live-set") parent.subscriptionIds.delete(handleId);
        return undefined;
      }
    }
  }

  async #callWriteHandle(
    handleId: string,
    handle: Extract<Handle, { type: "write" }>,
    method: string,
    args: unknown[],
    context: RpcCallContext,
  ): Promise<unknown> {
    if (method === "query") {
      const [sql, options, reportStats = false] = args as [
        string,
        QueryOptions | undefined,
        boolean | undefined,
      ];
      return new ColumnarResult(
        encodeQueryResult(
          await handle.session.query(sql, {
            ...options,
            signal: context.signal,
            ...(reportStats
              ? {
                  onStats: (stats) => {
                    this.scope.postMessage(rpcEvent(context.requestId, "stats", stats));
                  },
                }
              : {}),
          }),
        ),
      );
    }
    if (method === "execute") {
      const [sql, params] = args as [string, readonly QueryValue[] | undefined];
      return handle.session.execute(sql, params);
    }
    if (method === "stage") {
      const [op, tableName, input] = args as [unknown, string, never];
      if (!isStageOp(op)) {
        throw new Error(`Unsupported write stage operation: ${String(op)}`);
      }
      return handle.session[op](tableName, input);
    }
    if (method === "commit") return this.#settleWriteHandle(handleId, handle, true);
    if (method === "abort") return this.#settleWriteHandle(handleId, handle, false);
    throw new Error(`Unsupported write method: ${method}`);
  }

  /**
   * Hands one slice of an encoded snapshot back. The slice is copied rather than viewed: a
   * subarray shares the whole buffer, and structured clone copies the buffer behind a view, not
   * the window into it — so every read would post the entire snapshot again.
   */
  async #callSnapshotExport(
    handleId: string,
    handle: Extract<Handle, { type: "snapshot-export" }>,
    method: string,
    args: unknown[],
  ): Promise<unknown> {
    switch (method) {
      case "read": {
        if (args.length !== 0) throw new TypeError("Snapshot stream read takes no arguments");
        if (handle.reading) throw new Error("Snapshot stream already has a read in flight");
        if (handle.done) return { done: true };
        handle.reading = true;
        try {
          const next = await handle.iterator.next();
          if (next.done === true) {
            handle.done = true;
            return { done: true };
          }
          // The iterator may yield a view into a 64 MiB block. Transfer only this bounded slice;
          // detaching the larger backing buffer would corrupt subsequent slices.
          const chunk = new Uint8Array(next.value.byteLength);
          chunk.set(next.value);
          return new SnapshotChunkResult(chunk);
        } finally {
          handle.reading = false;
        }
      }
      case "close": {
        await handle.iterator.return?.();
        this.#handles.delete(handleId);
        return undefined;
      }
      default:
        throw new Error(`Unsupported snapshot export method: ${method}`);
    }
  }

  /** Collects an uploaded snapshot chunk by chunk, then loads it, reporting progress as events. */
  async #callSnapshotImport(
    handleId: string,
    handle: Extract<Handle, { type: "snapshot-import" }>,
    method: string,
    args: unknown[],
  ): Promise<unknown> {
    switch (method) {
      case "write": {
        const chunk = args[0];
        if (!(chunk instanceof Uint8Array)) {
          throw new TypeError("Snapshot chunk must be a Uint8Array");
        }
        await handle.queue.push(chunk);
        return undefined;
      }
      case "finish": {
        try {
          handle.queue.finish();
          await handle.task;
        } finally {
          this.#handles.delete(handleId);
        }
        return undefined;
      }
      case "close": {
        handle.queue.fail(new Error("Snapshot import transfer was cancelled"));
        await handle.task.catch(() => undefined);
        this.#handles.delete(handleId);
        return undefined;
      }
      case "cancel": {
        handle.abort.abort(new Error("Snapshot import was cancelled"));
        handle.queue.fail(handle.abort.signal.reason);
        await handle.task.catch(() => undefined);
        this.#handles.delete(handleId);
        return undefined;
      }
      default:
        throw new Error(`Unsupported snapshot import method: ${method}`);
    }
  }

  async #callWriter(
    handleId: string,
    writer: BufferedTableWriter,
    method: string,
    args: unknown[],
  ): Promise<unknown> {
    switch (method) {
      case "add":
        return writer.add(args[0] as Readonly<Record<string, BatchValue>>);
      case "flush":
        return writer.flush();
      case "requestFlush":
        writer.requestFlush();
        return undefined;
      case "discard":
        return writer.discard();
      case "stats":
        return { pendingRowCount: writer.pendingRowCount, estimatedBytes: writer.estimatedBytes };
      case "close": {
        const result = await writer.close();
        this.#handles.delete(handleId);
        return result;
      }
      default:
        throw new Error(`Unsupported buffered writer method: ${method}`);
    }
  }

  async #callLiveSet(
    handleId: string,
    handle: Extract<Handle, { type: "live-set" }>,
    method: string,
    args: unknown[],
  ): Promise<unknown> {
    switch (method) {
      case "subscribe": {
        // The client names the subscription so it can route the initial change event, which is
        // posted before this call's own result frame.
        const subscriptionId = this.#claimHandleId(args[0]);
        const query = args[1] as LiveQueryInput;
        let subscription: LiveQuerySubscription | undefined;
        try {
          subscription = await handle.set.subscribe(query, {
            onChange: (result) => {
              const encoded = encodeQueryResult(result);
              this.scope.postMessage(rpcEvent(subscriptionId, "change", encoded.payload), {
                transfer: encoded.transfer,
              });
            },
            onError: (error) => {
              this.scope.postMessage(rpcEvent(subscriptionId, "error", serializeError(error)));
            },
            onComplete: () => {
              this.scope.postMessage(rpcEvent(subscriptionId, "complete", null));
              this.#handles.delete(subscriptionId);
              const parent = this.#handles.get(handleId);
              if (parent?.type === "live-set") parent.subscriptionIds.delete(subscriptionId);
            },
          });
          this.#publishHandle(subscriptionId, {
            type: "live-subscription",
            subscription,
            setId: handleId,
          });
        } catch (error) {
          subscription?.close();
          this.#releaseHandleId(subscriptionId);
          throw error;
        }
        handle.subscriptionIds.add(subscriptionId);
        return { dependencyTableIds: subscription.dependencyTableIds };
      }
      case "observe": {
        const subscriptionId = this.#claimHandleId(args[0]);
        const query = args[1] as LiveQueryInput;
        let subscription: LiveQuerySubscription | undefined;
        try {
          subscription = await handle.set.observe(query, {
            onInvalidate: (invalidation) => {
              this.scope.postMessage(rpcEvent(subscriptionId, "invalidate", invalidation));
            },
            onError: (error) => {
              this.scope.postMessage(rpcEvent(subscriptionId, "error", serializeError(error)));
            },
            onComplete: () => {
              this.scope.postMessage(rpcEvent(subscriptionId, "complete", null));
              this.#handles.delete(subscriptionId);
              const parent = this.#handles.get(handleId);
              if (parent?.type === "live-set") parent.subscriptionIds.delete(subscriptionId);
            },
          });
          this.#publishHandle(subscriptionId, {
            type: "live-subscription",
            subscription,
            setId: handleId,
          });
        } catch (error) {
          subscription?.close();
          this.#releaseHandleId(subscriptionId);
          throw error;
        }
        handle.subscriptionIds.add(subscriptionId);
        return { dependencyTableIds: subscription.dependencyTableIds };
      }
      case "stats":
        return handle.set.stats;
      case "notifyLocalCommit":
        handle.set.notifyLocalCommit();
        return undefined;
      case "refresh":
        return handle.set.refresh();
      case "close":
        this.#closeLiveSet(handleId, handle);
        return undefined;
      default:
        throw new Error(`Unsupported live query method: ${method}`);
    }
  }

  #claimHandleId(value: unknown): string {
    if (typeof value !== "string" || value.length === 0) {
      throw new TypeError("Handle ID must be a non-empty string");
    }
    if (this.#handles.has(value) || this.#reservedHandleIds.has(value)) {
      throw new Error(`Handle ID already exists: ${value}`);
    }
    if (
      this.#handles.size + this.#reservedHandleIds.size + this.#settlingWritePromises.size >=
      MAX_WORKER_HANDLES_PER_CONNECTION
    ) {
      throw new Error(
        `Database connection cannot hold more than ${String(MAX_WORKER_HANDLES_PER_CONNECTION)} open handles (active or settling)`,
      );
    }
    this.#reservedHandleIds.add(value);
    return value;
  }

  #claimGeneratedHandleId(): string {
    for (;;) {
      const id = crypto.randomUUID();
      if (!this.#handles.has(id) && !this.#reservedHandleIds.has(id))
        return this.#claimHandleId(id);
    }
  }

  #publishHandle(id: string, handle: Handle): void {
    if (this.#disposed) throw new Error("Database connection is disposed");
    if (!this.#reservedHandleIds.delete(id)) throw new Error(`Handle ID was not reserved: ${id}`);
    this.#handles.set(id, handle);
    if (handle.type === "write") this.#armWriteHandleIdleTimer(id, handle);
  }

  #releaseHandleId(id: string): void {
    this.#reservedHandleIds.delete(id);
  }

  #beginWriteHandleCall(handle: Extract<Handle, { type: "write" }>): void {
    if (!handle.open) throw new Error("Write handle is closed");
    if (handle.activeCalls !== 0) {
      throw new Error("Write handle already has a call in flight");
    }
    handle.activeCalls = 1;
    handle.activeCallDone = new Promise<void>((resolve) => {
      handle.finishActiveCall = resolve;
    });
    if (handle.idleTimer !== undefined) {
      clearTimeout(handle.idleTimer);
      handle.idleTimer = undefined;
    }
  }

  #endWriteHandleCall(handleId: string, handle: Extract<Handle, { type: "write" }>): void {
    handle.activeCalls -= 1;
    if (handle.activeCalls < 0) throw new Error("Write handle activity count underflow");
    const finishActiveCall = handle.finishActiveCall;
    handle.activeCallDone = undefined;
    handle.finishActiveCall = undefined;
    finishActiveCall?.();
    if (handle.open && handle.activeCalls === 0) this.#armWriteHandleIdleTimer(handleId, handle);
  }

  #armWriteHandleIdleTimer(handleId: string, handle: Extract<Handle, { type: "write" }>): void {
    if (!handle.open || handle.activeCalls !== 0 || this.#handles.get(handleId) !== handle) return;
    if (handle.idleTimer !== undefined) clearTimeout(handle.idleTimer);
    handle.idleTimer = setTimeout(() => {
      handle.idleTimer = undefined;
      if (!handle.open || handle.activeCalls !== 0 || this.#handles.get(handleId) !== handle)
        return;
      // Delete first: a late commit or stage frame must fail rather than revive an expired scope
      // while its asynchronous durable rollback is still joining.
      void this.#settleWriteHandle(handleId, handle, false).catch(() => undefined);
    }, this.#writeHandleIdleTimeoutMs);
    unrefTimer(handle.idleTimer);
  }

  async #settleWriteHandle(
    handleId: string,
    handle: Extract<Handle, { type: "write" }>,
    commit: boolean,
  ): Promise<{ version: number | null } | undefined> {
    if (!handle.open) throw new Error("Write handle is closed");
    handle.open = false;
    if (handle.idleTimer !== undefined) {
      clearTimeout(handle.idleTimer);
      handle.idleTimer = undefined;
    }
    if (this.#handles.get(handleId) === handle) this.#handles.delete(handleId);
    handle.finish(commit);
    const settlement = handle.done.catch((error: unknown) => {
      if (!commit && error instanceof WriteScopeAbortedError) return undefined;
      throw error;
    });
    this.#settlingWritePromises.add(settlement);
    void settlement.then(
      () => this.#settlingWritePromises.delete(settlement),
      () => this.#settlingWritePromises.delete(settlement),
    );
    return settlement;
  }

  #closeLiveSet(handleId: string, handle: Extract<Handle, { type: "live-set" }>): void {
    handle.set.close();
    for (const subscriptionId of handle.subscriptionIds) this.#handles.delete(subscriptionId);
    this.#handles.delete(handleId);
  }

  async #dispose(): Promise<void> {
    this.#disposed = true;
    this.#reservedHandleIds.clear();
    const handles = [...this.#handles.entries()];
    this.#handles.clear();
    const handleCleanup: Array<Promise<unknown>> = [];
    // Stop every abandoned-write deadline before waiting on active calls. The cleared handle map
    // also prevents their completions from rearming one while disposal is in progress.
    for (const [, handle] of handles) {
      if (handle.type === "write" && handle.idleTimer !== undefined) {
        clearTimeout(handle.idleTimer);
        handle.idleTimer = undefined;
      }
    }
    // Signal every handle before awaiting any cleanup. Imports and iterators can have an admitted
    // RPC blocked inside them; closing sequentially could wait on the first while never reaching
    // the signal that lets a later one finish.
    for (const [handleId, handle] of handles) {
      try {
        if (handle.type === "snapshot") {
          handle.release();
          handleCleanup.push(handle.done.catch(() => undefined));
        } else if (handle.type === "write") {
          // A session operation owns the transaction revision until it returns. Aborting sooner
          // can race its final journal update and leave an active owner until TTL. Once it exits,
          // abort only if commit/abort/idle expiry has not already chosen a terminal outcome.
          handleCleanup.push(
            (async () => {
              await handle.activeCallDone;
              if (handle.open) {
                await this.#settleWriteHandle(handleId, handle, false).catch(() => undefined);
              }
            })(),
          );
        } else if (handle.type === "writer") handleCleanup.push(handle.writer.close());
        else if (handle.type === "query-cursor") {
          handleCleanup.push(Promise.resolve(handle.iterator.return?.()));
        } else if (handle.type === "snapshot-export") {
          handleCleanup.push(Promise.resolve(handle.iterator.return?.()));
        } else if (handle.type === "snapshot-import") {
          handle.abort.abort(new Error("Database connection was disposed"));
          handle.queue.fail(new Error("Database connection was disposed"));
          handleCleanup.push(handle.task.catch(() => undefined));
        } else if (handle.type === "live-set") this.#closeLiveSet(handleId, handle);
      } catch {
        // Dispose every handle even when one close fails; the client is already gone.
      }
    }
    await Promise.allSettled(handleCleanup);
    // Commit/abort/expiry removes its handle before durable settlement completes so late RPCs
    // fail closed. Join those detached operations before closing the database or injected store.
    while (this.#settlingWritePromises.size > 0) {
      await Promise.allSettled([...this.#settlingWritePromises]);
    }
    // A handle whose lease admission was already in flight when disposal began is not yet in the
    // handle map. Its opener observes the disposed publication, releases any admitted lease, and
    // settles only after that cleanup is complete.
    while (this.#openingHandlePromises.size > 0) {
      await Promise.allSettled([...this.#openingHandlePromises]);
    }
    await this.#waitForInFlightRpcs();
    await this.database.close();
    await this.options.onDispose?.();
  }

  #waitForInFlightRpcs(): Promise<void> {
    if (this.#inFlightRpcCount === 0) return Promise.resolve();
    this.#inFlightRpcDrain ??= new Promise<void>((resolve) => {
      this.#resolveInFlightRpcDrain = resolve;
    });
    return this.#inFlightRpcDrain;
  }
}

/**
 * Answers client RPC frames against a database you constructed yourself. Use this in a custom
 * worker entry when you need non-cloneable construction options (a custom store, `now`,
 * `createId`) — the client's init frame is acknowledged without reconstructing anything.
 */
export function exposeDatabase(
  database: MinnowDatabase,
  scope: RpcScope,
  options: ExposeDatabaseOptions = {},
): void {
  const server = new DatabaseRpcServer(database, scope, options);
  scope.addEventListener("message", (event: MessageEvent<unknown>) => {
    let request: RpcRequest | null;
    try {
      request = parseRpcRequest(event.data);
    } catch (error) {
      const requestId = requestIdOf(event.data);
      if (requestId !== undefined) scope.postMessage(rpcFailure(requestId, error));
      return;
    }
    if (request !== null) void server.handle(request);
  });
}

/**
 * Full worker-side wiring: waits for the client's init frame, builds the store it describes,
 * constructs the database, and exposes it. The store is closed when the client disposes.
 */
export function attachDatabaseWorker(
  scope: RpcScope,
  options: AttachDatabaseWorkerOptions = {},
): void {
  let initialized: Promise<DatabaseRpcServer> | undefined;
  let initFailure: unknown;
  scope.addEventListener("message", (event: MessageEvent<unknown>) => {
    let request: RpcRequest | null;
    try {
      request = parseRpcRequest(event.data);
    } catch (error) {
      const requestId = requestIdOf(event.data);
      if (requestId !== undefined) scope.postMessage(rpcFailure(requestId, error));
      return;
    }
    if (request === null) return;
    if (request.kind === "rpc-init" && initialized === undefined) {
      initFailure = undefined;
      const attempt = createServer(scope, request.payload as DatabaseInitPayload, options);
      initialized = attempt;
      attempt.catch((error: unknown) => {
        // Leave the worker reusable: a failed store open (quota, blocked upgrade) may be
        // retried. Remember why it failed — the client pipelines calls behind init, so the
        // ones that land after this reset must carry the real reason, not a generic refusal.
        if (initialized === attempt) {
          initialized = undefined;
          initFailure = error;
        }
      });
    }
    const pending = initialized;
    if (pending === undefined) {
      const failure =
        initFailure === undefined
          ? new Error("Database is not initialized: send init first")
          : new Error(
              `Database initialization failed: ${
                initFailure instanceof Error
                  ? `${initFailure.name}: ${initFailure.message}`
                  : typeof initFailure === "string"
                    ? initFailure
                    : "unknown reason"
              }`,
              { cause: initFailure },
            );
      scope.postMessage(rpcFailure(request.requestId, failure));
      return;
    }
    void pending
      .then((server) => server.handle(request))
      .catch((error: unknown) => {
        scope.postMessage(rpcFailure(request.requestId, error));
      });
  });
}

async function createServer(
  scope: RpcScope,
  payload: DatabaseInitPayload,
  options: AttachDatabaseWorkerOptions,
): Promise<DatabaseRpcServer> {
  const store = await createStore(payload.store, options);
  const database = new MinnowDatabase(store, payload.options ?? {});
  return new DatabaseRpcServer(database, scope, {
    ...(payload.options?.transactionIdleTimeoutMs === undefined
      ? {}
      : { writeHandleIdleTimeoutMs: payload.options.transactionIdleTimeoutMs }),
    onDispose: () => store.close(),
    onVisibility: (visible) => {
      (store as { setForeground?: (foreground: boolean) => void }).setForeground?.(visible);
    },
  });
}

/**
 * The composition root: the one place adapters are named. Each loads only when its descriptor
 * asks for it — a bundler splits the adapters into their own chunks, so an application's worker
 * downloads the store it opens and none of the others.
 */
async function createStore(
  descriptor: StoreDescriptor,
  options: AttachDatabaseWorkerOptions,
): Promise<BlockStore> {
  if (descriptor.kind === "memory") {
    const { MemoryBlockStore } = await import("../storage/memory.js");
    return new MemoryBlockStore();
  }
  if (descriptor.kind === "opfs") {
    const { OpfsBlockStore } = await import("../storage/opfs/index.js");
    return OpfsBlockStore.open({
      name: descriptor.name,
      ...(descriptor.durability === undefined ? {} : { durability: descriptor.durability }),
    });
  }
  const { IndexedDbBlockStore } = await import("../storage/indexeddb.js");
  return IndexedDbBlockStore.open({
    name: descriptor.name,
    ...(descriptor.durability === undefined ? {} : { durability: descriptor.durability }),
    ...(descriptor.uniqueKeyCacheBytes === undefined
      ? {}
      : { uniqueKeyCacheBytes: descriptor.uniqueKeyCacheBytes }),
    ...(options.indexedDB === undefined ? {} : { indexedDB: options.indexedDB }),
  });
}

function requestIdOf(value: unknown): string | undefined {
  if (typeof value === "object" && value !== null && "requestId" in value) {
    const requestId = value.requestId;
    if (typeof requestId === "string") return requestId;
  }
  return undefined;
}

/** A worker cleanup deadline must not keep a Node process alive by itself. */
function unrefTimer(timer: ReturnType<typeof setTimeout>): void {
  const candidate = timer as unknown as { unref?: () => void };
  candidate.unref?.();
}
