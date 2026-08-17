import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import {
  parseRpcRequest,
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
} from "./database.js";
import { LiveQuerySet, type LiveQueryInput, type LiveQuerySubscription } from "./live.js";
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
  { kind: "memory" } | { kind: "indexeddb"; name: string; durability?: IDBTransactionDurability };

/** The cloneable subset of MinnowDatabaseOptions; function-valued seams stay worker-side. */
export type WireDatabaseOptions = Pick<
  MinnowDatabaseOptions,
  "compression" | "rowsPerBlock" | "maxCommitRetries" | "spillOwnerLeaseMs" | "bufferPoolBytes"
>;

export interface DatabaseInitPayload {
  store: StoreDescriptor;
  options?: WireDatabaseOptions;
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

class WriteScopeAbortedError extends Error {
  override readonly name = "WriteScopeAbortedError";

  constructor() {
    super("Write scope aborted");
  }
}

/** The slice of DedicatedWorkerGlobalScope (or MessagePort) the host needs. */
export interface RpcScope {
  addEventListener(type: "message", listener: (event: MessageEvent<unknown>) => void): void;
  postMessage(message: unknown): void;
}

export interface ExposeDatabaseOptions {
  /** Called when the client sends its dispose frame, after every handle has been closed. */
  onDispose?: () => void | Promise<void>;
}

export interface AttachDatabaseWorkerOptions {
  /** Test seam for environments without a global IndexedDB (e.g. fake-indexeddb). */
  indexedDB?: IDBFactory;
}

type Handle =
  | { type: "snapshot"; release: () => void }
  | { type: "snapshot-export"; bytes: Uint8Array }
  | { type: "snapshot-import"; chunks: Uint8Array[]; byteLength: number }
  | {
      type: "write";
      session: WriteSession;
      finish: (commit: boolean) => void;
      done: Promise<{ version: number | null }>;
    }
  | { type: "writer"; writer: BufferedTableWriter }
  | { type: "live-set"; set: LiveQuerySet; subscriptionIds: Set<string> }
  | { type: "live-subscription"; subscription: LiveQuerySubscription; setId: string };

class DatabaseRpcServer {
  readonly #handles = new Map<string, Handle>();
  #disposed = false;

  constructor(
    private readonly database: MinnowDatabase,
    private readonly scope: RpcScope,
    private readonly options: ExposeDatabaseOptions,
  ) {}

  async handle(request: RpcRequest): Promise<void> {
    if (request.kind === "rpc-init") {
      this.scope.postMessage(rpcResult(request.requestId, { ready: true }));
      return;
    }
    try {
      if (this.#disposed) throw new Error("Database connection is disposed");
      const result =
        request.handleId === null
          ? await this.#callRoot(request.method, request.args)
          : await this.#callHandle(request.handleId, request.method, request.args);
      this.scope.postMessage(rpcResult(request.requestId, result));
    } catch (error) {
      this.scope.postMessage(rpcFailure(request.requestId, error));
    }
  }

  async #callRoot(method: string, args: unknown[]): Promise<unknown> {
    const database = this.database as unknown as Record<
      string,
      (...forwarded: unknown[]) => Promise<unknown>
    >;
    switch (method) {
      case "createTable":
      case "listTables":
      case "insertBatch":
      case "insert":
      case "upsertBatch":
      case "upsert":
      case "updateBatch":
      case "update":
      case "deleteBatch":
      case "readTable":
      case "query":
      case "run":
      case "runStatement":
      case "explain":
      case "execute":
      case "listVisibleSegments":
      case "cleanupQuerySpill":
      case "compactTable":
      case "compactTableStep":
      case "resumeCompactionJob":
      case "listCompactionJobs":
      case "cancelCompactionJob":
      case "collectGarbage":
      case "collectGarbageStep":
      case "resumeGarbageCollectionJob":
      case "bufferPoolStats":
      case "listGarbageCollectionJobs":
        return database[method]?.(...args);
      case "migrate": {
        const result = await this.database.migrate(deserializeSchema(args[0] as WireSchema));
        return {
          createdTables: result.createdTables,
          alteredTables: result.alteredTables,
          steps: serializeMigrationSteps(result.steps),
        };
      }
      case "writeOpen": {
        // Same deferred-callback shape as snapshotOpen: the scoped write() stays pending
        // until the client commits or aborts, so its staged transaction spans the RPC session.
        const handleId = crypto.randomUUID();
        let sessionResolve!: (session: WriteSession) => void;
        const sessionReady = new Promise<WriteSession>((resolveSession) => {
          sessionResolve = resolveSession;
        });
        let finish!: (commit: boolean) => void;
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
        const session = await sessionReady;
        this.#handles.set(handleId, { type: "write", session, finish, done });
        return { handleId };
      }
      case "snapshotOpen": {
        // The scoped snapshot() holds its lease until the callback settles; the handle keeps
        // the callback pending until the client closes it, so the pin spans the RPC session.
        const handleId = crypto.randomUUID();
        let release!: () => void;
        const version = await new Promise<number | null>((resolveVersion, rejectVersion) => {
          void this.database
            .snapshot(async (session) => {
              resolveVersion(session.version);
              await new Promise<void>((resolveRelease) => {
                release = resolveRelease;
              });
            })
            .catch((error: unknown) => {
              rejectVersion(error instanceof Error ? error : new Error(String(error)));
            });
        });
        this.#handles.set(handleId, { type: "snapshot", release });
        return { handleId, version };
      }
      case "exportSnapshotOpen": {
        // The snapshot is encoded here and held by the handle so the client can pull it across
        // in slices. One result frame carrying the whole thing would be a structured clone the
        // size of the database, and the main thread would sit still for all of it.
        const handleId = crypto.randomUUID();
        const bytes = await this.database.exportSnapshot();
        this.#handles.set(handleId, { type: "snapshot-export", bytes });
        return { handleId, byteLength: bytes.byteLength };
      }
      case "importSnapshotOpen": {
        // Named by the client, like the other event-producing handles, so its progress route
        // exists before the first frame the load can emit.
        const handleId = this.#claimHandleId(args[0]);
        this.#handles.set(handleId, { type: "snapshot-import", chunks: [], byteLength: 0 });
        return { handleId };
      }
      case "bufferedWriter": {
        // Event-producing handles are named by the client so its event routes exist before the
        // first frame the handle can emit.
        const handleId = this.#claimHandleId(args[0]);
        const writer = this.database.bufferedWriter(args[1] as string, {
          ...(args[2] as Omit<BufferedWriterOptions, "onError">),
          onError: (error) => {
            this.scope.postMessage(rpcEvent(handleId, "error", serializeError(error)));
          },
        });
        this.#handles.set(handleId, { type: "writer", writer });
        return { handleId };
      }
      case "liveQueries": {
        const handleId = this.#claimHandleId(args[0]);
        const { pollIntervalMs, channelName } = (args[1] ?? {}) as {
          pollIntervalMs?: number;
          channelName?: string;
        };
        // The database owns channelName resolution (and closes the channel with the set).
        const set = this.database.liveQueries({
          ...(channelName === undefined ? {} : { channelName }),
          ...(pollIntervalMs === undefined ? {} : { pollIntervalMs }),
        });
        this.#handles.set(handleId, { type: "live-set", set, subscriptionIds: new Set() });
        return { handleId };
      }
      case "dispose": {
        await this.#dispose();
        return { disposed: true };
      }
      default:
        throw new Error(`Unsupported database method: ${method}`);
    }
  }

  async #callHandle(handleId: string, method: string, args: unknown[]): Promise<unknown> {
    const handle = this.#handles.get(handleId);
    if (handle === undefined) throw new Error(`Unknown handle: ${handleId}`);
    switch (handle.type) {
      case "snapshot": {
        if (method !== "close") throw new Error(`Unsupported snapshot method: ${method}`);
        handle.release();
        this.#handles.delete(handleId);
        return undefined;
      }
      case "write": {
        if (method === "query") {
          const [sql, options] = args as [string, { params?: unknown } | undefined];
          return handle.session.query(sql, options as never);
        }
        if (method === "stage") {
          const [op, tableName, input] = args as [unknown, string, never];
          if (!isStageOp(op)) {
            throw new Error(`Unsupported write stage operation: ${String(op)}`);
          }
          return handle.session[op](tableName, input);
        }
        if (method === "commit") {
          handle.finish(true);
          this.#handles.delete(handleId);
          return handle.done;
        }
        if (method === "abort") {
          handle.finish(false);
          this.#handles.delete(handleId);
          return handle.done.catch((error: unknown) => {
            if (error instanceof WriteScopeAbortedError) return undefined;
            throw error;
          });
        }
        throw new Error(`Unsupported write method: ${method}`);
      }
      case "snapshot-export":
        return this.#callSnapshotExport(handleId, handle, method, args);
      case "snapshot-import":
        return this.#callSnapshotImport(handleId, handle, method, args);
      case "writer":
        return this.#callWriter(handleId, handle.writer, method, args);
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

  /**
   * Hands one slice of an encoded snapshot back. The slice is copied rather than viewed: a
   * subarray shares the whole buffer, and structured clone copies the buffer behind a view, not
   * the window into it — so every read would post the entire snapshot again.
   */
  #callSnapshotExport(
    handleId: string,
    handle: Extract<Handle, { type: "snapshot-export" }>,
    method: string,
    args: unknown[],
  ): unknown {
    switch (method) {
      case "read": {
        const [offset, length] = args as [unknown, unknown];
        if (!Number.isSafeInteger(offset) || (offset as number) < 0) {
          throw new RangeError("Snapshot read offset must be a non-negative whole number");
        }
        if (!Number.isSafeInteger(length) || (length as number) <= 0) {
          throw new RangeError("Snapshot read length must be a positive whole number");
        }
        const start = offset as number;
        return handle.bytes.slice(start, start + (length as number));
      }
      case "close": {
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
        handle.chunks.push(chunk);
        handle.byteLength += chunk.byteLength;
        return undefined;
      }
      case "finish": {
        const bytes = new Uint8Array(handle.byteLength);
        let offset = 0;
        for (const chunk of handle.chunks) {
          bytes.set(chunk, offset);
          offset += chunk.byteLength;
        }
        // Dropped before the load starts: the decoded snapshot is another copy of these bytes,
        // and holding the chunks too would make three of them.
        handle.chunks.length = 0;
        try {
          await this.database.importSnapshot(bytes, {
            onProgress: (progress) => {
              this.scope.postMessage(rpcEvent(handleId, "progress", progress));
            },
          });
        } finally {
          this.#handles.delete(handleId);
        }
        return undefined;
      }
      case "close": {
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
        const subscription = await handle.set.subscribe(query, {
          onChange: (result) => {
            this.scope.postMessage(rpcEvent(subscriptionId, "change", result));
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
        this.#handles.set(subscriptionId, {
          type: "live-subscription",
          subscription,
          setId: handleId,
        });
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
    if (this.#handles.has(value)) throw new Error(`Handle ID already exists: ${value}`);
    return value;
  }

  #closeLiveSet(handleId: string, handle: Extract<Handle, { type: "live-set" }>): void {
    handle.set.close();
    for (const subscriptionId of handle.subscriptionIds) this.#handles.delete(subscriptionId);
    this.#handles.delete(handleId);
  }

  async #dispose(): Promise<void> {
    this.#disposed = true;
    const handles = [...this.#handles.entries()];
    this.#handles.clear();
    for (const [handleId, handle] of handles) {
      try {
        if (handle.type === "snapshot") handle.release();
        else if (handle.type === "write") {
          handle.finish(false);
          await handle.done.catch(() => undefined);
        } else if (handle.type === "writer") await handle.writer.close();
        else if (handle.type === "live-set") this.#closeLiveSet(handleId, handle);
      } catch {
        // Dispose every handle even when one close fails; the client is already gone.
      }
    }
    await this.options.onDispose?.();
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
      const attempt = createServer(scope, request.payload as DatabaseInitPayload, options);
      initialized = attempt;
      attempt.catch(() => {
        // Leave the worker reusable: a failed store open (quota, blocked upgrade) may be retried.
        if (initialized === attempt) initialized = undefined;
      });
    }
    const pending = initialized;
    if (pending === undefined) {
      scope.postMessage(
        rpcFailure(request.requestId, new Error("Database is not initialized: send init first")),
      );
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
  return new DatabaseRpcServer(database, scope, { onDispose: () => store.close() });
}

async function createStore(
  descriptor: StoreDescriptor,
  options: AttachDatabaseWorkerOptions,
): Promise<BlockStore> {
  if (descriptor.kind === "memory") return new MemoryBlockStore();
  return IndexedDbBlockStore.open({
    name: descriptor.name,
    ...(descriptor.durability === undefined ? {} : { durability: descriptor.durability }),
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
