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
  MinnowDatabase,
  type MinnowDatabaseOptions,
  type BufferedTableWriter,
  type BatchValue,
  type BufferedWriterOptions,
} from "./database.js";
import { LiveQuerySet, type LiveQueryInput, type LiveQuerySubscription } from "./live.js";
import type { PreparedQuery } from "./query.js";
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
  "compression" | "rowsPerBlock" | "maxCommitRetries" | "spillOwnerLeaseMs" | "prepareCacheBytes"
>;

export interface DatabaseInitPayload {
  store: StoreDescriptor;
  options?: WireDatabaseOptions;
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
  | { type: "prepared"; prepared: PreparedQuery }
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
      case "prepareQuery": {
        const prepared = await this.database.prepareQuery(
          args[0] as string,
          args[1] as Parameters<MinnowDatabase["prepareQuery"]>[1],
        );
        const handleId = crypto.randomUUID();
        this.#handles.set(handleId, { type: "prepared", prepared });
        return { handleId, sql: prepared.sql, tables: prepared.tables };
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
      case "prepared":
        return this.#callPrepared(handleId, handle.prepared, method);
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

  #callPrepared(handleId: string, prepared: PreparedQuery, method: string): unknown {
    switch (method) {
      case "execute":
        return prepared.execute();
      case "memoryUsage":
        return prepared.memoryUsage;
      case "close":
        prepared.close();
        this.#handles.delete(handleId);
        return undefined;
      default:
        throw new Error(`Unsupported prepared query method: ${method}`);
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
        if (handle.type === "prepared") handle.prepared.close();
        else if (handle.type === "writer") await handle.writer.close();
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
