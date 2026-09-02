/**
 * The public worker-host surface (`@minnowdb/core/worker-host`): the RPC server from
 * worker-server.ts plus attachDatabaseWorker(), whose stock store factory can open any of the
 * three shipped stores. That factory is the one place every adapter is named, so it lives here
 * rather than beside the server — the per-store entries import worker-server.ts directly and a
 * bundler that cannot split worker code never sees the adapters they leave out.
 */
import type { BlockStore } from "../storage/types.js";
import {
  attachWorkerHost,
  type RpcScope,
  type StoreDescriptor,
  type WorkerStoreFactory,
  type WorkerStoreOptions,
} from "./worker-server.js";

export {
  exposeDatabase,
  singleStoreFactory,
  MAX_WORKER_HANDLES_PER_CONNECTION,
  type DatabaseInitPayload,
  type ExposeDatabaseOptions,
  type RpcScope,
  type StoreDescriptor,
  type WireDatabaseOptions,
  type WorkerStoreFactory,
  type WorkerStoreOptions,
} from "./worker-server.js";

export interface AttachDatabaseWorkerOptions extends WorkerStoreOptions {
  /**
   * Replaces the stock store factory. `singleStoreFactory()` builds one that opens a single
   * kind and refuses the rest, the way the `@minnowdb/core/worker/{indexeddb,opfs,memory}`
   * entries do — though a custom entry importing this module still carries every adapter the
   * stock factory names unless its bundler splits worker code.
   */
  createStore?: WorkerStoreFactory;
}

/**
 * Full worker-side wiring: waits for the client's init frame, builds the store it describes,
 * constructs the database, and exposes it. The store is closed when the client disposes.
 */
export function attachDatabaseWorker(
  scope: RpcScope,
  options: AttachDatabaseWorkerOptions = {},
): void {
  attachWorkerHost(scope, options.createStore ?? createStore, options);
}

/**
 * The stock composition root: the one place every adapter is named. Each loads only when its
 * descriptor asks for it, so a bundler that splits worker code (esbuild `--splitting`, Vite
 * `worker.format: "es"`) emits the adapters as lazy chunks and the worker downloads the store it
 * opens. A bundler that cannot split a worker (Vite's default `iife` worker format) inlines all
 * three; the per-store entries exist for that case.
 */
async function createStore(
  descriptor: StoreDescriptor,
  options: WorkerStoreOptions,
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
