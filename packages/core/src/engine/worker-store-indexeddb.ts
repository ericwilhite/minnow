/**
 * The IndexedDB-only store factory behind `@minnowdb/core/worker/indexeddb`. Kept apart from the
 * entry so a test can attach it to an in-process scope without the entry's `self` side effect.
 */
import { IndexedDbBlockStore } from "../storage/indexeddb.js";
import { singleStoreFactory, type WorkerStoreFactory } from "./worker-server.js";

export const indexedDbWorkerStore: WorkerStoreFactory = singleStoreFactory(
  "indexeddb",
  (descriptor, options) =>
    IndexedDbBlockStore.open({
      name: descriptor.name,
      ...(descriptor.durability === undefined ? {} : { durability: descriptor.durability }),
      ...(descriptor.uniqueKeyCacheBytes === undefined
        ? {}
        : { uniqueKeyCacheBytes: descriptor.uniqueKeyCacheBytes }),
      ...(options.indexedDB === undefined ? {} : { indexedDB: options.indexedDB }),
    }),
);
