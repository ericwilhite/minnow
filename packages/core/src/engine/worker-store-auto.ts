import { IndexedDbBlockStore } from "../storage/indexeddb.js";
import { OpfsBlockStore } from "../storage/opfs/index.js";
import { openAutoStore } from "./auto-store.js";
import type { BlockStore } from "../storage/types.js";
import { unsupportedStoreKindError, type WorkerStoreFactory } from "./worker-server.js";

/**
 * The store factory behind `@minnowdb/core/worker/auto`: both durable adapters, statically, so a
 * bundler that cannot split a worker still ships only the two an `auto` descriptor can resolve
 * to. Explicit `opfs` and `indexeddb` descriptors open as they would on their own entries.
 */
export const autoWorkerStore: WorkerStoreFactory = async (descriptor, options) => {
  const diagnostic =
    options.onDiagnostic === undefined ? {} : { onDiagnostic: options.onDiagnostic };
  switch (descriptor.kind) {
    case "opfs":
      return OpfsBlockStore.open({
        name: descriptor.name,
        ...(descriptor.durability === undefined ? {} : { durability: descriptor.durability }),
        ...diagnostic,
      });
    case "indexeddb":
      return IndexedDbBlockStore.open({
        name: descriptor.name,
        ...(descriptor.durability === undefined ? {} : { durability: descriptor.durability }),
        ...(descriptor.uniqueKeyCacheBytes === undefined
          ? {}
          : { uniqueKeyCacheBytes: descriptor.uniqueKeyCacheBytes }),
      });
    case "auto":
      return openAutoStore(descriptor.name, (kind): Promise<BlockStore> =>
        kind === "opfs"
          ? OpfsBlockStore.open({
              name: descriptor.name,
              ...(descriptor.opfs?.durability === undefined
                ? {}
                : { durability: descriptor.opfs.durability }),
              ...diagnostic,
            })
          : IndexedDbBlockStore.open({
              name: descriptor.name,
              ...(descriptor.indexeddb?.durability === undefined
                ? {}
                : { durability: descriptor.indexeddb.durability }),
              ...(descriptor.indexeddb?.uniqueKeyCacheBytes === undefined
                ? {}
                : { uniqueKeyCacheBytes: descriptor.indexeddb.uniqueKeyCacheBytes }),
            }),
      );
    default:
      throw unsupportedStoreKindError("auto", descriptor.kind);
  }
};
