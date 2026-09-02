/**
 * The OPFS-only store factory behind `@minnowdb/core/worker/opfs`. Kept apart from the entry so
 * a test can attach it to an in-process scope without the entry's `self` side effect.
 */
import { OpfsBlockStore } from "../storage/opfs/index.js";
import { singleStoreFactory, type WorkerStoreFactory } from "./worker-server.js";

export const opfsWorkerStore: WorkerStoreFactory = singleStoreFactory("opfs", (descriptor) =>
  OpfsBlockStore.open({
    name: descriptor.name,
    ...(descriptor.durability === undefined ? {} : { durability: descriptor.durability }),
  }),
);
