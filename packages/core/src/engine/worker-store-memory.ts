/**
 * The memory-only store factory behind `@minnowdb/core/worker/memory`. Kept apart from the entry
 * so a test can attach it to an in-process scope without the entry's `self` side effect.
 */
import { MemoryBlockStore } from "../storage/memory.js";
import { singleStoreFactory, type WorkerStoreFactory } from "./worker-server.js";

export const memoryWorkerStore: WorkerStoreFactory = singleStoreFactory(
  "memory",
  () => new MemoryBlockStore(),
);
