/**
 * The storage surface, in three layers with enforced dependency directions
 * (contract-boundaries.test.ts):
 *
 * - **The contract** — `types.ts` and `snapshot.ts`: the `BlockStore` capability interfaces,
 *   record types, and error classes. The engine depends on nothing else here.
 * - **The adapters** — `indexeddb.ts`, `memory.ts`, `opfs/`: implementations, each owning its
 *   own low-level strategy.
 * - **The toolkit** — `toolkit/`, published as `@minnowdb/core/storage/toolkit` rather than
 *   from this barrel: optional building blocks for writing new adapters. The contract never
 *   references it.
 */
export * from "./indexeddb.js";
export * from "./memory.js";
export * from "./opfs/index.js";
export * from "./persistence.js";
export * from "./snapshot.js";
export * from "./types.js";
