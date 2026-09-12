/// <reference lib="webworker" />
/**
 * Dedicated-worker entry that bundles the two durable stores, published as
 * `@minnowdb/core/worker/auto`. It serves `{ kind: "auto", name }` — OPFS where the worker can
 * hold synchronous access handles, IndexedDB elsewhere, remembered per name — as well as an
 * explicit `opfs` or `indexeddb` descriptor. Use it in place of `@minnowdb/core/worker` when the
 * bundler cannot split worker code and the in-memory store is not wanted in the bundle.
 *
 *   // db-worker.ts: import "@minnowdb/core/worker/auto";
 *   new Worker(new URL("./db-worker.ts", import.meta.url), { type: "module" })
 */
import { attachWorkerHost } from "./worker-server.js";
import { autoWorkerStore } from "./worker-store-auto.js";

attachWorkerHost(self, autoWorkerStore);
