/// <reference lib="webworker" />
/**
 * Dedicated-worker entry that bundles only the IndexedDB store, published as
 * `@minnowdb/core/worker/indexeddb`. Use it in place of `@minnowdb/core/worker` when the bundler
 * cannot split worker code (Vite's default `iife` worker format inlines every adapter the stock
 * entry can load). An init frame for another store kind is refused with an error that names the
 * entry supporting it.
 *
 *   // db-worker.ts: import "@minnowdb/core/worker/indexeddb";
 *   new Worker(new URL("./db-worker.ts", import.meta.url), { type: "module" })
 */
import { attachWorkerHost } from "./worker-server.js";
import { indexedDbWorkerStore } from "./worker-store-indexeddb.js";

attachWorkerHost(self, indexedDbWorkerStore);
