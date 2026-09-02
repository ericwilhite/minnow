/// <reference lib="webworker" />
/**
 * Dedicated-worker entry that bundles only the OPFS store, published as
 * `@minnowdb/core/worker/opfs`. Use it in place of `@minnowdb/core/worker` when the bundler
 * cannot split worker code (Vite's default `iife` worker format inlines every adapter the stock
 * entry can load). An init frame for another store kind is refused with an error that names the
 * entry supporting it.
 *
 *   // db-worker.ts: import "@minnowdb/core/worker/opfs";
 *   new Worker(new URL("./db-worker.ts", import.meta.url), { type: "module" })
 */
import { attachWorkerHost } from "./worker-server.js";
import { opfsWorkerStore } from "./worker-store-opfs.js";

attachWorkerHost(self, opfsWorkerStore);
