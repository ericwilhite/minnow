/// <reference lib="webworker" />
/**
 * Ready-made dedicated-worker entry, published as `@minnowdb/core/worker`. Point a
 * module worker at it and connect a MinnowDatabaseClient — the client's init frame carries the
 * store descriptor and options, so this entry needs no configuration of its own:
 *
 *   // db-worker.ts: import "@minnowdb/core/worker";
 *   new Worker(new URL("./db-worker.ts", import.meta.url), { type: "module" })
 *
 * For non-cloneable construction options (custom store, `now`, `createId`), write your own entry
 * and import `exposeDatabase()` from `@minnowdb/core/worker-host` instead.
 */
import { attachDatabaseWorker } from "./worker-host.js";

attachDatabaseWorker(self);
