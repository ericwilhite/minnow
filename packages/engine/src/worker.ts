/// <reference lib="webworker" />
/**
 * Ready-made dedicated-worker entry, published as `@browserdatabase/engine/worker`. Point a
 * module worker at it and connect a BrowserDatabaseClient — the client's init frame carries the
 * store descriptor and options, so this entry needs no configuration of its own:
 *
 *   new Worker(new URL("@browserdatabase/engine/worker", import.meta.url), { type: "module" })
 *
 * For non-cloneable construction options (custom store, `now`, `createId`), write your own entry
 * and call exposeDatabase() instead.
 */
import { attachDatabaseWorker } from "./worker-host.js";

attachDatabaseWorker(self);
