/**
 * One browser tab of the interaction-plan simulator.
 *
 * The plan runner and its shadow model live in the Playwright test process; each connection the
 * plan names is a real tab holding a `MinnowDatabaseClient` over the published module worker and
 * a real IndexedDB or OPFS database shared by every tab. This page exposes exactly the
 * `SimulatedConnection` surface the runner drives -- execute, query, reopen, maintain, and a
 * crash that terminates the worker with a call in flight -- so the same seeded plan that runs
 * in-process over the Node stores runs across real tabs, real workers, and real storage.
 */
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import type { QueryValue } from "@minnowdb/core";

type StoreKind = "indexeddb" | "opfs";

interface TabExecuteResult {
  kind: string;
  rowCount?: number;
}

interface TabQueryResult {
  columns: string[];
  rows: Array<Record<string, unknown>>;
}

/** Errors cross `page.evaluate` as plain text, so the name travels inside the message. */
interface TabFailure {
  failed: true;
  name: string;
  message: string;
}

let client: MinnowDatabaseClient | undefined;
let worker: Worker | undefined;
let descriptor: { kind: StoreKind; name: string } | undefined;
const pageErrors: string[] = [];

window.addEventListener("error", (event) => pageErrors.push(event.message));
window.addEventListener("unhandledrejection", (event) =>
  pageErrors.push(event.reason instanceof Error ? event.reason.message : String(event.reason)),
);

function spawn(): Worker {
  return new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
}

async function connect(): Promise<MinnowDatabaseClient> {
  if (descriptor === undefined) throw new Error("open() first");
  worker = spawn();
  client = new MinnowDatabaseClient(worker, {
    store: descriptor,
    onWorkerError: (event) => pageErrors.push(`${event.kind}: ${event.error.message}`),
  });
  await client.ready();
  return client;
}

function failure(error: unknown): TabFailure {
  const name = error instanceof Error ? error.name : "Error";
  const message = error instanceof Error ? error.message : String(error);
  return { failed: true, name, message };
}

const tab = {
  async open(kind: StoreKind, name: string): Promise<void> {
    descriptor = { kind, name };
    await connect();
  },
  async execute(sql: string, params?: QueryValue[]): Promise<TabExecuteResult | TabFailure> {
    if (client === undefined) throw new Error("open() first");
    try {
      const result = await client.execute(sql, params);
      return "rowCount" in result
        ? { kind: result.kind, rowCount: result.rowCount }
        : { kind: result.kind };
    } catch (error) {
      return failure(error);
    }
  },
  async query(sql: string, params?: QueryValue[]): Promise<TabQueryResult | TabFailure> {
    if (client === undefined) throw new Error("open() first");
    try {
      const result = await client.query(
        sql,
        params === undefined ? { memoize: false } : { memoize: false, params },
      );
      return { columns: [...result.columns], rows: result.rows };
    } catch (error) {
      return failure(error);
    }
  },
  async reopen(): Promise<void> {
    if (client !== undefined) {
      await client.close({ terminateWorker: true }).catch(() => undefined);
    }
    await connect();
  },
  /** Terminates the worker outright, mid-call if one is pending -- a tab crash. */
  async crash(): Promise<void> {
    // Let a call issued just before reach the worker's queue before the process dies.
    await new Promise((resolve) => setTimeout(resolve, 2));
    worker?.terminate();
  },
  async maintain(table: string): Promise<void> {
    if (client === undefined) throw new Error("open() first");
    await client.compactTable(table);
    await client.collectGarbage();
  },
  async close(): Promise<void> {
    await client?.close({ terminateWorker: true }).catch(() => undefined);
    client = undefined;
  },
  pageErrors(): string[] {
    return [...pageErrors];
  },
};

Object.assign(window, { simulatorTab: tab });
const ready = document.querySelector("#ready");
if (ready !== null) ready.textContent = "Interaction simulator tab ready";
