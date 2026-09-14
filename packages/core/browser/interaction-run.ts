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
import { MinnowDatabaseClient, type DatabaseWorkerErrorEvent } from "@minnowdb/core/client";
import type { QueryValue } from "@minnowdb/core";
import { serializeError, type SerializedError } from "@minnowdb/core/worker-protocol";

type StoreKind = "indexeddb" | "opfs";

interface TabExecuteResult {
  kind: string;
  rowCount?: number;
}

interface TabQueryResult {
  columns: string[];
  rows: Array<Record<string, unknown>>;
}

/** Explicit transport keeps rehydrated worker errors intact across Playwright's page boundary. */
interface TabFailure extends SerializedError {
  failed: true;
}

interface TabError {
  source: "window-error" | "unhandled-rejection" | "worker";
  error: SerializedError;
  kind?: DatabaseWorkerErrorEvent["kind"];
  context?: string;
}

let client: MinnowDatabaseClient | undefined;
let worker: Worker | undefined;
let crashed = false;
let descriptor: { kind: StoreKind; name: string } | undefined;
const pageErrors: TabError[] = [];

window.addEventListener("error", (event) =>
  pageErrors.push({
    source: "window-error",
    error: serializeError(event.error ?? new Error(event.message)),
  }),
);
window.addEventListener("unhandledrejection", (event) => {
  pageErrors.push({
    source: "unhandled-rejection",
    error: serializeError(event.reason),
  });
});

function spawn(): Worker {
  return new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
}

async function connect(): Promise<MinnowDatabaseClient> {
  if (descriptor === undefined) throw new Error("open() first");
  crashed = false;
  worker = spawn();
  client = new MinnowDatabaseClient(worker, {
    store: descriptor,
    onWorkerError: (event) =>
      pageErrors.push({
        source: "worker",
        kind: event.kind,
        context: event.context,
        error: serializeError(event.error),
      }),
  });
  await client.ready();
  return client;
}

function failure(error: unknown): TabFailure {
  return { failed: true, ...serializeError(error) };
}

async function lifecycle(action: () => Promise<void>): Promise<TabFailure | undefined> {
  try {
    await action();
    return undefined;
  } catch (error) {
    return failure(error);
  }
}

const tab = {
  open(kind: StoreKind, name: string): Promise<TabFailure | undefined> {
    return lifecycle(async () => {
      descriptor = { kind, name };
      await connect();
    });
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
  reopen(): Promise<TabFailure | undefined> {
    return lifecycle(async () => {
      // A crashed connection is already gone; only a live one has a worker worth disposing.
      if (client !== undefined && !crashed) {
        await client.close({ terminateWorker: true });
      }
      await connect();
    });
  },
  /** Terminates the worker outright, mid-call if one is pending -- a tab crash. */
  async crash(): Promise<void> {
    // Let a call issued just before reach the worker's queue before the process dies.
    await new Promise((resolve) => setTimeout(resolve, 2));
    worker?.terminate();
    crashed = true;
    // A page cannot observe its own `terminate()`, and the client's deadline measures silence,
    // so a call the dead worker was carrying would otherwise sit for the whole request timeout
    // before reporting an unknown outcome -- a minute of it, long enough to expire another tab's
    // open SQL transaction, which is not what a crash means. Report the loss here instead: the
    // in-flight call fails at once as `DatabaseWorkerOutcomeUnknownError`, exactly what a
    // crashed connection leaves behind.
    await client?.close({ terminateWorker: true, timeoutMs: 1 }).catch(() => undefined);
  },
  maintain(table: string): Promise<TabFailure | undefined> {
    return lifecycle(async () => {
      if (client === undefined) throw new Error("open() first");
      await client.compactTable(table);
      await client.collectGarbage();
    });
  },
  close(): Promise<TabFailure | undefined> {
    if (crashed) {
      client = undefined;
      return Promise.resolve(undefined);
    }
    return lifecycle(async () => {
      await client?.close({ terminateWorker: true });
      client = undefined;
    });
  },
  pageErrors(): TabError[] {
    return [...pageErrors];
  },
};

Object.assign(window, { simulatorTab: tab });
const ready = document.querySelector("#ready");
if (ready !== null) ready.textContent = "Interaction simulator tab ready";
