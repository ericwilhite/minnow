import { MinnowDatabaseClient } from "@minnowdb/core/client";

let client: MinnowDatabaseClient;
let worker: Worker;
let release: (() => void) | undefined;
let staged: Promise<unknown> | undefined;
let acknowledged = 0;
let workerFailure: string | undefined;
let loading: Promise<void> | undefined;

export async function open(
  name: string,
  kind: "indexeddb" | "opfs",
  requestTimeoutMs = 5_000,
  holdLeadership = false,
): Promise<void> {
  worker = new Worker(
    holdLeadership
      ? new URL("./frozen-worker.ts", import.meta.url)
      : new URL("./published-worker.ts", import.meta.url),
    { type: "module" },
  );
  workerFailure = undefined;
  worker.addEventListener("error", (event) => {
    workerFailure = event.message;
  });
  worker.addEventListener("messageerror", () => {
    workerFailure = "messageerror";
  });
  client = new MinnowDatabaseClient(worker, {
    store: { kind, name, durability: "strict" },
    requestTimeoutMs,
  });
  await client.ready();
}

export async function initialize(): Promise<void> {
  await client.execute("CREATE TABLE stock(id INTEGER PRIMARY KEY, qty INTEGER)");
  await client.execute("CREATE TABLE sales(id INTEGER PRIMARY KEY, total INTEGER)");
  await client.execute("CREATE TABLE lines(id INTEGER PRIMARY KEY, sale_id INTEGER, qty INTEGER)");
  await client.execute("CREATE TABLE tickets(id INTEGER PRIMARY KEY)");
  await client.insertBatch("stock", [{ id: 1, qty: 1000 }]);
}

export async function tickets(offset: number, count = 32): Promise<void> {
  try {
    await Promise.all(
      Array.from({ length: count }, (_, id) =>
        client.insertBatch("tickets", [{ id: id + offset }]),
      ),
    );
  } catch (error) {
    const details = (value: unknown): unknown =>
      value instanceof Error
        ? {
            name: value.name,
            message: value.message,
            cause: value.cause === undefined ? undefined : details(value.cause),
          }
        : String(value);
    throw new Error(JSON.stringify({ offset, count, workerFailure, error: details(error) }), {
      cause: error,
    });
  }
}

async function checkout(id: number, pause?: () => Promise<void>): Promise<void> {
  await client.write(async (tx) => {
    await tx.execute("UPDATE stock SET qty = qty - 1 WHERE id = 1");
    await tx.insertBatch("sales", [{ id, total: 1000 }]);
    await tx.insertBatch("lines", [{ id, sale_id: id, qty: 1 }]);
    await pause?.();
  });
}

export async function stage(id: number): Promise<void> {
  let ready!: () => void;
  const started = new Promise<void>((resolve) => {
    ready = resolve;
  });
  const paused = new Promise<void>((resolve) => {
    release = resolve;
  });
  staged = checkout(id, () => {
    ready();
    return paused;
  });
  void staged.catch(() => undefined);
  await started;
}

export async function publish(): Promise<void> {
  release?.();
  await staged;
}
export async function killStaged(): Promise<void> {
  worker.terminate();
  release?.();
  await staged?.catch(() => undefined);
  await client.close().catch(() => undefined);
}

export async function startLoad(count = 100): Promise<void> {
  acknowledged = 0;
  loading = (async () => {
    for (let id = 10; id < 10 + count; id += 1) {
      await checkout(id);
      acknowledged += 1;
    }
  })();
  void loading.catch(() => undefined);
}
export function progress(): number {
  return acknowledged;
}
export async function killDuringLoad(): Promise<number> {
  worker.terminate();
  const before = acknowledged;
  await loading?.catch(() => undefined);
  await client.close().catch(() => undefined);
  return before;
}
export async function counts() {
  return client.snapshot(async (snapshot) => {
    const qty = (await snapshot.query("SELECT qty FROM stock")).rows[0]?.qty;
    const sales = (await snapshot.query("SELECT COUNT(*) AS n FROM sales")).rows[0]?.n;
    const lines = (await snapshot.query("SELECT COUNT(*) AS n FROM lines")).rows[0]?.n;
    const tickets = (await snapshot.query("SELECT COUNT(*) AS n FROM tickets")).rows[0]?.n;
    return { qty, sales, lines, tickets };
  });
}
export async function close(): Promise<void> {
  await client.close({ terminateWorker: true });
}

let releaseLock: (() => void) | undefined;
let heldLock: Promise<void> | undefined;
export async function holdAdmission(name: string): Promise<void> {
  let ready!: () => void;
  const started = new Promise<void>((resolve) => {
    ready = resolve;
  });
  const paused = new Promise<void>((resolve) => {
    releaseLock = resolve;
  });
  heldLock = navigator.locks
    .request(`minnowdb-write:minnowdb-live:indexeddb:${name}`, () => {
      ready();
      return paused;
    })
    .then(() => undefined);
  await started;
}
export async function releaseAdmission(): Promise<void> {
  releaseLock?.();
  await heldLock;
}
export async function closeWhileAdmissionBlocked(name: string): Promise<string> {
  const { IndexedDbBlockStore } = await import("@minnowdb/core/storage/indexeddb");
  const { MinnowDatabase } = await import("@minnowdb/core");
  const store = await IndexedDbBlockStore.open({ name });
  const db = new MinnowDatabase(store);
  const pending = db.insertBatch("tickets", [{ id: 99 }]).then(
    () => "published",
    (error: unknown) => (error instanceof Error ? error.message : String(error)),
  );
  const lockName = `minnowdb-write:minnowdb-live:indexeddb:${name}`;
  for (;;) {
    const state = await navigator.locks.query();
    if (state.pending?.some(({ name }) => name === lockName)) break;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  await db.close();
  store.close();
  return pending;
}

let releaseHold: (() => Promise<void>) | undefined;
/**
 * Holds a readwrite transaction on the database's catalog from this page's own connection, and
 * keeps it alive with one read after another until `releaseDatabase()`: every transaction that
 * overlaps it, in any tab, waits meanwhile — what a transaction a browser never finishes does.
 */
export async function holdDatabase(name: string): Promise<void> {
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("open failed"));
  });
  const transaction = db.transaction("catalog", "readwrite");
  const catalog = transaction.objectStore("catalog");
  let holding = true;
  const done = new Promise<void>((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onabort = () => reject(transaction.error ?? new Error("hold aborted"));
  });
  await new Promise<void>((started) => {
    const next = (): void => {
      catalog.get("hold").onsuccess = () => {
        started();
        if (holding) next();
      };
    };
    next();
  });
  releaseHold = async () => {
    holding = false;
    await done;
    db.close();
  };
}
export async function releaseDatabase(): Promise<void> {
  await releaseHold?.();
  releaseHold = undefined;
}
/**
 * The stalled worker stays alive: its schema read is still queued behind the hold, and
 * terminating a worker with a queued transaction is what wedges WebKit in the first place.
 */
const stalledWorkers: Worker[] = [];
/** Opens a client whose store gives up after two seconds of silence; returns how init ended. */
export async function openWhileHeld(name: string): Promise<{ name: string; message: string }> {
  const stalled = new Worker(new URL("./unresponsive-worker.ts", import.meta.url), {
    type: "module",
  });
  stalledWorkers.push(stalled);
  // A client deadline shorter than the store's (12 s, unresponsive-worker.ts): only the worker's
  // progress reports keep the client from calling a live worker dead before the store names the
  // stall. Four seconds leaves a slow runner room to load the worker module before its first
  // report, which a one-second deadline did not.
  const stalledClient = new MinnowDatabaseClient(stalled, {
    store: { kind: "indexeddb", name },
    requestTimeoutMs: 4_000,
  });
  try {
    await stalledClient.ready();
    return { name: "ready", message: "" };
  } catch (error) {
    return error instanceof Error
      ? { name: error.name, message: error.message }
      : { name: "unknown", message: String(error) };
  }
}

export async function receipts() {
  return client.snapshot(async (snapshot) => ({
    sales: (await snapshot.query("SELECT id, total FROM sales ORDER BY id")).rows,
    lines: (await snapshot.query("SELECT id, sale_id, qty FROM lines ORDER BY id")).rows,
  }));
}

export async function readStock() {
  return (await client.query("SELECT qty FROM stock WHERE id = 1")).rows;
}

export async function ticketOutcomes(offset: number, count: number) {
  return Promise.all(
    Array.from({ length: count }, async (_, index) => {
      const id = offset + index;
      try {
        await client.insertBatch("tickets", [{ id }]);
        return { id, outcome: "acknowledged" };
      } catch (error) {
        if (!(error instanceof Error) || error.name !== "DatabaseWorkerOutcomeUnknownError")
          throw error;
        return { id, outcome: "unknown" };
      }
    }),
  );
}
export async function ticketIds() {
  return (await client.query("SELECT id FROM tickets ORDER BY id")).rows.map((row) =>
    Number(row.id),
  );
}

let recoveryUpdates = 0;
let recoveryRows: unknown[] = [];
const recoveryErrors: string[] = [];
export async function startRecoverySubscription(): Promise<void> {
  recoveryUpdates = 0;
  recoveryErrors.length = 0;
  const live = client.liveQueries({ pollIntervalMs: 50 });
  await live.subscribe("SELECT qty FROM stock WHERE id = 1", {
    onChange: (result) => {
      recoveryUpdates += 1;
      recoveryRows = result.rows;
    },
    onError: (error) => {
      recoveryErrors.push(String(error));
    },
  });
}
export function recoverySubscriptionState() {
  return { updates: recoveryUpdates, rows: recoveryRows, errors: [...recoveryErrors] };
}
export async function changeRecoveryStock(): Promise<void> {
  await client.execute("UPDATE stock SET qty = 999 WHERE id = 1");
}
export async function recoveryMigrationFailure() {
  const { OpfsCoordinationError, column, schema, table } = await import("@minnowdb/core");
  try {
    await client.migrate(
      schema([table("stock", { id: column.number().unique(), qty: column.number().nullable() })]),
    );
    return { transient: false, name: "resolved", reason: null };
  } catch (error) {
    return {
      transient: error instanceof OpfsCoordinationError,
      name: error instanceof Error ? error.name : "unknown",
      reason: error instanceof OpfsCoordinationError ? error.reason : null,
    };
  }
}
export async function refuseRecoveryDeletion(name: string): Promise<boolean> {
  const { deleteOpfsDatabase, OpfsDatabaseInUseError } =
    await import("@minnowdb/core/storage/opfs");
  try {
    await deleteOpfsDatabase({ name });
    return false;
  } catch (error) {
    return error instanceof OpfsDatabaseInUseError;
  }
}
