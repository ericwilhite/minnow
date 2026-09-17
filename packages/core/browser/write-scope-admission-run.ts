import { MinnowDatabaseClient } from "@minnowdb/core/client";

type StoreKind = "indexeddb" | "opfs";

function openClient(store: { kind: StoreKind; name: string }): MinnowDatabaseClient {
  return new MinnowDatabaseClient(
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" }),
    // Zero commit retries: a retry would hide a writer that did not get its turn.
    { store, databaseOptions: { maxCommitRetries: 0 } },
  );
}

/** A cold sync through the published worker, including autocommit bookkeeping between chunks. */
export async function runWriteScopeAdmission(kind: StoreKind) {
  const store = { kind, name: `scope-admission-${crypto.randomUUID()}` };
  let client = openClient(store);
  let callbacks = 0;
  try {
    await client.execute("CREATE TABLE sync_state (id INTEGER PRIMARY KEY, done INTEGER)");
    for (let table = 0; table < 45; table += 1) {
      await client.execute(
        `CREATE TABLE items_${String(table)} (id INTEGER PRIMARY KEY, value INTEGER)`,
      );
    }
    let next = 0;
    await Promise.all(
      Array.from({ length: 12 }, async () => {
        while (next < 45) {
          const table = next++;
          await client.write(async (tx) => {
            callbacks += 1;
            await tx.upsertBatch(
              `items_${String(table)}`,
              Array.from({ length: 867 }, (_, id) => ({ id, value: table })),
            );
          });
          await client.upsertBatch("sync_state", [{ id: table, done: 0 }]);
          await client.execute("UPDATE sync_state SET done = 1 WHERE id = ?", [table]);
        }
      }),
    );
    await client.close({ terminateWorker: true });
    client = openClient(store);
    const counts = [];
    for (let table = 0; table < 45; table += 1) {
      const { rows } = await client.query(
        `SELECT COUNT(*) AS n, SUM(id) AS ids, SUM(value) AS values FROM items_${String(table)}`,
      );
      counts.push(rows[0]);
    }
    return {
      callbacks,
      counts,
      completed: (await client.query("SELECT COUNT(*) AS n FROM sync_state WHERE done = 1")).rows,
    };
  } finally {
    await client.close({ terminateWorker: true });
  }
}

let shared: MinnowDatabaseClient | undefined;
let sharedStalls = 0;

/** Opens this tab's own published worker over a database other tabs share. */
export async function openShared(kind: StoreKind, name: string): Promise<void> {
  shared = new MinnowDatabaseClient(
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" }),
    {
      store: { kind, name },
      databaseOptions: { maxCommitRetries: 0 },
      onWorkerError: (event) => {
        if (event.error.name === "WriteAdmissionStalledError") sharedStalls += 1;
      },
    },
  );
  await shared.ready();
}

export async function initializeShared(tables: number): Promise<void> {
  if (shared === undefined) throw new Error("Shared client is not open");
  await shared.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, tab INTEGER, n INTEGER)");
  await shared.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)");
  await shared.execute("INSERT INTO counter VALUES (1, 0)");
  for (let table = 0; table < tables; table += 1) {
    await shared.execute(
      `CREATE TABLE items_${String(table)} (id INTEGER PRIMARY KEY, tab INTEGER, value INTEGER)`,
    );
  }
}

/**
 * This tab's share of a burst every tab runs at once: `concurrency` workers each take tables
 * from a shared range, loading them inside write scopes that also read-modify-write one shared
 * counter, with an autocommit batch and a SQL statement of bookkeeping between scopes, and one
 * SQL transaction per worker. Every scope must run exactly once with no conflict of any kind.
 */
export async function burstShared(options: {
  tab: number;
  concurrency: number;
  tables: readonly number[];
  rowsPerTable: number;
}): Promise<{ callbacks: number; conflicts: string[]; counterReads: number[] }> {
  const client = shared;
  if (client === undefined) throw new Error("Shared client is not open");
  let callbacks = 0;
  const conflicts: string[] = [];
  const counterReads: number[] = [];
  const remaining = [...options.tables];
  await Promise.all(
    Array.from({ length: options.concurrency }, async (_, worker) => {
      try {
        for (;;) {
          const table = remaining.shift();
          if (table === undefined) break;
          const { result } = await client.write(async (tx) => {
            callbacks += 1;
            const before = (await tx.query("SELECT value FROM counter WHERE id = 1")).rows[0]
              ?.value as number;
            await tx.upsertBatch(
              `items_${String(table)}`,
              Array.from({ length: options.rowsPerTable }, (_, id) => ({
                id,
                tab: options.tab,
                value: table,
              })),
            );
            await tx.execute("UPDATE counter SET value = ? WHERE id = 1", [before + 1]);
            return before;
          });
          counterReads.push(result);
          await client.upsertBatch("ledger", [
            { id: options.tab * 1_000 + table, tab: options.tab, n: 0 },
          ]);
          await client.execute("UPDATE ledger SET n = n + 1 WHERE id = ?", [
            options.tab * 1_000 + table,
          ]);
        }
        void worker;
      } catch (error) {
        conflicts.push(error instanceof Error ? `${error.name}: ${error.message}` : String(error));
      }
    }),
  );
  // One SQL transaction per tab (a connection holds one at a time), taking its turn like a
  // scope does while the other tabs are still bursting.
  try {
    await client.execute("BEGIN");
    const before = (await client.query("SELECT value FROM counter WHERE id = 1")).rows[0]
      ?.value as number;
    await client.execute("UPDATE counter SET value = ? WHERE id = 1", [before + 1]);
    await client.execute("COMMIT");
    counterReads.push(before);
  } catch (error) {
    conflicts.push(error instanceof Error ? `${error.name}: ${error.message}` : String(error));
  }
  return { callbacks, conflicts, counterReads };
}

/** A schema change from this tab while the other tabs are mid-burst. */
export async function migrateShared(column: string): Promise<string | null> {
  if (shared === undefined) throw new Error("Shared client is not open");
  try {
    await shared.execute(`ALTER TABLE ledger ADD COLUMN ${column} TEXT`);
    await shared.execute(`CREATE INDEX ledger_${column} ON ledger (${column})`);
    return null;
  } catch (error) {
    return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
  }
}

export async function verifyShared(options: { tables: number; rowsPerTable: number }): Promise<{
  counter: unknown;
  ledger: unknown;
  tables: unknown[];
  columns: string[];
  stalls: number;
}> {
  if (shared === undefined) throw new Error("Shared client is not open");
  const tables = [];
  for (let table = 0; table < options.tables; table += 1) {
    tables.push(
      (
        await shared.query(
          `SELECT COUNT(*) AS n, SUM(id) AS ids, SUM(value) AS values, COUNT(DISTINCT tab) AS tabs FROM items_${String(table)}`,
          { memoize: false },
        )
      ).rows[0],
    );
  }
  return {
    counter: (await shared.query("SELECT value FROM counter WHERE id = 1", { memoize: false }))
      .rows[0],
    ledger: (
      await shared.query("SELECT COUNT(*) AS n, SUM(n) AS applied FROM ledger", {
        memoize: false,
      })
    ).rows[0],
    tables,
    columns:
      (await shared.listTables())
        .find((table) => table.name === "ledger")
        ?.columns.map((column) => column.name) ?? [],
    stalls: sharedStalls,
  };
}

let heldScope: Promise<unknown> | undefined;
let releaseHeldScope: (() => void) | undefined;

/** Opens a scope and parks inside its callback, holding the database's writer turn. */
export async function holdSharedScope(): Promise<void> {
  if (shared === undefined) throw new Error("Shared client is not open");
  let entered!: () => void;
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const parked = new Promise<void>((resolve) => {
    releaseHeldScope = resolve;
  });
  heldScope = shared.write(async (tx) => {
    await tx.insertBatch("ledger", [{ id: 999_999, tab: -1, n: 0 }]);
    entered();
    await parked;
  });
  void heldScope.catch(() => undefined);
  await started;
}

export async function releaseSharedScope(): Promise<void> {
  releaseHeldScope?.();
  await heldScope;
}

/**
 * A write issued while another tab holds the turn: reports whether it was still waiting after
 * `waitMs`, then whether closing this client let it go without waiting for the holder.
 */
export async function writeWhileHeld(waitMs: number): Promise<{
  waited: boolean;
  outcome: string;
  closeMs: number;
}> {
  if (shared === undefined) throw new Error("Shared client is not open");
  const pending = shared.insertBatch("ledger", [{ id: 999_998, tab: -2, n: 0 }]).then(
    () => "published",
    (error: unknown) => (error instanceof Error ? error.name : String(error)),
  );
  const waited = await Promise.race([
    pending.then(() => false),
    new Promise<boolean>((resolve) => setTimeout(() => resolve(true), waitMs)),
  ]);
  const startedAt = performance.now();
  await shared.close({ terminateWorker: true });
  const outcome = await pending;
  shared = undefined;
  return { waited, outcome, closeMs: performance.now() - startedAt };
}

export async function closeShared(): Promise<void> {
  await shared?.close({ terminateWorker: true });
  shared = undefined;
}

/**
 * Throughput measurement, not a correctness test: the fw-ui shape (wide rows, guarded upserts
 * replacing offline rows, an offline-key flip, and autocommit bookkeeping between scopes) loaded
 * at a given scope concurrency through one worker over a real store. Every run persists exactly
 * the same rows, so elapsed time is comparable across concurrency levels; the maintenance and
 * staging figures come from the engine's own accounting after the run.
 */
export async function benchmarkBurst(options: {
  kind: StoreKind;
  concurrency: number;
  tables: number;
  rowsPerTable: number;
}): Promise<{
  elapsedMs: number;
  callbacks: number;
  conflicts: number;
  rows: number;
  stagedRows: number;
  maintenance: { pendingCommitDebt: number; collectionRunning: boolean };
  storage: unknown;
}> {
  const store = { kind: options.kind, name: `bench-${crypto.randomUUID()}` };
  const client = new MinnowDatabaseClient(
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" }),
    { store, databaseOptions: { maxCommitRetries: 0 } },
  );
  let callbacks = 0;
  let conflicts = 0;
  let stagedRows = 0;
  const columns = Array.from({ length: 18 }, (_, index) => `c${String(index)}`);
  try {
    await client.execute("CREATE TABLE sync_state (id INTEGER PRIMARY KEY, done INTEGER)");
    for (let table = 0; table < options.tables; table += 1) {
      await client.execute(
        `CREATE TABLE items_${String(table)} (id TEXT PRIMARY KEY, offline INTEGER, _synced INTEGER, ${columns
          .map((column) => `${column} TEXT`)
          .join(", ")})`,
      );
    }
    const row = (table: number, id: number, offline: boolean) => ({
      id: offline ? `offline-${String(table)}-${String(id)}` : `srv-${String(table)}-${String(id)}`,
      offline: offline ? 1 : 0,
      _synced: offline ? 0 : 1,
      ...Object.fromEntries(columns.map((column) => [column, `${column}-${String(id)}-value`])),
    });
    const startedAt = performance.now();
    let next = 0;
    await Promise.all(
      Array.from({ length: options.concurrency }, async () => {
        while (next < options.tables) {
          const table = next++;
          const name = `items_${String(table)}`;
          try {
            const { result } = await client.write(async (tx) => {
              callbacks += 1;
              // Offline rows first, then the server-confirmed twins replace them under a guard.
              const offline = await tx.upsertBatch(
                name,
                Array.from({ length: options.rowsPerTable }, (_, id) => row(table, id, true)),
              );
              const confirmed = await tx.upsertBatch(
                name,
                Array.from({ length: options.rowsPerTable }, (_, id) => row(table, id, false)),
                { conflictWhere: { column: "_synced", operator: "=", value: 0 } },
              );
              await tx.execute(`DELETE FROM ${name} WHERE offline = 1`);
              return offline.rowCount + confirmed.rowCount;
            });
            stagedRows += result;
            await client.upsertBatch("sync_state", [{ id: table, done: 0 }]);
            await client.execute("UPDATE sync_state SET done = 1 WHERE id = ?", [table]);
          } catch {
            conflicts += 1;
          }
        }
      }),
    );
    const elapsedMs = performance.now() - startedAt;
    let rows = 0;
    for (let table = 0; table < options.tables; table += 1) {
      rows += (await client.query(`SELECT COUNT(*) AS n FROM items_${String(table)}`)).rows[0]
        ?.n as number;
    }
    const maintenance = await client.maintenanceStatus();
    const storage = await client.storageStats().catch(() => undefined);
    return {
      elapsedMs,
      callbacks,
      conflicts,
      rows,
      stagedRows,
      maintenance: {
        pendingCommitDebt: maintenance.pendingCommitDebt,
        collectionRunning: maintenance.collectionRunning,
      },
      storage,
    };
  } finally {
    await client.close({ terminateWorker: true });
  }
}
