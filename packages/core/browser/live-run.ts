/**
 * The live-query layer on real browser storage, the way an application runs it: a module worker
 * holding an IndexedDB or OPFS database, a page holding hundreds of subscriptions over tables of
 * realistic size, and the clock stopped by the moment a change reaches the page. Every number
 * therefore includes the store's own commit, the worker's sweep, the channel, and the rows
 * being rebuilt on the main thread — the latency between a write and the re-render it causes.
 *
 * Not a test: `live-bench.spec.ts` drives it only when asked, and reports rather than asserts.
 */
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { LiveQueryManager, type LiveQuerySource } from "@minnowdb/core/live";
import type { QueryResult, QueryValue } from "@minnowdb/core";

type StoreKind = "indexeddb" | "opfs";

interface LiveBenchOptions {
  store: StoreKind;
  /** Rows in the orders table before any subscription is registered. */
  rows: number;
  /** Distinct window subscriptions over the orders table. */
  subscriptions: number;
  /** Commits per timed case. */
  samples: number;
}

interface Timing {
  medianMs: number;
  p95Ms: number;
  samples: number;
}

interface LiveBenchReport {
  store: StoreKind;
  rows: number;
  subscriptions: number;
  seedMs: number;
  /** One single-row insert with nothing subscribed. */
  writeFloor: Timing;
  /** Registering every low-level subscription, initial delivery included. */
  subscribeMs: number;
  /** Registering the typed queries, first snapshot included. */
  typedSubscribeMs: number;
  /** One inserted row that enters every window: until every subscriber has its new rows. */
  insertVisible: Timing;
  /** One row edited inside every window. */
  updateVisible: Timing;
  /** One row edited that no window holds: until the sweep is done, nothing delivered. */
  updateHidden: Timing;
  /** One row deleted from every window (the window must be refilled). */
  deleteVisible: Timing;
  /** A commit to an unrelated table: the cost of ruling every subscription out. */
  unrelated: Timing;
  /** Fifty single-row inserts issued back to back, until the last one is reflected. */
  burst50Ms: number;
  stats: Record<string, number>;
  /** The page's JS heap (Chromium only) before any subscription and after the burst, in MiB. */
  pageHeapMiB: { before: number; after: number } | null;
  /** Snapshot publications on the typed queries per visible change: one each, no more. */
  typedEmitsPerVisibleChange: number;
  typedRowIdentityPreserved: boolean;
}

/** Progress the driving test can read back when a phase takes too long. */
function phase(name: string): void {
  console.info(`live-bench phase: ${name}`);
}

function spawn(): Worker {
  return new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
}

function timing(samples: number[]): Timing {
  const sorted = [...samples].sort((left, right) => left - right);
  const at = (fraction: number): number =>
    sorted[Math.min(sorted.length - 1, Math.floor(fraction * sorted.length))] ?? 0;
  return { medianMs: at(0.5), p95Ms: at(0.95), samples: sorted.length };
}

const STATUSES = ["open", "paid", "shipped", "closed"] as const;

function batch(firstId: number, count: number, placedFrom: number): Record<string, QueryValue[]> {
  const ids = Array.from({ length: count }, (_, index) => firstId + index);
  return {
    id: ids,
    customer_id: ids.map((id) => id % 5_000),
    status: ids.map((id) => STATUSES[id % 4] ?? "open"),
    total: ids.map((id) => ((id * 7_919) % 100_000) / 100),
    placed_at: ids.map((id) => new Date(placedFrom + id * 60_000)),
    note: ids.map((id) => `order ${String(id)} ${"x".repeat(id % 40)}`),
  };
}

async function runLiveBench(options: LiveBenchOptions): Promise<LiveBenchReport> {
  const name = `live-bench-${String(Date.now())}-${String(Math.random()).slice(2)}`;
  const worker = spawn();
  const client = new MinnowDatabaseClient(worker, { store: { kind: options.store, name } });
  await client.ready();
  await client.createTable({
    name: "orders",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "customer_id", type: "number" },
      { name: "status", type: "string" },
      { name: "total", type: "number" },
      { name: "placed_at", type: "datetime" },
      { name: "note", type: "string" },
    ],
  });
  await client.createTable({
    name: "audit",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "message", type: "string" },
    ],
  });
  phase("seed");
  const seedStart = performance.now();
  const placedFrom = Date.UTC(2026, 0, 1);
  for (let start = 0; start < options.rows; start += 20_000) {
    const count = Math.min(20_000, options.rows - start);
    await client.insertBatch("orders", { columns: batch(start + 1, count, placedFrom) });
  }
  await client.insertBatch("audit", { columns: { id: [1], message: ["seed"] } });
  const seedMs = performance.now() - seedStart;
  // The store's own cost of one single-row commit, with nothing subscribed: the floor under
  // every latency below. These rows are `paid` (id % 4 === 1), so no window ever holds them.
  const floorSamples: number[] = [];
  for (let sample = 0; sample < options.samples; sample += 1) {
    const started = performance.now();
    await client.insertBatch("orders", {
      columns: batch(5_000_001 + sample * 4, 1, placedFrom),
    });
    floorSamples.push(performance.now() - started);
  }
  const writeFloor = timing(floorSamples);

  const heapMiB = (): number | null => {
    const memory = (performance as Performance & { memory?: { usedJSHeapSize: number } }).memory;
    return memory === undefined ? null : memory.usedJSHeapSize / 1_048_576;
  };
  const heapBefore = heapMiB();
  phase("subscribe");
  const live = client.liveQueries();
  // Every subscription is a window a list component would render: open orders, newest first,
  // a different threshold per subscription so no two share a group.
  const sqlFor = (index: number): string =>
    `SELECT id, customer_id, status, total, placed_at FROM orders WHERE status = 'open' AND total > ${String(index % 40)} ORDER BY placed_at DESC, id LIMIT 50`;
  let fired = 0;
  let expected = 0;
  let settle: (() => void) | undefined;
  const onChange = (): void => {
    fired += 1;
    if (fired === expected) settle?.();
  };
  const onError = (error: unknown): void => {
    console.error(
      `live subscription error: ${error instanceof Error ? error.message : String(error)}`,
    );
  };
  const subscribeStart = performance.now();
  await Promise.all(
    Array.from({ length: options.subscriptions }, (_, index) =>
      live.subscribe(sqlFor(index), { onChange, onError }),
    ),
  );
  const subscribeMs = performance.now() - subscribeStart;

  // The same windows as typed queries with a decoder, as the Kysely adapter registers them.
  const manager = new LiveQueryManager(client);
  let typedEmits = 0;
  const typedRows: Array<ReadonlyArray<Record<string, QueryValue>>> = [];
  const typedStart = performance.now();
  const typed = Array.from({ length: options.subscriptions }, (_, index) => {
    const sql = sqlFor(index);
    const source: LiveQuerySource<Record<string, QueryValue>> = {
      query: sql,
      execute: async (signal) =>
        (await client.query(sql, signal === undefined ? {} : { signal })).rows,
      decode: (result: QueryResult) => result.rows,
    };
    const query = manager.watch(source);
    query.subscribe(() => {
      const snapshot = query.getSnapshot();
      if (snapshot.status !== "ready") return;
      typedEmits += 1;
      typedRows[index] = snapshot.rows;
    });
    return query;
  });
  await Promise.all(typed.map((query) => query.refresh()));
  const typedSubscribeMs = performance.now() - typedStart;

  let nextId = 10_000_000;
  const commit = async (write: () => Promise<unknown>, count: number): Promise<number> => {
    fired = 0;
    expected = count;
    const settled = new Promise<void>((resolve) => {
      settle = resolve;
    });
    const started = performance.now();
    await write();
    if (count > 0) {
      await Promise.race([
        settled,
        new Promise<never>((_, reject) => {
          setTimeout(() => {
            reject(
              new Error(`${String(fired)} of ${String(count)} subscriptions notified in 20 s`),
            );
          }, 20_000);
        }),
      ]);
    } else await live.refresh();
    return performance.now() - started;
  };
  const timed = async (write: () => Promise<unknown>, count: number): Promise<Timing> => {
    const samples: number[] = [];
    await commit(write, count);
    for (let sample = 0; sample < options.samples; sample += 1) {
      samples.push(await commit(write, count));
    }
    return timing(samples);
  };
  const all = options.subscriptions;
  const visibleRow = (): Record<string, QueryValue[]> => {
    const id = nextId++;
    return {
      id: [id],
      customer_id: [1],
      status: ["open"],
      total: [999],
      placed_at: [new Date(Date.UTC(2027, 0, 1) + id)],
      note: ["visible"],
    };
  };
  let lastVisible = 0;
  phase("insert visible");
  const insertVisible = await timed(async () => {
    const row = visibleRow();
    lastVisible = row.id?.[0] as number;
    await client.insertBatch("orders", { columns: row });
  }, all);
  // Let every typed query apply the last delivery before its snapshots are compared.
  await Promise.all(typed.map((query) => query.refresh()));
  const typedBeforeUpdate = typedRows.map((rows) => rows);
  const emitsBefore = typedEmits;
  let updates = 0;
  phase("update visible");
  const updateVisible = await timed(async () => {
    updates += 1;
    await client.updateBatch("orders", {
      keys: [lastVisible],
      changes: { total: [900 + (updates % 50)] },
    });
  }, all);
  // Let the typed queries settle before judging them.
  await Promise.all(typed.map((query) => query.refresh()));
  const typedEmitsPerVisibleChange =
    (typedEmits - emitsBefore) / ((options.samples + 1) * options.subscriptions);
  const typedRowIdentityPreserved = typedRows.every((rows, index) => {
    const before = typedBeforeUpdate[index];
    if (before === undefined || rows.length < 2 || before.length < 2) return false;
    // The edited row is the newest (first); the second row was untouched and keeps its object.
    return rows[1] === before[1] && rows[0] !== before[0];
  });
  phase("update hidden");
  const updateHidden = await timed(
    () => client.updateBatch("orders", { keys: [50], changes: { total: [1] } }),
    0,
  );
  phase("delete visible");
  const deleteVisible = await timed(async () => {
    const row = visibleRow();
    const id = row.id?.[0] as number;
    await client.insertBatch("orders", { columns: row });
    // The insert notified everyone too; wait it out so the delete's own delivery is what is timed.
    await live.refresh();
    fired = 0;
    await client.deleteBatch("orders", { keys: [id] });
  }, all);
  phase("unrelated");
  const unrelated = await timed(
    () =>
      client.insertBatch("audit", {
        columns: { id: [nextId++], message: ["unrelated"] },
      }),
    0,
  );
  phase("burst");
  const burstStart = performance.now();
  fired = 0;
  expected = all * 50;
  const burstSettled = new Promise<void>((resolve) => {
    settle = resolve;
  });
  for (let index = 0; index < 50; index += 1) {
    await client.insertBatch("orders", { columns: visibleRow() });
  }
  // Sweeps coalesce under a burst, so fewer than 50 deliveries per subscription may arrive;
  // wait for the last row to be reflected instead.
  const lastId = nextId - 1;
  const burstDeadline = performance.now() + 60_000;
  await Promise.race([
    burstSettled,
    (async () => {
      for (;;) {
        if (performance.now() > burstDeadline) {
          const snapshot = typed[0]?.getSnapshot();
          throw new Error(
            `burst never settled: typed[0] is ${String(snapshot?.status)} with first id ${String(snapshot?.rows[0]?.id)}, wanted ${String(lastId)}`,
          );
        }
        await live.refresh();
        const seen = await client.query(
          `SELECT COUNT(*) AS n FROM orders WHERE id = ${String(lastId)}`,
        );
        if (seen.rows[0]?.n === 1 && typedRows[0]?.some((row) => row.id === lastId) === true) {
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    })(),
  ]);
  const burst50Ms = performance.now() - burstStart;
  phase("done");
  const heapAfter = heapMiB();
  const stats = (await live.stats()) as unknown as Record<string, number>;
  await manager.close();
  await live.close();
  await client.close({ terminateWorker: true });
  return {
    store: options.store,
    rows: options.rows,
    subscriptions: options.subscriptions,
    seedMs,
    writeFloor,
    subscribeMs,
    typedSubscribeMs,
    insertVisible,
    updateVisible,
    updateHidden,
    deleteVisible,
    unrelated,
    burst50Ms,
    stats,
    pageHeapMiB:
      heapBefore === null || heapAfter === null ? null : { before: heapBefore, after: heapAfter },
    typedEmitsPerVisibleChange,
    typedRowIdentityPreserved,
  };
}

(window as typeof window & { runLiveBench: typeof runLiveBench }).runLiveBench = runLiveBench;
const ready = document.querySelector("#ready");
if (ready !== null) ready.textContent = "Live-query benchmark ready";
