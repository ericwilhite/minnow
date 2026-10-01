/**
 * Measures how long background work holds the event loop.
 *
 * Each workload loads a table, makes its writes, then lets background maintenance run until it
 * goes quiet. A 1 ms watchdog records the longest stretch it could not run during the writes and
 * during maintenance, and a point query issued every 20 ms records the worst latency a reader
 * saw. Numbers depend on the machine; `event-loop-stalls.test.ts` holds the same shapes to a
 * loose bound in CI.
 *
 *   npm run benchmark:stalls
 *   npm run benchmark:stalls -- --only 1,3
 *   npm run benchmark:stalls -- --store opfs
 *
 * `--store opfs` runs the native OPFS store, with its write-ahead log and checkpoints, over the
 * in-memory file tree the OPFS tests use.
 */
import { performance } from "node:perf_hooks";
import { MemoryBlockStore } from "../packages/core/src/storage/index.ts";
import { OpfsBlockStore } from "../packages/core/src/storage/opfs/index.ts";
import { MemoryOpfs } from "../packages/core/src/testing/opfs-shim.ts";
import { MinnowDatabase } from "../packages/core/src/engine/database.ts";

type Row = Record<string, unknown>;
interface Column {
  name: string;
  type: "number" | "string" | "datetime";
  nullable?: boolean;
}
interface Workload {
  name: string;
  keyless?: boolean;
  columns: Column[];
  rows: number;
  row: (id: number, version: number) => Row;
  churn: (database: MinnowDatabase, ids: number[], row: Workload["row"]) => Promise<void>;
}

function shuffle<T>(values: readonly T[], seed: number): T[] {
  const result = [...values];
  let state = seed;
  for (let index = result.length - 1; index > 0; index -= 1) {
    state = (state * 1_103_515_245 + 12_345) % 2 ** 31;
    const other = state % (index + 1);
    const value = result[index];
    const swap = result[other];
    if (value === undefined || swap === undefined) continue;
    result[index] = swap;
    result[other] = value;
  }
  return result;
}

function wideColumns(): Column[] {
  const columns: Column[] = [{ name: "id", type: "number" }];
  for (let index = 0; index < 13; index += 1) {
    columns.push({ name: `n${String(index)}`, type: "number", nullable: true });
    columns.push({ name: `d${String(index)}`, type: "datetime", nullable: true });
    columns.push({ name: `s${String(index)}`, type: "string", nullable: true });
  }
  return columns;
}

function wideRow(id: number, version: number): Row {
  const row: Row = { id };
  for (let index = 0; index < 13; index += 1) {
    row[`n${String(index)}`] = id * 13 + index + version;
    row[`d${String(index)}`] = new Date(1_700_000_000_000 + id * 1_000 + index);
    row[`s${String(index)}`] = `v${String((id + version) % 97)}-${String(index)}`;
  }
  return row;
}

const narrowColumns: Column[] = [
  { name: "id", type: "number" },
  { name: "region", type: "string" },
  { name: "amount", type: "number" },
  { name: "label", type: "string" },
];

function narrowRow(id: number, version: number): Row {
  return {
    id,
    region: `r${String(id % 7)}`,
    amount: id + version,
    label: `label-${String(id)}-${String(version % 3)}`,
  };
}

function refreshes(count: number, shuffled: boolean) {
  return async (database: MinnowDatabase, ids: number[], row: Workload["row"]) => {
    for (let version = 1; version <= count; version += 1) {
      const order = shuffled ? shuffle(ids, version) : ids;
      for (let start = 0; start < order.length; start += 50_000) {
        await database.upsertBatch(
          "t",
          order.slice(start, start + 50_000).map((id) => row(id, version)) as never,
        );
      }
    }
  };
}

const workloads: Workload[] = [
  {
    name: "wide 7k rows, same-order refresh x8",
    columns: wideColumns(),
    rows: 7_000,
    row: wideRow,
    churn: refreshes(8, false),
  },
  {
    name: "wide 7k rows, shuffled refresh x4",
    columns: wideColumns(),
    rows: 7_000,
    row: wideRow,
    churn: refreshes(4, true),
  },
  {
    name: "narrow 200k rows, same-order refresh x2",
    columns: narrowColumns,
    rows: 200_000,
    row: narrowRow,
    churn: refreshes(2, false),
  },
  {
    name: "narrow 200k rows, shuffled refresh x2",
    columns: narrowColumns,
    rows: 200_000,
    row: narrowRow,
    churn: refreshes(2, true),
  },
  {
    name: "narrow 200k rows, 40 point-update statements",
    columns: narrowColumns,
    rows: 200_000,
    row: narrowRow,
    churn: async (database) => {
      for (let index = 0; index < 40; index += 1) {
        const ids = Array.from(
          { length: 200 },
          (_, offset) => (index * 7_919 + offset * 104_729) % 200_000,
        );
        await database.execute(
          `UPDATE t SET amount = amount + 1 WHERE id IN (${[...new Set(ids)].join(",")})`,
        );
      }
    },
  },
  {
    name: "narrow 200k rows, 34 scattered-delete statements",
    columns: narrowColumns,
    rows: 200_000,
    row: narrowRow,
    churn: async (database) => {
      for (let index = 0; index < 34; index += 1) {
        const ids = Array.from({ length: 1_000 }, (_, offset) => (index + offset * 199) % 200_000);
        await database.execute(`DELETE FROM t WHERE id IN (${ids.join(",")})`);
      }
    },
  },
  {
    name: "keyless, 60 appends of 5k rows",
    keyless: true,
    columns: narrowColumns,
    rows: 5_000,
    row: narrowRow,
    churn: async (database, _ids, row) => {
      for (let batch = 1; batch < 60; batch += 1) {
        await database.insertBatch(
          "t",
          Array.from({ length: 5_000 }, (_, index) => row(batch * 5_000 + index, 0)) as never,
        );
      }
    },
  },
  {
    name: "secondary index build over 200k rows",
    columns: narrowColumns,
    rows: 200_000,
    row: narrowRow,
    churn: async (database) => {
      await database.execute("CREATE INDEX t_amount ON t (amount)");
      await database.query("SELECT id FROM t WHERE amount = 1234", { memoize: false });
    },
  },
  {
    name: "unique index build over 200k rows",
    columns: narrowColumns,
    rows: 200_000,
    row: narrowRow,
    churn: async (database) => {
      await database.execute("CREATE UNIQUE INDEX t_label ON t (label)");
      await database.query("SELECT id FROM t WHERE label = 'label-1234-0'", { memoize: false });
    },
  },
  {
    name: "full-text index build over 200k rows",
    columns: [
      { name: "id", type: "number" },
      { name: "body", type: "string" },
    ],
    rows: 200_000,
    row: (id) => ({
      id,
      body: `item ${String(id)} quick brown fox ${String(id % 977)} jumps over ${String(id % 31)}`,
    }),
    churn: async (database) => {
      await database.query("SELECT id FROM t WHERE MATCH(body) AGAINST 'quick fox'", {
        memoize: false,
      });
    },
  },
  {
    name: "live aggregate under 20 upserts of 10k rows",
    columns: narrowColumns,
    rows: 200_000,
    row: narrowRow,
    churn: async (database, _ids, row) => {
      await database
        .liveQueries()
        .subscribe("SELECT region, COUNT(*) AS n, SUM(amount) AS total FROM t GROUP BY region", {
          onChange: () => undefined,
        });
      for (let batch = 0; batch < 20; batch += 1) {
        await database.upsertBatch(
          "t",
          Array.from({ length: 10_000 }, (_, index) =>
            row(batch * 10_000 + index, batch + 1),
          ) as never,
        );
      }
    },
  },
];

async function visibleSegmentIds(database: MinnowDatabase): Promise<string[]> {
  const ids: string[] = [];
  let page = await database.listVisibleSegmentPage("t", { limit: 64 });
  for (;;) {
    ids.push(...page.records.map((segment) => segment.id));
    if (page.nextCursor === null) return ids;
    page = await database.listVisibleSegmentPage("t", { cursor: page.nextCursor });
  }
}

const storeIndex = process.argv.indexOf("--store");
const storeKind = storeIndex < 0 ? "memory" : process.argv[storeIndex + 1];
if (storeKind !== "memory" && storeKind !== "opfs") {
  throw new TypeError(`--store must be memory or opfs, not ${String(storeKind)}`);
}

async function openStore() {
  if (storeKind === "memory") return new MemoryBlockStore();
  return OpfsBlockStore.open({ name: "stall-survey", root: new MemoryOpfs().root });
}

async function measure(workload: Workload) {
  const errors: string[] = [];
  const store = await openStore();
  const database = new MinnowDatabase(store, {
    onBackgroundError: (error) => errors.push(String(error)),
  });
  await database.createTable({
    name: "t",
    ...(workload.keyless === true ? {} : { uniqueKey: "id" }),
    columns: workload.columns,
  });
  const ids = Array.from({ length: workload.rows }, (_, id) => id);
  for (let start = 0; start < ids.length; start += 50_000) {
    await database.insertBatch(
      "t",
      ids.slice(start, start + 50_000).map((id) => workload.row(id, 0)) as never,
    );
  }

  let longest = 0;
  let last = performance.now();
  const watchdog = setInterval(() => {
    const now = performance.now();
    longest = Math.max(longest, now - last);
    last = now;
  }, 1);
  let worstQuery = 0;
  let probing = true;
  const probe = (async () => {
    for (let index = 0; probing; index += 1) {
      const started = performance.now();
      await database.query(`SELECT id FROM t WHERE id = ${String(index % workload.rows)}`, {
        memoize: false,
      });
      worstQuery = Math.max(worstQuery, performance.now() - started);
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  })();

  const writeStarted = performance.now();
  await workload.churn(database, ids, workload.row);
  const writeMs = performance.now() - writeStarted;
  // Let the watchdog fire once, so a block that ends with the writes is counted with them.
  await new Promise((resolve) => setTimeout(resolve, 5));
  const writeBlock = longest;
  const writeQuery = worstQuery;
  longest = 0;
  worstQuery = 0;

  const maintenanceStarted = performance.now();
  let previous = "";
  for (let quiet = 0; quiet < 20;) {
    await new Promise((resolve) => setTimeout(resolve, 50));
    const active = (await database.listCompactionJobs("t")).some(
      (job) => job.state !== "published" && job.state !== "cancelled" && job.state !== "aborted",
    );
    const layout = JSON.stringify(await visibleSegmentIds(database));
    quiet = !active && layout === previous ? quiet + 1 : 0;
    previous = layout;
  }
  const maintenanceMs = performance.now() - maintenanceStarted - 1_000;
  probing = false;
  await probe;
  clearInterval(watchdog);
  const segments = (await visibleSegmentIds(database)).length;
  await database.close();
  store.close();
  return {
    workload: workload.name,
    "writes ms": Math.round(writeMs),
    "writes: longest block ms": Math.round(writeBlock),
    "writes: worst query ms": Math.round(writeQuery),
    "maintenance ms": Math.max(0, Math.round(maintenanceMs)),
    "maintenance: longest block ms": Math.round(longest),
    "maintenance: worst query ms": Math.round(worstQuery),
    "segments after": segments,
    errors: errors.length,
  };
}

const onlyIndex = process.argv.indexOf("--only");
const only =
  onlyIndex < 0 ? undefined : new Set((process.argv[onlyIndex + 1] ?? "").split(",").map(Number));
const results = [];
for (const [index, workload] of workloads.entries()) {
  if (only !== undefined && !only.has(index)) continue;
  results.push(await measure(workload));
}
console.table(results);
