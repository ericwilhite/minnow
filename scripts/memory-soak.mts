/**
 * The memory soak: does a database that runs all day keep a bounded amount of memory?
 *
 * The unit suites bound what the engine accounts for — the buffer pool, query reservations,
 * the store's footprint — but a leak is by definition what nobody accounts for: a map that
 * only grows, a closure that keeps a result alive, a listener never removed. The only honest
 * check is the heap itself, after a forced collection, across a long run of the same work.
 *
 * Each workload runs in rounds. The heap is measured after a GC at the end of every round, and
 * the assertion is a plateau: the last rounds may not sit more than a bounded margin above the
 * early ones. A steady climb is a leak; a one-time rise (caches filling, JIT warming) is not,
 * which is why the first rounds are warm-up and excluded.
 *
 *   npm run soak:memory                         # vite-node under node --expose-gc
 *   npm run soak:memory -- --rounds 12 --workload live
 *
 * Needs --expose-gc; the npm script passes it. Nothing here writes to the repository.
 */
import { MemoryBlockStore } from "../packages/core/src/storage/index.js";
import { MinnowDatabase } from "../packages/core/src/engine/database.js";

declare const gc: (() => void) | undefined;

function argument(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

const rounds = Number(argument("--rounds") ?? 8);
const only = argument("--workload");
/** Rounds excluded from the plateau check while caches fill and the JIT warms. */
const WARMUP_ROUNDS = 3;
/** How far above the post-warmup floor the final rounds may sit. */
const PLATEAU_RATIO = 1.25;
const PLATEAU_SLACK_BYTES = 8 * 1024 * 1024;

if (typeof gc !== "function") {
  console.error("The memory soak needs node --expose-gc (use npm run soak:memory).");
  process.exit(2);
}

const REGIONS = ["west", "east", "north", "south"] as const;
const ROWS = 20_000;

async function seededDatabase(): Promise<{ store: MemoryBlockStore; database: MinnowDatabase }> {
  const store = new MemoryBlockStore();
  // A small buffer pool, so it fills within the warm-up rounds: the pool is the one place the
  // engine is allowed to grow, up to its limit, and a climb that continues past a full pool is
  // what this soak exists to catch.
  const database = new MinnowDatabase(store, {
    rowsPerBlock: 2048,
    bufferPoolBytes: 8 * 1024 * 1024,
  });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "region", type: "string", nullable: true },
      { name: "amount", type: "number" },
      { name: "joined", type: "datetime", nullable: true },
      { name: "label", type: "string" },
    ],
  });
  const rows = Array.from({ length: ROWS }, (_, index) => ({
    id: index + 1,
    region: index % 7 === 0 ? null : (REGIONS[index % REGIONS.length] ?? "west"),
    amount: (index * 37) % 1000,
    joined: index % 5 === 0 ? null : new Date(1_700_000_000_000 + index * 1_000),
    label: `label-${String(index % 97)}`,
  }));
  for (let start = 0; start < rows.length; start += 5_000) {
    await database.insertBatch("items", rows.slice(start, start + 5_000));
  }
  return { store, database };
}

interface Workload {
  readonly name: string;
  readonly description: string;
  /** Runs one round against a database created once for the whole soak. */
  run(database: MinnowDatabase, round: number): Promise<void>;
}

const WORKLOADS: readonly Workload[] = [
  {
    name: "queries",
    description: "many distinct and repeated queries, memo and buffer pool exercised",
    async run(database, round) {
      for (let index = 0; index < 300; index += 1) {
        const low = (index * 131 + round * 17) % ROWS;
        await database.query(
          `SELECT id, region, amount FROM items WHERE id BETWEEN ${String(low)} AND ${String(low + 400)} ORDER BY id`,
          { memoize: index % 2 === 0 },
        );
        await database.query(
          "SELECT region, COUNT(*) AS n, SUM(amount) AS s FROM items GROUP BY region",
        );
        await database.query("SELECT id, label FROM items WHERE id = ?", { params: [low + 1] });
      }
    },
  },
  {
    name: "writes",
    description: "point updates and deletes with background folds and collection",
    async run(database, round) {
      for (let index = 0; index < 150; index += 1) {
        const id = ((index * 53 + round * 7) % (ROWS - 1_000)) + 1;
        await database.execute("UPDATE items SET amount = amount + 1 WHERE id = ?", [id]);
        if (index % 10 === 0) {
          await database.query("SELECT COUNT(*) AS n FROM items WHERE amount > 500", {
            memoize: false,
          });
        }
      }
      // Let the background loops run down before the heap is read.
      await new Promise((resolve) => setTimeout(resolve, 300));
    },
  },
  {
    name: "live",
    description: "live query sets and subscriptions opened and closed, commits swept",
    async run(database, round) {
      for (let index = 0; index < 20; index += 1) {
        const live = database.liveQueries();
        const subscriptions = await Promise.all(
          Array.from({ length: 10 }, (_, k) =>
            live.subscribe(`SELECT COUNT(*) AS n FROM items WHERE amount > ${String(900 + k)}`, {
              onChange: () => undefined,
            }),
          ),
        );
        await database.insertBatch("items", [
          {
            id: 1_000_000 + round * 1_000 + index,
            region: "west",
            amount: 999,
            joined: null,
            label: "x",
          },
        ]);
        await live.refresh();
        for (const subscription of subscriptions) subscription.close();
        live.close();
      }
    },
  },
  {
    name: "clients",
    description: "databases opened and closed over one store",
    async run(_database, round) {
      const store = new MemoryBlockStore();
      for (let index = 0; index < 15; index += 1) {
        const database = new MinnowDatabase(store);
        if (index === 0) {
          await database.createTable({
            name: "t",
            uniqueKey: "id",
            columns: [
              { name: "id", type: "number" },
              { name: "v", type: "number" },
            ],
          });
        }
        await database.insertBatch("t", [{ id: round * 100 + index, v: index }]);
        await database.query("SELECT COUNT(*) AS n FROM t");
        await database.close();
      }
    },
  },
];

function heapAfterGc(): number {
  gc?.();
  gc?.();
  return process.memoryUsage().heapUsed;
}

const selected = only === undefined ? WORKLOADS : WORKLOADS.filter((w) => w.name === only);
if (selected.length === 0) {
  console.error(
    `Unknown workload: ${String(only)}. Known: ${WORKLOADS.map((w) => w.name).join(", ")}`,
  );
  process.exit(2);
}
if (!Number.isFinite(rounds) || rounds <= WARMUP_ROUNDS + 1) {
  console.error(`--rounds must be greater than ${String(WARMUP_ROUNDS + 1)}`);
  process.exit(2);
}

let failed = false;
for (const workload of selected) {
  const { database } = await seededDatabase();
  const samples: number[] = [];
  for (let round = 0; round < rounds; round += 1) {
    await workload.run(database, round);
    samples.push(heapAfterGc());
  }
  const settled = samples.slice(WARMUP_ROUNDS);
  const floor = Math.min(...settled);
  const last = samples[samples.length - 1] ?? 0;
  const limit = floor * PLATEAU_RATIO + PLATEAU_SLACK_BYTES;
  const ok = last <= limit;
  failed ||= !ok;
  const megabytes = (bytes: number): string => (bytes / (1024 * 1024)).toFixed(1);
  console.log(
    `${ok ? "ok  " : "LEAK"} ${workload.name.padEnd(8)} ${workload.description}\n` +
      `     heap after gc per round (MB): ${samples.map(megabytes).join(" ")}\n` +
      `     post-warmup floor ${megabytes(floor)} MB, last ${megabytes(last)} MB, limit ${megabytes(limit)} MB`,
  );
  await database.close();
}
process.exit(failed ? 1 : 0);
