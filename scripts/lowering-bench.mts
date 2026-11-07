/**
 * The cost of lowering one statement when the owner's plan cache misses (lower/lowerer.ts): parse,
 * front-end passes, typing and IR construction, warm JIT, over a small retail catalog.
 *
 * Target: a point read lowers well inside one cross-tab round trip (55–280 µs, PROPOSAL.md), so
 * a cold plan never dominates ruling R6's 0.25 ms warm point read; every M1 statement below 1 ms.
 * M3: a join of up to 8 relations (exhaustive join ordering) and the greedy order above it stay
 * below 1 ms, so join planning never costs more than the round trip it replaces several times.
 *
 *   npx vite-node scripts/lowering-bench.mts [--rounds 2000]
 *
 * Nothing here writes to the repository.
 */
import { createLowerer } from "../packages/core/src/next/lower/create.ts";
import type { LoweringContext } from "../packages/core/src/next/lower/lowerer.ts";
import { MemoryCatalog } from "../packages/core/src/next/lower/memory-catalog.ts";

const roundsFlag = process.argv.indexOf("--rounds");
const rounds = roundsFlag === -1 ? 2000 : Number(process.argv[roundsFlag + 1]);

const lowerer = createLowerer();
const catalog = new MemoryCatalog();
const context = (): LoweringContext => ({
  catalog: catalog.view,
  statistics: { table: () => ({ rowCount: 100_000, column: () => null }) },
  costs: {
    decodePerValue: 1,
    filterPerRow: 1,
    hashBuildPerRow: 20,
    hashProbePerRow: 15,
    aggregatePerRow: 5,
    sortPerRowLog: 3,
    emitPerValue: 2,
    spillBytesPerSecond: 600e6,
  },
});
for (const sql of [
  "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(40) NOT NULL, region TEXT, tier SMALLINT DEFAULT 1, joined TIMESTAMP)",
  "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id INTEGER NOT NULL, amount NUMERIC(10, 2), qty INTEGER, note TEXT, total DOUBLE PRECISION GENERATED ALWAYS AS (qty * 2.5) STORED)",
  "CREATE TABLE regions (name TEXT PRIMARY KEY, manager TEXT)",
  ...Array.from(
    { length: 9 },
    (_, index) =>
      `CREATE TABLE t${String(index)} (id INTEGER PRIMARY KEY, next INTEGER, v INTEGER)`,
  ),
]) {
  const lowered = lowerer.lower(sql, context());
  if (lowered.kind !== "ddl" || lowered.change === null) throw new Error(sql);
  catalog.apply(lowered.change);
}

const cases: ReadonlyArray<readonly [string, string]> = [
  ["point read", "SELECT name, tier FROM customers WHERE id = $1"],
  [
    "filtered top-N",
    "SELECT id, name FROM customers WHERE region = $1 AND tier >= 2 ORDER BY joined DESC LIMIT 20",
  ],
  [
    "aggregate",
    "SELECT customer_id, count(*), sum(qty), avg(amount) FROM orders GROUP BY customer_id HAVING count(*) > 1 ORDER BY 2 DESC",
  ],
  ["insert", "INSERT INTO orders (id, customer_id, amount, qty) VALUES ($1, $2, $3, $4)"],
  ["update by key", "UPDATE orders SET qty = qty + 1 WHERE id = $1 RETURNING total"],
  ["delete", "DELETE FROM customers WHERE region = 'east'"],
  [
    "3-way star",
    "SELECT c.name, r.manager, o.amount FROM orders o JOIN customers c ON c.id = o.customer_id JOIN regions r ON r.name = c.region WHERE o.qty > $1",
  ],
  ["6-way chain", chain(6)],
  // The exhaustive join-order search's largest case (EXHAUSTIVE_JOIN_LIMIT relations).
  ["8-way chain", chain(8)],
  ["9-way chain", chain(9)],
  [
    "correlated sub",
    "SELECT c.name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.qty > 3) AND c.id NOT IN (SELECT customer_id FROM orders WHERE amount > 100)",
  ],
  [
    "window",
    "SELECT customer_id, amount, row_number() OVER (PARTITION BY customer_id ORDER BY amount DESC), sum(qty) OVER (PARTITION BY customer_id) FROM orders",
  ],
  [
    "update from",
    "UPDATE customers c SET tier = 2 FROM orders o WHERE o.customer_id = c.id AND o.amount > $1",
  ],
];

/** `t0 ⋈ t1 ⋈ … ⋈ t(n-1)` along `next`. */
function chain(length: number): string {
  const joins = Array.from(
    { length: length - 1 },
    (_, index) => `JOIN t${String(index + 1)} ON t${String(index + 1)}.id = t${String(index)}.next`,
  ).join(" ");
  return `SELECT t0.v, t${String(length - 1)}.v FROM t0 ${joins} WHERE t0.v > 3`;
}

function median(samples: number[]): number {
  const sorted = [...samples].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

for (const [label, sql] of cases) {
  for (let warm = 0; warm < 200; warm += 1) lowerer.lower(sql, context());
  const samples: number[] = [];
  for (let round = 0; round < rounds; round += 1) {
    const start = performance.now();
    lowerer.lower(sql, context());
    samples.push((performance.now() - start) * 1000);
  }
  const sorted = [...samples].sort((a, b) => a - b);
  const p95 = sorted[Math.floor(sorted.length * 0.95)] ?? 0;
  console.log(
    `${label.padEnd(16)} median ${median(samples).toFixed(1).padStart(7)} µs   p95 ${p95.toFixed(1).padStart(7)} µs`,
  );
}
