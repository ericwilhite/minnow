/**
 * The cost of lowering one statement when the owner's plan cache misses (lower/lowerer.ts): parse,
 * front-end passes, typing and IR construction, warm JIT, over a small retail catalog.
 *
 * Target: a point read lowers well inside one cross-tab round trip (55–280 µs, PROPOSAL.md), so
 * a cold plan never dominates ruling R6's 0.25 ms warm point read; every M1 statement below 1 ms.
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
];

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
