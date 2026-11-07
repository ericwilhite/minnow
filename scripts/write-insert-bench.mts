/**
 * Built write-path A/B bench. Capture the parent's core dist under ~/.cache before rebuilding;
 * pass both dist directories. No vite-node transforms occur in the timed code.
 *
 * node scripts/write-insert-bench.mts --baseline ~/.cache/b3-old/dist --mode unique
 * node scripts/write-insert-bench.mts --baseline ~/.cache/b3-old/dist --mode single --durability relaxed
 * node scripts/write-insert-bench.mts --baseline ~/.cache/b3-old/dist --mode single --durability strict
 * node scripts/write-insert-bench.mts --baseline ~/.cache/b3-old/dist --mode load --load-mode tx
 * node scripts/write-insert-bench.mts --baseline ~/.cache/b3-old/dist --mode load --load-mode commits
 *
 * Component: unique-key range/chain proof, including the old snapshot/reader setup. Whole paths: single inserts
 * after 100k keyed rows, or a durable 1M-row load including maintenance. Each pass builds its own inputs and database setup
 * outside the clock. Singles include the final flush; strict waits for every durable receipt.
 * One warmup pair is discarded, then >=5 interleaved pairs, alternating AB/BA order. CPU includes
 * all Node threads (including JIT/GC); wall is reported separately. The store is memory, no fsync,
 * parser, executor or owner/client transport. Optional --gc-before (node --expose-gc) collects
 * setup garbage before each clock; GC during the measured phase still counts. Each timed database is closed before the next run.
 * Each reported pass uses its own Node worker, after one excluded same-build warmup.
 * --in-process retains the diagnostic shared-process lane; compare isolated passes for acceptance.
 */
import { resolve, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { performance } from "node:perf_hooks";
import { loadavg } from "node:os";

type Support = typeof import("../packages/core/dist/next/write/test-support.js");
type Views = typeof import("../packages/core/dist/next/write/view.js");
const args = process.argv.slice(2);
const option = (name: string, fallback: string): string => {
  const at = args.indexOf(`--${name}`);
  return at < 0 ? fallback : (args[at + 1] ?? fallback);
};
const integer = (name: string, fallback: number, minimum = 1): number => {
  const n = Number(option(name, String(fallback)));
  if (!Number.isSafeInteger(n) || n < minimum) throw new Error(`Invalid --${name}`);
  return n;
};
const baseline = option("baseline", "");
if (!baseline) throw new Error("--baseline must name the parent's built core dist directory");
const mode = option("mode", "unique");
if (!["unique", "single", "load"].includes(mode)) throw new Error("Invalid --mode");
const durability = option("durability", "relaxed");
if (durability !== "strict" && durability !== "relaxed") throw new Error("Invalid --durability");
const lane: "strict" | "relaxed" = durability;
const loadMode = option("load-mode", "tx");
if (loadMode !== "tx" && loadMode !== "commits") throw new Error("Invalid --load-mode");
const gcBefore = args.includes("--gc-before");
if (gcBefore && typeof global.gc !== "function")
  throw new Error("--gc-before needs node --expose-gc");
const collectSetup = () => {
  if (gcBefore) global.gc?.();
};
const rounds = integer("rounds", 6, 5);
const iterations = integer("iterations", 1_000_000);
const singles = integer("singles", 20_000);
const rows = integer("rows", 1_000_000);
const batchSize = 2048;
const directories = {
  baseline: resolve(baseline),
  candidate: resolve(option("candidate", "packages/core/dist")),
};
type Label = keyof typeof directories;
interface Build {
  support: Support;
  views: Views;
}
const builds = {} as Record<Label, Build>;
const batches = (s: Support, count: number, size: number, start = 0) =>
  Array.from({ length: Math.ceil(count / size) }, (_, part) => {
    const ids = Array.from(
      { length: Math.min(size, count - part * size) },
      (_, k) => start + part * size + k,
    );
    return s.batch(
      s.ints(ids),
      s.texts(ids.map((id) => `value ${String(id)}`)),
      s.ints(ids.map((id) => id % 1000)),
    );
  });
for (const label of ["baseline", "candidate"] as const) {
  const support = (await import(
    pathToFileURL(join(directories[label], "next/write/test-support.js")).href
  )) as Support;
  builds[label] = {
    support,
    views: (await import(
      pathToFileURL(join(directories[label], "next/write/view.js")).href
    )) as Views,
  };
}
interface Result {
  cpuMs: number;
  wallMs: number;
  checksum?: number;
  flushes?: number;
  merges?: number;
}
const clock = () => ({ wall: performance.now(), cpu: process.cpuUsage() });
const elapsed = (start: ReturnType<typeof clock>): Result => {
  const used = process.cpuUsage(start.cpu);
  return { cpuMs: (used.user + used.system) / 1000, wallMs: performance.now() - start.wall };
};
async function measure(build: Build, label: Label): Promise<Result> {
  const s = build.support;
  // Only this pass retains its fixture; constructing it is outside the measured phase.
  const prepared =
    mode === "unique"
      ? []
      : batches(
          s,
          mode === "single" ? singles : rows,
          mode === "single" ? 1 : batchSize,
          mode === "single" ? 1_000_000 : 0,
        );
  const preload = mode === "single" || mode === "unique" ? batches(s, 100_000, batchSize) : [];
  const h = new s.Harness(lane, "memory");
  const p = await h.open();
  try {
    let t = 0;
    await s.autocommit(p, async (statement) => {
      t =
        (
          await statement.changeCatalog({
            kind: "createTable",
            table: s.tableInput("t", [
              s.column("id", s.INT, false),
              s.column("v", s.TEXT),
              s.column("n", s.INT),
            ]),
          })
        ).id ?? 0;
    });
    for (const batch of preload)
      await s.autocommit(p, (statement) => statement.table(t).insert(batch, null));
    await p.quiesce();
    if (mode === "unique") {
      // One open transaction supplies real, isolated own entries beyond the durable key ranges.
      const tx = await p.begin();
      const statement = tx.statement({ rules: s.rules });
      const count = 1024;
      await statement
        .table(t)
        .insert(batches(s, count, count, 1_000_000)[0] ?? s.batch(s.ints([])), null);
      await statement.end();
      const table = p.state().tables.get(t);
      const def = tx.catalog.table(t);
      if (table === undefined || def === undefined || def.primaryKey === null)
        throw new Error("No test table");
      const index = def.indexes.find((i) => i.id === def.primaryKey);
      const own = table.memtables[table.memtables.length - 1]?.deltas.get(def.primaryKey);
      if (index === undefined || own === undefined) throw new Error("No test index");
      const first = own.entryCount - count;
      const keys = Array.from({ length: count }, (_, k) => own.keyBytes(own.keyOf(first + k)));
      let matches = 0;
      collectSetup();
      const start = clock();
      for (let k = 0; k < iterations; k++) {
        const at = k & (count - 1);
        const key = keys[at];
        if (key === undefined) throw new Error("Missing key");
        const entry = first + at;
        const slot = own.keyOf(entry);
        let overlaps: boolean;
        if (label === "baseline") {
          const view = new build.views.SnapshotImpl({
            state: p.state(),
            bound: Number.MAX_SAFE_INTEGER,
            observedLsn: tx.baseLsn,
            catalog: tx.catalog,
            shared: tx.hooks.shared,
            label: "checks",
            statementView: false,
          });
          try {
            const reader = new build.views.ViewIndex(
              { ...index, unique: false },
              view.internalTable(t),
            );
            overlaps = reader.othersOverlap(key, key, own);
            reader.skipRunsOutside(key, key);
          } finally {
            view.releaseInternal();
          }
        } else
          overlaps =
            tx.catalog.table(t) !== def ||
            !build.views.otherIndexLayersEndBefore(table, index.id, key, own);
        if (overlaps || own.head(slot) !== entry || own.nextEntry(entry) >= 0) matches++;
      }
      const result = { ...elapsed(start), checksum: matches };
      if (matches !== 0) throw new Error("The component fixture is not isolated");
      tx.rollback();
      return result;
    }
    collectSetup();
    const start = clock();
    if (mode === "single") {
      for (const batch of prepared) {
        const tx = await p.begin();
        const statement = tx.statement({ rules: s.rules });
        await statement.table(t).insert(batch, null);
        await statement.end();
        const receipt = await tx.commit(null);
        if (durability === "strict") await receipt.durable;
      }
      await p.flush();
    } else {
      const commitBatches = loadMode === "tx" ? prepared.length : 5;
      for (let from = 0; from < prepared.length; from += commitBatches) {
        const tx = await p.begin();
        const statement = tx.statement({ rules: s.rules });
        for (const batch of prepared.slice(from, from + commitBatches))
          await statement.table(t).insert(batch, null);
        await statement.end();
        await (
          await tx.commit(null)
        ).durable;
      }
      await p.quiesce();
    }
    return { ...elapsed(start), flushes: p.maintenance.flushes, merges: p.maintenance.merges };
  } finally {
    await h.close();
  }
}
// Separate workers keep one build's feedback/GC state out of the other's measured pass.
const workerLabel = option("worker", "");
if (workerLabel !== "") {
  if (workerLabel !== "baseline" && workerLabel !== "candidate")
    throw new Error("Invalid --worker");
  const label: Label = workerLabel;
  await measure(builds[label], label);
  console.log(JSON.stringify({ isolatedWorker: true, ...(await measure(builds[label], label)) }));
  process.exit(0);
}
async function measurePass(label: Label): Promise<Result> {
  if (args.includes("--in-process")) return measure(builds[label], label);
  const worker = spawnSync(
    process.execPath,
    [...process.execArgv, fileURLToPath(import.meta.url), ...args, "--worker", label],
    { encoding: "utf8", maxBuffer: 1024 * 1024 },
  );
  if (worker.status !== 0)
    throw new Error(`Benchmark worker failed: ${worker.stderr}${worker.stdout}`);
  const line = worker.stdout.trim().split("\n").at(-1);
  if (!line) throw new Error("Benchmark worker returned no result");
  return JSON.parse(line) as Result;
}

const results: Record<Label, Result[]> = { baseline: [], candidate: [] };
for (let round = -1; round < rounds; round++) {
  const order: Label[] = round % 2 === 0 ? ["baseline", "candidate"] : ["candidate", "baseline"];
  for (const label of order) {
    const result = await measurePass(label);
    console.log(
      JSON.stringify({
        mode,
        loadMode,
        durability,
        label,
        round,
        warmup: round < 0,
        load: loadavg(),
        ...result,
      }),
    );
    if (round >= 0) results[label].push(result);
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
}
const median = (values: number[]): number => {
  const sorted = [...values].sort((a, b) => a - b);
  const at = Math.floor(sorted.length / 2);
  const value = sorted[at];
  if (value === undefined) throw new Error("No measured rounds");
  return sorted.length % 2 === 0 ? ((sorted[at - 1] ?? value) + value) / 2 : value;
};
const summary = (values: Result[]) => ({
  cpuMedianMs: median(values.map((v) => v.cpuMs)),
  cpuMinMs: Math.min(...values.map((v) => v.cpuMs)),
  cpuMaxMs: Math.max(...values.map((v) => v.cpuMs)),
  wallMedianMs: median(values.map((v) => v.wallMs)),
});
const before = summary(results.baseline),
  after = summary(results.candidate);
const operations = mode === "unique" ? iterations : mode === "single" ? singles : rows;
console.log(
  JSON.stringify({
    summary: true,
    processIsolation: !args.includes("--in-process"),
    workerWarmups: args.includes("--in-process") ? 0 : 1,
    gcBefore,
    fixtureLifetime: "per-pass",
    node: process.version,
    mode,
    loadMode,
    durability,
    rounds,
    operations,
    directories,
    baseline: before,
    candidate: after,
    baselineCpuUs: (before.cpuMedianMs * 1000) / operations,
    candidateCpuUs: (after.cpuMedianMs * 1000) / operations,
    beatsSpread: after.cpuMedianMs < before.cpuMinMs && before.cpuMedianMs > after.cpuMaxMs,
  }),
);
