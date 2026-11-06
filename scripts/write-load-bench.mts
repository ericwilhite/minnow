/**
 * The write path's load and insert costs (PROPOSAL "Targets": 1M-row keyed load ≤ 1.5 s durable;
 * relaxed prepared INSERT ≤ 20 µs owner CPU), owner-side, in Node, on the in-memory store:
 *
 *   node scripts/write-load-bench.mts [--rows 1000000] [--batch 2048]
 *     [--mode tx|commits|single] [--durability strict|relaxed] [--commit 10000] [--singles 20000]
 *
 * - `tx`: one transaction of `rows` keyed rows (id INT PRIMARY KEY, v TEXT, n INT) inserted in
 *   calls of `batch` rows (staged as it goes), committed, then quiesced (the fold).
 * - `commits`: the same rows in autocommit transactions of `commit` rows.
 * - `single`: `singles` one-row transactions after a 100k-row table, each acknowledged as its
 *   durability says (strict: after its flush; relaxed: at commit): µs per insert (wall and process
 *   CPU), the relaxed lane's owner cost.
 * Build core first (npx tsc -b packages/core). Optional --profile ~/.cache/load.cpuprofile
 * records only the measured phase; Inspector stop/serialization are outside its CPU/wall clock.
 * Rows are built before the clock starts (the engine's cost alone is measured). Reported: wall
 * and CPU per phase, rows/s, staged and flushed counts. The memory store has no
 * disk: durable here means the log's flush calls complete, not a real fsync. The machine is
 * shared: compare runs made back to back.
 */
import { Session, type Profiler } from "node:inspector";
import { writeFile, mkdir } from "node:fs/promises";
import { resolve, dirname, relative, join } from "node:path";
import { homedir, loadavg } from "node:os";
import { performance, PerformanceObserver } from "node:perf_hooks";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rules,
  tableInput,
  TEXT,
  texts,
} from "../packages/core/dist/next/write/test-support.js";

const args = process.argv.slice(2);
const option = (name: string, fallback: string): string => {
  const index = args.indexOf(`--${name}`);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};
// Build core first. Profiling includes load and quiesce, not input construction or close.
const profileFile = option("profile", "");
const inspector = profileFile ? new Session() : null;
const startProfile = async () => {
  if (!inspector) return;
  const file = resolve(profileFile);
  if (relative(join(homedir(), ".cache"), file).startsWith(".."))
    throw new Error("--profile must be under ~/.cache");
  await mkdir(dirname(file), { recursive: true });
  inspector.connect();
  await new Promise<void>((ok, fail) =>
    inspector.post("Profiler.enable", (error) => (error ? fail(error) : ok())),
  );
  await new Promise<void>((ok, fail) =>
    inspector.post("Profiler.start", (error) => (error ? fail(error) : ok())),
  );
};
const stopProfile = async () => {
  if (!inspector) return;
  const profile = await new Promise<Profiler.Profile>((ok, fail) =>
    inspector.post("Profiler.stop", (error, result) => (error ? fail(error) : ok(result.profile))),
  );
  inspector.disconnect();
  await writeFile(resolve(profileFile), JSON.stringify(profile));
};
const ROWS = Number(option("rows", "1000000"));
const BATCH = Number(option("batch", "2048"));
const MODE = option("mode", "tx");
const DURABILITY = option("durability", "strict") as "strict" | "relaxed";
const COMMIT = Number(option("commit", "10000"));
const SINGLES = Number(option("singles", "20000"));

// Garbage-collection pauses in the measured phases (every GC entry Node reports).
let gcMs = 0;
const gcObserver = new PerformanceObserver((list) => {
  for (const entry of list.getEntries()) gcMs += entry.duration;
});
gcObserver.observe({ entryTypes: ["gc"] });
/** GC time so far (entries arrive asynchronously: take what is buffered first). */
const gcSoFar = () => {
  for (const entry of gcObserver.takeRecords()) gcMs += entry.duration;
  return gcMs;
};

const cpu = () => {
  const usage = process.cpuUsage();
  return (usage.user + usage.system) / 1000;
};
const columns = (ids: number[]) =>
  batch(ints(ids), texts(ids.map((id) => `value ${String(id)}`)), ints(ids.map((id) => id % 1000)));

const harness = new Harness(DURABILITY, "memory", undefined, undefined, {});
const path = await harness.open();
let t = 0;
await autocommit(path, async (s) => {
  t =
    (
      await s.changeCatalog({
        kind: "createTable",
        table: tableInput("t", [column("id", INT, false), column("v", TEXT), column("n", INT)]),
      })
    ).id ?? 0;
});

const report: Record<string, unknown> = { mode: MODE, rows: ROWS, durability: DURABILITY };
if (MODE === "single") {
  for (let from = 0; from < 100_000; from += BATCH) {
    const ids = Array.from({ length: Math.min(BATCH, 100_000 - from) }, (_, k) => from + k);
    await autocommit(path, (s) => s.table(t).insert(columns(ids), null));
  }
  await path.quiesce();
  // Built before the clock starts: the engine's cost alone is measured.
  const singles = Array.from({ length: SINGLES }, (_, k) => columns([1_000_000 + k]));
  await startProfile();
  gcSoFar();
  gcMs = 0;
  report.loadAverageBefore = loadavg();
  const wall0 = performance.now();
  const cpu0 = cpu();
  // A commit is acknowledged as its mode says: strict after the log flush, relaxed at commit.
  for (const row of singles) {
    const tx = await path.begin();
    const s = tx.statement({ rules });
    await s.table(t).insert(row, null);
    await s.end();
    const receipt = await tx.commit(null);
    if (DURABILITY === "strict") await receipt.durable;
  }
  await path.flush();
  report.singleWallUs = Number((((performance.now() - wall0) * 1000) / SINGLES).toFixed(1));
  report.singleCpuUs = Number((((cpu() - cpu0) * 1000) / SINGLES).toFixed(1));
} else {
  // Every batch is built before the clock starts: the engine's cost alone is measured.
  const batches: Array<{ commit: number; batch: ReturnType<typeof columns> }> = [];
  for (let from = 0; from < ROWS; from += BATCH) {
    const ids = Array.from({ length: Math.min(BATCH, ROWS - from) }, (_, k) => from + k);
    batches.push({ commit: Math.floor(from / COMMIT), batch: columns(ids) });
  }
  await startProfile();
  gcSoFar();
  gcMs = 0;
  report.loadAverageBefore = loadavg();
  const wall0 = performance.now();
  const cpu0 = cpu();
  gcMs = 0;
  if (MODE === "tx") {
    const tx = await path.begin();
    const s = tx.statement({ rules });
    for (const { batch: part } of batches) await s.table(t).insert(part, null);
    await s.end();
    await (
      await tx.commit(null)
    ).durable;
  } else {
    // Batches of `batch` rows grouped into commits of about `commit` rows.
    for (let k = 0; k < batches.length;) {
      const commit = batches[k]?.commit;
      const tx = await path.begin();
      const s = tx.statement({ rules });
      for (; k < batches.length && batches[k]?.commit === commit; k++)
        await s.table(t).insert(batches[k]?.batch ?? columns([]), null);
      await s.end();
      await (
        await tx.commit(null)
      ).durable;
    }
  }
  const loaded = performance.now();
  const loadCpu = cpu();
  await path.quiesce();
  report.loadWallMs = Math.round(loaded - wall0);
  report.loadCpuMs = Math.round(loadCpu - cpu0);
  report.settleWallMs = Math.round(performance.now() - loaded);
  report.totalCpuMs = Math.round(cpu() - cpu0);
  report.gcMs = Math.round(gcSoFar());
  report.totalWallMs = Math.round(performance.now() - wall0);
  report.rowsPerSecond = Math.round(ROWS / ((performance.now() - wall0) / 1000));
  report.stages = path.maintenance.stages;
  report.stagedRows = path.maintenance.stagedRows;
  report.flushes = path.maintenance.flushes;
  report.merges = path.maintenance.merges;
  report.heapMiB = Math.round(process.memoryUsage().heapUsed / 1048576);
}
report.loadAverageAfter = loadavg();
await stopProfile(); // CPU/wall fields above exclude Inspector.stop and serialization.
console.log(JSON.stringify(report));
await harness.close();
