/**
 * Isolated built-core commit CPU A/B. Prepares a 10k-row transaction outside each clock;
 * measures commit encoding, log append and durability, then closes outside the clock. Each
 * iteration has its own database, reusing one columnar fixture. No SQL/transport/fsync timing.
 *
 * node scripts/write-commit-bench.mts --baseline ~/.cache/b3-parent/dist --durability relaxed
 * node scripts/write-commit-bench.mts --baseline ~/.cache/b3-parent/dist --durability strict
 *
 * One warmup pair, >=5 interleaved rounds (alternating AB/BA). CPU is process.cpuUsage across
 * all Node threads, including JIT/GC during the measured span. Wall and host load are separate.
 * Optional --gc-before (node --expose-gc) collects setup garbage before each component clock;
 * setup/cleanup and collection outside that clock are excluded. Use the whole-load lane for GC cost.
 * Each reported pass uses its own Node worker, after one excluded same-build warmup.
 * --in-process retains the diagnostic shared-process lane; compare isolated passes for acceptance.
 */
import { resolve, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { loadavg } from "node:os";
import { performance } from "node:perf_hooks";

type Support = typeof import("../packages/core/dist/next/write/test-support.js");
const args = process.argv.slice(2);
const option = (name: string, fallback: string): string => {
  const at = args.indexOf(`--${name}`);
  return at < 0 ? fallback : (args[at + 1] ?? fallback);
};
const integer = (name: string, fallback: number, minimum = 1): number => {
  const value = Number(option(name, String(fallback)));
  if (!Number.isSafeInteger(value) || value < minimum) throw new Error(`Invalid --${name}`);
  return value;
};
const baseline = option("baseline", "");
if (!baseline) throw new Error("--baseline must name the parent's built core dist");
const durability = option("durability", "relaxed");
if (durability !== "relaxed" && durability !== "strict") throw new Error("Invalid durability");
const lane: "strict" | "relaxed" = durability;
const rounds = integer("rounds", 6, 5);
const commits = integer("commits", 64);
const rows = integer("rows", 10000);
const gcBefore = args.includes("--gc-before");
if (gcBefore && typeof global.gc !== "function")
  throw new Error("--gc-before needs node --expose-gc");
const directories = {
  baseline: resolve(baseline),
  candidate: resolve(option("candidate", "packages/core/dist")),
};
type Label = keyof typeof directories;
const builds = {} as Record<Label, Support>;
for (const label of ["baseline", "candidate"] as const)
  builds[label] = (await import(
    pathToFileURL(join(directories[label], "next/write/test-support.js")).href
  )) as Support;
interface Result {
  cpuUs: number;
  wallUs: number;
  load: number[];
}
async function measure(label: Label): Promise<Result> {
  const s = builds[label];
  const prepared = Array.from({ length: Math.ceil(rows / 2048) }, (_, part) => {
    const ids = Array.from(
      { length: Math.min(2048, rows - part * 2048) },
      (_, k) => part * 2048 + k,
    );
    return s.batch(
      s.ints(ids),
      s.texts(ids.map((id) => `value ${String(id)}`)),
      s.ints(ids.map((id) => id % 1000)),
    );
  });
  let cpuUs = 0;
  let wallMs = 0;
  for (let commit = 0; commit < commits; commit++) {
    const h = new s.Harness(lane, "memory");
    const p = await h.open();
    try {
      let table = 0;
      await s.autocommit(p, async (statement) => {
        table =
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
      const tx = await p.begin();
      const statement = tx.statement({ rules: s.rules });
      for (const batch of prepared) await statement.table(table).insert(batch, null);
      await statement.end();
      if (gcBefore) global.gc?.();
      const cpu = process.cpuUsage();
      const wall = performance.now();
      const receipt = await tx.commit(null);
      await receipt.durable;
      await p.flush();
      const used = process.cpuUsage(cpu);
      cpuUs += used.user + used.system;
      wallMs += performance.now() - wall;
      if (!receipt.wrote) throw new Error("Empty benchmark commit");
    } finally {
      await h.close();
    }
  }
  return { cpuUs: cpuUs / commits, wallUs: (wallMs * 1000) / commits, load: loadavg() };
}
// Separate workers keep one build's feedback/GC state out of the other's measured pass.
const workerLabel = option("worker", "");
if (workerLabel !== "") {
  if (workerLabel !== "baseline" && workerLabel !== "candidate")
    throw new Error("Invalid --worker");
  const label: Label = workerLabel;
  await measure(label);
  console.log(JSON.stringify({ isolatedWorker: true, ...(await measure(label)) }));
  process.exit(0);
}
async function measurePass(label: Label): Promise<Result> {
  if (args.includes("--in-process")) return measure(label);
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
        label,
        round,
        warmup: round < 0,
        rows,
        commits,
        durability,
        gcBefore,
        ...result,
      }),
    );
    if (round >= 0) results[label].push(result);
  }
}
const median = (values: number[]): number => {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? ((sorted[mid - 1] ?? 0) + (sorted[mid] ?? 0)) / 2
    : (sorted[mid] ?? 0);
};
const summarize = (label: Label) => {
  const cpu = results[label].map((value) => value.cpuUs);
  const wall = results[label].map((value) => value.wallUs);
  return {
    medianCpuUs: median(cpu),
    minCpuUs: Math.min(...cpu),
    maxCpuUs: Math.max(...cpu),
    medianWallUs: median(wall),
  };
};
console.log(
  JSON.stringify({
    summary: true,
    processIsolation: !args.includes("--in-process"),
    workerWarmups: args.includes("--in-process") ? 0 : 1,
    component: "prepared 10k-row commit and durability",
    gcBefore,
    durability,
    rows,
    commits,
    rounds,
    baseline: summarize("baseline"),
    candidate: summarize("candidate"),
  }),
);
