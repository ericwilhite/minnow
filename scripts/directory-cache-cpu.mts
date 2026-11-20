/** Native directory sizing only. A/A precedes A/B, nine independent balanced pairs
 * per boundary, natural GC and all-thread process CPU. Whole SELECT cells require
 * the separate native campaign; this component cannot establish query acceptance.
 *
 * node scripts/directory-cache-cpu.mts --baseline <frozen-runtime> \
 *   --candidate <frozen-runtime> --output <new-evidence-directory>
 */
import { createHash } from "node:crypto";
import { appendFileSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve, join } from "node:path";
import { cpus, loadavg, totalmem } from "node:os";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { manifest } from "./next-executor-quiet.mts";
import { runIndependentCPU, type CPUArm } from "./lib/independent-cpu.mts";
import type { DirectoryComponentConfig } from "./directory-cache-cpu-worker.mts";

const option = (name: string) => {
  const at = process.argv.indexOf(name);
  const value = process.argv[at + 1];
  if (at < 0 || !value || value.startsWith("--")) throw new Error(`Missing ${name}`);
  return resolve(value);
};
const hash = (bytes: Uint8Array | string) => createHash("sha256").update(bytes).digest("hex");
const sourceFiles = [
  "directory-cache-cpu.mts",
  "directory-cache-cpu-worker.mts",
  "next-executor-quiet.mts",
  "next-percentile-component.mts",
  "lib/independent-cpu.mts",
  "lib/query-root-lifetimes.mts",
  "lib/paired-cpu.mts",
];
const sources = async () =>
  Object.fromEntries(
    await Promise.all(
      sourceFiles.map(async (name) => [name, hash(await readFile(new URL(name, import.meta.url)))]),
    ),
  );
async function main() {
  if (
    process.env.NODE_V8_COVERAGE ||
    [...process.execArgv, process.env.NODE_OPTIONS ?? ""].some((value) =>
      /--disable-optimizing-compilers|--jitless|--no-opt(?:\s|$)/u.test(value),
    )
  )
    throw new Error("CPU arms require ordinary optimization without coverage diagnostics");
  const runner = {
    node: process.version,
    execArgv: process.execArgv,
    nodeOptions: process.env.NODE_OPTIONS ?? "",
    cpus: cpus(),
    totalMemoryBytes: totalmem(),
    initialLoad: loadavg(),
    affinity:
      process.platform === "linux"
        ? (/^Cpus_allowed_list:\s*(.+)$/mu
            .exec(await readFile("/proc/self/status", "utf8"))?.[1]
            ?.trim() ?? null)
        : null,
  };
  const baselineRoot = option("--baseline"),
    candidateRoot = option("--candidate"),
    output = option("--output"),
    baseline = await manifest(baselineRoot),
    candidate = await manifest(candidateRoot),
    sourceHashes = await sources();
  if (baseline.revision === candidate.revision)
    throw new Error("A true-parent component requires distinct compiled revision identities");
  // No existing raw evidence can be overwritten, including a partial campaign.
  await mkdir(output);
  const policy = {
    rows: 16384,
    columns: 30,
    warmups: 5,
    repeats: 64,
    pairs: 9,
    settleMs: 1000,
    workloads: ["directory_size_retained", "directory_size_once"],
    boundary: "sizing",
    gain: "paired median CPU saving exceeds full paired range AND A/A median bias + IQR",
    limits: [
      "Node MemoryBackend, metadata sizing component; no browser or OPFS",
      "Rows are logical rows sized, not delivered SELECT rows",
      "No whole-query acceptance, cold I/O, fsync or renderer RSS claim",
      "Non-retaining control begins without parsed directory entries",
    ],
  } as const;
  await writeFile(
    join(output, "input.json"),
    JSON.stringify({ policy, runner, baseline, candidate, sourceHashes }, null, 2) + "\n",
  );
  const abort = new AbortController(),
    stop = () => abort.abort(new Error("Directory component interrupted"));
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  const arm = (root: string, revision: string): CPUArm => ({
    module: fileURLToPath(new URL("./directory-cache-cpu-worker.mts", import.meta.url)),
    cwd: root,
    revision,
    execArgv: ["--max-old-space-size=2048"],
    data: {
      runtimeRoot: root,
      rows: policy.rows,
      columns: policy.columns,
      warmups: policy.warmups,
      repeats: policy.repeats,
    } satisfies DirectoryComponentConfig,
  });
  const events = (value: unknown) =>
    appendFileSync(join(output, "events.jsonl"), JSON.stringify(value) + "\n");
  async function run(aa: boolean) {
    const result = await runIndependentCPU({
      control: arm(aa ? candidateRoot : baselineRoot, aa ? candidate.revision : baseline.revision),
      candidate: arm(candidateRoot, candidate.revision),
      purpose: aa ? "identical-build-control" : "true-parent",
      workloads: policy.workloads,
      boundaries: [policy.boundary],
      primaryBoundary: policy.boundary,
      pairs: policy.pairs,
      settleMs: policy.settleMs,
      signal: abort.signal,
      onEvent: events,
      onSample: (sample) =>
        appendFileSync(join(output, aa ? "aa.jsonl" : "ab.jsonl"), JSON.stringify(sample) + "\n"),
    });
    const pids = result.records.flatMap((record) => record.samples.map((s) => s.identity.pid));
    if (new Set(pids).size !== policy.workloads.length * 2 * policy.pairs)
      throw new Error("A component worker was reused");
    for (const record of result.records)
      for (const sample of record.samples) {
        const detail = sample.detail as {
          completedLifetimes?: number;
          rowsPerLifetime?: number;
          cache?: {
            hits: number;
            misses: number;
            entries: number;
            residentBytes: number;
            evictableBytes: number;
          } | null;
        };
        if (
          detail.completedLifetimes !== policy.repeats ||
          detail.rowsPerLifetime !== policy.rows ||
          sample.rows !== policy.rows * policy.repeats
        )
          throw new Error("Incomplete directory sizing volume");
        if (
          detail.cache &&
          (detail.cache.residentBytes !== detail.cache.evictableBytes ||
            (record.workload === "directory_size_retained" &&
              (detail.cache.hits === 0 || detail.cache.misses !== 0)) ||
            (record.workload === "directory_size_once" &&
              (detail.cache.entries !== 0 || detail.cache.hits !== 0)))
        )
          throw new Error("Directory component did not exercise its declared retention boundary");
      }
    await writeFile(
      join(output, aa ? "aa.json" : "ab.json"),
      JSON.stringify(result, null, 2) + "\n",
    );
    return result;
  }
  try {
    const aa = await run(true);
    if (
      aa.records.some(
        (record) => record.acceptance.acceptedGain || record.acceptance.significantRegression,
      )
    )
      throw new Error("A/A false signal; refusing component A/B acceptance");
    const ab = await run(false);
    const results = ab.records.map((record, index) => {
      const control = aa.records[index];
      if (!control || control.workload !== record.workload) throw new Error("Missing A/A workload");
      const bias = Math.abs(control.acceptance.medianDeltaMs),
        cpu = record.acceptance,
        gain =
          -cpu.medianDeltaMs > cpu.rangeDeltaMs &&
          -cpu.medianDeltaMs > bias + cpu.iqrDeltaMs &&
          cpu.favoursCandidate >= cpu.requiredFavourable;
      return {
        workload: record.workload,
        aaBiasMs: bias,
        cpu,
        cpuPerSizing: Object.fromEntries(
          Object.entries(cpu)
            .filter(([key]) => key.endsWith("Ms"))
            .map(([key, value]) => [key, Number(value) / policy.repeats]),
        ),
        acceptedComponentGain: record.workload === "directory_size_retained" && gain,
        noAcceptedRegression: !cpu.significantRegression,
      };
    });
    if (
      !isDeepStrictEqual(await manifest(baselineRoot), baseline) ||
      !isDeepStrictEqual(await manifest(candidateRoot), candidate) ||
      !isDeepStrictEqual(await sources(), sourceHashes)
    )
      throw new Error("Component inputs changed during observation");
    const accepted =
      results.every((result) => result.noAcceptedRegression) &&
      results.some((result) => result.acceptedComponentGain);
    await writeFile(
      join(output, "results.json"),
      JSON.stringify(
        { policy, runner, baseline, candidate, sourceHashes, results, accepted },
        null,
        2,
      ) + "\n",
    );
    console.log(JSON.stringify({ accepted, results }));
    if (!accepted) process.exitCode = 1;
  } finally {
    process.off("SIGINT", stop);
    process.off("SIGTERM", stop);
  }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
