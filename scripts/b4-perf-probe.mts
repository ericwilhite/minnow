/** BRANCH ONLY: untimed W1M directory-validation work counts. No CPU experiment. */
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { resolve, relative, join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { startCPUWorker } from "./lib/independent-cpu.mts";
import type { B4WorkerData, B4Verification } from "./b4-perf-worker.mts";
assert(
  process.env.GITHUB_ACTIONS === "true" && process.env.GITHUB_RUN_ID,
  "Workflow-only untimed W1M diagnostic; do not run on the shared development host",
);
const { values } = parseArgs({
  options: { runtime: { type: "string" }, output: { type: "string" }, arm: { type: "string" } },
});
assert(
  values.runtime && values.output && (values.arm === "parent" || values.arm === "cache"),
  "Use --runtime, --output and --arm parent|cache",
);
const arm = values.arm;
const expectedRevision =
  arm === "parent"
    ? "84d10cff312e5c42dfa9602b3eb83423859db0b0"
    : "35814378581cec75b8da6e62cdb23aa173a863a4";
const runtime = resolve(values.runtime),
  output = resolve(values.output),
  root = process.cwd();
assert(
  runtime.includes("/.cache/") &&
    output.includes("/.cache/") &&
    process.env.TMPDIR?.includes("/.cache/"),
);
assert.equal(
  execFileSync("git", ["status", "--porcelain"], { cwd: runtime, encoding: "utf8" }).trim(),
  "",
  "Runtime must be clean",
);
const revision = execFileSync("git", ["rev-parse", "HEAD"], {
  cwd: runtime,
  encoding: "utf8",
}).trim();
assert.equal(revision, expectedRevision, "Pin the exact parent or verified cache fix");
assert(!existsSync(join(output, "protocol.json")), "Keep prior attempts; use a fresh output");
await mkdir(output, { recursive: true });
const json = (name: string, value: unknown) =>
  writeFile(join(output, name), JSON.stringify(value, null, 2));
async function hashes(directory: string) {
  const entries = await readdir(directory, { recursive: true, withFileTypes: true });
  const result: Record<string, string> = {};
  for (const entry of entries
    .filter((entry) => entry.isFile())
    .sort((a, b) => join(a.parentPath, a.name).localeCompare(join(b.parentPath, b.name)))) {
    const file = join(entry.parentPath, entry.name);
    result[relative(directory, file)] = createHash("sha256")
      .update(await readFile(file))
      .digest("hex");
  }
  return result;
}
const dist = join(runtime, "packages/core/dist/next"),
  workloadRoot = join(root, "scripts/next-executor-workload"),
  source = await hashes(dist),
  workload = await hashes(workloadRoot);
const controller = new AbortController();
for (const signal of ["SIGINT", "SIGTERM"] as const)
  process.once(signal, () => controller.abort(new Error(signal)));
await json("protocol.json", {
  started: new Date().toISOString(),
  arm,
  parentRevision: "84d10cff312e5c42dfa9602b3eb83423859db0b0",
  cacheRevision: "35814378581cec75b8da6e62cdb23aa173a863a4",
  historicalBeforeRevision: "bc9eef50d493ea784766a056a41d8bd1f2704284",
  revision,
  node: process.version,
  rows: 1_000_000,
  columns: 200,
  capBytes: 256 * 2 ** 20,
  queries: 10,
  source,
  workload,
  scope: "Untimed exact function-call diagnostic; no benchmark or S1 acceptance",
  diagnosticFlags: ["--disable-optimizing-compilers"],
  reason:
    "Node26 optimizing compilers omit function counters after tier-up; raw first attempt retained. Each query checks a separate known-count witness.",
  limits: [
    "Node MemoryBackend, not browser/OPFS",
    "W5M not run",
    "fsync, renderer RSS, cold I/O not measured",
  ],
});
let worker: Awaited<ReturnType<typeof startCPUWorker>> | undefined;
try {
  const data: B4WorkerData = {
    runtimeRoot: runtime,
    workloadRoot,
    rows: 1_000_000,
    capBytes: 256 * 2 ** 20,
    repeats: 10,
    imageIdentity: `untimed-directory-counts:${revision}`,
    expectedDirectoryCache: arm === "cache",
  };
  worker = await startCPUWorker(
    {
      module: fileURLToPath(new URL("./b4-perf-worker.mts", import.meta.url)),
      cwd: runtime,
      revision,
      data,
      execArgv: ["--max-old-space-size=3072", "--v8-pool-size=1", "--disable-optimizing-compilers"],
      env: { UV_THREADPOOL_SIZE: "1" },
    },
    {
      arm: arm === "parent" ? "control" : "candidate",
      cohort: 0,
      workload: "scan_selective_30col",
      boundary: "frames",
    },
    undefined,
    controller.signal,
  );
  const result = (await worker.ask({
    op: "command",
    name: "validation-counts",
    data: { workload: "scan_selective_30col", queries: 10 },
  })) as {
    queries: number;
    counts: Array<{
      query: number;
      parseDirectory: number;
      validateSketch: number;
      readRegisters: number;
      verification: B4Verification;
      witness: number;
    }>;
  };
  await json("counts.json", result);
  assert.equal(result.queries, 10);
  assert.equal(result.counts.length, 10);
  const first = result.counts[0];
  assert(first);
  assert.deepEqual(
    first.verification,
    {
      rows: 1000,
      fingerprint: "20eab7d93f1b8a98d53b91174d7baa00d9f266752650fc1cbf952aa2cb9f2745",
      rawFingerprint: "20eab7d93f1b8a98d53b91174d7baa00d9f266752650fc1cbf952aa2cb9f2745",
      exactFingerprint: "34e7a8480b77faad2c58cf3eee7ce085245752824ce49c238c12f86e3b30d4ed",
      columns: [
        { name: "id", type: { kind: "int64" } },
        { name: "ts", type: { kind: "timestamp", zoned: false } },
        { name: "day", type: { kind: "date" } },
        { name: "d00", type: { kind: "text" } },
        { name: "d01", type: { kind: "text" } },
        { name: "d02", type: { kind: "text" } },
        { name: "d03", type: { kind: "text" } },
        { name: "d04", type: { kind: "text" } },
        { name: "d05", type: { kind: "text" } },
        { name: "h00", type: { kind: "text" } },
        { name: "h01", type: { kind: "text" } },
        { name: "h05", type: { kind: "text" } },
        { name: "i00", type: { kind: "int32", bits: 32 } },
        { name: "i01", type: { kind: "int32", bits: 32 } },
        { name: "i02", type: { kind: "int32", bits: 32 } },
        { name: "i03", type: { kind: "int32", bits: 32 } },
        { name: "i04", type: { kind: "int32", bits: 32 } },
        { name: "f00", type: { kind: "float64" } },
        { name: "f01", type: { kind: "float64" } },
        { name: "f02", type: { kind: "float64" } },
        { name: "f03", type: { kind: "float64" } },
        { name: "n00", type: { kind: "decimal", precision: 12, scale: 2 } },
        { name: "n01", type: { kind: "decimal", precision: 12, scale: 2 } },
        { name: "n02", type: { kind: "decimal", precision: 12, scale: 2 } },
        { name: "b00", type: { kind: "bool" } },
        { name: "b01", type: { kind: "bool" } },
        { name: "x00", type: { kind: "int32", bits: 32 } },
        { name: "x01", type: { kind: "float64" } },
        { name: "x02", type: { kind: "text" } },
        { name: "x03", type: { kind: "bool" } },
      ],
    },
    "Complete results/types must match accepted historical BEFORE",
  );
  for (const [index, row] of result.counts.entries()) {
    assert.equal(row.query, index);
    assert.equal(
      row.witness,
      10106,
      "Coverage must report a known positive call count in EVERY query",
    );
    if (arm === "parent")
      assert(
        row.parseDirectory > 0 && row.validateSketch > 0 && row.readRegisters > 0,
        "Fresh uncached parent must keep target counters visible in every query",
      );
    assert.deepEqual(row.verification, first.verification, "Repeated complete results must match");
    for (const value of [row.parseDirectory, row.validateSketch, row.readRegisters])
      assert(Number.isSafeInteger(value) && value >= 0);
    console.log(
      JSON.stringify({
        arm,
        query: index,
        parseDirectory: row.parseDirectory,
        validateSketch: row.validateSketch,
        readRegisters: row.readRegisters,
      }),
    );
  }
  assert(
    first.parseDirectory > 0 && first.validateSketch > 0,
    "Coverage must see cold first validation",
  );
  await worker.close();
  worker = undefined;
  await json("complete.json", {
    revision,
    queries: 10,
    completed: new Date().toISOString(),
    arm,
    performanceClaim: false,
    fullS1Acceptance: false,
  });
} catch (error) {
  await json("failure.json", {
    revision,
    error: error instanceof Error ? error.stack : String(error),
  });
  throw error;
} finally {
  if (worker) await worker.close();
  const finalSource = await hashes(dist),
    finalWorkload = await hashes(workloadRoot);
  await json("final-hashes.json", { finalSource, finalWorkload });
  assert.deepEqual(finalSource, source);
  assert.deepEqual(finalWorkload, workload);
}
