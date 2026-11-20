/** Complete first encoded selection and reader close over already loaded pages.
 * Uses real production readers from separately built roots. Fixture construction, reader open,
 * page load, predicate compilation, output buffers and verification are untimed. First-use
 * predicate workspace allocation and release are timed. Nine adjacent AB/BA CPU pairs include
 * every process thread and natural GC; no forced GC, I/O, scan or aggregate gain is inferred.
 */
import { strict as assert } from "node:assert";
import { cpuUsage } from "node:process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { tmpdir, loadavg } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import type { ByteBudget } from "../packages/core/dist/next/storage/extents.js";
import type { PhysicalType } from "../packages/core/dist/next/types.js";
import { defined, writeGroup } from "../packages/core/dist/next/page/test-support.js";
import { pairedCPU } from "./lib/paired-cpu.mts";

type ReaderModule = typeof import("../packages/core/dist/next/page/reader.js");
const option = (name: string, fallback: string) => {
  const at = process.argv.indexOf(name);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
const repo = resolve(dirname(fileURLToPath(import.meta.url)), ".."),
  controlRoot = resolve(option("--control-root", repo)),
  candidateRoot = resolve(option("--candidate-root", repo)),
  output = resolve(option("--output", join(tmpdir(), "next-selection-workspace"))),
  rounds = Number(option("--rounds", "9")),
  readers = Number(option("--readers", "128")),
  warmups = Number(option("--warmups", "4"));
for (const [name, count] of Object.entries({ rounds, readers, warmups }))
  if (!Number.isSafeInteger(count) || count < (name === "rounds" ? 9 : 1))
    throw new RangeError(`${name}: positive counts and at least nine rounds required`);
await mkdir(output, { recursive: true });
const load = async (root: string): Promise<ReaderModule> =>
  (await import(
    pathToFileURL(join(root, "packages/core/dist/next/page/reader.js")).href
  )) as ReaderModule;
const control = await load(controlRoot),
  candidate = await load(candidateRoot);
const sha = async (root: string) =>
  createHash("sha256")
    .update(await readFile(join(root, "packages/core/dist/next/page/reader.js")))
    .digest("hex");
const sources = {
    control: { root: controlRoot, sha256: await sha(controlRoot) },
    candidate: { root: candidateRoot, sha256: await sha(candidateRoot) },
  },
  affinity =
    process.platform === "linux"
      ? (/^Cpus_allowed_list:\s*(.*)$/mu.exec(await readFile("/proc/self/status", "utf8"))?.[1] ??
        null)
      : null;
class Budget implements ByteBudget {
  used = 0;
  peak = 0;
  allocate(bytes: number): boolean {
    this.used += bytes;
    this.peak = Math.max(this.peak, this.used);
    return true;
  }
  free(bytes: number): void {
    this.used -= bytes;
  }
}
const records = [];
for (const nullable of [false, true]) {
  const rows = 4096,
    type: PhysicalType = { kind: "int32", bits: 32 },
    values = Array.from({ length: rows }, (_, row) =>
      nullable && row % 17 === 0 ? null : (Math.imul(row, 0x9e37_79b1) >>> 0) & 0x3fff_ffff,
    ),
    expected = values.flatMap((v, row) => (v !== null && v < 500_000_000 ? [row] : [])),
    fixture = await writeGroup([{ type, values }], { gzip: false, pageBytes: 16 * 1024 }),
    chunk = defined(fixture.group.columns[0]);
  async function setup(api: ReaderModule) {
    const batch = [];
    try {
      for (let i = 0; i < readers; i++) {
        const budget = new Budget(),
          reader = await api.ChunkReader.open(fixture.space, chunk, { budget });
        const item = {
          reader,
          budget,
          out: new Uint16Array(rows),
          count: 0,
          before: 0,
          compiled: defined(reader.compilePredicate({ op: "<", value: 500_000_000 }) ?? undefined),
        };
        batch.push(item);
        const [first, end] = reader.pagesInRange(0, rows);
        await reader.load(Array.from({ length: end - first }, (_, k) => first + k));
        item.before = budget.used;
        budget.peak = budget.used;
      }
      return batch;
    } catch (error) {
      for (const item of batch) item.reader.close();
      throw error;
    }
  }
  type Batch = Awaited<ReturnType<typeof setup>>;
  function run(batch: Batch) {
    let at = 0;
    try {
      for (; at < batch.length; at++) {
        const item = defined(batch[at]);
        try {
          item.count = item.reader.select(item.compiled, 0, rows, item.out);
        } finally {
          item.reader.close();
        }
      }
    } finally {
      for (; at < batch.length; at++) defined(batch[at]).reader.close();
    }
  }
  function verify(batch: Batch): number {
    let peak = 0;
    for (const item of batch) {
      assert.deepEqual(Array.from(item.out.subarray(0, item.count)), expected);
      assert.equal(item.budget.used, 0, "reader retained a charge");
      peak = Math.max(peak, item.budget.peak - item.before);
    }
    return peak;
  }
  for (let k = 0; k < warmups; k++)
    for (const api of [control, candidate]) {
      const batch = await setup(api);
      run(batch);
      verify(batch);
    }
  const samples: Array<{
    round: number;
    arm: "control" | "candidate";
    cpuMs: number;
    peakNewBytesPerReader: number;
    load: number[];
  }> = [];
  for (let round = 0; round < rounds; round++) {
    const order =
      round % 2 === 0 ? (["control", "candidate"] as const) : (["candidate", "control"] as const);
    for (const arm of order) {
      const batch = await setup(arm === "control" ? control : candidate),
        start = cpuUsage();
      run(batch);
      const used = cpuUsage(start),
        cpuMs = (used.user + used.system) / 1000,
        peakNewBytesPerReader = verify(batch);
      samples.push({ round, arm, cpuMs, peakNewBytesPerReader, load: loadavg() });
    }
  }
  const result = pairedCPU(
    samples.filter((s) => s.arm === "control").map((s) => s.cpuMs),
    samples.filter((s) => s.arm === "candidate").map((s) => s.cpuMs),
  );
  records.push({
    fixture: nullable ? "nullable plain int32" : "all-valid plain int32",
    rowsPerSample: rows * readers,
    result,
    samples,
    medianSavingBeatsFullRange: -result.medianDeltaMs > result.rangeDeltaMs,
  });
  await writeFile(
    join(output, "result.json"),
    JSON.stringify(
      {
        node: process.version,
        controlRoot,
        candidateRoot,
        rounds,
        readers,
        warmups,
        affinity,
        sources,
        records,
      },
      null,
      2,
    ),
  );
  console.log(
    JSON.stringify({
      nullable,
      ...result,
      pairs: undefined,
      medianSavingBeatsFullRange: -result.medianDeltaMs > result.rangeDeltaMs,
    }),
  );
}
assert.equal(await sha(controlRoot), sources.control.sha256, "control changed during comparison");
assert.equal(
  await sha(candidateRoot),
  sources.candidate.sha256,
  "candidate changed during comparison",
);
await writeFile(
  join(output, "sources.json"),
  JSON.stringify(
    {
      ...sources,
      affinity,
      boundary: "first encoded selection through reader close; setup/load/verification excluded",
    },
    null,
    2,
  ),
);
