/**
 * Built run-encoding component A/B benchmark (CPU across all Node threads, plus wall).
 * Capture the parent core dist under os.tmpdir(), then build the candidate. Examples:
 * node --expose-gc scripts/write-run-bench.mts --baseline "$TMPDIR/old/dist" --mode delta --gc-before
 * Modes: delta (sorted delta -> run), bloom (hash pairs -> stored
 * filter), encoder (ordered entries -> run). --keys 65536 --iterations 32 --rounds 6 defaults.
 * --hot-key sends every row to one non-unique key; use --iterations 1 for the quadratic parent.
 * Inputs and optional setup GC are outside the clock. One discarded warmup pair precedes >=5
 * measured AB/BA pairs. Exact bytes/metadata are compared before timing; checksums keep results
 * live during timing. No storage, log, SQL, query or durability cost is claimed here. Use the
 * separate write-insert-bench --mode load for whole-load regression checks.
 */
import { deepStrictEqual } from "node:assert";
import { loadavg } from "node:os";
import { resolve, join } from "node:path";
import { pathToFileURL } from "node:url";
import { performance } from "node:perf_hooks";

type Runs = typeof import("../packages/core/dist/next/write/runs.js");
type Deltas = typeof import("../packages/core/dist/next/write/key-hash.js");
const args = process.argv.slice(2);
const option = (name: string, fallback: string) => {
  const at = args.indexOf(`--${name}`);
  return at < 0 ? fallback : (args[at + 1] ?? fallback);
};
const integer = (name: string, fallback: number, minimum = 1) => {
  const n = Number(option(name, String(fallback)));
  if (!Number.isSafeInteger(n) || n < minimum) throw new Error(`Invalid --${name}`);
  return n;
};
const baseline = option("baseline", "");
if (!baseline) throw new Error("--baseline must name the parent's built core dist");
const mode = option("mode", "delta");
if (!["delta", "bloom", "encoder"].includes(mode)) throw new Error("Invalid --mode");
const hotKey = args.includes("--hot-key");
const keys = integer("keys", 65_536);
const iterations = integer("iterations", 32);
const rounds = integer("rounds", 6, 5);
const keyBytes = integer("key-bytes", 8, 4);
const gcBefore = args.includes("--gc-before");
if (gcBefore && !global.gc) throw new Error("--gc-before requires node --expose-gc");
const directories = {
  baseline: resolve(baseline),
  candidate: resolve(option("candidate", "packages/core/dist")),
};
type Label = keyof typeof directories;
interface Build {
  runs: Runs;
  delta: InstanceType<Deltas["KeyDelta"]>;
}
const builds = {} as Record<Label, Build>;
// Distinct ascending keys, with nonzero source offset. These approximate short INT index keys;
// --key-bytes exercises longer compound/text keys without changing the timed operations.
const input = Array.from({ length: keys }, (_, k) => {
  const key = new Uint8Array(keyBytes + 6).fill(0x61);
  new DataView(key.buffer).setUint32(3, hotKey ? 0 : k, false);
  return key;
});
const hashes = new Uint32Array(keys * 2);
for (const label of ["baseline", "candidate"] as const) {
  const url = (file: string) => pathToFileURL(join(directories[label], file)).href;
  const runs = (await import(url("next/write/runs.js"))) as Runs;
  const { KeyDelta } = (await import(url("next/write/key-hash.js"))) as Deltas;
  const { xxHash32 } = (await import(
    url("next/hash/xxhash32.js")
  )) as typeof import("../packages/core/dist/next/hash/xxhash32.js");
  const delta = new KeyDelta();
  for (let k = 0; k < keys; k++) {
    const key = input[k];
    if (!key) throw new Error("Missing input");
    delta.add(key, 3, 3 + keyBytes, k + 1, 1, false);
    if (label === "baseline") {
      hashes[k * 2] = xxHash32(key, 0, 3, 3 + keyBytes);
      hashes[k * 2 + 1] = xxHash32(key, 0x9747b28c, 3, 3 + keyBytes) | 1;
    }
  }
  builds[label] = { runs, delta };
}
function encode(build: Build) {
  if (mode === "bloom") return build.runs.encodeBloom(hashes, hashes.length);
  const builder = new build.runs.RunBuilder();
  if (mode === "delta") {
    const pending = build.delta.forEachSorted(1, (arena, start, end, rowId, tombstone) => {
      builder.addRange(arena, start, end, rowId, tombstone);
    });
    if (pending instanceof Promise) return pending.then(() => builder.finish());
  } else
    for (let k = 0; k < input.length; k++) {
      const key = input[k];
      if (!key) throw new Error("Missing input");
      builder.addRange(key, 3, 3 + keyBytes, k + 1, false);
    }
  return builder.finish();
}
deepStrictEqual(await encode(builds.candidate), await encode(builds.baseline));
interface Result {
  cpuMs: number;
  wallMs: number;
  checksum: number;
}
const results: Record<Label, Result[]> = { baseline: [], candidate: [] };
for (let round = -1; round < rounds; round++) {
  const order: Label[] = round % 2 === 0 ? ["baseline", "candidate"] : ["candidate", "baseline"];
  for (const label of order) {
    if (gcBefore) global.gc?.();
    const loadAverageBefore = loadavg();
    let checksum = 0;
    const wall = performance.now();
    const cpu = process.cpuUsage();
    for (let k = 0; k < iterations; k++) {
      const pending = encode(builds[label]);
      const run = pending instanceof Promise ? await pending : pending;
      if (run instanceof Uint8Array) checksum += run.length + (run[5] ?? 0);
      else if (run) checksum += run.entryCount + run.parts.reduce((n, part) => n + part.length, 0);
    }
    const used = process.cpuUsage(cpu);
    const result = {
      cpuMs: (used.user + used.system) / 1000,
      wallMs: performance.now() - wall,
      checksum,
    };
    console.log(
      JSON.stringify({
        mode,
        label,
        round,
        warmup: round < 0,
        loadAverageBefore,
        loadAverageAfter: loadavg(),
        ...result,
      }),
    );
    if (round >= 0) results[label].push(result);
    await new Promise<void>((done) => setImmediate(done));
  }
}
const median = (xs: number[]) => {
  const sorted = [...xs].sort((a, b) => a - b);
  return (
    ((sorted[Math.floor((sorted.length - 1) / 2)] ?? 0) +
      (sorted[Math.floor(sorted.length / 2)] ?? 0)) /
    2
  );
};
const summary = (xs: Result[]) => ({
  cpuMedianMs: median(xs.map((x) => x.cpuMs)),
  cpuMinMs: Math.min(...xs.map((x) => x.cpuMs)),
  cpuMaxMs: Math.max(...xs.map((x) => x.cpuMs)),
  wallMedianMs: median(xs.map((x) => x.wallMs)),
});
const before = summary(results.baseline),
  after = summary(results.candidate);
console.log(
  JSON.stringify({
    summary: true,
    node: process.version,
    mode,
    keys,
    keyBytes,
    hotKey,
    iterations,
    rounds,
    gcBefore,
    directories,
    baseline: before,
    candidate: after,
    beatsSpread: after.cpuMedianMs < before.cpuMinMs && before.cpuMedianMs > after.cpuMaxMs,
  }),
);
