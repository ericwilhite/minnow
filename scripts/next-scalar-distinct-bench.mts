/** Scalar catalogue COUNT(DISTINCT h01), COUNT(DISTINCT h05), with untimed input/setup. */
import { strict as assert } from "node:assert";
import { cpuUsage } from "node:process";
import { loadavg, homedir } from "node:os";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { join, resolve } from "node:path";
import { Session } from "node:inspector/promises";
import { DistinctSet } from "../packages/core/dist/next/exec/agg/distinct.js";
import { ScalarByteDistinct } from "../packages/core/dist/next/exec/agg/scalar-byte-distinct.js";
import { GroupTable } from "../packages/core/dist/next/exec/agg/groups.js";
import { Governor } from "../packages/core/dist/next/memory/governor.js";
import { xxHash32, xxHash32View } from "../packages/core/dist/next/hash/xxhash32.js";
import type { Vector } from "../packages/core/dist/next/vector/vector.js";
import * as lib from "../packages/core/dist/next/exec/kernels/test-support.js";
import { value } from "./next-executor-workload/gen.mjs";

function option(name: string, fallback: string): string {
  const at = process.argv.indexOf(name);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
}
const rows = Number(option("--rows", "1000000")),
  rounds = Number(option("--rounds", "7")),
  warmups = Number(option("--warmups", "12")),
  iterations = Number(option("--iterations", "1")),
  profile = option("--profile", "0") === "1",
  directory = resolve(option("--output", join(homedir(), ".cache/b4-scalar-distinct/baseline")));
for (const [name, n] of Object.entries({ rows, rounds, warmups, iterations }))
  if (!Number.isSafeInteger(n) || n < (name === "rounds" ? 5 : 1)) throw new RangeError(name);
if (!directory.startsWith(join(homedir(), ".cache/"))) throw new Error("scratch belongs in cache");
await mkdir(directory, { recursive: true });
const input: Array<readonly Vector[]> = [],
  model = [new Set<string>(), new Set<string>()];
for (let first = 0; first < rows; first += 2048) {
  const length = Math.min(2048, rows - first);
  input.push(
    [1, 5].map((k, c) =>
      lib.texts(
        Array.from({ length }, (_, i) => {
          const text = String(value({ fam: "h", k }, first + i, rows));
          model[c]?.add(text);
          return text;
        }),
      ),
    ),
  );
}
const counts = model.map((set) => set.size);
for (const set of model) set.clear();
const peaks: Record<string, number> = {};
const ALL_POLICIES = ["legacy", "intern", "hash", "compact", "hashView"] as const;
type Policy = (typeof ALL_POLICIES)[number];
async function setup(policy: Policy) {
  const governor = new Governor(256 << 20),
    grant = await governor.admit({
      label: `component/scalar-${policy}`,
      minimumBytes: 16 << 20,
      spillable: true,
    });
  const sets =
      policy === "legacy" || policy === "compact"
        ? [0, 1].map(() =>
            policy === "compact"
              ? new ScalarByteDistinct(lib.TEXT, grant)
              : new DistinctSet(lib.TEXT, grant, 2048),
          )
        : [],
    tables = policy === "intern" ? [0, 1].map(() => new GroupTable([lib.TEXT], grant)) : [],
    one: Vector[][] = [[], []],
    ids = new Int32Array(2048),
    out = { rows: new Uint16Array(2048), count: 0 };
  return () => {
    const result = [0, 0];
    try {
      for (const columns of input)
        for (let c = 0; c < 2; c++) {
          const vector = columns[c];
          if (vector === undefined || vector.form !== "flat" || vector.data.layout !== "varlen")
            throw new Error("flat UTF-8 fixture");
          if (policy === "legacy" || policy === "compact") {
            const set = sets[c];
            if (set === undefined) throw new Error("set");
            result[c] =
              (result[c] ?? 0) + set.filter(vector, ids, vector.length, null, true, out).count;
          } else if (policy === "intern") {
            const table = tables[c],
              keys = one[c];
            if (table === undefined || keys === undefined) throw new Error("table");
            keys[0] = vector;
            table.assign(keys, vector.length, null, ids);
          } else {
            const { bytes, offsets } = vector.data;
            const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
            let hash = result[c] ?? 0;
            for (let i = 0; i < vector.length; i++)
              hash =
                (hash +
                  (policy === "hashView"
                    ? xxHash32View(view, 0, offsets[i] ?? 0, offsets[i + 1] ?? 0)
                    : xxHash32(bytes, 0, offsets[i] ?? 0, offsets[i + 1] ?? 0))) >>>
                0;
            result[c] = hash;
          }
        }
      if (policy === "intern") for (let c = 0; c < 2; c++) result[c] = tables[c]?.groupCount ?? 0;
      if (policy !== "hash" && policy !== "hashView") assert.deepEqual(result, counts);
      peaks[policy] = governor.peakBytes;
      return JSON.stringify(result);
    } finally {
      for (const set of sets) set.close();
      for (const table of tables) table.close();
      grant.close();
      assert.equal(governor.usedBytes, 0, "component leaked");
    }
  };
}
const policies = option("--policies", "legacy,compact,hash,hashView")
    .split(",")
    .map((name): Policy => {
      const found = ALL_POLICIES.find((p) => p === name);
      if (found === undefined) throw new Error("Unknown policy " + name);
      return found;
    }),
  samples: Record<string, number[]> = Object.fromEntries(policies.map((p) => [p, []])),
  expected = new Map<string, string>(),
  order: string[] = [],
  loads: number[][] = [];
for (const policy of policies)
  for (let i = 0; i < warmups; i++) {
    const run = await setup(policy);
    expected.set(policy, run());
  }
if (expected.has("hash") && expected.has("hashView"))
  assert.equal(expected.get("hashView"), expected.get("hash"), "hash span checksum changed");
for (let round = 0; round < rounds; round++) {
  loads.push(loadavg());
  for (let i = 0; i < policies.length; i++) {
    const policy = policies[(i + round) % policies.length];
    if (policy === undefined) throw new Error("policy");
    let cpu = 0;
    for (let j = 0; j < iterations; j++) {
      const run = await setup(policy),
        start = cpuUsage(),
        result = run(),
        spent = cpuUsage(start);
      cpu += (spent.user + spent.system) / 1000;
      assert.equal(result, expected.get(policy));
    }
    samples[policy]?.push(cpu / iterations);
    order.push(policy);
  }
}
if (profile)
  for (const policy of policies) {
    const run = await setup(policy),
      session = new Session();
    session.connect();
    await session.post("Profiler.enable");
    await session.post("Profiler.setSamplingInterval", { interval: 100 });
    await session.post("Profiler.start");
    run();
    const { profile: captured } = await session.post("Profiler.stop");
    session.disconnect();
    await writeFile(join(directory, `${policy}.cpuprofile`), JSON.stringify(captured));
  }
const hashes: Record<string, string> = {};
for (const name of [
  "scripts/next-scalar-distinct-bench.mts",
  "scripts/next-executor-workload/gen.mjs",
  "packages/core/dist/next/exec/agg/groups.js",
  "packages/core/dist/next/exec/agg/distinct.js",
  "packages/core/dist/next/exec/agg/scalar-byte-distinct.js",
  "packages/core/dist/next/hash/xxhash32.js",
])
  hashes[name] = createHash("sha256")
    .update(await readFile(name))
    .digest("hex");
const report = {
  lane: "Node",
  timing: "process user+system CPU",
  rows,
  columns: ["h01", "h05"],
  counts,
  rounds,
  warmups,
  iterations,
  order,
  load: loads,
  hashes,
  peaks,
  cpuMs: Object.fromEntries(
    policies.map((p) => {
      const values = samples[p] ?? [],
        sorted = [...values].sort((a, b) => a - b);
      return [
        p,
        {
          samples: values,
          median: sorted[sorted.length >>> 1],
          spread: (sorted.at(-1) ?? 0) - (sorted[0] ?? 0),
        },
      ];
    }),
  ),
  limits:
    "Diagnostic regions overlap: intern-only drops the scalar seen-marker/output selection; hash-only drops equality/storage. They are not additive or production alternatives. Input construction and state setup excluded; filtering, validation, teardown included. No browser or whole-query acceptance.",
};
await writeFile(join(directory, "baseline.json"), JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify(report));
