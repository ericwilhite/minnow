/**
 * Measures index run merges (packages/core/src/next/write/merge.ts) on the in-memory store,
 * owner-side, in Node:
 *
 *   npx vite-node scripts/write-merge-bench.mts [--rows 1000000] [--key int|uuid]
 *     [--merges auto|off|manual] [--checkpoints 131072,262144]
 *
 * A keyed load of `rows` rows (key, n INT) in 2,048-row strict autocommit inserts, then a wait
 * until maintenance settles. Reported:
 * - load and settle: wall and process CPU. Merges run in the same thread, interleaved with the
 *   load (`mergeWallMs` spans that interleaving); `--merges manual` loads without them and then
 *   runs them alone, so `isolated.mergeCpuMs` is their CPU;
 * - write amplification: index bytes written by flushes and merges over the index's final bytes,
 *   and all bytes written (groups, runs, directories, roots) over the flushes' bytes;
 * - the primary key's runs per level, the root's bytes and the charged resident metadata;
 * - warm point probes (key → locator through the key index, one key per call, all blocks cached):
 *   µs per probe for present keys and for absent ones (a unique check of a new key), the median of
 *   5 rounds, at the final run count. With `--checkpoints`, also at each of those row counts during
 *   the load (run count vs latency).
 * The machine is shared: compare runs made back to back.
 */
import { performance } from "node:perf_hooks";
import { encodeKeyBatch, KeyBatch } from "../packages/core/src/next/keys/key-encoding.ts";
import type { PhysicalType } from "../packages/core/src/next/types.ts";
import type { FlatVector } from "../packages/core/src/next/vector/vector.ts";
import type { WritePathImpl } from "../packages/core/src/next/write/engine.ts";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  tableInput,
} from "../packages/core/src/next/write/test-support.ts";

const args = process.argv.slice(2);
const option = (name: string, fallback: string): string => {
  const index = args.indexOf(`--${name}`);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};
const ROWS = Number(option("rows", "1000000"));
const KEY: PhysicalType = option("key", "int") === "uuid" ? { kind: "uuid" } : INT;
/** `manual`: the load runs without merges, then they run alone (their CPU, isolated). */
const MODE = option("merges", "auto") as "auto" | "off" | "manual";
const CHECKPOINTS = option("checkpoints", "")
  .split(",")
  .filter((text) => text !== "")
  .map(Number);
const PROBES = 20_000;
const FREE = { allocate: () => true, free: () => undefined };

const cpu = (): number => {
  const usage = process.cpuUsage();
  return (usage.user + usage.system) / 1000;
};
const median = (values: number[]): number => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
};

function keys(ids: readonly number[]): FlatVector {
  if (KEY.kind !== "uuid") return ints([...ids]);
  const bytes = new Uint8Array(ids.length * 16);
  ids.forEach((id, k) => {
    let x = (id + 1) * 0x9e3779b1;
    for (let b = 0; b < 16; b++) {
      x = Math.imul(x ^ (x >>> 15), 0x2c1b3c6d) >>> 0;
      bytes[k * 16 + b] = x & 0xff;
    }
  });
  return {
    form: "flat",
    type: KEY,
    length: ids.length,
    validity: null,
    data: { layout: "fixed16", bytes },
  };
}

/** µs per single-key probe of `ids` (warm: one pass first), median of 5 rounds. */
async function probe(path: WritePathImpl, t: number, ids: readonly number[]): Promise<number> {
  const view = path.pin();
  const index = view.index(t, path.catalog.table(t)?.primaryKey ?? 0);
  const encoded = ids.map((id) => {
    const batchKeys = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: keys([id]), direction: "asc", nulls: "first" }],
      { length: 1, selection: null },
      batchKeys,
    );
    return batchKeys;
  });
  const out = { probes: new Int32Array(1), locators: new Float64Array(1), count: 0 };
  const rounds: number[] = [];
  for (let round = 0; round < 6; round++) {
    const start = performance.now();
    for (const key of encoded) {
      const result = index.probe(key, out);
      if (result instanceof Promise) await result;
    }
    if (round > 0) rounds.push(((performance.now() - start) * 1000) / encoded.length);
  }
  for (const key of encoded) key.close();
  view.release();
  return median(rounds);
}

const sample = (limit: number, offset: number): number[] =>
  Array.from(
    { length: PROBES },
    (_, k) => ((Math.imul(k + 1, 2_654_435_761) >>> 0) % limit) + offset,
  );

const harness = new Harness("strict", "memory", undefined, undefined, { merges: { mode: MODE } });
const path = await harness.open();
let t = 0;
await autocommit(path, async (s) => {
  t =
    (
      await s.changeCatalog({
        kind: "createTable",
        table: tableInput("t", [column("id", KEY, false), column("n", INT)]),
      })
    ).id ?? 0;
});
const levels = () => {
  const runs =
    path
      .state()
      .tables.get(t)
      ?.runs.get(path.catalog.table(t)?.primaryKey ?? 0) ?? [];
  const counts: number[] = [];
  for (const run of runs) counts[run.level] = (counts[run.level] ?? 0) + 1;
  return { runs: runs.length, perLevel: Array.from(counts, (count) => count ?? 0), list: runs };
};
const checkpoints: Array<Record<string, number>> = [];
const loadWall = performance.now();
const loadCpu = cpu();
for (let start = 0; start < ROWS; start += 2048) {
  const n = Math.min(2048, ROWS - start);
  const ids = Array.from({ length: n }, (_, k) => start + k);
  await autocommit(path, (s) =>
    s.table(t).insert(batch(keys(ids), ints(ids.map((id) => id % 1000))), null),
  );
  const done = start + n;
  if (CHECKPOINTS.includes(done)) {
    await path.quiesce();
    checkpoints.push({
      rows: done,
      runs: levels().runs,
      presentProbeUs: Number((await probe(path, t, sample(done, 0))).toFixed(2)),
    });
  }
}
const loadMs = performance.now() - loadWall;
const loadCpuMs = cpu() - loadCpu;
const settleWall = performance.now();
await path.quiesce();
let isolated: { mergeWallMs: number; mergeCpuMs: number } | null = null;
if (MODE === "manual") {
  const wall = performance.now();
  const before = cpu();
  await path.runMaintenance();
  isolated = {
    mergeWallMs: Math.round(performance.now() - wall),
    mergeCpuMs: Math.round(cpu() - before),
  };
}
const settleMs = performance.now() - settleWall;
const totalCpuMs = cpu() - loadCpu;
const m = path.maintenance;
const level = levels();
let indexBytes = 0;
for (const run of level.list) indexBytes += run.entries.extent.length;
const present = await probe(path, t, sample(ROWS, 0));
const absent = await probe(path, t, sample(ROWS, ROWS));
console.log(
  JSON.stringify({
    rows: ROWS,
    key: KEY.kind,
    mode: MODE,
    loadMs: Math.round(loadMs),
    loadCpuMs: Math.round(loadCpuMs),
    settleMs: Math.round(settleMs),
    totalCpuMs: Math.round(totalCpuMs),
    mergeWallMs: Math.round(m.mergeMs),
    merges: m.merges,
    mergesByLevel: m.mergesByLevel,
    indexWriteAmplification: Number(
      ((m.flushRunBytes + m.mergeBytes) / Math.max(1, indexBytes)).toFixed(2),
    ),
    totalWriteAmplification: Number(
      ((m.flushBytes + m.mergeBytes) / Math.max(1, m.flushBytes)).toFixed(3),
    ),
    pkRuns: level.runs,
    pkRunsPerLevel: level.perLevel,
    rootBytes: harness.space.root?.length ?? 0,
    residentMetadataBytes: path.residentMetadataBytes,
    presentProbeUs: Number(present.toFixed(2)),
    absentProbeUs: Number(absent.toFixed(2)),
    isolated,
    mergeEntriesIn: m.mergeEntriesIn,
    checkpoints,
  }),
);
await harness.close();
