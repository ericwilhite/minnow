/**
 * Hash-join microbenchmarks (track B7). Not a test; run with
 *
 *     npx vite-node packages/core/src/next/exec/join.bench.ts [shape …]
 *
 * (or bundle it with esbuild and run `node --expose-gc`, which GCs before every timed run).
 *
 * Each shape runs the real operator through real pipelines (`drive`), input batches prebuilt and
 * untimed, output consumed by a summing sink that reads vectors directly. Reports min / median
 * of the measured runs and the governor's peak. Shapes:
 * - `design`: the design report's kernel shape (design-joins-agg-spill.md "Microbenchmark
 *   evidence"): 1M unique sparse int32 build keys, 1M probes that all hit, sum of build row ids.
 *   Its reference numbers are 530.7 ms (old ByteJoinIndex) → 58.5 ms (typed radix16), on a
 *   different machine: `reference` re-runs that radix16 kernel here for a fair ratio.
 * - `factInt`, `semi`, `anti`: measurements.md's W1M join shapes on dense BIGINT keys.
 * - `text`: 1M × 1M UUID-text keys (join_fact_fact_text); `spill`: the same under a 64 MiB cap.
 */
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import { BATCH_ROWS, type Batch, type FlatVector, type Vector } from "../vector/vector.js";
import { ArraySource } from "./join-fixtures.js";
import { HashJoin, hashJoinMinimumBytes, type HashJoinKind, type HashJoinSpec } from "./join.js";
import { drive, type Operator, type PushResult } from "./pipeline.js";

const INT: PhysicalType = { kind: "int32", bits: 32 };
const BIG: PhysicalType = { kind: "int64" };
const DBL: PhysicalType = { kind: "float64" };
const TEXT: PhysicalType = { kind: "text" };
const N = Number(process.env.ROWS ?? 1_000_000);
const RUNS = Number(process.env.RUNS ?? 7);

function flat(
  type: PhysicalType,
  values: Float64Array | Int32Array,
  at: number,
  n: number,
): FlatVector {
  if (type.kind === "int32")
    return {
      form: "flat",
      type,
      length: n,
      validity: null,
      data: { layout: "i32", values: (values as Int32Array).slice(at, at + n) },
    };
  if (type.kind === "int64")
    return {
      form: "flat",
      type,
      length: n,
      validity: null,
      data: {
        layout: "int64",
        values: { repr: "safe", values: Float64Array.from(values.subarray(at, at + n)) },
      },
    };
  return {
    form: "flat",
    type,
    length: n,
    validity: null,
    data: { layout: "f64", values: Float64Array.from(values.subarray(at, at + n)) },
  };
}

function textVector(strings: readonly string[], at: number, n: number): FlatVector {
  const encoder = new TextEncoder();
  const parts = strings.slice(at, at + n).map((s) => encoder.encode(s));
  const offsets = new Int32Array(n + 1);
  let total = 0;
  parts.forEach((p, i) => {
    total += p.length;
    offsets[i + 1] = total;
  });
  const bytes = new Uint8Array(total);
  parts.forEach((p, i) => bytes.set(p, offsets[i] ?? 0));
  return {
    form: "flat",
    type: TEXT,
    length: n,
    validity: null,
    data: { layout: "varlen", bytes, offsets },
  };
}

function batches(
  rows: number,
  column: (c: number, at: number, n: number) => Vector,
  columns: number,
): Batch[] {
  const out: Batch[] = [];
  for (let at = 0; at < rows; at += BATCH_ROWS) {
    const n = Math.min(BATCH_ROWS, rows - at);
    out.push({
      length: n,
      columns: Array.from({ length: columns }, (_, c) => column(c, at, n)),
      selection: null,
    });
  }
  return out;
}

/** Sums one numeric output column (and counts rows), reading vectors directly. */
class SumSink implements Operator {
  finished = false;
  count = 0;
  sum = 0;
  constructor(readonly column: number) {}
  push(batch: Batch): PushResult {
    const count = batch.selection === null ? batch.length : batch.selection.count;
    this.count += count;
    if (this.column < 0) return "ready";
    const vector = batch.columns[this.column];
    if (vector?.form !== "flat") return "ready";
    const data = vector.data;
    const values =
      data.layout === "i32" || data.layout === "f64"
        ? data.values
        : data.layout === "int64" && data.values.repr === "safe"
          ? data.values.values
          : null;
    if (values === null) return "ready";
    const rows = batch.selection?.rows;
    let sum = 0;
    if (rows === undefined) for (let i = 0; i < count; i++) sum += values[i] ?? 0;
    else for (let i = 0; i < count; i++) sum += values[rows[i] ?? 0] ?? 0;
    this.sum += sum;
    return "ready";
  }
  ready(): Promise<void> {
    return Promise.resolve();
  }
  finish(): void {
    this.finished = true;
  }
  abort(): void {
    this.finished = true;
  }
}

interface Shape {
  readonly name: string;
  readonly kind: HashJoinKind;
  readonly buildTypes: readonly PhysicalType[];
  readonly probeTypes: readonly PhysicalType[];
  readonly build: () => Batch[];
  readonly probe: () => Batch[];
  readonly sumColumn: number;
  readonly cap?: number;
  readonly check: (sink: SumSink) => void;
}

function expectEqual(name: string, got: number, want: number): void {
  if (got !== want) throw new Error(`${name}: got ${String(got)}, want ${String(want)}`);
}

const shapes: Record<string, () => Shape> = {
  design: () => {
    const build = Int32Array.from({ length: N }, (_, i) => i * 17 + 1);
    const ids = Int32Array.from({ length: N }, (_, i) => i);
    const probe = Int32Array.from({ length: N }, (_, i) => ((i * 7919) % N) * 17 + 1);
    return {
      name: "design 1M×1M sparse int32",
      kind: "inner",
      buildTypes: [INT, INT],
      probeTypes: [INT],
      build: () => batches(N, (c, at, n) => flat(INT, c === 0 ? build : ids, at, n), 2),
      probe: () => batches(N, (_, at, n) => flat(INT, probe, at, n), 1),
      sumColumn: 2,
      check: (sink) => {
        expectEqual("design", sink.sum, (N * (N - 1)) / 2);
      },
    };
  },
  factInt: () => {
    // join_fact_fact_int: b.id dense BIGINT PK, a.i20 FK; 1M probes after `a.i05 < 5` (all here).
    const ids = Float64Array.from({ length: N }, (_, i) => i);
    const f01 = Float64Array.from({ length: N }, (_, i) => (i % 1000) / 8);
    const fk = Float64Array.from({ length: N }, (_, i) => (i * 7919) % N);
    return {
      name: "fact×fact dense BIGINT 1M×1M",
      kind: "inner",
      buildTypes: [BIG, DBL],
      probeTypes: [BIG],
      build: () =>
        batches(N, (c, at, n) => flat(c === 0 ? BIG : DBL, c === 0 ? ids : f01, at, n), 2),
      probe: () => batches(N, (_, at, n) => flat(BIG, fk, at, n), 1),
      sumColumn: 2,
      check: (sink) => {
        expectEqual("factInt rows", sink.count, N);
      },
    };
  },
  semi: () => semiAnti("semi"),
  anti: () => semiAnti("anti"),
  text: () => textShape(undefined),
  spill: () => textShape(64 * 1024 * 1024),
};

function semiAnti(kind: "semi" | "anti"): Shape {
  // join_semi_exists / join_anti_not_exists: build = b filtered (half the ids), probe = a.i20.
  const half = N >> 1;
  const ids = Float64Array.from({ length: half }, (_, i) => i * 2);
  const fk = Float64Array.from({ length: N }, (_, i) => (i * 7919) % N);
  return {
    name: `${kind} dense BIGINT 500k build × 1M`,
    kind,
    buildTypes: [BIG],
    probeTypes: [BIG],
    build: () => batches(half, (_, at, n) => flat(BIG, ids, at, n), 1),
    probe: () => batches(N, (_, at, n) => flat(BIG, fk, at, n), 1),
    sumColumn: -1,
    check: (sink) => {
      expectEqual(kind, sink.count, N / 2);
    },
  };
}

function uuid(i: number): string {
  const h = (i * 2654435761) >>> 0;
  const g = (i * 40503 + 12345) >>> 0;
  return `${h.toString(16).padStart(8, "0")}-${(g & 0xffff).toString(16).padStart(4, "0")}-4${(i & 0xfff).toString(16).padStart(3, "0")}-a${((g >>> 16) & 0xfff).toString(16).padStart(3, "0")}-${i.toString(16).padStart(12, "0")}`;
}

function textShape(cap: number | undefined): Shape {
  // join_fact_fact_text: b.h00 = a.h07 (UUID text), SUM(b.f02).
  // Batches are built here and the JS strings dropped, so they do not inflate the timed heap.
  let keys: string[] | null = Array.from({ length: N }, (_, i) => uuid(i));
  const f02 = Float64Array.from({ length: N }, (_, i) => i % 97);
  const buildBatches = batches(
    N,
    (c, at, n) => (c === 0 ? textVector(keys ?? [], at, n) : flat(DBL, f02, at, n)),
    2,
  );
  keys = Array.from({ length: N }, (_, i) => uuid((i * 7919) % N));
  const probeBatches = batches(N, (_, at, n) => textVector(keys ?? [], at, n), 1);
  keys = null;
  return {
    name: `text UUID 1M×1M${cap === undefined ? "" : " under 64 MiB"}`,
    kind: "inner",
    buildTypes: [TEXT, DBL],
    probeTypes: [TEXT],
    build: () => buildBatches,
    probe: () => probeBatches,
    sumColumn: 2,
    ...(cap === undefined ? {} : { cap }),
    check: (sink) => {
      expectEqual("text rows", sink.count, N);
    },
  };
}

async function runOnce(
  shape: Shape,
  build: Batch[],
  probe: Batch[],
): Promise<{ ms: number; peak: number; spilled: number; table: string | null }> {
  const spec: HashJoinSpec = {
    kind: shape.kind,
    probeTypes: shape.probeTypes,
    buildTypes: shape.buildTypes,
    probeKeys: [0],
    buildKeys: [0],
    nullSafe: [false],
    residual: null,
    publishes: [],
  };
  const governor = new Governor(shape.cap ?? 4 * 1024 * 1024 * 1024);
  const grant = await governor.admit({
    label: "bench",
    minimumBytes: hashJoinMinimumBytes(spec),
    spillable: true,
  });
  const sink = new SumSink(shape.sumColumn);
  const cpu = process.cpuUsage();
  const start = performance.now();
  const join = new HashJoin(spec, { grant, spill: new MemorySpillService(), queryId: 1 });
  await drive(new ArraySource(build), join.build, undefined, { yieldIntervalMs: Infinity });
  const built = performance.now();
  await drive(new ArraySource(probe), join.probe(sink), undefined, { yieldIntervalMs: Infinity });
  const ms = performance.now() - start;
  const used = process.cpuUsage(cpu);
  cpuMs.push((used.user + used.system) / 1000);
  buildMs.push(built - start);
  shape.check(sink);
  return {
    ms,
    peak: governor.peakBytes,
    spilled: join.profile.spilledBytes,
    table: join.profile.table,
  };
}

/** The design report's typed radix16 kernel, re-run here (its code, unchanged in substance). */
function reference(): number {
  const hash = (x: number) => {
    x = Math.imul(x ^ (x >>> 16), 0x7feb352d);
    x = Math.imul(x ^ (x >>> 15), 0x846ca68b);
    return (x ^ (x >>> 16)) >>> 0;
  };
  const B = N;
  const build = Int32Array.from({ length: B }, (_, i) => i * 17 + 1);
  const probe = Int32Array.from({ length: N }, (_, i) => ((i * 7919) % B) * 17 + 1);
  const table = (keys: Int32Array, ids: Int32Array) => {
    let cap = 1;
    while (cap < keys.length / 0.7) cap *= 2;
    const ks = new Int32Array(cap);
    const rs = new Int32Array(cap).fill(-1);
    for (let i = 0; i < keys.length; i++) {
      let s = (hash(keys[i] ?? 0) >>> 4) & (cap - 1);
      while ((rs[s] ?? 0) >= 0) s = (s + 1) & (cap - 1);
      ks[s] = keys[i] ?? 0;
      rs[s] = ids[i] ?? 0;
    }
    return { ks, rs, cap };
  };
  const partition = (a: Int32Array) => {
    const count = new Uint32Array(16);
    for (const k of a) count[hash(k) & 15] = (count[hash(k) & 15] ?? 0) + 1;
    const off = new Uint32Array(17);
    for (let p = 0; p < 16; p++) off[p + 1] = (off[p] ?? 0) + (count[p] ?? 0);
    const next = off.slice();
    const keys = new Int32Array(a.length);
    const ids = new Int32Array(a.length);
    for (let i = 0; i < a.length; i++) {
      const p = hash(a[i] ?? 0) & 15;
      const j = next[p] ?? 0;
      next[p] = j + 1;
      keys[j] = a[i] ?? 0;
      ids[j] = i;
    }
    return { off, keys, ids };
  };
  const start = performance.now();
  const b = partition(build);
  const p = partition(probe);
  let sum = 0;
  for (let i = 0; i < 16; i++) {
    const t = table(
      b.keys.subarray(b.off[i], b.off[i + 1]),
      b.ids.subarray(b.off[i], b.off[i + 1]),
    );
    for (let j = p.off[i] ?? 0; j < (p.off[i + 1] ?? 0); j++) {
      const k = p.keys[j] ?? 0;
      let s = (hash(k) >>> 4) & (t.cap - 1);
      let r = -1;
      while ((t.rs[s] ?? -1) >= 0) {
        if (t.ks[s] === k) {
          r = t.rs[s] ?? -1;
          break;
        }
        s = (s + 1) & (t.cap - 1);
      }
      sum += r;
    }
  }
  const ms = performance.now() - start;
  expectEqual("reference", sum, (N * (N - 1)) / 2);
  return ms;
}

const buildMs: number[] = [];
const cpuMs: number[] = [];
const stats = (ms: number[]) => {
  const sorted = [...ms].sort((a, b) => a - b);
  return `min ${(sorted[0] ?? 0).toFixed(1)} ms, median ${(sorted[sorted.length >> 1] ?? 0).toFixed(1)} ms`;
};

const wanted = process.argv.slice(2);
const names = wanted.length > 0 ? wanted : ["reference", ...Object.keys(shapes)];
for (const name of names) {
  if (name === "reference") {
    const times: number[] = [];
    for (let i = 0; i <= RUNS; i++) {
      const ms = reference();
      if (i > 0) times.push(ms);
    }
    console.log(`reference (design radix16 kernel): ${stats(times)}`);
    continue;
  }
  const make = shapes[name];
  if (make === undefined) throw new Error(`No shape ${name}`);
  const shape = make();
  const times: number[] = [];
  buildMs.length = 0;
  cpuMs.length = 0;
  let last = { peak: 0, spilled: 0, table: null as string | null };
  const build = shape.build();
  const probe = shape.probe();
  for (let i = 0; i <= RUNS; i++) {
    (globalThis as { gc?: () => void }).gc?.();
    const result = await runOnce(shape, build, probe);
    if (i > 0) times.push(result.ms);
    last = result;
  }
  console.log(
    `${shape.name}: ${stats(times)} (build ${stats(buildMs.slice(1))}; CPU ${stats(cpuMs.slice(1))}); table ${String(last.table)}, peak ${(last.peak / 2 ** 20).toFixed(1)} MiB, spilled ${(last.spilled / 2 ** 20).toFixed(1)} MiB`,
  );
}
