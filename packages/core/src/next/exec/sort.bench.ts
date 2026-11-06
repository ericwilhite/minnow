/**
 * Sort microbenchmarks (track B7, increment 2): measurements.md's W1M sort and window shapes
 * through the real operators, inputs prebuilt and untimed. Not a test; bundle and run:
 *
 *     npx esbuild packages/core/src/next/exec/sort.bench.ts --bundle --platform=node \
 *       --format=esm --outfile=$HOME/.cache/sort-bench.mjs && node --expose-gc $HOME/.cache/sort-bench.mjs [shape …]
 *
 * Shapes (DuckDB-Wasm single-thread W1M targets in measurements.md, whole queries):
 * - `numeric`: ORDER BY f09, id returning id, f09, d06 (≤ 252 ms)
 * - `text`: ORDER BY h01, id returning id, h01 (≤ 521 ms)
 * - `topn`: ORDER BY f09 DESC, id LIMIT 100 returning id, f09, h00 (≤ 143 ms)
 * - `topk`: ROW_NUMBER() OVER (PARTITION BY d04 ORDER BY f05 DESC, id) <= 3 (≤ 365 ms)
 * - `spill` / `spillFree`: ORDER BY h02, id LIMIT 10 OFFSET 990,000 over id, d00, h01, h02,
 *   f09 under a 64 MiB cap / unbounded (Gate A: spilled ≤ 3× unbounded).
 */
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import { BATCH_ROWS, type Batch, type FlatVector } from "../vector/vector.js";
import { ArraySource } from "./join-fixtures.js";
import { drive, type Operator, type PushResult } from "./pipeline.js";
import type { SortColumn } from "./sort-keys.js";
import { GroupTopKOperator, SortOperator, sortMinimumBytes, type SortProfile } from "./sort.js";

const N = Number(process.env.ROWS ?? 1_000_000);
const RUNS = Number(process.env.RUNS ?? 5);
const BIG: PhysicalType = { kind: "int64" };
const DBL: PhysicalType = { kind: "float64" };
const TEXT: PhysicalType = { kind: "text" };
const encoder = new TextEncoder();

function f64(values: Float64Array, type: PhysicalType, at: number, n: number): FlatVector {
  const slice = values.slice(at, at + n);
  return type.kind === "int64"
    ? {
        form: "flat",
        type,
        length: n,
        validity: null,
        data: { layout: "int64", values: { repr: "safe", values: slice } },
      }
    : { form: "flat", type, length: n, validity: null, data: { layout: "f64", values: slice } };
}

function text(make: (i: number) => string, at: number, n: number): FlatVector {
  const parts = Array.from({ length: n }, (_, i) => encoder.encode(make(at + i)));
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

const hash = (i: number, salt: number) => (Math.imul(i ^ salt, 2654435761) >>> 0) ^ (i * 40503);
const uuid = (i: number, salt: number) =>
  `${hash(i, salt).toString(16).padStart(8, "0")}-${(hash(i, salt + 1) & 0xffff).toString(16).padStart(4, "0")}-4${i.toString(16).padStart(11, "0")}`;

function batches(columns: ReadonlyArray<(at: number, n: number) => FlatVector>): Batch[] {
  const out: Batch[] = [];
  for (let at = 0; at < N; at += BATCH_ROWS) {
    const n = Math.min(BATCH_ROWS, N - at);
    out.push({ length: n, columns: columns.map((make) => make(at, n)), selection: null });
  }
  return out;
}

class CountSink implements Operator {
  rows = 0;
  push(batch: Batch): PushResult {
    this.rows += batch.selection === null ? batch.length : batch.selection.count;
    return "ready";
  }
  ready(): Promise<void> {
    return Promise.resolve();
  }
  finish(): void {
    this.rows += 0;
  }
  abort(): void {
    this.rows += 0;
  }
}

const ids = Float64Array.from({ length: N }, (_, i) => i);
const f09 = Float64Array.from({ length: N }, (_, i) => (hash(i, 9) % 1_000_000) / 7);
const f05 = Float64Array.from({ length: N }, (_, i) => (hash(i, 5) % 100_000) / 3);
const key = (column: number, direction: "asc" | "desc" = "asc"): SortColumn => ({
  column,
  direction,
  nulls: "last",
  collation: null,
});

interface Shape {
  readonly types: PhysicalType[];
  readonly input: Batch[];
  readonly run: (
    context: { grant: never; spill: MemorySpillService; queryId: number },
    sink: Operator,
  ) => Operator;
  readonly cap?: number;
  readonly expect: number;
}

function shape(name: string): Shape {
  switch (name) {
    case "numeric": {
      const types = [BIG, DBL, TEXT];
      return {
        types,
        input: batches([
          (a, n) => f64(ids, BIG, a, n),
          (a, n) => f64(f09, DBL, a, n),
          (a, n) => text((i) => `d06-${String(hash(i, 6) % 40)}`, a, n),
        ]),
        run: (c, s) =>
          new SortOperator(
            { types, keys: [key(1), key(0)], limit: null, offset: 0, withTies: false },
            c,
            s,
          ),
        expect: N,
      };
    }
    case "text": {
      const types = [BIG, TEXT];
      return {
        types,
        input: batches([(a, n) => f64(ids, BIG, a, n), (a, n) => text((i) => uuid(i, 1), a, n)]),
        run: (c, s) =>
          new SortOperator(
            { types, keys: [key(1), key(0)], limit: null, offset: 0, withTies: false },
            c,
            s,
          ),
        expect: N,
      };
    }
    case "topn": {
      const types = [BIG, DBL, TEXT];
      return {
        types,
        input: batches([
          (a, n) => f64(ids, BIG, a, n),
          (a, n) => f64(f09, DBL, a, n),
          (a, n) => text((i) => uuid(i, 0), a, n),
        ]),
        run: (c, s) =>
          new SortOperator(
            { types, keys: [key(1, "desc"), key(0)], limit: 100, offset: 0, withTies: false },
            c,
            s,
          ),
        expect: 100,
      };
    }
    case "topk": {
      const types = [TEXT, BIG, DBL];
      return {
        types,
        input: batches([
          (a, n) => text((i) => `d04-${String(hash(i, 4) % 100)}`, a, n),
          (a, n) => f64(ids, BIG, a, n),
          (a, n) => f64(f05, DBL, a, n),
        ]),
        run: (c, s) =>
          new GroupTopKOperator(
            { types, partitionBy: [0], orderBy: [key(2, "desc"), key(1)], k: 3 },
            c,
            s,
          ),
        expect: 3 * new Set(Array.from({ length: N }, (_, i) => hash(i, 4) % 100)).size,
      };
    }
    case "spill":
    case "spillFree": {
      const types = [BIG, TEXT, TEXT, TEXT, DBL];
      return {
        types,
        input: batches([
          (a, n) => f64(ids, BIG, a, n),
          (a, n) => text((i) => `d00-${String(hash(i, 0) % 5)}`, a, n),
          (a, n) => text((i) => uuid(i, 11), a, n),
          (a, n) => text((i) => uuid(i, 12), a, n),
          (a, n) => f64(f09, DBL, a, n),
        ]),
        run: (c, s) =>
          new SortOperator(
            {
              types,
              keys: [key(3), key(0)],
              limit: 10,
              offset: Math.floor(N * 0.99),
              withTies: false,
            },
            c,
            s,
          ),
        ...(name === "spill" ? { cap: 64 * 1024 * 1024 } : {}),
        expect: 10,
      };
    }
    default:
      throw new Error(`No shape ${name}`);
  }
}

const stats = (ms: number[]) => {
  const sorted = [...ms].sort((a, b) => a - b);
  return `min ${(sorted[0] ?? 0).toFixed(1)} ms, median ${(sorted[sorted.length >> 1] ?? 0).toFixed(1)} ms`;
};

const names =
  process.argv.slice(2).length > 0
    ? process.argv.slice(2)
    : ["numeric", "text", "topn", "topk", "spillFree", "spill"];
for (const name of names) {
  const s = shape(name);
  const times: number[] = [];
  const cpu: number[] = [];
  let profile: SortProfile | null = null;
  let peak = 0;
  for (let i = 0; i <= RUNS; i++) {
    (globalThis as { gc?: () => void }).gc?.();
    const governor = new Governor(s.cap ?? 4 * 1024 * 1024 * 1024);
    const grant = await governor.admit({
      label: "bench",
      minimumBytes: sortMinimumBytes(s.types),
      spillable: true,
    });
    const sink = new CountSink();
    const usage = process.cpuUsage();
    const start = performance.now();
    const operator = s.run(
      { grant: grant as never, spill: new MemorySpillService(), queryId: 1 },
      sink,
    );
    const execute = () =>
      drive(new ArraySource(s.input), operator, undefined, { yieldIntervalMs: Infinity });
    const profileRegion = (
      globalThis as {
        minnowBenchProfile?: (label: string, run: () => void | Promise<void>) => Promise<void>;
      }
    ).minnowBenchProfile;
    if (i === RUNS && profileRegion !== undefined) await profileRegion(name, execute);
    else await execute();
    const ms = performance.now() - start;
    const used = process.cpuUsage(usage);
    if (sink.rows !== s.expect)
      throw new Error(`${name}: ${String(sink.rows)} rows, want ${String(s.expect)}`);
    if (i > 0) {
      times.push(ms);
      cpu.push((used.user + used.system) / 1000);
    }
    profile = (operator as SortOperator).profile;
    peak = governor.peakBytes;
  }
  if (process.env.BENCH_JSON === "1") console.log(JSON.stringify({ name, cpuMs: cpu }));
  console.log(
    `${name}: ${stats(times)} (CPU ${stats(cpu)}); peak ${(peak / 2 ** 20).toFixed(1)} MiB, runs ${String(profile?.runs ?? 0)}, spilled ${((profile?.spilledBytes ?? 0) / 2 ** 20).toFixed(1)} MiB, mode ${profile?.mode ?? "?"}`,
  );
}
