/**
 * Window microbenchmarks (track B7, increment 3): measurements.md's five W1M window shapes
 * through the real operator, inputs prebuilt (the harness generator's values) and untimed. Not a
 * test; bundle and run:
 *
 *     npx esbuild packages/core/src/next/exec/window.bench.ts --bundle --platform=node \
 *       --format=esm --outfile=$HOME/.cache/window-bench.mjs && node --expose-gc $HOME/.cache/window-bench.mjs [shape …]
 *
 * Shapes (DuckDB-Wasm single-thread W1M medians in measurements.md, whole queries):
 * - `topk`: ROW_NUMBER() OVER (PARTITION BY d04 ORDER BY f05 DESC, id) (365 ms; `topkGroup`: the
 *   same through GroupTopKOperator, k = 3)
 * - `rank`: RANK() OVER (PARTITION BY d07 ORDER BY i06 DESC) (282 ms)
 * - `runningSum`: SUM(f06) OVER (PARTITION BY d00 ORDER BY id ROWS UNBOUNDED PRECEDING) (344 ms)
 * - `lagLead`: LAG(i08), LEAD(i08) OVER (PARTITION BY d05 ORDER BY id) (273 ms)
 * - `movingAvg`: AVG(f07) OVER (PARTITION BY d03 ORDER BY id ROWS 9 PRECEDING) (320 ms)
 * SORT=0 skips the optional standalone SortOperator diagnostic. Isolate the window's own
 * ordering with window-order.bench.ts; the two sort implementations have different costs.
 * STREAM=1 forces every partition to stream. RUNS=7 BENCH_JSON=1 records CPU samples and load.
 */
import { loadavg } from "node:os";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { type Batch } from "../vector/vector.js";
import { ArraySource } from "./join-fixtures.js";
import { drive, type Operator, type PushResult } from "./pipeline.js";
import { GroupTopKOperator, SortOperator, sortMinimumBytes } from "./sort.js";
import { windowShape, windowKey } from "./window-bench-fixtures.js";
import { WindowOperator, windowMinimumBytes, type WindowSpec } from "./window.js";

const N = Number(process.env.ROWS ?? 1_000_000);
const RUNS = Number(process.env.RUNS ?? 5);
const LAYOUT = process.env.LAYOUT ?? "sql";
const STREAM = process.env.STREAM === "1";
/** Text dimensions as dictionary vectors (what a scan of dictionary pages yields); DICT=0: flat. */
const DICT = process.env.DICT !== "0";
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

const stats = (ms: number[]) => {
  const sorted = [...ms].sort((a, b) => a - b);
  return `min ${(sorted[0] ?? 0).toFixed(0)}, median ${(sorted[sorted.length >> 1] ?? 0).toFixed(0)} ms`;
};

async function time(
  make: (grant: never, sink: Operator) => Operator,
  minimum: number,
  label: string,
) {
  const wall: number[] = [];
  const cpu: number[] = [];
  const loads: number[][] = [];
  let out = 0;
  for (let r = 0; r <= RUNS; r++) {
    (globalThis as { gc?: () => void }).gc?.();
    const governor = new Governor(4 * 1024 * 1024 * 1024);
    const grant = await governor.admit({ label: "bench", minimumBytes: minimum, spillable: true });
    const sink = new CountSink();
    const usage = process.cpuUsage();
    const start = performance.now();
    const op = make(grant as never, sink);
    const execute = () =>
      drive(new ArraySource(input), op, undefined, { yieldIntervalMs: Infinity });
    const profileRegion = (
      globalThis as {
        minnowBenchProfile?: (label: string, run: () => void | Promise<void>) => Promise<void>;
      }
    ).minnowBenchProfile;
    if (r === RUNS && profileRegion !== undefined) await profileRegion(label, execute);
    else await execute();
    const profile = (op as { profile?: { groupedRows?: number; groupedFallbacks?: number } })
      .profile;
    if (r === 0 && profile?.groupedRows !== undefined)
      process.stderr.write(
        `  grouped ${String(profile.groupedRows)} rows, ${String(profile.groupedFallbacks)} fallbacks\n`,
      );
    const ms = performance.now() - start;
    const used = process.cpuUsage(usage);
    out = sink.rows;
    if (r > 0) {
      wall.push(ms);
      cpu.push((used.user + used.system) / 1000);
      loads.push(loadavg());
    }
  }
  return { wall: stats(wall), cpu: stats(cpu), out, cpuMs: cpu, loads };
}

let input: Batch[] = [];
const names =
  process.argv.slice(2).length > 0
    ? process.argv.slice(2)
    : ["topk", "topkGroup", "rank", "runningSum", "lagLead", "movingAvg"];
for (const name of names) {
  const s = windowShape(name, N, DICT, LAYOUT);
  input = s.input;
  const spec: WindowSpec = STREAM ? { ...s.spec, streamRows: 1 } : s.spec;
  const context = (grant: never) => ({ grant, spill: new MemorySpillService(), queryId: 1 });
  const keys = [...spec.partitionBy.map((c) => windowKey(c)), ...spec.orderBy];
  const run =
    name === "topkGroup"
      ? await time(
          (grant, sink) =>
            new GroupTopKOperator(
              { types: s.types, partitionBy: spec.partitionBy, orderBy: spec.orderBy, k: 3 },
              context(grant),
              sink,
            ),
          sortMinimumBytes(s.types),
          name,
        )
      : await time(
          (grant, sink) => new WindowOperator(spec, context(grant), sink),
          windowMinimumBytes(s.types, spec.functions.length),
          name,
        );
  const sort =
    process.env.SORT === "0"
      ? { wall: "-", cpu: "-" }
      : await time(
          (grant, sink) =>
            new SortOperator(
              { types: s.types, keys, limit: null, offset: 0, withTies: false },
              context(grant),
              sink,
            ),
          sortMinimumBytes(s.types),
          `sort-${name}`,
        );
  if (process.env.BENCH_JSON === "1")
    console.log(
      JSON.stringify({ name, layout: LAYOUT, cpuMs: run.cpuMs, load: run.loads, rows: run.out }),
    );
  console.log(
    `${name}: ${run.wall} (CPU ${run.cpu}); sort alone ${sort.wall} (CPU ${sort.cpu}); ${String(run.out)} rows`,
  );
}
