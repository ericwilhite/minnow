/** Running float SUM / moving float AVG kernels on the Gate B partitions and row order.
 * Input, sorting, extraction, buffers and validation are untimed. Aggregate includes its own
 * prefix/count construction; bounds are measured separately. No inverse float subtraction.
 * ROWS=1000000 RUNS=7 node --expose-gc bundled.mjs [runningSum movingAvg]
 */
import { loadavg } from "node:os";
import { BATCH_ROWS } from "../vector/vector.js";
import { valueAt } from "./join-fixtures.js";
import { Slice, runToEnd } from "./slices.js";
import { windowShape } from "./window-bench-fixtures.js";
import {
  aggregate,
  frameBounds,
  type NumberArg,
  type NumberResult,
  type Peers,
} from "./window-frames.js";
import { GroupedRows } from "./window-sort.js";
const n = Number(process.env.ROWS ?? 1_000_000),
  runs = Number(process.env.RUNS ?? 7);
for (const name of process.argv.slice(2).length
  ? process.argv.slice(2)
  : ["runningSum", "movingAvg"]) {
  if (name !== "runningSum" && name !== "movingAvg") throw new Error("Expected SUM/AVG shape");
  const { types, input, spec } = windowShape(name, n, true, "sql");
  let held = 0;
  const charge = {
    allocate(bytes: number) {
      held += bytes;
      return true;
    },
    free(bytes: number) {
      held -= bytes;
    },
  };
  const source = new GroupedRows(types, spec.partitionBy, spec.orderBy, charge, true);
  for (const batch of input) if (!source.add(batch)) throw new Error("Input refused");
  const order = runToEnd(source.sortJob(new Slice(Infinity)));
  if (order === null) throw new Error("Sort refused");
  const call = spec.functions[0];
  if (call?.arg == null) throw new Error("Missing aggregate argument");
  const partitions = Array.from({ length: source.groups }, (_, p) => {
    const start = order.starts[p] ?? 0,
      size = (order.starts[p + 1] ?? 0) - start;
    const values = Float64Array.from({ length: size }, (_, row) => {
      const id = order.ids[start + row] ?? -1;
      const column = input[Math.floor(id / BATCH_ROWS)]?.columns[call.arg ?? 0];
      if (column === undefined) throw new Error("Missing input");
      return Number(valueAt(column, id % BATCH_ROWS));
    });
    const ids = Int32Array.from({ length: size }, (_, i) => i);
    const peers: Peers = {
      n: size,
      groups: size,
      start: ids,
      end: ids,
      group: ids,
      groupStart: ids,
      groupEnd: ids,
    };
    const arg: NumberArg = { values, valid: new Uint8Array(size).fill(1), big: null };
    const out: NumberResult = {
      values: new Float64Array(size),
      valid: new Uint8Array(size),
      big: null,
    };
    return {
      peers,
      arg,
      out,
      starts: new Int32Array(size),
      ends: new Int32Array(size),
      work: { counts: new Int32Array(size + 1), prefix: new Float64Array(size + 1) },
    };
  });
  const slice = new Slice(Infinity);
  const bounds = () => {
    for (const p of partitions)
      runToEnd(frameBounds(call.frame, true, p.peers, null, p.starts, p.ends, slice));
  };
  const evaluate = () => {
    for (const p of partitions)
      runToEnd(
        aggregate(
          name === "runningSum" ? "sum" : "avg",
          p.arg,
          false,
          p.peers,
          "no-others",
          p.starts,
          p.ends,
          p.out,
          slice,
          p.work,
        ),
      );
  };
  bounds();
  evaluate();
  for (const p of partitions) {
    let prefix = 0;
    for (let i = 0; i < p.peers.n; i++) {
      prefix += p.arg.values[i] ?? 0;
      let expected = prefix;
      if (name === "movingAvg") {
        expected = 0;
        for (let j = Math.max(0, i - 9); j <= i; j++) expected += p.arg.values[j] ?? 0;
        expected /= Math.min(10, i + 1);
      }
      if (p.out.valid[i] !== 1 || p.out.values[i] !== expected)
        throw new Error("Wrong frame result (exact ordered additions required)");
    }
  }
  for (let run = 0; run <= runs; run++) {
    (globalThis as { gc?: () => void }).gc?.();
    const before = process.cpuUsage();
    bounds();
    const boundCpu = process.cpuUsage(before);
    const start = process.cpuUsage();
    evaluate();
    const used = process.cpuUsage(start);
    if (run > 0)
      console.log(
        JSON.stringify({
          name,
          rows: n,
          partitions: partitions.length,
          boundsCpuMs: (boundCpu.user + boundCpu.system) / 1000,
          aggregateCpuMs: (used.user + used.system) / 1000,
          load: loadavg(),
        }),
      );
  }
  source.close();
  if (held !== 0) throw new Error("Leaked source charge");
}
