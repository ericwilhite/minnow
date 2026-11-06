/**
 * Isolated grouped window sort CPU benchmark (prebuilt input, no evaluator or output gather).
 * Bundle with esbuild and run node --expose-gc. Compare revision bundles in >=5 interleaved
 * rounds. GROUPS defaults to 50; use 5, 50, 100, 128, 129 and 1000 to cover both rank paths.
 */
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import { GroupedRows } from "./window-sort.js";
import { Slice, runToEnd } from "./slices.js";

const n = Number(process.env.ROWS ?? 1_000_000);
const groups = Number(process.env.GROUPS ?? 50);
const runs = Number(process.env.RUNS ?? 5);
const type: PhysicalType = { kind: "int32", bits: 32 };
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
const rows = new GroupedRows(
  [type, type],
  [0],
  [{ column: 1, direction: "asc", nulls: "last", collation: null }],
  charge,
);
function vector(values: Int32Array): FlatVector {
  return {
    form: "flat",
    type,
    length: values.length,
    validity: null,
    data: { layout: "i32", values },
  };
}
const partitionValues = new Set<number>();
for (let at = 0; at < n; at += 2048) {
  const length = Math.min(2048, n - at);
  const part = Int32Array.from(
    { length },
    (_, i) => (Math.imul(at + i, 2654435761) >>> 0) % groups,
  );
  for (const value of part) partitionValues.add(value);
  const key = Int32Array.from({ length }, (_, i) => at + i);
  if (!rows.add({ length, columns: [vector(part), vector(key)], selection: null }))
    throw new Error("Input refused");
}
const partitions = [...partitionValues].sort((a, b) => a - b);
for (let r = 0; r <= runs; r++) {
  (globalThis as { gc?: () => void }).gc?.();
  const start = process.cpuUsage();
  const order = runToEnd(rows.sortJob(new Slice(Infinity)));
  const used = process.cpuUsage(start);
  if (order === null) throw new Error("Sort refused");
  let count = 0;
  for (let g = 0; g < rows.groups; g++) {
    let previous = -1;
    for (let j = order.starts[g] ?? 0; j < (order.starts[g + 1] ?? 0); j++) {
      const id = order.ids[j] ?? -1;
      if ((Math.imul(id, 2654435761) >>> 0) % groups !== partitions[g])
        throw new Error("Wrong partition");
      if (id <= previous) throw new Error("Partition lost arrival order");
      previous = id;
      count++;
    }
  }
  if (count !== n) throw new Error("Lost rows");
  rows.releaseSort();
  if (r > 0)
    console.log(JSON.stringify({ groups, rows: n, cpuMs: (used.user + used.system) / 1000 }));
}
rows.close();
if (held !== 0) throw new Error("Leaked charge");
