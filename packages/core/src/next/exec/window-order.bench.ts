/**
 * Gate B ordering only: the same prebuilt input as window.bench, including normalized words
 * and row-store arrival, prepared before timing. No evaluator/output gather. Allocation and
 * peer construction are part of ordering. Verify the full permutation, stable ties, peer flags
 * and partition membership outside timing. Run >=7 interleaved revision pairs; record load.
 * ROWS=1000000 RUNS=1 node --expose-gc bundle.mjs [topk rank runningSum lagLead movingAvg]
 */
import { loadavg } from "node:os";
import { BATCH_ROWS } from "../vector/vector.js";
import { valueAt } from "./join-fixtures.js";
import { Slice, runToEnd } from "./slices.js";
import { GroupedRows } from "./window-sort.js";
import { windowShape } from "./window-bench-fixtures.js";
const n = Number(process.env.ROWS ?? 1_000_000),
  runs = Number(process.env.RUNS ?? 7);
for (const name of process.argv.slice(2).length
  ? process.argv.slice(2)
  : ["topk", "rank", "runningSum", "lagLead", "movingAvg"]) {
  const layout = process.env.LAYOUT ?? "sql";
  const { types, input, spec } = windowShape(name, n, true, layout);
  const partitionColumn = spec.partitionBy[0] ?? 0;
  let held = 0;
  const rows = new GroupedRows(
    types,
    spec.partitionBy,
    spec.orderBy,
    {
      allocate(bytes) {
        held += bytes;
        return true;
      },
      free(bytes) {
        held -= bytes;
        if (held < 0) throw new Error("Over-release");
      },
    },
    true,
  );
  for (const batch of input) if (!rows.add(batch)) throw new Error("Input refused");
  // Decode each partition dictionary once outside timing; full row/permutation validation
  // must not allocate a decoded string for every comparison.
  const partitions = input.map((batch) => {
    const vector = batch.columns[partitionColumn];
    if (vector?.form !== "dictionary") throw new Error("Expected partition dictionary");
    const dictionary = Array.from({ length: vector.dictionary.length }, (_, i) =>
      valueAt(vector.dictionary, i),
    );
    return { codes: vector.codes, dictionary };
  });
  const value = (id: number, column: number) => {
    if (column === partitionColumn) {
      const partition = partitions[Math.floor(id / BATCH_ROWS)];
      if (partition === undefined) throw new Error("Missing partition");
      return partition.dictionary[partition.codes[id % BATCH_ROWS] ?? 0] ?? null;
    }
    const vector = input[Math.floor(id / BATCH_ROWS)]?.columns[column];
    if (vector === undefined) throw new Error("Missing input");
    return valueAt(vector, id % BATCH_ROWS);
  };
  const compare = (a: number, b: number) => {
    for (const key of spec.orderBy) {
      const x = value(a, key.column),
        y = value(b, key.column);
      if (x === y) continue;
      if (x === null || y === null)
        return x === null ? (key.nulls === "first" ? -1 : 1) : key.nulls === "first" ? 1 : -1;
      return (x < y ? -1 : 1) * (key.direction === "asc" ? 1 : -1);
    }
    return 0;
  };
  for (let r = 0; r <= runs; r++) {
    (globalThis as { gc?: () => void }).gc?.();
    const start = process.cpuUsage();
    const execute = () => runToEnd(rows.sortJob(new Slice(Infinity)));
    const region = (
      globalThis as { minnowBenchProfile?: <T>(name: string, run: () => T) => Promise<T> }
    ).minnowBenchProfile;
    const order = r === runs && region !== undefined ? await region(name, execute) : execute();
    const used = process.cpuUsage(start);
    const load = loadavg();
    if (order === null) throw new Error("Ordering refused");
    const seen = new Uint8Array(n);
    let hash = 0;
    for (let p = 0; p < rows.groups; p++) {
      const part = rows.partitionValue(order.groupOf[p] ?? 0);
      if (part === null) throw new Error("Missing partition");
      const expected = valueAt(part, 0);
      const start = order.starts[p] ?? 0,
        end = order.starts[p + 1] ?? 0;
      for (let at = start; at < end; at++) {
        const id = order.ids[at] ?? -1;
        if (id < 0 || id >= n || seen[id]) throw new Error("Lost/duplicate row");
        seen[id] = 1;
        if (value(id, partitionColumn) !== expected) throw new Error("Wrong partition");
        const c = at === start ? 1 : compare(order.ids[at - 1] ?? 0, id);
        if (at > start && (c > 0 || (c === 0 && id < (order.ids[at - 1] ?? 0))))
          throw new Error("Wrong/stale order");
        if (order.breaks[at] !== (at === start || c !== 0 ? 1 : 0)) throw new Error("Wrong peers");
        hash = Math.imul(hash ^ id, 16777619) ^ (order.breaks[at] ?? 0);
      }
    }
    if (seen.some((x) => x !== 1)) throw new Error("Missing rows");
    rows.releaseSort();
    if (r > 0)
      console.log(
        JSON.stringify({
          name,
          layout,
          load,
          rows: n,
          cpuMs: (used.user + used.system) / 1000,
          hash: hash >>> 0,
        }),
      );
  }
  rows.close();
  if (held !== 0) throw new Error("Leaked ordering charge");
}
