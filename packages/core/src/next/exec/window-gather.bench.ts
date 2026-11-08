/** Window held-row gather only, using each frozen shape's exact row permutation/layout.
 * Input, normalization, sorting and warmup are untimed. Includes target allocation and freeing
 * per partition, as WindowEvaluator.#held does. Excludes arguments, output and evaluation.
 * ROWS=1000000 RUNS=7 LAYOUT=sql (or compact) node --expose-gc bundled.mjs [shapes]
 */
import { loadavg } from "node:os";
import { BATCH_ROWS } from "../vector/vector.js";
import { ColumnStore } from "./column-store.js";
import { valueAt } from "./join-fixtures.js";
import { Slice, runToEnd } from "./slices.js";
import { windowShape } from "./window-bench-fixtures.js";
import { GroupedRows } from "./window-sort.js";
const n = Number(process.env.ROWS ?? 1_000_000),
  runs = Number(process.env.RUNS ?? 7);
for (const name of process.argv.slice(2).length
  ? process.argv.slice(2)
  : ["topk", "rank", "runningSum", "lagLead", "movingAvg"]) {
  const layout = process.env.LAYOUT ?? "sql";
  const { types, input, spec } = windowShape(name, n, true, layout);
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
  const partition = spec.partitionBy[0] ?? -1;
  const columns = types.map((_, c) => (c === partition ? -1 : c));
  const out = new ColumnStore(types, charge);
  const copy = (validate: boolean) => {
    let seen = 0;
    for (let p = 0; p < source.groups; p++) {
      const start = order.starts[p] ?? 0,
        end = order.starts[p + 1] ?? 0;
      for (let at = start; at < end; at += BATCH_ROWS) {
        const count = Math.min(BATCH_ROWS, end - at);
        if (out.appendFrom(source.store, columns, order.ids, at, count) !== count)
          throw new Error("Gather refused");
        seen += count;
      }
      if (validate) {
        for (let row = start; row < end; row++) {
          const id = order.ids[row] ?? -1;
          for (let c = 0; c < types.length; c++) {
            if (c === partition) continue; // The evaluator emits this column as a constant.
            const original = input[Math.floor(id / BATCH_ROWS)]?.columns[c];
            if (original === undefined) throw new Error("Missing original row");
            const got = out.chunk(c, Math.floor((row - start) / out.chunkRows));
            if (valueAt(got, (row - start) % out.chunkRows) !== valueAt(original, id % BATCH_ROWS))
              throw new Error("Wrong gathered value");
          }
        }
      }
      out.truncate(0);
    }
    if (seen !== n) throw new Error("Lost rows");
  };
  copy(true);
  for (let run = 0; run <= runs; run++) {
    (globalThis as { gc?: () => void }).gc?.();
    const cpu = process.cpuUsage();
    copy(false);
    const used = process.cpuUsage(cpu);
    if (run > 0)
      console.log(
        JSON.stringify({
          name,
          layout,
          rows: n,
          cpuMs: (used.user + used.system) / 1000,
          load: loadavg(),
        }),
      );
  }
  out.close();
  source.close();
  if (held !== 0) throw new Error("Leaked gather charge");
}
