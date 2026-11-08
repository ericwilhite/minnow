/** Window held-row gather only, using each frozen shape's exact row permutation/layout.
 * Input, normalization, sorting and warmup are untimed. Includes target allocation and freeing
 * per partition, as WindowEvaluator.#held does. Excludes arguments, output and evaluation.
 * Interleaved rounds, per-run CPU and load. SQL tests check logical output restoration.
 * COMPARE=amounts isolates materialized vs scalar offsets for the frozen lagLead shape.
 * ROWS=1000000 RUNS=7 node --expose-gc bundled.mjs [shapes]
 */
import type { ScalarExpr } from "../ir/ir.js";
import { windowInputLayout, remapWindowFunction } from "./window-input.js";
import { loadavg } from "node:os";
import { BATCH_ROWS } from "../vector/vector.js";
import { ColumnStore } from "./column-store.js";
import { valueAt } from "./join-fixtures.js";
import { Slice, runToEnd } from "./slices.js";
import { windowShape, materializeWindowAmounts } from "./window-bench-fixtures.js";
import { GroupedRows } from "./window-sort.js";
const n = Number(process.env.ROWS ?? 1_000_000),
  runs = Number(process.env.RUNS ?? 7);
for (const name of process.argv.slice(2).length
  ? process.argv.slice(2)
  : ["topk", "rank", "runningSum", "lagLead", "movingAvg"]) {
  const setup = (deduplicate: boolean) => {
    const original = windowShape(name, n, true, "sql");
    const scalar = process.env.COMPARE === "amounts";
    const shape = scalar && !deduplicate ? materializeWindowAmounts(original) : original;
    const first = shape.input[0];
    const projection: ScalarExpr[] = shape.types.map((type, column) => ({
      op: "column",
      type,
      index: first?.columns.findIndex((value) => value === first.columns[column]) ?? column,
    }));
    const physical = windowInputLayout(projection, null, scalar || deduplicate);
    const { types } = physical;
    const input = shape.input.map((batch) => ({
      ...batch,
      columns: physical.exprs.map((expr) => {
        const vector = expr.op === "column" ? batch.columns[expr.index] : undefined;
        if (vector === undefined) throw new Error("missing column");
        return vector;
      }),
    }));
    const spec = {
      ...shape.spec,
      types,
      partitionBy: shape.spec.partitionBy.map(physical.index),
      orderBy: shape.spec.orderBy.map((key) => ({ ...key, column: physical.index(key.column) })),
      functions: shape.spec.functions.map((call) => remapWindowFunction(call, physical.index)),
    };
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
              if (
                valueAt(got, (row - start) % out.chunkRows) !== valueAt(original, id % BATCH_ROWS)
              )
                throw new Error("Wrong gathered value");
            }
          }
        }
        out.truncate(0);
      }
      if (seen !== n) throw new Error("Lost rows");
    };
    copy(true);
    return {
      copy,
      close() {
        out.close();
        source.close();
        if (held !== 0) throw new Error("Leaked gather charge");
      },
    };
  };
  const parent = setup(false),
    candidate = setup(true);
  parent.copy(false);
  candidate.copy(false);
  for (let run = 0; run < runs; run++) {
    for (const variant of run % 2 === 0 ? ["parent", "candidate"] : ["candidate", "parent"]) {
      (globalThis as { gc?: () => void }).gc?.();
      const load = loadavg(),
        cpu = process.cpuUsage();
      (variant === "parent" ? parent : candidate).copy(false);
      const used = process.cpuUsage(cpu);
      console.log(
        JSON.stringify({
          name,
          variant,
          run,
          rows: n,
          cpuMs: (used.user + used.system) / 1000,
          load,
        }),
      );
    }
  }
  parent.close();
  candidate.close();
}
