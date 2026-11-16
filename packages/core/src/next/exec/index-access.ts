/** Native planner index input: prepaid cursors, bounded locator sorting and physical fetches. */
import type { IndexRangePlan, KeyRange } from "../access/key-index.js";
import type { TableSnapshot } from "../access/snapshot.js";
import type { Rel } from "../ir/ir.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import type { Grant, Loan } from "../memory/governor.js";
import { batchTargetFor } from "../memory/governor.js";
import { locatorPosition, locatorSegment } from "../vector/locator.js";
import { BATCH_ROWS, type Batch } from "../vector/vector.js";
import { compileExpr, type CompileContext } from "./kernels/compile.js";
import { OutputArena, isValidSlot } from "./kernels/vectors.js";
import { selectedSegment } from "./late-fetch.js";
import { pushAndWait, type Operator, type Transfer } from "./pipeline.js";
import type { QueryRuntime, OperatorIdentity } from "./run.js";
import { SortOperator } from "./sort.js";
import { TableScan } from "./table-scan.js";

export type IndexRel = Extract<Rel, { op: "keyLookup" | "indexRange" }>;
const EMPTY = new Uint8Array(0);
const LOCATOR = { kind: "float64" as const };
export function cursorMinimum(runtime: QueryRuntime, rel: IndexRel): number {
  const index = runtime.view.index(rel.tableId, rel.indexId);
  const plan =
    index.def.kind === "ordered"
      ? index.planRange?.({ lower: null, upper: null }, "asc")
      : index.planProbe?.(EMPTY);
  if (!plan) throw new TypeError("Native index has no bounded cursor plan");
  return plan.minimumBytes;
}
export function indexExpressions(rel: IndexRel, context: CompileContext) {
  return rel.op === "keyLookup"
    ? rel.keys.map((tuple) => tuple.map((expr) => compileExpr(expr, context)))
    : [
        rel.lower?.keys.map((expr) => compileExpr(expr, context)) ?? [],
        rel.upper?.keys.map((expr) => compileExpr(expr, context)) ?? [],
      ];
}
export const indexMetadata = (rel: IndexRel): number =>
  32768 +
  128 *
    (rel.op === "keyLookup"
      ? rel.keys.reduce((n, t) => n + t.length, 0)
      : (rel.lower?.keys.length ?? 0) + (rel.upper?.keys.length ?? 0));

/** Input locators are unique and sorted, or belong to one cursor (one row per index tuple).
 * This deliberately keeps at most one physical read borrow through downstream readiness. */
class IndexFetch implements Operator {
  readonly #locator;
  #table: TableSnapshot | null = null;
  #scan: TableScan | null = null;
  #pending: Promise<void> = Promise.resolve();
  #last = -1;
  constructor(
    readonly runtime: QueryRuntime,
    readonly rel: IndexRel,
    readonly grant: Grant,
    readonly context: CompileContext,
    readonly sink: Operator,
    readonly unique: boolean,
  ) {
    this.#locator = context.arena.flat({ kind: "locator", tableId: rel.tableId }).vector;
  }
  push(batch: Batch, transfer?: Transfer): "blocked" {
    this.#pending = this.#read(batch).finally(() => transfer?.release());
    return "blocked";
  }
  ready() {
    return this.#pending;
  }
  get closed() {
    return this.#pending.catch(() => undefined);
  }
  async #read(batch: Batch): Promise<void> {
    const input = batch.columns[0];
    if (input?.form !== "flat" || input.data.layout !== "f64")
      throw new TypeError("Index locator layout");
    this.#table ??= await this.runtime.table(this.rel.tableId, this.grant);
    for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
      this.runtime.signal.throwIfAborted();
      const row = batch.selection?.rows[k] ?? k,
        locator = input.data.values[row] ?? -1;
      if (this.unique && locator === this.#last) continue;
      this.#last = locator;
      const source = this.#table.segments[locatorSegment(locator)];
      if (!source || locatorPosition(locator) >= source.rowCount)
        throw new RangeError("Index locator outside snapshot");
      const segment = selectedSegment(source, 0, new Uint16Array([locatorPosition(locator)]));
      const scan = new TableScan({
        table: {
          tableId: this.#table.tableId,
          schemaVersion: this.#table.schemaVersion,
          columns: this.#table.columns,
          segments: [segment],
          liveRowCount: 1,
        },
        columns: this.rel.columns,
        filter: null,
        emitLocator: false,
        pages: this.runtime.readPages(this.runtime.view.pages),
        reads: this.runtime.request.reads,
        budget: this.grant,
        context: this.context,
        targetBytes: batchTargetFor(this.runtime.request.governor.capBytes, 4),
        boundedMemory: true,
        rowsNeeded: () => 1,
        loans: { borrow: (bytes) => this.grant.borrow(bytes, this.runtime.signal) },
      });
      this.#scan = scan;
      try {
        const item = await scan.next();
        if (!item || (item.batch.selection?.count ?? item.batch.length) !== 1)
          throw new RangeError("Index omitted physical row");
        try {
          let output = item.batch;
          if (this.rel.emitLocator) {
            const data = this.#locator.data;
            if (data.layout !== "f64") throw new TypeError("Index locator output layout");
            data.values[output.selection?.rows[0] ?? 0] = locator;
            this.#locator.length = output.length;
            output = { ...output, columns: [...output.columns, this.#locator] };
          }
          await pushAndWait(this.sink, output);
        } finally {
          item.transfer?.release();
        }
      } finally {
        scan.abort();
        this.#scan = null;
      }
    }
  }
  finish() {
    return this.sink.finish();
  }
  abort(reason: unknown): void {
    this.#scan?.abort();
    this.sink.abort(reason);
  }
  close(): void {
    this.#table = null;
  }
}
export async function runIndexAccess(
  runtime: QueryRuntime,
  rel: IndexRel,
  sink: Operator,
  minimum: number,
  sortIdentity: OperatorIdentity | null,
  sortMinimum: number,
): Promise<void> {
  const grant = await runtime.admit(`${rel.op}#${String(rel.id)}`, minimum, true, rel);
  const arena = new OutputArena(grant, BATCH_ROWS);
  runtime.dispose(() => arena.close());
  const context: CompileContext = {
    arena,
    input: rel.output.slice(0, rel.columns.length).map((c) => c.type),
    params: runtime.params,
  };
  arena.take(indexMetadata(rel));
  const expressions = indexExpressions(rel, { ...context, input: [] });
  const keys = [new KeyBatch(grant, Infinity), new KeyBatch(grant, Infinity)] as const;
  runtime.dispose(() => {
    for (const key of keys) key.close();
  });
  const vector = arena.flat(LOCATOR).vector;
  if (vector.data.layout !== "f64") throw new TypeError("Cursor output layout");
  const locators = vector.data.values;
  const fetch = new IndexFetch(runtime, rel, grant, context, sink, rel.op === "keyLookup");
  runtime.onAbort((reason) => fetch.abort(reason));
  runtime.dispose(() => fetch.close());
  runtime.dispose(() => fetch.closed);
  const sort =
    sortIdentity === null
      ? null
      : new SortOperator(
          {
            types: [LOCATOR],
            keys: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
            limit: null,
            offset: 0,
            withTies: false,
          },
          {
            grant: await runtime.admit(
              `sort#${String(sortIdentity.id)}`,
              sortMinimum,
              true,
              sortIdentity,
            ),
            spill: runtime.request.spill,
            queryId: runtime.request.queryId,
            signal: runtime.signal,
          },
          fetch,
        );
  if (sort) {
    runtime.onAbort((reason) => sort.abort(reason));
    runtime.dispose(() => sort.closed);
  }
  const loan: { current: Loan | null } = { current: null };
  const encode = async (
    tuple: ReturnType<typeof indexExpressions>[number],
    out: KeyBatch,
  ): Promise<Uint8Array | null> => {
    const columns = tuple.map((expr) =>
      expr.evaluate({ length: 1, columns: [], selection: null }, null),
    );
    if (columns.some((column) => !isValidSlot(column, 0))) return null;
    for (;;) {
      const result = encodeKeyBatch(
        "equality",
        columns.map((vector) => ({ vector, direction: "asc" as const, nulls: "last" as const })),
        { length: 1, selection: null },
        out,
      );
      if (result.done) return out.key(0);
      if (loan.current) throw new RangeError("Index key needs another unrepaid progress loan");
      loan.current = await grant.borrow(
        2 * result.requiredBytes + cursorMinimum(runtime, rel),
        runtime.signal,
      );
    }
  };
  const run = async (plan: IndexRangePlan) => {
    runtime.signal.throwIfAborted();
    const cursor = plan.open({ budget: grant, signal: runtime.signal });
    runtime.onAbort(() => cursor.close());
    try {
      for (;;) {
        runtime.signal.throwIfAborted();
        const n = await cursor.next(locators);
        if (n === 0) break;
        vector.length = n;
        await pushAndWait(sort ?? fetch, { length: n, selection: null, columns: [vector] });
      }
    } finally {
      cursor.close();
    }
  };
  runtime.pipeline();
  try {
    const index = runtime.view.index(rel.tableId, rel.indexId);
    if (rel.op === "keyLookup") {
      for (const tuple of expressions) {
        const out = keys[0];
        const key = await encode(tuple, out);
        if (key === null) continue;
        const plan = index.planProbe?.(key);
        if (!plan) throw new TypeError("Native index has no probe cursor");
        await run(plan);
      }
    } else {
      const lower = rel.lower === null ? null : await encode(expressions[0] ?? [], keys[0]);
      const upper = rel.upper === null ? null : await encode(expressions[1] ?? [], keys[1]);
      if ((rel.lower === null || lower !== null) && (rel.upper === null || upper !== null)) {
        const range: KeyRange = {
          lower:
            lower === null
              ? null
              : { kind: rel.lower?.inclusive ? "atLeast" : "after", key: lower },
          upper:
            upper === null
              ? null
              : { kind: rel.upper?.inclusive ? "through" : "before", key: upper },
        };
        const plan = index.planRange?.(range, rel.descending ? "desc" : "asc");
        if (!plan) throw new TypeError("Native index has no range plan");
        await run(plan);
      }
    }
    if (sort) await sort.finish();
    else await fetch.finish();
  } finally {
    keys.forEach((key) => key.writer.reset());
    arena.retireVarlen();
    loan.current?.repay();
  }
}
