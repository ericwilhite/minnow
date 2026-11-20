/** Native planner index input: prepaid cursors, bounded locator sorting and physical fetches. */
import type { IndexCursor, IndexRangePlan, KeyRange, MaybePromise } from "../access/key-index.js";
import { SnapshotReleasedError, type TableSnapshot } from "../access/snapshot.js";
import type { Rel, ScalarExpr } from "../ir/ir.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import type { Grant, Loan } from "../memory/governor.js";
import { batchTargetFor } from "../memory/governor.js";
import { compareBytes } from "../vector/varlen.js";
import { locatorPosition, locatorSegment } from "../vector/locator.js";
import { BATCH_ROWS, type Batch } from "../vector/vector.js";
import { compileExpr, type CompileContext } from "./kernels/compile.js";
import { OutputArena, isValidSlot } from "./kernels/vectors.js";
import { selectedSegment } from "./late-fetch.js";
import { pushAndWait, taskClock, yieldTask, type Operator, type Transfer } from "./pipeline.js";
import type { QueryRuntime, OperatorIdentity } from "./run.js";
import { SortOperator } from "./sort.js";
import { ModifyTargetSet } from "./modify-target-set.js";
import { TableScan } from "./table-scan.js";

export type IndexRel = Extract<Rel, { op: "keyLookup" | "indexRange" }>;
const EMPTY = new Uint8Array(0);
const LOCATOR = { kind: "float64" as const };
/** Two fixed KeyBatch buffers leave this prepaid growth overlap before cursor I/O. */
export const INDEX_KEY_FIXED_SLACK = 16384 - 2 * (4096 + 4 * 65);
export const SMALL_INDEX_IN_KEYS = 16;
export const indexFetchTarget = (cap: number) => Math.min(64 * 1024, batchTargetFor(cap, 4));
export function cursorMinimum(runtime: QueryRuntime, rel: IndexRel): number {
  const index = runtime.view.index(rel.tableId, rel.indexId);
  const plan =
    index.def.kind === "ordered"
      ? index.planRange?.({ lower: null, upper: null }, "asc")
      : index.planProbe?.(EMPTY);
  if (!plan) throw new TypeError("Native index has no bounded cursor plan");
  return plan.minimumBytes;
}
/** The immutable plan owns IN-list metadata; execution compiles only one tuple at a time. */
export function indexTuples(rel: IndexRel): ReadonlyArray<readonly ScalarExpr[]> {
  return rel.op === "keyLookup" ? rel.keys : [rel.lower?.keys ?? [], rel.upper?.keys ?? []];
}
export function indexMetadata(rel: IndexRel): number {
  let arity = 0;
  for (const tuple of indexTuples(rel)) arity = Math.max(arity, tuple.length);
  return 32768 + 128 * arity;
}

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
    readonly seen: ModifyTargetSet | null,
    readonly stopped: () => boolean,
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
      if (this.stopped()) return;
      const row = batch.selection?.rows[k] ?? k,
        locator = input.data.values[row] ?? -1;
      if (this.unique && locator === this.#last) continue;
      if (this.seen && (await this.seen.testAndSet(this.rel.tableId, locator))) continue;
      this.runtime.signal.throwIfAborted();
      if (this.runtime.view.released) throw new SnapshotReleasedError();
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
        targetBytes: indexFetchTarget(this.runtime.request.governor.capBytes),
        boundedMemory: true,
        rowsNeeded: () => 1,
        loans: { borrow: (bytes) => this.grant.borrow(bytes, this.runtime.signal) },
      });
      this.#scan = scan;
      try {
        let item = await scan.next();
        // A selected memory position can lie after several byte-bounded prefixes.
        // Empty prefixes still made physical progress; only source end omits a row.
        while (item && (item.batch.selection?.count ?? item.batch.length) === 0) {
          item.transfer?.release();
          item = await scan.next();
        }
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
  keyIdentity: OperatorIdentity,
  keyMinimum: number,
  keyBudget: () => MaybePromise<Grant>,
  trackLocators: boolean,
  sortIdentity: OperatorIdentity | null,
  sortMinimum: number,
  stopped: () => boolean,
): Promise<void> {
  const grant = await runtime.admit(`${rel.op}#${String(rel.id)}`, minimum, true, rel);
  const arena = new OutputArena(grant, BATCH_ROWS);
  runtime.dispose(() => arena.close());
  const context: CompileContext = {
    arena,
    input: rel.output.slice(0, rel.columns.length).map((c) => c.type),
    params: runtime.params,
  };
  const keyGrant = await keyBudget();
  let keyLoan: Loan | null = null;
  const keyScratch = new OutputArena(keyGrant, 1);
  const keys: KeyBatch[] = [];
  let keysClosed = false;
  const closeKeys = () => {
    if (keysClosed) return;
    keysClosed = true;
    for (const key of keys) key.close();
    keyScratch.close();
    keyLoan?.repay();
  };
  runtime.dispose(closeKeys);
  keyLoan = await keyGrant.borrow(
    keyMinimum + runtime.operatorLoanBytes(keyIdentity),
    runtime.signal,
  );
  keyScratch.take(indexMetadata(rel));
  keys.push(new KeyBatch(keyGrant, Infinity));
  keys.push(new KeyBatch(keyGrant, Infinity));
  const firstKey = keys[0],
    secondKey = keys[1];
  if (!firstKey || !secondKey) throw new Error("Missing index key buffers");
  const vector = arena.flat(LOCATOR).vector;
  if (vector.data.layout !== "f64") throw new TypeError("Cursor output layout");
  const locators = vector.data.values;
  const seen = trackLocators
    ? new ModifyTargetSet(grant, runtime.request.spill, runtime.request.queryId, runtime.signal)
    : null;
  if (seen) runtime.dispose(() => seen.close());
  const fetch = new IndexFetch(
    runtime,
    rel,
    grant,
    context,
    sink,
    rel.op === "keyLookup",
    seen,
    stopped,
  );
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
  const expressions = indexTuples(rel);
  const encode = (tuple: readonly ScalarExpr[], out: KeyBatch): Uint8Array | null => {
    const tupleArena = new OutputArena(keyGrant, 1);
    try {
      const scope = { ...context, arena: tupleArena, input: [] };
      const columns = tuple.map((expr) =>
        compileExpr(expr, scope).evaluate({ length: 1, columns: [], selection: null }, null),
      );
      if (columns.some((column) => !isValidSlot(column, 0))) return null;
      const result = encodeKeyBatch(
        "equality",
        columns.map((vector) => ({ vector, direction: "asc" as const, nulls: "last" as const })),
        { length: 1, selection: null },
        out,
      );
      if (!result.done) throw new RangeError("Prepaid index key workspace did not fit");
      return out.key(0);
    } finally {
      tupleArena.close();
    }
  };
  let activeCursor: IndexCursor | null = null;
  runtime.onAbort(() => activeCursor?.close());
  const run = async (plan: IndexRangePlan) => {
    runtime.signal.throwIfAborted();
    const cursor = plan.open({ budget: keyGrant, signal: runtime.signal });
    activeCursor = cursor;
    try {
      for (;;) {
        runtime.signal.throwIfAborted();
        if (stopped()) break;
        const n = await cursor.next(locators);
        if (n === 0) break;
        vector.length = n;
        await pushAndWait(sort ?? fetch, { length: n, selection: null, columns: [vector] });
      }
    } finally {
      cursor.close();
      activeCursor = null;
    }
  };
  runtime.pipeline();
  try {
    const index = runtime.view.index(rel.tableId, rel.indexId);
    if (rel.op === "keyLookup") {
      // SQL keys contain only literals, parameters and casts. Their deterministic
      // coercions are ready before the first probe: a satisfied LIMIT must not
      // hide a later key's data error. Bindings already validate bare parameters.
      // Volatile or row-dependent expressions are outside the key-input contract.
      const constant = (expr: ScalarExpr): boolean =>
        expr.op === "literal" ||
        expr.op === "param" ||
        (expr.op === "cast" && constant(expr.input));
      if (expressions.length > 1)
        for (const tuple of expressions) {
          if (taskClock.due()) await yieldTask();
          runtime.signal.throwIfAborted();
          if (tuple.some((expr) => expr.op === "cast") && tuple.every(constant))
            encode(tuple, secondKey);
        }
      for (let position = 0; position < expressions.length; position++) {
        if (stopped()) break;
        if (taskClock.due()) await yieldTask();
        runtime.signal.throwIfAborted();
        const tuple = expressions[position];
        if (!tuple) throw new Error("Missing key tuple");
        const key = encode(tuple, firstKey);
        if (key === null) continue;
        if (sort === null && seen === null && expressions.length > 1) {
          // Full keys name disjoint row sets. Bound this quadratic comparison to
          // sixteen tuples; larger complete query lists use the charged locator bitmap.
          if (expressions.length > SMALL_INDEX_IN_KEYS) throw new Error("Unbounded direct IN list");
          let duplicate = false;
          for (let prior = 0; prior < position; prior++) {
            const previous = expressions[prior];
            if (!previous) throw new Error("Missing prior key");
            const bytes = encode(previous, secondKey);
            if (bytes !== null && compareBytes(bytes, 0, bytes.length, key, 0, key.length) === 0) {
              duplicate = true;
              break;
            }
          }
          if (duplicate) continue;
        }
        const plan = index.planProbe?.(key);
        if (!plan) throw new TypeError("Native index has no probe cursor");
        await run(plan);
      }
    } else {
      const lower = rel.lower === null ? null : encode(expressions[0] ?? [], firstKey);
      const upper = rel.upper === null ? null : encode(expressions[1] ?? [], secondKey);
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
    closeKeys();
    arena.retireVarlen();
  }
}
