/** Fixed-width UNION ALL fixpoint: two bounded FIFO work tables, spilling wide frontiers. */
import type { Rel, ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import { samePhysicalType } from "../types.js";
import type { Batch } from "../vector/vector.js";
import { BATCH_ROWS } from "../vector/vector.js";
import type { Grant } from "../memory/governor.js";
import type { SpillService } from "../spill/contract.js";
import { UnsupportedPlanError } from "./errors.js";
import { ColumnStore } from "./column-store.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";
import { PageWriter, SpillFiles } from "./spill-pages.js";
import { RecursiveTape } from "./recursive-tape.js";
import { ProjectOperator, FilterOperator } from "./filter.js";
import type { CompileContext } from "./kernels/compile.js";
import { Slice } from "./slices.js";
import { afterYield, yieldTask, YIELD_INTERVAL_MS, type Operator } from "./pipeline.js";

type RecursiveRel = Extract<Rel, { op: "recursive" }>;
const WIDTH: Readonly<Record<string, number>> = {
  bool: 1,
  int32: 4,
  int64: 8,
  float32: 4,
  float64: 8,
  date: 4,
  time: 8,
  timestamp: 8,
  interval: 16,
  uuid: 16,
};
export function recursiveLayout(types: readonly PhysicalType[]): {
  rows: number;
  pool: number;
  metadata: number;
} {
  if (types.length === 0 || types.some((type) => WIDTH[type.kind] === undefined))
    throw new UnsupportedPlanError("recursive work tables require fixed-width columns", {
      construct: "recursive",
    });
  const width = types.reduce((sum, type) => sum + (WIDTH[type.kind] ?? 0), 0);
  let rows = 1;
  while (rows * 2 <= BATCH_ROWS && rows * 2 * width <= 65536) rows *= 2;
  // Includes typed page descriptors, validity, read/decode and promotion/copy overlap. No loans.
  const page = 2 ** Math.ceil(Math.log2(4096 + 256 * types.length + rows * (width + 1)));
  return { rows, pool: 8 * page + 65536, metadata: 8192 + 512 * types.length + 2 * BATCH_ROWS };
}
function fixedExpression(expr: ScalarExpr): boolean {
  if (expr.type.kind === "text") return expr.op === "literal" || expr.op === "param";
  if (WIDTH[expr.type.kind] === undefined) return false;
  for (const value of Object.values(expr)) {
    if (value === null || typeof value !== "object" || ArrayBuffer.isView(value)) continue;
    for (const child of Array.isArray(value) ? value : [value]) {
      if (child === null || typeof child !== "object") continue;
      if ("op" in child && !fixedExpression(child as ScalarExpr)) return false;
      if (
        "then" in child &&
        (!fixedExpression((child as { then: ScalarExpr }).then) ||
          !fixedExpression((child as { when: ScalarExpr }).when))
      )
        return false;
    }
  }
  return true;
}
export function validateRecursive(rel: RecursiveRel): void {
  if (!rel.all)
    throw new UnsupportedPlanError("recursive UNION DISTINCT membership", {
      construct: "recursive",
    });
  recursiveLayout(rel.output.map((col) => col.type));
  const check = (node: Rel, step: boolean): void => {
    if (node.output.some((col) => WIDTH[col.type.kind] === undefined))
      throw new UnsupportedPlanError("variable-width recursive producer", {
        construct: "recursive",
      });
    if (node.op === "project") {
      if (!node.exprs.every(fixedExpression))
        throw new UnsupportedPlanError("variable-width recursive expression", {
          construct: "recursive",
        });
      check(node.input, step);
      return;
    }
    if (node.op === "filter") {
      if (!fixedExpression(node.predicate))
        throw new UnsupportedPlanError("variable-width recursive predicate", {
          construct: "recursive",
        });
      check(node.input, step);
      return;
    }
    if (step) {
      if (node.op !== "workTableScan" || node.workTable !== rel.workTable)
        throw new UnsupportedPlanError("nonlinear recursive step", { construct: "recursive" });
      return;
    }
    if (node.op === "values") {
      if (!node.rows.every((row) => row.every(fixedExpression)))
        throw new UnsupportedPlanError("variable-width recursive seed expression", {
          construct: "recursive",
        });
      return;
    }
    if (node.op === "scan") {
      if (node.filter !== null && !fixedExpression(node.filter))
        throw new UnsupportedPlanError("variable-width recursive seed predicate", {
          construct: "recursive",
        });
      return;
    }
    if (node.op === "series") {
      check(node.input, false);
      return;
    }
    throw new UnsupportedPlanError("recursive seed needs a simple fixed-width producer", {
      construct: "recursive",
    });
  };
  check(rel.base, false);
  check(rel.step, true);
  for (const side of [rel.base, rel.step])
    if (
      side.output.length !== rel.output.length ||
      side.output.some((col, i) => !samePhysicalType(col.type, rel.output[i]?.type ?? col.type))
    )
      throw new TypeError("Recursive work-table types differ");
}
/** Reused once per execution, never rebuilt per iteration or added to runtime disposal lists. */
export function compileRecursiveStep(
  rel: Rel,
  context: CompileContext,
  sink: Operator,
  wrap: (node: Rel, sink: Operator) => Operator = (_node, sink) => sink,
): Operator {
  if (rel.op === "workTableScan") return wrap(rel, sink);
  if (rel.op === "project" || rel.op === "filter") {
    const ctx = { ...context, input: rel.input.output.map((col) => col.type) },
      output = wrap(rel, sink);
    const operator =
      rel.op === "project"
        ? new ProjectOperator(rel.exprs, ctx, output)
        : new FilterOperator(rel.predicate, ctx, output);
    return compileRecursiveStep(rel.input, context, operator, wrap);
  }
  throw new UnsupportedPlanError("nonlinear recursive step", { construct: "recursive" });
}
class WorkQueue {
  readonly store: ColumnStore;
  #tape: RecursiveTape | null = null;
  #writing = true;
  #any = false;
  constructor(
    readonly types: readonly PhysicalType[],
    readonly io: SpillIo,
    readonly id: number,
    rows: number,
  ) {
    this.store = new ColumnStore(types, io.pool, { chunkRows: rows, targetBytes: 65536 });
    this.store.vectors();
  }
  get empty(): boolean {
    return !this.#any;
  }
  get spilled(): boolean {
    return this.#tape !== null;
  }
  async #flush(): Promise<void> {
    if (this.store.rows === 0) return;
    this.#tape ??= new RecursiveTape(this.io, this.id);
    await this.#tape.append(this.store.vectors(), this.store.rows);
    this.io.reset(this.store);
  }
  async append(batch: Batch): Promise<void> {
    if (!this.#writing) throw new TypeError("Append to a sealed work table");
    const count = batch.selection?.count ?? batch.length;
    let at = 0;
    while (at < count) {
      this.io.checkAborted();
      const paced = this.io.pace?.();
      if (paced != null) await paced;
      const copied = this.store.append(
        batch.columns,
        batch.selection?.rows ?? null,
        at,
        count - at,
      );
      at += copied;
      if (copied > 0) this.#any = true;
      if (at < count) {
        if (this.store.rows === 0)
          throw new RangeError("Recursive fixed row must fit its prepaid staging buffer");
        await this.#flush();
      }
    }
  }
  async each(visit: (batch: Batch) => Promise<boolean>): Promise<void> {
    this.#writing = false;
    if (this.#tape !== null) {
      await this.#flush();
      await this.#tape.each(this.types, async (columns, rows) =>
        visit({ length: rows, columns, selection: null }),
      );
    } else if (this.store.rows > 0)
      await visit({ length: this.store.rows, columns: this.store.vectors(), selection: null });
  }
  async reset(): Promise<void> {
    if (this.#tape !== null) {
      await this.#tape.close();
      this.#tape = null;
    }
    this.io.reset(this.store);
    this.#any = false;
    this.#writing = true;
  }
  async close(): Promise<void> {
    try {
      await this.#tape?.close();
    } finally {
      this.#tape = null;
      this.store.close();
    }
  }
}
export interface RecursiveContext {
  readonly grant: Grant;
  readonly spill: SpillService;
  readonly queryId: number;
  readonly signal?: AbortSignal;
  readonly stopped?: () => boolean;
  readonly rowsNeeded?: () => number;
}
export class RecursiveOperator {
  readonly base: Operator;
  readonly #step: Operator;
  readonly #slice = new Slice(YIELD_INTERVAL_MS);
  readonly #charge: Charge;
  readonly #pool: Pool;
  readonly #files: SpillFiles;
  readonly #pages: PageWriter;
  readonly #queues: readonly [WorkQueue, WorkQueue];
  readonly #selected: Uint16Array;
  #current: WorkQueue;
  #next: WorkQueue;
  #basePending: Promise<void> | null = null;
  #stepPending: Promise<void> | null = null;
  #finishPending: Promise<void> | null = null;
  #closing: Promise<void> | null = null;
  #aborted = false;
  #reason: unknown;
  #passes = 0;
  constructor(
    readonly rel: RecursiveRel,
    context: CompileContext,
    readonly options: RecursiveContext,
    readonly downstream: Operator,
    wrap?: (node: Rel, sink: Operator) => Operator,
  ) {
    const types = rel.output.map((col) => col.type),
      layout = recursiveLayout(types);
    context.arena.take(layout.metadata);
    this.#selected = new Uint16Array(BATCH_ROWS);
    this.#charge = new Charge(options.grant);
    this.#pool = new Pool(this.#charge, layout.pool);
    this.#files = new SpillFiles(options.spill, options.queryId, this.#charge);
    this.#pages = new PageWriter(this.#pool, 16384);
    const io = new SpillIo(
      this.#pool,
      this.#files,
      this.#pages,
      {
        grant: options.grant,
        queryId: options.queryId,
        ...(options.signal === undefined ? {} : { signal: options.signal }),
      },
      () => this.#check(),
      () => this.#pace(),
    );
    this.#current = new WorkQueue(types, io, 0, layout.rows);
    this.#next = new WorkQueue(types, io, 1, layout.rows);
    this.#queues = [this.#current, this.#next];
    const stepSink: Operator = {
      push: (batch, transfer) => {
        this.#stepPending = Promise.resolve()
          .then(() => this.#accept(batch, this.#next))
          .finally(() => transfer?.release());
        return "blocked";
      },
      ready: () => this.#stepPending ?? Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    this.#step = compileRecursiveStep(rel.step, context, stepSink, wrap);
    this.base = {
      push: (batch, transfer) => {
        this.#basePending = Promise.resolve()
          .then(() => this.#accept(batch, this.#current))
          .finally(() => transfer?.release());
        return "blocked";
      },
      ready: () => this.#basePending ?? Promise.resolve(),
      finish: () => this.#finish(),
      abort: (reason) => this.abort(reason),
    };
  }
  #check(): void {
    this.options.signal?.throwIfAborted();
    if (this.#aborted) throw this.#reason;
  }
  #pace(): Promise<void> | null {
    this.#check();
    return this.#slice.due()
      ? yieldTask().then(() => {
          afterYield(this.#slice.clock);
          this.#check();
        })
      : null;
  }
  async #accept(batch: Batch, queue: WorkQueue): Promise<void> {
    this.#check();
    if (this.options.stopped?.()) return;
    await queue.append(batch);
    this.#check();
    if (this.downstream.push(batch) === "blocked") await this.downstream.ready();
    this.#check();
  }
  #finish(): Promise<void> {
    this.#finishPending ??= Promise.resolve().then(async () => {
      await this.#basePending;
      try {
        while (!this.#current.empty && !this.options.stopped?.()) {
          this.#check();
          const paced = this.#pace();
          if (paced !== null) await paced;
          if (this.#current.spilled) this.#passes++;
          await this.#current.each(async (batch) => {
            let at = 0;
            while (at < batch.length && !this.options.stopped?.()) {
              this.#check();
              const count = Math.min(
                batch.length - at,
                Math.max(1, this.options.rowsNeeded?.() ?? BATCH_ROWS),
              );
              for (let i = 0; i < count; i++) this.#selected[i] = at + i;
              const selected =
                at === 0 && count === batch.length
                  ? batch
                  : { ...batch, selection: { rows: this.#selected, count } };
              if (this.#step.push(selected) === "blocked") await this.#step.ready();
              at += count;
              const pause = this.#pace();
              if (pause !== null) await pause;
            }
            return !this.options.stopped?.();
          });
          await this.#current.reset();
          const previous = this.#current;
          this.#current = this.#next;
          this.#next = previous;
        }
        this.#check();
        await this.downstream.finish();
      } finally {
        await this.#close();
      }
    });
    return this.#finishPending;
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    try {
      this.#step.abort(reason);
    } finally {
      this.downstream.abort(reason);
    }
  }
  async #close(): Promise<void> {
    this.#closing ??= (async () => {
      let abortFailure: unknown,
        abortFailed = false;
      try {
        this.#step.abort(undefined);
      } catch (error) {
        abortFailure = error;
        abortFailed = true;
      }
      const results = await Promise.allSettled(this.#queues.map((queue) => queue.close()));
      try {
        await this.#files.closeAll();
      } finally {
        this.#pages.close();
        this.#pool.close();
      }
      if (abortFailed) throw abortFailure;
      const failure = results.find((result) => result.status === "rejected");
      if (failure?.status === "rejected") throw failure.reason;
    })();
    return this.#closing;
  }
  get profile(): { spilledBytes: number; spillPasses: number } {
    return {
      spilledBytes: this.#files.written,
      spillPasses: this.#files.written === 0 ? 0 : this.#passes,
    };
  }
  get closed(): Promise<void> {
    return Promise.allSettled([this.#basePending, this.#stepPending, this.#finishPending]).then(
      () => this.#close(),
    );
  }
}
