import { inheritFtsWorkspace, installFtsHost, type FtsMatchNode } from "./kernels/fts.js";
/** One borrowed row at a time: resumable JSON kernels without changing synchronous kernel callers. */
import type { ScalarExpr } from "../ir/ir.js";
import type { Batch, Selection, Vector } from "../vector/vector.js";
import { FilterOperator } from "./filter.js";
import { compileExpr, type CompileContext } from "./kernels/compile.js";
import { installJsonHost, type JsonHost, type JsonMemory, type JsonNode } from "./kernels/json.js";
import { OutputArena, type Compiled } from "./kernels/vectors.js";
import {
  afterYield,
  yieldTask,
  YIELD_INTERVAL_MS,
  type Operator,
  type PushResult,
  type Transfer,
} from "./pipeline.js";
import { Slice, runSliced, type SlicedJob } from "./slices.js";
import { JsonWorkspace } from "./json-workspace.js";

/** Each extractor has a source-sized sub-budget inside a completely prepaid workspace. */
class JsonMemorySlice implements JsonMemory {
  #used = 0;
  constructor(readonly capacity: number) {}
  // eslint-disable-next-line require-yield -- fixed, prepaid credit never waits
  *take(bytes: number): SlicedJob<void> {
    if (this.#used + bytes > this.capacity)
      throw new Error("JSON extraction exceeded its admitted input bound");
    this.#used += bytes;
  }
  free(bytes: number): void {
    if (bytes > this.#used) throw new Error("JSON extraction workspace underflow");
    this.#used -= bytes;
  }
}

/** Internal suspension, caught only by the owner. Reused per node, no per-row Error stack. */
class JsonRequest extends Error {
  batch!: Batch;
  rows: Selection | null = null;
  constructor(readonly node: JsonNode) {
    super("Internal JSON suspension");
  }
}
export interface JsonExpressionContext {
  /** Prepaid row workspace. Raw JSON uses its source envelope + 1; expanding frames
   * checkpoint and re-admit when this shared budget is exhausted. */
  readonly bytes: number;
  /** Expanding expressions checkpoint instead of exceeding this shared row credit. */
  readonly expanding?: boolean;
  readonly signal?: AbortSignal;
  readonly stopped?: () => boolean;
}
/** A suspension resumes its current projection; completed columns are never re-evaluated. */
class JsonProject implements Operator {
  readonly #exprs: readonly Compiled[];
  readonly #columns: Vector[] = [];
  #cursor = 0;
  constructor(
    exprs: readonly ScalarExpr[],
    context: CompileContext,
    readonly downstream: Operator,
    readonly pin: (value: Vector) => void,
  ) {
    context.arena.take(192 + 96 * exprs.length);
    this.#exprs = exprs.map((expr) => compileExpr(expr, context));
  }
  push(batch: Batch): PushResult {
    while (this.#cursor < this.#exprs.length) {
      const expr = this.#exprs[this.#cursor];
      if (expr === undefined) throw new Error("Missing JSON projection");
      const value = expr.evaluate(batch, batch.selection);
      this.#columns[this.#cursor] = value;
      this.pin(value);
      this.#cursor++;
    }
    const result = this.downstream.push({
      length: batch.length,
      selection: batch.selection,
      columns: this.#columns,
    });
    this.#cursor = 0;
    return result;
  }
  endRow(): void {
    this.#columns.length = 0;
    this.#cursor = 0;
  }
  ready(): Promise<void> {
    return this.downstream.ready();
  }
  finish() {
    return this.downstream.finish();
  }
  abort(reason: unknown): void {
    this.downstream.abort(reason);
  }
}

export class JsonExpressions implements Operator, JsonHost {
  readonly slice = new Slice(YIELD_INTERVAL_MS);
  readonly #arena: OutputArena;
  readonly #workspace: JsonWorkspace | null;
  #running = false;
  #variableArenaBytes = 0;
  readonly #inner: Operator;
  readonly #project: JsonProject | null;
  readonly #nodes: JsonNode[] = [];
  readonly #ftsNodes: FtsMatchNode[] = [];
  readonly #dependencies = new Map<JsonNode, Set<JsonNode>>();
  readonly #users = new Map<JsonNode, number>();
  readonly #pinned = new Set<ArrayBufferLike>();
  readonly #requests = new Map<JsonNode, JsonRequest>();
  readonly #completed = new Map<JsonNode, number>();
  readonly #one: Selection;
  readonly #batch: { length: number; columns: readonly Vector[]; selection: Selection };
  #epoch = 0;
  #pending: Promise<void> | null = null;
  #aborted = false;
  #reason: unknown;
  #closed = false;
  constructor(
    exprs: readonly ScalarExpr[] | ScalarExpr,
    readonly context: CompileContext,
    readonly shared: JsonExpressionContext,
    readonly downstream: Operator,
  ) {
    context.arena.take(1024 + 2);
    this.#workspace = shared.expanding ? new JsonWorkspace(shared.bytes) : null;
    this.#one = { rows: new Uint16Array(1), count: 1 };
    this.#batch = { length: 0, columns: [], selection: this.#one };
    this.#arena = new OutputArena(
      {
        allocate: (bytes) => {
          if (this.#running && this.#workspace !== null) {
            this.#workspace.takeNow(bytes);
            this.#variableArenaBytes += bytes;
          } else context.arena.take(bytes);
          return true;
        },
        free: (bytes) => {
          const variable = Math.min(bytes, this.#variableArenaBytes);
          this.#workspace?.free(variable);
          this.#variableArenaBytes -= variable;
          if (bytes > variable) context.arena.give(bytes - variable);
        },
      },
      context.arena.capacity,
      true,
    );
    inheritFtsWorkspace(context.arena, this.#arena);
    installFtsHost(this.#arena, (node) => this.#ftsNodes.push(node));
    installJsonHost(this.#arena, this);
    const compile = { ...context, arena: this.#arena };
    this.#project = Array.isArray(exprs)
      ? new JsonProject(exprs, compile, downstream, (value) => {
          const flat =
            value.form === "constant"
              ? value.value
              : value.form === "dictionary"
                ? value.dictionary
                : value;
          if (flat.data.layout === "varlen") this.#pinned.add(flat.data.bytes.buffer);
        })
      : null;
    this.#inner = this.#project ?? new FilterOperator(exprs as ScalarExpr, compile, downstream);
    installFtsHost(this.#arena, null);
    this.#arena.take((this.#workspace === null ? this.#nodes.length : 1) * shared.bytes);
    this.#running = true;
  }
  get workspaceCount(): number {
    return this.#nodes.length;
  }
  memory(node: JsonNode): JsonMemory {
    this.#arena.take(1024);
    this.#dependencies.set(node, new Set());
    this.#users.set(node, 0);
    const memory = this.#workspace ?? new JsonMemorySlice(this.shared.bytes);
    this.#nodes.push(node);
    this.#requests.set(node, new JsonRequest(node));
    return memory;
  }
  evaluate(node: JsonNode, batch: Batch, rows: Selection | null): Vector {
    if (this.#completed.get(node) === this.#epoch) {
      // The owner records the exact vector, whose byte borrow lasts to the end of this row.
      const value = this.#values.get(node);
      if (value === undefined) throw new Error("Missing JSON row value");
      return value;
    }
    const request = this.#requests.get(node);
    if (request === undefined) throw new Error("Unregistered JSON kernel");
    request.batch = batch;
    request.rows = rows;
    throw request;
  }
  readonly #values = new Map<JsonNode, Vector>();
  #check(): void {
    this.shared.signal?.throwIfAborted();
    if (this.#aborted) throw this.#reason;
  }
  /** Retire intermediate JSON values only after their consuming node has captured them.
   * Already projected columns keep a pin through downstream readiness. Sibling inputs of
   * an unfinished node retain references, so eviction cannot cause recomputation cycles. */
  inputsConsumed(node: JsonNode): void {
    const dependencies = this.#dependencies.get(node);
    if (!dependencies) return;
    for (const child of dependencies) {
      const users = (this.#users.get(child) ?? 0) - 1;
      if (users < 0) throw new Error("JSON dependency underflow");
      this.#users.set(child, users);
      const vector = this.#values.get(child);
      const flat = vector?.form === "flat" ? vector : undefined;
      if (
        users === 0 &&
        flat?.data.layout === "varlen" &&
        !this.#pinned.has(flat.data.bytes.buffer)
      ) {
        child.reset();
        this.#completed.delete(child);
        this.#values.delete(child);
      }
    }
    dependencies.clear();
  }
  async #compute(request: JsonRequest): Promise<void> {
    try {
      for (;;) {
        this.#check();
        if (this.slice.due()) {
          await yieldTask();
          afterYield(this.slice.clock);
          this.#check();
        }
        try {
          const job = request.node.run(request.batch, request.rows);
          let value: Vector;
          try {
            value = await runSliced(job, this.slice, () => this.#check());
          } catch (error) {
            request.node.discard(job);
            throw error;
          }
          this.#completed.set(request.node, this.#epoch);
          this.#values.set(request.node, value);
          return;
        } catch (error) {
          if (!(error instanceof JsonRequest)) throw error;
          const dependencies = this.#dependencies.get(request.node);
          if (!dependencies) throw new Error("Missing JSON dependency owner", { cause: error });
          if (!dependencies.has(error.node)) {
            dependencies.add(error.node);
            this.#users.set(error.node, (this.#users.get(error.node) ?? 0) + 1);
          }
          await this.#compute(error);
        }
      }
    } finally {
      this.inputsConsumed(request.node);
    }
  }
  push(batch: Batch, transfer?: Transfer): PushResult {
    this.#pending = this.#rows(batch).finally(() => {
      // The producer may free or recycle these buffers as soon as the borrow ends.
      this.#batch.columns = [];
      this.#batch.length = 0;
      transfer?.release();
    });
    return "blocked";
  }
  async #rows(batch: Batch): Promise<void> {
    const count = batch.selection?.count ?? batch.length;
    const hasFts = this.#ftsNodes.length > 0;
    this.#batch.length = batch.length;
    this.#batch.columns = batch.columns;
    for (let k = 0; k < count; k++) {
      this.#check();
      if (this.shared.stopped?.()) break;
      this.#epoch++;
      this.#one.rows[0] = batch.selection?.rows[k] ?? k;
      // FTS arguments are inert named columns. Prepare without replaying any JSON or
      // volatile scalar; keep document errors deferred until the kernel is demanded.
      if (hasFts)
        for (const node of this.#ftsNodes) {
          try {
            await runSliced(node.compute(this.#batch, this.#one, this.slice), this.slice, () =>
              this.#check(),
            );
          } catch (error) {
            this.#check();
            node.fail(error);
          }
        }
      for (;;) {
        try {
          if (this.#inner.push(this.#batch) === "blocked") await this.#inner.ready();
          break;
        } catch (error) {
          if (!(error instanceof JsonRequest)) throw error;
          await this.#compute(error);
        }
      }
      this.#project?.endRow();
      if (hasFts) for (const node of this.#ftsNodes) node.clear();
      this.#pinned.clear();
      for (const node of this.#nodes) node.reset();
      this.#values.clear();
      if (this.slice.due()) {
        await yieldTask();
        afterYield(this.slice.clock);
      }
    }
  }
  ready(): Promise<void> {
    return this.#pending ?? Promise.resolve();
  }
  async finish(): Promise<void> {
    try {
      await this.#pending;
      this.#check();
      await this.#inner.finish();
    } finally {
      this.#close();
    }
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    try {
      this.#inner.abort(reason);
    } finally {
      if (this.#pending === null) this.#close();
      else void this.#pending.catch(() => undefined).then(() => this.#close());
    }
  }
  #close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#project?.endRow();
    for (const node of this.#ftsNodes) node.clear();
    this.#pinned.clear();
    for (const node of this.#nodes) node.reset();
    this.#values.clear();
    this.#requests.clear();
    this.#completed.clear();
    this.#arena.close();
  }
  get closed(): Promise<void> {
    return this.#pending?.catch(() => undefined).then(() => this.#close()) ?? Promise.resolve();
  }
}
