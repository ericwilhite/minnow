/**
 * The row host for VALUES cells (design note P1-phase-expression-design.md, mechanism A).
 *
 * Expressions with variable-size output (text and array concatenation, casts to text, JSON
 * constructors and casts) compile only against a `JsonHost`. In the pipeline that host is
 * `JsonExpressions`, which grows by cutting a producer phase. A VALUES list evaluates its cells
 * outside the pipeline, one row per `next()`, from literals and parameters only, so it needs no
 * phase: this host evaluates one row's JSON nodes and finds their memory itself.
 *
 * Memory law:
 * - Every byte a node keeps is charged before it is allocated: `JsonNode` sizes its output
 *   exactly (a counting pass) and calls `take(bytes)` before it creates the buffer, and `take`
 *   charges the bytes to a grant before returning.
 * - Small rows use the operator's admitted slack. No governor call is made while a row fits, so
 *   a many-row INSERT costs the same admissions as one of literals.
 * - A row that does not fit is measured, not guessed: the attempt stops at the first take that
 *   does not fit, every byte of the row is freed, and the row is retried once room for the
 *   measured need (the bytes held at that take, plus the take) exists. Each retry learns a
 *   larger need, so it ends.
 * - Room comes from holder growth: a separate row grant admitted with `holdsCredit`. That queue
 *   is served before new admissions and never draws on the progress-loan reserve. A progress
 *   loan would: the row stays borrowed downstream while the modification spool copies it, and
 *   that copy may take the spool's own loan from the same reserve, which would then wait for
 *   ours (testing/values-row-host.test.ts pins this).
 * - Growth happens only inside `next()`, after the previous row's borrow has ended (a batch
 *   returned without a transfer is borrowed until the following `next()`), so nothing of ours is
 *   held downstream and the consumer is not mid-push while we wait. Growing while a row is still
 *   borrowed is an invariant error.
 * - A row whose need alone exceeds the governor's usable capacity is refused with 53200 before
 *   any wait. A wait ends on the statement's signal; an admission completed after cancellation
 *   is closed, never kept.
 */
import type { Batch, Vector } from "../vector/vector.js";
import type { Grant, MemoryGovernor } from "../memory/governor.js";
import { MemoryAdmissionError } from "../memory-values.js";
import type { JsonHost, JsonMemory, JsonNode } from "./kernels/json.js";
import type { Compiled } from "./kernels/vectors.js";
import { afterYield, yieldTask, YIELD_INTERVAL_MS } from "./pipeline.js";
import { Slice, runSliced, type SlicedJob } from "./slices.js";

/** Row room admitted with the operator: rows whose JSON nodes fit make no governor call. */
export const VALUES_ROW_SLACK = 64 * 1024;

/** Internal: a node asked for a value not computed yet in this row. Caught only by the host. */
class RowRequest extends Error {
  constructor(readonly node: JsonNode) {
    super("Internal VALUES row request");
  }
}

/** Internal: a take that does not fit; `need` is the row's measured requirement so far. */
export class RowRoomNeeded extends Error {
  constructor(readonly need: number) {
    super(`A VALUES row needs ${String(need)} bytes of row room`);
  }
}

/** Thrown when growth is attempted while a returned row is still borrowed downstream. */
export class RowBorrowInvariantError extends Error {
  readonly code = "MINNOW_VALUES_ROW_BORROWED";
  constructor() {
    super("VALUES row room grew while the previous row was still borrowed downstream");
    this.name = "RowBorrowInvariantError";
  }
}

export interface RowHostOptions {
  /** The operator's admitted grant: kernel metadata plus the small-row slack. */
  readonly grant: Grant;
  readonly governor: MemoryGovernor;
  readonly signal: AbortSignal;
  readonly label: string;
  /** Admits the row grant as holder growth (`holdsCredit`), metered by the query runtime. */
  readonly admitRow: (bytes: number) => Grant | Promise<Grant>;
}

export class ValuesRowHost implements JsonHost {
  readonly slice = new Slice(YIELD_INTERVAL_MS);
  readonly #nodes: JsonNode[] = [];
  readonly #completed = new Map<JsonNode, number>();
  readonly #values = new Map<JsonNode, Vector>();
  readonly #requests = new Map<JsonNode, RowRequest>();
  /** Children each node read while computing; retired once their last reader completed. */
  readonly #dependencies = new Map<JsonNode, Set<JsonNode>>();
  readonly #users = new Map<JsonNode, number>();
  #epoch = 0;
  #used = 0;
  #rowGrant: Grant | null = null;
  #borrowed = false;
  /** Governor admissions made for row room (tests read it). */
  growths = 0;
  readonly #memory: JsonMemory;
  #options: RowHostOptions | null = null;

  constructor() {
    this.#memory = { take: (bytes) => this.#take(bytes), free: (bytes) => this.#free(bytes) };
  }

  // Charged before the caller allocates; a miss stops the attempt (see the file comment).
  // eslint-disable-next-line require-yield -- a take either fits now or ends the attempt
  *#take(bytes: number): SlicedJob<void> {
    if (bytes === 0) return;
    const options = this.#bound();
    const target = this.#rowGrant ?? options.grant;
    if (!target.allocate(bytes)) throw new RowRoomNeeded(this.#used + bytes);
    this.#used += bytes;
  }

  #free(bytes: number): void {
    if (bytes === 0) return;
    const options = this.#bound();
    if (bytes > this.#used) throw new RangeError("VALUES row room underflow");
    (this.#rowGrant ?? options.grant).free(bytes);
    this.#used -= bytes;
  }

  #bound(): RowHostOptions {
    if (this.#options === null) throw new TypeError("VALUES row host is not bound");
    return this.#options;
  }

  /** Binds the executing operator's grant (compilation sizing never takes row bytes). */
  bind(options: RowHostOptions): void {
    this.#options = options;
  }

  memory(node: JsonNode): JsonMemory {
    this.#nodes.push(node);
    this.#requests.set(node, new RowRequest(node));
    this.#dependencies.set(node, new Set());
    return this.#memory;
  }

  /** Forgets the previous row's compiled nodes: each row compiles its cells afresh on its own
   * arena (exec/plan.ts), so the admitted minimum is one row's, not the statement's. */
  beginRow(): void {
    if (this.#borrowed) throw new RowBorrowInvariantError();
    if (this.#used !== 0) throw new Error("VALUES row bytes leaked past their row");
    this.#nodes.length = 0;
    this.#requests.clear();
    this.#dependencies.clear();
    this.#users.clear();
  }

  evaluate(node: JsonNode): Vector {
    if (this.#completed.get(node) === this.#epoch) {
      const value = this.#values.get(node);
      if (value === undefined) throw new Error("Missing VALUES row value");
      return value;
    }
    const request = this.#requests.get(node);
    if (request === undefined) throw new Error("Unregistered VALUES row kernel");
    throw request;
  }

  /** Bytes of row room the current row holds. */
  get usedBytes(): number {
    return this.#used;
  }

  /**
   * Ends the previous row's borrow: called at the start of `next()`, when the consumer has
   * finished with the row it was given. Frees that row's bytes; the row room is kept.
   */
  endBorrow(): void {
    this.#releaseRow();
    this.#borrowed = false;
  }

  /** Evaluates one row's cells, growing row room by measured retries. The row stays borrowed
   * until the next `endBorrow()`. */
  async row(cells: readonly Compiled[], batch: Batch, signal: AbortSignal): Promise<Vector[]> {
    if (this.#borrowed) throw new RowBorrowInvariantError();
    for (;;) {
      try {
        const columns: Vector[] = [];
        for (const cell of cells) columns.push(await this.#cell(cell, batch, signal));
        this.#borrowed = true;
        return columns;
      } catch (error) {
        if (!(error instanceof RowRoomNeeded)) throw error;
        this.#releaseRow();
        await this.#grow(error.need);
      }
    }
  }

  /** Closes the row grant. Idempotent; the runtime also closes it at disposal. */
  close(): void {
    this.#releaseRow();
    this.#rowGrant?.close();
    this.#rowGrant = null;
  }

  async #grow(need: number): Promise<void> {
    const options = this.#bound();
    if (this.#borrowed) throw new RowBorrowInvariantError();
    if (this.#used !== 0) throw new Error("VALUES row room grew with row bytes still held");
    // The admission rule's usable capacity: the cap less the progress reserve. The governor
    // refuses above its own (also less permanent holdings) with the same 53200.
    const usable = options.governor.capBytes - options.governor.reserveBytes;
    if (need > usable) throw new MemoryAdmissionError(`${options.label}/row`, need, usable);
    // Nothing of this row is held while we wait: the old row grant is empty, so close it
    // before asking for the larger one.
    this.#rowGrant?.close();
    this.#rowGrant = null;
    options.signal.throwIfAborted();
    this.growths++;
    const grant = await options.admitRow(need);
    if (options.signal.aborted) {
      grant.close();
      options.signal.throwIfAborted();
    }
    this.#rowGrant = grant;
  }

  async #cell(cell: Compiled, batch: Batch, signal: AbortSignal): Promise<Vector> {
    for (;;) {
      try {
        return cell.evaluate(batch, null);
      } catch (error) {
        if (!(error instanceof RowRequest)) throw error;
        await this.#compute(error, batch, signal);
      }
    }
  }

  async #compute(request: RowRequest, batch: Batch, signal: AbortSignal): Promise<void> {
    for (;;) {
      signal.throwIfAborted();
      if (this.slice.due()) {
        await yieldTask();
        afterYield(this.slice.clock);
        signal.throwIfAborted();
      }
      try {
        const job = request.node.run(batch, null);
        let value: Vector;
        try {
          value = await runSliced(job, this.slice, () => signal.throwIfAborted());
        } catch (error) {
          request.node.discard(job);
          throw error;
        }
        this.#completed.set(request.node, this.#epoch);
        this.#values.set(request.node, value);
        this.#retireInputs(request.node);
        return;
      } catch (error) {
        if (!(error instanceof RowRequest)) throw error;
        const dependencies = this.#dependencies.get(request.node);
        if (dependencies === undefined) throw new Error("Unregistered VALUES row kernel");
        if (!dependencies.has(error.node)) {
          dependencies.add(error.node);
          this.#users.set(error.node, (this.#users.get(error.node) ?? 0) + 1);
        }
        await this.#compute(error, batch, signal);
      }
    }
  }

  /** A completed node owns a copy of what it read: children no other node still needs give
   * their bytes back now, so a chain of concatenations holds two values, not all of them. */
  #retireInputs(node: JsonNode): void {
    const dependencies = this.#dependencies.get(node);
    if (dependencies === undefined) return;
    for (const child of dependencies) {
      const users = (this.#users.get(child) ?? 0) - 1;
      if (users < 0) throw new Error("VALUES row dependency underflow");
      this.#users.set(child, users);
      if (users === 0) {
        child.reset();
        this.#completed.delete(child);
        this.#values.delete(child);
      }
    }
    dependencies.clear();
  }

  #releaseRow(): void {
    for (const node of this.#nodes) node.reset();
    this.#values.clear();
    this.#completed.clear();
    for (const dependencies of this.#dependencies.values()) dependencies.clear();
    this.#users.clear();
    this.#epoch++;
    if (this.#used !== 0) throw new Error("VALUES row bytes leaked past their row");
  }
}
