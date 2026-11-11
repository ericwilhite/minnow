/** Expanding JSON retires producer credit before a larger atomic admission.
 * Prefix output is file-backed until this pass completes; no ancestor sees replayed rows. */
import type { MaybePromise } from "../access/key-index.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import type { SpillService } from "../spill/contract.js";
import type { Grant } from "../memory/governor.js";
import type { Transfer, Operator, PushResult } from "./pipeline.js";
import type { Batch, Selection } from "../vector/vector.js";
import { afterYield, yieldTask, YIELD_INTERVAL_MS } from "./pipeline.js";
import { Slice } from "./slices.js";
import type { CompileContext } from "./kernels/compile.js";
import { JsonExpressions } from "./json-expressions.js";
import { JsonWorkspaceGrowth } from "./json-workspace.js";
import { ExecutionError } from "./errors.js";
import { JsonRecords } from "./json-records.js";
import { ParameterCheckpoint } from "./parameter-checkpoint.js";
import { TapeDirectory } from "./agg/hash-tape.js";
import { SpillFiles, PageWriter } from "./spill-pages.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";

export const JSON_PHASE_IO = 512 << 10;
export const JSON_PHASE_FIXED = 4096;
export interface JsonPhase {
  readonly emit: boolean;
  readonly error: ExecutionError | null;
  readonly prefix: JsonRecords;
  readonly suffix: JsonRecords;
  readonly parameters: ParameterCheckpoint;
  readonly files: SpillFiles;
  readonly directory: TapeDirectory;
  readonly capacity: number;
  readonly retainedBytes: number;
  readonly progressBytes: number;
  readonly writtenAtHandoff: number;
}
export async function closeJsonPhase(saved: JsonPhase): Promise<void> {
  const results = await Promise.allSettled([
    saved.prefix.close(),
    saved.suffix.close(),
    saved.files.closeAll(),
  ]);
  const failures = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
  if (failures.length)
    throw new AggregateError(
      failures.map((r) => r.reason as unknown),
      "JSON phase files failed to close",
    );
}
export interface JsonPhaseContext {
  readonly grant: Grant;
  readonly spill: SpillService;
  readonly queryId: number;
  readonly capacity: number;
  readonly retainedBytes: number;
  readonly progressBytes: number;
  readonly signal: AbortSignal;
  readonly resume?: JsonPhase;
  readonly rowsNeeded?: () => number;
  readonly stopped?: () => boolean;
  readonly handoff: (saved: JsonPhase) => Promise<void>;
  readonly dispose: (release: () => MaybePromise<void>) => void;
}
export class JsonPhaseOperator implements Operator {
  readonly #controller = new AbortController();
  readonly #slice = new Slice(YIELD_INTERVAL_MS);
  readonly #read: SpillIo;
  readonly #write: SpillIo;
  readonly #files: SpillFiles;
  readonly #directory: TapeDirectory;
  readonly #prefix: JsonRecords;
  readonly #suffix: JsonRecords;
  readonly #evaluator: JsonExpressions | null;
  readonly #one: Selection;
  readonly #parentAbort: () => void;
  #required = 0;
  #produced = 0;
  #failedRow = false;
  #rowError: ExecutionError | null = null;
  #pending: Promise<void> | null = null;
  #restore: Promise<void> | null = null;
  #restoreIo: Promise<void> | null = null;
  #finishWork: Promise<void> | null = null;
  #finished: Promise<void> | null = null;
  #cleanup: Promise<void> | null = null;
  #transferred = false;
  #constructed = false;
  readonly #constructionCleanup: Array<() => MaybePromise<void>> = [];
  #aborted = false;
  #reason: unknown;
  constructor(
    exprs: readonly ScalarExpr[] | ScalarExpr,
    readonly context: CompileContext,
    readonly shared: JsonPhaseContext,
    output: readonly PhysicalType[],
    readonly downstream: Operator,
  ) {
    context.arena.take(JSON_PHASE_FIXED);
    this.#one = { rows: new Uint16Array(1), count: 1 };
    shared.dispose(() => this.#close());
    this.#rowError = shared.resume?.error ?? null;
    this.#produced = shared.resume?.prefix.rowCount ?? 0;
    context.arena.take(shared.retainedBytes);
    this.#files = new SpillFiles(shared.spill, shared.queryId, {
      allocate: () => false,
      free: () => {
        throw new Error("Unbuffered JSON checkpoint freed write cache");
      },
    });
    this.#constructionCleanup.push(() => this.#files.closeAll());
    const create = () => {
      const grant = shared.grant;
      const pool = new Pool(new Charge(grant), JSON_PHASE_IO);
      this.#constructionCleanup.push(() => {
        pool.settle();
        pool.close();
      });
      const pages = new PageWriter(pool, 16384);
      this.#constructionCleanup.push(() => pages.close());
      return new SpillIo(
        pool,
        this.#files,
        pages,
        { grant, queryId: shared.queryId, signal: this.#controller.signal },
        () => this.#check(),
        () => this.#pace(),
      );
    };
    this.#read = create();
    this.#write = create();
    this.#directory = new TapeDirectory(this.#write, this.#write.pool);
    this.#constructionCleanup.push(() => this.#directory.detach());
    this.#prefix = new JsonRecords(this.#write, shared.spill, output);
    this.#constructionCleanup.push(() => this.#prefix.close());
    this.#suffix = new JsonRecords(this.#write, shared.spill, context.input);
    this.#constructionCleanup.push(() => this.#suffix.close());
    const collector: Operator = {
      push: (batch) => {
        this.#pendingOutput = this.#collect(batch);
        return "blocked";
      },
      ready: () => this.#pendingOutput ?? Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    this.#evaluator = shared.resume?.emit
      ? null
      : new JsonExpressions(
          exprs,
          context,
          { bytes: shared.capacity, expanding: true, signal: this.#controller.signal },
          collector,
        );
    this.#parentAbort = () => this.abort(shared.signal.reason);
    shared.signal.addEventListener("abort", this.#parentAbort, { once: true });
    this.#constructed = true;
    this.#constructionCleanup.length = 0;
    if (shared.signal.aborted) this.#parentAbort();
  }
  #pendingOutput: Promise<void> | null = null;
  async #collect(batch: Batch): Promise<void> {
    const n = batch.selection?.count ?? batch.length;
    for (let i = 0; i < n; i++) {
      await this.#prefix.write(batch.columns, batch.selection?.rows[i] ?? i);
      this.#produced++;
    }
  }
  #check(): void {
    if (this.#aborted) throw this.#reason;
    this.shared.signal.throwIfAborted();
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
  async #rows(batch: Batch): Promise<void> {
    const n = batch.selection?.count ?? batch.length;
    for (let i = 0; i < n; i++) {
      this.#check();
      if (this.stopped) break;
      const row = batch.selection?.rows[i] ?? i;
      if (!this.#required) {
        this.#one.rows[0] = row;
        const evaluator = this.#evaluator;
        if (!evaluator) throw new Error("JSON delivery cannot consume source rows");
        try {
          evaluator.push({ ...batch, selection: this.#one });
          await evaluator.ready();
        } catch (e) {
          if (!(e instanceof JsonWorkspaceGrowth)) {
            if (!this.deferRowError(e)) throw e;
            break;
          }
          this.#required = e.required;
          evaluator.abort(e);
          await evaluator.closed;
        }
      }
      if (this.#required) await this.#suffix.write(batch.columns, row);
      const pause = this.#pace();
      if (pause) await pause;
    }
  }
  /** Only row data exceptions are delayed to their stream position. Cancellation, storage
   * faults, unsupported plans and admission failures always unwind immediately. */
  deferRowError(error: unknown): boolean {
    if (!(error instanceof ExecutionError) || !error.sqlState.startsWith("22")) return false;
    if (this.#failedRow) return true;
    this.#rowError = new ExecutionError(error.message, error.code, error.sqlState);
    this.#failedRow = true;
    this.#evaluator?.abort(error);
    return true;
  }
  get stopped(): boolean {
    return (
      this.shared.stopped?.() === true ||
      (!this.shared.resume?.emit &&
        (this.#failedRow || this.#produced >= (this.shared.rowsNeeded?.() ?? Infinity)))
    );
  }
  push(batch: Batch, transfer?: Transfer): PushResult {
    this.#pending = this.#rows(batch).finally(() => transfer?.release());
    return "blocked";
  }
  ready(): Promise<void> {
    return this.#pending ?? this.#restore ?? Promise.resolve();
  }
  /** The recipe calls this instead of reopening the consumed input subtree. */
  resume(): Promise<void> {
    const saved = this.shared.resume;
    if (!saved) throw new Error("Missing saved JSON continuation");
    this.#restoreIo = (async () => {
      if (saved.emit) {
        await saved.prefix.read(this.#read, this.shared.retainedBytes, async (columns, length) => {
          if (this.shared.stopped?.()) return;
          if (this.downstream.push({ columns, length, selection: null }) === "blocked")
            await this.downstream.ready();
        });
      } else {
        await saved.prefix.copyTo(this.#prefix, this.#read);
        await saved.suffix.read(this.#read, this.shared.retainedBytes, async (columns, length) =>
          this.#rows({ columns, length, selection: null }),
        );
      }
      await closeJsonPhase(saved);
    })();
    return (this.#restore = this.#restoreIo.then(() => this.finish()));
  }
  finish(): Promise<void> {
    if (this.#finished) return this.#finished;
    this.#finishWork = this.#finish();
    return (this.#finished = this.#finishWork.then(
      () => this.#close(),
      async (e: unknown) => {
        this.abort(e);
        await this.#close().catch(() => undefined);
        throw e;
      },
    ));
  }
  async #finish(): Promise<void> {
    await this.#pending;
    this.#check();
    if (this.shared.resume?.emit) {
      if (this.#rowError && !this.shared.stopped?.()) throw this.#rowError;
      await this.downstream.finish();
      return;
    }
    if (!this.#required && !this.#failedRow) await this.#evaluator?.finish();
    else await this.#evaluator?.closed;
    await this.#prefix.seal();
    await this.#suffix.seal();
    const parameters = new ParameterCheckpoint(this.#directory);
    await parameters.save(this.context.params.slots);
    this.#directory.detach();
    const emit = !this.#required;
    const retainedBytes = (emit ? this.#prefix : this.#suffix).retainedBytes;
    const saved: JsonPhase = {
      emit,
      error: this.#rowError,
      prefix: this.#prefix,
      suffix: this.#suffix,
      parameters,
      files: this.#files,
      directory: this.#directory,
      capacity: this.#required,
      retainedBytes,
      progressBytes: Math.max(this.shared.progressBytes, emit ? 4 * retainedBytes : 0),
      writtenAtHandoff:
        this.#files.written + this.#prefix.files.written + this.#suffix.files.written,
    };
    await this.shared.handoff(saved);
    this.#transferred = true;
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    this.#controller.abort(reason);
    this.#evaluator?.abort(reason);
    this.downstream.abort(reason);
    void this.#close().catch(() => undefined);
  }
  #close(): Promise<void> {
    return (this.#cleanup ??= (async () => {
      if (!this.#constructed) {
        const failures: unknown[] = [];
        for (const release of this.#constructionCleanup.splice(0).reverse()) {
          try {
            await release();
          } catch (error) {
            failures.push(error);
          }
        }
        if (failures.length) throw new AggregateError(failures, "JSON constructor cleanup failed");
        return;
      }
      await this.#pending?.catch(() => undefined);
      await this.#restoreIo?.catch(() => undefined);
      await this.#finishWork?.catch(() => undefined);
      // resume() can be awaiting finish(), so cleanup never awaits its own restore wrapper.
      await this.#evaluator?.closed;
      const errors: unknown[] = [];
      const closing = await Promise.allSettled(
        this.#transferred
          ? []
          : [this.#prefix.close(), this.#suffix.close(), this.#files.closeAll()],
      );
      for (const r of closing) if (r.status === "rejected") errors.push(r.reason);
      for (const io of [this.#read, this.#write]) {
        try {
          io.pages.close();
          io.pool.settle();
          io.pool.close();
        } catch (e) {
          errors.push(e);
        }
      }
      try {
        this.#directory.detach();
      } catch (e) {
        errors.push(e);
      }
      this.shared.signal.removeEventListener("abort", this.#parentAbort);
      if (errors.length) throw new AggregateError(errors, "JSON checkpoint cleanup failed");
    })());
  }
  get spilledBytes(): number {
    const saved = this.shared.resume;
    // HashTape can append FIFO directory pages when a detached tape is first read.
    // Attribute those late writes to this restore, including parameter restore before construction.
    const restored = saved
      ? saved.files.written +
        saved.prefix.files.written +
        saved.suffix.files.written -
        saved.writtenAtHandoff
      : 0;
    return this.#files.written + this.#prefix.files.written + this.#suffix.files.written + restored;
  }
  get spillPasses(): number {
    const saved = this.shared.resume;
    return saved &&
      saved.files.written + saved.prefix.files.written + saved.suffix.files.written > 0
      ? 1
      : 0;
  }
  get closed(): Promise<void> {
    return this.#cleanup ?? Promise.resolve();
  }
}
