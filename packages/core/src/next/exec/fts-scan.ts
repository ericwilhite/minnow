/** Scan fallback: accumulate exact corpus counts, spill raw columns, then score a bounded frame.
 * No document/token set survives a cell; df is a per-document OR, never a sum over columns.
 */
import {
  FtsStatsAccumulator,
  bm25DocumentScore,
  tokenize,
  termMatches,
  type FtsQueryTerm,
} from "../full-text.js";
import type { FtsScanCall } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import { BATCH_ROWS, type Batch, type Vector } from "../vector/vector.js";
import { compileExpr, type CompileContext } from "./kernels/compile.js";
import { ftsWorkspace, queryTerms, queryText } from "./kernels/fts.js";
import { countIn, rowsIn } from "./kernels/vectors.js";
import { ftsValue, FTS_WORKSPACE_BYTES } from "./fts-value.js";
import { pushAndWait, type Operator, type PushResult, type Transfer } from "./pipeline.js";
import { SortOperator, sortMinimumBytes, type SortContext } from "./sort.js";
import { Slice, runSliced, type SlicedJob } from "./slices.js";

const ORDINAL: PhysicalType = { kind: "int64" };
const EMPTY: Batch = { length: 1, columns: [], selection: null };
export function ftsScanMinimum(
  types: readonly PhysicalType[],
  calls: number,
  workspace = FTS_WORKSPACE_BYTES,
): number {
  return (
    sortMinimumBytes([...types, ORDINAL]) +
    workspace +
    BATCH_ROWS * 9 +
    calls * (BATCH_ROWS * 9 + 8192)
  );
}

interface CorpusCall {
  columns: readonly number[];
  terms: readonly FtsQueryTerm[];
  counts: FtsStatsAccumulator;
}

export class FtsScanOperator implements Operator {
  readonly #sort: SortOperator;
  readonly #slice: Slice;
  readonly #calls: readonly CorpusCall[];
  readonly #outputs;
  readonly #ordinal;
  #nextOrdinal = 0;
  readonly #jobs = new Set<Promise<void>>();
  #pending: Promise<void> = Promise.resolve();
  #aborted = false;
  #reason: unknown;
  #closing: Promise<void> | undefined;
  constructor(
    types: readonly PhysicalType[],
    calls: readonly FtsScanCall[],
    context: CompileContext,
    readonly options: SortContext,
    readonly downstream: Operator,
  ) {
    this.#slice = new Slice(4, options.clock);
    ftsWorkspace(context.arena);
    context.arena.take(calls.length * 8192);
    this.#calls = calls.map((call) => {
      const terms = queryTerms(queryText(compileExpr(call.query, context).evaluate(EMPTY, null)));
      return { columns: call.columns, terms, counts: new FtsStatsAccumulator(terms) };
    });
    this.#outputs = calls.map(() => context.arena.flat({ kind: "float64" }));
    this.#ordinal = context.arena.flat(ORDINAL).vector;
    const emitter: Operator = {
      push: (batch) => {
        this.#pending = this.#track(this.#emit(batch));
        return "blocked";
      },
      ready: () => this.#pending,
      finish: () => downstream.finish(),
      abort: (reason) => downstream.abort(reason),
    };
    // The sorter owns its buffers, not this enclosing arena/corpus workspace. Keep the
    // enclosing account alive until its document jobs and downstream borrows have drained.
    const grant = new Proxy(options.grant, {
      get(target, key) {
        if (key === "close") return () => undefined;
        const value: unknown = Reflect.get(target, key, target);
        return typeof value === "function"
          ? (value as (...args: unknown[]) => unknown).bind(target)
          : value;
      },
    });
    this.#sort = new SortOperator(
      {
        types: [...types, ORDINAL],
        keys: [{ column: types.length, direction: "asc", nulls: "last", collation: null }],
        limit: null,
        offset: 0,
        withTies: false,
      },
      { ...options, grant },
      emitter,
    );
  }
  #check(): void {
    if (this.#aborted) throw this.#reason;
    if (this.options.signal?.aborted) throw this.options.signal.reason;
  }
  #track(job: Promise<void>): Promise<void> {
    this.#jobs.add(job);
    void job.finally(() => this.#jobs.delete(job)).catch(() => undefined);
    return job;
  }
  *#document(
    batch: Batch,
    row: number,
    call: CorpusCall,
  ): SlicedJob<{ length: number; frequencies: number[]; any: boolean }> {
    let length = 0,
      any = false;
    const frequencies = new Array<number>(call.terms.length).fill(0);
    for (const column of call.columns) {
      const input = batch.columns[column];
      if (!input) throw new RangeError("FTS input column vanished");
      const text = ftsValue(input, row);
      if (text !== undefined) {
        any = true;
        const tokens = tokenize(text);
        length += tokens.length;
        for (let t = 0; t < tokens.length; t++) {
          const token = tokens[t];
          if (token === undefined) throw new Error("FTS token vanished");
          for (let term = 0; term < call.terms.length; term++) {
            const query = call.terms[term];
            if (!query) throw new Error("FTS query term vanished");
            if (termMatches(token, query)) frequencies[term] = (frequencies[term] ?? 0) + 1;
          }
          if ((t & 63) === 0 && this.#slice.due()) yield;
        }
      }
      if (this.#slice.due()) yield;
    }
    return { length, frequencies, any };
  }
  *#collectRows(batch: Batch): SlicedJob<void> {
    const rows = rowsIn(batch.selection),
      count = countIn(batch.selection, batch.length);
    for (let i = 0; i < count; i++)
      for (const call of this.#calls) {
        const doc = yield* this.#document(batch, rows[i] ?? 0, call);
        let mask = 0;
        for (let term = 0; term < doc.frequencies.length; term++)
          if (doc.frequencies[term]) mask |= 1 << term;
        call.counts.addDocumentCounts(mask, doc.length);
      }
  }
  async #collect(batch: Batch): Promise<void> {
    await runSliced(this.#collectRows(batch), this.#slice, () => this.#check());
    this.#check();
    const ordinal = this.#ordinal;
    if (ordinal.data.layout !== "int64" || ordinal.data.values.repr !== "safe")
      throw new Error("FTS ordinal layout");
    const rows = rowsIn(batch.selection),
      count = countIn(batch.selection, batch.length);
    for (let k = 0; k < count; k++) {
      if (this.#nextOrdinal > Number.MAX_SAFE_INTEGER)
        throw new RangeError("FTS corpus row count overflow");
      ordinal.data.values.values[rows[k] ?? 0] = this.#nextOrdinal++;
    }
    ordinal.length = batch.length;
    if (this.#sort.push({ ...batch, columns: [...batch.columns, ordinal] }) === "blocked")
      await this.#sort.ready();
  }
  *#emitRows(batch: Batch): SlicedJob<void> {
    for (let c = 0; c < this.#calls.length; c++) {
      const call = this.#calls[c],
        out = this.#outputs[c];
      if (!call || !out) throw new Error("FTS emit buffer vanished");
      out.validity.fill(0);
      out.vector.validity = out.validity;
      out.vector.length = batch.length;
      if (out.vector.data.layout !== "f64") throw new TypeError("Expected BM25 output");
      for (let row = 0; row < batch.length; row++) {
        const doc = yield* this.#document(batch, row, call);
        if (doc.any) {
          out.validity[row >>> 5] = (out.validity[row >>> 5] ?? 0) | (1 << (row & 31));
          out.vector.data.values[row] = bm25DocumentScore(
            doc.frequencies,
            doc.length,
            call.counts.stats,
          );
        }
      }
    }
  }
  async #emit(batch: Batch): Promise<void> {
    await runSliced(this.#emitRows(batch), this.#slice, () => this.#check());
    this.#check();
    const columns: Vector[] = [
      ...batch.columns.slice(0, -1),
      ...this.#outputs.map((out) => out.vector),
    ];
    await pushAndWait(this.downstream, { ...batch, columns });
    this.#check();
  }
  push(batch: Batch, transfer?: Transfer): PushResult {
    this.#check();
    this.#pending = this.#track(this.#collect(batch).finally(() => transfer?.release()));
    return "blocked";
  }
  ready(): Promise<void> {
    return this.#pending;
  }
  async finish(): Promise<void> {
    await this.#pending;
    this.#check();
    await this.#sort.finish();
  }
  get profile() {
    return this.#sort.profile;
  }
  get closed(): Promise<void> {
    return this.#closing ?? Promise.all([...this.#jobs, this.#sort.closed]).then(() => undefined);
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    this.downstream.abort(reason);
    this.#closing = Promise.allSettled([...this.#jobs]).then(async () => {
      this.#sort.abort(reason);
      await this.#sort.closed;
    });
  }
}
