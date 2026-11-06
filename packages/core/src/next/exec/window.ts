/**
 * The window operator (B7 increment 3): `window` rels with PostgreSQL's frames (ROWS / RANGE /
 * GROUPS with offsets, EXCLUDE), grant-or-spill like the sort and the join, sliced per R23.
 *
 * - **Order.** Rows are sorted by (PARTITION BY, ORDER BY) with the sort operator (sort.ts:
 *   its runs, merges, slices and abort checks), over the same grant (the evaluator closes it).
 * - **Partitions.** The evaluator takes the sorted rows one partition at a time. Its rows are
 *   held in a column store; a partition larger than the grant spills them to pages in order,
 *   so only the function arguments stay resident (a column store of the argument columns, and
 *   one per value function for its gathers). At the partition's end, peers, frame bounds and
 *   every function are computed over typed arrays (window-frames.ts), then the rows go out in
 *   batches: the held columns (pages, then the resident chunks, as views) and one column per
 *   function.
 * - **Memory.** Every array is charged: the per-partition scratch grows to the largest
 *   partition and is reused; an argument store that does not fit borrows one progress loan
 *   (rule 5) for its partition.
 * - **Slices (R23).** Arrival, the per-partition kernels and emission are sliced jobs: a task
 *   yield when the slice is due, the abort check at every resumption and after backpressure.
 *
 * Supported: row_number, rank, dense_rank, percent_rank, cume_dist, ntile; lag, lead (offset a
 * constant or a column; default a constant), first_value, last_value, nth_value over any type;
 * count, count_star, and sum / avg / min / max of integers and floats (min / max also dates and
 * timestamps). RANGE offsets need one numeric ORDER BY key. Decimal and interval arguments are
 * refused (TypeError), as is a lag/lead default that is a column.
 */
import type { Grant, Loan } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import { setInt128 } from "../vector/int128.js";
import type { Batch, FlatVector, Vector } from "../vector/vector.js";
import { BATCH_ROWS } from "../vector/vector.js";
import { ColumnStore, nullConstant } from "./column-store.js";
import {
  YIELD_INTERVAL_MS,
  isPromise,
  type Operator,
  type PushResult,
  type Transfer,
} from "./pipeline.js";
import { keysEqual, type SortColumn } from "./sort-keys.js";
import { PartitionStream, streamable, type StreamHost } from "./window-stream.js";
import { SortOperator, sortMinimumBytes, type SortContext, type SortProfile } from "./sort.js";
import { Slice, runSliced, type SlicedJob } from "./slices.js";
import { PageWriter, SpillFiles, SpilledRun } from "./spill-pages.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";
import {
  DEFAULT_ROW,
  NULL_ROW,
  aggregate,
  fillPeers,
  frameBounds,
  ranking,
  values as valueRows,
  type AggregateKind,
  type Frame,
  type NumberArg,
  type NumberResult,
  type Peers,
  type RangeKey,
  type RankingKind,
  type ValueKind,
} from "./window-frames.js";

type MaybePromise<T> = T | Promise<T>;

export interface WindowFunctionSpec {
  /** The IR's function name (`WindowCall.fn`). */
  readonly fn: string;
  /** The argument's input column (null for row_number, count_star and the ranking functions). */
  readonly arg: number | null;
  /** lag/lead's offset (default 1), ntile's buckets, nth_value's n: a constant or a column. */
  readonly amount?: number | { readonly column: number } | null;
  /** lag/lead's default: a one-row vector of the argument's type; null for NULL. */
  readonly fallback?: Vector | null;
  /** Null: PostgreSQL's default frame. */
  readonly frame: Frame | null;
  readonly type: PhysicalType;
}

export interface WindowSpec {
  readonly types: readonly PhysicalType[];
  readonly partitionBy: readonly number[];
  readonly orderBy: readonly SortColumn[];
  readonly functions: readonly WindowFunctionSpec[];
  /**
   * Rows past which a partition streams (bounded by its frames, not its size: window-stream.ts)
   * instead of being held whole. Default `STREAM_ROWS`; tests lower it.
   */
  readonly streamRows?: number;
}

/** Partitions up to this many rows are evaluated resident; larger ones stream. */
export const STREAM_ROWS = 65_536;

export interface WindowProfile {
  rowsIn: number;
  rowsOut: number;
  partitions: number;
  largestPartition: number;
  /** Partitions whose rows spilled while they were gathered. */
  spilledPartitions: number;
  /** Partitions evaluated as a stream (past `streamRows`). */
  streamedPartitions: number;
  spilledBytes: number;
  peakHeldBytes: number;
  loans: number;
  sort: SortProfile;
}

const POOL_BYTES = 1024 * 1024;
const RANKING: ReadonlySet<string> = new Set([
  "row_number",
  "rank",
  "dense_rank",
  "percent_rank",
  "cume_dist",
  "ntile",
]);
const VALUE: ReadonlySet<string> = new Set([
  "lag",
  "lead",
  "first_value",
  "last_value",
  "nth_value",
]);
const AGGREGATE: ReadonlySet<string> = new Set(["count", "count_star", "sum", "avg", "min", "max"]);

/** Bytes a window operator needs admitted before it runs (the sort's, then its own). */
export function windowMinimumBytes(types: readonly PhysicalType[], functions = 1): number {
  let row = 16;
  for (const type of types) row += type.kind === "text" || type.kind === "bytes" ? 24 : 17;
  return (
    sortMinimumBytes(types) +
    POOL_BYTES +
    2 * 2048 * row +
    16 * 1024 * (types.length + 2) +
    functions * (BATCH_ROWS * 24 + 32 * 1024) +
    // A streaming partition's working set for a bounded frame: its ring (4,096 rows), a few
    // pages in flight and the output chunk (window-stream.ts).
    functions * 4096 * 48 +
    4096 * 16 +
    1024 * 1024 +
    64 * 1024
  );
}

function numeric(type: PhysicalType): boolean {
  return (
    type.kind === "int32" ||
    type.kind === "int64" ||
    type.kind === "float32" ||
    type.kind === "float64"
  );
}

function integral(type: PhysicalType): boolean {
  return type.kind === "int32" || type.kind === "int64";
}

/** Refuses what the evaluator does not support (lowering produced it; see the module comment). */
function check(spec: WindowSpec): void {
  for (const call of spec.functions) {
    const known = RANKING.has(call.fn) || VALUE.has(call.fn) || AGGREGATE.has(call.fn);
    if (!known) throw new TypeError(`Window function ${call.fn} is not supported`);
    const arg = call.arg === null ? null : spec.types[call.arg];
    if (call.arg !== null && arg === undefined)
      throw new RangeError("Window argument out of range");
    if (AGGREGATE.has(call.fn) && call.fn !== "count_star" && call.fn !== "count") {
      const ok =
        arg !== undefined &&
        arg !== null &&
        (numeric(arg) ||
          ((call.fn === "min" || call.fn === "max") &&
            (arg.kind === "date" || arg.kind === "timestamp" || arg.kind === "time")));
      if (!ok)
        throw new TypeError(
          `Window ${call.fn} over ${arg?.kind ?? "nothing"} is not supported yet`,
        );
    }
    if (VALUE.has(call.fn) && call.arg === null)
      throw new TypeError(`Window ${call.fn} needs an argument`);
    const amount = call.amount;
    if (amount !== undefined && amount !== null && typeof amount !== "number") {
      const type = spec.types[amount.column];
      if (type === undefined || !integral(type))
        throw new TypeError(`Window ${call.fn}'s amount must be an integer column`);
    }
    if (call.frame?.unit === "range") {
      const offsets =
        call.frame.start.kind === "preceding" ||
        call.frame.start.kind === "following" ||
        call.frame.end.kind === "preceding" ||
        call.frame.end.kind === "following";
      const key = spec.orderBy[0];
      const type = key === undefined ? undefined : spec.types[key.column];
      if (offsets && (spec.orderBy.length !== 1 || type === undefined || !rangeable(type)))
        throw new TypeError("RANGE with an offset needs one numeric ORDER BY key");
    }
  }
}

function rangeable(type: PhysicalType): boolean {
  return numeric(type) || type.kind === "date";
}

/** A grant whose `close` is the evaluator's (the sort finishes first and must not close it). */
function deferClose(grant: Grant): Grant {
  return new Proxy(grant, {
    get(target, property) {
      if (property === "close") return () => undefined;
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function"
        ? (value as (...args: unknown[]) => unknown).bind(target)
        : value;
    },
  });
}

/** `ROW_NUMBER() / RANK() / … OVER (PARTITION BY … ORDER BY … frame)` (see the module comment). */
export class WindowOperator implements Operator {
  readonly spec: WindowSpec;
  readonly #sort: SortOperator;
  readonly #evaluator: Evaluator;

  constructor(spec: WindowSpec, context: SortContext, downstream: Operator) {
    check(spec);
    this.spec = spec;
    const grant = deferClose(context.grant);
    const shared: SortContext = { ...context, grant };
    this.#evaluator = new Evaluator(spec, shared, context.grant, downstream);
    const keys: SortColumn[] = [
      ...spec.partitionBy.map((column): SortColumn => ({
        column,
        direction: "asc",
        nulls: "last",
        collation: null,
      })),
      ...spec.orderBy,
    ];
    this.#sort = new SortOperator(
      { types: spec.types, keys, limit: null, offset: 0, withTies: false },
      shared,
      this.#evaluator,
    );
  }

  get profile(): WindowProfile {
    const profile = this.#evaluator.profile;
    profile.sort = this.#sort.profile;
    return profile;
  }

  get closed(): Promise<void> {
    return Promise.all([this.#sort.closed, this.#evaluator.closed]).then(() => undefined);
  }

  push(batch: Batch, transfer?: Transfer): PushResult {
    try {
      return this.#sort.push(batch, transfer);
    } catch (error) {
      this.abort(error);
      throw error;
    }
  }

  ready(): Promise<void> {
    return this.#sort.ready().catch((error: unknown) => {
      this.abort(error);
      throw error;
    });
  }

  finish(): MaybePromise<void> {
    let result: MaybePromise<void>;
    try {
      result = this.#sort.finish();
    } catch (error) {
      this.abort(error);
      throw error;
    }
    if (!isPromise(result)) return;
    return result.catch((error: unknown) => {
      this.abort(error);
      throw error;
    });
  }

  abort(reason: unknown): void {
    this.#sort.abort(reason);
    this.#evaluator.abort(reason);
  }
}

/** One function's per-batch output buffers. */
interface Output {
  readonly f64: Float64Array;
  readonly f32: Float32Array;
  readonly i32: Int32Array;
  readonly big: BigInt64Array;
  readonly validity: Uint32Array;
  /** Value functions: the gathered values of one batch. */
  readonly gather: ColumnStore | null;
  readonly ids: Int32Array;
}

/**
 * The partition evaluator: the sort's downstream. Receives sorted rows, gathers one partition
 * at a time, computes its functions and pushes its rows with their results.
 */
class Evaluator implements Operator {
  readonly spec: WindowSpec;
  readonly charge: Charge;
  readonly pool: Pool;
  readonly files: SpillFiles;
  readonly pages: PageWriter;
  readonly io: SpillIo;
  readonly slice = new Slice(YIELD_INTERVAL_MS);
  readonly #partKeys: readonly SortColumn[];
  /** The current partition's rows not spilled (spilled ones are `#run`'s pages, before them). */
  readonly #held: ColumnStore;
  #run: SpilledRun | null = null;
  /** The current partition streams: its rows go to `#run` as they arrive (`#staged`). */
  #streaming = false;
  #staged: ColumnStore | null = null;
  /** Argument columns of the partition (numeric functions, amounts, the RANGE key). */
  readonly #argColumns: readonly number[];
  readonly #args: ColumnStore;
  /** Value functions: the argument column of the partition, then a NULL row and the default. */
  readonly #sources: ReadonlyArray<ColumnStore | null>;
  /** Key columns of the last row taken (partition and peer tests across batches). */
  readonly #last: ColumnStore;
  readonly #keyColumns: readonly number[];
  #lastVectors: Vector[] = [];
  #rows = 0;
  #newGroup = new Uint8Array(0);
  #scratch: Scratch | null = null;
  #scratchBytes = 0;
  #newGroupBytes = 0;
  readonly #outputs: Output[];
  #loan: Loan | null = null;
  /** Every function can stream: a partition the grant cannot hold streams instead of borrowing. */
  readonly #canStream: boolean;
  #pending: Promise<void> | null = null;
  #aborted: unknown = undefined;
  #closed = false;
  #closing: Promise<void> | null = null;
  readonly profile: WindowProfile = {
    rowsIn: 0,
    rowsOut: 0,
    partitions: 0,
    largestPartition: 0,
    spilledPartitions: 0,
    streamedPartitions: 0,
    spilledBytes: 0,
    peakHeldBytes: 0,
    loans: 0,
    sort: undefined as unknown as SortProfile,
  };

  constructor(
    spec: WindowSpec,
    readonly context: SortContext,
    readonly grant: Grant,
    readonly downstream: Operator,
  ) {
    this.spec = spec;
    this.#partKeys = spec.partitionBy.map((column): SortColumn => ({
      column,
      direction: "asc",
      nulls: "last",
      collation: null,
    }));
    this.charge = new Charge(context.grant);
    this.pool = new Pool(this.charge, POOL_BYTES);
    this.files = new SpillFiles(context.spill, context.queryId, this.charge);
    this.pages = new PageWriter(this.pool, 16 * 1024);
    this.io = new SpillIo(this.pool, this.files, this.pages, context, () => {
      this.checkAborted();
    });
    this.#held = new ColumnStore(spec.types, this.charge);
    this.#canStream = spec.functions.every((call) => streamable(spec, call) === null);
    const columns = new Set<number>();
    for (const call of spec.functions) {
      if (call.arg !== null && !VALUE.has(call.fn)) columns.add(call.arg);
      if (call.amount !== undefined && call.amount !== null && typeof call.amount !== "number")
        columns.add(call.amount.column);
    }
    const rangeKey = spec.orderBy[0];
    if (rangeKey !== undefined && spec.functions.some((call) => call.frame?.unit === "range"))
      columns.add(rangeKey.column);
    this.#argColumns = [...columns];
    this.#args = new ColumnStore(
      this.#argColumns.map((c) => spec.types[c] ?? { kind: "int32", bits: 32 }),
      this.charge,
    );
    this.#sources = spec.functions.map((call) =>
      VALUE.has(call.fn) && call.arg !== null
        ? new ColumnStore([spec.types[call.arg] ?? { kind: "int32", bits: 32 }], this.charge)
        : null,
    );
    this.#keyColumns = [
      ...new Set([...spec.partitionBy, ...spec.orderBy.map((key) => key.column)]),
    ];
    this.#last = new ColumnStore(
      this.#keyColumns.map((c) => spec.types[c] ?? { kind: "int32", bits: 32 }),
      this.pool,
      { chunkRows: 1, varlenBytes: 64 },
    );
    this.#outputs = spec.functions.map((call, f) => ({
      f64: new Float64Array(BATCH_ROWS),
      f32: new Float32Array(BATCH_ROWS),
      i32: new Int32Array(BATCH_ROWS),
      big: new BigInt64Array(BATCH_ROWS),
      validity: new Uint32Array(BATCH_ROWS >>> 5),
      gather:
        this.#sources[f] === null
          ? null
          : new ColumnStore([call.type], this.pool, { varlenBytes: 4096 }),
      ids: new Int32Array(BATCH_ROWS),
    }));
    if (!this.charge.allocate(spec.functions.length * BATCH_ROWS * 32))
      throw new RangeError("A window's minimum must be admitted");
  }

  checkAborted(): void {
    if (this.#aborted !== undefined)
      throw this.#aborted instanceof Error
        ? this.#aborted
        : new Error("Aborted", { cause: this.#aborted });
    this.context.signal?.throwIfAborted();
  }

  #sliced<T>(job: SlicedJob<T>): MaybePromise<T> {
    this.slice.reset();
    return runSliced(job, this.slice, () => {
      this.checkAborted();
    });
  }

  push(batch: Batch, transfer?: Transfer): PushResult {
    let result: MaybePromise<void>;
    try {
      this.checkAborted();
      result = this.#sliced(this.#take(batch));
    } catch (error) {
      this.abort(error);
      throw error;
    }
    if (!isPromise(result)) {
      transfer?.release();
      return "ready";
    }
    this.#pending = result.then(
      () => transfer?.release(),
      (error: unknown) => {
        transfer?.release();
        this.abort(error);
        throw error;
      },
    );
    return "blocked";
  }

  ready(): Promise<void> {
    const pending = this.#pending ?? Promise.resolve();
    this.#pending = null;
    return pending;
  }

  finish(): MaybePromise<void> {
    let result: MaybePromise<void>;
    try {
      this.checkAborted();
      result = this.#sliced(this.#flushPartition());
    } catch (error) {
      this.abort(error);
      throw error;
    }
    const done = async () => {
      this.close();
      await this.closed;
      await this.downstream.finish();
    };
    const fail = (error: unknown): never => {
      this.abort(error);
      throw error;
    };
    if (isPromise(result)) return result.then(done, fail);
    return done().catch(fail);
  }

  // --- arrival ---------------------------------------------------------------------------

  /** Takes a sorted batch: finds partition and peer boundaries, gathers, finishes partitions. */
  *#take(batch: Batch): SlicedJob<void> {
    const count = batch.selection === null ? batch.length : batch.selection.count;
    if (batch.selection !== null) throw new TypeError("The sort pushes dense batches");
    this.profile.rowsIn += count;
    const columns = batch.columns;
    const types = this.spec.types;
    let from = 0;
    for (let r = 0; r < count; r++) {
      if (this.#rows + (r - from) === 0) {
        this.#flag(0, 1);
        continue;
      }
      const prev = r === 0 ? this.#lastVectors : columns;
      const p = r === 0 ? 0 : r - 1;
      if (!keysEqual(types, this.#partKeys, prev, p, columns, r)) {
        yield* this.#gather(columns, from, r - from);
        from = r;
        yield* this.#flushPartition();
        this.#flag(0, 1);
        continue;
      }
      const peer = keysEqual(types, this.spec.orderBy, prev, p, columns, r);
      this.#flag(this.#rows + (r - from), peer ? 0 : 1);
      if ((r & 255) === 255 && this.slice.tick()) yield;
    }
    yield* this.#gather(columns, from, count - from);
    if (count > 0) this.#remember(columns, count - 1);
  }

  /** Marks partition row `i` as starting a peer group (1) or not (0), growing the flags. */
  #flag(i: number, value: number): void {
    // A streaming partition finds its peers again as it is read.
    if (this.#streaming) return;
    if (i >= this.#newGroup.length) {
      const size = Math.max(4096, this.#newGroup.length * 2, i + 1);
      if (!this.charge.allocate(size - this.#newGroupBytes))
        throw new RangeError("Window peer flags could not be allocated");
      const grown = new Uint8Array(size);
      grown.set(this.#newGroup);
      this.#newGroup = grown;
      this.#newGroupBytes = size;
    }
    this.#newGroup[i] = value;
  }

  /** Keeps the key columns of the last row taken (the next batch's first row compares to it). */
  #remember(columns: readonly Vector[], row: number): void {
    // No partition or order keys: every row is in one partition and a peer of every other.
    if (this.#keyColumns.length === 0) return;
    const last = this.#last;
    last.reset();
    if (
      last.append(
        this.#keyColumns.map((c) => columnOf(columns, c)),
        null,
        row,
        1,
      ) !== 1
    )
      throw new RangeError("A window key row could not be held");
    // Indexed like the input (keysEqual reads key columns only).
    const held = last.vectors();
    const vectors: Vector[] = [];
    this.#keyColumns.forEach((c, k) => {
      vectors[c] = columnOf(held, k);
    });
    this.#lastVectors = vectors;
  }

  /** Appends rows `from … from + count` of a batch to the partition (held, arguments, sources). */
  *#gather(columns: readonly Vector[], from: number, count: number): SlicedJob<void> {
    if (count <= 0) return;
    if (!this.#streaming && this.#rows + count > (this.spec.streamRows ?? STREAM_ROWS))
      yield* this.#startStream();
    if (this.#streaming) {
      yield* this.#stage(columns, from, count);
      return;
    }
    // Arguments and sources take every row (a loan when they do not fit).
    // (A store of no columns takes no rows: skip it when no function needs arguments.)
    if (this.#argColumns.length > 0) {
      const argVectors = this.#argColumns.map((c) => columnOf(columns, c));
      if (!(yield* this.#appendAll(this.#args, argVectors, from, count, true))) {
        // The grant cannot hold this partition's arguments: it streams from here.
        yield* this.#startStream();
        yield* this.#stage(columns, from, count);
        return;
      }
    }
    for (let f = 0; f < this.#sources.length; f++) {
      const source = this.#sources[f];
      const call = this.spec.functions[f];
      if (source === null || source === undefined || call?.arg === null || call === undefined)
        continue;
      if (!(yield* this.#appendAll(source, [columnOf(columns, call.arg)], from, count, true))) {
        yield* this.#startStream();
        yield* this.#stage(columns, from, count);
        return;
      }
    }
    // Held rows: a partition past the grant spills them, in order.
    let done = 0;
    while (done < count) {
      const taken = this.#held.append(columns, null, from + done, count - done);
      done += taken;
      if (done >= count) break;
      if (this.#held.rows > 0) {
        yield this.#spillHeld();
        continue;
      }
      // Not even one row fits an empty store: it goes to the pages directly (bounded steps).
      yield this.#stageRow(columns, from + done);
      done++;
    }
    this.#rows += count;
  }

  /**
   * Appends every row to an argument or source store: true when done. `streamable`: on a
   * refusal, false (the caller streams the partition instead); else one progress loan.
   */
  *#appendAll(
    store: ColumnStore,
    vectors: readonly Vector[],
    from: number,
    count: number,
    streamable = false,
  ): SlicedJob<boolean> {
    let done = 0;
    while (done < count) {
      done += store.append(vectors, null, from + done, count - done);
      if (done >= count) return true;
      if (streamable && this.#canStream) return false;
      if (this.#loan !== null) throw new RangeError("A window partition's arguments do not fit");
      // One bounded step (rule 5): the next row (its chunk, if it opens one) and headroom.
      const need = store.appendNeed(vectors, from + done) + 64 * 1024;
      yield this.grant.borrow(need, this.context.signal).then((loan) => {
        this.#loan = loan;
        this.profile.loans++;
      });
    }
    return true;
  }

  /** Rows of a streaming partition go straight to its run. */
  *#stage(columns: readonly Vector[], from: number, count: number): SlicedJob<void> {
    const staged = (this.#staged ??= this.#staging());
    const run = (this.#run ??= new SpilledRun());
    const header = { run: 0, partition: this.profile.partitions };
    yield this.io.stage(staged, run, header, columns, null, from, count);
    this.#rows += count;
  }

  /**
   * The partition outgrew `streamRows`: its rows so far go to the run, its per-row stores and
   * arrays are freed, and from now on rows go straight to the run (ruling on review B7-p2d).
   */
  *#startStream(): SlicedJob<void> {
    for (const call of this.spec.functions) {
      const why = streamable(this.spec, call);
      if (why !== null)
        throw new TypeError(
          `A window partition of more than ${String(this.spec.streamRows ?? STREAM_ROWS)} rows cannot stream ${why}`,
        );
    }
    if (this.#held.rows > 0) yield this.#spillHeld();
    this.#args.truncate(0);
    for (const source of this.#sources) source?.truncate(0);
    this.#dropScratch();
    this.#streaming = true;
  }

  #staging(): ColumnStore {
    return new ColumnStore(this.spec.types, this.pool, {
      chunkRows: 512,
      targetBytes: 64 * 1024,
      varlenBytes: 4096,
    });
  }

  /** Writes the held rows to the partition's run and empties the store. */
  async #spillHeld(): Promise<void> {
    const run = (this.#run ??= new SpilledRun());
    const staging = this.#staging();
    const header = { run: 0, partition: this.profile.partitions };
    const held = this.#held;
    try {
      for (let k = 0; k < held.chunkCount; k++) {
        const vectors = this.spec.types.map((_, c) => held.chunk(c, k));
        const rows = vectors[0]?.length ?? 0;
        await this.io.stage(staging, run, header, vectors, null, 0, rows);
      }
      await this.io.flush(staging, run, header);
    } finally {
      staging.close();
    }
    held.truncate(0);
    this.profile.spilledBytes = this.files.written;
  }

  async #stageRow(columns: readonly Vector[], row: number): Promise<void> {
    const run = (this.#run ??= new SpilledRun());
    const staging = this.#staging();
    const header = { run: 0, partition: this.profile.partitions };
    try {
      await this.io.stage(staging, run, header, columns, null, row, 1);
      await this.io.flush(staging, run, header);
    } finally {
      staging.close();
    }
    this.profile.spilledBytes = this.files.written;
  }

  // --- a partition ------------------------------------------------------------------------

  /** Computes and emits the current partition, then empties it. */
  *#flushPartition(): SlicedJob<void> {
    const n = this.#rows;
    if (n === 0) return;
    this.profile.partitions++;
    this.profile.largestPartition = Math.max(this.profile.largestPartition, n);
    if (this.#run !== null) this.profile.spilledPartitions++;
    if (this.#streaming) {
      yield* this.#streamPartition();
      yield* this.#reset();
      return;
    }
    const scratch = yield* this.#scratchFor(n);
    if (scratch === null) {
      // Its per-row arrays do not fit: the partition streams after all (all of it to the run).
      if (this.#held.rows > 0) yield this.#spillHeld();
      this.#args.truncate(0);
      for (const source of this.#sources) source?.truncate(0);
      this.#streaming = true;
      yield* this.#streamPartition();
      yield* this.#reset();
      return;
    }
    const peers = scratch.peers;
    fillPeers(peers, this.#newGroup, n);
    const args = this.#extract(scratch, n);
    yield* this.#compute(scratch, args, n);
    yield* this.#emit(scratch, n);
    yield* this.#reset();
  }

  /** Evaluates a streaming partition from its run (window-stream.ts). */
  *#streamPartition(): SlicedJob<void> {
    this.profile.streamedPartitions++;
    const run = (this.#run ??= new SpilledRun());
    const staged = this.#staged;
    if (staged !== null) {
      yield this.io.flush(staged, run, { run: 0, partition: this.profile.partitions });
      staged.close();
      this.#staged = null;
    }
    const host: StreamHost = {
      io: this.io,
      charge: this.charge,
      pool: this.pool,
      slice: this.slice,
      push: (columns, count) => this.#pushDense(columns, count),
      resultVector: (call, f, values, valid, big, count) => {
        const output = this.#outputs[f];
        if (output === undefined) throw new RangeError("No window output");
        return this.#vectorOf(call, output, values, valid, big, 0, count);
      },
    };
    const stream = new PartitionStream(this.spec, host);
    try {
      yield* stream.run(run);
    } finally {
      stream.close();
    }
  }

  *#pushDense(columns: Vector[], count: number): SlicedJob<void> {
    this.profile.rowsOut += count;
    if (this.downstream.push({ length: count, columns, selection: null }) === "blocked")
      yield this.downstream.ready();
  }

  /** The partition scratch, grown (charged) to `n` rows: reused across partitions. */
  /** The partition scratch; null when the grant refuses it and the partition can stream. */
  *#scratchFor(n: number): SlicedJob<Scratch | null> {
    const current = this.#scratch;
    if (current !== null && current.capacity >= n) return current;
    const capacity = Math.max(n, 2 * (current?.capacity ?? 1024));
    const bytes = Scratch.bytes(capacity, this.spec.functions.length, this.#argColumns.length);
    if (!this.charge.allocate(bytes - this.#scratchBytes)) {
      if (this.#canStream) return null;
      if (this.#loan !== null) throw new RangeError("A window partition's scratch does not fit");
      yield this.grant.borrow(bytes - this.#scratchBytes, this.context.signal).then((loan) => {
        this.#loan = loan;
        this.profile.loans++;
      });
      if (!this.charge.allocate(bytes - this.#scratchBytes))
        throw new RangeError("A window partition's scratch could not be held");
    }
    this.#scratchBytes = bytes;
    this.#scratch = new Scratch(capacity, this.spec.functions.length, this.#argColumns.length);
    return this.#scratch;
  }

  /** The argument columns as numbers (and bigints for int64 past 2^53). */
  #extract(scratch: Scratch, n: number): NumberArg[] {
    const store = this.#args;
    return this.#argColumns.map((_, a) => {
      const values = (scratch.argValues[a] ?? new Float64Array(0)).subarray(0, n);
      const valid = (scratch.argValid[a] ?? new Uint8Array(0)).subarray(0, n);
      let big: bigint[] | null = null;
      for (let k = 0; k < store.chunkCount; k++) {
        const vector = store.chunk(a, k);
        const base = k * store.chunkRows;
        const bits = vector.validity;
        const data = vector.data;
        for (let i = 0; i < vector.length; i++) {
          const ok = bits === null || ((bits[i >>> 5] ?? 0) & (1 << (i & 31))) !== 0;
          valid[base + i] = ok ? 1 : 0;
          if (!ok) {
            values[base + i] = 0;
            continue;
          }
          switch (data.layout) {
            case "i32":
            case "f32":
            case "f64":
            case "bool":
              values[base + i] = data.values[i] ?? 0;
              break;
            case "int64":
              if (data.values.repr === "safe") values[base + i] = data.values.values[i] ?? 0;
              else {
                const v = data.values.values[i] ?? 0n;
                values[base + i] = Number(v);
                if (!Number.isSafeInteger(Number(v)) || BigInt(Number(v)) !== v) {
                  if (big === null) {
                    big = new Array<bigint>(n).fill(0n);
                    for (let j = 0; j < base + i; j++) big[j] = BigInt(values[j] ?? 0);
                  }
                }
                if (big !== null) big[base + i] = v;
              }
              break;
            default:
              // Only count reads other types (check() keeps sums and extremes numeric): validity.
              values[base + i] = 0;
          }
          if (big !== null && !(data.layout === "int64" && data.values.repr === "int64"))
            big[base + i] = BigInt(values[base + i] ?? 0);
        }
      }
      return { values, valid, big };
    });
  }

  #argOf(args: readonly NumberArg[], column: number): NumberArg {
    const a = this.#argColumns.indexOf(column);
    const arg = args[a];
    if (arg === undefined) throw new RangeError("Window argument column not gathered");
    return arg;
  }

  /** An amount (lag/lead offset, ntile buckets, nth n) per row; null when absent. */
  #amountOf(
    call: WindowFunctionSpec,
    args: readonly NumberArg[],
    scratch: Scratch,
    f: number,
    n: number,
  ): NumberArg | null {
    const amount = call.amount;
    if (amount === undefined || amount === null) return null;
    if (typeof amount !== "number") return this.#argOf(args, amount.column);
    // A constant: the function's own amount arrays (charged with the scratch).
    const values = (scratch.amountValues[f] ?? new Float64Array(0)).subarray(0, n).fill(amount);
    const valid = (scratch.amountValid[f] ?? new Uint8Array(0)).subarray(0, n).fill(1);
    return { values, valid, big: null };
  }

  *#compute(scratch: Scratch, args: readonly NumberArg[], n: number): SlicedJob<void> {
    const spec = this.spec;
    const peers = scratch.peers;
    const ordered = spec.orderBy.length > 0;
    const first = spec.orderBy[0];
    let key: RangeKey | null = null;
    if (first !== undefined && spec.functions.some((call) => call.frame?.unit === "range")) {
      const arg = this.#argOf(args, first.column);
      key = {
        values: arg.values,
        valid: arg.valid,
        descending: first.direction === "desc",
        nullsFirst: first.nulls === "first",
      };
    }
    for (let f = 0; f < spec.functions.length; f++) {
      const call = spec.functions[f];
      if (call === undefined) continue;
      const out: NumberResult = {
        values: (scratch.resultValues[f] ?? new Float64Array(0)).subarray(0, n),
        valid: (scratch.resultValid[f] ?? new Uint8Array(0)).subarray(0, n),
        big: null,
      };
      if (RANKING.has(call.fn)) {
        const amount = this.#amountOf(call, args, scratch, f, n);
        yield* ranking(call.fn as RankingKind, peers, amount, out, this.slice);
        scratch.big[f] = null;
        continue;
      }
      const starts = scratch.starts.subarray(0, n);
      const ends = scratch.ends.subarray(0, n);
      const framed = call.fn !== "lag" && call.fn !== "lead";
      if (framed) yield* frameBounds(call.frame, ordered, peers, key, starts, ends, this.slice);
      const exclude = call.frame?.exclude ?? "no-others";
      if (VALUE.has(call.fn)) {
        const amount = this.#amountOf(call, args, scratch, f, n);
        const targets = (scratch.targets[f] ?? new Int32Array(0)).subarray(0, n);
        yield* valueRows(
          call.fn as ValueKind,
          peers,
          exclude,
          starts,
          ends,
          amount,
          targets,
          this.slice,
        );
        continue;
      }
      const arg = call.arg === null ? null : this.#argOf(args, call.arg);
      const type = call.arg === null ? null : spec.types[call.arg];
      const exact = type !== null && type !== undefined && integral(type);
      yield* aggregate(
        call.fn as AggregateKind,
        arg,
        exact,
        peers,
        exclude,
        starts,
        ends,
        out,
        this.slice,
      );
      scratch.big[f] = out.big;
    }
  }

  // --- emission ---------------------------------------------------------------------------

  *#emit(scratch: Scratch, n: number): SlicedJob<void> {
    // Value functions gather from their sources: rows 0 … n − 1, then NULL, then the default.
    for (let f = 0; f < this.#sources.length; f++) {
      const source = this.#sources[f];
      const call = this.spec.functions[f];
      if (source === null || source === undefined || call === undefined) continue;
      const type = source.types[0] ?? call.type;
      yield* this.#appendAll(source, [nullConstant(type, 1)], 0, 1);
      const fallback = call.fallback ?? nullConstant(type, 1);
      yield* this.#appendAll(source, [fallback], 0, 1);
    }
    let at = 0;
    const run = this.#run;
    if (run !== null) {
      for (let p = 0; p < run.pages.length; p++) {
        let page: { bytes: Uint8Array; charged: number } | null = null;
        yield this.io.readPage(run, p).then((held) => {
          page = held;
        });
        const held = page as { bytes: Uint8Array; charged: number } | null;
        if (held === null) throw new RangeError("A window page was not read");
        try {
          let columns: readonly Vector[] = [];
          let rows = 0;
          yield this.io.decode(held, this.spec.types).then((decoded) => {
            columns = decoded.columns;
            rows = decoded.rows;
          });
          // A page holds at most one staging chunk (512 rows): one batch.
          yield* this.#push(columns, scratch, at, rows, n);
          at += rows;
        } finally {
          this.io.releasePage(held);
        }
      }
    }
    const store = this.#held;
    for (let k = 0; k < store.chunkCount; k++) {
      const columns = this.spec.types.map((_, c) => store.chunk(c, k));
      const rows = columns[0]?.length ?? 0;
      yield* this.#push(columns, scratch, at, rows, n);
      at += rows;
    }
  }

  /** Pushes `count` rows (their held columns) with the functions' results for rows `at …`. */
  *#push(
    held: readonly Vector[],
    scratch: Scratch,
    at: number,
    count: number,
    n: number,
  ): SlicedJob<void> {
    const columns: Vector[] = [...held];
    for (let f = 0; f < this.spec.functions.length; f++) {
      const call = this.spec.functions[f];
      const output = this.#outputs[f];
      if (call === undefined || output === undefined) continue;
      if (output.gather !== null) {
        const source = this.#sources[f];
        const targets = scratch.targets[f];
        if (source === null || source === undefined || targets === undefined) continue;
        for (let i = 0; i < count; i++) {
          const t = targets[at + i] ?? NULL_ROW;
          output.ids[i] = t === NULL_ROW ? n : t === DEFAULT_ROW ? n + 1 : t;
        }
        output.gather.reset(true);
        let taken = 0;
        while (taken < count) {
          taken += output.gather.appendFrom(source, [0], output.ids, taken, count - taken);
          if (taken < count)
            yield this.io.need(output.gather.appendFromNeed(source, [0], output.ids[taken] ?? 0));
        }
        columns.push(columnOf(output.gather.vectors(), 0));
        continue;
      }
      columns.push(this.#resultVector(call, output, scratch, f, at, count));
    }
    this.profile.rowsOut += count;
    if (this.downstream.push({ length: count, columns, selection: null }) === "blocked")
      yield this.downstream.ready();
  }

  /** A numeric function's results for rows `at … at + count` as a vector of its type. */
  #resultVector(
    call: WindowFunctionSpec,
    output: Output,
    scratch: Scratch,
    f: number,
    at: number,
    count: number,
  ): FlatVector {
    return this.#vectorOf(
      call,
      output,
      scratch.resultValues[f] ?? new Float64Array(0),
      scratch.resultValid[f] ?? new Uint8Array(0),
      scratch.big[f] ?? null,
      at,
      count,
    );
  }

  /** Results `at … at + count` of `values` / `valid` / `big` as a vector of the call's type. */
  #vectorOf(
    call: WindowFunctionSpec,
    output: Output,
    values: Float64Array,
    valid: Uint8Array,
    big: bigint[] | null,
    at: number,
    count: number,
  ): FlatVector {
    let allValid = true;
    output.validity.fill(0);
    for (let i = 0; i < count; i++)
      if (valid[at + i] === 1)
        output.validity[i >>> 5] = (output.validity[i >>> 5] ?? 0) | (1 << (i & 31));
      else allValid = false;
    const validity = allValid ? null : output.validity;
    const type = call.type;
    switch (type.kind) {
      case "int32":
      case "date":
        for (let i = 0; i < count; i++) output.i32[i] = values[at + i] ?? 0;
        return {
          form: "flat",
          type,
          length: count,
          validity,
          data: { layout: "i32", values: output.i32 },
        };
      case "float32":
        for (let i = 0; i < count; i++) output.f32[i] = values[at + i] ?? 0;
        return {
          form: "flat",
          type,
          length: count,
          validity,
          data: { layout: "f32", values: output.f32 },
        };
      case "float64":
        return {
          form: "flat",
          type,
          length: count,
          validity,
          data: { layout: "f64", values: values.subarray(at, at + count) },
        };
      case "int64":
      case "timestamp":
      case "time": {
        if (big === null)
          return {
            form: "flat",
            type,
            length: count,
            validity,
            data: {
              layout: "int64",
              values: { repr: "safe", values: values.subarray(at, at + count) },
            },
          };
        for (let i = 0; i < count; i++) output.big[i] = big[at + i] ?? 0n;
        return {
          form: "flat",
          type,
          length: count,
          validity,
          data: { layout: "int64", values: { repr: "int64", values: output.big } },
        };
      }
      case "decimal": {
        // An exact sum (scale 0): doubles while safe, else int64, else 128-bit limbs.
        let wide = false;
        let beyond64 = false;
        if (big !== null)
          for (let i = 0; i < count; i++) {
            const v = big[at + i] ?? 0n;
            if (v > 9007199254740991n || v < -9007199254740991n) wide = true;
            if (v > 9223372036854775807n || v < -9223372036854775808n) beyond64 = true;
          }
        const coefficients = !wide
          ? {
              repr: "safe" as const,
              values:
                big === null
                  ? values.slice(at, at + count)
                  : Float64Array.from({ length: count }, (_, i) => Number(big[at + i] ?? 0n)),
            }
          : !beyond64
            ? {
                repr: "int64" as const,
                values: BigInt64Array.from({ length: count }, (_, i) => big?.[at + i] ?? 0n),
              }
            : (() => {
                const limbs = new Uint32Array(count * 4);
                for (let i = 0; i < count; i++) setInt128(limbs, i, big?.[at + i] ?? 0n);
                return { repr: "int128" as const, limbs };
              })();
        return {
          form: "flat",
          type,
          length: count,
          validity,
          data: { layout: "decimal", scale: 0, scales: null, coefficients, specials: null },
        };
      }
      default:
        throw new TypeError(`Window results of type ${type.kind} are not supported`);
    }
  }

  /** Empties the partition (held rows, run, arguments, sources) and repays a loan. */
  *#reset(): SlicedJob<void> {
    // Multi-chunk stores: truncate frees every chunk (reset is for one-chunk stores).
    this.#held.truncate(0);
    this.#args.truncate(0);
    for (const source of this.#sources) source?.truncate(0);
    const run = this.#run;
    this.#run = null;
    this.#rows = 0;
    this.#streaming = false;
    this.#staged?.close();
    this.#staged = null;
    if (run !== null) yield this.files.release(run);
    this.profile.peakHeldBytes = Math.max(this.profile.peakHeldBytes, this.charge.peak);
    if (this.#loan !== null) {
      // The loan may back the scratch and the peer flags: they go first, then the loan.
      this.#dropScratch();
      this.#loan.repay();
      this.#loan = null;
    }
  }

  /** Frees the partition scratch and the peer flags (grown again by the next partition). */
  #dropScratch(): void {
    if (this.#scratchBytes > 0) this.charge.free(this.#scratchBytes);
    this.#scratchBytes = 0;
    this.#scratch = null;
    if (this.#newGroupBytes > 0) this.charge.free(this.#newGroupBytes);
    this.#newGroupBytes = 0;
    this.#newGroup = new Uint8Array(0);
  }

  // --- lifecycle -------------------------------------------------------------------------

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.profile.peakHeldBytes = Math.max(this.profile.peakHeldBytes, this.charge.peak);
    this.#held.close();
    this.#args.close();
    for (const source of this.#sources) source?.close();
    for (const output of this.#outputs) output.gather?.close();
    this.#last.close();
    this.#staged?.close();
    this.#staged = null;
    if (this.#scratchBytes > 0) this.charge.free(this.#scratchBytes);
    if (this.#newGroupBytes > 0) this.charge.free(this.#newGroupBytes);
    this.charge.free(this.spec.functions.length * BATCH_ROWS * 32);
    this.#scratchBytes = 0;
    this.#newGroupBytes = 0;
    this.#loan?.repay();
    this.#loan = null;
    this.pages.close();
    this.pool.close();
    this.profile.spilledBytes = this.files.written;
    this.#closing = this.files.closeAll().finally(() => {
      this.grant.close();
    });
    this.#closing.catch(() => undefined);
  }

  get closed(): Promise<void> {
    return this.#closing ?? Promise.resolve();
  }

  abort(reason: unknown): void {
    if (this.#aborted === undefined) this.#aborted = reason ?? new Error("Aborted");
    this.close();
  }
}

/** The partition scratch: peers, frame bounds and per-function / per-argument arrays. */
class Scratch {
  readonly peers: Peers;
  readonly starts: Int32Array;
  readonly ends: Int32Array;
  readonly resultValues: Float64Array[];
  readonly resultValid: Uint8Array[];
  readonly targets: Int32Array[];
  readonly argValues: Float64Array[];
  readonly argValid: Uint8Array[];
  readonly amountValues: Float64Array[];
  readonly amountValid: Uint8Array[];
  /** Exact results past 2^53 (bigints, charged with the scratch's per-row allowance). */
  readonly big: Array<bigint[] | null>;

  static bytes(capacity: number, functions: number, args: number): number {
    // Peers (5 × 4), bounds (2 × 4), per function 22 (results, targets, a constant amount), per
    // argument 9, and an allowance for the kernels' prefix arrays (12 per row).
    return capacity * (20 + 8 + 22 * functions + 9 * args + 12);
  }

  constructor(
    readonly capacity: number,
    functions: number,
    args: number,
  ) {
    this.peers = {
      n: 0,
      start: new Int32Array(capacity),
      end: new Int32Array(capacity),
      group: new Int32Array(capacity),
      groupStart: new Int32Array(capacity),
      groupEnd: new Int32Array(capacity),
      groups: 0,
    };
    this.starts = new Int32Array(capacity);
    this.ends = new Int32Array(capacity);
    this.resultValues = Array.from({ length: functions }, () => new Float64Array(capacity));
    this.resultValid = Array.from({ length: functions }, () => new Uint8Array(capacity));
    this.targets = Array.from({ length: functions }, () => new Int32Array(capacity));
    this.argValues = Array.from({ length: args }, () => new Float64Array(capacity));
    this.argValid = Array.from({ length: args }, () => new Uint8Array(capacity));
    this.amountValues = Array.from({ length: functions }, () => new Float64Array(capacity));
    this.amountValid = Array.from({ length: functions }, () => new Uint8Array(capacity));
    this.big = Array.from({ length: functions }, () => null);
  }
}

function columnOf<T>(columns: readonly T[], c: number): T {
  const column = columns[c];
  if (column === undefined) throw new RangeError(`No column ${String(c)}`);
  return column;
}
