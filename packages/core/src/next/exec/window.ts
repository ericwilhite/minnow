/**
 * The window operator (B7 increment 3): `window` rels with PostgreSQL's frames (ROWS / RANGE /
 * GROUPS with offsets, EXCLUDE), grant-or-spill like the sort and the join, sliced per R23.
 *
 * - **Order.** Rows are ordered by (PARTITION BY, ORDER BY): by the grouped sort where it
 *   applies (window-sort.ts: partition ids, normalized key words, radix passes, one gather), else
 *   by the sort operator (sort.ts: its runs, merges, slices and abort checks), over the same
 *   grant (the evaluator closes it). A grouped sort the grant refuses moves its rows to the
 *   sort operator (`#fallback`).
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
 * constant, a column or a lazy scalar kernel; default a constant or a lazy scalar kernel),
 * first_value, last_value and nth_value over any type;
 * count, count_star, and sum / avg / min / max of integers and floats (min / max also dates and
 * timestamps). RANGE offsets need one numeric ORDER BY key. Decimal and interval arguments are
 * refused (TypeError) for aggregate arguments; value functions gather any supported type.
 */
import { WindowCheckpoint, WINDOW_ORDINAL_BYTES } from "./window-checkpoint.js";
import { slotOf, isValidSlot } from "./kernels/vectors.js";
import { NumericOutOfRangeError } from "./errors.js";
import { WINDOW_FLOAT_STATE_BYTES } from "./window-float.js";
import type { Grant, Loan } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import { setInt128 } from "../vector/int128.js";
import type { Batch, FlatVector, Vector } from "../vector/vector.js";
import { BATCH_ROWS } from "../vector/vector.js";
import { ColumnStore, nullConstant } from "./column-store.js";
import {
  YIELD_INTERVAL_MS,
  isPromise,
  yieldTask,
  type Operator,
  type PushResult,
  type Transfer,
} from "./pipeline.js";
import { keysEqual, markChanges, type SortColumn } from "./sort-keys.js";
import { PartitionStream, streamable, type StreamHost } from "./window-stream.js";
import { GroupedRows, groupedSortable } from "./window-sort.js";
import {
  GroupTopKOperator,
  SortOperator,
  sortMinimumBytes,
  type SortContext,
  type SortProfile,
} from "./sort.js";
import { Slice, runSliced, type SlicedJob } from "./slices.js";
import { PageWriter, SpillFiles, SpilledRun } from "./spill-pages.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";
import {
  WindowError,
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
  /** Current-row int4 offset, evaluated only when that output row is demanded. */
  readonly offsetValue?: (batch: Batch) => Vector;
  /** Evaluated only for DEFAULT_ROW positions of the current (sorted) input batch. */
  readonly defaultValue?: (batch: Batch) => Vector;
  /** Null: PostgreSQL's default frame. */
  readonly frame: Frame | null;
  readonly type: PhysicalType;
}

export interface WindowSpec {
  /** Proven sole ranking-function upper bound; the outer predicate remains in place. */
  readonly topK?: number;
  /** Phase planner opt-in: preserve peer-stable input on an actual nonempty cut. */
  readonly checkpointable?: boolean;
  readonly types: readonly PhysicalType[];
  readonly partitionBy: readonly number[];
  readonly orderBy: readonly SortColumn[];
  readonly functions: readonly WindowFunctionSpec[];
  /**
   * Rows past which a partition streams (bounded by its frames, not its size: window-stream.ts)
   * instead of being held whole. Default `STREAM_ROWS`; tests lower it.
   */
  readonly streamRows?: number;
  /**
   * Test seam: `"sort"` orders rows with the sort operator even where the grouped sort applies
   * (window-sort.ts). Default: the grouped sort when it applies.
   */
  readonly sortPath?: "grouped" | "sort";
}

/**
 * Partitions up to this many rows are evaluated resident while the grant holds them (one it
 * refuses streams anyway); larger ones always stream.
 */
export const STREAM_ROWS = 1 << 22;

export interface WindowProfile {
  /** A proven sole ranking bound avoids full window payload/evaluation. */
  boundedRanking: boolean;
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
  /** Rows ordered by the grouped sort (window-sort.ts), and its refusals (rows moved to the sort). */
  groupedRows: number;
  groupedFallbacks: number;
  sort: SortProfile;
}

const POOL_BYTES = 1024 * 1024;
/** Charged bytes of an int64-derived bigint value and its fixed-capacity array slot (up to 128 bits). */
const BIGINT_BYTES = 48;
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
    functions * (BATCH_ROWS * (32 + 9 + BIGINT_BYTES) + 32 * 1024) +
    // A streaming partition's working set for a bounded frame: its ring (4,096 rows), a few
    // pages in flight and the output chunk (window-stream.ts).
    functions * 4096 * 48 +
    functions * 3 * WINDOW_FLOAT_STATE_BYTES +
    4096 * 16 +
    1024 * 1024 +
    64 * 1024
  );
}

/** Complete native minimum for the phase-capable variant, including its hidden key column. */
export function windowRetainingMinimumBytes(types: readonly PhysicalType[], functions = 1): number {
  return (
    windowMinimumBytes(types, functions) +
    WINDOW_ORDINAL_BYTES +
    sortMinimumBytes([...types, { kind: "int64" }]) -
    sortMinimumBytes(types)
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
export function validateWindowSpec(spec: WindowSpec): void {
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

/**
 * Whether anything but the partitioning reads the (single) partition column: a function's
 * argument or amount, or an ORDER BY key. Otherwise the grouped path neither stores nor gathers
 * it per row (window-sort.ts): its value goes out as a constant.
 */
function partitionRead(spec: WindowSpec): boolean {
  if (
    spec.functions.some((call) => call.defaultValue !== undefined || call.offsetValue !== undefined)
  )
    return true;
  const part = spec.partitionBy[0];
  if (part === undefined || spec.partitionBy.length !== 1) return true;
  if (spec.orderBy.some((key) => key.column === part)) return true;
  return spec.functions.some(
    (call) =>
      call.arg === part ||
      (call.amount !== undefined &&
        call.amount !== null &&
        typeof call.amount !== "number" &&
        call.amount.column === part),
  );
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

/** Output demand applies to evaluation, never to the blocking input sort. */
export interface WindowContext extends SortContext {
  readonly stopped?: () => boolean;
}

/** `ROW_NUMBER() / RANK() / … OVER (PARTITION BY … ORDER BY … frame)` (see the module comment). */
export class WindowOperator implements Operator {
  readonly spec: WindowSpec;
  #sort: SortOperator | GroupTopKOperator;
  #boundedRanking: boolean;
  readonly #context: SortContext;
  readonly #evaluator: Evaluator;
  /** The grouped sort's rows while it applies (null: the sort operator has the rows). */
  #grouped: GroupedRows | null = null;
  #pending: Promise<void> | null = null;
  #inputWork: Promise<void> | null = null;
  #started = false;
  #finishing = false;
  #checkpointed = false;
  #restoreStarted = false;
  #priorSpilledBytes = 0;
  #nextOrdinal = 0n;
  #ordinals: BigInt64Array | null = null;
  #phaseWork: Promise<unknown> | null = null;
  #aborting: Promise<void> | null = null;
  readonly #cancel = new AbortController();
  readonly #grant: Grant;

  constructor(spec: WindowSpec, context: WindowContext, downstream: Operator) {
    validateWindowSpec(spec);
    this.spec = spec;
    const grant = deferClose(context.grant);
    this.#grant = context.grant;
    const shared: WindowContext = {
      ...context,
      grant,
      ...(spec.checkpointable
        ? {
            signal: context.signal
              ? AbortSignal.any([context.signal, this.#cancel.signal])
              : this.#cancel.signal,
          }
        : {}),
    };
    this.#context = shared;
    this.#evaluator = new Evaluator(spec, shared, context.grant, downstream);
    const call = spec.functions.length === 1 ? spec.functions[0] : undefined;
    this.#boundedRanking =
      spec.topK !== undefined &&
      Number.isSafeInteger(spec.topK) &&
      spec.topK > 0 &&
      call?.frame === null &&
      (call.fn === "rank" || call.fn === "row_number");
    if (
      !this.#boundedRanking &&
      spec.sortPath !== "sort" &&
      groupedSortable(spec.types, spec.partitionBy, spec.orderBy)
    )
      try {
        this.#grouped = new GroupedRows(
          spec.types,
          spec.partitionBy,
          spec.orderBy,
          new Charge(grant),
          partitionRead(spec),
        );
      } catch {
        // Its first buffers were refused: the sort operator takes the rows.
        this.#grouped = null;
      }
    this.#sort = this.#boundedRanking
      ? new GroupTopKOperator(
          {
            types: spec.types,
            partitionBy: spec.partitionBy,
            orderBy: spec.orderBy,
            k: spec.topK ?? 1,
            ranking: call?.fn === "rank" ? "rank" : "row_number",
            partitionNulls: "last",
          },
          shared,
          downstream,
        )
      : this.#makeSort(false);
  }

  #makeSort(ordinals: boolean): SortOperator {
    const spec = this.spec;
    const keys: SortColumn[] = [
      ...spec.partitionBy.map((column): SortColumn => ({
        column,
        direction: "asc",
        nulls: "last",
        collation: null,
      })),
      ...spec.orderBy,
      ...(ordinals
        ? [
            {
              column: spec.types.length,
              direction: "asc" as const,
              nulls: "last" as const,
              collation: null,
            },
          ]
        : []),
    ];
    return new SortOperator(
      {
        types: ordinals ? [...spec.types, { kind: "int64" }] : spec.types,
        keys,
        limit: null,
        offset: 0,
        withTies: false,
      },
      ordinals ? { ...this.#context, columnar: false } : this.#context,
      ordinals
        ? {
            push: (batch, transfer) =>
              this.#evaluator.push(
                { ...batch, columns: batch.columns.slice(0, spec.types.length) },
                transfer,
              ),
            ready: () => this.#evaluator.ready(),
            finish: () => this.#evaluator.finish(),
            abort: (reason) => this.#evaluator.abort(reason),
          }
        : this.#evaluator,
    );
  }

  /** Only an actual cut/fallback pays for stable continuation ordinals. */
  async #enableOrdinals(): Promise<void> {
    if (this.#ordinals) return;
    if (this.#sort instanceof GroupTopKOperator)
      this.#sort.abort(new Error("Window ranking route retired"));
    else this.#sort.discard();
    await this.#sort.closed;
    this.#boundedRanking = false;
    this.#evaluator.checkAborted();
    if (!this.#grant.allocate(WINDOW_ORDINAL_BYTES))
      throw new RangeError("Window ordinals must be admitted");
    this.#ordinals = new BigInt64Array(BATCH_ROWS);
    this.#sort = this.#makeSort(true);
  }

  #withOrdinals(batch: Batch, start: bigint): Batch {
    const values = this.#ordinals;
    if (!values) return batch;
    const selection = batch.selection;
    for (let i = 0; i < (selection?.count ?? batch.length); i++) {
      const row = selection ? (selection.rows[i] ?? 0) : i;
      values[row] = start + BigInt(i);
    }
    return {
      ...batch,
      columns: [
        ...batch.columns,
        {
          form: "flat",
          type: { kind: "int64" },
          length: batch.length,
          validity: null,
          data: { layout: "int64", values: { repr: "int64", values } },
        },
      ],
    };
  }

  get profile(): WindowProfile {
    const profile = this.#evaluator.profile;
    profile.sort = this.#sort.profile;
    profile.boundedRanking = this.#boundedRanking;
    if (this.#boundedRanking) {
      // A checkpoint may replay fewer candidates than original arrivals.
      profile.rowsIn = this.spec.checkpointable ? Number(this.#nextOrdinal) : profile.sort.rowsIn;
      profile.rowsOut = profile.sort.rowsOut;
    }
    profile.spilledBytes = this.#priorSpilledBytes + this.#evaluator.files.written;
    return profile;
  }

  /**
   * The grant refused the grouped sort: its rows move to the sort operator (the last chunk
   * first, each freed once the sort has it, so memory passes from one to the other), and the
   * sort operator takes everything from here on.
   */
  async #fallback(): Promise<void> {
    const grouped = this.#grouped;
    if (grouped === null) return;
    this.#grouped = null;
    this.#evaluator.profile.groupedFallbacks++;
    try {
      if (this.spec.checkpointable) await this.#enableOrdinals();
      const store = grouped.store;
      for (let k = store.chunkCount - 1; k >= 0; k--) {
        const columns = grouped.batchColumns(k);
        const length = Math.min(store.chunkRows, store.rows - k * store.chunkRows);
        if (
          length > 0 &&
          this.#sort.push(
            this.#withOrdinals({ length, columns, selection: null }, BigInt(k * store.chunkRows)),
          ) === "blocked"
        )
          await this.#sort.ready();
        store.truncate(k * store.chunkRows);
        await yieldTask();
        this.#evaluator.checkAborted();
      }
    } finally {
      grouped.close();
    }
  }

  get closed(): Promise<void> {
    return (
      this.#aborting ??
      Promise.all([this.#sort.closed, this.#evaluator.closed]).then(() => undefined)
    );
  }

  /** PostgreSQL calculate_frame_offsets runs even for empty input, when first executed. */
  #start(): void {
    if (this.#started) return;
    for (const call of this.spec.functions) {
      if (RANKING.has(call.fn) || call.frame === null) continue;
      for (const bound of [call.frame.start, call.frame.end]) {
        if (bound.kind !== "preceding" && bound.kind !== "following") continue;
        if (bound.offset === null) throw new WindowError("frame offset must not be null", "22004");
        // RANGE's in_range checks the sign only when comparing two non-NULL keys.
        if (call.frame.unit !== "range" && (bound.offset < 0 || Number.isNaN(bound.offset)))
          throw new WindowError("frame offset must not be negative or NaN", "22013");
      }
    }
    this.#started = true;
  }

  push(batch: Batch, transfer?: Transfer): PushResult {
    if (this.#checkpointed || this.#phaseWork || this.#finishing)
      throw new TypeError("Window input boundary is sealed");
    const start = this.#nextOrdinal;
    if (this.spec.checkpointable)
      this.#nextOrdinal += BigInt(batch.selection?.count ?? batch.length);
    try {
      this.#start();
      const grouped = this.#grouped;
      if (grouped !== null) {
        this.#evaluator.checkAborted();
        if (grouped.add(batch)) {
          transfer?.release();
          return "ready";
        }
        this.#pending = this.#fallback().then(() => {
          if (this.#sort.push(this.#withOrdinals(batch, start), transfer) === "blocked")
            return this.#sort.ready();
        });
        const work = this.#pending;
        this.#inputWork = work;
        void work.then(
          () => {
            if (this.#inputWork === work) this.#inputWork = null;
          },
          () => {
            if (this.#inputWork === work) this.#inputWork = null;
          },
        );
        return "blocked";
      }
      return this.#sort.push(this.#withOrdinals(batch, start), transfer);
    } catch (error) {
      this.abort(error);
      throw error;
    }
  }

  ready(): Promise<void> {
    const pending = this.#pending;
    this.#pending = null;
    return (pending ?? this.#sort.ready()).catch((error: unknown) => {
      this.abort(error);
      throw error;
    });
  }

  finish(): MaybePromise<void> {
    if (this.#checkpointed || this.#phaseWork) throw new TypeError("Window input is checkpointed");
    this.#finishing = true;
    this.#dropOrdinals();
    let result: MaybePromise<void>;
    try {
      this.#start();
      const grouped = this.#grouped;
      if (grouped !== null) {
        // Once sorted, the sort operator is not needed: released before the evaluator ends.
        const sorted = this.#evaluator.finishGrouped(grouped, () => {
          if (this.#sort instanceof SortOperator) this.#sort.discard();
        });
        const after = (done: boolean): MaybePromise<void> => {
          if (done) {
            this.#grouped = null;
            return;
          }
          // The grant refused the sort's arrays: the sort operator sorts instead.
          return this.#fallback().then(() => {
            this.#dropOrdinals();
            return this.#sort.finish();
          });
        };
        result = isPromise(sorted) ? sorted.then(after) : after(sorted);
      } else {
        result = this.#sort.finish();
        if (this.#boundedRanking) {
          const close = async () => {
            this.#evaluator.close();
            await this.closed;
          };
          result = isPromise(result) ? result.then(close) : close();
        }
      }
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

  checkpoint(): Promise<WindowCheckpoint> {
    if (!this.spec.checkpointable || this.#finishing || this.#checkpointed || this.#phaseWork)
      return Promise.reject(
        new TypeError("Window checkpoint requires an unfinished input boundary"),
      );
    const work = (async () => {
      await this.ready();
      // The grouped store is in arrival order. Its reverse-chunk fallback needs
      // explicit ordinals; an untouched sort instead snapshots stable peer order.
      if (this.#grouped && this.#grouped.rows > 0) await this.#fallback();
      const records = await this.#sort.checkpoint();
      try {
        this.#evaluator.checkAborted();
        this.#checkpointed = true;
        return new WindowCheckpoint(
          records,
          this.#nextOrdinal,
          this.profile.spilledBytes + this.profile.sort.spilledBytes,
          this.#ordinals !== null,
        );
      } catch (error) {
        await records.close();
        throw error;
      }
    })();
    this.#phaseWork = work;
    void work.then(
      () => {
        if (this.#phaseWork === work) this.#phaseWork = null;
      },
      () => {
        if (this.#phaseWork === work) this.#phaseWork = null;
      },
    );
    return work;
  }
  restore(saved: WindowCheckpoint): Promise<void> {
    if (
      !this.spec.checkpointable ||
      this.#started ||
      this.#phaseWork ||
      this.#finishing ||
      this.#restoreStarted
    )
      return Promise.reject(new TypeError("Window restore requires a new admitted operator"));
    this.#restoreStarted = true;
    const work = (async () => {
      // Buffered producers can hand off with no retained ancestor prefix, even
      // at plentiful memory. No rows need conversion: keep the ordinary route.
      if (saved.nextOrdinal === 0n && saved.records.tape.rows === 0) {
        await saved.close();
        this.#priorSpilledBytes = saved.spilledBytes;
        return;
      }
      this.#grouped?.close();
      this.#grouped = null;
      // A proven bounded ranking needs only its best prefix candidates. A pre-finish
      // snapshot retains every relevant key/value, so replay into the bounded route is exact.
      // A row_number whose row identity matters has its explicit SQL tie-breaker in the key.
      // Explicit-ordinal snapshots from a general route still use its general continuation.
      const bounded = this.#boundedRanking && !saved.hasOrdinals;
      if (!bounded) await this.#enableOrdinals();
      let ordinal = 0n;
      await saved.records.read(this.#evaluator.io, async (columns, rows) => {
        // Before finish, sort runs are chronological, each stable for equal keys;
        // the resident tail follows them. Only peer order matters to a window.
        const batch = { columns, length: rows, selection: null };
        const input = bounded || saved.hasOrdinals ? batch : this.#withOrdinals(batch, ordinal);
        ordinal += BigInt(rows);
        if (this.#sort.push(input) === "blocked") await this.#sort.ready();
      });
      this.#evaluator.checkAborted();
      await saved.close();
      this.#nextOrdinal = saved.nextOrdinal;
      this.#priorSpilledBytes = saved.spilledBytes;
    })();
    this.#phaseWork = work;
    void work.then(
      () => {
        if (this.#phaseWork === work) this.#phaseWork = null;
      },
      () => {
        if (this.#phaseWork === work) this.#phaseWork = null;
      },
    );
    return work;
  }
  #dropOrdinals(): void {
    if (this.#ordinals) {
      this.#ordinals = null;
      this.#grant.free(WINDOW_ORDINAL_BYTES);
    }
  }
  abort(reason: unknown): void {
    this.#cancel.abort(reason);
    if (this.spec.checkpointable) {
      if (this.#aborting) return;
      const work = this.#phaseWork;
      this.#aborting = (async () => {
        await Promise.allSettled([work, this.#inputWork]);
        this.#dropOrdinals();
        this.#grouped?.close();
        this.#grouped = null;
        this.#sort.abort(reason);
        const errors: unknown[] = [];
        try {
          await this.#sort.closed;
        } catch (error) {
          errors.push(error);
        }
        this.#evaluator.abort(reason);
        try {
          await this.#evaluator.closed;
        } catch (error) {
          errors.push(error);
        }
        if (errors.length) throw new AggregateError(errors, "Window retirement failed");
      })();
      void this.#aborting.catch(() => undefined);
      return;
    }
    if (this.#phaseWork) {
      this.#aborting ??= this.#phaseWork
        .catch(() => undefined)
        .then(() => {
          this.abort(reason);
          return Promise.all([this.#sort.closed, this.#evaluator.closed]).then(() => undefined);
        });
      return;
    }
    this.#dropOrdinals();
    this.#sort.abort(reason);
    if (this.#boundedRanking) {
      this.#aborting ??= (async () => {
        try {
          await this.#sort.closed;
        } finally {
          this.#evaluator.abort(reason);
          await this.#evaluator.closed;
        }
      })();
      void this.#aborting.catch(() => undefined);
      return;
    }
    this.#evaluator.abort(reason);
    this.#grouped?.close();
    this.#grouped = null;
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
  /** One batch's partition and peer boundaries (`markChanges`), charged (2 bytes a row). */
  #partChanged = new Uint8Array(BATCH_ROWS);
  #peerChanged = new Uint8Array(BATCH_ROWS);
  /** A shifted input segment when a lazy default requires a row-level delivery boundary. */
  #defaultBatch: ColumnStore | null = null;
  #scratch: Scratch | null = null;
  /** Bytes charged for the partition's bigint arrays (exact int64 past 2^53). */
  #bigBytes = 0;
  #scratchBytes = 0;
  #newGroupBytes = 0;
  readonly #outputs: Output[];
  /** Every input column (the grouped path's gathers). */
  readonly #allColumns: readonly number[];
  /**
   * The grouped path's gather of the held rows: the partition column skipped (−1), since a
   * partition's rows all share its value, which goes out as a constant (`#partitionValue`).
   */
  readonly #heldColumns: readonly number[];
  readonly #partitionColumn: number;
  #partitionStore: ColumnStore | null = null;
  #partitionValue: FlatVector | null = null;
  /** The grouped path's block of rows for a streaming partition (made when first needed). */
  #block: ColumnStore | null = null;
  #blockRows = 0;
  /** Held row ids of the grouped path's current block (its arguments copy from them). */
  readonly #identity = new Int32Array(BATCH_ROWS);
  #loan: Loan | null = null;
  /** Every function can stream: a partition the grant cannot hold streams instead of borrowing. */
  readonly #canStream: boolean;
  #pending: Promise<void> | null = null;
  #aborted: unknown = undefined;
  #closed = false;
  #closing: Promise<void> | null = null;
  readonly profile: WindowProfile = {
    boundedRanking: false,
    rowsIn: 0,
    rowsOut: 0,
    partitions: 0,
    largestPartition: 0,
    spilledPartitions: 0,
    streamedPartitions: 0,
    spilledBytes: 0,
    peakHeldBytes: 0,
    loans: 0,
    groupedRows: 0,
    groupedFallbacks: 0,
    sort: undefined as unknown as SortProfile,
  };

  constructor(
    spec: WindowSpec,
    readonly context: WindowContext,
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
    this.#allColumns = spec.types.map((_, c) => c);
    this.#partitionColumn = spec.partitionBy.length === 1 ? (spec.partitionBy[0] ?? -1) : -1;
    // Skipped only when nothing else reads it (else args and sources read it from the held rows).
    const skip = partitionRead(spec) ? -1 : this.#partitionColumn;
    this.#heldColumns = this.#allColumns.map((c) => (c === skip && c >= 0 ? -1 : c));
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
    if (
      !this.charge.allocate(
        spec.functions.length * (BATCH_ROWS * 32 + 3 * WINDOW_FLOAT_STATE_BYTES) + 2 * BATCH_ROWS,
      )
    )
      throw new RangeError("A window's minimum must be admitted");
  }

  checkAborted(): void {
    if (this.#aborted !== undefined)
      throw this.#aborted instanceof Error
        ? this.#aborted
        : new Error("Aborted", { cause: this.#aborted });
    this.context.signal?.throwIfAborted();
  }

  /** Runs a job against the worker's task clock (R23.5: nothing here starts a slice). */
  #sliced<T>(job: SlicedJob<T>): MaybePromise<T> {
    return runSliced(job, this.slice, () => {
      this.checkAborted();
    });
  }

  push(batch: Batch, transfer?: Transfer): PushResult {
    let result: MaybePromise<void>;
    try {
      this.checkAborted();
      if (this.context.stopped?.()) {
        transfer?.release();
        return "ready";
      }
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

  /**
   * The grouped sort's path (window-sort.ts): orders the rows, takes each partition straight
   * from their store (one gather into the partition's stores), then finishes like `finish`.
   * False, with nothing emitted, when the grant refused the sort's arrays.
   */
  finishGrouped(rows: GroupedRows, onSorted: () => void): MaybePromise<boolean> {
    let result: MaybePromise<boolean>;
    try {
      this.checkAborted();
      result = this.#sliced(this.#groupedJob(rows, onSorted));
    } catch (error) {
      this.abort(error);
      throw error;
    }
    const done = async (sorted: boolean): Promise<boolean> => {
      if (!sorted) return false;
      this.close();
      await this.closed;
      await this.downstream.finish();
      return true;
    };
    const fail = (error: unknown): never => {
      this.abort(error);
      throw error;
    };
    if (isPromise(result)) return result.then(done, fail);
    return done(result).catch(fail);
  }

  *#groupedJob(rows: GroupedRows, onSorted: () => void): SlicedJob<boolean> {
    const order = yield* rows.sortJob(this.slice);
    if (order === null) return false;
    onSorted();
    const { ids, starts, breaks, groupOf } = order;
    this.profile.rowsIn += rows.rows;
    this.profile.groupedRows += rows.rows;
    for (let p = 0; p + 1 < starts.length && !this.context.stopped?.(); p++) {
      const s = starts[p] ?? 0;
      const e = starts[p + 1] ?? 0;
      if (e > s && this.#partitionColumn >= 0) {
        // The partition's value, once: its rows go out with it as a constant.
        const value = (this.#partitionStore ??= new ColumnStore(
          [this.spec.types[this.#partitionColumn] ?? { kind: "int32", bits: 32 }],
          this.pool,
          { chunkRows: 1, varlenBytes: 64 },
        ));
        value.truncate(0);
        const one = rows.partitionValue(groupOf[p] ?? 0);
        if (one !== null) {
          while (value.append([one], null, 0, 1) < 1)
            yield this.io.need(value.appendNeed([one], 0));
          this.#partitionValue = value.chunk(0, 0);
        }
      }
      for (let at = s; at < e; at += BATCH_ROWS) {
        const count = Math.min(BATCH_ROWS, e - at);
        for (let i = 0; i < count; i++) {
          const j = at + i;
          this.#flag(this.#rows + i, j === s ? 1 : (breaks[j] ?? 1));
        }
        yield* this.#gatherFrom(rows.store, ids, at, count);
      }
      yield* this.#flushPartition();
      this.#partitionValue = null;
    }
    // The partitions hold their own copies: the rows and the sort's arrays go now.
    rows.close();
    return true;
  }

  /**
   * `#gather` for the grouped path: rows `ids[at … at + count)` of `store`. The held rows are
   * gathered first (the one random gather); arguments and sources then copy from them in order
   * when they all stayed in the held store, else from `store` again.
   */
  *#gatherFrom(store: ColumnStore, ids: Int32Array, at: number, count: number): SlicedJob<void> {
    if (!this.#streaming && this.#rows + count > (this.spec.streamRows ?? STREAM_ROWS))
      yield* this.#startStream();
    if (this.#streaming) {
      yield* this.#stageFrom(store, ids, at, count);
      return;
    }
    const before = this.#held.rows;
    let spilled = false;
    let done = 0;
    while (done < count) {
      done += this.#held.appendFrom(store, this.#heldColumns, ids, at + done, count - done);
      if (done >= count) break;
      spilled = true;
      if (this.#held.rows > 0) {
        yield this.#spillHeld();
        continue;
      }
      // Not even one row fits an empty store: it goes to the pages directly.
      const block = yield* this.#blockOf(store, ids, at + done, 1);
      yield this.#stageRow(
        this.spec.types.map((_, c) => block.chunk(c, 0)),
        0,
      );
      done++;
    }
    let source = store;
    let rowIds = ids;
    let from = at;
    if (!spilled) {
      // The block is the held rows before … before + count.
      const identity = this.#identity;
      for (let i = 0; i < count; i++) identity[i] = before + i;
      source = this.#held;
      rowIds = identity;
      from = 0;
    }
    this.#rows += count;
    if (
      this.#argColumns.length > 0 &&
      !(yield* this.#appendAllFrom(this.#args, source, this.#argColumns, rowIds, from, count))
    ) {
      // The partition streams: the held rows (this block's included) go to its run.
      yield* this.#startStream();
      return;
    }
    for (let f = 0; f < this.#sources.length; f++) {
      const target = this.#sources[f];
      const call = this.spec.functions[f];
      if (target === null || target === undefined || call?.arg === null || call === undefined)
        continue;
      if (!(yield* this.#appendAllFrom(target, source, [call.arg], rowIds, from, count))) {
        yield* this.#startStream();
        return;
      }
    }
  }

  /** `#appendAll` from a store by row ids (true when done; false: the partition streams). */
  *#appendAllFrom(
    target: ColumnStore,
    source: ColumnStore,
    columns: readonly number[],
    ids: Int32Array,
    at: number,
    count: number,
  ): SlicedJob<boolean> {
    let done = 0;
    while (done < count) {
      done += target.appendFrom(source, columns, ids, at + done, count - done);
      if (done >= count) return true;
      if (this.#canStream) return false;
      if (this.#loan !== null) throw new RangeError("A window partition's arguments do not fit");
      const need = target.appendFromNeed(source, columns, ids[at + done] ?? 0) + 64 * 1024;
      yield this.grant.borrow(need, this.context.signal).then((loan) => {
        this.#loan = loan;
        this.profile.loans++;
      });
    }
    return true;
  }

  /**
   * Rows `ids[at …]` (up to `count`) of `store` copied into the block store (at least one), the
   * partition column skipped as in the held rows.
   */
  *#blockOf(
    store: ColumnStore,
    ids: Int32Array,
    at: number,
    count: number,
  ): SlicedJob<ColumnStore> {
    const block = (this.#block ??= new ColumnStore(this.spec.types, this.pool, {
      chunkRows: BATCH_ROWS,
      targetBytes: 64 * 1024,
      varlenBytes: 4096,
    }));
    block.truncate(0);
    for (;;) {
      const taken = block.appendFrom(store, this.#heldColumns, ids, at, count);
      if (taken > 0) {
        this.#blockRows = taken;
        return block;
      }
      yield this.io.need(block.appendFromNeed(store, this.#heldColumns, ids[at] ?? 0));
    }
  }

  /** A streaming partition's rows from a store: staged to its run a block at a time. */
  *#stageFrom(store: ColumnStore, ids: Int32Array, at: number, count: number): SlicedJob<void> {
    let done = 0;
    while (done < count) {
      const block = yield* this.#blockOf(store, ids, at + done, Math.min(count - done, BATCH_ROWS));
      const taken = this.#blockRows;
      for (let k = 0; k < block.chunkCount; k++) {
        const columns = this.spec.types.map((_, c) => block.chunk(c, k));
        yield* this.#stage(columns, 0, Math.min(block.chunkRows, block.rows - k * block.chunkRows));
      }
      done += taken;
    }
  }

  // --- arrival ---------------------------------------------------------------------------

  /** Takes a sorted batch: finds partition and peer boundaries, gathers, finishes partitions. */
  *#take(batch: Batch): SlicedJob<void> {
    const count = batch.selection === null ? batch.length : batch.selection.count;
    if (batch.selection !== null) throw new TypeError("The sort pushes dense batches");
    this.profile.rowsIn += count;
    const columns = batch.columns;
    const types = this.spec.types;
    // Partition and peer boundaries for the whole batch, column by column (row 0 against the
    // last row taken).
    if (count > this.#partChanged.length) {
      if (!this.charge.allocate(2 * count))
        throw new RangeError("Window boundary flags could not be allocated");
      this.charge.free(2 * this.#partChanged.length);
      this.#partChanged = new Uint8Array(count);
      this.#peerChanged = new Uint8Array(count);
    }
    const partChanged = this.#partChanged;
    const peerChanged = this.#peerChanged;
    partChanged.fill(0, 0, count);
    peerChanged.fill(0, 0, count);
    markChanges(types, this.#partKeys, columns, count, partChanged);
    markChanges(types, this.spec.orderBy, columns, count, peerChanged);
    if (count > 0 && this.#rows > 0) {
      const prev = this.#lastVectors;
      partChanged[0] = keysEqual(types, this.#partKeys, prev, 0, columns, 0) ? 0 : 1;
      peerChanged[0] = keysEqual(types, this.spec.orderBy, prev, 0, columns, 0) ? 0 : 1;
    }
    let from = 0;
    for (let r = 0; r < count; r++) {
      if (this.#rows + (r - from) === 0) {
        this.#flag(0, 1);
        continue;
      }
      if (partChanged[r] === 1) {
        yield* this.#gather(columns, from, r - from);
        from = r;
        yield* this.#flushPartition();
        if (this.context.stopped?.()) return;
        this.#flag(0, 1);
        continue;
      }
      this.#flag(this.#rows + (r - from), peerChanged[r] ?? 1);
      if ((r & 1023) === 1023 && this.slice.due()) yield;
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
      // Charged in full while the old flags are copied (governor rule 4), then the old freed.
      if (!this.charge.allocate(size))
        throw new RangeError("Window peer flags could not be allocated");
      const grown = new Uint8Array(size);
      grown.set(this.#newGroup);
      this.#newGroup = grown;
      this.charge.free(this.#newGroupBytes);
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
        const rows = Math.min(held.chunkRows, held.rows - k * held.chunkRows);
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
    if (this.context.stopped?.()) {
      yield* this.#reset();
      return;
    }
    this.profile.partitions++;
    this.profile.largestPartition = Math.max(this.profile.largestPartition, n);
    if (this.#run !== null) this.profile.spilledPartitions++;
    if (this.#streaming) {
      yield* this.#streamPartition();
      yield* this.#reset();
      return;
    }
    let scratch = yield* this.#scratchFor(n);
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
    // Exact arguments and potentially unsafe sums keep charged bigint arrays before they exist.
    const bytes = yield* this.#bigReservation(n);
    if (bytes > 0) {
      if (!this.charge.allocate(bytes)) {
        if (this.#canStream) {
          // Drop the local owner before streamResident releases the resident scratch.
          // eslint-disable-next-line no-useless-assignment
          scratch = null;
          yield* this.#streamResident();
          return;
        }
        if (this.#loan !== null)
          throw new RangeError("A window partition's exact values do not fit");
        yield this.grant.borrow(bytes, this.context.signal).then((loan) => {
          this.#loan = loan;
          this.profile.loans++;
        });
        if (!this.charge.allocate(bytes))
          throw new RangeError("A window partition's exact values could not be held");
      }
      this.#bigBytes = bytes;
    }
    const args = this.#extract(scratch, n);
    try {
      yield* this.#compute(scratch, args, n);
    } finally {
      args.length = 0;
    }
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
      stopped: () => this.context.stopped?.() === true,
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

  /** The grouped path's partition column as a constant (its rows were gathered without it). */
  #withPartition(columns: Vector[], count: number): Vector[] {
    const value = this.#partitionValue;
    if (value !== null)
      columns[this.#partitionColumn] = { form: "constant", type: value.type, length: count, value };
    return columns;
  }

  *#pushDense(columns: Vector[], count: number): SlicedJob<void> {
    this.#withPartition(columns, count);
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
    // The new scratch is charged in full while the old one is live (governor rule 4), then
    // the old one's charge goes (review B7-p2d-1 #5).
    if (!this.charge.allocate(bytes)) {
      if (this.#canStream) return null;
      if (this.#loan !== null) throw new RangeError("A window partition's scratch does not fit");
      yield this.grant.borrow(bytes, this.context.signal).then((loan) => {
        this.#loan = loan;
        this.profile.loans++;
      });
      if (!this.charge.allocate(bytes))
        throw new RangeError("A window partition's scratch could not be held");
    }
    this.#scratch = new Scratch(capacity, this.spec.functions.length, this.#argColumns.length);
    this.charge.free(this.#scratchBytes);
    this.#scratchBytes = bytes;
    return this.#scratch;
  }

  /** The argument columns as numbers (and bigints for int64 past 2^53). */
  /**
   * Reserve exact arguments and every prefix/result that can promote, including sums of
   * individually safe integers. n * max(abs(input)) bounds every integer frame/prefix sum.
   * Int64 inputs and an addressable JS array need at most 128 bits; 48 bytes per value/slot
   * is conservative for that width, rather than a universal arbitrary-bigint estimate.
   */
  *#bigReservation(n: number): SlicedJob<number> {
    const store = this.#args;
    const magnitudes: number[] = [];
    const wide: boolean[] = [];
    let slots = 0;
    for (let a = 0; a < this.#argColumns.length; a++) {
      let max = 0;
      let exact = false;
      for (let k = 0; k < store.chunkCount; k++) {
        const vector = store.chunk(a, k);
        const data = vector.data;
        if (data.layout !== "int64" && data.layout !== "i32") continue;
        for (let i = 0; i < vector.length; i++) {
          if (vector.validity !== null && ((vector.validity[i >>> 5] ?? 0) & (1 << (i & 31))) === 0)
            continue;
          const value =
            data.layout === "i32"
              ? (data.values[i] ?? 0)
              : data.values.repr === "safe"
                ? (data.values.values[i] ?? 0)
                : (data.values.values[i] ?? 0n);
          const number = Number(value);
          max = Math.max(max, Math.abs(number));
          exact ||= !Number.isSafeInteger(number);
        }
        if (this.slice.due()) yield;
      }
      magnitudes[a] = max;
      wide[a] = exact;
      if (exact) slots += n;
    }
    for (const call of this.spec.functions) {
      if (call.arg === null) continue;
      const a = this.#argColumns.indexOf(call.arg);
      if (
        (call.fn === "sum" || call.fn === "avg") &&
        (wide[a] || n * (magnitudes[a] ?? 0) > Number.MAX_SAFE_INTEGER)
      )
        slots += 2 * n + 1;
      else if ((call.fn === "min" || call.fn === "max") && wide[a]) slots += n;
    }
    return slots * BIGINT_BYTES;
  }

  /** Charge refusal switches the entire resident partition to bounded streamed evaluation. */
  *#streamResident(): SlicedJob<void> {
    this.#dropScratch();
    if (this.#held.rows > 0) yield this.#spillHeld();
    this.#args.truncate(0);
    for (const source of this.#sources) source?.truncate(0);
    this.#streaming = true;
    yield* this.#streamPartition();
    yield* this.#reset();
  }

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
        big: arg.big,
        descending: first.direction === "desc",
        nullsFirst: first.nulls === "first",
      };
    }
    for (let f = 0; f < spec.functions.length; f++) {
      const call = spec.functions[f];
      if (call === undefined) continue;
      const errorFlags = scratch.resultErrors[f];
      if (errorFlags === undefined) throw new Error("Missing window error flags");
      const out: NumberResult = {
        values: (scratch.resultValues[f] ?? new Float64Array(0)).subarray(0, n),
        valid: (scratch.resultValid[f] ?? new Uint8Array(0)).subarray(0, n),
        big: null,
        errors: errorFlags.subarray(0, n),
      };
      out.errors?.fill(0);
      if (RANKING.has(call.fn)) {
        const amount = this.#amountOf(call, args, scratch, f, n);
        yield* ranking(call.fn as RankingKind, peers, amount, out, this.slice);
        scratch.big[f] = null;
        continue;
      }
      const starts = scratch.starts.subarray(0, n);
      const ends = scratch.ends.subarray(0, n);
      const framed = call.fn !== "lag" && call.fn !== "lead";
      if (framed || call.frame !== null)
        yield* frameBounds(call.frame, ordered, peers, key, starts, ends, this.slice);
      const exclude = call.frame?.exclude ?? "no-others";
      if (VALUE.has(call.fn)) {
        if (call.offsetValue !== undefined) continue;
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
        scratch,
        type?.kind === "float32",
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
      for (let p = 0; p < run.pages.length && !this.context.stopped?.(); p++) {
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
    for (let k = 0; k < store.chunkCount && !this.context.stopped?.(); k++) {
      const columns = this.spec.types.map((_, c) => store.chunk(c, k));
      const rows = Math.min(store.chunkRows, store.rows - k * store.chunkRows);
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
    if (this.context.stopped?.()) return;
    // Deliver the valid prefix before inspecting a later frame. LIMIT may finish, or a
    // cursor may cancel while this prefix is borrowed, without ever requesting that row.
    let prefix = count;
    for (const errors of scratch.resultErrors)
      for (let i = 0; i < prefix; i++)
        if (errors[at + i] === 1) {
          prefix = i;
          break;
        }
    if (prefix < count) {
      if (prefix > 0) yield* this.#push(held, scratch, at, prefix, n);
      if (this.context.stopped?.()) return;
      throw new NumericOutOfRangeError("value out of range: overflow");
    }
    // Offsets and defaults can throw or be volatile. Split scalar-offset rows and rows
    // needing a lazy default, evaluating each after its preceding output is consumed.
    // Other batches retain their normal vector path. The temporary segment is owned through readiness.
    if (
      count > 1 &&
      this.spec.functions.some(
        (call, f) =>
          call.offsetValue !== undefined ||
          (call.defaultValue !== undefined &&
            scratch.targets[f]?.subarray(at, at + count).includes(DEFAULT_ROW)),
      )
    ) {
      let from = 0;
      while (from < count && !this.context.stopped?.()) {
        const needsDefault = (row: number) =>
          this.spec.functions.some(
            (call, f) =>
              call.offsetValue !== undefined ||
              (call.defaultValue !== undefined && scratch.targets[f]?.[at + row] === DEFAULT_ROW),
          );
        let end = from + 1;
        if (!needsDefault(from)) while (end < count && !needsDefault(end)) end++;
        if (from === 0) yield* this.#push(held, scratch, at, end, n);
        else {
          const segment = (this.#defaultBatch ??= new ColumnStore(this.spec.types, this.pool, {
            chunkRows: BATCH_ROWS,
            varlenBytes: 4096,
          }));
          segment.reset(true);
          let done = from;
          while (done < end) {
            const taken = segment.append(held, null, done, end - done);
            if (taken === 0) yield this.io.need(segment.appendNeed(held, done));
            done += taken;
          }
          yield* this.#push(segment.vectors(), scratch, at + from, end - from, n);
        }
        from = end;
      }
      return;
    }
    const columns: Vector[] = this.#withPartition([...held], count);
    for (let f = 0; f < this.spec.functions.length; f++) {
      const call = this.spec.functions[f];
      const output = this.#outputs[f];
      if (call === undefined || output === undefined) continue;
      if (output.gather !== null) {
        const source = this.#sources[f];
        const targets = scratch.targets[f];
        if (source === null || source === undefined || targets === undefined) continue;
        if (call.offsetValue !== undefined) {
          const offset = call.offsetValue({ length: count, columns, selection: null });
          for (let row = 0; row < count; row++) {
            const [flat, slot] = slotOf(offset, row);
            if (!isValidSlot(flat, slot)) targets[at + row] = NULL_ROW;
            else {
              if (flat.data.layout !== "i32") throw new TypeError("Window offset must be int4");
              const amount = flat.data.values[slot] ?? 0;
              const target = call.fn === "lag" ? at + row - amount : at + row + amount;
              targets[at + row] = target >= 0 && target < n ? target : DEFAULT_ROW;
            }
          }
        }
        let fallback: Vector | null = null;
        if (call.defaultValue !== undefined) {
          // Reuse the charged id scratch as selection storage, then restore gather ids.
          const rows = new Uint16Array(output.ids.buffer, 0, BATCH_ROWS);
          let selected = 0;
          for (let i = 0; i < count; i++) if (targets[at + i] === DEFAULT_ROW) rows[selected++] = i;
          if (selected > 0)
            fallback = call.defaultValue({
              length: count,
              columns: this.#withPartition([...held], count),
              selection: { rows, count: selected },
            });
        }
        for (let i = 0; i < count; i++) {
          const t = targets[at + i] ?? NULL_ROW;
          output.ids[i] = t === NULL_ROW ? n : t === DEFAULT_ROW ? n + 1 : t;
        }
        output.gather.reset(true);
        let taken = 0;
        while (taken < count) {
          const useDefault = fallback !== null && targets[at + taken] === DEFAULT_ROW;
          let end = taken + 1;
          while (
            end < count &&
            (fallback !== null && targets[at + end] === DEFAULT_ROW) === useDefault
          )
            end++;
          const amount =
            useDefault && fallback !== null
              ? output.gather.append([fallback], null, taken, end - taken)
              : output.gather.appendFrom(source, [0], output.ids, taken, end - taken);
          if (amount === 0)
            yield this.io.need(
              useDefault && fallback !== null
                ? output.gather.appendNeed([fallback], taken)
                : output.gather.appendFromNeed(source, [0], output.ids[taken] ?? 0),
            );
          taken += amount;
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
    this.#scratch?.big.fill(null);
    if (this.#bigBytes > 0) this.charge.free(this.#bigBytes);
    this.#bigBytes = 0;
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
    this.#block?.close();
    this.#block = null;
    this.#partitionStore?.close();
    this.#partitionStore = null;
    this.#partitionValue = null;
    this.#last.close();
    this.#staged?.close();
    this.#staged = null;
    if (this.#scratchBytes > 0) this.charge.free(this.#scratchBytes);
    if (this.#newGroupBytes > 0) this.charge.free(this.#newGroupBytes);
    this.#scratch?.big.fill(null);
    if (this.#bigBytes > 0) this.charge.free(this.#bigBytes);
    this.#bigBytes = 0;
    this.charge.free(
      this.spec.functions.length * (BATCH_ROWS * 32 + 3 * WINDOW_FLOAT_STATE_BYTES) +
        2 * this.#partChanged.length,
    );
    this.#scratchBytes = 0;
    this.#newGroupBytes = 0;
    this.#loan?.repay();
    this.#loan = null;
    this.#defaultBatch?.close();
    this.#defaultBatch = null;
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
  readonly resultErrors: Uint8Array[];
  readonly targets: Int32Array[];
  readonly argValues: Float64Array[];
  readonly argValid: Uint8Array[];
  readonly amountValues: Float64Array[];
  readonly amountValid: Uint8Array[];
  /** Exact results past 2^53 (bigints, charged with the scratch's per-row allowance). */
  readonly big: Array<bigint[] | null>;
  /** The aggregate kernels' prefix arrays (capacity + 1; the 12-byte per-row allowance). */
  readonly counts: Int32Array;
  readonly prefix: Float64Array;

  static bytes(capacity: number, functions: number, args: number): number {
    // Peers (5 × 4), bounds (2 × 4), per function 23 (results, errors, targets, a constant amount), per
    // argument 9, and an allowance for the kernels' prefix arrays (12 per row).
    return capacity * (20 + 8 + 23 * functions + 9 * args + 12) + 12;
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
    this.resultErrors = Array.from({ length: functions }, () => new Uint8Array(capacity));
    this.targets = Array.from({ length: functions }, () => new Int32Array(capacity));
    this.argValues = Array.from({ length: args }, () => new Float64Array(capacity));
    this.argValid = Array.from({ length: args }, () => new Uint8Array(capacity));
    this.amountValues = Array.from({ length: functions }, () => new Float64Array(capacity));
    this.amountValid = Array.from({ length: functions }, () => new Uint8Array(capacity));
    this.big = Array.from({ length: functions }, () => null);
    this.counts = new Int32Array(capacity + 1);
    this.prefix = new Float64Array(capacity + 1);
  }
}

function columnOf<T>(columns: readonly T[], c: number): T {
  const column = columns[c];
  if (column === undefined) throw new RangeError(`No column ${String(c)}`);
  return column;
}
