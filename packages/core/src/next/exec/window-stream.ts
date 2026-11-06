/**
 * Streaming evaluation of one window partition too large to hold (ruling on review B7-p2d):
 * memory is bounded by the frame, not the partition.
 *
 * The partition's rows are a spilled run, in order. A first pass (only when a function needs
 * it) counts the rows and takes whole-partition totals and the last row. The main pass reads
 * the pages once, keeping a deque of decoded pages and ring arrays per row (argument values,
 * sequential cumulative sums and counts, running extremes, peer groups, RANGE keys) only for
 * the rows some function can still need: from the earliest frame start (or lag target, or the
 * peer start an EXCLUDE needs) of the next row to output, to the rows read ahead. A row goes out
 * once every function's frame for it is complete: its frame end is read (ROWS / GROUPS / RANGE
 * offsets, peers, lead offsets) or the partition is over.
 *
 * Results equal the resident evaluator's (window-frames.ts) exactly:
 * - Exact sums and counts are differences of cumulative values (bigints past 2^53).
 * - Float sums add the frame's members in order. A frame that starts at the partition's head
 *   uses the cumulative sum (itself a left-to-right sum), then adds the rest of the members in
 *   order. A moving frame re-adds its members from the ring.
 * - MIN / MAX take a running extreme for a head-anchored prefix, else scan the frame's members.
 *
 * Not streamed: a moving-head frame ending at UNBOUNDED FOLLOWING for float sums or MIN/MAX,
 * the same with EXCLUDE (both would re-read the rest of the partition per row, as PostgreSQL
 * re-aggregates), lag/lead/ntile/nth_value amounts from a column, and nth_value past
 * `NTH_LIMIT`. `streamable` names them; the operator refuses such a partition past its
 * resident limit.
 */
import type { PhysicalType } from "../types.js";
import type { FlatVector, Vector } from "../vector/vector.js";
import { BATCH_ROWS } from "../vector/vector.js";
import { ColumnStore, nullConstant } from "./column-store.js";
import { keysEqual, markChanges, type SortColumn } from "./sort-keys.js";
import type { Slice, SlicedJob } from "./slices.js";
import type { SpilledRun } from "./spill-pages.js";
import type { Charge, Pool, SpillIo } from "./spill-pool.js";
import {
  WINDOW_STEP_ROWS,
  floatRangeContains,
  WindowError,
  type Frame,
  type FrameBound,
} from "./window-frames.js";

/** nth_value's largest n a stream serves from its copy of the partition's first rows. */
export const NTH_LIMIT = 4096;

export interface StreamFunction {
  readonly fn: string;
  readonly arg: number | null;
  readonly amount?: number | { readonly column: number } | null;
  readonly fallback?: Vector | null;
  readonly frame: Frame | null;
  readonly type: PhysicalType;
}

export interface StreamSpec {
  readonly types: readonly PhysicalType[];
  readonly orderBy: readonly SortColumn[];
  readonly functions: readonly StreamFunction[];
}

const VALUE = new Set(["lag", "lead", "first_value", "last_value", "nth_value"]);

function integral(type: PhysicalType | undefined): boolean {
  return type?.kind === "int32" || type?.kind === "int64";
}

/** The frame a call uses (PostgreSQL's default when null). */
function effective(call: StreamFunction, ordered: boolean): Frame {
  if (call.frame !== null) return call.frame;
  const up: FrameBound = { kind: "unbounded-preceding", offset: null };
  return ordered
    ? { unit: "range", start: up, end: { kind: "current-row", offset: null }, exclude: "no-others" }
    : {
        unit: "rows",
        start: up,
        end: { kind: "unbounded-following", offset: null },
        exclude: "no-others",
      };
}

/** Why a call cannot stream, or null when it can. */
export function streamable(spec: StreamSpec, call: StreamFunction): string | null {
  const amount = call.amount;
  if (amount !== undefined && amount !== null && typeof amount !== "number")
    return `${call.fn} with an amount from a column`;
  if (call.fn === "nth_value" && typeof amount === "number" && amount > NTH_LIMIT)
    return `nth_value past ${String(NTH_LIMIT)}`;
  if (
    VALUE.has(call.fn) ||
    ["row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "ntile"].includes(call.fn)
  )
    return null;
  const frame = effective(call, spec.orderBy.length > 0);
  const type = call.arg === null ? undefined : spec.types[call.arg];
  const exact =
    call.fn === "count" ||
    call.fn === "count_star" ||
    ((call.fn === "sum" || call.fn === "avg") && integral(type));
  if (frame.end.kind === "unbounded-following" && !exact)
    if (frame.start.kind !== "unbounded-preceding" || frame.exclude !== "no-others")
      return `${call.fn} over a moving frame to UNBOUNDED FOLLOWING`;
  return null;
}

/** Whether a call needs the partition's row count (or totals) before its first row. */
function needsCount(call: StreamFunction, ordered: boolean): boolean {
  if (call.fn === "ntile" || call.fn === "percent_rank" || call.fn === "cume_dist") return true;
  if (
    call.fn === "lag" ||
    call.fn === "lead" ||
    call.fn === "row_number" ||
    call.fn === "rank" ||
    call.fn === "dense_rank"
  )
    return false;
  return effective(call, ordered).end.kind === "unbounded-following";
}

/** Per-call state over the ring. */
interface CallState {
  readonly call: StreamFunction;
  /** A one-row NULL of the call's type (made once, for the gathers of NULL results). */
  nullValue?: Vector;
  readonly frame: Frame;
  readonly exact: boolean;
  /** SUM / AVG keep cumulative sums; COUNT only counts (review B7-p2d-1 #1). */
  readonly sums: boolean;
  /** MIN / MAX keep extremes, exactly (bigints) once an int64 value passes 2^53 (#2). */
  readonly extremes: boolean;
  /** Exact argument values and running extremes by ring row, once a value passes 2^53. */
  vBig: bigint[] | null;
  extremeBig: bigint[] | null;
  runExtremeBig: bigint | null;
  totalExtremeBig: bigint | null;
  /** Ring arrays: argument value, validity, cumulative sum / count through the row, running extreme. */
  v: Float64Array;
  ok: Uint8Array;
  sum: Float64Array;
  count: Float64Array;
  extreme: Float64Array;
  /** Exact cumulative sums past 2^53 (bigints, same indexing), else null. */
  big: bigint[] | null;
  /** Running totals while reading (the next row's cumulative values start from them). */
  runSum: number;
  runBig: bigint | null;
  runCount: number;
  runExtreme: number;
  runSeen: boolean;
  /** Pass A totals. */
  totalSum: number;
  totalBig: bigint | null;
  totalCount: number;
  totalExtreme: number;
  totalSeen: boolean;
  /** Value functions: the partition's first rows (nth / first) and its last row. */
  head: ColumnStore | null;
  last: ColumnStore | null;
}

interface Page {
  readonly start: number;
  readonly rows: number;
  readonly columns: readonly FlatVector[];
  readonly held: { bytes: Uint8Array; charged: number };
}

export interface StreamHost {
  readonly io: SpillIo;
  readonly charge: Charge;
  /** Output and gather stores draw from the spill pool (room made per bounded step). */
  readonly pool: Pool;
  readonly slice: Slice;
  /** Pushes `count` dense rows (payload columns then one column per function). */
  push(columns: Vector[], count: number): SlicedJob<void>;
  /** A numeric function's results as a vector of its type. */
  resultVector(
    call: StreamFunction,
    f: number,
    values: Float64Array,
    valid: Uint8Array,
    big: bigint[] | null,
    count: number,
  ): FlatVector;
}

const SAFE = 9007199254740991;
/** Charged bytes of one bigint ring slot (the array slot and a heap BigInt). */
const BIGINT_BYTES = 48;

/** An int64 value past 2^53 (its double rounds: only its bigint is exact). */
function wide(x: { value: number; big: bigint | null }): boolean {
  return x.big !== null && (x.value > SAFE || x.value < -SAFE);
}

function numberAt(vector: Vector, row: number): { ok: boolean; value: number; big: bigint | null } {
  const flat =
    vector.form === "flat" ? vector : vector.form === "constant" ? vector.value : vector.dictionary;
  const slot =
    vector.form === "flat" ? row : vector.form === "constant" ? 0 : (vector.codes[row] ?? 0);
  const validity = vector.form === "constant" ? vector.value.validity : vector.validity;
  const r = vector.form === "constant" ? 0 : row;
  if (validity !== null && ((validity[r >>> 5] ?? 0) & (1 << (r & 31))) === 0)
    return { ok: false, value: 0, big: null };
  const data = flat.data;
  switch (data.layout) {
    case "i32":
    case "f32":
    case "f64":
    case "bool":
      return { ok: true, value: data.values[slot] ?? 0, big: null };
    case "int64": {
      if (data.values.repr === "safe")
        return { ok: true, value: data.values.values[slot] ?? 0, big: null };
      const v = data.values.values[slot] ?? 0n;
      return { ok: true, value: Number(v), big: v };
    }
    default:
      return { ok: true, value: 0, big: null };
  }
}

/** Streams one spilled partition (see the module comment). */
export class PartitionStream {
  readonly #calls: CallState[];
  readonly #ordered: boolean;
  #n = -1;
  /** Peer groups in the partition (the counting pass), or −1 when not counted. */
  #totalGroups = -1;
  /** The next row to output (diagnostics). */
  #next = 0;
  #pages: Page[] = [];
  /** Partition rows read (the next row the reader gives is `#read`). */
  #read = 0;
  #ended = false;
  /** Ring base and capacity (rows `#base … #read` live in the ring arrays). */
  #base = 0;
  #capacity = 0;
  #ringBytes = 0;
  #resultBytes = 0;
  #resultBig: Array<bigint[] | null> = [];
  // Peer groups: each row's group, each group's first row (from `#groupBase`).
  #group = new Int32Array(0);
  #groupStarts: number[] = [];
  #groupBase = 0;
  #groups = 0;
  #key = new Float64Array(0);
  #keyOk = new Uint8Array(0);
  /** Exact RANGE keys by ring row, once a key passes 2^53 (else null). */
  #keyBig: bigint[] | null = null;
  /** Whether a call has a RANGE frame (its offsets compare the first key). */
  readonly #ranged: boolean;
  /** Bigint ring arrays live (each charged BIGINT_BYTES a ring slot). */
  #bigArrays = 0;
  readonly #prev: ColumnStore;
  #prevVectors: Vector[] = [];
  /** The current page's peer changes (`markChanges`), charged as it grows. */
  #changed = new Uint8Array(0);
  readonly #orderColumns: readonly number[];
  readonly #gathers: Array<ColumnStore | null>;
  readonly #out: ColumnStore;

  constructor(
    readonly spec: StreamSpec,
    readonly host: StreamHost,
  ) {
    this.#ordered = spec.orderBy.length > 0;
    this.#ranged = spec.functions.some((call) => call.frame?.unit === "range");
    this.#orderColumns = spec.orderBy.map((key) => key.column);
    this.#prev = new ColumnStore(
      this.#orderColumns.map((c) => spec.types[c] ?? { kind: "int32", bits: 32 }),
      host.charge,
      { chunkRows: 1, varlenBytes: 64 },
    );
    this.#out = new ColumnStore(spec.types, host.pool, { targetBytes: 1 << 20 });
    this.#calls = spec.functions.map((call) => {
      const type = call.arg === null ? undefined : spec.types[call.arg];
      const value = VALUE.has(call.fn) && call.arg !== null;
      return {
        call,
        frame: effective(call, this.#ordered),
        exact: call.fn === "count" || call.fn === "count_star" || integral(type),
        sums: call.fn === "sum" || call.fn === "avg",
        extremes: call.fn === "min" || call.fn === "max",
        vBig: null,
        extremeBig: null,
        runExtremeBig: null,
        totalExtremeBig: null,
        v: new Float64Array(0),
        ok: new Uint8Array(0),
        sum: new Float64Array(0),
        count: new Float64Array(0),
        extreme: new Float64Array(0),
        big: null,
        runSum: 0,
        runBig: null,
        runCount: 0,
        runExtreme: Number.NaN,
        runSeen: false,
        totalSum: 0,
        totalBig: null,
        totalCount: 0,
        totalExtreme: Number.NaN,
        totalSeen: false,
        head: value
          ? new ColumnStore([type ?? call.type], host.charge, { varlenBytes: 4096 })
          : null,
        last: value
          ? new ColumnStore([type ?? call.type], host.charge, { chunkRows: 1, varlenBytes: 64 })
          : null,
      };
    });
    this.#gathers = spec.functions.map((call) =>
      VALUE.has(call.fn) ? new ColumnStore([call.type], host.pool, { varlenBytes: 4096 }) : null,
    );
  }

  /** Frees everything it holds. */
  close(): void {
    for (const page of this.#pages) this.host.io.releasePage(page.held);
    this.#pages = [];
    this.#resultBig.fill(null);
    if (this.#resultBytes > 0) this.host.charge.free(this.#resultBytes);
    this.#resultBytes = 0;
    this.#keyBig = null;
    for (const state of this.#calls) {
      state.big = null;
      state.vBig = null;
      state.extremeBig = null;
      state.runBig = null;
      state.runExtremeBig = null;
      state.totalBig = null;
      state.totalExtremeBig = null;
    }
    if (this.#ringBytes > 0) this.host.charge.free(this.#ringBytes);
    this.#ringBytes = 0;
    this.#prev.close();
    if (this.#changed.length > 0) this.host.charge.free(this.#changed.length);
    this.#changed = new Uint8Array(0);
    this.#out.close();
    for (const state of this.#calls) {
      state.head?.close();
      state.last?.close();
    }
    for (const gather of this.#gathers) gather?.close();
  }

  *run(run: SpilledRun): SlicedJob<void> {
    if (this.#calls.some((state) => needsCount(state.call, this.#ordered))) yield* this.#count(run);
    yield* this.#main(run);
  }

  // --- pass A ----------------------------------------------------------------------------

  *#readPage(run: SpilledRun, p: number): SlicedJob<Page> {
    let held: { bytes: Uint8Array; charged: number } | null = null;
    yield this.host.io.readPage(run, p).then((page) => {
      held = page;
    });
    const page = held as { bytes: Uint8Array; charged: number } | null;
    if (page === null) throw new RangeError("A window page was not read");
    let columns: readonly FlatVector[] = [];
    let rows = 0;
    yield this.host.io.decode(page, this.spec.types).then((decoded) => {
      columns = decoded.columns;
      rows = decoded.rows;
    });
    return { start: 0, rows, columns, held: page };
  }

  *#count(run: SpilledRun): SlicedJob<void> {
    let n = 0;
    let groups = 0;
    const prev = new ColumnStore(
      this.#orderColumns.map((c) => this.spec.types[c] ?? { kind: "int32", bits: 32 }),
      this.host.charge,
      { chunkRows: 1, varlenBytes: 64 },
    );
    let prevVectors: Vector[] = [];
    for (let p = 0; p < run.pages.length; p++) {
      const page = yield* this.#readPage(run, p);
      try {
        for (const state of this.#calls) {
          const arg = state.call.arg;
          if (arg !== null && !VALUE.has(state.call.fn)) {
            const vector = page.columns[arg];
            if (vector === undefined) continue;
            for (let r = 0; r < page.rows; r++) {
              const x = numberAt(vector, r);
              if (!x.ok) continue;
              state.totalCount++;
              if (state.sums) {
                if (
                  state.exact &&
                  (state.totalBig !== null ||
                    x.big !== null ||
                    Math.abs(state.totalSum + x.value) > SAFE)
                ) {
                  state.totalBig =
                    (state.totalBig ?? BigInt(state.totalSum)) + (x.big ?? BigInt(x.value));
                } else state.totalSum += x.value;
              }
              if (state.extremes) {
                const max = state.call.fn === "max";
                if (state.totalExtremeBig !== null || wide(x)) {
                  const v = x.big ?? BigInt(x.value);
                  const best =
                    state.totalExtremeBig ?? (state.totalSeen ? BigInt(state.totalExtreme) : null);
                  state.totalExtremeBig = best === null || (max ? v > best : v < best) ? v : best;
                  state.totalExtreme = Number(state.totalExtremeBig);
                } else if (!state.totalSeen || better(x.value, state.totalExtreme, max))
                  state.totalExtreme = x.value;
                state.totalSeen = true;
              }
            }
          }
          if (state.last !== null && arg !== null && page.rows > 0) {
            state.last.reset();
            state.last.append(
              [page.columns[arg] ?? nullConstant(state.call.type, 1)],
              null,
              page.rows - 1,
              1,
            );
          }
        }
        // Peer groups (the last group's end is then known without reading ahead).
        const types = this.spec.types;
        const order = this.spec.orderBy;
        for (let r = 0; r < page.rows; r++) {
          const same =
            n + r > 0 &&
            (r === 0
              ? keysEqual(types, order, prevVectors, 0, page.columns, r)
              : keysEqual(types, order, page.columns, r - 1, page.columns, r));
          if (!same) groups++;
        }
        if (page.rows > 0 && this.#orderColumns.length > 0) {
          prev.reset();
          prev.append(
            this.#orderColumns.map(
              (c) => page.columns[c] ?? nullConstant({ kind: "int32", bits: 32 }, 1),
            ),
            null,
            page.rows - 1,
            1,
          );
          const held = prev.vectors();
          prevVectors = [];
          this.#orderColumns.forEach((c, k) => {
            const v = held[k];
            if (v !== undefined) prevVectors[c] = v;
          });
        }
        n += page.rows;
      } finally {
        this.host.io.releasePage(page.held);
      }
      if (this.host.slice.due()) yield;
    }
    prev.close();
    this.#n = n;
    this.#totalGroups = groups;
  }

  // --- the ring --------------------------------------------------------------------------

  /** Grows or compacts the ring so row `#read` fits (keeping rows from `keep`). */
  #fit(keep: number): void {
    const need = this.#read - this.#base + 1;
    if (need <= this.#capacity) return;
    // Rows before `keep` are no longer needed: slide the live rows down when that frees half.
    const drop = Math.max(0, keep - this.#base);
    if (drop >= this.#capacity / 2 && drop > 0) {
      this.#slide(drop);
      return;
    }
    const capacity = Math.max(1024, this.#capacity * 2);
    const bytes = capacity * this.#perRow() + this.#bigArrays * BIGINT_BYTES;
    // The new arrays are charged in full while the old ones are still live (governor rule 4:
    // the overlap), and the old charge goes once they are copied (review B7-p2d-1 #5).
    if (!this.host.charge.allocate(bytes))
      throw new RangeError(
        `A window frame's rows do not fit the grant (rows ${String(this.#base)}…${String(this.#read)} kept from ${String(keep)}; next ${String(this.#next)} ready ${String(this.#next < this.#read && this.#ready(this.#next))} bounds ${this.#next < this.#read ? this.#calls.map((c) => JSON.stringify(this.#bounds(c, this.#next))).join(" ") : "-"}; ${this.#calls.map((c) => c.call.fn + (c.call.frame === null ? "" : " " + c.frame.unit + " " + c.frame.start.kind + ".." + c.frame.end.kind)).join(", ")})`,
      );
    const old = this.#ringBytes;
    this.#ringBytes = bytes;
    const grow = <T extends Float64Array | Int32Array | Uint8Array>(
      old: T,
      make: (n: number) => T,
    ): T => {
      const next = make(capacity);
      next.set(old.subarray(0, this.#read - this.#base));
      return next;
    };
    this.#group = grow(this.#group, (n) => new Int32Array(n));
    this.#key = grow(this.#key, (n) => new Float64Array(n));
    this.#keyOk = grow(this.#keyOk, (n) => new Uint8Array(n));
    for (const s of this.#calls) {
      s.v = grow(s.v, (n) => new Float64Array(n));
      s.ok = grow(s.ok, (n) => new Uint8Array(n));
      s.sum = grow(s.sum, (n) => new Float64Array(n));
      s.count = grow(s.count, (n) => new Float64Array(n));
      s.extreme = grow(s.extreme, (n) => new Float64Array(n));
    }
    // Bigint backing arrays also grow at explicit capacities: no uncharged push/splice
    // spare capacity. Their replacements overlap the old arrays under the charge above.
    const growBig = (array: bigint[] | null): bigint[] | null => {
      if (array === null) return null;
      const next = new Array<bigint>(capacity);
      for (let i = 0; i < this.#read - this.#base; i++) next[i] = array[i] ?? 0n;
      return next;
    };
    this.#keyBig = growBig(this.#keyBig);
    for (const s of this.#calls) {
      s.big = growBig(s.big);
      s.vBig = growBig(s.vBig);
      s.extremeBig = growBig(s.extremeBig);
    }
    this.#capacity = capacity;
    this.host.charge.free(old);
  }

  /** Ring bytes a row: the typed arrays, and each live bigint array's slot (BIGINT_BYTES). */
  #perRow(): number {
    return 4 + 9 + this.#calls.length * (8 + 1 + 8 + 8 + 8) + this.#bigArrays * BIGINT_BYTES;
  }

  /**
   * A bigint ring array goes live (exact sums, extremes or RANGE keys): its slots are charged
   * for the ring's capacity now, and with every ring growth after.
   */
  #chargeBig(): void {
    const bytes = (Math.max(this.#capacity, 1024) + 1) * BIGINT_BYTES;
    if (!this.host.charge.allocate(bytes))
      throw new RangeError("A window frame's exact (bigint) values do not fit the grant");
    this.#ringBytes += bytes;
    this.#bigArrays++;
  }

  /** MIN / MAX: exact values and extremes from ring row `at` on (earlier ones were safe). */
  #widen(s: CallState, at: number): void {
    this.#chargeBig();
    this.#chargeBig();
    s.vBig = new Array<bigint>(this.#capacity);
    s.extremeBig = new Array<bigint>(this.#capacity);
    for (let k = 0; k < at; k++) {
      s.vBig[k] = s.ok[k] === 1 ? BigInt(s.v[k] ?? 0) : 0n;
      s.extremeBig[k] = (s.count[k] ?? 0) > 0 ? BigInt(s.extreme[k] ?? 0) : 0n;
    }
    if (s.runSeen) s.runExtremeBig = BigInt(s.runExtreme);
  }

  #slide(drop: number): void {
    const live = this.#read - this.#base - drop;
    const move = (a: Float64Array | Int32Array | Uint8Array) => a.copyWithin(0, drop, drop + live);
    move(this.#group);
    move(this.#key);
    move(this.#keyOk);
    const moveBig = (array: bigint[] | null) => {
      if (array === null) return;
      array.copyWithin(0, drop, drop + live);
      array.fill(0n, live);
    };
    for (const s of this.#calls) {
      move(s.v);
      move(s.ok);
      move(s.sum);
      move(s.count);
      move(s.extreme);
      moveBig(s.big);
      moveBig(s.vBig);
      moveBig(s.extremeBig);
    }
    moveBig(this.#keyBig);
    this.#base += drop;
  }

  // --- the main pass ---------------------------------------------------------------------

  /** The page's peer changes into `#changed` (row r against row r − 1, for r ≥ 1). */
  #pageChanges(page: Page): void {
    if (this.#orderColumns.length === 0) return;
    if (page.rows > this.#changed.length) {
      const size = Math.max(page.rows, 2 * this.#changed.length, 512);
      if (!this.host.charge.allocate(size))
        throw new RangeError("A window stream's peer flags could not be allocated");
      this.host.charge.free(this.#changed.length);
      this.#changed = new Uint8Array(size);
    }
    this.#changed.fill(0, 0, page.rows);
    markChanges(this.spec.types, this.spec.orderBy, page.columns, page.rows, this.#changed);
  }

  /** Takes row `#read` from `page` (row r) into the ring. */
  #take(page: Page, r: number, keep: number): void {
    this.#fit(keep);
    const j = this.#read;
    const at = j - this.#base;
    // Peer group: a new one when the ORDER BY keys differ from the previous row's (within the
    // page: its flags; its first row: the last row of the page before, kept in `#prev`).
    const columns = page.columns;
    const same =
      j > 0 &&
      (this.#orderColumns.length === 0 ||
        (r > 0
          ? this.#changed[r] === 0
          : keysEqual(this.spec.types, this.spec.orderBy, this.#prevVectors, 0, columns, r)));
    if (!same) {
      this.#groupStarts.push(j);
      this.#groups++;
    }
    this.#group[at] = this.#groups - 1;
    if (this.#orderColumns.length > 0 && r === page.rows - 1) {
      this.#prev.reset();
      this.#prev.append(
        this.#orderColumns.map((c) => columns[c] ?? nullConstant({ kind: "int32", bits: 32 }, 1)),
        null,
        r,
        1,
      );
      const held = this.#prev.vectors();
      const vectors: Vector[] = [];
      this.#orderColumns.forEach((c, k) => {
        const v = held[k];
        if (v !== undefined) vectors[c] = v;
      });
      this.#prevVectors = vectors;
    }
    if (this.#orderColumns.length > 0) {
      const key = numberAt(
        columns[this.#orderColumns[0] ?? 0] ?? nullConstant({ kind: "int32", bits: 32 }, 1),
        r,
      );
      this.#key[at] = key.value;
      this.#keyOk[at] = key.ok ? 1 : 0;
      // A RANGE key past 2^53 keeps its exact value (review B7-p2d-1 #3).
      if (this.#keyBig === null && key.ok && wide(key) && this.#ranged) {
        this.#chargeBig();
        this.#keyBig = new Array<bigint>(this.#capacity);
        for (let k = 0; k < at; k++) this.#keyBig[k] = BigInt(this.#key[k] ?? 0);
      }
      if (this.#keyBig !== null) this.#keyBig[at] = key.ok ? (key.big ?? BigInt(key.value)) : 0n;
    }
    for (const s of this.#calls) {
      const arg = s.call.arg;
      if (arg === null) {
        s.ok[at] = 1;
        s.count[at] = ++s.runCount;
        continue;
      }
      if (VALUE.has(s.call.fn)) {
        if (s.head !== null && j < NTH_LIMIT)
          s.head.append([columns[arg] ?? nullConstant(s.call.type, 1)], null, r, 1);
        continue;
      }
      const x = numberAt(columns[arg] ?? nullConstant(s.call.type, 1), r);
      s.v[at] = x.value;
      s.ok[at] = x.ok ? 1 : 0;
      if (s.extremes && s.vBig === null && x.ok && wide(x)) this.#widen(s, at);
      if (s.vBig !== null) s.vBig[at] = x.ok ? (x.big ?? BigInt(x.value)) : 0n;
      if (x.ok) {
        s.runCount++;
        if (s.sums) {
          if (
            s.exact &&
            (s.runBig !== null || x.big !== null || Math.abs(s.runSum + x.value) > SAFE)
          ) {
            if (s.big === null) {
              this.#chargeBig();
              s.big = new Array<bigint>(this.#capacity);
              for (let k = 0; k < at; k++) s.big[k] = BigInt(s.sum[k] ?? 0);
            }
            s.runBig = (s.runBig ?? BigInt(s.runSum)) + (x.big ?? BigInt(x.value));
          } else s.runSum += x.value;
        }
        if (s.extremes) {
          const max = s.call.fn === "max";
          if (s.vBig !== null) {
            const v = s.vBig[at] ?? 0n;
            const best = s.runExtremeBig;
            s.runExtremeBig = best === null || (max ? v > best : v < best) ? v : best;
            s.runExtreme = Number(s.runExtremeBig);
          } else if (!s.runSeen || better(x.value, s.runExtreme, max)) s.runExtreme = x.value;
          s.runSeen = true;
        }
      }
      s.sum[at] = s.runSum;
      if (s.big !== null) s.big[at] = s.runBig ?? BigInt(s.runSum);
      s.count[at] = s.runCount;
      s.extreme[at] = s.runSeen ? s.runExtreme : Number.NaN;
      if (s.extremeBig !== null) s.extremeBig[at] = s.runExtremeBig ?? 0n;
    }
    this.#read++;
  }

  /** The first row of group g, or −1 when not known yet. */
  #groupStart(g: number): number {
    if (g < 0) return 0;
    if (this.#totalGroups >= 0 && g >= this.#totalGroups) return this.#n;
    if (g >= this.#groups) return this.#ended ? this.#total() : -1;
    return this.#groupStarts[g - this.#groupBase] ?? -1;
  }

  /** The last row of group g, or −1 when not known yet. */
  #groupEnd(g: number): number {
    if (this.#totalGroups >= 0 && g >= this.#totalGroups - 1) return this.#n - 1;
    if (g >= this.#groups) return this.#ended ? this.#total() - 1 : -1;
    if (g + 1 < this.#groups) return (this.#groupStarts[g + 1 - this.#groupBase] ?? 0) - 1;
    return this.#ended ? this.#read - 1 : -1;
  }

  #total(): number {
    return this.#n >= 0 ? this.#n : this.#read;
  }

  #groupOf(i: number): number {
    return this.#group[i - this.#base] ?? 0;
  }

  /** One bound of row i's frame for a state, or null when not known yet. */
  #bound(s: CallState, i: number, b: FrameBound, end: boolean): number | null {
    const frame = s.frame;
    const g = this.#groupOf(i);
    const ps = this.#groupStart(g);
    const pe = this.#groupEnd(g);
    {
      switch (b.kind) {
        case "unbounded-preceding":
          return 0;
        case "unbounded-following":
          return this.#n >= 0 ? this.#n - 1 : this.#ended ? this.#read - 1 : null;
        case "current-row":
          if (frame.unit === "rows") return i;
          return end ? (pe < 0 ? null : pe) : ps;
      }
      const offset = b.offset;
      if (offset === null) throw new WindowError("frame offset must not be null", "22004");
      if (offset < 0 || Number.isNaN(offset))
        throw new WindowError("frame offset must not be negative or NaN", "22013");
      const sign = b.kind === "preceding" ? -1 : 1;
      if (frame.unit === "rows") {
        const at = i + sign * offset;
        if (end) {
          if (at < this.#read) return at;
          return this.#ended ? this.#read - 1 : null;
        }
        if (at <= 0) return 0;
        if (at < this.#read) return at;
        return this.#ended ? this.#read : null;
      }
      if (frame.unit === "groups") {
        const target = g + sign * offset;
        if (end) {
          if (target < 0) return -1;
          const e = this.#groupEnd(target);
          return e < 0 ? null : e;
        }
        const st = this.#groupStart(target);
        return st < 0 ? null : st;
      }
      return this.#rangeBound(i, sign * offset, end, ps, pe);
    }
  }

  /**
   * Frame bounds of row i for a state: [s, e] (e may be n − 1 past the rows read when the
   * count is known), or null when not known yet.
   */
  #bounds(s: CallState, i: number): [number, number] | null {
    const a = this.#bound(s, i, s.frame.start, false);
    const b = this.#bound(s, i, s.frame.end, true);
    if (a === null || b === null) return null;
    return [a, b];
  }

  /** RANGE offset bound of row i (keys sorted along the direction; NULLs frame their peers). */
  #rangeBound(i: number, delta: number, end: boolean, ps: number, pe: number): number | null {
    const at = i - this.#base;
    if (this.#keyOk[at] !== 1) return end ? (pe < 0 ? null : pe) : ps;
    if (this.#keyBig !== null) return this.#rangeBoundExact(i, delta, end);
    const base = this.#key[at] ?? 0;
    // Finite offsets cannot move an infinity; no offset moves a NaN. Their boundary is
    // the peer edge even when older peer rows have left the ring.
    if (Number.isNaN(base) || (!Number.isFinite(base) && Number.isFinite(delta)))
      return end ? (pe < 0 ? null : pe) : ps;
    const descending = this.spec.orderBy[0]?.direction === "desc";
    const nullsFirst = this.spec.orderBy[0]?.nulls === "first";
    const contains = (j: number) =>
      this.#keyOk[j - this.#base] !== 1
        ? end
          ? nullsFirst
          : !nullsFirst
        : floatRangeContains(
            this.#key[j - this.#base] ?? 0,
            this.#key[at] ?? 0,
            delta,
            descending,
            end,
          );
    // Find either side of i: offsets leave NaN/infinity peers unchanged, so the boundary
    // need not lie on the side of i suggested by the offset's sign.
    let j = i;
    if (end) {
      while (j >= this.#base && !contains(j)) j--;
      while (j + 1 < this.#read && contains(j + 1)) j++;
      if (j + 1 >= this.#read && !this.#ended) return null;
      return j;
    }
    while (j > this.#base && contains(j - 1)) j--;
    while (j < this.#read && !contains(j)) j++;
    if (j >= this.#read) return this.#ended ? this.#total() : null;
    return j;
  }

  /**
   * `#rangeBound` over exact int64 keys (a key passed 2^53): the same walks, in bigints, the
   * offset applied exactly (review B7-p2d-1 #3). NULL keys are below or above every value.
   */
  #rangeBoundExact(i: number, delta: number, end: boolean): number | null {
    const keys = this.#keyBig;
    if (keys === null) return null;
    if (!Number.isInteger(delta))
      throw new RangeError("A RANGE offset over int8 keys is an integer");
    const dir = this.spec.orderBy[0]?.direction === "desc" ? -1n : 1n;
    const target = (keys[i - this.#base] ?? 0n) * dir + BigInt(delta);
    const nullLow = this.spec.orderBy[0]?.nulls === "first";
    // −1, 0, 1: row j's key (along the direction) against the target.
    const cmp = (j: number): number => {
      if (this.#keyOk[j - this.#base] !== 1) return nullLow ? -1 : 1;
      const v = (keys[j - this.#base] ?? 0n) * dir;
      return v < target ? -1 : v > target ? 1 : 0;
    };
    const okAt = (j: number) => j >= this.#base;
    if (end) {
      if (delta >= 0) {
        let j = i;
        while (j + 1 < this.#read && okAt(j + 1) && cmp(j + 1) <= 0) j++;
        if (j + 1 >= this.#read && !this.#ended) return null;
        return j;
      }
      let j = i;
      while (j >= this.#base && (!okAt(j) || cmp(j) > 0)) j--;
      return j < this.#base ? this.#base - 1 : j;
    }
    if (delta <= 0) {
      let j = i;
      while (j - 1 >= this.#base && okAt(j - 1) && cmp(j - 1) >= 0) j--;
      return j;
    }
    let j = i;
    while (j < this.#read && (!okAt(j) || cmp(j) < 0)) j++;
    if (j >= this.#read) return this.#ended ? this.#total() : null;
    return j;
  }

  /** Members of row i's frame [s, e] after EXCLUDE: up to three ranges. */
  #members(s: CallState, i: number, a: number, b: number, out: number[]): number {
    const g = this.#groupOf(i);
    const ps = this.#groupStart(g);
    const pe = this.#groupEnd(g);
    let k = 0;
    const push = (x: number, y: number) => {
      if (x <= y) {
        out[k++] = x;
        out[k++] = y;
      }
    };
    switch (s.frame.exclude) {
      case "no-others":
        push(a, b);
        break;
      case "current-row":
        push(a, Math.min(b, i - 1));
        push(Math.max(a, i + 1), b);
        break;
      case "group":
        push(a, Math.min(b, ps - 1));
        push(Math.max(a, pe + 1), b);
        break;
      case "ties":
        push(a, Math.min(b, ps - 1));
        if (i >= a && i <= b) push(i, i);
        push(Math.max(a, pe + 1), b);
        break;
    }
    return k >>> 1;
  }

  /** Whether row i can be computed now (every frame end and peer end known). */
  #ready(i: number): boolean {
    for (const s of this.#calls) {
      const fn = s.call.fn;
      if (
        fn === "row_number" ||
        fn === "rank" ||
        fn === "dense_rank" ||
        fn === "ntile" ||
        fn === "percent_rank"
      )
        continue;
      if (fn === "lag" || fn === "lead") {
        const k = typeof s.call.amount === "number" ? s.call.amount : 1;
        const target = fn === "lag" ? i - k : i + k;
        if (target >= this.#read && !this.#ended) return false;
        continue;
      }
      if (fn === "cume_dist") {
        if (this.#groupEnd(this.#groupOf(i)) < 0) return false;
        continue;
      }
      const bounds = this.#bounds(s, i);
      if (bounds === null) return false;
      if (s.frame.exclude !== "no-others" && this.#groupEnd(this.#groupOf(i)) < 0) return false;
      // A value function needs its member row itself (read, or the partition's last row).
      if (VALUE.has(fn) && !this.#ended) {
        const target = this.#target(s, i, bounds);
        if (target >= this.#read && !(target === this.#n - 1 && s.last?.rows === 1)) return false;
        continue;
      }
      // A frame end past the rows read is only served by the pass-A totals (UNBOUNDED FOLLOWING).
      if (bounds[1] >= this.#read && s.frame.end.kind !== "unbounded-following" && !this.#ended)
        return false;
    }
    return true;
  }

  /** The earliest row any call needs for row i (the ring keeps rows from there). */
  #keepFrom(i: number): number {
    // A row not read yet needs at least what the last row read needs (retention only moves on).
    if (i >= this.#read) return this.#read > 0 ? this.#keepFrom(this.#read - 1) : this.#base;
    let keep = i;
    const g = this.#groupOf(i);
    const ps = this.#groupStart(g);
    for (const s of this.#calls) {
      const fn = s.call.fn;
      if (fn === "lag" || fn === "lead") {
        // The target row, whichever way it lies (a negative offset reverses the direction:
        // lead(x, −k) looks back, review B7-p2d-1 #4).
        const k = typeof s.call.amount === "number" ? s.call.amount : 1;
        keep = Math.min(keep, fn === "lag" ? i - k : i + k);
        continue;
      }
      if (
        fn === "row_number" ||
        fn === "rank" ||
        fn === "dense_rank" ||
        fn === "ntile" ||
        fn === "percent_rank" ||
        fn === "cume_dist"
      )
        continue;
      if (s.frame.exclude !== "no-others") keep = Math.min(keep, ps - 1);
      // An end behind the row (… PRECEDING): its row holds the cumulative values read for it.
      if (s.frame.end.kind === "preceding") {
        const end = this.#bound(s, i, s.frame.end, true);
        keep = Math.min(keep, end ?? this.#base);
      }
      if (s.frame.start.kind === "unbounded-preceding") continue;
      // A start ahead of the row needs nothing before it (but the row before, for differences).
      if (s.frame.start.kind === "following" && s.frame.unit === "rows") {
        keep = Math.min(keep, i - 1);
        continue;
      }
      // RANGE/GROUPS FOLLOWING can still start at an earlier peer (zero offsets, NaN
      // or infinity). Retain its actual prefix boundary, not merely i - 1. An unknown
      // start conservatively keeps everything read so far.
      const start = this.#bound(s, i, s.frame.start, false);
      keep = Math.min(keep, start === null ? this.#base : start - 1);
    }
    return Math.max(this.#base, Math.min(Math.max(0, keep), this.#read));
  }

  *#main(run: SpilledRun): SlicedJob<void> {
    // Result doubles/validity and possible exact result arrays are separate from ring state.
    const bytes = this.#calls.length * BATCH_ROWS * (9 + BIGINT_BYTES);
    if (!this.host.charge.allocate(bytes))
      throw new RangeError("A window's result batch does not fit");
    this.#resultBytes = bytes;
    const values = this.#calls.map(() => new Float64Array(BATCH_ROWS));
    const valid = this.#calls.map(() => new Uint8Array(BATCH_ROWS));
    const bigs: Array<bigint[] | null> = (this.#resultBig = this.#calls.map(() => null));
    let next = 0;
    let outRows = 0;
    let p = 0;
    const ranges: number[] = [0, 0, 0, 0, 0, 0];
    const flush = function* (this: PartitionStream): SlicedJob<void> {
      if (outRows === 0) return;
      const columns: Vector[] = this.#out.vectors();
      for (let f = 0; f < this.#calls.length; f++) {
        const s = this.#calls[f];
        if (s === undefined) continue;
        const gather = this.#gathers[f];
        if (gather !== null && gather !== undefined) {
          const vector = gather.vectors()[0];
          if (vector !== undefined) columns.push(vector);
          continue;
        }
        columns.push(
          this.host.resultVector(
            s.call,
            f,
            values[f] ?? new Float64Array(0),
            valid[f] ?? new Uint8Array(0),
            bigs[f] ?? null,
            outRows,
          ),
        );
      }
      yield* this.host.push(columns, outRows);
      this.#out.reset(true);
      for (const gather of this.#gathers) gather?.reset(true);
      for (let f = 0; f < bigs.length; f++) bigs[f] = null;
      outRows = 0;
    }.bind(this);
    for (;;) {
      // Read ahead until the next row to output is ready (or the run is over).
      while (next >= this.#read || !this.#ready(next)) {
        if (p >= run.pages.length) {
          this.#ended = true;
          if (this.#n < 0) this.#n = this.#read;
          break;
        }
        const page = yield* this.#readPage(run, p++);
        const placed = { ...page, start: this.#read };
        this.#pages.push(placed);
        this.#next = next;
        const keep = this.#keepFrom(next);
        this.#pageChanges(placed);
        for (let r = 0; r < page.rows; r++) {
          this.#take(placed, r, keep);
          if ((r & 511) === 511 && this.host.slice.due()) yield;
        }
        if (this.host.slice.due()) yield;
      }
      if (next >= this.#read) break;
      // Once the run is read every row is ready: anything else is a stall, not a wait.
      if (this.#ended && !this.#ready(next)) throw new RangeError(this.#stall(next));
      // Output every ready row, in order.
      while (next < this.#read && this.#ready(next)) {
        const page = this.#pageOf(next);
        if (page === null) throw new RangeError("A window row's page is gone");
        // A run of ready rows of one page goes into the output chunk at once.
        const pageEnd = page.start + page.rows;
        let end = next + 1;
        while (
          end < this.#read &&
          end < pageEnd &&
          end - next < BATCH_ROWS - outRows &&
          this.#ready(end)
        )
          end++;
        const taken = this.#out.append(page.columns, null, next - page.start, end - next);
        if (taken === 0) {
          // The output chunk is full of bytes: flush it; an empty one makes room for one row.
          if (outRows > 0) yield* flush();
          else yield this.host.io.need(this.#out.appendNeed(page.columns, next - page.start));
          continue;
        }
        for (let j = next; j < next + taken; j++) {
          for (let f = 0; f < this.#calls.length; f++) {
            const s = this.#calls[f];
            if (s === undefined) continue;
            const computation = this.#compute(
              s,
              f,
              j,
              outRows,
              values[f] ?? new Float64Array(0),
              valid[f] ?? new Uint8Array(0),
              bigs,
              ranges,
            );
            if (computation !== undefined) yield* computation;
          }
          outRows++;
          if (this.host.slice.due()) yield;
        }
        next += taken;
        if (outRows === BATCH_ROWS) yield* flush();
        if (this.host.slice.due()) yield;
      }
      // Release what no future row needs.
      this.#release(Math.min(next, this.#keepFrom(next)));
    }
    yield* flush();
    if (this.#n >= 0 && next !== this.#n) throw new RangeError("A window stream lost rows");
  }

  /** Which call could not be computed at row i after the run was read (a defect). */
  #stall(i: number): string {
    const g = this.#groupOf(i);
    const notes = this.#calls.map(
      (state) => `${state.call.fn} ${JSON.stringify(this.#bounds(state, i))}`,
    );
    return `A window stream stalled at row ${String(i)} (group ${String(g)}, groups ${String(this.#groupBase)}…${String(this.#groups)}, base ${String(this.#base)}, read ${String(this.#read)}): ${notes.join("; ")}`;
  }

  #pageOf(row: number): Page | null {
    for (const page of this.#pages)
      if (row >= page.start && row < page.start + page.rows) return page;
    return null;
  }

  #release(keep: number): void {
    while (this.#pages.length > 0) {
      const first = this.#pages[0];
      if (first === undefined || first.start + first.rows > keep) break;
      this.host.io.releasePage(first.held);
      this.#pages.shift();
    }
    // Groups wholly before `keep` (but the one holding keep − 1) go too.
    while (this.#groupStarts.length > 1 && (this.#groupStarts[1] ?? 0) <= keep - 1) {
      this.#groupStarts.shift();
      this.#groupBase++;
    }
  }

  /** One function's result for row i, at output position `at`. */
  #compute(
    s: CallState,
    f: number,
    i: number,
    at: number,
    values: Float64Array,
    valid: Uint8Array,
    bigs: Array<bigint[] | null>,
    ranges: number[],
  ): void | SlicedJob<void> {
    const fn = s.call.fn;
    const g = this.#groupOf(i);
    const ps = this.#groupStart(g);
    const n = this.#n;
    valid[at] = 1;
    switch (fn) {
      case "row_number":
        values[at] = i + 1;
        return;
      case "rank":
        values[at] = ps + 1;
        return;
      case "dense_rank":
        values[at] = g + 1;
        return;
      case "percent_rank":
        values[at] = n <= 1 ? 0 : ps / (n - 1);
        return;
      case "cume_dist":
        values[at] = (this.#groupEnd(g) + 1) / n;
        return;
      case "ntile": {
        const count = typeof s.call.amount === "number" ? s.call.amount : null;
        if (count === null) {
          valid[at] = 0;
          values[at] = 0;
          return;
        }
        if (count <= 0)
          throw new WindowError("argument of ntile must be greater than zero", "22014");
        const size = Math.floor(n / count);
        const larger = n % count;
        const boundary = larger * (size + 1);
        values[at] =
          i < boundary
            ? Math.floor(i / (size + 1)) + 1
            : larger + Math.floor((i - boundary) / Math.max(size, 1)) + 1;
        return;
      }
    }
    if (VALUE.has(fn)) {
      this.#value(s, f, i, ranges);
      return;
    }
    const bounds = this.#bounds(s, i);
    if (bounds === null) throw new RangeError("A window row was computed before it was ready");
    const k = this.#members(s, i, bounds[0], bounds[1], ranges);
    // Counts of valid rows in the members (cumulative differences; the tail past the rows read
    // is the pass-A total's).
    const countTo = (j: number) =>
      j < 0 ? 0 : j >= this.#read ? s.totalCount : (s.count[j - this.#base] ?? 0);
    const sumTo = (j: number) =>
      j < 0 ? 0 : j >= this.#read ? s.totalSum : (s.sum[j - this.#base] ?? 0);
    const bigTo = (j: number): bigint =>
      j < 0
        ? 0n
        : j >= this.#read
          ? (s.totalBig ?? BigInt(s.totalSum))
          : (s.big?.[j - this.#base] ?? BigInt(s.sum[j - this.#base] ?? 0));
    let rows = 0;
    let count = 0;
    for (let r = 0; r < k; r++) {
      const a = ranges[2 * r] ?? 0;
      const b = ranges[2 * r + 1] ?? -1;
      rows += b - a + 1;
      count += fn === "count_star" ? 0 : countTo(b) - countTo(a - 1);
    }
    if (fn === "count_star" || fn === "count") {
      values[at] = fn === "count_star" ? rows : count;
      return;
    }
    if (count === 0) {
      values[at] = 0;
      valid[at] = 0;
      return;
    }
    if (fn === "sum" || fn === "avg") {
      if (s.exact) {
        const useBig = s.big !== null || s.totalBig !== null;
        if (useBig) {
          let sum = 0n;
          for (let r = 0; r < k; r++)
            sum += bigTo(ranges[2 * r + 1] ?? -1) - bigTo((ranges[2 * r] ?? 0) - 1);
          values[at] = fn === "avg" ? Number(sum) / count : Number(sum);
          if (fn === "sum") exactOut(bigs, f, at, values, valid)[at] = sum;
        } else {
          let sum = 0;
          for (let r = 0; r < k; r++)
            sum += sumTo(ranges[2 * r + 1] ?? -1) - sumTo((ranges[2 * r] ?? 0) - 1);
          values[at] = fn === "avg" ? sum / count : sum;
          const out = bigs[f];
          if (out !== null && out !== undefined) out[at] = BigInt(sum);
        }
        return;
      }
      // Floats: the members in order. A first range from the head is the cumulative sum.
      let sum = 0;
      let r = 0;
      if (k > 0 && (ranges[0] ?? 0) === 0) {
        sum = sumTo(ranges[1] ?? -1);
        r = 1;
      }
      if (scanRows(ranges, r, k) > WINDOW_STEP_ROWS)
        return this.#longFrame(s, f, at, values, valid, bigs, ranges, r, k, {
          kind: "sum",
          value: sum,
          count,
        });
      for (; r < k; r++) {
        const b = ranges[2 * r + 1] ?? -1;
        for (let j = ranges[2 * r] ?? 0; j <= b; j++)
          if (s.ok[j - this.#base] === 1) sum += s.v[j - this.#base] ?? 0;
      }
      values[at] = fn === "avg" ? sum / count : sum;
      return;
    }
    // MIN / MAX: a head-anchored prefix from the running extreme, the rest scanned.
    const max = fn === "max";
    if (s.vBig !== null || s.totalExtremeBig !== null) {
      // Exact int64 extremes (a value past 2^53 was seen): compared and returned as bigints.
      let best: bigint | null = null;
      let r = 0;
      if (k > 0 && (ranges[0] ?? 0) === 0) {
        const b = ranges[1] ?? -1;
        if (b >= this.#read)
          best = s.totalSeen ? (s.totalExtremeBig ?? BigInt(s.totalExtreme)) : null;
        else if (countTo(b) > 0)
          best = s.extremeBig?.[b - this.#base] ?? BigInt(s.extreme[b - this.#base] ?? 0);
        r = 1;
      }
      if (scanRows(ranges, r, k) > WINDOW_STEP_ROWS)
        return this.#longFrame(s, f, at, values, valid, bigs, ranges, r, k, {
          kind: "big",
          value: best,
        });
      for (; r < k; r++) {
        const b = ranges[2 * r + 1] ?? -1;
        for (let j = ranges[2 * r] ?? 0; j <= b; j++) {
          if (s.ok[j - this.#base] !== 1) continue;
          const v = s.vBig?.[j - this.#base] ?? BigInt(s.v[j - this.#base] ?? 0);
          if (best === null || (max ? v > best : v < best)) best = v;
        }
      }
      values[at] = best === null ? 0 : Number(best);
      valid[at] = best === null ? 0 : 1;
      exactOut(bigs, f, at, values, valid)[at] = best ?? 0n;
      return;
    }
    let best = Number.NaN;
    let seen = false;
    let r = 0;
    if (k > 0 && (ranges[0] ?? 0) === 0) {
      const b = ranges[1] ?? -1;
      if (b >= this.#read) {
        best = s.totalExtreme;
        seen = s.totalSeen;
      } else if (countTo(b) > 0) {
        best = s.extreme[b - this.#base] ?? Number.NaN;
        seen = true;
      }
      r = 1;
    }
    if (scanRows(ranges, r, k) > WINDOW_STEP_ROWS)
      return this.#longFrame(s, f, at, values, valid, bigs, ranges, r, k, {
        kind: "float",
        value: best,
        seen,
      });
    for (; r < k; r++) {
      const b = ranges[2 * r + 1] ?? -1;
      for (let j = ranges[2 * r] ?? 0; j <= b; j++) {
        if (s.ok[j - this.#base] !== 1) continue;
        const v = s.v[j - this.#base] ?? 0;
        if (!seen || better(v, best, max)) best = v;
        seen = true;
      }
    }
    values[at] = best;
    valid[at] = seen ? 1 : 0;
    const out = bigs[f];
    if (out !== null && out !== undefined) out[at] = seen ? BigInt(best) : 0n;
  }

  /** Long moving-frame scans suspend inside a frame; short frames keep the synchronous path. */
  *#longFrame(
    s: CallState,
    f: number,
    at: number,
    values: Float64Array,
    valid: Uint8Array,
    bigs: Array<bigint[] | null>,
    ranges: readonly number[],
    from: number,
    k: number,
    state:
      | { kind: "sum"; value: number; count: number }
      | { kind: "float"; value: number; seen: boolean }
      | { kind: "big"; value: bigint | null },
  ): SlicedJob<void> {
    const max = s.call.fn === "max";
    for (let r = from; r < k; r++) {
      const end = ranges[2 * r + 1] ?? -1;
      for (let j = ranges[2 * r] ?? 0; j <= end; j++) {
        const slot = j - this.#base;
        if (s.ok[slot] === 1) {
          if (state.kind === "sum") state.value += s.v[slot] ?? 0;
          else if (state.kind === "big") {
            const value = s.vBig?.[slot] ?? BigInt(s.v[slot] ?? 0);
            if (state.value === null || (max ? value > state.value : value < state.value))
              state.value = value;
          } else {
            const value = s.v[slot] ?? 0;
            if (!state.seen || better(value, state.value, max)) state.value = value;
            state.seen = true;
          }
        }
        if ((j & (WINDOW_STEP_ROWS - 1)) === WINDOW_STEP_ROWS - 1 && this.host.slice.due()) yield;
      }
    }
    if (state.kind === "sum")
      values[at] = s.call.fn === "avg" ? state.value / state.count : state.value;
    else if (state.kind === "big") {
      values[at] = state.value === null ? 0 : Number(state.value);
      valid[at] = state.value === null ? 0 : 1;
      exactOut(bigs, f, at, values, valid)[at] = state.value ?? 0n;
    } else {
      values[at] = state.value;
      valid[at] = state.seen ? 1 : 0;
      const out = bigs[f];
      if (out !== null && out !== undefined) out[at] = state.seen ? BigInt(state.value) : 0n;
    }
  }

  /** The member row a first / last / nth value reads for row i (−1: none). */
  #target(s: CallState, i: number, bounds: [number, number]): number {
    const ranges: number[] = [0, 0, 0, 0, 0, 0];
    const k = this.#members(s, i, bounds[0], bounds[1], ranges);
    if (k === 0) return -1;
    if (s.call.fn === "first_value") return ranges[0] ?? -1;
    if (s.call.fn === "last_value") return ranges[2 * k - 1] ?? -1;
    const nth = typeof s.call.amount === "number" ? s.call.amount : 0;
    let left = nth - 1;
    if (left < 0) return -1;
    for (let r = 0; r < k; r++) {
      const a = ranges[2 * r] ?? 0;
      const b = ranges[2 * r + 1] ?? -1;
      if (left <= b - a) return a + left;
      left -= b - a + 1;
    }
    return -1;
  }

  /** A value function's result row for row i, appended to its gather store. */
  #value(s: CallState, f: number, i: number, ranges: number[]): void {
    const gather = this.#gathers[f];
    if (gather === null || gather === undefined) return;
    const fn = s.call.fn;
    const arg = s.call.arg ?? 0;
    const type = s.call.type;
    const fromRow = (row: number) => {
      // The partition's first rows are in `head`, later rows in the pages, the last in `last`.
      const page = this.#pageOf(row);
      if (page !== null)
        gather.append(
          [page.columns[arg] ?? (s.nullValue ??= nullConstant(type, 1))],
          null,
          row - page.start,
          1,
        );
      else if (s.head !== null && row < s.head.rows) gather.appendFrom(s.head, [0], [row], 0, 1);
      else if (s.last !== null && row === this.#n - 1 && s.last.rows === 1)
        gather.appendFrom(s.last, [0], [0], 0, 1);
      else throw new RangeError("A window value row is gone");
    };
    const nul = () => gather.append([(s.nullValue ??= nullConstant(type, 1))], null, 0, 1);
    if (fn === "lag" || fn === "lead") {
      const k = typeof s.call.amount === "number" ? s.call.amount : 1;
      const target = fn === "lag" ? i - k : i + k;
      if (target >= 0 && target < this.#total()) fromRow(target);
      else if (s.call.fallback !== undefined && s.call.fallback !== null)
        gather.append([s.call.fallback], null, 0, 1);
      else nul();
      return;
    }
    const bounds = this.#bounds(s, i);
    if (bounds === null) throw new RangeError("A window row was computed before it was ready");
    const k = this.#members(s, i, bounds[0], bounds[1], ranges);
    if (k === 0) {
      nul();
      return;
    }
    if (fn === "first_value") {
      fromRow(ranges[0] ?? 0);
      return;
    }
    if (fn === "last_value") {
      fromRow(ranges[2 * k - 1] ?? 0);
      return;
    }
    const nth = typeof s.call.amount === "number" ? s.call.amount : null;
    if (nth === null) {
      nul();
      return;
    }
    if (nth <= 0) throw new WindowError("argument of nth_value must be greater than zero", "22016");
    let left = nth - 1;
    for (let r = 0; r < k; r++) {
      const a = ranges[2 * r] ?? 0;
      const b = ranges[2 * r + 1] ?? -1;
      if (left <= b - a) {
        fromRow(a + left);
        return;
      }
      left -= b - a + 1;
    }
    nul();
  }
}

/**
 * A function's exact output column for the current output chunk: made when its first row needs
 * a bigint, the chunk's earlier rows (exact as doubles) converted then (the result vector then
 * reads every row from it).
 */
function exactOut(
  bigs: Array<bigint[] | null>,
  f: number,
  at: number,
  values: Float64Array,
  valid: Uint8Array,
): bigint[] {
  let out = bigs[f] ?? null;
  if (out === null) {
    out = new Array<bigint>(BATCH_ROWS).fill(0n);
    for (let k = 0; k < at; k++) if (valid[k] === 1) out[k] = BigInt(values[k] ?? 0);
    bigs[f] = out;
  }
  return out;
}

function better(v: number, best: number, max: boolean): boolean {
  if (Number.isNaN(v)) return max && !Number.isNaN(best);
  if (Number.isNaN(best)) return !max;
  return max ? v > best : v < best;
}

/** Members that still require a scan (prefix ranges were already handled by cumulative state). */
function scanRows(ranges: readonly number[], from: number, count: number): number {
  let rows = 0;
  for (let r = from; r < count; r++)
    rows += Math.max(0, (ranges[2 * r + 1] ?? -1) - (ranges[2 * r] ?? 0) + 1);
  return rows;
}
