/**
 * Window functions over one partition (B7 increment 3): peer groups, frame bounds and the
 * functions themselves, on typed arrays. PostgreSQL's rules (the reference interpreter,
 * testing/ir-reference.ts on rebuild/lower-p2b, follows them too):
 *
 * - **Peers** are rows with equal ORDER BY keys; without ORDER BY every row is a peer.
 * - **Frames.** ROWS counts rows, GROUPS counts peer groups, RANGE compares the (single, numeric)
 *   ORDER BY key with the offset along the sort direction; a NULL key frames only its NULL peers.
 *   CURRENT ROW is the row (ROWS) or its peer group's edge (RANGE, GROUPS). A null frame is
 *   PostgreSQL's default: RANGE UNBOUNDED PRECEDING to CURRENT ROW with ORDER BY, the whole
 *   partition without. EXCLUDE CURRENT ROW / GROUP / TIES remove the row, its peers, or its
 *   peers but itself; members stay in order.
 * - **Aggregates** see the frame's members in order. Float sums add in that order (PostgreSQL
 *   has no inverse transition for floats: a moving frame is summed again from its head), so a
 *   running sum is a left-to-right prefix and a sliding one is re-added. Exact (integer) sums and
 *   counts use prefix sums. MIN/MAX order NaN above every number.
 * - **Ranking**: row_number, rank, dense_rank, percent_rank, cume_dist, ntile (PostgreSQL's
 *   bucket sizes). **Values**: lag/lead (offset per row, may be negative), first_value,
 *   last_value, nth_value over the frame; their results are row indexes (−1: NULL, −2: the
 *   default) that the operator gathers.
 *
 * Errors are PostgreSQL's: a NULL frame offset 22004, a negative one 22013, ntile ≤ 0 22014,
 * nth_value ≤ 0 22016.
 */
import { SLICE_ROWS, type Slice, type SlicedJob } from "./slices.js";

/** Fixed-cost prefix/frame work between task-clock checks. */
export const WINDOW_STEP_ROWS = 1024;

export type FrameUnit = "rows" | "range" | "groups";
export type FrameBoundKind =
  "unbounded-preceding" | "preceding" | "current-row" | "following" | "unbounded-following";

export interface FrameBound {
  readonly kind: FrameBoundKind;
  /** The offset of `preceding` / `following` (a constant of the query); null otherwise. */
  readonly offset: number | null;
}

export interface Frame {
  readonly unit: FrameUnit;
  readonly start: FrameBound;
  readonly end: FrameBound;
  readonly exclude: "no-others" | "current-row" | "group" | "ties";
}

/** A SQL error raised by window evaluation, with PostgreSQL's SQLSTATE. */
export class WindowError extends Error {
  constructor(
    message: string,
    readonly code: string,
  ) {
    super(message);
    this.name = "WindowError";
  }
}

/** Peer groups of one partition of `n` rows. */
export interface Peers {
  n: number;
  /** First and last row of each row's peer group. */
  readonly start: Int32Array;
  readonly end: Int32Array;
  /** Each row's peer group number. */
  readonly group: Int32Array;
  /** First and last row of each group; `groups` of them. */
  readonly groupStart: Int32Array;
  readonly groupEnd: Int32Array;
  groups: number;
}

/** The ORDER BY key as numbers, for RANGE offsets (one key; NULL where `valid` is 0). */
export interface RangeKey {
  readonly values: Float64Array;
  readonly valid: Uint8Array;
  /** Exact keys when an int64 key passes 2^53 (its double rounds), else null. */
  readonly big?: readonly bigint[] | null;
  readonly descending: boolean;
  /** Whether NULLs sort first (they then precede every number). */
  readonly nullsFirst: boolean;
}

/** Fills the group arrays from per-row "starts a new peer group" flags (`newGroup[0]` is 1). */
export function fillPeers(peers: Peers, newGroup: Uint8Array, n: number): void {
  peers.n = n;
  let groups = 0;
  for (let i = 0; i < n; i++) {
    if (i === 0 || newGroup[i] === 1) {
      if (groups > 0) peers.groupEnd[groups - 1] = i - 1;
      peers.groupStart[groups] = i;
      groups++;
    }
    peers.group[i] = groups - 1;
  }
  if (groups > 0) peers.groupEnd[groups - 1] = n - 1;
  peers.groups = groups;
  for (let i = 0; i < n; i++) {
    const g = peers.group[i] ?? 0;
    peers.start[i] = peers.groupStart[g] ?? 0;
    peers.end[i] = peers.groupEnd[g] ?? 0;
  }
}

function offsetOf(bound: FrameBound): number {
  const offset = bound.offset;
  if (offset === null) throw new WindowError("frame offset must not be null", "22004");
  if (offset < 0) throw new WindowError("frame offset must not be negative", "22013");
  return offset;
}

/**
 * Frame bounds `[starts[i], ends[i]]` of every row (start > end: empty), before EXCLUDE. A
 * sliced job: the RANGE scans check the deadline per row block.
 */
export function* frameBounds(
  frame: Frame | null,
  ordered: boolean,
  peers: Peers,
  key: RangeKey | null,
  starts: Int32Array,
  ends: Int32Array,
  slice: Slice,
): SlicedJob<void> {
  const n = peers.n;
  if (frame === null) {
    for (let i = 0; i < n; i++) {
      starts[i] = 0;
      ends[i] = ordered ? (peers.end[i] ?? i) : n - 1;
    }
    return;
  }
  for (const [bound, end] of [
    [frame.start, false],
    [frame.end, true],
  ] as const) {
    const out = end ? ends : starts;
    switch (bound.kind) {
      case "unbounded-preceding":
        out.fill(0, 0, n);
        continue;
      case "unbounded-following":
        out.fill(n - 1, 0, n);
        continue;
      case "current-row":
        if (frame.unit === "rows") for (let i = 0; i < n; i++) out[i] = i;
        else out.set((end ? peers.end : peers.start).subarray(0, n));
        continue;
    }
    const offset = offsetOf(bound);
    const sign = bound.kind === "preceding" ? -1 : 1;
    if (frame.unit === "rows") {
      for (let i = 0; i < n; i++) {
        const at = i + sign * offset;
        out[i] = end ? Math.min(n - 1, at) : Math.max(0, Math.min(n, at));
      }
      continue;
    }
    if (frame.unit === "groups") {
      for (let i = 0; i < n; i++) {
        const g = (peers.group[i] ?? 0) + sign * offset;
        if (g < 0) out[i] = end ? -1 : 0;
        else if (g >= peers.groups) out[i] = end ? n - 1 : n;
        else out[i] = (end ? peers.groupEnd[g] : peers.groupStart[g]) ?? 0;
      }
      continue;
    }
    if (key === null) throw new TypeError("RANGE with an offset needs one numeric ORDER BY key");
    yield* rangeBounds(key, peers, sign * offset, end, out, slice);
  }
}

/**
 * RANGE offset bounds: the first row whose key is ≥ key + delta (start) or the last whose key
 * is ≤ key + delta (end), along the sort direction. For a non-NULL row, NULL keys sort as below
 * every value (NULLS FIRST) or above every value (NULLS LAST), as in PostgreSQL (checked with
 * PGlite: `1 PRECEDING` after NULLS FIRST includes the NULL rows). A NULL row's offset bounds
 * are its peer group's. Rows are sorted by the key, so a moving pointer finds every bound in
 * one pass.
 */
function* rangeBounds(
  key: RangeKey,
  peers: Peers,
  delta: number,
  end: boolean,
  out: Int32Array,
  slice: Slice,
): SlicedJob<void> {
  const n = peers.n;
  const { values, valid, descending, nullsFirst } = key;
  const big = key.big ?? null;
  if (big !== null) {
    // Exact int64 keys (review B7-p2d-1 #3): the same walk in bigints, the offset exact.
    if (!Number.isInteger(delta))
      throw new RangeError("A RANGE offset over int8 keys is an integer");
    const dir = descending ? -1n : 1n;
    const step = BigInt(delta);
    // −1, 0, 1: row j's key (along the direction) against `target`; NULLs below or above.
    const cmp = (j: number, target: bigint): number => {
      if (valid[j] !== 1) return nullsFirst ? -1 : 1;
      const v = (big[j] ?? 0n) * dir;
      return v < target ? -1 : v > target ? 1 : 0;
    };
    let p = 0;
    for (let i = 0; i < n; i++) {
      if (valid[i] === 0) {
        out[i] = (end ? peers.end[i] : peers.start[i]) ?? i;
        continue;
      }
      const target = (big[i] ?? 0n) * dir + step;
      if (end) {
        while (p < n && cmp(p, target) <= 0) p++;
        out[i] = p - 1;
      } else {
        while (p < n && cmp(p, target) < 0) p++;
        out[i] = p;
      }
      if ((i & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
    }
    return;
  }
  let p = 0;
  for (let i = 0; i < n; i++) {
    if (valid[i] === 0) {
      out[i] = (end ? peers.end[i] : peers.start[i]) ?? i;
      continue;
    }
    const contains = (j: number) =>
      valid[j] !== 1
        ? end
          ? nullsFirst
          : !nullsFirst
        : floatRangeContains(values[j] ?? 0, values[i] ?? 0, delta, descending, end);
    if (end) {
      while (p < n && contains(p)) p++;
      out[i] = p - 1;
    } else {
      while (p < n && !contains(p)) p++;
      out[i] = p;
    }
    if ((i & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
  }
}

/**
 * PostgreSQL float.c in_range_float8_float8: NaN is above every non-NaN and equal to
 * itself; an infinite base and cancelling infinite offset admit every non-NaN value.
 * NULL validity is handled by the caller, never encoded as an infinity sentinel.
 */
export function floatRangeContains(
  value: number,
  base: number,
  delta: number,
  descending: boolean,
  end: boolean,
): boolean {
  const less = end !== descending;
  if (Number.isNaN(value)) return Number.isNaN(base) || !less;
  if (Number.isNaN(base)) return less;
  const target = base + (descending ? -delta : delta);
  if (Number.isNaN(target)) return true;
  return less ? value <= target : value >= target;
}

/** A numeric argument of one partition: values and validity per row. */
export interface NumberArg {
  readonly values: Float64Array;
  readonly valid: Uint8Array;
  /** Exact integers too large for a double (int64 beyond 2^53): the values as bigints. */
  readonly big: bigint[] | null;
}

/** Results of a numeric function: values (or bigints) and validity per row. */
export interface NumberResult {
  readonly values: Float64Array;
  readonly valid: Uint8Array;
  big: bigint[] | null;
}

/**
 * The members of row i's frame as up to three ordered ranges (EXCLUDE applied): writes
 * [from, to] pairs into `ranges` and returns how many.
 */
function membersOf(
  exclude: Frame["exclude"],
  i: number,
  s: number,
  e: number,
  ps: number,
  pe: number,
  ranges: Int32Array,
): number {
  let k = 0;
  const push = (a: number, b: number) => {
    if (a <= b) {
      ranges[k++] = a;
      ranges[k++] = b;
    }
  };
  switch (exclude) {
    case "no-others":
      push(s, e);
      break;
    case "current-row":
      push(s, Math.min(e, i - 1));
      push(Math.max(s, i + 1), e);
      break;
    case "group":
      push(s, Math.min(e, ps - 1));
      push(Math.max(s, pe + 1), e);
      break;
    case "ties":
      push(s, Math.min(e, ps - 1));
      if (i >= s && i <= e) push(i, i);
      push(Math.max(s, pe + 1), e);
      break;
  }
  return k >>> 1;
}

export type AggregateKind = "count_star" | "count" | "sum" | "avg" | "min" | "max";

/**
 * An aggregate over each row's frame. `exact`: integer arguments (prefix sums are exact);
 * otherwise floats, added in frame order. `out.big` is set for exact sums past 2^53.
 */
export function* aggregate(
  kind: AggregateKind,
  arg: NumberArg | null,
  exact: boolean,
  peers: Peers,
  exclude: Frame["exclude"],
  starts: Int32Array,
  ends: Int32Array,
  out: NumberResult,
  slice: Slice,
  work: AggregateScratch | null = null,
): SlicedJob<void> {
  const n = peers.n;
  const ranges = new Int32Array(6);
  const valid = arg?.valid ?? null;
  const values = arg?.values ?? null;
  // Prefix counts of valid rows (count, avg's divisor, "all NULL" for sum/min/max).
  const counts = work !== null && work.counts.length > n ? work.counts : new Int32Array(n + 1);
  counts[0] = 0;
  for (let i = 0; i < n; i++) {
    counts[i + 1] = (counts[i] ?? 0) + (valid === null || valid[i] === 1 ? 1 : 0);
    if ((i & (WINDOW_STEP_ROWS - 1)) === WINDOW_STEP_ROWS - 1 && slice.due()) yield;
  }
  const countIn = (a: number, b: number) => (counts[b + 1] ?? 0) - (counts[a] ?? 0);
  const running = exclude === "no-others" && (yield* isRunning(starts, n, slice));
  // Exact sums: prefix sums in doubles while they stay exact, else bigints.
  let prefix: Float64Array | null = null;
  let bigPrefix: bigint[] | null = null;
  if ((kind === "sum" || kind === "avg") && values !== null && valid !== null) {
    if (exact && arg?.big !== null && arg?.big !== undefined) {
      bigPrefix = new Array<bigint>(n + 1);
      bigPrefix[0] = 0n;
      for (let i = 0; i < n; i++) {
        bigPrefix[i + 1] = (bigPrefix[i] ?? 0n) + (valid[i] === 1 ? (arg.big[i] ?? 0n) : 0n);
        if ((i & (WINDOW_STEP_ROWS - 1)) === WINDOW_STEP_ROWS - 1 && slice.due()) yield;
      }
    } else if (exact || running) {
      // Exact integers below 2^53 in total, or floats added left to right (a running frame's
      // members are a prefix, so its sum is the prefix itself).
      prefix = work !== null && work.prefix.length > n ? work.prefix : new Float64Array(n + 1);
      prefix[0] = 0;
      let sum = 0;
      let unsafe = false;
      for (let i = 0; i < n; i++) {
        if (valid[i] === 1) sum += values[i] ?? 0;
        prefix[i + 1] = sum;
        if (exact && !Number.isSafeInteger(sum)) unsafe = true;
        if ((i & (WINDOW_STEP_ROWS - 1)) === WINDOW_STEP_ROWS - 1 && slice.due()) yield;
      }
      if (unsafe) {
        bigPrefix = new Array<bigint>(n + 1);
        bigPrefix[0] = 0n;
        for (let i = 0; i < n; i++) {
          bigPrefix[i + 1] = (bigPrefix[i] ?? 0n) + (valid[i] === 1 ? BigInt(values[i] ?? 0) : 0n);
          if ((i & (WINDOW_STEP_ROWS - 1)) === WINDOW_STEP_ROWS - 1 && slice.due()) yield;
        }
        prefix = null;
      }
    }
  }
  if (bigPrefix !== null) out.big = new Array<bigint>(n);
  if (exclude === "no-others" && bigPrefix === null && kind !== "min" && kind !== "max") {
    // The common case: one member range per row, [start, end] (empty when end < start).
    for (let i = 0; i < n; i++) {
      const s = starts[i] ?? 0;
      const e = ends[i] ?? -1;
      const empty = e < s;
      const count = empty ? 0 : (counts[e + 1] ?? 0) - (counts[s] ?? 0);
      if (kind === "count_star" || kind === "count") {
        out.values[i] = kind === "count" ? count : empty ? 0 : e - s + 1;
        out.valid[i] = 1;
      } else if (count === 0) {
        out.values[i] = 0;
        out.valid[i] = 0;
      } else {
        let sum: number;
        if (prefix !== null) sum = (prefix[e + 1] ?? 0) - (prefix[s] ?? 0);
        else {
          // Floats over a moving frame: re-added from the frame's head, in order.
          sum = 0;
          // A frame without NULLs (every member counted) needs no validity test.
          if (values === null) sum = 0;
          else if (valid === null || count === e - s + 1)
            for (let j = s; j <= e; j++) {
              sum += values[j] ?? 0;
              if ((j & (WINDOW_STEP_ROWS - 1)) === WINDOW_STEP_ROWS - 1 && slice.due()) yield;
            }
          else
            for (let j = s; j <= e; j++) {
              if (valid[j] === 1) sum += values[j] ?? 0;
              if ((j & (WINDOW_STEP_ROWS - 1)) === WINDOW_STEP_ROWS - 1 && slice.due()) yield;
            }
          if (e - s > 64 && slice.due()) yield;
        }
        out.values[i] = kind === "avg" ? sum / count : sum;
        out.valid[i] = 1;
      }
      if ((i & 1023) === 1023 && slice.due()) yield;
    }
    return;
  }
  let runningExtreme = Number.NaN;
  let runningSeen = 0;
  let runningAt = -1;
  for (let i = 0; i < n; i++) {
    const s = starts[i] ?? 0;
    const e = ends[i] ?? -1;
    const k = membersOf(exclude, i, s, e, peers.start[i] ?? i, peers.end[i] ?? i, ranges);
    let count = 0;
    for (let r = 0; r < k; r++) count += countIn(ranges[2 * r] ?? 0, ranges[2 * r + 1] ?? -1);
    if (kind === "count_star" || kind === "count") {
      let rows = 0;
      for (let r = 0; r < k; r++) rows += (ranges[2 * r + 1] ?? -1) - (ranges[2 * r] ?? 0) + 1;
      out.values[i] = kind === "count_star" ? rows : count;
      out.valid[i] = 1;
    } else if (count === 0) {
      out.values[i] = 0;
      out.valid[i] = 0;
      if (out.big !== null) out.big[i] = 0n;
    } else if (kind === "sum" || kind === "avg") {
      if (bigPrefix !== null && out.big !== null) {
        let sum = 0n;
        for (let r = 0; r < k; r++)
          sum +=
            (bigPrefix[(ranges[2 * r + 1] ?? -1) + 1] ?? 0n) -
            (bigPrefix[ranges[2 * r] ?? 0] ?? 0n);
        if (kind === "sum") out.big[i] = sum;
        out.values[i] = kind === "avg" ? Number(sum) / count : Number(sum);
      } else if (prefix !== null && (exact || running)) {
        let sum = 0;
        for (let r = 0; r < k; r++)
          sum += (prefix[(ranges[2 * r + 1] ?? -1) + 1] ?? 0) - (prefix[ranges[2 * r] ?? 0] ?? 0);
        // A running float frame's sum is the prefix itself (exact subtraction of 0).
        out.values[i] = kind === "avg" ? sum / count : sum;
      } else {
        // Floats over a moving frame: re-added from the frame's head, in order.
        let sum = 0;
        for (let r = 0; r < k; r++) {
          const to = ranges[2 * r + 1] ?? -1;
          for (let j = ranges[2 * r] ?? 0; j <= to; j++) {
            if (valid?.[j] === 1) sum += values?.[j] ?? 0;
            if ((j & (WINDOW_STEP_ROWS - 1)) === WINDOW_STEP_ROWS - 1 && slice.due()) yield;
          }
          if (to - (ranges[2 * r] ?? 0) > 64 && slice.due()) yield;
        }
        out.values[i] = kind === "avg" ? sum / count : sum;
      }
      out.valid[i] = 1;
    } else {
      // MIN / MAX (NaN is above every number).
      const max = kind === "max";
      if (running && k === 1 && (ranges[0] ?? 0) === 0) {
        const to = ranges[1] ?? -1;
        for (let j = runningAt + 1; j <= to; j++) {
          if (valid?.[j] === 1) {
            const v = values?.[j] ?? 0;
            if (runningSeen === 0 || better(v, runningExtreme, max)) runningExtreme = v;
            runningSeen++;
          }
          if ((j & (WINDOW_STEP_ROWS - 1)) === WINDOW_STEP_ROWS - 1 && slice.due()) yield;
        }
        runningAt = Math.max(runningAt, to);
        out.values[i] = runningExtreme;
      } else {
        let best = Number.NaN;
        let seen = false;
        for (let r = 0; r < k; r++) {
          const to = ranges[2 * r + 1] ?? -1;
          for (let j = ranges[2 * r] ?? 0; j <= to; j++) {
            if (valid?.[j] === 1) {
              const v = values?.[j] ?? 0;
              if (!seen || better(v, best, max)) best = v;
              seen = true;
            }
            if ((j & (WINDOW_STEP_ROWS - 1)) === WINDOW_STEP_ROWS - 1 && slice.due()) yield;
          }
          if (to - (ranges[2 * r] ?? 0) > 64 && slice.due()) yield;
        }
        out.values[i] = best;
      }
      out.valid[i] = 1;
      if (arg?.big !== null && arg?.big !== undefined && out.big === null)
        out.big = new Array<bigint>(n);
    }
    if ((i & 1023) === 1023 && slice.due()) yield;
  }
  // MIN/MAX of exact big integers: the bigint of the chosen row (found again by value).
  if (
    (kind === "min" || kind === "max") &&
    arg?.big !== null &&
    arg?.big !== undefined &&
    out.big !== null
  )
    yield* bigExtremes(kind === "max", arg, peers, exclude, starts, ends, out, slice);
}

/** MIN/MAX over exact bigints (int64 past 2^53): compared as bigints. */
function* bigExtremes(
  max: boolean,
  arg: NumberArg,
  peers: Peers,
  exclude: Frame["exclude"],
  starts: Int32Array,
  ends: Int32Array,
  out: NumberResult,
  slice: Slice,
): SlicedJob<void> {
  const ranges = new Int32Array(6);
  const big = arg.big ?? [];
  const result = out.big ?? [];
  for (let i = 0; i < peers.n; i++) {
    const k = membersOf(
      exclude,
      i,
      starts[i] ?? 0,
      ends[i] ?? -1,
      peers.start[i] ?? i,
      peers.end[i] ?? i,
      ranges,
    );
    let best: bigint | null = null;
    for (let r = 0; r < k; r++)
      for (let j = ranges[2 * r] ?? 0; j <= (ranges[2 * r + 1] ?? -1); j++) {
        if (arg.valid[j] === 1) {
          const v = big[j] ?? 0n;
          if (best === null || (max ? v > best : v < best)) best = v;
        }
        if ((j & (WINDOW_STEP_ROWS - 1)) === WINDOW_STEP_ROWS - 1 && slice.due()) yield;
      }
    result[i] = best ?? 0n;
    if (slice.tick()) yield;
  }
}

function better(v: number, best: number, max: boolean): boolean {
  // NaN sorts above every number (PostgreSQL's float order).
  if (Number.isNaN(v)) return max && !Number.isNaN(best);
  if (Number.isNaN(best)) return !max;
  return max ? v > best : v < best;
}

/** Whether every frame starts at the partition's first row. */
function* isRunning(starts: Int32Array, n: number, slice: Slice): SlicedJob<boolean> {
  for (let i = 0; i < n; i++) {
    if ((starts[i] ?? 0) !== 0) return false;
    if ((i & (WINDOW_STEP_ROWS - 1)) === WINDOW_STEP_ROWS - 1 && slice.due()) yield;
  }
  return true;
}

/** Reusable prefix arrays for `aggregate` (the caller's charged scratch, n + 1 entries). */
export interface AggregateScratch {
  readonly counts: Int32Array;
  readonly prefix: Float64Array;
}

export type RankingKind =
  "row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist" | "ntile";

/** A ranking function; `buckets` is ntile's argument per row (NULL where `valid` is 0). */
export function* ranking(
  kind: RankingKind,
  peers: Peers,
  buckets: NumberArg | null,
  out: NumberResult,
  slice: Slice,
): SlicedJob<void> {
  const n = peers.n;
  for (let i = 0; i < n; i++) {
    if ((i & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
    out.valid[i] = 1;
    switch (kind) {
      case "row_number":
        out.values[i] = i + 1;
        break;
      case "rank":
        out.values[i] = (peers.start[i] ?? i) + 1;
        break;
      case "dense_rank":
        out.values[i] = (peers.group[i] ?? 0) + 1;
        break;
      case "percent_rank":
        out.values[i] = n <= 1 ? 0 : (peers.start[i] ?? i) / (n - 1);
        break;
      case "cume_dist":
        out.values[i] = ((peers.end[i] ?? i) + 1) / n;
        break;
      case "ntile": {
        if (buckets?.valid[i] !== 1) {
          out.valid[i] = 0;
          out.values[i] = 0;
          break;
        }
        const count = buckets.values[i] ?? 0;
        if (count <= 0)
          throw new WindowError("argument of ntile must be greater than zero", "22014");
        // PostgreSQL: n / count rows per bucket, the first n % count buckets one row more.
        const size = Math.floor(n / count);
        const larger = n % count;
        const boundary = larger * (size + 1);
        out.values[i] =
          i < boundary
            ? Math.floor(i / (size + 1)) + 1
            : larger + Math.floor((i - boundary) / Math.max(size, 1)) + 1;
        break;
      }
    }
  }
}

export type ValueKind = "lag" | "lead" | "first_value" | "last_value" | "nth_value";

/** Result row index meaning SQL NULL / the function's default. */
export const NULL_ROW = -1;
export const DEFAULT_ROW = -2;

/**
 * A value function's source row for each row (or NULL_ROW / DEFAULT_ROW). `amount`: lag/lead's
 * offset (default 1) or nth_value's n, per row; null for the others.
 */
export function* values(
  kind: ValueKind,
  peers: Peers,
  exclude: Frame["exclude"],
  starts: Int32Array,
  ends: Int32Array,
  amount: NumberArg | null,
  out: Int32Array,
  slice: Slice,
): SlicedJob<void> {
  const n = peers.n;
  const ranges = new Int32Array(6);
  for (let i = 0; i < n; i++) {
    if ((i & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
    if (kind === "lag" || kind === "lead") {
      if (amount !== null && amount.valid[i] !== 1) {
        out[i] = NULL_ROW;
        continue;
      }
      const offset = amount === null ? 1 : (amount.values[i] ?? 1);
      const target = kind === "lag" ? i - offset : i + offset;
      out[i] = target >= 0 && target < n ? target : DEFAULT_ROW;
      continue;
    }
    const k = membersOf(
      exclude,
      i,
      starts[i] ?? 0,
      ends[i] ?? -1,
      peers.start[i] ?? i,
      peers.end[i] ?? i,
      ranges,
    );
    if (kind === "first_value") {
      out[i] = k > 0 ? (ranges[0] ?? NULL_ROW) : NULL_ROW;
      continue;
    }
    if (kind === "last_value") {
      out[i] = k > 0 ? (ranges[2 * k - 1] ?? NULL_ROW) : NULL_ROW;
      continue;
    }
    if (amount?.valid[i] !== 1) {
      out[i] = NULL_ROW;
      continue;
    }
    const nth = amount.values[i] ?? 0;
    if (nth <= 0) throw new WindowError("argument of nth_value must be greater than zero", "22016");
    let left = nth - 1;
    out[i] = NULL_ROW;
    for (let r = 0; r < k; r++) {
      const a = ranges[2 * r] ?? 0;
      const b = ranges[2 * r + 1] ?? -1;
      if (left <= b - a) {
        out[i] = a + left;
        break;
      }
      left -= b - a + 1;
    }
  }
}
