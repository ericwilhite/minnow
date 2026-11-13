/**
 * The window operator against an oracle with PostgreSQL's window semantics: the reference
 * interpreter's `windowRows` (testing/ir-reference.ts on rebuild/lower-p2b, itself checked
 * against PostgreSQL), ported to rows here. Peers make the order of equal-key rows free, so the
 * oracle runs on the operator's own output order after checking that order is sorted; every
 * function value must then match exactly (float sums add in frame order on both sides).
 * window-pglite.test.ts holds the operator to PostgreSQL itself. Generated specs cover every
 * function, ROWS / RANGE / GROUPS with offsets, every EXCLUDE, NULL keys, resident and spilled
 * (tight grants spill the sort and big partitions), with and without backpressure.
 */
import { afterAll, describe, expect, it } from "vitest";
import { createReach, WINDOW_RING_GROWTH_CASES } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor, type Grant } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import {
  ArraySource,
  BIG,
  CollectSink,
  ResolvedBlockSink,
  DBL,
  INT,
  TEXT,
  batchesOf,
  vectorOf,
  type Row,
  type Value,
} from "./join-fixtures.js";
import { drive, type Operator } from "./pipeline.js";
import type { SortColumn } from "./sort-keys.js";
import type { Frame, FrameBound } from "./window-frames.js";
import { streamable } from "./window-stream.js";
import {
  WindowOperator,
  windowMinimumBytes,
  type WindowFunctionSpec,
  type WindowSpec,
} from "./window.js";

const SEEDS = seedsFor("next-window", [0x3d1, 0xb7_3, 0x77]);
// Columns: p (partition, NULLs), k (order, ties, NULLs), x (float, NULLs), y (int64), t (text),
// o (small int: offsets, buckets), id (unique).
export const TYPES: PhysicalType[] = [INT, INT, DBL, BIG, TEXT, INT, INT];
const [P, K, X, Y, T, O, ID] = [0, 1, 2, 3, 4, 5, 6];

export function generateRows(random: () => number, n: number, parts: number, keys = 20): Row[] {
  return Array.from({ length: n }, (_, i): Row => [
    random() < 0.05 ? null : Math.floor(random() * parts),
    random() < 0.08 ? null : Math.floor(random() * keys),
    random() < 0.1 ? null : Math.round(random() * 1e4) / 64,
    random() < 0.05 ? null : BigInt(Math.floor(random() * 2e6) - 1e6),
    random() < 0.1 ? null : `t${String(Math.floor(random() * 50))}`,
    random() < 0.1 ? null : Math.floor(random() * 3),
    i,
  ]);
}

// --- the oracle (ported from ir-reference.ts windowRows) ----------------------------------

function compareKey(a: Value, b: Value, key: SortColumn): number {
  if (a === null || b === null) {
    if (a === null && b === null) return 0;
    return (a === null) === (key.nulls === "first") ? -1 : 1;
  }
  const order = a < b ? -1 : a > b ? 1 : 0;
  return key.direction === "desc" ? -order : order;
}

function sameKeys(a: Row, b: Row, keys: readonly SortColumn[]): boolean {
  return keys.every((key) => compareKey(a[key.column] ?? null, b[key.column] ?? null, key) === 0);
}

/** Expected function values for one partition's rows, in their (sorted) order. */
export function oracle(spec: WindowSpec, rows: readonly Row[]): Value[][] {
  const n = rows.length;
  const ordered = spec.orderBy.length > 0;
  const peerStart: number[] = [];
  const peerEnd: number[] = [];
  for (let i = 0; i < n; i++) {
    let s = i;
    while (s > 0 && sameKeys(rows[s - 1] ?? [], rows[i] ?? [], spec.orderBy)) s--;
    let e = i;
    while (e + 1 < n && sameKeys(rows[e + 1] ?? [], rows[i] ?? [], spec.orderBy)) e++;
    peerStart.push(s);
    peerEnd.push(e);
  }
  const groupOf: number[] = [];
  const groups: Array<[number, number]> = [];
  for (let i = 0; i < n; i++) {
    if (i === 0 || peerStart[i] === i) groups.push([i, peerEnd[i] ?? i]);
    groupOf.push(groups.length - 1);
  }
  const first = spec.orderBy[0];
  const descending = first?.direction === "desc";
  const sortKey = (row: Row): Value => (first === undefined ? null : (row[first.column] ?? null));
  const frameOf = (frame: Frame | null, index: number): number[] => {
    const peers: [number, number] = [peerStart[index] ?? index, peerEnd[index] ?? index];
    if (frame === null) return Array.from({ length: ordered ? peers[1] + 1 : n }, (_, i) => i);
    const position = (bound: FrameBound, end: boolean): number => {
      switch (bound.kind) {
        case "unbounded-preceding":
          return 0;
        case "unbounded-following":
          return n - 1;
        case "current-row":
          return frame.unit === "rows" ? index : end ? peers[1] : peers[0];
      }
      const amount = bound.offset ?? 0;
      const sign = bound.kind === "preceding" ? -1 : 1;
      if (frame.unit === "rows") return index + sign * amount;
      if (frame.unit === "groups") {
        const group = (groupOf[index] ?? 0) + sign * amount;
        if (group < 0) return end ? -1 : 0;
        if (group >= groups.length) return end ? n - 1 : n;
        const g = groups[group] ?? [0, 0];
        return end ? g[1] : g[0];
      }
      // RANGE with an offset. PostgreSQL (checked with PGlite; the reference interpreter differs
      // here): for a non-NULL row, NULL keys sort below every value with NULLS FIRST and above
      // every value with NULLS LAST; a NULL row's offset bounds are its peers'.
      const own = sortKey(rows[index] ?? []);
      if (own === null) return end ? peers[1] : peers[0];
      const step = (descending ? -sign : sign) * amount;
      // An int8 key compares exactly (bigints: PostgreSQL's in_range for int8), others as
      // numbers.
      const exact = typeof own === "bigint";
      const target = exact ? 0 : Number(own) + step;
      const targetBig = exact ? own + BigInt(step) : 0n;
      const nullsFirst = first?.nulls === "first";
      const inside = (candidate: Row) => {
        const key = sortKey(candidate);
        // In sort order: a NULL is before every value (NULLS FIRST) or after (NULLS LAST).
        const c =
          key === null
            ? nullsFirst !== descending
              ? -1
              : 1
            : exact
              ? (key as bigint) < targetBig
                ? -1
                : (key as bigint) > targetBig
                  ? 1
                  : 0
              : Number(key) < target
                ? -1
                : Number(key) > target
                  ? 1
                  : 0;
        const before = descending ? c > 0 : c < 0;
        const after = descending ? c < 0 : c > 0;
        return end ? !after : !before;
      };
      if (end) {
        for (let c = n - 1; c >= 0; c--) if (inside(rows[c] ?? [])) return c;
        return -1;
      }
      for (let c = 0; c < n; c++) if (inside(rows[c] ?? [])) return c;
      return n;
    };
    const start = Math.max(0, position(frame.start, false));
    const end = Math.min(n - 1, position(frame.end, true));
    const members: number[] = [];
    for (let c = start; c <= end; c++) {
      const peer = c >= peers[0] && c <= peers[1];
      if (frame.exclude === "current-row" && c === index) continue;
      if (frame.exclude === "group" && peer) continue;
      if (frame.exclude === "ties" && peer && c !== index) continue;
      members.push(c);
    }
    return members;
  };
  const amountOf = (call: WindowFunctionSpec, row: Row): Value => {
    const amount = call.amount;
    if (amount === undefined || amount === null) return null;
    return typeof amount === "number" ? amount : (row[amount.column] ?? null);
  };
  return rows.map((row, index) =>
    spec.functions.map((call): Value => {
      const arg = (target: number): Value =>
        call.arg === null ? true : (rows[target]?.[call.arg] ?? null);
      switch (call.fn) {
        case "row_number":
          return BigInt(index + 1);
        case "rank":
          return BigInt((peerStart[index] ?? index) + 1);
        case "dense_rank":
          return BigInt((groupOf[index] ?? 0) + 1);
        case "percent_rank":
          return n <= 1 ? 0 : (peerStart[index] ?? index) / (n - 1);
        case "cume_dist":
          return ((peerEnd[index] ?? index) + 1) / n;
        case "ntile": {
          const buckets = amountOf(call, row);
          if (buckets === null) return null;
          const count = Number(buckets);
          const size = Math.floor(n / count);
          const larger = n % count;
          const boundary = larger * (size + 1);
          return index < boundary
            ? Math.floor(index / (size + 1)) + 1
            : larger + Math.floor((index - boundary) / Math.max(size, 1)) + 1;
        }
        case "lag":
        case "lead": {
          const offset =
            call.amount === undefined || call.amount === null ? 1 : amountOf(call, row);
          if (offset === null) return null;
          const target = call.fn === "lag" ? index - Number(offset) : index + Number(offset);
          if (target >= 0 && target < n) return arg(target);
          return call.fallback === undefined || call.fallback === null ? null : fallbackValue(call);
        }
      }
      const frame = frameOf(call.frame, index);
      switch (call.fn) {
        case "first_value":
          return frame[0] === undefined ? null : arg(frame[0]);
        case "last_value": {
          const last = frame.at(-1);
          return last === undefined ? null : arg(last);
        }
        case "nth_value": {
          const nth = amountOf(call, row);
          if (nth === null) return null;
          const target = frame[Number(nth) - 1];
          return target === undefined ? null : arg(target);
        }
      }
      const present = frame.map(arg).filter((v): v is Exclude<Value, null> => v !== null);
      switch (call.fn) {
        case "count_star":
          return BigInt(frame.length);
        case "count":
          return BigInt(present.length);
      }
      if (present.length === 0) return null;
      if (call.fn === "min" || call.fn === "max") {
        let best = present[0] ?? null;
        for (const v of present) {
          if (typeof v === "number" && typeof best === "number") {
            if (Number.isNaN(v)) best = call.fn === "max" ? v : Number.isNaN(best) ? v : best;
            else if (Number.isNaN(best)) best = call.fn === "min" ? v : best;
            else if (call.fn === "max" ? v > best : v < best) best = v;
          } else if (best !== null && (call.fn === "max" ? v > best : v < best)) best = v;
        }
        return best;
      }
      if (typeof present[0] === "bigint") {
        const sum = present.reduce<bigint>((a, v) => a + (v as bigint), 0n);
        return call.fn === "sum" ? sum : Number(sum) / present.length;
      }
      let sum = 0;
      for (const v of present) sum += Number(v);
      if (call.fn === "sum") return call.type.kind === "int64" ? BigInt(sum) : sum;
      return sum / present.length;
    }),
  );
}

function fallbackValue(call: WindowFunctionSpec): Value {
  const vector = call.fallback;
  if (vector?.form !== "flat") return null;
  const data = vector.data;
  if (data.layout === "varlen")
    return new TextDecoder().decode(
      data.bytes.subarray(data.offsets[0] ?? 0, data.offsets[1] ?? 0),
    );
  if (data.layout === "f64" || data.layout === "i32") return data.values[0] ?? null;
  if (data.layout === "int64")
    return data.values.repr === "safe"
      ? BigInt(data.values.values[0] ?? 0)
      : (data.values.values[0] ?? 0n);
  return null;
}

// --- the operator -------------------------------------------------------------------------

/** A grant-local observer has no shared method to restore after an abandoned body. */
function observeGrant(grant: Grant, observed?: number[]): Grant {
  if (observed === undefined) return grant;
  return new Proxy(grant, {
    get(target, key) {
      if (key === "allocate")
        return (bytes: number) => {
          observed.push(bytes);
          return target.allocate(bytes);
        };
      const value: unknown = Reflect.get(target, key, target);
      return typeof value === "function"
        ? (value as (...args: unknown[]) => unknown).bind(target)
        : value;
    },
  });
}

export async function runWindow(
  spec: WindowSpec,
  rows: readonly Row[],
  options: {
    slack?: number;
    blockEvery?: number;
    batchRows?: number;
    dictionary?: boolean;
    int64?: "safe" | "int64";
    allocations?: number[];
  } = {},
) {
  const minimum = windowMinimumBytes(spec.types, spec.functions.length);
  const cap =
    options.slack === undefined
      ? 512 << 20
      : Math.max(
          minimum + options.slack + (1 << 20),
          Math.ceil(((minimum + options.slack) * 8) / 7),
        );
  const governor = new Governor(cap);
  const spill = new MemorySpillService();
  const grant = await governor.admit({ label: "window", minimumBytes: minimum, spillable: true });
  // Observe only this history's grant. An abandoned body cannot capture another test's
  // allocations or restore over its observer (unlike a shared Charge.prototype spy).
  const ownedGrant = observeGrant(grant, options.allocations);
  const sink = new CollectSink(options.blockEvery ?? 0);
  const op = new WindowOperator(spec, { grant: ownedGrant, spill, queryId: 6 }, sink);
  await drive(
    new ArraySource(
      batchesOf(spec.types, rows, {
        batchRows: options.batchRows ?? 2048,
        dictionary: options.dictionary ?? false,
        ...(options.int64 === undefined ? {} : { int64: options.int64 }),
      }),
    ),
    op,
  );
  await op.closed;
  expect(sink.finished).toBe(true);
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
  return { rows: sink.rows, profile: op.profile };
}

/** Output ⊆ input once each, sorted by (partition, order), and every value the oracle's. */
export function checkWindow(spec: WindowSpec, input: readonly Row[], output: readonly Row[]): void {
  const width = spec.types.length;
  expect(output.length).toBe(input.length);
  const ids = new Set(output.map((row) => row[ID]));
  expect(ids.size).toBe(input.length);
  const partKeys: SortColumn[] = spec.partitionBy.map((column) => ({
    column,
    direction: "asc",
    nulls: "last",
    collation: null,
  }));
  const keys = [...partKeys, ...spec.orderBy];
  // Partitions are contiguous runs; within one, rows are in ORDER BY order.
  const seen = new Set<string>();
  let start = 0;
  for (let i = 1; i <= output.length; i++) {
    const prev = output[i - 1] ?? [];
    const row = output[i];
    if (row !== undefined) {
      let order = 0;
      for (const key of keys) {
        order = compareKey(prev[key.column] ?? null, row[key.column] ?? null, key);
        if (order !== 0) break;
      }
      expect(order).toBeLessThanOrEqual(0);
      if (sameKeys(prev, row, partKeys)) continue;
    }
    const partition = output.slice(start, i);
    const name = JSON.stringify(spec.partitionBy.map((c) => String(partition[0]?.[c] ?? null)));
    expect(seen.has(name)).toBe(false);
    seen.add(name);
    const expected = oracle(
      spec,
      partition.map((r) => r.slice(0, width)),
    );
    partition.forEach((r, j) => {
      const actual = r.slice(width);
      const want = expected[j] ?? [];
      actual.forEach((value, f) => {
        const call = spec.functions[f];
        const w = want[f] ?? null;
        const label = `${call?.fn ?? "?"} row ${String(j)} of partition ${name} (frame ${JSON.stringify(call?.frame)})`;
        if (
          process.env.WINDOW_DEBUG === "1" &&
          !(
            value === w ||
            (typeof value === "number" && typeof w === "number" && Number.isNaN(w))
          ) &&
          JSON.stringify(String(value)) !== JSON.stringify(String(w))
        )
          process.stderr.write(
            `MISMATCH ${label} actual ${String(value)} expected ${String(w)} row ${JSON.stringify(partition[j]?.map(String))} neighbours ${JSON.stringify(partition.slice(Math.max(0, j - 3), j + 4).map((x) => [x[K], x[T]].map(String)))}\n`,
          );
        if (typeof value === "number" && typeof w === "number" && Number.isNaN(w))
          expect(Number.isNaN(value), label).toBe(true);
        else expect(value, label).toEqual(w);
      });
    });
    start = i;
  }
}

// --- generated specs ------------------------------------------------------------------------

const KINDS = [
  "unbounded-preceding",
  "preceding",
  "current-row",
  "following",
  "unbounded-following",
] as const;

function randomFrame(random: () => number, rangeOk: boolean): Frame | null {
  if (random() < 0.2) return null;
  const units = rangeOk ? (["rows", "range", "groups"] as const) : (["rows", "groups"] as const);
  const unit = units[Math.floor(random() * units.length)] ?? "rows";
  let a = Math.floor(random() * 4);
  let b = 1 + Math.floor(random() * 4);
  if (a > b) [a, b] = [b, a];
  const bound = (index: number): FrameBound => {
    const kind = KINDS[index] ?? "current-row";
    return {
      kind,
      offset: kind === "preceding" || kind === "following" ? Math.floor(random() * 4) : null,
    };
  };
  const excludes = ["no-others", "current-row", "group", "ties"] as const;
  return {
    unit,
    start: bound(a),
    end: bound(b),
    exclude: excludes[Math.floor(random() * excludes.length)] ?? "no-others",
  };
}

const TEXT_FALLBACK = vectorOf(TEXT, ["dflt"]);

function randomCall(random: () => number, rangeOk: boolean): WindowFunctionSpec {
  const pick = <T>(items: readonly T[]): T => items[Math.floor(random() * items.length)] as T;
  const fn = pick([
    "row_number",
    "rank",
    "dense_rank",
    "percent_rank",
    "cume_dist",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "nth_value",
    "count",
    "count_star",
    "sum",
    "avg",
    "min",
    "max",
  ]);
  const frame = randomFrame(random, rangeOk);
  switch (fn) {
    case "row_number":
    case "rank":
    case "dense_rank":
      return { fn, arg: null, frame: null, type: BIG };
    case "percent_rank":
    case "cume_dist":
      return { fn, arg: null, frame: null, type: DBL };
    case "ntile":
      return {
        fn,
        arg: null,
        amount: random() < 0.5 ? 1 + Math.floor(random() * 5) : null,
        frame: null,
        type: INT,
      };
    case "lag":
    case "lead": {
      const text = random() < 0.5;
      return {
        fn,
        arg: text ? T : Y,
        amount:
          random() < 0.3 ? { column: O } : random() < 0.5 ? null : Math.floor(random() * 4) - 1,
        fallback: text && random() < 0.5 ? TEXT_FALLBACK : null,
        frame: null,
        type: text ? TEXT : BIG,
      };
    }
    case "first_value":
    case "last_value":
      return { fn, arg: random() < 0.5 ? T : X, frame, type: random() < 0.5 ? TEXT : DBL };
    case "nth_value":
      return {
        fn,
        arg: T,
        amount: 1 + Math.floor(random() * 3),
        frame,
        type: TEXT,
      };
    case "count_star":
      return { fn, arg: null, frame, type: BIG };
    case "count":
      return { fn, arg: pick([X, Y, T]), frame, type: BIG };
  }
  const arg = pick([X, Y, K]);
  const argType = arg === X ? DBL : arg === Y ? BIG : INT;
  const type: PhysicalType =
    fn === "avg"
      ? DBL
      : fn === "sum"
        ? argType === INT
          ? BIG
          : argType === BIG
            ? { kind: "decimal", precision: null, scale: 0 }
            : DBL
        : argType;
  return { fn, arg, frame, type };
}

function fixTypes(call: WindowFunctionSpec): WindowFunctionSpec {
  // Value functions return their argument's type.
  if (call.fn === "first_value" || call.fn === "last_value")
    return { ...call, type: call.arg === T ? TEXT : DBL };
  return call;
}

function randomSpec(random: () => number): WindowSpec {
  const partitionBy = random() < 0.8 ? [P] : [];
  const rangeOnly = random() < 0.4;
  const direction = random() < 0.5 ? "asc" : "desc";
  const nulls = random() < 0.5 ? "first" : "last";
  const orderBy: SortColumn[] =
    random() < 0.1
      ? []
      : rangeOnly
        ? [{ column: K, direction, nulls, collation: null }]
        : [
            { column: K, direction, nulls, collation: null },
            ...(random() < 0.5
              ? [{ column: ID, direction: "asc" as const, nulls: "last" as const, collation: null }]
              : []),
          ];
  const rangeOk = orderBy.length === 1;
  const count = 1 + Math.floor(random() * 4);
  const functions = Array.from({ length: count }, () => fixTypes(randomCall(random, rangeOk)));
  // Without ORDER BY, only frames that need no order are meaningful; RANGE offsets need one key.
  return { types: TYPES, partitionBy, orderBy, functions };
}

describe("window: generated against the oracle", () => {
  for (const seed of SEEDS)
    for (const tight of [false, true])
      it(`seed ${String(seed)}${tight ? ", spilled" : ""}`, async () => {
        const random = mulberry32(seed + (tight ? 101 : 0));
        let spilled = 0;
        for (let round = 0; round < (tight ? 6 : 24); round++) {
          let spec = randomSpec(random);
          // A partition too large for the grant streams: only streamable calls (column-valued
          // amounts and the like hold a whole partition, window-stream.ts).
          if (tight) {
            const base = spec;
            const functions = base.functions.filter((call) => streamable(base, call) === null);
            if (functions.length === 0) continue;
            // Streams past 1,024 rows (the oracle is quadratic per partition: at least eight).
            spec = { ...base, partitionBy: [P], functions, streamRows: 1024 };
          }
          const parts = tight
            ? 16 + Math.floor(random() * 32)
            : random() < 0.2
              ? 1
              : 1 + Math.floor(random() * 40);
          // Tight: many keys, so peer groups (and the frames built on them) stay small while the
          // sort spills and large partitions stream; memory is bounded by the frame.
          const rows = generateRows(
            random,
            tight ? 24_000 : 1 + Math.floor(random() * 3000),
            parts,
            tight ? 5000 : 20,
          );
          if (process.env.WINDOW_DEBUG === "1")
            process.stderr.write(
              `ROUND ${String(round)} rows ${String(rows.length)} ${JSON.stringify(spec.functions.map((f) => [f.fn, f.arg, f.amount, f.frame]))} part ${JSON.stringify(spec.partitionBy)} order ${JSON.stringify(spec.orderBy)}\n`,
            );
          const result = await runWindow(spec, rows, {
            // The minimum grant: the sort spills, and a partition too large to hold streams.
            ...(tight ? { slack: 0 } : {}),
            blockEvery: random() < 0.3 ? 2 : 0,
            batchRows: 1 + Math.floor(random() * 2048),
          });
          const checkStart = performance.now();
          checkWindow(spec, rows, result.rows);
          if (process.env.WINDOW_DEBUG === "1")
            process.stderr.write(
              `TIMING round ${String(round)} check ${(performance.now() - checkStart).toFixed(0)} ms\n`,
            );
          spilled +=
            result.profile.sort.runs +
            result.profile.spilledPartitions +
            result.profile.streamedPartitions;
        }
        if (tight) expect(spilled).toBeGreaterThan(0);
      }, 120_000);
});

describe("window: the grouped sort against the oracle (window-sort.ts)", () => {
  // Partition keys of every grouped kind (int, text — plain or dictionary — int64) or none;
  // ORDER BY keys of ints, floats (NaN-free, NULLs) and int64 (negative), either direction and
  // NULL placement; both paths on the same rows, and a grant tight enough to move the rows to
  // the sort operator part way (the fallback).
  const parts = [[], [P], [T], [Y]] as const;
  const orderKeys = [K, X, Y, ID] as const;
  for (const seed of seedsFor("next-window-grouped", [0x6a1, 0x2b7]))
    it(`seed ${String(seed)}`, async () => {
      const random = mulberry32(seed);
      let grouped = 0;
      let fallbacks = 0;
      for (let round = 0; round < 18; round++) {
        const partitionBy = [...(parts[Math.floor(random() * parts.length)] ?? [])];
        const orderBy: SortColumn[] = Array.from(
          { length: random() < 0.15 ? 0 : 1 + Math.floor(random() * 2) },
          (): SortColumn => ({
            column: orderKeys[Math.floor(random() * orderKeys.length)] ?? K,
            direction: random() < 0.5 ? "asc" : "desc",
            nulls: random() < 0.5 ? "first" : "last",
            collation: null,
          }),
        );
        const single = orderBy.length === 1 ? orderBy[0]?.column : undefined;
        const rangeOk = single === K || single === X;
        const functions = Array.from({ length: 1 + Math.floor(random() * 3) }, () =>
          fixTypes(randomCall(random, rangeOk)),
        );
        const tight = round % 6 === 5;
        const base: WindowSpec = { types: TYPES, partitionBy, orderBy, functions };
        const spec: WindowSpec = tight
          ? {
              ...base,
              functions: functions.filter((call) => streamable(base, call) === null),
              streamRows: 1024,
            }
          : base;
        if (spec.functions.length === 0) continue;
        const rows = generateRows(
          random,
          tight ? 24_000 : 1 + Math.floor(random() * 3000),
          tight ? 24 : 1 + Math.floor(random() * 40),
          tight ? 5000 : 20,
        );
        const options = {
          ...(tight ? { slack: 0 } : {}),
          blockEvery: random() < 0.3 ? 2 : 0,
          batchRows: 1 + Math.floor(random() * 2048),
          dictionary: random() < 0.5,
        };
        const result = await runWindow(spec, rows, options);
        checkWindow(spec, rows, result.rows);
        grouped += result.profile.groupedRows > 0 ? 1 : 0;
        fallbacks += result.profile.groupedFallbacks;
        // The sort operator's path gives the same rows (as sets: order among peers is free).
        if (!tight) {
          const sorted = await runWindow({ ...spec, sortPath: "sort" }, rows, options);
          expect(sorted.profile.groupedRows).toBe(0);
          const key = (row: readonly unknown[]) =>
            JSON.stringify(row, (_, v: unknown) => (typeof v === "bigint" ? `${String(v)}n` : v));
          const order = (rowsOf: ReadonlyArray<readonly unknown[]>) =>
            new Map(rowsOf.map((row) => [row[ID], key(row)]));
          const mine = order(result.rows);
          const theirs = order(sorted.rows);
          // Peer-order-dependent functions (row_number, first_value, …) may differ among
          // peers: compare only when the order is total (ID is a key).
          if (orderBy.some((k) => k.column === ID))
            for (const [id, row] of mine) expect(row, `id ${String(id)}`).toBe(theirs.get(id));
        }
      }
      expect(grouped).toBeGreaterThan(10);
      void fallbacks;
    }, 600_000);

  it("rows the grant refuses move to the sort operator part way (the fallback)", async () => {
    const random = mulberry32(0x5f1);
    const rows = generateRows(random, 60_000, 30, 5000);
    const spec: WindowSpec = {
      types: TYPES,
      partitionBy: [T],
      orderBy: [
        { column: K, direction: "desc", nulls: "first", collation: null },
        { column: ID, direction: "asc", nulls: "last", collation: null },
      ],
      functions: [
        { fn: "row_number", arg: null, frame: null, type: BIG },
        { fn: "lag", arg: X, frame: null, type: DBL },
        {
          fn: "sum",
          arg: X,
          frame: {
            unit: "rows",
            start: { kind: "preceding", offset: 3 },
            end: { kind: "current-row", offset: null },
            exclude: "no-others",
          },
          type: DBL,
        },
      ],
      streamRows: 1024,
    };
    for (const dictionary of [false, true]) {
      const result = await runWindow(spec, rows, { slack: 0, dictionary, batchRows: 700 });
      expect(result.profile.groupedFallbacks).toBe(1);
      expect(result.profile.sort.rowsIn).toBe(60_000);
      checkWindow(spec, rows, result.rows);
    }
  }, 120_000);

  it("int64 keys past 2^53 order exactly in their two normalized words (BigInt64Array)", async () => {
    // Values one apart around ±2^53 and ±2^62 (a double cannot tell them apart), as ORDER BY
    // keys both ways and as partition keys.
    const values = [0n, 1n, 2n, 3n, 5n].flatMap((d) => [
      TWO53 + d,
      TWO53 - d,
      -TWO53 - d,
      (1n << 62n) + d,
      -(1n << 62n) - d,
    ]);
    const rows = values.flatMap((y, i) => [0, 1].map((p): Row => [p, 0, 0, y, null, 0, 2 * i + p]));
    for (const direction of ["asc", "desc"] as const)
      for (const partitionBy of [[P], [Y]]) {
        const spec: WindowSpec = {
          types: TYPES,
          partitionBy,
          orderBy: [{ column: Y, direction, nulls: "last", collation: null }],
          functions: [
            { fn: "row_number", arg: null, frame: null, type: BIG },
            { fn: "rank", arg: null, frame: null, type: BIG },
          ],
        };
        const result = await runWindow(spec, rows, { int64: "int64", batchRows: 7 });
        expect(result.profile.groupedRows).toBe(rows.length);
        checkWindow(spec, rows, result.rows);
      }
  });

  it("a partition column a function reads is stored and gathered (first_value, count of it)", async () => {
    const random = mulberry32(0x91);
    const rows = generateRows(random, 2000, 7);
    const spec: WindowSpec = {
      types: TYPES,
      partitionBy: [T],
      orderBy: [{ column: ID, direction: "desc", nulls: "last", collation: null }],
      functions: [
        { fn: "first_value", arg: T, frame: null, type: TEXT },
        { fn: "count", arg: T, frame: null, type: BIG },
        { fn: "lag", arg: T, frame: null, type: TEXT },
      ],
    };
    for (const dictionary of [false, true]) {
      const result = await runWindow(spec, rows, { dictionary, batchRows: 300 });
      expect(result.profile.groupedRows).toBe(2000);
      checkWindow(spec, rows, result.rows);
    }
  });
});

// --- exact int64, wide floats, long offsets (review B7-p2d-1; the generator ruling) --------

const TWO53 = 9007199254740992n;
const BIG_VALUES = [
  TWO53 - 1n,
  TWO53,
  TWO53 + 1n,
  TWO53 + 3n,
  -TWO53 - 1n,
  -TWO53 - 5n,
  (1n << 62n) + 7n,
  -(1n << 62n) - 9n,
  12n,
  -40n,
];
const WIDE_FLOATS = [0.1, -0.3, 1e20, -1e20, 4503599627370495.5, 9007199254740991, 7.25, -2.5];

/**
 * Rows whose int64 column y holds values past ±2^53 (sent as BigInt64Array) and whose float x
 * has fractions next to magnitudes past 2^53, so running counts and sums cross it.
 */
function wideRows(random: () => number, n: number, parts: number): Row[] {
  const pick = <V>(items: readonly V[]): V => items[Math.floor(random() * items.length)] as V;
  return Array.from({ length: n }, (_, i): Row => [
    random() < 0.05 ? null : Math.floor(random() * parts),
    random() < 0.08 ? null : Math.floor(random() * 20),
    random() < 0.1 ? null : random() < 0.6 ? pick(WIDE_FLOATS) : Math.round(random() * 1e4) / 64,
    random() < 0.08 ? null : pick(BIG_VALUES) + BigInt(Math.floor(random() * 3)),
    random() < 0.1 ? null : `t${String(Math.floor(random() * 50))}`,
    random() < 0.1 ? null : Math.floor(random() * 3),
    i,
  ]);
}

const EXACT_STATES = [
  "int64 past 2^53 as BigInt64Array",
  "MIN/MAX over int64 past 2^53",
  "RANGE offsets over int64 keys past 2^53",
  "COUNT over floats crossing 2^53",
  "SUM over floats crossing 2^53",
  "lag/lead looking back past the 4,096-row head",
  "lag/lead looking ahead past the 4,096-row head",
  "grouped sort over int64 keys past 2^53",
] as const;

describe("window: exact int64, wide floats and long offsets against the oracle", () => {
  const reach = createReach(
    "next-window-exact",
    (["resident", "streamed"] as const).flatMap((path) =>
      EXACT_STATES.map((state) => `${path}: ${state}` as const),
    ),
  );
  afterAll(() => {
    reach.assertReached();
  });
  const frames: Array<Frame | null> = [
    null,
    {
      unit: "rows",
      start: { kind: "unbounded-preceding", offset: null },
      end: { kind: "current-row", offset: null },
      exclude: "no-others",
    },
    {
      unit: "rows",
      start: { kind: "preceding", offset: 3 },
      end: { kind: "current-row", offset: null },
      exclude: "no-others",
    },
    {
      unit: "rows",
      start: { kind: "current-row", offset: null },
      end: { kind: "following", offset: 2 },
      exclude: "no-others",
    },
  ];
  const DECIMAL: PhysicalType = { kind: "decimal", precision: null, scale: 0 };
  for (const seed of seedsFor("next-window-exact", [0x5e1, 0x2a7]))
    for (const streamed of [false, true])
      it(`seed ${String(seed)}${streamed ? ", streamed" : ""}`, async () => {
        const random = mulberry32(seed + (streamed ? 31 : 0));
        const path = streamed ? "streamed" : "resident";
        for (let round = 0; round < 14; round++) {
          const pick = <V>(items: readonly V[]): V =>
            items[Math.floor(random() * items.length)] as V;
          const long = round % 4 === 3;
          const range = !long && random() < 0.3;
          const direction = random() < 0.5 ? "asc" : "desc";
          const nulls = random() < 0.5 ? "first" : "last";
          const orderBy: SortColumn[] = range
            ? [{ column: Y, direction, nulls, collation: null }]
            : long
              ? [{ column: ID, direction: "asc", nulls: "last", collation: null }]
              : pick([
                  [
                    { column: Y, direction, nulls, collation: null },
                    { column: ID, direction: "asc", nulls: "last", collation: null },
                  ],
                  [
                    { column: K, direction, nulls, collation: null },
                    { column: ID, direction: "asc", nulls: "last", collation: null },
                  ],
                ] as SortColumn[][]);
          let functions: WindowFunctionSpec[];
          if (long) {
            // Offsets past the 4,096-row head cache, either sign, both functions.
            functions = Array.from({ length: 2 }, (): WindowFunctionSpec => ({
              fn: pick(["lag", "lead"]),
              arg: pick([Y, ID]),
              amount: pick([-5000, -4500, 4500, 5000, -1, 1]),
              frame: null,
              type: BIG,
            }));
            functions = functions.map((call) => ({ ...call, type: call.arg === Y ? BIG : INT }));
          } else if (range) {
            const frame: Frame = {
              unit: "range",
              start: pick([
                { kind: "preceding", offset: pick([0, 1, 3]) },
                { kind: "unbounded-preceding", offset: null },
              ] as FrameBound[]),
              end: pick([
                { kind: "following", offset: pick([0, 2]) },
                { kind: "current-row", offset: null },
              ] as FrameBound[]),
              exclude: "no-others",
            };
            functions = [
              { fn: "count_star", arg: null, frame, type: BIG },
              { fn: pick(["min", "max"]), arg: Y, frame, type: BIG },
              { fn: "sum", arg: Y, frame, type: DECIMAL },
            ];
          } else {
            const frame = pick(frames);
            functions = [
              { fn: "count", arg: X, frame, type: BIG },
              { fn: "sum", arg: X, frame, type: DBL },
              { fn: pick(["min", "max"]), arg: Y, frame, type: BIG },
              { fn: pick(["count", "sum", "avg"]), arg: Y, frame, type: BIG },
            ].map((call) =>
              call.fn === "sum" && call.arg === Y
                ? { ...call, type: DECIMAL }
                : call.fn === "avg"
                  ? { ...call, type: DBL }
                  : call,
            );
          }
          const base: WindowSpec = {
            types: TYPES,
            partitionBy: long ? pick([[], [P]]) : random() < 0.5 ? [P] : [Y],
            orderBy,
            functions,
          };
          const spec: WindowSpec = streamed
            ? {
                ...base,
                partitionBy: long ? base.partitionBy : [P],
                functions: functions.filter((call) => streamable(base, call) === null),
                streamRows: 64,
              }
            : base;
          if (spec.functions.length === 0) continue;
          const n = long ? 9500 + Math.floor(random() * 1500) : 200 + Math.floor(random() * 1800);
          const rows = wideRows(random, n, long ? 2 : 1 + Math.floor(random() * 6));
          const result = await runWindow(spec, rows, {
            int64: "int64",
            batchRows: 1 + Math.floor(random() * 2048),
            blockEvery: random() < 0.3 ? 2 : 0,
          });
          checkWindow(spec, rows, result.rows);
          // Reach: what this history exercised.
          reach.beginHistory();
          const wideY = rows.some((row) => {
            const y = row[Y];
            return typeof y === "bigint" && (y > TWO53 - 1n || y < 1n - TWO53);
          });
          const uses = (fn: string, arg: number) =>
            spec.functions.some((call) => call.fn === fn && call.arg === arg);
          const usesY =
            spec.functions.some((call) => call.arg === Y) ||
            spec.orderBy.some((key) => key.column === Y);
          if (wideY && usesY) reach.reach(`${path}: int64 past 2^53 as BigInt64Array`);
          if (wideY && (uses("min", Y) || uses("max", Y)))
            reach.reach(`${path}: MIN/MAX over int64 past 2^53`);
          if (wideY && range && spec.functions.some((call) => call.frame?.unit === "range"))
            reach.reach(`${path}: RANGE offsets over int64 keys past 2^53`);
          const wideX = rows.some((row) => Math.abs(Number(row[X] ?? 0)) >= 1e16);
          if (wideX && uses("count", X)) reach.reach(`${path}: COUNT over floats crossing 2^53`);
          if (wideX && uses("sum", X)) reach.reach(`${path}: SUM over floats crossing 2^53`);
          const sizes = new Map<string, number>();
          for (const row of rows) {
            const key = spec.partitionBy.length > 0 ? String(row[P]) : "";
            sizes.set(key, (sizes.get(key) ?? 0) + 1);
          }
          const largest = Math.max(...sizes.values());
          for (const call of spec.functions) {
            if (call.fn !== "lag" && call.fn !== "lead") continue;
            const k = typeof call.amount === "number" ? call.amount : 1;
            const back = call.fn === "lag" ? k > 0 : k < 0;
            if (Math.abs(k) > 4096 && largest > 4096 + Math.abs(k))
              reach.reach(
                `${path}: lag/lead looking ${back ? "back" : "ahead"} past the 4,096-row head`,
              );
          }
          if (
            wideY &&
            result.profile.groupedRows > 0 &&
            spec.orderBy.some((key) => key.column === Y)
          )
            reach.reach(`${path}: grouped sort over int64 keys past 2^53`);
        }
      }, 600_000);
});

describe("window: growth charges the overlap (review B7-p2d-1 #5, governor rule 4)", () => {
  // A growing array is charged in full while the old one is still live, then the old charge is
  // freed: the charge calls must include each new size whole (not its difference).
  async function allocations(
    spec: WindowSpec,
    rows: readonly Row[],
    check = true,
  ): Promise<number[]> {
    const sizes: number[] = [];
    const result = await runWindow(spec, rows, { allocations: sizes });
    if (check) checkWindow(spec, rows, result.rows);
    else expect(result.rows.length).toBe(rows.length);
    return sizes;
  }
  it("an abandoned allocation observer cannot capture or retire a later history", async () => {
    const governor = new Governor(8 << 20);
    const first = await governor.admit({ label: "old", minimumBytes: 1024, spillable: true });
    const second = await governor.admit({ label: "next", minimumBytes: 1024, spillable: true });
    const a: number[] = [],
      b: number[] = [];
    const old = observeGrant(first, a),
      next = observeGrant(second, b);
    try {
      expect(old.allocate(17)).toBe(true);
      expect(next.allocate(23)).toBe(true);
      // The abandoned observer is still alive after the later history starts.
      expect(old.allocate(19)).toBe(true);
      expect(a).toEqual([17, 19]);
      expect(b).toEqual([23]);
      old.close();
      expect(next.allocate(29)).toBe(true);
      expect(b).toEqual([23, 29]);
    } finally {
      first.close();
      second.close();
    }
    expect(governor.usedBytes).toBe(0);
  });

  const sum = (offset: number): WindowFunctionSpec => ({
    fn: "sum",
    arg: X,
    frame: {
      unit: "rows",
      start: { kind: "preceding", offset },
      end: { kind: "current-row", offset: null },
      exclude: "no-others",
    },
    type: DBL,
  });

  for (const { rows: count, state } of WINDOW_RING_GROWTH_CASES)
    it(`a streamed partition's ring (${String(count)} rows)`, async () => {
      // Keep the original seed, 30k-row history and 20k lookback. A singleton frame still
      // retains the ring's rows but avoids re-adding 20,001 floats per output under V8 coverage.
      const rows = generateRows(mulberry32(0x77), count, 1).map((row): Row => [0, ...row.slice(1)]);
      const reach = createReach(`ring ${String(count)}`, [state]);
      reach.beginHistory();
      const call = sum(20_000);
      const spec: WindowSpec = {
        types: TYPES,
        partitionBy: [P],
        orderBy: [{ column: ID, direction: "asc", nulls: "last", collation: null }],
        functions: [
          {
            ...call,
            frame: {
              unit: "rows",
              start: { kind: "preceding", offset: 20_000 },
              exclude: "no-others",
              end: { kind: "preceding", offset: 20_000 },
            },
          },
        ],
        streamRows: 64,
      };
      const perRow = 4 + 9 + 1 * (8 + 1 + 8 + 8 + 8);
      const sizes = await allocations(spec, rows);
      expect(sizes).toContain(16_384 * perRow);
      if (count > 16_384) expect(sizes).toContain(32_768 * perRow);
      else expect(sizes).not.toContain(32_768 * perRow);
      reach.reach(state);
      reach.assertReached();
    }, 120_000);

  it("a resident partition's scratch", async () => {
    // Partitions of 3,000 then 10,000 rows: the scratch grows from 3,000 to 10,000 rows.
    const rows = [
      ...Array.from({ length: 3000 }, (_, i): Row => [0, i, i / 8, BigInt(i), null, 1, i]),
      ...Array.from({ length: 10_000 }, (_, i): Row => [1, i, i / 8, BigInt(i), null, 1, 3000 + i]),
    ];
    const spec: WindowSpec = {
      types: TYPES,
      partitionBy: [P],
      orderBy: [{ column: ID, direction: "asc", nulls: "last", collation: null }],
      functions: [sum(3)],
    };
    // Scratch.bytes(capacity, functions 1, arguments 1).
    const scratch = (capacity: number) => capacity * (20 + 8 + 23 + 9 + 12) + 12;
    const sizes = await allocations(spec, rows);
    expect(sizes).toContain(scratch(3000));
    expect(sizes).toContain(scratch(10_000));
  }, 120_000);
});

describe("window: streamed partitions against the oracle", () => {
  // `streamRows` 64: nearly every partition streams (window-stream.ts), through its frames.
  for (const seed of SEEDS)
    it(`seed ${String(seed)}, streamed`, async () => {
      const random = mulberry32(seed + 202);
      let streamed = 0;
      for (let round = 0; round < 24; round++) {
        const base = randomSpec(random);
        const functions = base.functions.filter((call) => streamable(base, call) === null);
        if (functions.length === 0) continue;
        const spec: WindowSpec = { ...base, functions, streamRows: 64 };
        const parts = random() < 0.3 ? 1 : 1 + Math.floor(random() * 8);
        const rows = generateRows(random, 1 + Math.floor(random() * 3000), parts);
        if (process.env.WINDOW_DEBUG === "1")
          process.stderr.write(
            `STREAM ROUND ${String(round)} rows ${String(rows.length)} ${JSON.stringify(spec.functions.map((f) => [f.fn, f.arg, f.amount, f.frame]))} part ${JSON.stringify(spec.partitionBy)} order ${JSON.stringify(spec.orderBy)}\n`,
          );
        const result = await runWindow(spec, rows, {
          blockEvery: random() < 0.3 ? 2 : 0,
          batchRows: 1 + Math.floor(random() * 2048),
        });
        checkWindow(spec, rows, result.rows);
        streamed += result.profile.streamedPartitions;
      }
      expect(streamed).toBeGreaterThan(0);
    }, 120_000);
});

describe("window: errors with PostgreSQL's SQLSTATE", () => {
  const rows = generateRows(mulberry32(5), 50, 3);
  const cases: Array<[string, WindowFunctionSpec, string]> = [
    [
      "a negative frame offset",
      {
        fn: "sum",
        arg: X,
        frame: {
          unit: "rows",
          start: { kind: "preceding", offset: -1 },
          end: { kind: "current-row", offset: null },
          exclude: "no-others",
        },
        type: DBL,
      },
      "22013",
    ],
    [
      "a NULL frame offset",
      {
        fn: "sum",
        arg: X,
        frame: {
          unit: "rows",
          start: { kind: "preceding", offset: null },
          end: { kind: "current-row", offset: null },
          exclude: "no-others",
        },
        type: DBL,
      },
      "22004",
    ],
    ["ntile(0)", { fn: "ntile", arg: null, amount: 0, frame: null, type: INT }, "22014"],
    ["nth_value(t, 0)", { fn: "nth_value", arg: T, amount: 0, frame: null, type: TEXT }, "22016"],
  ];
  for (const [name, call, code] of cases)
    it(`${name}: ${code}`, async () => {
      const spec: WindowSpec = {
        types: TYPES,
        partitionBy: [P],
        orderBy: [{ column: K, direction: "asc", nulls: "last", collation: null }],
        functions: [call],
      };
      await expect(runWindow(spec, rows)).rejects.toMatchObject({ code });
    });
});

describe("window: R23 slices and the abort signal", () => {
  const spec = (streamRows?: number): WindowSpec => ({
    types: TYPES,
    partitionBy: [P],
    orderBy: [
      { column: K, direction: "asc", nulls: "last", collation: null },
      { column: ID, direction: "asc", nulls: "last", collation: null },
    ],
    functions: [
      { fn: "row_number", arg: null, frame: null, type: BIG },
      {
        fn: "sum",
        arg: X,
        frame: {
          unit: "rows",
          start: { kind: "preceding", offset: 9 },
          end: { kind: "current-row", offset: null },
          exclude: "no-others",
        },
        type: DBL,
      },
      { fn: "lag", arg: T, frame: null, type: TEXT },
    ],
    ...(streamRows === undefined ? {} : { streamRows }),
  });
  async function operator(s: WindowSpec, sink: Operator, signal?: AbortSignal) {
    const minimum = windowMinimumBytes(s.types, s.functions.length);
    const governor = new Governor(512 << 20);
    const spill = new MemorySpillService();
    const grant = await governor.admit({ label: "window", minimumBytes: minimum, spillable: true });
    const op = new WindowOperator(
      s,
      { grant, spill, queryId: 8, ...(signal ? { signal } : {}) },
      sink,
    );
    return { op, governor, spill };
  }
  async function pushAll(op: Operator, rows: readonly Row[]) {
    for (const batch of batchesOf(TYPES, rows)) if (op.push(batch) === "blocked") await op.ready();
  }
  for (const path of ["grouped", "sort"] as const)
    for (const streamed of [false, true])
      it(`a zero-delay timer runs before finish completes (${streamed ? "streamed" : "resident"}, ${path})`, async () => {
        const sink = new CollectSink();
        const { op } = await operator(
          { ...spec(streamed ? 1024 : undefined), sortPath: path },
          sink,
        );
        await pushAll(op, generateRows(mulberry32(3), 150_000, 3));
        let serviced = false;
        const timer = setTimeout(() => {
          serviced = true;
        }, 0);
        await op.finish();
        clearTimeout(timer);
        expect(serviced).toBe(true);
        expect(sink.rows).toHaveLength(150_000);
        expect(op.profile.streamedPartitions > 0).toBe(streamed);
        expect(op.profile.groupedRows).toBe(path === "grouped" ? 150_000 : 0);
      }, 120_000);

  for (const path of ["grouped", "sort"] as const)
    it(`a zero-delay timer runs before finish completes, downstream blocked with a resolved ready (R23.4, ${path})`, async () => {
      const sink = new ResolvedBlockSink();
      const { op } = await operator({ ...spec(), sortPath: path }, sink);
      await pushAll(op, generateRows(mulberry32(7), 150_000, 3));
      let serviced = false;
      const timer = setTimeout(() => {
        serviced = true;
      }, 0);
      await op.finish();
      clearTimeout(timer);
      expect(serviced).toBe(true);
      expect(sink.rows).toBe(150_000);
    }, 120_000);

  for (const path of ["grouped", "sort"] as const)
    it(`an abort before finish rejects and releases everything (${path})`, async () => {
      const controller = new AbortController();
      const sink = new CollectSink();
      const { op, governor, spill } = await operator(
        { ...spec(), sortPath: path },
        sink,
        controller.signal,
      );
      await pushAll(op, generateRows(mulberry32(4), 5000, 3));
      controller.abort(new Error("cancel requested"));
      await expect(Promise.resolve().then(() => op.finish())).rejects.toThrow("cancel requested");
      expect(sink.rows).toHaveLength(0);
      await op.closed;
      expect(governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    });

  it("an abort while the grouped sort runs rejects and releases everything", async () => {
    const controller = new AbortController();
    const sink = new CollectSink();
    const { op, governor, spill } = await operator(spec(), sink, controller.signal);
    await pushAll(op, generateRows(mulberry32(6), 200_000, 3));
    setTimeout(() => {
      controller.abort(new Error("cancelled mid-sort"));
    }, 0);
    await expect(Promise.resolve().then(() => op.finish())).rejects.toThrow("cancelled mid-sort");
    await op.closed;
    expect(sink.rows.length).toBeLessThan(200_000);
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  }, 60_000);

  it("emission checks the signal after downstream backpressure", async () => {
    const controller = new AbortController();
    let pushes = 0;
    const sink: Operator = {
      push: () => {
        pushes++;
        controller.abort(new Error("cancelled while blocked"));
        return "blocked";
      },
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    const { op, governor } = await operator(spec(), sink, controller.signal);
    await pushAll(op, generateRows(mulberry32(5), 6000, 1));
    await expect(Promise.resolve().then(() => op.finish())).rejects.toThrow(
      "cancelled while blocked",
    );
    expect(pushes).toBe(1);
    await op.closed;
    expect(governor.usedBytes).toBe(0);
  });
});

describe("window: a streamed partition across ring slides", () => {
  it("frames that look back keep their rows when the ring slides (ROWS 1 PRECEDING, lag)", async () => {
    // One 8,000-row partition, totally ordered: the stream's ring (1,024 rows at first) slides
    // several times while rows that frames still read must stay.
    const orderBy: SortColumn[] = [
      { column: K, direction: "asc", nulls: "last", collation: null },
      { column: ID, direction: "asc", nulls: "last", collation: null },
    ];
    const spec: WindowSpec = {
      types: TYPES,
      partitionBy: [],
      orderBy,
      functions: [
        {
          fn: "count",
          arg: T,
          frame: {
            unit: "rows",
            start: { kind: "preceding", offset: 1 },
            end: { kind: "current-row", offset: null },
            exclude: "no-others",
          },
          type: BIG,
        },
        {
          fn: "sum",
          arg: X,
          frame: {
            unit: "rows",
            start: { kind: "preceding", offset: 3 },
            end: { kind: "preceding", offset: 1 },
            exclude: "no-others",
          },
          type: DBL,
        },
        { fn: "lag", arg: T, amount: 2, frame: null, type: TEXT },
      ],
      streamRows: 64,
    };
    const rows = generateRows(mulberry32(11), 8000, 1, 5000);
    const result = await runWindow(spec, rows, { batchRows: 333 });
    expect(result.profile.streamedPartitions).toBe(1);
    checkWindow(spec, rows, result.rows);
  }, 120_000);
});
