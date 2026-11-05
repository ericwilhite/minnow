/**
 * Test support for the heavy operators (track B7): batches from JS rows, a source, a collecting
 * sink with optional backpressure, values back out of vectors, and a nested-loop join oracle.
 * Imported only by tests and benches.
 */
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import { setInt128 } from "../vector/int128.js";
import { decodeUtf8 } from "../vector/varlen.js";
import {
  newDictionaryId,
  type Batch,
  type FlatVector,
  type Selection,
  type Vector,
  type VectorData,
} from "../vector/vector.js";
import {
  drive,
  type BatchSource,
  type Operator,
  type PushResult,
  type SourceItem,
} from "./pipeline.js";
import { HashJoin, hashJoinMinimumBytes, type HashJoinSpec, type VectorPredicate } from "./join.js";
import type { HashJoinKind } from "./join.js";

export type Value = null | boolean | number | bigint | string;
export type Row = readonly Value[];

export const INT: PhysicalType = { kind: "int32", bits: 32 };
export const BIG: PhysicalType = { kind: "int64" };
export const DBL: PhysicalType = { kind: "float64" };
export const TEXT: PhysicalType = { kind: "text" };
export const BOOL: PhysicalType = { kind: "bool" };
export const DATE: PhysicalType = { kind: "date" };
export const NUM: PhysicalType = { kind: "decimal", precision: 12, scale: 2 };
export const ANYNUM: PhysicalType = { kind: "decimal", precision: null, scale: null };

const encoder = new TextEncoder();

export interface BatchOptions {
  /** Rows per batch (default 2,048). */
  readonly batchRows?: number;
  /** Emit int64 columns as BigInt64Array (default safe Float64Array). */
  readonly int64?: "safe" | "int64";
  /** Text columns as dictionary vectors. */
  readonly dictionary?: boolean;
  /** Add a selection that skips every `skip`-th physical row (the skipped rows are garbage). */
  readonly skip?: number;
}

/** A flat vector of `type` holding `values` (decimal values are scaled coefficients as bigint). */
export function vectorOf(
  type: PhysicalType,
  values: readonly Value[],
  options: BatchOptions = {},
): Vector {
  const n = values.length;
  const validity = new Uint32Array((n + 31) >>> 5).fill(0xffff_ffff);
  let nulls = false;
  for (let i = 0; i < n; i++)
    if (values[i] === null) {
      nulls = true;
      validity[i >>> 5] = (validity[i >>> 5] ?? 0) & ~(1 << (i & 31));
    }
  let data: VectorData;
  switch (type.kind) {
    case "bool":
      data = { layout: "bool", values: Uint8Array.from(values, (v) => (v === true ? 1 : 0)) };
      break;
    case "int32":
    case "date":
      data = { layout: "i32", values: Int32Array.from(values, (v) => Number(v ?? 0)) };
      break;
    case "float64":
      data = { layout: "f64", values: Float64Array.from(values, (v) => Number(v ?? 0)) };
      break;
    case "int64":
    case "timestamp":
    case "time":
      data =
        options.int64 === "int64"
          ? {
              layout: "int64",
              values: { repr: "int64", values: BigInt64Array.from(values, (v) => BigInt(v ?? 0)) },
            }
          : {
              layout: "int64",
              values: { repr: "safe", values: Float64Array.from(values, (v) => Number(v ?? 0)) },
            };
      break;
    case "decimal": {
      const scale = type.scale;
      if (scale !== null)
        data = {
          layout: "decimal",
          scale,
          scales: null,
          coefficients: { repr: "safe", values: Float64Array.from(values, (v) => Number(v ?? 0)) },
          specials: null,
        };
      else {
        const limbs = new Uint32Array(n * 4);
        values.forEach((v, i) => {
          setInt128(limbs, i, BigInt(v ?? 0));
        });
        data = {
          layout: "decimal",
          scale: null,
          scales: Uint16Array.from(values, (_, i) => i % 3),
          coefficients: { repr: "int128", limbs },
          specials: null,
        };
      }
      break;
    }
    case "text":
    case "bytes":
    case "enum": {
      const parts = values.map((v) => encoder.encode(typeof v === "string" ? v : ""));
      if (options.dictionary === true) {
        const distinct = [...new Set(values.filter((v): v is string => typeof v === "string"))];
        const dict = vectorOf(type, distinct) as FlatVector;
        const index = new Map(distinct.map((v, i) => [v, i]));
        return {
          form: "dictionary",
          type,
          length: n,
          codes: Int32Array.from(values, (v) => (typeof v === "string" ? (index.get(v) ?? 0) : 7)),
          validity: nulls ? validity : null,
          dictionary: dict,
          dictionaryId: newDictionaryId(),
        };
      }
      const offsets = new Int32Array(n + 1);
      let total = 0;
      parts.forEach((p, i) => {
        total += p.length;
        offsets[i + 1] = total;
      });
      const bytes = new Uint8Array(total);
      parts.forEach((p, i) => bytes.set(p, offsets[i] ?? 0));
      data = { layout: "varlen", bytes, offsets };
      break;
    }
    default:
      throw new TypeError(`No fixture for ${type.kind}`);
  }
  return { form: "flat", type, length: n, validity: nulls ? validity : null, data };
}

/** Batches of `rows` (row-major) over `types`. */
export function batchesOf(
  types: readonly PhysicalType[],
  rows: readonly Row[],
  options: BatchOptions = {},
): Batch[] {
  const size = options.batchRows ?? 2048;
  const batches: Batch[] = [];
  for (let start = 0; start < rows.length; start += size) {
    let slice = rows.slice(start, start + size);
    let selection: Selection | null = null;
    if (options.skip !== undefined && options.skip > 1) {
      // Interleave garbage rows that the selection skips.
      const physical: Row[] = [];
      const active: number[] = [];
      for (const row of slice) {
        if (physical.length % options.skip === options.skip - 1)
          physical.push(types.map((t) => garbage(t)));
        active.push(physical.length);
        physical.push(row);
      }
      slice = physical;
      selection = { rows: Uint16Array.from(active), count: active.length };
    }
    batches.push({
      length: slice.length,
      columns: types.map((type, c) =>
        vectorOf(
          type,
          slice.map((row) => row[c] ?? null),
          options,
        ),
      ),
      selection,
    });
  }
  return batches;
}

function garbage(type: PhysicalType): Value {
  switch (type.kind) {
    case "text":
      return "garbage";
    case "bool":
      return true;
    case "int64":
      return 999_999n;
    default:
      return 999_999;
  }
}

/** The value, or a thrown error when it is null or undefined (test assertions). */
export function defined<T>(value: T | null | undefined): T {
  if (value === null || value === undefined) throw new Error("Expected a value");
  return value;
}

export class ArraySource implements BatchSource {
  #index = 0;
  constructor(readonly batches: readonly Batch[]) {}
  next(): SourceItem | null {
    const batch = this.batches[this.#index++];
    return batch === undefined ? null : { batch };
  }
  abort(): void {
    this.#index = this.batches.length;
  }
}

function bitSet(validity: Uint32Array | null, i: number): boolean {
  return validity === null || ((validity[i >>> 5] ?? 0) & (1 << (i & 31))) !== 0;
}

/** Value of `row` of `vector` (int64 as bigint, decimal as its coefficient bigint, text as string). */
export function valueAt(vector: Vector, row: number): Value {
  let flat: FlatVector;
  let slot: number;
  if (vector.form === "flat") {
    if (!bitSet(vector.validity, row)) return null;
    flat = vector;
    slot = row;
  } else if (vector.form === "constant") {
    if (!bitSet(vector.value.validity, 0)) return null;
    flat = vector.value;
    slot = 0;
  } else {
    if (!bitSet(vector.validity, row)) return null;
    flat = vector.dictionary;
    slot = vector.codes[row] ?? 0;
  }
  const data = flat.data;
  switch (data.layout) {
    case "bool":
      return data.values[slot] === 1;
    case "i32":
    case "f64":
    case "f32":
      return data.values[slot] ?? 0;
    case "int64":
      return data.values.repr === "safe"
        ? BigInt(data.values.values[slot] ?? 0)
        : (data.values.values[slot] ?? 0n);
    case "varlen":
      return decodeUtf8(data, slot);
    case "decimal": {
      const c = data.coefficients;
      if (c.repr === "safe") return BigInt(c.values[slot] ?? 0);
      if (c.repr === "int64") return c.values[slot] ?? 0n;
      if (c.repr === "wide") return c.values[slot] ?? 0n;
      const base = slot * 4;
      const u =
        BigInt(c.limbs[base] ?? 0) |
        (BigInt(c.limbs[base + 1] ?? 0) << 32n) |
        (BigInt(c.limbs[base + 2] ?? 0) << 64n) |
        (BigInt(c.limbs[base + 3] ?? 0) << 96n);
      return BigInt.asIntN(128, u);
    }
    default:
      throw new TypeError(`No fixture reader for ${data.layout}`);
  }
}

/** The active rows of a batch as JS rows. */
export function rowsOf(batch: Batch): Row[] {
  const rows: Row[] = [];
  const count = batch.selection === null ? batch.length : batch.selection.count;
  for (let i = 0; i < count; i++) {
    const row = batch.selection === null ? i : (batch.selection.rows[i] ?? 0);
    rows.push(batch.columns.map((column) => valueAt(column, row)));
  }
  return rows;
}

/** Collects output rows; with `blockEvery`, every n-th push returns `blocked` for one task. */
export class CollectSink implements Operator {
  readonly rows: Row[] = [];
  pushes = 0;
  finished = false;
  aborted: unknown = undefined;
  #pending: Promise<void> | null = null;
  constructor(readonly blockEvery = 0) {}
  push(batch: Batch): PushResult {
    if (this.#pending !== null) throw new Error("push while blocked");
    if (batch.length > 2048) throw new Error("batch longer than BATCH_ROWS");
    if (batch.selection !== null)
      for (let i = 1; i < batch.selection.count; i++)
        if ((batch.selection.rows[i] ?? 0) <= (batch.selection.rows[i - 1] ?? 0))
          throw new Error("selection not ascending");
    this.rows.push(...rowsOf(batch));
    this.pushes++;
    if (this.blockEvery > 0 && this.pushes % this.blockEvery === 0) {
      this.#pending = new Promise((resolve) => setTimeout(resolve, 0)).then(() => {
        this.#pending = null;
      });
      return "blocked";
    }
    return "ready";
  }
  ready(): Promise<void> {
    return this.#pending ?? Promise.resolve();
  }
  finish(): void {
    this.finished = true;
  }
  abort(reason: unknown): void {
    this.aborted = reason;
  }
}

/** A residual from a JS function over (probe row, build row) with SQL three-valued results. */
export function residualOf(
  probeWidth: number,
  fn: (probe: Row, build: Row) => boolean | null,
): VectorPredicate {
  const select = (batch: Batch, out: Selection, want: boolean | null): Selection => {
    const count = batch.selection === null ? batch.length : batch.selection.count;
    let kept = 0;
    for (let i = 0; i < count; i++) {
      const row = batch.selection === null ? i : (batch.selection.rows[i] ?? 0);
      const values = batch.columns.map((column) => valueAt(column, row));
      if (fn(values.slice(0, probeWidth), values.slice(probeWidth)) === want)
        out.rows[kept++] = row;
    }
    out.count = kept;
    return out;
  };
  return {
    filter: (batch, out) => select(batch, out, true),
    unknown: (batch, out) => select(batch, out, null),
  };
}

export interface RunOptions extends BatchOptions {
  /** Governor cap; default 256 MiB. `"tight"`: the join's minimum plus `slack` bytes. */
  readonly cap?: number | "tight";
  readonly slack?: number;
  readonly blockEvery?: number;
  readonly direct?: boolean;
  readonly probeBatchRows?: number;
}

export interface RunResult {
  readonly rows: Row[];
  readonly join: HashJoin;
  readonly governor: Governor;
  readonly spill: MemorySpillService;
}

/** Runs a hash join over JS rows through real pipelines. */
export async function runJoin(
  spec: HashJoinSpec,
  build: readonly Row[],
  probe: readonly Row[],
  options: RunOptions = {},
): Promise<RunResult> {
  const minimum = hashJoinMinimumBytes(spec);
  let cap: number;
  if (options.cap === "tight") {
    const want = minimum + (options.slack ?? 64 * 1024);
    cap = Math.max(want + (1 << 20), Math.ceil((want * 8) / 7) + 1);
  } else cap = options.cap ?? 256 * 1024 * 1024;
  const governor = new Governor(cap);
  const spill = new MemorySpillService();
  const grant = await governor.admit({ label: "join", minimumBytes: minimum, spillable: true });
  const join = new HashJoin(spec, {
    grant,
    spill,
    queryId: 1,
    ...(options.direct === undefined ? {} : { direct: options.direct }),
  });
  await drive(new ArraySource(batchesOf(spec.buildTypes, build, options)), join.build);
  const sink = new CollectSink(options.blockEvery ?? 0);
  await drive(
    new ArraySource(
      batchesOf(spec.probeTypes, probe, {
        ...options,
        batchRows: options.probeBatchRows ?? options.batchRows ?? 2048,
      }),
    ),
    join.probe(sink),
  );
  if (!sink.finished) throw new Error("sink not finished");
  return { rows: sink.rows, join, governor, spill };
}

// --- oracle -------------------------------------------------------------------------------------

function sqlEqual(a: Value, b: Value): boolean {
  if (typeof a === "number" && typeof b === "number")
    return a === b || (Number.isNaN(a) && Number.isNaN(b));
  return a === b;
}

export interface OracleSpec {
  readonly kind: HashJoinKind;
  readonly probeKeys: readonly number[];
  readonly buildKeys: readonly number[];
  readonly nullSafe: readonly boolean[];
  readonly correlated?: number;
  readonly residual?: (probe: Row, build: Row) => boolean | null;
  readonly probeWidth: number;
  readonly buildWidth: number;
}

/** Nested-loop join with SQL semantics: the oracle the generated tests compare against. */
export function oracleJoin(spec: OracleSpec, build: readonly Row[], probe: readonly Row[]): Row[] {
  const keyMatch = (p: Row, b: Row, from: number, to: number) => {
    for (let i = from; i < to; i++) {
      const x = p[spec.probeKeys[i] ?? 0] ?? null;
      const y = b[spec.buildKeys[i] ?? 0] ?? null;
      if (x === null || y === null) {
        if (!(spec.nullSafe[i] === true && x === null && y === null)) return false;
      } else if (!sqlEqual(x, y)) return false;
    }
    return true;
  };
  const residual = (p: Row, b: Row) =>
    spec.residual === undefined ? true : spec.residual(p, b) === true;
  const keys = spec.probeKeys.length;
  // Without a residual, candidates come from an index on the full key (nested loop otherwise).
  const keyText = (row: Row, columns: readonly number[]): string | null => {
    const parts: string[] = [];
    for (let i = 0; i < keys; i++) {
      const value = row[columns[i] ?? 0] ?? null;
      if (value === null && spec.nullSafe[i] !== true) return null;
      parts.push(
        value === null
          ? "∅"
          : typeof value === "number"
            ? Number.isNaN(value)
              ? "NaN"
              : String(value === 0 ? 0 : value)
            : `${typeof value}:${String(value)}`,
      );
    }
    return parts.join("\u0001");
  };
  const index = new Map<string, number[]>();
  if (spec.residual === undefined && spec.kind !== "mark")
    build.forEach((b, j) => {
      const text = keyText(b, spec.buildKeys);
      if (text === null) return;
      const list = index.get(text) ?? [];
      list.push(j);
      index.set(text, list);
    });
  const indexed = spec.residual === undefined && spec.kind !== "mark";
  const out: Row[] = [];
  const nullBuild = new Array<Value>(spec.buildWidth).fill(null);
  const nullProbe = new Array<Value>(spec.probeWidth).fill(null);
  const buildMatched = new Array<boolean>(build.length).fill(false);
  for (const p of probe) {
    if (spec.kind === "mark") {
      // Ruling R20.1: three-valued OR over the C-matching rows of (T = T' AND residual).
      const c = spec.correlated ?? 0;
      let mark: Value = false;
      for (const b of build) {
        if (!keyMatch(p, b, 0, c)) continue;
        let cond: boolean | null = true;
        for (let i = c; i < keys && cond !== false; i++) {
          const x = p[spec.probeKeys[i] ?? 0] ?? null;
          const y = b[spec.buildKeys[i] ?? 0] ?? null;
          if (x === null || y === null) cond = null;
          else if (!sqlEqual(x, y)) cond = false;
        }
        if (cond !== false && spec.residual !== undefined) {
          const r = spec.residual(p, b);
          cond = r === false ? false : r === null ? null : cond;
        }
        if (cond === true) {
          mark = true;
          break;
        }
        if (cond === null) mark = null;
      }
      out.push([...p, mark]);
      continue;
    }
    let any = false;
    const text = indexed ? keyText(p, spec.probeKeys) : null;
    const candidates = indexed ? (text === null ? [] : (index.get(text) ?? [])) : null;
    for (const j of candidates ?? build.map((_, index) => index)) {
      const b = build[j] ?? [];
      if (!keyMatch(p, b, 0, keys) || !residual(p, b)) continue;
      any = true;
      buildMatched[j] = true;
      if (
        spec.kind === "inner" ||
        spec.kind === "left" ||
        spec.kind === "right" ||
        spec.kind === "full"
      )
        out.push([...p, ...b]);
    }
    if (spec.kind === "semi" && any) out.push(p);
    if (spec.kind === "anti" && !any) out.push(p);
    if ((spec.kind === "left" || spec.kind === "full") && !any) out.push([...p, ...nullBuild]);
  }
  if (spec.kind === "right" || spec.kind === "full")
    build.forEach((b, j) => {
      if (!buildMatched[j]) out.push([...nullProbe, ...b]);
    });
  return out;
}

/** A canonical, sorted form of a row multiset for comparison. */
export function canonical(rows: readonly Row[]): string[] {
  return rows
    .map((row) =>
      JSON.stringify(row, (_, v: unknown) =>
        typeof v === "bigint"
          ? `${v.toString()}n`
          : typeof v === "number" && Number.isNaN(v)
            ? "NaN"
            : v,
      ),
    )
    .sort();
}
