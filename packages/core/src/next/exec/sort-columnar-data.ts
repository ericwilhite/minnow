/** Numeric sort words and a single gather of each payload column. No key byte strings. */
import { layoutOf, type PhysicalType } from "../types.js";
import { allocateData, dataBytes, type FlatVector, type VectorData } from "../vector/vector.js";
import { ColumnStore, wordsOf } from "./column-store.js";
import type { SortColumn } from "./sort-keys.js";
import { frameTransferList } from "./frame-buffers.js";
import type { Slice, SlicedJob } from "./slices.js";

type Varlen = Extract<VectorData, { layout: "varlen" }>;
export interface GatherColumn {
  readonly type: PhysicalType;
  readonly chunks: FlatVector[];
  readonly nullable: boolean;
  readonly floats: Float64Array[];
  readonly integers: Int32Array[];
  readonly singles: Float32Array[];
  readonly booleans: Uint8Array[];
  readonly wide: Int32Array[];
  readonly variable: Varlen[];
}

/** Cache homogeneous primitive arrays once, outside the row gather loops. */
export function* gatherSource(
  store: ColumnStore,
  c: number,
  slice: Slice,
): SlicedJob<GatherColumn> {
  const type = store.types[c];
  if (!type) throw new TypeError("Missing gather type");
  const source: GatherColumn = {
    type,
    nullable: store.hasNulls(c),
    chunks: [],
    floats: [],
    integers: [],
    singles: [],
    booleans: [],
    wide: [],
    variable: [],
  };
  for (let k = 0; k < store.chunkCount; k++) {
    const chunk = store.chunk(c, k),
      data = chunk.data;
    source.chunks.push(chunk);
    switch (data.layout) {
      case "f64":
        source.floats.push(data.values);
        break;
      case "i32":
        source.integers.push(data.values);
        break;
      case "f32":
        source.singles.push(data.values);
        break;
      case "bool":
        source.booleans.push(data.values);
        break;
      case "int64":
        if (data.values.repr === "safe") source.floats.push(data.values.values);
        else source.wide.push(wordsOf(data.values.values));
        break;
      case "varlen":
        source.variable.push(data);
        break;
      default:
        throw new TypeError("Unsupported gather layout");
    }
    if ((k & 31) === 31 && slice.due()) yield;
  }
  return source;
}

function copyFloats(
  arrays: Float64Array[],
  ids: Int32Array,
  from: number,
  n: number,
  out: Float64Array,
  shift: number,
  mask: number,
): void {
  for (let i = 0; i < n; i++) {
    const id = ids[from + i] ?? 0;
    out[i] = arrays[id >>> shift]?.[id & mask] ?? 0;
  }
}

export function numericWordCount(type: PhysicalType): number {
  switch (type.kind) {
    case "bool":
    case "int32":
    case "date":
      return 1;
    case "int64":
    case "time":
    case "timestamp":
    case "float32":
    case "float64":
      return 2;
    default:
      return 0;
  }
}

/** Chunk-sized calls are synchronous; the caller checks the shared clock between them. */
function encodeChunk(
  chunk: FlatVector,
  header: Uint32Array | null,
  high: Uint32Array | null,
  low: Uint32Array,
  at: number,
  key: SortColumn,
  floating: Float64Array,
  floatingWords: Uint32Array,
): void {
  const data = chunk.data;
  const bits = chunk.validity;
  const flip = key.direction === "desc" ? 0xffff_ffff : 0;
  for (let i = 0; i < chunk.length; i++) {
    const out = at + i;
    if (bits !== null && ((bits[i >>> 5] ?? 0) & (1 << (i & 31))) === 0) {
      if (header !== null) header[out] = key.nulls === "first" ? 0 : 2;
      continue;
    }
    if (header !== null) header[out] = 1;
    let h = 0;
    let l: number;
    switch (data.layout) {
      case "bool":
        l = data.values[i] === 1 ? 1 : 0;
        break;
      case "i32":
        l = ((data.values[i] ?? 0) ^ 0x8000_0000) >>> 0;
        break;
      case "int64": {
        const values = data.values;
        if (values.repr === "safe") {
          const value = values.values[i] ?? 0;
          const upper = Math.floor(value / 4_294_967_296);
          h = ((upper >>> 0) ^ 0x8000_0000) >>> 0;
          l = (value - upper * 4_294_967_296) >>> 0;
        } else {
          const words = wordsOf(values.values);
          h = ((words[2 * i + 1] ?? 0) ^ 0x8000_0000) >>> 0;
          l = (words[2 * i] ?? 0) >>> 0;
        }
        break;
      }
      case "f32":
      case "f64": {
        const value = data.values[i] ?? 0;
        floating[0] = value !== value ? NaN : value === 0 ? 0 : value;
        h = floatingWords[1] ?? 0;
        l = floatingWords[0] ?? 0;
        if (h >= 0x8000_0000) {
          h = ~h >>> 0;
          l = ~l >>> 0;
        } else h = (h | 0x8000_0000) >>> 0;
        break;
      }
      default:
        throw new TypeError("Non-numeric columnar sort key");
    }
    if (high !== null) high[out] = (h ^ flip) >>> 0;
    low[out] = (l ^ flip) >>> 0;
  }
}

export function* encodeNumericColumns(
  store: ColumnStore,
  keys: readonly SortColumn[],
  columns: readonly Uint32Array[],
  floating: Float64Array,
  floatingWords: Uint32Array,
  slice: Slice,
): SlicedJob<void> {
  let word = 0;
  for (const key of keys) {
    const type = store.types[key.column];
    if (type === undefined) throw new TypeError("Missing sort key type");
    const header = store.hasNulls(key.column) ? (columns[word++] ?? null) : null;
    const high = numericWordCount(type) === 2 ? (columns[word++] ?? null) : null;
    const low = columns[word++];
    if (low === undefined) throw new TypeError("Missing numeric key words");
    for (let k = 0; k < store.chunkCount; k++) {
      encodeChunk(
        store.chunk(key.column, k),
        header,
        high,
        low,
        k * store.chunkRows,
        key,
        floating,
        floatingWords,
      );
      if (slice.due()) yield;
    }
  }
}

export function varlenGatherBytes(
  source: GatherColumn,
  ids: Int32Array,
  from: number,
  n: number,
  shift: number,
  mask: number,
): number {
  let bytes = 0;
  for (let i = from; i < from + n; i++) {
    const id = ids[i] ?? 0;
    const data = source.variable[id >>> shift];
    if (!data) throw new TypeError("Missing varlen chunk");
    const slot = id & mask;
    bytes += (data.offsets[slot + 1] ?? 0) - (data.offsets[slot] ?? 0);
  }
  return bytes;
}

export function gatherBytes(source: GatherColumn, n: number, varlen: number): number {
  return dataBytes(source.type, n, varlen) + (source.nullable ? 4 * Math.ceil(n / 32) : 0);
}

/** Each output is a dedicated exact-length buffer; payload values never pass through keys. */
export function gatherColumn(
  source: GatherColumn,
  ids: Int32Array,
  from: number,
  n: number,
  shift: number,
  mask: number,
  varlen: number,
): FlatVector {
  const chunks = source.chunks;
  const first = chunks[0];
  if (first === undefined) throw new TypeError("Missing gather source");
  let data: VectorData;
  if (first.data.layout === "int64" && first.data.values.repr === "int64")
    data = { layout: "int64", values: { repr: "int64", values: new BigInt64Array(n) } };
  else data = allocateData(source.type, n, varlen);
  const validity = source.nullable ? new Uint32Array(Math.ceil(n / 32)).fill(0xffff_ffff) : null;
  if (validity !== null)
    for (let i = 0; i < n; i++) {
      const id = ids[from + i] ?? 0;
      const bits = chunks[id >>> shift]?.validity;
      if (bits != null && ((bits[(id & mask) >>> 5] ?? 0) & (1 << (id & 31))) === 0)
        validity[i >>> 5] = (validity[i >>> 5] ?? 0) & ~(1 << (i & 31));
    }
  switch (data.layout) {
    case "f64":
    case "int64": {
      if (data.layout === "int64" && data.values.repr === "int64") {
        const out = wordsOf(data.values.values);
        for (let i = 0; i < n; i++) {
          const id = ids[from + i] ?? 0;
          const words = source.wide[id >>> shift],
            slot = (id & mask) * 2;
          out[i * 2] = words?.[slot] ?? 0;
          out[i * 2 + 1] = words?.[slot + 1] ?? 0;
        }
      } else {
        const out = data.layout === "f64" ? data.values : (data.values.values as Float64Array);
        copyFloats(source.floats, ids, from, n, out, shift, mask);
      }
      break;
    }
    case "i32":
      for (let i = 0; i < n; i++) {
        const id = ids[from + i] ?? 0;
        data.values[i] = source.integers[id >>> shift]?.[id & mask] ?? 0;
      }
      break;
    case "f32":
      for (let i = 0; i < n; i++) {
        const id = ids[from + i] ?? 0;
        data.values[i] = source.singles[id >>> shift]?.[id & mask] ?? 0;
      }
      break;
    case "bool":
      for (let i = 0; i < n; i++) {
        const id = ids[from + i] ?? 0;
        data.values[i] = source.booleans[id >>> shift]?.[id & mask] ?? 0;
      }
      break;
    case "varlen": {
      let at = 0;
      for (let i = 0; i < n; i++) {
        const id = ids[from + i] ?? 0,
          input = source.variable[id >>> shift],
          slot = id & mask;
        if (!input) throw new TypeError("Missing varlen gather");
        const start = input.offsets[slot] ?? 0,
          end = input.offsets[slot + 1] ?? 0;
        if (end - start <= 4096)
          for (let b = start; b < end; b++) data.bytes[at++] = input.bytes[b] ?? 0;
        else {
          data.bytes.set(input.bytes.subarray(start, end), at);
          at += end - start;
        }
        data.offsets[i + 1] = at;
      }
      break;
    }
    default:
      throw new TypeError(`Unsupported columnar payload ${layoutOf(source.type)}`);
  }
  return { form: "flat", type: source.type, length: n, validity, data };
}

/** A whole unchanged chunk can move; subranges/padded arenas remain a gather. The caller has
 * already proved a permutation, so no emitted source row is used by another output frame. */
export function moveGatherColumn(
  source: GatherColumn,
  ids: Int32Array,
  from: number,
  n: number,
  shift: number,
  mask: number,
): FlatVector | null {
  const first = ids[from] ?? -1;
  if (first < 0 || (first & mask) !== 0) return null;
  const chunk = source.chunks[first >>> shift];
  if (chunk?.length !== n) return null;
  for (let i = 1; i < n; i++) if (ids[from + i] !== first + i) return null;
  const data = chunk.data;
  if (data.layout === "varlen") {
    if (data.offsets.length !== n + 1 || data.bytes.length !== data.offsets[n]) return null;
  } else if (data.layout === "int64") {
    if (data.values.values.length !== n) return null;
  } else if (
    data.layout === "bool" ||
    data.layout === "i32" ||
    data.layout === "f32" ||
    data.layout === "f64"
  ) {
    if (data.values.length !== n) return null;
  } else return null;
  if (chunk.validity !== null && chunk.validity.length !== Math.ceil(n / 32)) return null;
  try {
    frameTransferList({ rowCount: n, columns: [chunk], dictionaries: [] });
  } catch {
    return null;
  }
  return chunk;
}
