/**
 * BENCH ADAPTER, not the executor's production result sink (it does not exist yet).
 * Owns compact scan batches (no selection vector) as the current owner's FramePayload.
 * ResultChannel numbers/credits frames; the caller uses the unchanged owner transfer policy.
 * Identity dictionaries are sent once up to the cap, then text dictionaries flatten per frame.
 * Refuses selections and capped non-text dictionaries (outside the wide SELECT * fixture).
 * This policy is a stand-in for future frame assembly. No production import here.
 */
import type { ByteBudget } from "../../../packages/core/src/next/storage/extents.js";
import { bigintBytes } from "../../../packages/core/src/next/memory/governor.js";
import {
  frameByteLength,
  STREAM_DICTIONARY_BYTES,
  type FrameOffer,
  type WireFlat,
  type WireVector,
} from "../../../packages/core/src/next/exec/result.js";
import type {
  Batch,
  DictionaryVector,
  FlatVector,
  Int64Values,
  VectorData,
} from "../../../packages/core/src/next/vector/vector.js";

const REGISTRY_ENTRY_BYTES = 64;
const validityBytes = (v: FlatVector, n: number) =>
  v.validity === null ? 0 : 4 * Math.ceil(n / 32);

function flatBytes(vector: FlatVector, n: number): number {
  const d = vector.data;
  let bytes = validityBytes(vector, n);
  switch (d.layout) {
    case "bool":
    case "i32":
    case "f32":
    case "f64":
      return bytes + n * d.values.BYTES_PER_ELEMENT;
    case "int64":
      return bytes + 8 * n;
    case "fixed16":
      return bytes + 16 * n;
    case "interval":
      return bytes + 16 * n;
    case "varlen":
      return bytes + 4 * (n + 1) + (d.offsets[n] ?? 0) - (d.offsets[0] ?? 0);
    case "decimal": {
      bytes += (d.scales === null ? 0 : 2 * n) + (d.specials === null ? 0 : n);
      const c = d.coefficients;
      if (c.repr === "int128") return bytes + 16 * n;
      if (c.repr !== "wide") return bytes + 8 * n;
      for (let k = 0; k < n; k++) bytes += bigintBytes(c.values[k] ?? 0n);
      return bytes;
    }
  }
}

/** Copy floats as bytes: preserve NaN payloads/signalling bits and signed zero. */
const f64 = (a: Float64Array, n: number) =>
  new Float64Array(new Uint8Array(a.buffer, a.byteOffset, 8 * n).slice().buffer);
const f32 = (a: Float32Array, n: number) =>
  new Float32Array(new Uint8Array(a.buffer, a.byteOffset, 4 * n).slice().buffer);
const integers = (a: Int64Values, n: number): Int64Values =>
  a.repr === "safe"
    ? { repr: "safe", values: f64(a.values, n) }
    : { repr: "int64", values: a.values.slice(0, n) };

/** Own offsets with a typed copy; only biased slices need a subtraction pass. */
function ownedOffsets(source: Int32Array, n: number, first: number): Int32Array {
  const out = source.slice(0, n + 1);
  if (first !== 0) for (let k = 0; k <= n; k++) out[k] = (out[k] ?? 0) - first;
  return out;
}

function ownFlat(vector: FlatVector, n: number): WireFlat {
  const d = vector.data;
  let data: VectorData;
  switch (d.layout) {
    case "bool":
    case "i32":
      data = { layout: d.layout, values: d.values.slice(0, n) } as VectorData;
      break;
    case "f64":
      data = { layout: "f64", values: f64(d.values, n) };
      break;
    case "f32":
      data = { layout: "f32", values: f32(d.values, n) };
      break;
    case "int64":
      data = { layout: "int64", values: integers(d.values, n) };
      break;
    case "fixed16":
      data = { layout: "fixed16", bytes: d.bytes.slice(0, 16 * n) };
      break;
    case "interval":
      data = {
        layout: "interval",
        months: d.months.slice(0, n),
        days: d.days.slice(0, n),
        micros: integers(d.micros, n),
      };
      break;
    case "varlen": {
      const first = d.offsets[0] ?? 0,
        end = d.offsets[n] ?? first;
      data = {
        layout: "varlen",
        bytes: d.bytes.slice(first, end),
        offsets: ownedOffsets(d.offsets, n, first),
      };
      break;
    }
    case "decimal": {
      const c = d.coefficients;
      data = {
        layout: "decimal",
        scale: d.scale,
        scales: d.scales?.slice(0, n) ?? null,
        specials: d.specials?.slice(0, n) ?? null,
        coefficients:
          c.repr === "int128"
            ? { repr: "int128", limbs: c.limbs.slice(0, 4 * n) }
            : c.repr === "wide"
              ? { repr: "wide", values: c.values.slice(0, n) }
              : integers(c, n),
      };
      break;
    }
  }
  return {
    form: "flat",
    length: n,
    data,
    validity: vector.validity?.slice(0, Math.ceil(n / 32)) ?? null,
  };
}

const valid = (v: DictionaryVector, k: number) =>
  v.validity === null || ((v.validity[k >>> 5] ?? 0) & (1 << (k & 31))) !== 0;
function textFlattenBytes(v: DictionaryVector, n: number): number {
  const d = v.dictionary.data;
  if (d.layout !== "varlen") throw new Error("Bench cap fallback only supports text dictionaries");
  let bytes = 4 * (n + 1) + (v.validity === null ? 0 : 4 * Math.ceil(n / 32));
  for (let k = 0; k < n; k++)
    if (valid(v, k)) {
      const code = v.codes[k] ?? 0;
      if (code < 0 || code >= v.dictionary.length) throw new Error("Bad bench dictionary code");
      bytes += (d.offsets[code + 1] ?? 0) - (d.offsets[code] ?? 0);
    }
  return bytes;
}
function ownTextFlatten(v: DictionaryVector, n: number, plannedBytes: number): WireFlat {
  const d = v.dictionary.data;
  if (d.layout !== "varlen") throw new Error("Not text");
  const size = plannedBytes - 4 * (n + 1) - (v.validity === null ? 0 : 4 * Math.ceil(n / 32));
  const bytes = new Uint8Array(size),
    offsets = new Int32Array(n + 1);
  let at = 0;
  for (let k = 0; k < n; k++) {
    if (valid(v, k)) {
      const code = v.codes[k] ?? 0,
        start = d.offsets[code] ?? 0,
        end = d.offsets[code + 1] ?? start;
      bytes.set(d.bytes.subarray(start, end), at);
      at += end - start;
    }
    offsets[k + 1] = at;
  }
  return {
    form: "flat",
    length: n,
    validity: v.validity?.slice(0, Math.ceil(n / 32)) ?? null,
    data: { layout: "varlen", bytes, offsets },
  };
}

export class BenchFrameAssembler {
  readonly #ids = new WeakMap<FlatVector, number>();
  #next = 1;
  #sentBytes = 0;
  #registryBytes = 0;
  #live = 0;
  #closed = false;
  #flattened = 0;
  get flattenedColumns(): number {
    return this.#flattened;
  }
  get dictionaryBytes(): number {
    return this.#sentBytes;
  }
  constructor(
    readonly budget: ByteBudget,
    readonly dictionaryCap = STREAM_DICTIONARY_BYTES,
  ) {}

  offer(batch: Batch): FrameOffer {
    if (this.#closed) throw new Error("Closed bench assembler");
    if (batch.selection !== null) throw new Error("Bench adapter expects dense SELECT * output");
    const n = batch.length,
      newIds = new Map<FlatVector, number>();
    const fallback = new Map<DictionaryVector, number>();
    let bytes = 0,
      deltaBytes = 0;
    for (const vector of batch.columns) {
      if (vector.length < n) throw new Error("Short scan vector");
      if (vector.form === "flat") bytes += flatBytes(vector, n);
      else if (vector.form === "constant") bytes += flatBytes(vector.value, 1);
      else {
        if (!this.#ids.has(vector.dictionary) && !newIds.has(vector.dictionary)) {
          const size = flatBytes(vector.dictionary, vector.dictionary.length);
          if (
            this.#sentBytes +
              deltaBytes +
              this.#registryBytes +
              (newIds.size + 1) * REGISTRY_ENTRY_BYTES +
              size >
            this.dictionaryCap
          )
            fallback.set(vector, textFlattenBytes(vector, n));
          else {
            newIds.set(vector.dictionary, this.#next + newIds.size);
            deltaBytes += size;
          }
        }
        bytes += fallback.has(vector)
          ? (fallback.get(vector) ?? 0)
          : 4 * n + (vector.validity === null ? 0 : 4 * Math.ceil(n / 32));
      }
    }
    bytes += deltaBytes;
    const registryBytes = newIds.size * REGISTRY_ENTRY_BYTES;
    if (this.#sentBytes + deltaBytes + this.#registryBytes + registryBytes > this.dictionaryCap)
      throw new Error("Bench adapter stream dictionary cap exceeded");
    if (!this.budget.allocate(bytes + registryBytes))
      throw new RangeError("Bench result budget refused frame");
    try {
      const dictionaries = [...newIds].map(([v, id]) => ({
        id,
        from: 0,
        values: ownFlat(v, v.length),
      }));
      const columns = batch.columns.map((vector): WireVector => {
        if (vector.form === "flat") return ownFlat(vector, n);
        if (vector.form === "constant")
          return { form: "constant", length: n, value: ownFlat(vector.value, 1) };
        if (fallback.has(vector)) return ownTextFlatten(vector, n, fallback.get(vector) ?? 0);
        const id = this.#ids.get(vector.dictionary) ?? newIds.get(vector.dictionary);
        if (id === undefined) throw new Error("Missing bench stream dictionary");
        return {
          form: "dictionary",
          length: n,
          dictionary: id,
          codes: vector.codes.slice(0, n),
          validity: vector.validity?.slice(0, Math.ceil(n / 32)) ?? null,
        };
      });
      const payload = { rowCount: n, columns, dictionaries };
      if (frameByteLength(payload) !== bytes) throw new Error("Frame sizing mismatch");
      for (const [v, id] of newIds) this.#ids.set(v, id);
      this.#next += newIds.size;
      this.#sentBytes += deltaBytes;
      this.#registryBytes += registryBytes;
      this.#live++;
      this.#flattened += fallback.size;
      return { payload, requiresLsn: 0, free: this.#releaseFrame(bytes) };
    } catch (error) {
      this.budget.free(bytes + registryBytes);
      throw error;
    }
  }
  // A separate activation keeps the release handle from retaining the serializer's borrowed
  // dictionaries/fallback vectors through a shared closure context (the p2j lifetime rule).
  #releaseFrame(bytes: number): () => void {
    let released = false;
    return () => {
      if (!released) {
        released = true;
        this.#live--;
        this.budget.free(bytes);
      }
    };
  }
  close(): void {
    if (this.#live !== 0) throw new Error("Bench frame credit still outstanding");
    if (!this.#closed) {
      this.#closed = true;
      this.budget.free(this.#registryBytes);
      this.#registryBytes = 0;
    }
  }
}
