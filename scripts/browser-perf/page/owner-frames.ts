/**
 * BENCH ADAPTER, not the executor's production result sink (it does not exist yet).
 * Owns compact scan batches (no selection vector) as the current owner's FramePayload.
 * ResultChannel numbers/credits frames; the caller uses the unchanged owner transfer policy.
 * Identity dictionaries are sent once up to the cap, then text dictionaries flatten per frame.
 * Refuses selections and capped non-text dictionaries (outside the wide SELECT * fixture).
 * A compact flat batch with explicit detachable-buffer permission is adopted as-is, with the
 * same result budget, retained through frame credit. Otherwise this adapter owns copies.
 * This policy is a stand-in for future frame assembly. No production import here.
 */
import type { Transfer } from "../../../packages/core/src/next/exec/pipeline.js";
import type { ByteBudget } from "../../../packages/core/src/next/storage/extents.js";
import { bigintBytes } from "../../../packages/core/src/next/memory/governor.js";
import {
  frameByteLength,
  frameTransferList,
  FrameBufferError,
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

/** Cheap eligibility check; ResultChannel still validates/sizes the completed FramePayload.
 * Never synthesize one-column payloads just to test permission (allocates per column/batch).
 */
function ownedView(
  view: ArrayBufferView,
  bytes: number,
  allowed: ReadonlySet<ArrayBuffer>,
): boolean {
  return (
    view.buffer instanceof ArrayBuffer &&
    view.byteOffset === 0 &&
    view.byteLength === bytes &&
    view.buffer.byteLength === bytes &&
    allowed.has(view.buffer)
  );
}
function ownedValidity(
  v: FlatVector | DictionaryVector,
  n: number,
  allowed: ReadonlySet<ArrayBuffer>,
): boolean {
  return v.validity === null || ownedView(v.validity, 4 * Math.ceil(n / 32), allowed);
}
function ownsFlat(v: FlatVector, n: number, allowed: ReadonlySet<ArrayBuffer>): boolean {
  if (!ownedValidity(v, n, allowed)) return false;
  const d = v.data;
  switch (d.layout) {
    case "bool":
    case "i32":
    case "f32":
    case "f64":
      return ownedView(d.values, n * d.values.BYTES_PER_ELEMENT, allowed);
    case "int64":
      return ownedView(d.values.values, 8 * n, allowed);
    case "fixed16":
      return ownedView(d.bytes, 16 * n, allowed);
    case "interval":
      return (
        ownedView(d.months, 4 * n, allowed) &&
        ownedView(d.days, 4 * n, allowed) &&
        ownedView(d.micros.values, 8 * n, allowed)
      );
    case "varlen":
      return (
        (d.offsets[0] ?? 0) === 0 &&
        ownedView(d.offsets, 4 * (n + 1), allowed) &&
        ownedView(d.bytes, d.offsets[n] ?? 0, allowed)
      );
    case "decimal": {
      if (d.scales !== null && !ownedView(d.scales, 2 * n, allowed)) return false;
      if (d.specials !== null && !ownedView(d.specials, n, allowed)) return false;
      const c = d.coefficients;
      return c.repr === "int128"
        ? ownedView(c.limbs, 16 * n, allowed)
        : c.repr !== "wide" && ownedView(c.values, 8 * n, allowed);
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
  #copied = 0;
  #adopted = 0;
  #liveCopied = 0;
  #liveAdopted = 0;
  #adoptedFrames = 0;
  get copyStats(): {
    copiedBytes: number;
    adoptedBytes: number;
    liveCopiedBytes: number;
    liveAdoptedBytes: number;
    adoptedFrames: number;
  } {
    return {
      copiedBytes: this.#copied,
      adoptedBytes: this.#adopted,
      liveCopiedBytes: this.#liveCopied,
      liveAdoptedBytes: this.#liveAdopted,
      adoptedFrames: this.#adoptedFrames,
    };
  }
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

  offer(batch: Batch, transfer?: Transfer): FrameOffer {
    try {
      if (this.#closed) throw new Error("Closed bench assembler");
      if (
        transfer?.detachableBuffers &&
        transfer.detachableBudget === this.budget &&
        batch.selection === null &&
        batch.columns.every(
          (v) =>
            v.form === "flat" &&
            !(v.data.layout === "decimal" && v.data.coefficients.repr === "wide"),
        )
      ) {
        const payload = {
          rowCount: batch.length,
          dictionaries: [],
          columns: batch.columns.map((v): WireFlat => {
            if (v.form !== "flat" || v.length < batch.length)
              throw new Error("Invalid terminal flat batch");
            return { form: "flat", length: batch.length, data: v.data, validity: v.validity };
          }),
        };
        let buffers: ArrayBuffer[] | null;
        try {
          buffers = frameTransferList(payload);
        } catch (error) {
          if (!(error instanceof FrameBufferError)) throw error;
          buffers = null;
        }
        if (buffers !== null && buffers.every((b) => transfer.detachableBuffers?.has(b))) {
          const bytes = frameByteLength(payload);
          this.#live++;
          this.#adoptedFrames++;
          this.#adopted += bytes;
          this.#liveAdopted += bytes;
          return { payload, requiresLsn: 0, free: this.#releaseAdopted(transfer, bytes) };
        }
      }
      return this.#copy(batch, transfer);
    } catch (error) {
      transfer?.release();
      throw error;
    }
  }

  // Independent activation: no borrowed vectors/page descriptors in retained release closures.
  #releaseAdopted(transfer: Transfer, bytes: number): () => void {
    let released = false;
    return () => {
      if (!released) {
        released = true;
        this.#live--;
        this.#liveAdopted -= bytes;
        transfer.release();
      }
    };
  }

  #copy(batch: Batch, transfer?: Transfer): FrameOffer {
    if (this.#closed) throw new Error("Closed bench assembler");
    if (batch.selection !== null) throw new Error("Bench adapter expects dense SELECT * output");
    const n = batch.length,
      newIds = new Map<FlatVector, number>();
    const adopted = new Map<FlatVector, WireFlat>();
    const adoptedCodes = new Set<DictionaryVector>();
    let adoptedBytes = 0;
    const fallback = new Map<DictionaryVector, number>();
    let bytes = 0,
      deltaBytes = 0;
    for (const vector of batch.columns) {
      if (vector.length < n) throw new Error("Short scan vector");
      if (vector.form === "flat") {
        const size = flatBytes(vector, n);
        bytes += size;
        if (
          transfer?.detachableBuffers &&
          transfer.detachableBudget === this.budget &&
          ownsFlat(vector, n, transfer.detachableBuffers)
        ) {
          adopted.set(vector, {
            form: "flat",
            length: n,
            data: vector.data,
            validity: vector.validity,
          });
          adoptedBytes += size;
        }
      } else if (vector.form === "constant") bytes += flatBytes(vector.value, 1);
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
        if (
          !fallback.has(vector) &&
          transfer?.detachableBuffers &&
          transfer.detachableBudget === this.budget &&
          ownedView(vector.codes, 4 * n, transfer.detachableBuffers) &&
          ownedValidity(vector, n, transfer.detachableBuffers)
        ) {
          adoptedCodes.add(vector);
          adoptedBytes += 4 * n + (vector.validity === null ? 0 : 4 * Math.ceil(n / 32));
        }
      }
    }
    bytes += deltaBytes;
    const registryBytes = newIds.size * REGISTRY_ENTRY_BYTES;
    if (this.#sentBytes + deltaBytes + this.#registryBytes + registryBytes > this.dictionaryCap)
      throw new Error("Bench adapter stream dictionary cap exceeded");
    const copiedBytes = bytes - adoptedBytes;
    if (!this.budget.allocate(copiedBytes + registryBytes))
      throw new RangeError("Bench result budget refused frame");
    try {
      const dictionaries = [...newIds].map(([v, id]) => ({
        id,
        from: 0,
        values: ownFlat(v, v.length),
      }));
      const columns = batch.columns.map((vector): WireVector => {
        if (vector.form === "flat") return adopted.get(vector) ?? ownFlat(vector, n);
        if (vector.form === "constant")
          return { form: "constant", length: n, value: ownFlat(vector.value, 1) };
        if (fallback.has(vector)) return ownTextFlatten(vector, n, fallback.get(vector) ?? 0);
        const id = this.#ids.get(vector.dictionary) ?? newIds.get(vector.dictionary);
        if (id === undefined) throw new Error("Missing bench stream dictionary");
        return {
          form: "dictionary",
          length: n,
          dictionary: id,
          codes: adoptedCodes.has(vector) ? vector.codes : vector.codes.slice(0, n),
          validity: adoptedCodes.has(vector)
            ? vector.validity
            : (vector.validity?.slice(0, Math.ceil(n / 32)) ?? null),
        };
      });
      const payload = { rowCount: n, columns, dictionaries };
      if (frameByteLength(payload) !== bytes) throw new Error("Frame sizing mismatch");
      for (const [v, id] of newIds) this.#ids.set(v, id);
      this.#next += newIds.size;
      this.#sentBytes += deltaBytes;
      this.#registryBytes += registryBytes;
      this.#live++;
      this.#copied += copiedBytes;
      this.#liveCopied += copiedBytes;
      this.#adopted += adoptedBytes;
      this.#liveAdopted += adoptedBytes;
      if (adoptedBytes > 0) this.#adoptedFrames++;
      else transfer?.release();
      this.#flattened += fallback.size;
      return {
        payload,
        requiresLsn: 0,
        free: this.#releaseFrame(
          copiedBytes,
          adoptedBytes,
          adoptedBytes > 0 ? transfer : undefined,
        ),
      };
    } catch (error) {
      this.budget.free(copiedBytes + registryBytes);
      throw error;
    }
  }
  // A separate activation keeps the release handle from retaining the serializer's borrowed
  // dictionaries/fallback vectors through a shared closure context (the p2j lifetime rule).
  #releaseFrame(bytes: number, adoptedBytes = 0, transfer?: Transfer): () => void {
    let released = false;
    return () => {
      if (!released) {
        released = true;
        this.#live--;
        this.#liveCopied -= bytes;
        this.#liveAdopted -= adoptedBytes;
        this.budget.free(bytes);
        transfer?.release();
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
