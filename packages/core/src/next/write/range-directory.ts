/**
 * Scalar positions in the existing variable-width run directory. Metadata is borrowed only
 * synchronously or copied into the cursor's one shared workspace before a task yield.
 * No per-block ref or fence arrays are built. Reverse traversal recovers each record's start
 * from its trailing length, checksum and the previous block's physical end (Bloom boundaries
 * are in the existing directory). Stored bytes are unchanged.
 */
import type { MaybePromise } from "../access/key-index.js";
import type { KeyRange } from "../access/key-index.js";
import type { SpaceReader } from "../access/space.js";
import type { BlobRef, RunRef } from "../format/manifest.js";
import type { Grant } from "../memory/governor.js";
import { taskClock, yieldTask } from "../exec/pipeline.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { ByteReader, need } from "./bytes.js";
import { rangeSide } from "./range-bound.js";

export function directoryBytes(ref: RunRef, direction: number): number {
  // Every Bloom-directory record has >=6 bytes; this bounds its small reverse-boundary array.
  return direction < 0 ? Math.ceil(ref.bloom.length / 6) * 8 : 0;
}
export class RangeDirectory {
  current: BlobRef | null = null;
  #count = 0;
  #block = 0;
  #header = 0;
  #start = 0;
  #end = 0;
  #partitionBlocks = 0;
  #partitions = 0;
  readonly #partitionEnds: Float64Array;
  constructor(
    readonly ref: RunRef,
    readonly space: SpaceReader,
    readonly budget: Grant,
    readonly range: KeyRange,
    readonly direction: number,
    readonly signal: AbortSignal,
    readonly scratch: Uint8Array,
  ) {
    this.#partitionEnds = new Float64Array(directoryBytes(ref, direction) / 8);
  }
  #check(): void {
    this.signal.throwIfAborted();
  }
  #read<T>(ref: BlobRef, work: (bytes: Uint8Array) => T): MaybePromise<T> {
    this.#check();
    const cached = this.space.peek(ref);
    if (cached !== null) return work(cached);
    // Directory blobs are reusable metadata, unlike the cursor's one-pass data blocks.
    return this.space.read([ref], { budget: this.budget, signal: this.signal }).then((lease) => {
      try {
        this.#check();
        return work(need(lease.bytes[0]));
      } finally {
        lease.release();
      }
    });
  }
  *#copy(ref: BlobRef): Generator<Promise<unknown>, Uint8Array, unknown> {
    const copied = this.#read(ref, (bytes) => {
      this.scratch.set(bytes);
    });
    if (copied instanceof Promise) yield copied;
    this.#check();
    return this.scratch.subarray(0, ref.length);
  }
  open(): MaybePromise<void> {
    return run(this.#open());
  }
  *#open(): Generator<Promise<unknown>, void, unknown> {
    const initial = this.#read(this.ref.entries, (bytes) => {
      const r = new ByteReader(bytes);
      this.#count = r.varuint();
      this.#header = r.offset;
      if (this.#count === 0 && !r.done)
        throw new StoreCorruptionError("An empty run index has trailing bytes");
      if (this.direction > 0 && this.range.lower === null && this.#count > 0)
        this.#forward(bytes, this.#header);
    });
    if (initial instanceof Promise) yield initial;
    if (this.#count === 0) return;
    if (this.direction > 0 && this.range.lower === null) return;
    this.#block = this.direction > 0 ? 0 : this.#count - 1;
    const bound = this.direction > 0 ? this.range.lower : this.range.upper;
    if (bound !== null) {
      const bytes = yield* this.#copy(this.ref.fences),
        f = new ByteReader(bytes);
      if (f.varuint() !== this.#count) throw new StoreCorruptionError("Run fence count differs");
      let candidate = -1;
      for (let i = 0; i < this.#count; i++) {
        if ((i & 127) === 0 && taskClock.due()) yield yieldTask();
        this.#check();
        const length = f.varuint(),
          start = f.offset;
        if (start + length > bytes.length) throw new StoreCorruptionError("Truncated run fence");
        f.offset += length;
        const side = rangeSide(this.range, bytes, start, f.offset);
        if (this.direction > 0 ? side < 0 : side <= 0) candidate = i;
        else break;
      }
      this.#block = this.direction > 0 ? Math.max(0, candidate) : candidate;
      if (this.#block < 0) return;
    }
    if (this.direction < 0) {
      const bytes = yield* this.#copy(this.ref.bloom);
      const r = new ByteReader(bytes);
      this.#partitionBlocks = r.varuint();
      this.#partitions = r.varuint();
      if (
        this.#partitionBlocks === 0 ||
        this.#partitions !== Math.ceil(this.#count / this.#partitionBlocks) ||
        this.#partitions > this.#partitionEnds.length
      )
        throw new StoreCorruptionError("Run Bloom directory does not cover its blocks");
      for (let i = 0; i < this.#partitions; i++) {
        if ((i & 127) === 0 && taskClock.due()) yield yieldTask();
        this.#check();
        this.#partitionEnds[i] = r.varuint();
        r.varuint();
        r.u32();
      }
      r.end();
    }
    if (this.direction < 0 && bound === null) {
      const last = this.#read(this.ref.entries, (bytes) =>
        this.#backward(bytes, bytes.length, this.#partitionEnds[this.#partitions - 1] ?? 0),
      );
      if (last instanceof Promise) yield last;
      return;
    }
    const bytes = yield* this.#copy(this.ref.entries),
      r = new ByteReader(bytes, this.#header);
    for (let i = 0; i < this.#block; i++) {
      if ((i & 127) === 0 && taskClock.due()) yield yieldTask();
      this.#check();
      r.varuint();
      r.varuint();
      r.u32();
    }
    this.#forward(bytes, r.offset);
  }
  #forward(bytes: Uint8Array, at: number): void {
    if (at < this.#header) throw new StoreCorruptionError("Invalid run directory offset");
    const r = new ByteReader(bytes, at),
      offset = r.varuint(),
      length = r.varuint(),
      checksum = r.u32();
    if (length < 2 || length > 4096 || offset + length > this.ref.entries.offset)
      throw new StoreCorruptionError("Invalid run block bounds");
    this.#start = at;
    this.#end = r.offset;
    this.current = { extent: this.ref.entries.extent, offset, length, checksum };
  }
  #backward(bytes: Uint8Array, end: number, blockEnd: number): void {
    let start = end - 5;
    if (start < this.#header) throw new StoreCorruptionError("Truncated reverse run directory");
    // The preceding offset's final varuint byte has its continuation bit clear.
    while (start > this.#header && (bytes[start - 1] ?? 0) >= 128) start--;
    const r = new ByteReader(bytes, start),
      length = r.varuint(),
      offset = blockEnd - length;
    if (r.offset !== end - 4 || offset < 0)
      throw new StoreCorruptionError("Invalid reverse run block");
    let width = 1;
    for (let n = offset; n >= 128; n = Math.floor(n / 128)) width++;
    this.#forward(bytes, start - width);
    if (this.#end !== end || this.current?.offset !== offset || this.current.length !== length)
      throw new StoreCorruptionError("Reverse run directory disagrees with its physical boundary");
  }
  step(): MaybePromise<boolean> {
    this.#check();
    const previous = this.current,
      start = this.#start,
      end = this.#end;
    this.#block += this.direction;
    if (previous === null || this.#block < 0 || this.#block >= this.#count) {
      if (
        previous !== null &&
        (this.direction > 0 ? this.#end !== this.ref.entries.length : this.#start !== this.#header)
      )
        throw new StoreCorruptionError("A run index has trailing records");
      this.current = null;
      return false;
    }
    return this.#read(this.ref.entries, (bytes) => {
      if (this.direction > 0) this.#forward(bytes, end);
      else {
        const boundary =
          (this.#block + 1) % this.#partitionBlocks === 0
            ? (this.#partitionEnds[Math.floor(this.#block / this.#partitionBlocks)] ?? 0)
            : previous.offset;
        this.#backward(bytes, start, boundary);
      }
      return true;
    });
  }
}
function run<T>(work: Generator<Promise<unknown>, T, unknown>): MaybePromise<T> {
  const step = (value?: unknown): MaybePromise<T> => {
    const next = work.next(value);
    return next.done ? next.value : next.value.then(step);
  };
  return step();
}
