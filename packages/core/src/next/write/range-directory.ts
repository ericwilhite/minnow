/** Scalar cursor positions over shared, governed navigation indexes. Stored bytes are unchanged. */
import { OverflowKeys } from "./overflow-keys.js";
import { ByteReader as KeyReader } from "../format/bytes.js";
import { readKeyPointer } from "../format/key-pointer.js";
import type { MaybePromise, KeyRange } from "../access/key-index.js";
import type { SpaceReader } from "../access/space.js";
import type { BlobRef, RunRef } from "../format/manifest.js";
import type { ByteBudget } from "../access/space.js";
import { taskClock, yieldTask } from "../exec/pipeline.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { ByteReader, need } from "./bytes.js";
import { rangeSide } from "./range-bound.js";
import {
  runNavigation,
  skipDirectoryRecord,
  withDirectory,
  withRawDirectory,
} from "./range-navigation.js";

/** There are no cursor-owned arrays proportional to directory size, in either direction. */
export function directoryBytes(_ref: RunRef, _direction: number): number {
  void _ref;
  void _direction;
  return 0;
}
export class RangeDirectory {
  current: BlobRef | null = null;
  #count = 0;
  #block = 0;
  #end = 0;
  constructor(
    readonly ref: RunRef,
    readonly space: SpaceReader,
    readonly budget: ByteBudget,
    readonly range: KeyRange,
    readonly direction: number,
    readonly signal: AbortSignal,
    readonly keys: OverflowKeys | null = null,
  ) {}
  open(): MaybePromise<void> {
    if (this.keys !== null) return this.#openOverflow();
    return runNavigation(this.#open());
  }
  async #openOverflow(): Promise<void> {
    const keys = need(this.keys);
    const count = await withRawDirectory(
      this.space,
      this.ref.entries,
      this.budget,
      this.signal,
      (bytes) => new ByteReader(bytes).varuint(),
    );
    this.#count = count;
    if (count === 0) return;
    const bound = this.direction > 0 ? this.range.lower : this.range.upper;
    let low = 0,
      high = count;
    if (bound !== null) {
      while (low < high) {
        const mid = Math.floor((low + high) / 2);
        // Copy only one bounded descriptor while the fence lease is held. Resolve its
        // payload AFTER withDirectory returns and releases that lease.
        const pointer = await withDirectory(
          this.space,
          this.ref.fences,
          "overflow-fences",
          this.budget,
          this.signal,
          function* (
            this: RangeDirectory,
            bytes: Uint8Array,
            positions: Uint32Array | null,
            actual: number,
            start: number,
          ) {
            if (actual !== count) throw new StoreCorruptionError("Run fence count differs");
            const r = new ByteReader(bytes, positions === null ? start : positions[mid]);
            if (positions === null)
              for (let k = 0; k < mid; k++) {
                if ((k & 127) === 0) {
                  this.signal.throwIfAborted();
                  if (taskClock.due()) yield yieldTask();
                }
                skipDirectoryRecord(r, "overflow-fences");
              }
            return readKeyPointer(new KeyReader(bytes.subarray(r.offset)), this.ref.fences.offset);
          }.bind(this),
        );
        const full = await keys.resolve(pointer);
        const side = rangeSide(this.range, full);
        if (this.direction > 0 ? side < 0 : side <= 0) low = mid + 1;
        else high = mid;
      }
      this.#block = this.direction > 0 ? Math.max(0, low - 1) : low - 1;
      if (this.#block < 0) return;
    } else this.#block = this.direction > 0 ? 0 : count - 1;
    await this.#at(this.#block, true);
  }
  *#open(): Generator<Promise<unknown>, void, unknown> {
    this.signal.throwIfAborted();
    if (this.direction > 0 && this.range.lower === null) {
      const initial = withRawDirectory(
        this.space,
        this.ref.entries,
        this.budget,
        this.signal,
        (bytes) => {
          const r = new ByteReader(bytes);
          this.#count = r.varuint();
          if (this.#count === 0) r.end();
          else this.#entry(bytes, r.offset);
        },
      );
      if (initial instanceof Promise) yield initial;
      return;
    }
    const bound = this.direction > 0 ? this.range.lower : this.range.upper;
    if (bound !== null) {
      const found = withDirectory(
        this.space,
        this.ref.fences,
        "fences",
        this.budget,
        this.signal,
        function* (
          this: RangeDirectory,
          bytes: Uint8Array,
          positions: Uint32Array | null,
          count: number,
          start: number,
        ) {
          this.#count = count;
          let candidate = -1;
          if (positions !== null) {
            let low = 0,
              high = count;
            while (low < high) {
              const mid = (low + high) >>> 1,
                r = new ByteReader(bytes, positions[mid]),
                length = r.varuint();
              const side = rangeSide(this.range, bytes, r.offset, r.offset + length);
              if (this.direction > 0 ? side < 0 : side <= 0) low = mid + 1;
              else high = mid;
            }
            candidate = low - 1;
          } else {
            const r = new ByteReader(bytes, start);
            for (let block = 0; block < count; block++) {
              if ((block & 127) === 0) {
                this.signal.throwIfAborted();
                if (taskClock.due()) yield yieldTask();
                this.signal.throwIfAborted();
              }
              const length = r.varuint(),
                first = r.offset;
              if (length > 2048 || first + length > bytes.length)
                throw new StoreCorruptionError("Truncated run fence");
              r.offset += length;
              const side = rangeSide(this.range, bytes, first, r.offset);
              if (this.direction > 0 ? side < 0 : side <= 0) candidate = block;
              else break;
            }
          }
          return this.direction > 0 ? Math.max(0, candidate) : candidate;
        }.bind(this),
      );
      this.#block = found instanceof Promise ? ((yield found) as number) : found;
      if (this.#block < 0) return;
    } else this.#block = -1; // the last block, obtained from the index's count
    const ready = this.#at(this.#block, bound !== null);
    if (ready instanceof Promise) yield ready;
    this.signal.throwIfAborted();
  }
  #entry(bytes: Uint8Array, at: number): void {
    const r = new ByteReader(bytes, at),
      offset = r.varuint(),
      length = r.varuint(),
      checksum = r.u32();
    if (length < 2 || length > 4096 || offset + length > this.ref.entries.offset)
      throw new StoreCorruptionError("Invalid run block bounds");
    this.#end = r.offset;
    if (this.#block === this.#count - 1 && this.#end !== bytes.length)
      throw new StoreCorruptionError("A run index has trailing records");
    this.current = { extent: this.ref.entries.extent, offset, length, checksum };
  }
  #at(block: number, hasFenceCount = false): MaybePromise<void> {
    return withDirectory(
      this.space,
      this.ref.entries,
      "entries",
      this.budget,
      this.signal,
      function* (
        this: RangeDirectory,
        bytes: Uint8Array,
        positions: Uint32Array | null,
        count: number,
        start: number,
      ) {
        if (hasFenceCount && this.#count !== count)
          throw new StoreCorruptionError("Run fence count differs");
        this.#count = count;
        if (count === 0) {
          this.current = null;
          return;
        }
        if (block < 0) this.#block = count - 1;
        const wanted = this.#block;
        if (wanted >= count) throw new StoreCorruptionError("Run block outside its directory");
        if (positions !== null) {
          this.#entry(bytes, positions[wanted] ?? start);
          return;
        }
        const r = new ByteReader(bytes, start);
        for (let k = 0; k < wanted; k++) {
          if ((k & 127) === 0) {
            this.signal.throwIfAborted();
            if (taskClock.due()) yield yieldTask();
            this.signal.throwIfAborted();
          }
          skipDirectoryRecord(r, "entries");
        }
        this.#entry(bytes, r.offset);
      }.bind(this),
    );
  }
  step(): MaybePromise<boolean> {
    this.signal.throwIfAborted();
    this.#block += this.direction;
    if (this.current === null || this.#block < 0 || this.#block >= this.#count) {
      if (this.current !== null && this.direction > 0 && this.#end !== this.ref.entries.length)
        throw new StoreCorruptionError("A run index has trailing records");
      this.current = null;
      return false;
    }
    const next =
      this.direction > 0
        ? withRawDirectory(this.space, this.ref.entries, this.budget, this.signal, (bytes) =>
            this.#entry(bytes, this.#end),
          )
        : this.#at(this.#block);
    return next instanceof Promise ? next.then(() => true) : true;
  }
}
