/** One retained head per input, one reusable decoded block; never materialize a range. */
import { taskClock, yieldTask } from "../exec/pipeline.js";
import { type KeyRange, type MaybePromise } from "../access/key-index.js";
import type { SpaceReader } from "../access/space.js";
import type { RunRef } from "../format/manifest.js";
import type { ByteBudget } from "../access/space.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { compareBytes } from "../vector/varlen.js";
import { need } from "./bytes.js";
import type { KeyDelta } from "./key-hash.js";

export interface RangeInput {
  key(): Uint8Array;
  bytes(): Uint8Array;
  keyStart(): number;
  keyEnd(): number;
  rowId: number;
  tombstone: boolean;
  advance(): MaybePromise<boolean>;
  close(): void;
}
export { rangeSide } from "./range-bound.js";
import { rangeSide } from "./range-bound.js";
import { RangeDirectory } from "./range-directory.js";

/** Version-aware position: writes may split leaves or grow arenas while a snapshot is pinned. */
export class DeltaRangeInput implements RangeInput {
  #position = -1;
  #version = -1;
  #started = false;
  #slot = -1;
  #entry = -1;
  #closed = false;
  #walked = 0;
  rowId = 0;
  tombstone = false;
  readonly #release: () => void;
  constructor(
    readonly delta: KeyDelta,
    readonly bound: number,
    readonly range: KeyRange,
    readonly direction: number,
    readonly signal?: AbortSignal,
  ) {
    this.#release = delta.retain();
  }
  key(): Uint8Array {
    return this.delta.keyBytes(this.#slot);
  }
  bytes(): Uint8Array {
    return this.delta.keyArena();
  }
  keyStart(): number {
    return this.delta.keyStart(this.#slot);
  }
  keyEnd(): number {
    return this.delta.keyEnd(this.#slot);
  }
  advance(): MaybePromise<boolean> {
    if (this.#closed || this.signal?.aborted === true) return false;
    const d = this.delta,
      order = d.ordered();
    if (order === null) return false;
    if (!this.#started) {
      this.#position = order.first(this.direction);
      const boundary = this.direction > 0 ? this.range.lower : this.range.upper;
      if (boundary !== null) {
        // Seek by the range rule, including all composite keys sharing a bound prefix.
        const position = order.seek((e) => {
          const side = rangeSide(
            this.range,
            d.keyArena(),
            d.keyStart(d.keyOf(e)),
            d.keyEnd(d.keyOf(e)),
          );
          return this.direction > 0 ? (side < 0 ? -1 : 0) : side > 0 ? 0 : -1;
        });
        this.#position = this.direction > 0 ? position : order.step(position, -1);
      }
      this.#started = true;
    } else if (this.#version !== order.version) {
      const position = order.seek((e) => {
        const slot = d.keyOf(e),
          arena = d.keyArena();
        const cmp =
          compareBytes(
            arena,
            d.keyStart(slot),
            d.keyEnd(slot),
            arena,
            d.keyStart(this.#slot),
            d.keyEnd(this.#slot),
          ) ||
          d.rowId(e) - this.rowId ||
          Number(d.isTombstone(e)) - Number(this.tombstone) ||
          e - this.#entry;
        return this.direction > 0 && cmp === 0 ? -1 : cmp;
      });
      this.#position = this.direction > 0 ? position : order.step(position, -1);
    } else this.#position = order.step(this.#position, this.direction);
    this.#version = order.version;
    for (;;) {
      const entry = order.at(this.#position);
      if (entry < 0) return false;
      const slot = d.keyOf(entry),
        side = rangeSide(this.range, d.keyArena(), d.keyStart(slot), d.keyEnd(slot));
      if (side * this.direction > 0) return false;
      if (side === 0 && d.seq(entry) <= this.bound) {
        this.#entry = entry;
        this.#slot = slot;
        this.rowId = d.rowId(entry);
        this.tombstone = d.isTombstone(entry);
        return true;
      }
      this.#entry = entry;
      this.#slot = slot;
      this.rowId = d.rowId(entry);
      this.tombstone = d.isTombstone(entry);
      if ((++this.#walked & 127) === 0 && taskClock.due())
        return yieldTask().then(() => this.advance());
      this.#position = order.step(this.#position, this.direction);
    }
  }
  close(): void {
    this.#closed = true;
    this.#release();
  }
}

/** One decoded block and scalar navigation, independent of the number of blocks. */
export function rangeRunBytes(_ref: RunRef, _direction: number): number {
  void _ref;
  void _direction;
  return 4096 + 512;
}
export class RunRangeInput implements RangeInput {
  readonly #directory: RangeDirectory;
  pending = true;
  readonly #buffer = new Uint8Array(4096);
  #bytes = this.#buffer.subarray(0, 0);
  readonly #view = new DataView(this.#buffer.buffer);
  #walk: Generator<Promise<unknown> | true, void, unknown> | null = null;
  #block = 0;
  #ordinal = -1;
  #count = 0;
  #loadedBlock: number | null = null;
  #leadingSkip = 0;
  #fixedEntryBytes = 0;
  #closed = false;
  #stopped(): boolean {
    return this.#closed || this.signal.aborted;
  }
  #walked = 0;
  #keyStart = 0;
  #keyEnd = 0;
  rowId = 0;
  tombstone = false;
  constructor(
    readonly space: SpaceReader,
    readonly budget: ByteBudget,
    readonly ref: RunRef,
    readonly range: KeyRange,
    readonly direction: number,
    readonly limit: number,
    readonly signal: AbortSignal,
    /** A primary-key coverage witness (or a staged own-put run) proves distinct puts only. */
    readonly pure = false,
  ) {
    this.rowId = direction > 0 ? -Infinity : Infinity;
    this.#directory = new RangeDirectory(ref, space, budget, range, direction, signal);
  }
  open(): MaybePromise<void> {
    return this.#directory.open();
  }
  key(): Uint8Array {
    return this.#bytes.subarray(this.#keyStart, this.#keyEnd);
  }
  bytes(): Uint8Array {
    return this.pending ? (this.direction > 0 ? this.ref.minKey : this.ref.maxKey) : this.#bytes;
  }
  keyStart(): number {
    return this.pending ? 0 : this.#keyStart;
  }
  keyEnd(): number {
    return this.pending ? this.bytes().length : this.#keyEnd;
  }
  advance(): MaybePromise<boolean> {
    this.#walk ??= this.#scan();
    return this.#step();
  }
  readonly #step = (value?: unknown): MaybePromise<boolean> => {
    const next = need(this.#walk).next(value);
    if (next.done) return false;
    return next.value === true ? true : next.value.then(this.#step);
  };
  *#scan(): Generator<Promise<unknown> | true, void, unknown> {
    while (!this.#stopped()) {
      const ref = this.#directory.current;
      if (ref === null) return;
      if (this.#loadedBlock !== this.#block) {
        const cached = this.space.peek(ref);
        if (cached !== null) this.#loaded(cached);
        else {
          const lease = (yield this.space.read([ref], {
            budget: this.budget,
            once: true,
            reuseBuffers: true,
            signal: this.signal,
          })) as import("../access/space.js").ReadLease;
          try {
            if (!this.#stopped()) this.#loaded(need(lease.bytes[0]));
          } finally {
            lease.release();
          }
        }
        if (this.#stopped()) return;
        this.#loadedBlock = this.#block;
      }
      if (this.#ordinal < 0 || this.#ordinal >= this.#count) {
        let ready = this.#directory.step();
        if (ready instanceof Promise) ready = (yield ready) as boolean;
        if (!ready || this.#stopped()) return;
        this.#block += this.direction;
        continue;
      }
      if ((++this.#walked & 127) === 0 && taskClock.due()) yield yieldTask();
      if (this.#stopped()) return;
      const bytes = this.#bytes,
        view = this.#view;
      let at = view.getUint16(2 + this.#ordinal * 2, true),
        length = 0,
        scale = 1;
      if (at < 2 + this.#count * 2 || at >= bytes.length)
        throw new StoreCorruptionError("Invalid index entry offset");
      for (;;) {
        const b = bytes[at++] ?? 0;
        length += (b & 127) * scale;
        if (b < 128) break;
        scale *= 128;
        if (scale > 2 ** 28) throw new StoreCorruptionError("Invalid key length");
      }
      if (length > 2048 || at + length + 7 > bytes.length)
        throw new StoreCorruptionError("Truncated index entry");
      this.#keyStart = at;
      this.#keyEnd = at + length;
      this.rowId =
        view.getUint32(at + length, true) + view.getUint16(at + length + 4, true) * 2 ** 32;
      this.tombstone = ((bytes[at + length + 6] ?? 0) & 1) !== 0;
      const side = rangeSide(this.range, this.#bytes, this.#keyStart, this.#keyEnd);
      if (side * this.direction > 0) return;
      if (side === 0 && (this.tombstone || this.rowId < this.limit)) yield true;
      this.#ordinal += this.direction;
    }
    return;
  }
  /** Consume the current block without keys or locators. Caller proves the run is wholly in
   * range and strictly before every other input. RunEncoder cancels equal (key,row) pairs;
   * the fixed suffix still decides tombstones and staged snapshot visibility per entry.
   * Leave the generator at the last consumed ordinal; advance resumes at its successor.
   */
  skipBlock(max: number): number {
    const bytes = this.#bytes,
      view = this.#view;
    let count = 0,
      last = this.#ordinal,
      visible = -1;
    if (this.pure && this.limit === Infinity) {
      count = Math.min(max, this.direction > 0 ? this.#count - this.#ordinal : this.#ordinal + 1);
      last = this.#ordinal + (count - 1) * this.direction;
      visible = last;
    } else
      for (
        let ordinal = this.#ordinal;
        ordinal >= 0 && ordinal < this.#count && count < max;
        ordinal += this.direction
      ) {
        const start = view.getUint16(2 + ordinal * 2, true);
        const end =
          ordinal + 1 < this.#count ? view.getUint16(2 + (ordinal + 1) * 2, true) : bytes.length;
        if (start < 2 + this.#count * 2 || end > bytes.length || end - start < 8)
          throw new StoreCorruptionError("Invalid index entry bounds");
        last = ordinal;
        if (((bytes[end - 1] ?? 0) & 1) !== 0) continue;
        const row = view.getUint32(end - 7, true) + view.getUint16(end - 3, true) * 2 ** 32;
        if (row >= this.limit) continue;
        count++;
        visible = ordinal;
        this.rowId = row;
      }
    this.#ordinal = last;
    if (visible >= 0) {
      let at = view.getUint16(2 + visible * 2, true),
        length = 0,
        scale = 1;
      for (;;) {
        const b = bytes[at++] ?? 0;
        length += (b & 127) * scale;
        if (b < 128) break;
        scale *= 128;
        if (scale > 2 ** 28) throw new StoreCorruptionError("Invalid key length");
      }
      const end =
        visible + 1 < this.#count ? view.getUint16(2 + (visible + 1) * 2, true) : bytes.length;
      if (length > 2048 || at + length + 7 !== end)
        throw new StoreCorruptionError("Truncated index entry");
      this.#keyStart = at;
      this.#keyEnd = at + length;
      this.rowId = view.getUint32(end - 7, true) + view.getUint16(end - 3, true) * 2 ** 32;
      this.tombstone = false;
    }
    return count;
  }
  /** Initial seek over certified fixed-width put pages, before creating the entry generator.
   * Directory lengths are checksummed metadata; no skipped block or key is materialized.
   */
  skipPages(max: number, entryBytes: number): MaybePromise<number> {
    const work = this.#skipPages(max, entryBytes);
    const step = (value?: unknown): MaybePromise<number> => {
      const next = work.next(value);
      return next.done ? next.value : next.value.then(step);
    };
    return step();
  }
  *#skipPages(max: number, entryBytes: number): Generator<Promise<unknown>, number, unknown> {
    this.#fixedEntryBytes = entryBytes;
    let skipped = 0;
    while (!this.#stopped() && skipped < max) {
      const ref = this.#directory.current;
      if (ref === null) break;
      const count = (ref.length - 2) / entryBytes;
      if (!Number.isSafeInteger(count) || count < 1)
        throw new StoreCorruptionError("Invalid fixed-width index block length");
      const n = Math.min(count, max - skipped);
      skipped += n;
      if (n < count) {
        this.#leadingSkip = n;
        break;
      }
      let ready = this.#directory.step();
      if (ready instanceof Promise) ready = (yield ready) as boolean;
      if (!ready) break;
      this.#block += this.direction;
      if (taskClock.due()) yield yieldTask();
    }
    return skipped;
  }
  #loaded(bytes: Uint8Array): void {
    this.#buffer.set(bytes);
    this.#bytes = this.#buffer.subarray(0, bytes.length);
    this.#count = this.#view.getUint16(0, true);
    if (bytes.length < 2 || this.#count * 2 + 2 > bytes.length)
      throw new StoreCorruptionError("Invalid index block entry count");
    if (this.#fixedEntryBytes > 0 && this.#count !== (bytes.length - 2) / this.#fixedEntryBytes)
      throw new StoreCorruptionError("Invalid fixed-width index block count");
    this.#ordinal = (this.direction > 0 ? 0 : this.#count - 1) + this.#leadingSkip * this.direction;
    this.#leadingSkip = 0;
  }
  close(): void {
    this.#closed = true;
    this.#walk = null;
  }
}
