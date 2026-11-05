/**
 * The group-scoped dictionary a writer builds for one varlen or uuid chunk: distinct entries in
 * first-seen order, looked up by an open-addressing hash of their bytes (`hashBytes`, the same
 * hash the distinct sketch uses). Entries are immutable once added; `truncate` drops the newest
 * ones when a page turns out cheaper plain (the writer then stops using the dictionary).
 *
 * Bounded: at most `maxEntries` entries and `maxBytes` entry bytes, and every growth is charged
 * first. A refused insert returns −1; the caller falls back to plain pages.
 */
import type { ByteCharge } from "../vector/varlen.js";
import { hashBytes } from "./sketch.js";

export const DEFAULT_DICTIONARY_ENTRIES = 65_536;
export const DEFAULT_DICTIONARY_BYTES = 256 * 1024;

export class DictionaryBuilder {
  readonly #fixed: boolean;
  readonly #charge: ByteCharge | null;
  readonly #maxEntries: number;
  readonly #maxBytes: number;
  bytes = new Uint8Array(0);
  /** varlen: entry i is `bytes[offsets[i] … offsets[i + 1])`. */
  offsets = new Int32Array(1);
  hashes = new Int32Array(0);
  #slots = new Int32Array(0);
  size = 0;
  byteLength = 0;
  #charged = 0;
  #frozen = false;

  constructor(
    fixed: boolean,
    charge: ByteCharge | null,
    maxEntries = DEFAULT_DICTIONARY_ENTRIES,
    maxBytes = DEFAULT_DICTIONARY_BYTES,
  ) {
    this.#fixed = fixed;
    this.#charge = charge;
    this.#maxEntries = maxEntries;
    this.#maxBytes = maxBytes;
  }

  get retainedBytes(): number {
    return this.#charged;
  }

  #grow(bytes: number): boolean {
    if (this.#charge !== null && !this.#charge.allocate(bytes)) return false;
    this.#charged += bytes;
    return true;
  }

  #ensure(entries: number, bytes: number): boolean {
    if (entries > this.#maxEntries || bytes > this.#maxBytes) return false;
    if (entries + 1 > this.hashes.length) {
      const capacity = Math.max(64, this.hashes.length * 2);
      const slots = capacity * 2;
      // Old and new arrays coexist while copying: charge the new ones in full first.
      const extra = capacity * 4 * (this.#fixed ? 1 : 2) + slots * 4;
      if (!this.#grow(extra)) return false;
      const hashes = new Int32Array(capacity);
      hashes.set(this.hashes.subarray(0, this.size));
      const old = this.hashes.length * 4 * (this.#fixed ? 1 : 2) + this.#slots.length * 4;
      this.hashes = hashes;
      if (!this.#fixed) {
        const offsets = new Int32Array(capacity + 1);
        offsets.set(this.offsets.subarray(0, this.size + 1));
        this.offsets = offsets;
      }
      this.#rehash(slots);
      this.#release(old);
    }
    if (bytes > this.bytes.length) {
      const size = Math.min(this.#maxBytes, Math.max(1024, this.bytes.length * 2, bytes));
      if (!this.#grow(size)) return false;
      const grown = new Uint8Array(size);
      grown.set(this.bytes.subarray(0, this.byteLength));
      this.#release(this.bytes.length);
      this.bytes = grown;
    }
    return true;
  }

  #release(bytes: number): void {
    this.#charged -= bytes;
    this.#charge?.free(bytes);
  }

  #rehash(slots: number): void {
    this.#slots = new Int32Array(slots);
    const mask = slots - 1;
    for (let entry = 0; entry < this.size; entry++) {
      let slot = (this.hashes[entry] ?? 0) & mask;
      while ((this.#slots[slot] ?? 0) !== 0) slot = (slot + 1) & mask;
      this.#slots[slot] = entry + 1;
    }
  }

  #equals(entry: number, source: Uint8Array, start: number, end: number): boolean {
    const entryStart = this.#fixed ? entry * 16 : (this.offsets[entry] ?? 0);
    const entryEnd = this.#fixed ? entryStart + 16 : (this.offsets[entry + 1] ?? 0);
    if (entryEnd - entryStart !== end - start) return false;
    for (let k = 0; k < end - start; k++)
      if (this.bytes[entryStart + k] !== source[start + k]) return false;
    return true;
  }

  /** The code of `source[start … end)`, inserting it if new; −1 when the dictionary is full. */
  codeOf(source: Uint8Array, start: number, end: number): number {
    if (this.#frozen) return -1;
    const hash = hashBytes(source, start, end);
    const slots = this.#slots;
    if (slots.length > 0) {
      const mask = slots.length - 1;
      let slot = hash & mask;
      for (;;) {
        const found = slots[slot] ?? 0;
        if (found === 0) break;
        if ((this.hashes[found - 1] ?? 0) === hash && this.#equals(found - 1, source, start, end))
          return found - 1;
        slot = (slot + 1) & mask;
      }
    }
    const length = end - start;
    if (!this.#ensure(this.size + 1, this.byteLength + length)) return -1;
    const entry = this.size;
    this.bytes.set(source.subarray(start, end), this.byteLength);
    this.byteLength += length;
    this.hashes[entry] = hash;
    if (!this.#fixed) this.offsets[entry + 1] = this.byteLength;
    this.size = entry + 1;
    const mask = this.#slots.length - 1;
    let slot = hash & mask;
    while ((this.#slots[slot] ?? 0) !== 0) slot = (slot + 1) & mask;
    this.#slots[slot] = entry + 1;
    return entry;
  }

  /** Drops entries ≥ `size` (and the lookup table: the dictionary takes no further inserts). */
  truncate(size: number): void {
    this.size = size;
    this.byteLength = this.#fixed ? size * 16 : (this.offsets[size] ?? 0);
    this.#release(this.#slots.length * 4);
    this.#slots = new Int32Array(0);
    this.#frozen = true;
  }

  /** Releases everything it holds. */
  close(): void {
    this.#release(this.#charged);
    this.bytes = new Uint8Array(0);
    this.hashes = new Int32Array(0);
    this.offsets = new Int32Array(1);
    this.#slots = new Int32Array(0);
  }
}
