/** Lossless byte classes for one short prefix plus a canonical unsigned decimal
 * suffix. Choose on the first demanded non-NULL key, never on a statistics hint.
 * Sparse suffixes allocate only their 4096-bit pages, not the intervening range.
 * Every other byte string stays in the disjoint ordinary exact byte set.
 */
import { OBJECT_OVERHEAD_BYTES } from "../../memory/governor.js";
import type { FlatVector } from "../../vector/vector.js";
import type { PhysicalType } from "../../types.js";
import { Charged } from "./groups.js";
const LIMIT = 1 << 20,
  DIRECTORY = LIMIT / 4096,
  PAGE_WORDS = 128,
  PREFIX_LIMIT = 16;
const EMPTY_PAGES: Array<Uint32Array | undefined> = [];
export class DecimalSuffixBitmap {
  #mode: "unknown" | "bytes" | "bitmap";
  #prefix: Uint8Array | null = null;
  #view: DataView | null = null;
  #pages: Array<Uint32Array | undefined> = EMPTY_PAGES;
  count = 0;
  pages = 0;
  constructor(
    readonly memory: Charged,
    enabled: boolean,
  ) {
    // The caller prepaid this helper object before construction.
    this.#mode = enabled ? "unknown" : "bytes";
  }
  get active(): boolean {
    return this.#mode !== "bytes";
  }
  /** One demanded row can create at most one page. The complete directory has
   * only 256 pages (128KiB of bits), so this constant-work bound needs no second
   * pass over input bytes. Unknown/generic keys may consume none of the credit.
   */
  growthBytes(rows: number): number {
    if (this.#mode === "bytes" || rows === 0) return 0;
    return (
      Math.min(rows, DIRECTORY - this.pages) * (4 * PAGE_WORDS + 2 * OBJECT_OVERHEAD_BYTES) +
      (this.#mode === "unknown" ? 4 * OBJECT_OVERHEAD_BYTES + 8 * DIRECTORY + PREFIX_LIMIT : 0)
    );
  }
  /** Null means this key belongs to the ordinary byte set. */
  insert(source: DataView, from: number, to: number): boolean | null {
    if (this.#mode === "bytes") return null;
    if (this.#mode === "unknown") {
      const key = this.#candidate(source, from, to);
      if (key < 0) {
        this.#mode = "bytes";
        return null;
      }
      let suffix = to;
      while (suffix > from) {
        const b = source.getUint8(suffix - 1);
        if (b < 48 || b > 57) break;
        suffix--;
      }
      const bytes = 4 * OBJECT_OVERHEAD_BYTES + 8 * DIRECTORY + suffix - from;
      this.memory.take(bytes);
      try {
        const prefix = new Uint8Array(suffix - from);
        for (let i = 0; i < prefix.length; i++) prefix[i] = source.getUint8(from + i);
        const view = new DataView(prefix.buffer),
          pages = new Array<Uint32Array | undefined>(DIRECTORY).fill(undefined);
        this.#prefix = prefix;
        this.#view = view;
        this.#pages = pages;
        this.#mode = "bitmap";
      } catch (error) {
        this.memory.give(bytes);
        throw error;
      }
    }
    const key = this.#number(source, from, to);
    if (key < 0) return null;
    const at = key >>> 12,
      word = (key & 4095) >>> 5,
      bit = 1 << (key & 31);
    let page = this.#pages[at];
    if (page === undefined) {
      this.memory.take(2 * OBJECT_OVERHEAD_BYTES);
      try {
        page = this.memory.make(Uint32Array, PAGE_WORDS);
      } catch (error) {
        this.memory.give(2 * OBJECT_OVERHEAD_BYTES);
        throw error;
      }
      this.#pages[at] = page;
      this.pages++;
    }
    if (((page[word] ?? 0) & bit) !== 0) return false;
    page[word] = (page[word] ?? 0) | bit;
    this.count++;
    return true;
  }
  #candidate(source: DataView, from: number, to: number): number {
    let start = to,
      value = 0,
      multiple = 1;
    while (start > from && to - start < 8) {
      const b = source.getUint8(start - 1);
      if (b < 48 || b > 57) break;
      start--;
      value += (b - 48) * multiple;
      multiple *= 10;
    }
    if (
      start === to ||
      start - from > PREFIX_LIMIT ||
      value >= LIMIT ||
      (to - start > 1 && source.getUint8(start) === 48)
    )
      return -1;
    if (start > from) {
      const b = source.getUint8(start - 1);
      if (b >= 48 && b <= 57) return -1;
    }
    return value;
  }
  #number(source: DataView, from: number, to: number): number {
    const prefix = this.#prefix,
      own = this.#view;
    if (prefix === null || own === null) return -1;
    const digits = to - from - prefix.length;
    if (digits < 1 || digits > 8) return -1;
    let i = 0;
    for (; i + 4 <= prefix.length; i += 4)
      if (source.getUint32(from + i, true) !== own.getUint32(i, true)) return -1;
    for (; i < prefix.length; i++) if (source.getUint8(from + i) !== prefix[i]) return -1;
    from += prefix.length;
    if (digits > 1 && source.getUint8(from) === 48) return -1;
    let value = 0;
    for (; from < to; from++) {
      const b = source.getUint8(from);
      if (b < 48 || b > 57) return -1;
      value = 10 * value + b - 48;
    }
    return value < LIMIT ? value : -1;
  }
  *members(): Generator<{ group: number; id: number } | undefined, void, void> {
    let count = 0;
    for (let p = 0; p < this.#pages.length; p++) {
      const page = this.#pages[p];
      if (page === undefined) continue;
      for (let w = 0; w < page.length; w++) {
        let bits = (page[w] ?? 0) >>> 0;
        while (bits !== 0) {
          const bit = 31 - Math.clz32(bits & -bits),
            value = p * 4096 + w * 32 + bit;
          yield { group: 0, id: -value - 1 };
          if ((++count & 511) === 0) yield;
          bits = (bits & (bits - 1)) >>> 0;
        }
      }
    }
  }
  memberBytes(id: number): number {
    let digits = 1;
    for (let value = -id - 1; value >= 10; value = Math.floor(value / 10)) digits++;
    return 8 + (this.#prefix?.length ?? 0) + digits;
  }
  /** The caller funds this exact one-key value through StateRow before construction. */
  memberValue(id: number, type: PhysicalType): FlatVector {
    const prefix = this.#prefix;
    if (prefix === null) throw new Error("Missing DISTINCT prefix");
    const bytes = new Uint8Array(this.memberBytes(id) - 8);
    bytes.set(prefix);
    let value = -id - 1,
      at = bytes.length;
    do {
      const next = Math.floor(value / 10);
      bytes[--at] = 48 + value - 10 * next;
      value = next;
    } while (value !== 0);
    return {
      form: "flat",
      type,
      length: 1,
      validity: null,
      data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
    };
  }
  close(): void {
    this.#prefix = null;
    this.#view = null;
    this.#pages = EMPTY_PAGES;
  }
}
