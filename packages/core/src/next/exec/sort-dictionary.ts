/** Owned byte-exact union of genuinely encoded text dictionaries, never per-row strings. */
import type { PhysicalType } from "../types.js";
import { newDictionaryId, type DictionaryVector, type FlatVector } from "../vector/vector.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { Slice, SlicedJob } from "./slices.js";

const LIMIT = 32768,
  SLOTS = 65536;
const BASE = 256 + SLOTS * 4 + (LIMIT + 1) * 4 + 4096;
export class SortDictionary {
  readonly #id = newDictionaryId();
  #slots = new Int32Array(0);
  #offsets = new Int32Array(0);
  #bytes = new Uint8Array(0);
  #cache = new WeakMap<FlatVector, Int32Array>();
  #vector: FlatVector | null = null;
  #charged = 0;
  #size = 0;
  #used = 0;
  constructor(
    readonly type: PhysicalType,
    readonly charge: ByteCharge,
  ) {}
  init(): boolean {
    if (!this.charge.allocate(BASE)) return false;
    this.#charged = BASE;
    try {
      this.#slots = new Int32Array(SLOTS).fill(-1);
      this.#offsets = new Int32Array(LIMIT + 1);
      this.#bytes = new Uint8Array(4096);
      return true;
    } catch (error) {
      this.close();
      throw error;
    }
  }
  *translate(source: FlatVector, slice: Slice): SlicedJob<Int32Array | null> {
    const data = source.data;
    // Nullable entries or another representation keep the general sort's existing recipe.
    if (data.layout !== "varlen" || source.validity !== null) return null;
    const old = this.#cache.get(source);
    if (old && old.length >= source.length) return old;
    const bytes = source.length * 4 + 128;
    if (!this.charge.allocate(bytes)) return null;
    this.#charged += bytes;
    const codes = new Int32Array(source.length);
    for (let entry = 0; entry < source.length; entry++) {
      const from = data.offsets[entry] ?? 0,
        to = data.offsets[entry + 1] ?? 0;
      let hash = 2166136261;
      for (let at = from; at < to; at++) {
        hash = Math.imul(hash ^ (data.bytes[at] ?? 0), 16777619);
        if ((at & 1023) === 1023 && slice.due()) yield;
      }
      let slot = hash & (SLOTS - 1),
        code: number;
      for (;;) {
        code = this.#slots[slot] ?? -1;
        if (code < 0) {
          if (this.#size === LIMIT) return null;
          const length = to - from,
            needed = this.#used + length;
          if (needed > this.#bytes.length) {
            const capacity = Math.max(needed, this.#bytes.length * 2);
            if (!this.charge.allocate(capacity)) return null;
            this.#charged += capacity;
            const next = new Uint8Array(capacity);
            next.set(this.#bytes);
            const previous = this.#bytes.length;
            this.#bytes = next;
            this.charge.free(previous);
            this.#charged -= previous;
          }
          code = this.#size++;
          this.#slots[slot] = code;
          this.#bytes.set(data.bytes.subarray(from, to), this.#used);
          this.#used = needed;
          this.#offsets[this.#size] = needed;
          break;
        }
        const start = this.#offsets[code] ?? 0,
          end = this.#offsets[code + 1] ?? 0;
        let equal = end - start === to - from;
        for (let k = 0; equal && k < to - from; k++) {
          equal = this.#bytes[start + k] === data.bytes[from + k];
          if ((k & 1023) === 1023 && slice.due()) yield;
        }
        if (equal) break;
        slot = (slot + 1) & (SLOTS - 1);
        if (slice.tick()) yield;
      }
      codes[entry] = code;
      if ((entry & 127) === 127 && slice.due()) yield;
    }
    this.#cache.set(source, codes);
    // Old translation native arrays may still be collected later; keep their conservative
    // charge until close instead of pretending WeakMap collection is a memory free event.
    return codes;
  }
  vector(): FlatVector {
    this.#vector ??= {
      form: "flat",
      type: this.type,
      length: this.#size,
      validity: null,
      data: { layout: "varlen", offsets: this.#offsets, bytes: this.#bytes },
    };
    return this.#vector;
  }
  wrap(codes: FlatVector): DictionaryVector {
    if (codes.data.layout !== "i32") throw new TypeError("Missing dictionary codes");
    return {
      form: "dictionary",
      dictionaryId: this.#id,
      type: this.type,
      length: codes.length,
      validity: codes.validity,
      codes: codes.data.values,
      dictionary: this.vector(),
    };
  }
  close(): void {
    this.#slots = new Int32Array(0);
    this.#offsets = new Int32Array(0);
    this.#bytes = new Uint8Array(0);
    this.#cache = new WeakMap();
    this.#vector = null;
    this.#size = 0;
    this.#used = 0;
    if (this.#charged) this.charge.free(this.#charged);
    this.#charged = 0;
  }
}
