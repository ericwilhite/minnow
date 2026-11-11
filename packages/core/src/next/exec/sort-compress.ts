/** Exact numeric key compression: prove ordered suffixes, rank bounded tuples, pack bits. */
import type { ByteCharge } from "../vector/varlen.js";
import { mergeSortJob } from "./sort-keys.js";
import type { Slice, SlicedJob } from "./slices.js";
const LIMIT = 32768,
  SLOTS = 65536;
interface Field {
  readonly values: Uint32Array;
  readonly shift: number;
  readonly bits: number;
  readonly min: number;
  readonly digits: number;
}
export interface CompressionStats {
  bits: number;
  orderedSuffixKeys: number;
  rankedKeys: number;
  words: number;
  rankRefusals: number;
}
export class SortKeyCompression {
  readonly stats: CompressionStats = {
    bits: 0,
    orderedSuffixKeys: 0,
    rankedKeys: 0,
    words: 0,
    rankRefusals: 0,
  };
  #arrays: Array<Int32Array | Uint32Array> = [];
  #charged = 0;
  constructor(readonly charge: ByteCharge) {}
  #array(n: number, signed = false): Int32Array | Uint32Array | null {
    const bytes = n * 4 + 128;
    if (!this.charge.allocate(bytes)) return null;
    this.#charged += bytes;
    const value = signed ? new Int32Array(n) : new Uint32Array(n);
    this.#arrays.push(value);
    return value;
  }
  #rollback(count: number, charged: number): void {
    this.#arrays.length = count;
    this.charge.free(this.#charged - charged);
    this.#charged = charged;
  }
  *#ordered(
    words: readonly Uint32Array[],
    from: number,
    count: number,
    n: number,
    slice: Slice,
  ): SlicedJob<boolean> {
    for (let row = 1; row < n; row++) {
      for (let c = from; c < from + count; c++) {
        const a = words[c]?.[row - 1] ?? 0,
          b = words[c]?.[row] ?? 0;
        if (a > b) return false;
        if (a < b) break;
      }
      if ((row & 1023) === 1023 && slice.due()) yield;
    }
    return true;
  }
  *#rank(words: readonly Uint32Array[], n: number, slice: Slice): SlicedJob<Uint32Array | null> {
    const count = this.#arrays.length,
      charged = this.#charged;
    let keep = false;
    try {
      const slots = this.#array(SLOTS),
        representatives = this.#array(LIMIT),
        codes = this.#array(n);
      if (
        !(slots instanceof Uint32Array) ||
        !(representatives instanceof Uint32Array) ||
        !(codes instanceof Uint32Array)
      )
        return null;
      let size = 0;
      for (let row = 0; row < n; row++) {
        let hash = 2166136261;
        for (const column of words) hash = Math.imul(hash ^ (column[row] ?? 0), 16777619);
        // Mix high word bits into the table index; numeric mantissas often have constant low
        // bits. Complete hash collisions still require tuple equality, never hash equality.
        hash = Math.imul(hash ^ (hash >>> 16), 0x7feb352d);
        hash = Math.imul(hash ^ (hash >>> 15), 0x846ca68b);
        hash ^= hash >>> 16;
        let slot = hash & (SLOTS - 1),
          probes = 0,
          code: number;
        for (;;) {
          const found = slots[slot] ?? 0;
          if (found === 0) {
            if (size === LIMIT) return null;
            code = size++;
            representatives[code] = row;
            slots[slot] = code + 1;
            break;
          }
          code = found - 1;
          const previous = representatives[code] ?? 0;
          let equal = true;
          for (const column of words)
            if (column[row] !== column[previous]) {
              equal = false;
              break;
            }
          if (equal) break;
          // Adversarial collisions decline this optimization; the original words remain exact.
          if (++probes === 128) return null;
          slot = (slot + 1) & (SLOTS - 1);
          if (slice.tick()) yield;
        }
        codes[row] = code;
        if ((row & 1023) === 1023 && slice.due()) yield;
      }
      const order = this.#array(size, true),
        temporary = this.#array(size, true),
        ranks = this.#array(size);
      if (
        !(order instanceof Int32Array) ||
        !(temporary instanceof Int32Array) ||
        !(ranks instanceof Uint32Array)
      )
        return null;
      for (let i = 0; i < size; i++) {
        order[i] = i;
        if ((i & 1023) === 1023 && slice.due()) yield;
      }
      yield* mergeSortJob(
        order,
        temporary,
        (a, b) => {
          const x = representatives[a] ?? 0,
            y = representatives[b] ?? 0;
          for (const column of words) {
            const av = column[x] ?? 0,
              bv = column[y] ?? 0;
            if (av !== bv) return av < bv ? -1 : 1;
          }
          return 0;
        },
        slice,
      );
      for (let rank = 0; rank < size; rank++) {
        ranks[order[rank] ?? 0] = rank;
        if ((rank & 1023) === 1023 && slice.due()) yield;
      }
      for (let row = 0; row < n; row++) {
        codes[row] = ranks[codes[row] ?? 0] ?? 0;
        if ((row & 1023) === 1023 && slice.due()) yield;
      }
      keep = true;
      this.stats.rankedKeys++;
      return codes;
    } finally {
      if (!keep) {
        this.stats.rankRefusals++;
        this.#rollback(count, charged);
      }
    }
  }
  *#field(values: Uint32Array, n: number, slice: Slice): SlicedJob<Field | null> {
    const first = values[0] ?? 0;
    let mask = 0,
      min = first,
      max = first;
    for (let row = 1; row < n; row++) {
      const value = values[row] ?? 0;
      mask |= value ^ first;
      min = Math.min(min, value);
      max = Math.max(max, value);
      if ((row & 1023) === 1023 && slice.due()) yield;
    }
    if (mask === 0) return null;
    const shift = 31 - Math.clz32(mask & -mask),
      bits = 32 - Math.clz32((max - min) / 2 ** shift);
    return {
      values,
      shift,
      bits,
      min,
      digits: Number((mask & 65535) !== 0) + Number(mask >>> 16 !== 0),
    };
  }
  *pack(
    words: readonly Uint32Array[],
    groups: readonly number[],
    n: number,
    slice: Slice,
  ): SlicedJob<Uint32Array[] | null> {
    if (!this.charge.allocate(4096)) return null;
    this.#charged += 4096;
    let active = groups.length,
      total = words.length;
    while (active > 0) {
      const length = groups[active - 1] ?? 0;
      if (!(yield* this.#ordered(words, total - length, length, n, slice))) break;
      active--;
      total -= length;
      this.stats.orderedSuffixKeys++;
    }
    const fields: Field[] = [];
    let from = 0,
      originalDigits = 0;
    for (let key = 0; key < active; key++) {
      const length = groups[key] ?? 0,
        original: Field[] = [];
      for (let c = from; c < from + length; c++) {
        const column = words[c];
        if (!column) throw Error("Missing key word");
        const field = yield* this.#field(column, n, slice);
        if (field) original.push(field);
      }
      originalDigits += original.reduce((sum, field) => sum + field.digits, 0);
      const width = original.reduce((sum, field) => sum + field.bits, 0);
      const ranked =
        width > 16 ? yield* this.#rank(words.slice(from, from + length), n, slice) : null;
      if (ranked) {
        const field = yield* this.#field(ranked, n, slice);
        if (field) fields.push(field);
      } else fields.push(...original);
      from += length;
    }
    const bits = fields.reduce((sum, field) => sum + field.bits, 0),
      output: Uint32Array[] = [];
    // Do not add another native packing pass when no rank, suffix or word/digit reduction
    // is available. A declined compression preserves the original normalized columns.
    if (
      this.stats.rankedKeys === 0 &&
      this.stats.orderedSuffixKeys === 0 &&
      Math.ceil(bits / 32) >= words.length &&
      Math.ceil(bits / 16) >= originalDigits
    )
      return null;
    for (let w = 0; w < Math.ceil(bits / 32); w++) {
      const values = this.#array(n);
      if (!(values instanceof Uint32Array)) return null;
      output.push(values);
    }
    this.stats.bits = bits;
    this.stats.words = output.length;
    for (let row = 0; row < n; row++) {
      let at = 0,
        used = 0;
      for (const field of fields) {
        const value = ((field.values[row] ?? 0) - field.min) / 2 ** field.shift;
        let left = field.bits;
        while (left > 0) {
          const take = Math.min(left, 32 - used),
            part = (value >>> (left - take)) & (2 ** take - 1),
            column = output[at];
          if (!column) throw Error("Missing packed word");
          column[row] = take === 32 ? part >>> 0 : (((column[row] ?? 0) << take) | part) >>> 0;
          left -= take;
          used += take;
          if (used === 32) {
            at++;
            used = 0;
          }
        }
      }
      if (used) {
        const column = output[at];
        if (column) column[row] = ((column[row] ?? 0) << (32 - used)) >>> 0;
      }
      if ((row & 1023) === 1023 && slice.due()) yield;
    }
    return output;
  }
  close(): void {
    this.#arrays.length = 0;
    if (this.#charged) this.charge.free(this.#charged);
    this.#charged = 0;
  }
}
