/**
 * Variable-width (UTF-8 / bytes) vector data: a reusable builder for kernels that produce
 * strings, byte comparison for equality and C-collation order, and the one place a stored value
 * becomes a JS string.
 *
 * Contract: equality and hashing of text are on bytes. Ordering under a collation other than
 * C uses sort-key bytes derived once per value (the executor's job), never a collation-aware
 * comparator per comparison. `decodeUtf8` runs only when a value leaves the engine.
 */
import type { VectorData } from "./vector.js";

type VarlenData = Extract<VectorData, { layout: "varlen" }>;

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

/**
 * Builds varlen data row by row into buffers reused across batches. `reset` starts a new batch
 * without freeing; the byte arena grows geometrically and is never shrunk while the operator
 * lives, so steady state allocates nothing.
 */
export class VarlenBuilder {
  #bytes: Uint8Array;
  #offsets: Int32Array;
  #rows = 0;
  #used = 0;

  constructor(capacityRows: number, initialBytes = 1024) {
    this.#bytes = new Uint8Array(Math.max(16, initialBytes));
    this.#offsets = new Int32Array(capacityRows + 1);
  }

  get rows(): number {
    return this.#rows;
  }

  /** Bytes the builder holds (for memory accounting). */
  get retainedBytes(): number {
    return this.#bytes.byteLength + this.#offsets.byteLength;
  }

  reset(): void {
    this.#rows = 0;
    this.#used = 0;
  }

  #reserve(extra: number): void {
    const needed = this.#used + extra;
    if (needed <= this.#bytes.length) return;
    let size = this.#bytes.length * 2;
    while (size < needed) size *= 2;
    const grown = new Uint8Array(size);
    grown.set(this.#bytes.subarray(0, this.#used));
    this.#bytes = grown;
  }

  #close(): void {
    if (this.#rows + 1 >= this.#offsets.length) throw new RangeError("Varlen builder is full");
    this.#rows += 1;
    this.#offsets[this.#rows] = this.#used;
  }

  appendBytes(source: Uint8Array, start = 0, end = source.length): void {
    this.#reserve(end - start);
    this.#bytes.set(source.subarray(start, end), this.#used);
    this.#used += end - start;
    this.#close();
  }

  /** Encodes a JS string (input boundary only: parameters, literals, inserted rows). */
  appendString(value: string): void {
    // UTF-16 code units expand to at most 3 UTF-8 bytes each.
    this.#reserve(value.length * 3);
    const { written } = encoder.encodeInto(value, this.#bytes.subarray(this.#used));
    this.#used += written;
    this.#close();
  }

  /** An empty slot (for a null row; the validity bitmap carries the null). */
  appendEmpty(): void {
    this.#close();
  }

  /** The built data. Views alias the builder's buffers until the next `reset`. */
  data(): VarlenData {
    return {
      layout: "varlen",
      bytes: this.#bytes.subarray(0, this.#used),
      offsets: this.#offsets.subarray(0, this.#rows + 1),
    };
  }
}

export function valueLength(data: VarlenData, index: number): number {
  return (data.offsets[index + 1] ?? 0) - (data.offsets[index] ?? 0);
}

/** Byte equality of slot `i` of `left` and slot `j` of `right`. */
export function varlenEqual(left: VarlenData, i: number, right: VarlenData, j: number): boolean {
  const leftStart = left.offsets[i] ?? 0;
  const length = (left.offsets[i + 1] ?? 0) - leftStart;
  const rightStart = right.offsets[j] ?? 0;
  if ((right.offsets[j + 1] ?? 0) - rightStart !== length) return false;
  for (let k = 0; k < length; k++)
    if (left.bytes[leftStart + k] !== right.bytes[rightStart + k]) return false;
  return true;
}

/** Unsigned byte order (= code point order for UTF-8, PostgreSQL's C collation): −1, 0, 1. */
export function compareBytes(
  left: Uint8Array,
  leftStart: number,
  leftEnd: number,
  right: Uint8Array,
  rightStart: number,
  rightEnd: number,
): number {
  const length = Math.min(leftEnd - leftStart, rightEnd - rightStart);
  for (let k = 0; k < length; k++) {
    const difference = (left[leftStart + k] ?? 0) - (right[rightStart + k] ?? 0);
    if (difference !== 0) return difference < 0 ? -1 : 1;
  }
  const difference = leftEnd - leftStart - (rightEnd - rightStart);
  return difference === 0 ? 0 : difference < 0 ? -1 : 1;
}

/**
 * Slot `index` as a JS string — the output boundary only. Short ASCII takes a loop without the
 * decoder call; anything else goes through a fatal TextDecoder (stored text is valid UTF-8, so a
 * failure is corruption, not data).
 */
export function decodeUtf8(data: VarlenData, index: number): string {
  const start = data.offsets[index] ?? 0;
  const end = data.offsets[index + 1] ?? 0;
  if (end - start <= 32) {
    let ascii = true;
    for (let k = start; k < end; k++)
      if ((data.bytes[k] ?? 0) >= 0x80) {
        ascii = false;
        break;
      }
    if (ascii) {
      let text = "";
      for (let k = start; k < end; k++) text += String.fromCharCode(data.bytes[k] ?? 0);
      return text;
    }
  }
  return decoder.decode(data.bytes.subarray(start, end));
}
