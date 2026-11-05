/**
 * Variable-width (UTF-8 / bytes) vector data: a reusable builder for kernels that produce
 * strings, byte comparison for equality and C-collation order, and the one place a stored value
 * becomes a JS string.
 *
 * Contract (DECISIONS.md C8): equality and hashing of text are on bytes, and the default order
 * is byte order (PostgreSQL's C collation, = code point order). An explicit ICU collation orders
 * by query-wide ranks computed with `Intl.Collator` (ties broken by bytes), or, when the distinct
 * values do not fit the grant, by an external comparison sort; neither derives sort-key bytes,
 * which JavaScript cannot. `decodeUtf8` runs only when a value leaves the engine, or on that
 * collation fallback.
 */
import { BATCH_TARGET_BYTES, type VectorData } from "./vector.js";

type VarlenData = Extract<VectorData, { layout: "varlen" }>;

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

/** The slice of a memory grant a builder needs (memory/governor.ts `Grant` satisfies it). */
export interface ByteCharge {
  tryGrow(bytes: number): boolean;
  release(bytes: number): void;
}

/**
 * - `ok`: appended.
 * - `full`: not appended; the batch already holds rows and this value would pass the byte target.
 *   Emit the batch, `reset`, and append again.
 * - `denied`: not appended; the grant refused the growth. Spill or wait for memory
 *   (exec/pipeline.ts `blocked`), then append again.
 */
export type AppendResult = "ok" | "full" | "denied";

/** UTF-8 length of a JS string without encoding it. */
export function utf8Length(value: string): number {
  let bytes = 0;
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code < 0x80) bytes += 1;
    else if (code < 0x800) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff && index + 1 < value.length) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        index += 1;
      } else bytes += 3;
    } else bytes += 3;
  }
  return bytes;
}

/**
 * Builds varlen data row by row into buffers reused across batches, under a memory grant.
 *
 * - Every byte it holds is charged: the initial arena and offsets at construction (part of the
 *   operator's admitted minimum), and each growth before it happens, counting the old and new
 *   arenas together while the copy runs (growth overlap).
 * - A batch stops at `targetBytes` (`full`), except that one value larger than the target may fill
 *   an empty batch on its own if the grant admits it.
 * - `reset` starts the next batch; if an oversized value grew the arena past the target, it shrinks
 *   back to the initial size and returns the difference to the grant.
 */
export class VarlenBuilder {
  readonly #charge: ByteCharge;
  readonly #targetBytes: number;
  readonly #initialBytes: number;
  #bytes: Uint8Array;
  readonly #offsets: Int32Array;
  #rows = 0;
  #used = 0;

  constructor(
    capacityRows: number,
    charge: ByteCharge,
    targetBytes = BATCH_TARGET_BYTES,
    initialBytes = 1024,
  ) {
    this.#charge = charge;
    this.#targetBytes = targetBytes;
    this.#initialBytes = Math.max(16, initialBytes);
    if (!charge.tryGrow(this.#initialBytes + (capacityRows + 1) * 4))
      throw new RangeError("A varlen builder's minimum must be admitted before it is built");
    this.#bytes = new Uint8Array(this.#initialBytes);
    this.#offsets = new Int32Array(capacityRows + 1);
  }

  get rows(): number {
    return this.#rows;
  }

  /** Bytes the builder holds, all of them charged to its grant. */
  get retainedBytes(): number {
    return this.#bytes.byteLength + this.#offsets.byteLength;
  }

  reset(): void {
    this.#rows = 0;
    this.#used = 0;
    if (this.#bytes.length > Math.max(this.#targetBytes, this.#initialBytes)) {
      this.#charge.release(this.#bytes.length - this.#initialBytes);
      this.#bytes = new Uint8Array(this.#initialBytes);
    }
  }

  #admit(extra: number): AppendResult {
    if (this.#rows + 1 >= this.#offsets.length) return "full";
    const needed = this.#used + extra;
    if (needed > this.#targetBytes && this.#rows > 0) return "full";
    if (needed <= this.#bytes.length) return "ok";
    let size = this.#bytes.length * 2;
    while (size < needed) size *= 2;
    if (this.#rows > 0 || needed <= this.#targetBytes)
      size = Math.min(size, Math.max(needed, this.#targetBytes));
    else size = needed;
    // Old and new arenas coexist during the copy: charge the new one in full first.
    if (!this.#charge.tryGrow(size)) return "denied";
    const grown = new Uint8Array(size);
    grown.set(this.#bytes.subarray(0, this.#used));
    this.#charge.release(this.#bytes.length);
    this.#bytes = grown;
    return "ok";
  }

  #close(): void {
    this.#rows += 1;
    this.#offsets[this.#rows] = this.#used;
  }

  appendBytes(source: Uint8Array, start = 0, end = source.length): AppendResult {
    const result = this.#admit(end - start);
    if (result !== "ok") return result;
    this.#bytes.set(source.subarray(start, end), this.#used);
    this.#used += end - start;
    this.#close();
    return "ok";
  }

  /** Encodes a JS string (input boundary only: parameters, literals, inserted rows). */
  appendString(value: string): AppendResult {
    const length = utf8Length(value);
    const result = this.#admit(length);
    if (result !== "ok") return result;
    const { written } = encoder.encodeInto(value, this.#bytes.subarray(this.#used));
    this.#used += written;
    this.#close();
    return "ok";
  }

  /** An empty slot (for a null row; the validity bitmap carries the null). */
  appendEmpty(): AppendResult {
    if (this.#rows + 1 >= this.#offsets.length) return "full";
    this.#close();
    return "ok";
  }

  /** The built data. Views alias the builder's buffers until the next `reset`. */
  data(): VarlenData {
    return {
      layout: "varlen",
      bytes: this.#bytes.subarray(0, this.#used),
      offsets: this.#offsets.subarray(0, this.#rows + 1),
    };
  }

  /** Returns everything to the grant. The builder is unusable afterwards. */
  close(): void {
    this.#charge.release(this.retainedBytes);
    this.#bytes = new Uint8Array(0);
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
