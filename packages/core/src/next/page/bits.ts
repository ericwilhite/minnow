/**
 * Bit-run helpers over u32 words (bit `i % 32` of word `i >>> 5`), shared by validity sections
 * and bool pages. Word-at-a-time where the offsets allow; nothing allocates.
 */

/** Bits `[bit, bit + 32)` of `words` as one u32 (bits past the end read as 0). */
function read32(words: Uint32Array, bit: number): number {
  const word = bit >>> 5;
  const shift = bit & 31;
  const low = (words[word] ?? 0) >>> shift;
  return shift === 0 ? low : (low | ((words[word + 1] ?? 0) << (32 - shift))) >>> 0;
}

/** Writes the low `count` (1 … 32) bits of `value` at bit `bit` of `words`. */
function write(words: Uint32Array, bit: number, value: number, count: number): void {
  const word = bit >>> 5;
  const shift = bit & 31;
  const mask = count === 32 ? 0xffff_ffff : (1 << count) - 1;
  const bits = value & mask;
  words[word] = ((words[word] ?? 0) & ~(mask << shift)) | (bits << shift);
  if (shift + count > 32) {
    const rest = shift + count - 32;
    const high = (1 << rest) - 1;
    words[word + 1] = ((words[word + 1] ?? 0) & ~high) | (bits >>> (32 - shift));
  }
}

/** Copies bits `[from, from + count)` of `source` to `[at, at + count)` of `target`. */
export function copyBits(
  source: Uint32Array,
  from: number,
  target: Uint32Array,
  at: number,
  count: number,
): void {
  if (count <= 0) return;
  if ((from & 31) === 0 && (at & 31) === 0) {
    const words = count >>> 5;
    const sourceWord = from >>> 5;
    const targetWord = at >>> 5;
    if (words > 0) target.set(source.subarray(sourceWord, sourceWord + words), targetWord);
    const rest = count & 31;
    if (rest !== 0) write(target, at + words * 32, source[sourceWord + words] ?? 0, rest);
    return;
  }
  let done = 0;
  while (count - done >= 32) {
    write(target, at + done, read32(source, from + done), 32);
    done += 32;
  }
  if (done < count) write(target, at + done, read32(source, from + done), count - done);
}

/** Sets bits `[at, at + count)` of `target` to `value`. */
export function fillBits(target: Uint32Array, at: number, count: number, value: boolean): void {
  if (count <= 0) return;
  const pattern = value ? 0xffff_ffff : 0;
  let bit = at;
  const end = at + count;
  const head = Math.min(end, (bit + 31) & ~31);
  if (head > bit) {
    write(target, bit, pattern, head - bit);
    bit = head;
  }
  const fullEnd = end & ~31;
  if (fullEnd > bit) {
    target.fill(pattern, bit >>> 5, fullEnd >>> 5);
    bit = fullEnd;
  }
  if (end > bit) write(target, bit, pattern, end - bit);
}

/** `target[at + k] = source[rows[start + k] − base]` for k < end − start, as bits. */
export function gatherBits(
  source: Uint32Array,
  rows: Uint16Array | Uint32Array | Int32Array,
  start: number,
  end: number,
  base: number,
  target: Uint32Array,
  at: number,
): void {
  for (let k = start; k < end; k++) {
    const row = (rows[k] ?? 0) - base;
    const bit = ((source[row >>> 5] ?? 0) >>> (row & 31)) & 1;
    const out = at + k - start;
    const word = out >>> 5;
    const mask = 1 << (out & 31);
    target[word] = bit === 1 ? (target[word] ?? 0) | mask : (target[word] ?? 0) & ~mask;
  }
}

function popcount(word: number): number {
  let value = word - ((word >>> 1) & 0x5555_5555);
  value = (value & 0x3333_3333) + ((value >>> 2) & 0x3333_3333);
  return (Math.imul((value + (value >>> 4)) & 0x0f0f_0f0f, 0x0101_0101) >>> 24) & 0xff;
}

/** Set bits among `[0, count)`. */
export function countBits(words: Uint32Array, count: number): number {
  const full = count >>> 5;
  let total = 0;
  for (let word = 0; word < full; word++) total += popcount(words[word] ?? 0);
  const rest = count & 31;
  if (rest !== 0) total += popcount((words[full] ?? 0) & ((1 << rest) - 1));
  return total;
}

/** `target[at + k] = bit (from + k)` of `source`, as 0 or 1, for k < count. */
export function unpackBools(
  source: Uint32Array,
  from: number,
  count: number,
  target: Uint8Array,
  at: number,
): void {
  let k = 0;
  if ((from & 31) === 0) {
    let word = from >>> 5;
    for (; k + 32 <= count; k += 32) {
      const bits = source[word++] ?? 0;
      const out = at + k;
      for (let bit = 0; bit < 32; bit++) target[out + bit] = (bits >>> bit) & 1;
    }
  }
  for (; k < count; k++) {
    const bit = from + k;
    target[at + k] = ((source[bit >>> 5] ?? 0) >>> (bit & 31)) & 1;
  }
}
