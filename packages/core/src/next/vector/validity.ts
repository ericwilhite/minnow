/**
 * Validity bitmaps (bit set = valid) and selection vectors. Word-at-a-time where it matters;
 * nothing here allocates except the explicit `allocate*` functions.
 */
import type { Selection, Validity } from "./vector.js";

export function allocateValidity(capacity: number): Validity {
  return new Uint32Array((capacity + 31) >>> 5);
}

export function isValid(validity: Validity | null, row: number): boolean {
  return validity === null || ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;
}

export function setValid(validity: Validity, row: number, valid: boolean): void {
  const word = row >>> 5;
  const bit = 1 << (row & 31);
  validity[word] = valid ? (validity[word] ?? 0) | bit : (validity[word] ?? 0) & ~bit;
}

/** Marks rows 0 … length valid and clears the bits past it. */
export function setAllValid(validity: Validity, length: number): void {
  const full = length >>> 5;
  validity.fill(0xffff_ffff, 0, full);
  if (full < validity.length) {
    const rest = length & 31;
    validity[full] = rest === 0 ? 0 : (1 << rest) - 1;
    validity.fill(0, full + 1);
  }
}

function popcount(word: number): number {
  let value = word - ((word >>> 1) & 0x5555_5555);
  value = (value & 0x3333_3333) + ((value >>> 2) & 0x3333_3333);
  return (Math.imul((value + (value >>> 4)) & 0x0f0f_0f0f, 0x0101_0101) >>> 24) & 0xff;
}

/** Valid slots among rows 0 … length. */
export function countValid(validity: Validity | null, length: number): number {
  if (validity === null) return length;
  const full = length >>> 5;
  let count = 0;
  for (let word = 0; word < full; word++) count += popcount(validity[word] ?? 0);
  const rest = length & 31;
  if (rest !== 0) count += popcount((validity[full] ?? 0) & ((1 << rest) - 1));
  return count;
}

/**
 * `target = left AND right` over rows 0 … length (the validity of a strict binary kernel). A null
 * input means all valid. Returns null when both are null, so the common case allocates nothing.
 */
export function andValidity(
  left: Validity | null,
  right: Validity | null,
  target: Validity,
  length: number,
): Validity | null {
  const words = (length + 31) >>> 5;
  if (left === null) {
    if (right === null) return null;
    target.set(right.subarray(0, words));
  } else if (right === null) target.set(left.subarray(0, words));
  else
    for (let word = 0; word < words; word++) target[word] = (left[word] ?? 0) & (right[word] ?? 0);
  return target;
}

export function allocateSelection(capacity: number): Selection {
  return { rows: new Uint16Array(capacity), count: 0 };
}

/** The active row at position `index` of a batch's selection (or `index` itself without one). */
export function activeRow(selection: Selection | null, index: number): number {
  return selection === null ? index : (selection.rows[index] ?? 0);
}

/** Narrows `target` to the active rows of `selection` (or 0 … length) that are valid. */
export function selectValid(
  selection: Selection | null,
  length: number,
  validity: Validity | null,
  target: Selection,
): Selection {
  const count = selection === null ? length : selection.count;
  let kept = 0;
  for (let index = 0; index < count; index++) {
    const row = selection === null ? index : (selection.rows[index] ?? 0);
    if (validity === null || ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0)
      target.rows[kept++] = row;
  }
  target.count = kept;
  return target;
}
