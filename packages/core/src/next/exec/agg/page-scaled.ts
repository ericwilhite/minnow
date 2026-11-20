/** Prebuilt scaled-float SUM kernels.
 * Reconstruct each demanded value exactly as scaledInto, then consume it in input order.
 * Validated unpatched integers have magnitude <= 2^53 and exponent >= 0; a value this small
 * cannot overflow addition to a finite float8 (the overflow gap at MAX is >= 2^970).
 * Patched values retain the full float8pl finite-overflow rule. No reassociation.
 * A patched page creates one borrowed DataView, covered by the caller's admitted metadata
 * workspace; no payload buffer is created and no view survives this synchronous call.
 */
import type { PageRows } from "../../page/consume.js";
import type { PageView } from "../../page/decode.js";
import { NumericOutOfRangeError } from "../errors.js";

function firstPatch(patches: Uint32Array | null, row: number): number {
  if (patches === null) return 0;
  let low = 0,
    high = patches.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((patches[middle] ?? 0) < row) low = middle + 1;
    else high = middle;
  }
  return low;
}

export function sumScaledF640Page(
  sums: Float64Array,
  has: Uint8Array,
  view: PageView,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): void {
  if (from === to || view.allNull) return;
  const base = view.baseNumber,
    power = 10 ** view.aux,
    validity = view.validity,
    patches = view.patches,
    bytes = view.patchBytes,
    patchValues =
      patches !== null && patches.length > 0 && bytes !== null
        ? new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
        : null;
  let sum = sums[0] ?? 0,
    present = has[0] ?? 0;
  let patch = firstPatch(patches, rows === null ? from : (rows[from] ?? 0) - firstRow);
  if (rows === null) {
    for (let row = from; row < to; row++) {
      if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
      if (patches !== null) while (patch < patches.length && (patches[patch] ?? 0) < row) patch++;
      if (patches !== null && patchValues !== null && patches[patch] === row) {
        const value = patchValues.getFloat64(patch * 8, true),
          next = sum + value;
        if (
          (next === Infinity || next === -Infinity) &&
          sum !== Infinity &&
          sum !== -Infinity &&
          value !== Infinity &&
          value !== -Infinity
        ) {
          sums[0] = sum;
          has[0] = present;
          throw new NumericOutOfRangeError("value out of range: overflow");
        }
        sum = next;
      } else sum += base / power;
      present = 1;
    }
  } else {
    for (let k = from; k < to; k++) {
      const row = (rows[k] ?? 0) - firstRow;
      if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
      if (patches !== null) while (patch < patches.length && (patches[patch] ?? 0) < row) patch++;
      if (patches !== null && patchValues !== null && patches[patch] === row) {
        const value = patchValues.getFloat64(patch * 8, true),
          next = sum + value;
        if (
          (next === Infinity || next === -Infinity) &&
          sum !== Infinity &&
          sum !== -Infinity &&
          value !== Infinity &&
          value !== -Infinity
        ) {
          sums[0] = sum;
          has[0] = present;
          throw new NumericOutOfRangeError("value out of range: overflow");
        }
        sum = next;
      } else sum += base / power;
      present = 1;
    }
  }
  sums[0] = sum;
  has[0] = present;
}

export function sumScaledF648Page(
  sums: Float64Array,
  has: Uint8Array,
  view: PageView,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): void {
  if (from === to || view.allNull) return;
  const deltas = view.unsigned as Uint8Array;
  const base = view.baseNumber,
    power = 10 ** view.aux,
    validity = view.validity,
    patches = view.patches,
    bytes = view.patchBytes,
    patchValues =
      patches !== null && patches.length > 0 && bytes !== null
        ? new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
        : null;
  let sum = sums[0] ?? 0,
    present = has[0] ?? 0;
  let patch = firstPatch(patches, rows === null ? from : (rows[from] ?? 0) - firstRow);
  if (rows === null) {
    for (let row = from; row < to; row++) {
      if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
      if (patches !== null) while (patch < patches.length && (patches[patch] ?? 0) < row) patch++;
      if (patches !== null && patchValues !== null && patches[patch] === row) {
        const value = patchValues.getFloat64(patch * 8, true),
          next = sum + value;
        if (
          (next === Infinity || next === -Infinity) &&
          sum !== Infinity &&
          sum !== -Infinity &&
          value !== Infinity &&
          value !== -Infinity
        ) {
          sums[0] = sum;
          has[0] = present;
          throw new NumericOutOfRangeError("value out of range: overflow");
        }
        sum = next;
      } else sum += (base + (deltas[row] ?? 0)) / power;
      present = 1;
    }
  } else {
    for (let k = from; k < to; k++) {
      const row = (rows[k] ?? 0) - firstRow;
      if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
      if (patches !== null) while (patch < patches.length && (patches[patch] ?? 0) < row) patch++;
      if (patches !== null && patchValues !== null && patches[patch] === row) {
        const value = patchValues.getFloat64(patch * 8, true),
          next = sum + value;
        if (
          (next === Infinity || next === -Infinity) &&
          sum !== Infinity &&
          sum !== -Infinity &&
          value !== Infinity &&
          value !== -Infinity
        ) {
          sums[0] = sum;
          has[0] = present;
          throw new NumericOutOfRangeError("value out of range: overflow");
        }
        sum = next;
      } else sum += (base + (deltas[row] ?? 0)) / power;
      present = 1;
    }
  }
  sums[0] = sum;
  has[0] = present;
}

export function sumScaledF6416Page(
  sums: Float64Array,
  has: Uint8Array,
  view: PageView,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): void {
  if (from === to || view.allNull) return;
  const deltas = view.unsigned as Uint16Array;
  const base = view.baseNumber,
    power = 10 ** view.aux,
    validity = view.validity,
    patches = view.patches,
    bytes = view.patchBytes,
    patchValues =
      patches !== null && patches.length > 0 && bytes !== null
        ? new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
        : null;
  let sum = sums[0] ?? 0,
    present = has[0] ?? 0;
  let patch = firstPatch(patches, rows === null ? from : (rows[from] ?? 0) - firstRow);
  if (rows === null) {
    for (let row = from; row < to; row++) {
      if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
      if (patches !== null) while (patch < patches.length && (patches[patch] ?? 0) < row) patch++;
      if (patches !== null && patchValues !== null && patches[patch] === row) {
        const value = patchValues.getFloat64(patch * 8, true),
          next = sum + value;
        if (
          (next === Infinity || next === -Infinity) &&
          sum !== Infinity &&
          sum !== -Infinity &&
          value !== Infinity &&
          value !== -Infinity
        ) {
          sums[0] = sum;
          has[0] = present;
          throw new NumericOutOfRangeError("value out of range: overflow");
        }
        sum = next;
      } else sum += (base + (deltas[row] ?? 0)) / power;
      present = 1;
    }
  } else {
    for (let k = from; k < to; k++) {
      const row = (rows[k] ?? 0) - firstRow;
      if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
      if (patches !== null) while (patch < patches.length && (patches[patch] ?? 0) < row) patch++;
      if (patches !== null && patchValues !== null && patches[patch] === row) {
        const value = patchValues.getFloat64(patch * 8, true),
          next = sum + value;
        if (
          (next === Infinity || next === -Infinity) &&
          sum !== Infinity &&
          sum !== -Infinity &&
          value !== Infinity &&
          value !== -Infinity
        ) {
          sums[0] = sum;
          has[0] = present;
          throw new NumericOutOfRangeError("value out of range: overflow");
        }
        sum = next;
      } else sum += (base + (deltas[row] ?? 0)) / power;
      present = 1;
    }
  }
  sums[0] = sum;
  has[0] = present;
}

export function sumScaledF6432Page(
  sums: Float64Array,
  has: Uint8Array,
  view: PageView,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): void {
  if (from === to || view.allNull) return;
  const deltas = view.unsigned as Uint32Array;
  const base = view.baseNumber,
    power = 10 ** view.aux,
    validity = view.validity,
    patches = view.patches,
    bytes = view.patchBytes,
    patchValues =
      patches !== null && patches.length > 0 && bytes !== null
        ? new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
        : null;
  let sum = sums[0] ?? 0,
    present = has[0] ?? 0;
  let patch = firstPatch(patches, rows === null ? from : (rows[from] ?? 0) - firstRow);
  if (rows === null) {
    for (let row = from; row < to; row++) {
      if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
      if (patches !== null) while (patch < patches.length && (patches[patch] ?? 0) < row) patch++;
      if (patches !== null && patchValues !== null && patches[patch] === row) {
        const value = patchValues.getFloat64(patch * 8, true),
          next = sum + value;
        if (
          (next === Infinity || next === -Infinity) &&
          sum !== Infinity &&
          sum !== -Infinity &&
          value !== Infinity &&
          value !== -Infinity
        ) {
          sums[0] = sum;
          has[0] = present;
          throw new NumericOutOfRangeError("value out of range: overflow");
        }
        sum = next;
      } else sum += (base + (deltas[row] ?? 0)) / power;
      present = 1;
    }
  } else {
    for (let k = from; k < to; k++) {
      const row = (rows[k] ?? 0) - firstRow;
      if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
      if (patches !== null) while (patch < patches.length && (patches[patch] ?? 0) < row) patch++;
      if (patches !== null && patchValues !== null && patches[patch] === row) {
        const value = patchValues.getFloat64(patch * 8, true),
          next = sum + value;
        if (
          (next === Infinity || next === -Infinity) &&
          sum !== Infinity &&
          sum !== -Infinity &&
          value !== Infinity &&
          value !== -Infinity
        ) {
          sums[0] = sum;
          has[0] = present;
          throw new NumericOutOfRangeError("value out of range: overflow");
        }
        sum = next;
      } else sum += (base + (deltas[row] ?? 0)) / power;
      present = 1;
    }
  }
  sums[0] = sum;
  has[0] = present;
}
