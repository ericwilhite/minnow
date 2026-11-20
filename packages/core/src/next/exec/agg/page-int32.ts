/** Prebuilt typed int32 page sums. Callers retain BATCH_ROWS (2048) transition steps;
 * each partial has magnitude at most 2^42 and is exact, including duplicate selections.
 * Dispatch is outside the row loop. The caller owns validity, selected positions and state.
 */
import { DELTA_BLOCK } from "../../page/format.js";
import type { PageRows } from "../../page/consume.js";
import type { PageView } from "../../page/decode.js";

export function sumI32PlainPage(
  view: PageView,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): number {
  const values = view.plain as Int32Array,
    validity = view.validity;
  let total = 0;
  if (rows === null) {
    if (validity === null) {
      for (let row = from; row < to; row++) total += values[row] ?? 0;
    } else {
      for (let row = from; row < to; row++)
        if (((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0) total += values[row] ?? 0;
    }
  } else {
    if (validity === null) {
      for (let k = from; k < to; k++) {
        const row = (rows[k] ?? 0) - firstRow;
        total += values[row] ?? 0;
      }
    } else {
      for (let k = from; k < to; k++) {
        const row = (rows[k] ?? 0) - firstRow;
        if (((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0) total += values[row] ?? 0;
      }
    }
  }
  return total;
}

export function sumI328Page(
  view: PageView,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): number {
  const values = view.unsigned as Uint8Array,
    validity = view.validity,
    base = view.baseLow;
  let total = 0;
  if (rows === null) {
    if (validity === null) {
      for (let row = from; row < to; row++) total += (base + (values[row] ?? 0)) | 0;
    } else {
      for (let row = from; row < to; row++)
        if (((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0)
          total += (base + (values[row] ?? 0)) | 0;
    }
  } else {
    if (validity === null) {
      for (let k = from; k < to; k++) {
        const row = (rows[k] ?? 0) - firstRow;
        total += (base + (values[row] ?? 0)) | 0;
      }
    } else {
      for (let k = from; k < to; k++) {
        const row = (rows[k] ?? 0) - firstRow;
        if (((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0)
          total += (base + (values[row] ?? 0)) | 0;
      }
    }
  }
  return total;
}

export function sumI3216Page(
  view: PageView,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): number {
  const values = view.unsigned as Uint16Array,
    validity = view.validity,
    base = view.baseLow;
  let total = 0;
  if (rows === null) {
    if (validity === null) {
      for (let row = from; row < to; row++) total += (base + (values[row] ?? 0)) | 0;
    } else {
      for (let row = from; row < to; row++)
        if (((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0)
          total += (base + (values[row] ?? 0)) | 0;
    }
  } else {
    if (validity === null) {
      for (let k = from; k < to; k++) {
        const row = (rows[k] ?? 0) - firstRow;
        total += (base + (values[row] ?? 0)) | 0;
      }
    } else {
      for (let k = from; k < to; k++) {
        const row = (rows[k] ?? 0) - firstRow;
        if (((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0)
          total += (base + (values[row] ?? 0)) | 0;
      }
    }
  }
  return total;
}

export function sumI3232Page(
  view: PageView,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): number {
  const values = view.unsigned as Uint32Array,
    validity = view.validity,
    base = view.baseLow;
  let total = 0;
  if (rows === null) {
    if (validity === null) {
      for (let row = from; row < to; row++) total += (base + (values[row] ?? 0)) | 0;
    } else {
      for (let row = from; row < to; row++)
        if (((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0)
          total += (base + (values[row] ?? 0)) | 0;
    }
  } else {
    if (validity === null) {
      for (let k = from; k < to; k++) {
        const row = (rows[k] ?? 0) - firstRow;
        total += (base + (values[row] ?? 0)) | 0;
      }
    } else {
      for (let k = from; k < to; k++) {
        const row = (rows[k] ?? 0) - firstRow;
        if (((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0)
          total += (base + (values[row] ?? 0)) | 0;
      }
    }
  }
  return total;
}

/** Delta checkpoints permit direct sequential consumption without a widened scratch lane. */
export function sumDeltaI320Page(
  view: PageView,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): number {
  const checkpoints = view.checkpoints;
  if (checkpoints === null) throw new TypeError("Expected delta checkpoints");
  const base = view.baseLow;

  let total = 0,
    position = -1,
    blockEnd = 0,
    value = 0;
  for (let k = from; k < to; k++) {
    const row = rows === null ? k : (rows[k] ?? 0) - firstRow;
    if (position < 0 || row >= blockEnd) {
      const block = Math.floor(row / DELTA_BLOCK);
      position = block * DELTA_BLOCK;
      blockEnd = position + DELTA_BLOCK;
      value = checkpoints[block * 2] ?? 0;
    }
    while (position < row) {
      position++;
      value = (value + base) | 0;
    }
    total += value;
  }
  return total;
}

/** Delta checkpoints permit direct sequential consumption without a widened scratch lane. */
export function sumDeltaI328Page(
  view: PageView,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): number {
  const checkpoints = view.checkpoints;
  if (checkpoints === null) throw new TypeError("Expected delta checkpoints");
  const base = view.baseLow;
  const deltas = view.unsigned as Uint8Array;
  let total = 0,
    position = -1,
    blockEnd = 0,
    value = 0;
  for (let k = from; k < to; k++) {
    const row = rows === null ? k : (rows[k] ?? 0) - firstRow;
    if (position < 0 || row >= blockEnd) {
      const block = Math.floor(row / DELTA_BLOCK);
      position = block * DELTA_BLOCK;
      blockEnd = position + DELTA_BLOCK;
      value = checkpoints[block * 2] ?? 0;
    }
    while (position < row) {
      position++;
      value = (value + base + (deltas[position] ?? 0)) | 0;
    }
    total += value;
  }
  return total;
}

/** Delta checkpoints permit direct sequential consumption without a widened scratch lane. */
export function sumDeltaI3216Page(
  view: PageView,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): number {
  const checkpoints = view.checkpoints;
  if (checkpoints === null) throw new TypeError("Expected delta checkpoints");
  const base = view.baseLow;
  const deltas = view.unsigned as Uint16Array;
  let total = 0,
    position = -1,
    blockEnd = 0,
    value = 0;
  for (let k = from; k < to; k++) {
    const row = rows === null ? k : (rows[k] ?? 0) - firstRow;
    if (position < 0 || row >= blockEnd) {
      const block = Math.floor(row / DELTA_BLOCK);
      position = block * DELTA_BLOCK;
      blockEnd = position + DELTA_BLOCK;
      value = checkpoints[block * 2] ?? 0;
    }
    while (position < row) {
      position++;
      value = (value + base + (deltas[position] ?? 0)) | 0;
    }
    total += value;
  }
  return total;
}

/** Delta checkpoints permit direct sequential consumption without a widened scratch lane. */
export function sumDeltaI3232Page(
  view: PageView,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): number {
  const checkpoints = view.checkpoints;
  if (checkpoints === null) throw new TypeError("Expected delta checkpoints");
  const base = view.baseLow;
  const deltas = view.unsigned as Uint32Array;
  let total = 0,
    position = -1,
    blockEnd = 0,
    value = 0;
  for (let k = from; k < to; k++) {
    const row = rows === null ? k : (rows[k] ?? 0) - firstRow;
    if (position < 0 || row >= blockEnd) {
      const block = Math.floor(row / DELTA_BLOCK);
      position = block * DELTA_BLOCK;
      blockEnd = position + DELTA_BLOCK;
      value = checkpoints[block * 2] ?? 0;
    }
    while (position < row) {
      position++;
      value = (value + base + (deltas[position] ?? 0)) | 0;
    }
    total += value;
  }
  return total;
}
