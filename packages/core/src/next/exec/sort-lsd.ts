/** Stable LSD on normalized fixed-width columns. Scratch is owned/charged by the caller. */
import type { Slice } from "./slices.js";
import type { RadixScratch } from "./sort-keys.js";

const LSD_STEP_ROWS = 1024;

export function* lsdSortIds(
  columns: readonly Uint32Array[],
  ids: Int32Array,
  temporary: Int32Array,
  n: number,
  slice: Slice,
  radix: RadixScratch,
): Generator<void, void, void> {
  for (let i = 0; i < n; i++) {
    ids[i] = i;
    if ((i & (LSD_STEP_ROWS - 1)) === LSD_STEP_ROWS - 1 && slice.due()) yield;
  }
  let source = ids;
  let target = temporary;
  const counts = radix.counts;
  for (let w = columns.length - 1; w >= 0; w--) {
    const values = columns[w];
    if (values === undefined) throw new Error("Missing normalized column");
    let diff = 0;
    const first = values[0] ?? 0;
    for (let at = 0; at < n; at += LSD_STEP_ROWS) {
      diff |= difference(values, first, Math.max(1, at), Math.min(n, at + LSD_STEP_ROWS));
      if (slice.due()) yield;
    }
    for (const shift of [0, 16]) {
      if (((diff >>> shift) & 65535) === 0) continue;
      counts.fill(0);
      for (let at = 0; at < n; at += LSD_STEP_ROWS) {
        histogram(values, source, counts, shift, at, Math.min(n, at + LSD_STEP_ROWS));
        if (slice.due()) yield;
      }
      for (let d = 0; d < 65536; d++) {
        counts[d + 1] = (counts[d + 1] ?? 0) + (counts[d] ?? 0);
        if ((d & (LSD_STEP_ROWS - 1)) === LSD_STEP_ROWS - 1 && slice.due()) yield;
      }
      for (let at = 0; at < n; at += LSD_STEP_ROWS) {
        scatter(values, source, target, counts, shift, at, Math.min(n, at + LSD_STEP_ROWS));
        if (slice.due()) yield;
      }
      [source, target] = [target, source];
    }
  }
  if (source !== ids)
    for (let at = 0; at < n; at += LSD_STEP_ROWS) {
      ids.set(source.subarray(at, Math.min(n, at + LSD_STEP_ROWS)), at);
      if (slice.due()) yield;
    }
}

// Hot kernels are synchronous and bounded; the owning generator checks the task clock between
// calls. No kernel state survives independently of the operator-owned arrays.
function difference(values: Uint32Array, first: number, from: number, to: number): number {
  let diff = 0;
  for (let i = from; i < to; i++) diff |= (values[i] ?? 0) ^ first;
  return diff;
}
function histogram(
  values: Uint32Array,
  source: Int32Array,
  counts: Int32Array,
  shift: number,
  from: number,
  to: number,
): void {
  for (let i = from; i < to; i++) {
    const digit = ((values[source[i] ?? 0] ?? 0) >>> shift) & 65535;
    counts[digit + 1] = (counts[digit + 1] ?? 0) + 1;
  }
}
function scatter(
  values: Uint32Array,
  source: Int32Array,
  target: Int32Array,
  counts: Int32Array,
  shift: number,
  from: number,
  to: number,
): void {
  for (let i = from; i < to; i++) {
    const id = source[i] ?? 0;
    const digit = ((values[id] ?? 0) >>> shift) & 65535;
    target[counts[digit] ?? 0] = id;
    counts[digit] = (counts[digit] ?? 0) + 1;
  }
}
