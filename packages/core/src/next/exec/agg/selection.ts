/** Exact numeric rank selection; NaN sorts after infinity, signed zeros compare equal. */
import type { SlicedJob } from "../slices.js";
import { Slice } from "../slices.js";

export function compareNumber(a: number, b: number, descending = false): number {
  const order =
    a === b
      ? 0
      : Number.isNaN(a)
        ? Number.isNaN(b)
          ? 0
          : 1
        : Number.isNaN(b)
          ? -1
          : a < b
            ? -1
            : 1;
  return descending ? -order : order;
}

/** In-place three-way introselect, with a paced heapsort fallback after bad partitions.
 * The caller owns/charges the complete array. No arrays or native sort are created here.
 */
export function* selectNumber(
  values: Float64Array,
  from: number,
  to: number,
  rank: number,
  descending: boolean,
  slice: Slice,
  partitionBudget = 2 * Math.ceil(Math.log2(to - from + 1)),
): SlicedJob<number> {
  if (from < 0 || to > values.length || rank < from || rank >= to)
    throw new RangeError("Invalid selection rank");
  let lo = from,
    hi = to,
    depth = partitionBudget,
    ticks = 0;
  const cmp = (a: number, b: number) => compareNumber(a, b, descending);
  const swap = (a: number, b: number) => {
    const v = values[a] ?? NaN;
    values[a] = values[b] ?? NaN;
    values[b] = v;
  };
  while (hi - lo > 1) {
    if (--depth < 0) {
      // A heap bounds adversarial pivot histories. Its indices are relative to lo.
      const n = hi - lo;
      function* sift(root: number, size: number): SlicedJob<void> {
        for (;;) {
          let child = 2 * root + 1;
          if (child >= size) return;
          if (child + 1 < size && cmp(values[lo + child] ?? NaN, values[lo + child + 1] ?? NaN) < 0)
            child++;
          if (cmp(values[lo + root] ?? NaN, values[lo + child] ?? NaN) >= 0) return;
          swap(lo + root, lo + child);
          root = child;
          if ((++ticks & 1023) === 0 && slice.due()) yield;
        }
      }
      for (let i = (n >>> 1) - 1; i >= 0; i--) {
        yield* sift(i, n);
        if ((++ticks & 1023) === 0 && slice.due()) yield;
      }
      for (let i = n - 1; i > 0; i--) {
        swap(lo, lo + i);
        yield* sift(0, i);
        if ((++ticks & 1023) === 0 && slice.due()) yield;
      }
      return values[rank] ?? NaN;
    }
    const mid = lo + ((hi - lo) >>> 1);
    if (cmp(values[lo] ?? NaN, values[mid] ?? NaN) > 0) swap(lo, mid);
    if (cmp(values[mid] ?? NaN, values[hi - 1] ?? NaN) > 0) swap(mid, hi - 1);
    if (cmp(values[lo] ?? NaN, values[mid] ?? NaN) > 0) swap(lo, mid);
    const pivot = values[mid] ?? NaN;
    let lower = lo,
      row = lo,
      upper = hi;
    while (row < upper) {
      const order = cmp(values[row] ?? NaN, pivot);
      if (order < 0) swap(row++, lower++);
      else if (order > 0) swap(row, --upper);
      else row++;
      if ((++ticks & 1023) === 0 && slice.due()) yield;
    }
    if (rank < lower) hi = lower;
    else if (rank >= upper) lo = upper;
    else return values[rank] ?? NaN;
  }
  return values[rank] ?? NaN;
}
