/** A long summary boundary is a prefix, never an equality/disjointness witness. */
import type { KeyRange } from "../access/key-index.js";
import type { RunRef } from "../format/manifest.js";
import { compareBytes } from "../vector/varlen.js";
export type RunSide = "min" | "max";
export function truncatedBound(run: RunRef, side: RunSide): boolean {
  const key = side === "min" ? run.keyStorage?.minimum : run.keyStorage?.maximum;
  return (run.encoding ?? 1) === 2 && key?.[0] === 1;
}
/** Zero can mean unknown on a prefix tie. A strict sign is always a proven full-key order. */
export function compareRunBound(run: RunRef, side: RunSide, key: Uint8Array): number {
  const bytes = side === "min" ? run.minKey : run.maxKey;
  if (!truncatedBound(run, side)) return compareBytes(bytes, 0, bytes.length, key, 0, key.length);
  const length = Math.min(bytes.length, key.length);
  const cmp = compareBytes(bytes, 0, length, key, 0, length);
  return cmp || (key.length <= bytes.length ? 1 : 0);
}
export function runOutsideKeys(run: RunRef, low: Uint8Array, high = low): boolean {
  return compareRunBound(run, "max", low) < 0 || compareRunBound(run, "min", high) > 0;
}
function starts(bytes: Uint8Array, prefix: Uint8Array): boolean {
  return (
    bytes.length >= prefix.length &&
    compareBytes(bytes, 0, prefix.length, prefix, 0, prefix.length) === 0
  );
}
function boundarySide(run: RunRef, side: RunSide, range: KeyRange): number {
  const bytes = side === "min" ? run.minKey : run.maxKey;
  const lower = range.lower;
  if (
    lower !== null &&
    (compareRunBound(run, side, lower.key) < 0 ||
      (lower.kind === "after" && starts(bytes, lower.key)))
  )
    return -1;
  const upper = range.upper;
  if (upper !== null) {
    const cmp = compareRunBound(run, side, upper.key);
    if (
      upper.kind === "before"
        ? cmp > 0 || (cmp === 0 && !truncatedBound(run, side))
        : cmp > 0 && !starts(bytes, upper.key)
    )
      return 1;
  }
  return 0;
}
/** A missing sign means retain and resolve; it must never discard a possible match. */
export function runOutsideRange(run: RunRef, range: KeyRange): boolean {
  return boundarySide(run, "max", range) < 0 || boundarySide(run, "min", range) > 0;
}
