import type { KeyRange } from "../access/key-index.js";
import { compareBytes } from "../vector/varlen.js";

export function rangeSide(range: KeyRange, key: Uint8Array, start = 0, end = key.length): number {
  const lower = range.lower;
  if (
    lower !== null &&
    (compareBytes(key, start, end, lower.key, 0, lower.key.length) < 0 ||
      (lower.kind === "after" && starts(key, start, end, lower.key)))
  )
    return -1;
  const upper = range.upper;
  if (upper !== null) {
    const cmp = compareBytes(key, start, end, upper.key, 0, upper.key.length);
    if (upper.kind === "before" ? cmp >= 0 : cmp > 0 && !starts(key, start, end, upper.key))
      return 1;
  }
  return 0;
}
function starts(key: Uint8Array, start: number, end: number, prefix: Uint8Array): boolean {
  if (end - start < prefix.length) return false;
  for (let i = 0; i < prefix.length; i++) if (key[start + i] !== prefix[i]) return false;
  return true;
}
