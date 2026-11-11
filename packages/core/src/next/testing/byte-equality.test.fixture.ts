/** Exact equality with the expected-length slice, including unaligned views and tails.
 * Portable word loads avoid a callback for every byte in large output-oracle checks.
 */
export function byteSliceEquals(actual: Uint8Array, from: number, expected: Uint8Array): boolean {
  const length = expected.byteLength;
  if (!Number.isSafeInteger(from) || from < 0 || from + length > actual.byteLength) return false;
  const a = new DataView(actual.buffer, actual.byteOffset + from, length);
  const b = new DataView(expected.buffer, expected.byteOffset, length);
  let at = 0;
  for (; at + 4 <= length; at += 4)
    if (a.getUint32(at, true) !== b.getUint32(at, true)) return false;
  for (; at < length; at++) if (a.getUint8(at) !== b.getUint8(at)) return false;
  return true;
}
