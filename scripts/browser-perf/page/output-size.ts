/** BENCH candidate: quarter-power-of-two output classes, aligned for every native layout.
 * Zero stays empty; the first class is 8 bytes. All returned capacity, including slack, is charged.
 */
export function outputSize(bytes: number): number {
  if (!Number.isSafeInteger(bytes) || bytes < 0) throw new RangeError("Invalid output size");
  if (bytes === 0) return 0;
  const step = Math.max(8, 2 ** (Math.floor(Math.log2(bytes)) - 2));
  const capacity = Math.ceil(bytes / step) * step;
  if (!Number.isSafeInteger(capacity)) throw new RangeError("Output size overflow");
  return capacity;
}
