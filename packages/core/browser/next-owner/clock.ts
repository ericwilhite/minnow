/** A performance.now interval on an integer nanosecond lattice, reported in microseconds.
 * Subtracting binary millisecond timestamps can put an exact 400 us browser tick just above 400.
 * Nearest-nanosecond representation removes that arithmetic artifact; it is not target slack.
 * Isolated browser clock steps are thousands of nanoseconds. Genuine above-target ticks stay
 * above target. Keep the same conversion for broker, transfer and point-read samples.
 */
export function elapsedMicros(startMs: number, endMs: number): number {
  return Math.round((endMs - startMs) * 1_000_000) / 1_000;
}
