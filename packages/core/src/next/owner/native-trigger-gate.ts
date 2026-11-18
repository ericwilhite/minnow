/** Release qualification gate. Final enablement requires the same histories at 32 MiB.
 * Tests may replace this internal function; no public option bypasses qualification. */
export function nativeTriggerFiringEnabled(): boolean {
  return false;
}
