/** Original SRF arity before default-step synthesis; no public logical-plan field. */
const counts = new WeakMap<object, number>();
let present = false;
export function seriesArgumentCount(value: object): number | undefined {
  return present ? counts.get(value) : undefined;
}
export function rememberSeriesArgumentCount(value: object, count: number): void {
  present = true;
  counts.set(value, count);
}
export function copySeriesArgumentCount(source: object, target: object): void {
  const count = seriesArgumentCount(source);
  if (count !== undefined) rememberSeriesArgumentCount(target, count);
}
