/** Preserve native DATE_PART identity without changing the shipped EXTRACT alias or public DTO. */
const sources = new WeakSet();
let present = false;
export function isDatePart(value: object): boolean {
  return present && sources.has(value);
}
export function rememberDatePart(value: object): void {
  present = true;
  sources.add(value);
}
export function copyDatePart(source: object, target: object): void {
  if (isDatePart(source)) rememberDatePart(target);
}
