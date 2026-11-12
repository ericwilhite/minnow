/** Native JSON identity; shipped aliases and published plan DTOs stay unchanged. */
const sources = new WeakMap<object, string>();
let present = false;
export function jsonCallSource(value: object): string | undefined {
  return present ? sources.get(value) : undefined;
}
export function rememberJsonCall(value: object, name: string): void {
  present = true;
  sources.set(value, name);
}
export function copyJsonCall(source: object, target: object): void {
  if (!present) return;
  const name = sources.get(source);
  if (name !== undefined) sources.set(target, name);
}
