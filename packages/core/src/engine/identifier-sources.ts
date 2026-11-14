/** Exact SQL identifier boundaries for typed lowering, outside the published plan DTO. */
interface IdentifierSource {
  readonly parts: readonly string[];
  readonly quoted: readonly boolean[];
}
const sources = new WeakMap<object, IdentifierSource>();
export function rememberIdentifierSource(
  value: object,
  parts: readonly string[],
  quoted: readonly boolean[],
): void {
  sources.set(value, { parts, quoted });
}
export function identifierSource(value: object): IdentifierSource | undefined {
  return sources.get(value);
}
export function copyIdentifierSource(source: object, target: object): void {
  const value = sources.get(source);
  if (value !== undefined) sources.set(target, value);
}
