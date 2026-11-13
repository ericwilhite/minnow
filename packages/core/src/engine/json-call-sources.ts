/** Native JSON provenance stays outside published plan DTOs and result binding ids. */
interface RowField {
  readonly qualifier: string;
  readonly ordinal: number;
}
interface Provenance {
  readonly call?: string;
  readonly name?: string;
  readonly field?: RowField;
  readonly row?: string;
}
const sources = new WeakMap<object, Provenance>();
let present = false;
const get = (value: object): Provenance | undefined => (present ? sources.get(value) : undefined);
function remember(value: object, data: Provenance): void {
  present = true;
  // Clones share immutable provenance; a later rename never mutates the cached source plan.
  sources.set(value, { ...sources.get(value), ...data });
}
export function jsonCallSource(value: object): string | undefined {
  return get(value)?.call;
}
export function rememberJsonCall(value: object, name: string): void {
  remember(value, { call: name });
}
export function rowOutputName(value: object): string | undefined {
  return get(value)?.name;
}
export function rememberRowOutputName(value: object, name: string): void {
  remember(value, { name });
}
export function rowFieldSource(value: object): RowField | undefined {
  return get(value)?.field;
}
export function rememberRowField(value: object, qualifier: string, ordinal: number): void {
  remember(value, { field: { qualifier, ordinal } });
}
export function rowObjectSource(value: object): string | undefined {
  return get(value)?.row;
}
export function rememberRowObject(value: object, qualifier: string): void {
  remember(value, { row: qualifier });
}
/** Clone hooks preserve call identity, record labels and exact field bindings together. */
export function copyJsonCall(source: object, target: object): void {
  const data = get(source);
  if (data !== undefined) sources.set(target, data);
}
