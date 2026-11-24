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
  readonly rewrite?: RewriteForm;
  readonly negation?: object;
}
/**
 * The PostgreSQL form the front end rewrote a node from, for next/'s lowering: `truth` for the
 * boolean of IS [NOT] TRUE|FALSE (written as IS [NOT] DISTINCT FROM it), `array` for the list
 * of `= ANY` / `<> ALL` over an ARRAY constructor (written as IN / NOT IN). A side table, never a
 * property: the AST's own keys (its query identity, 0.x's plans) stay unchanged.
 */
export type RewriteForm = "truth" | "array";
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
export function rewriteSource(value: object): RewriteForm | undefined {
  return get(value)?.rewrite;
}
export function rememberRewrite(value: object, form: RewriteForm): void {
  remember(value, { rewrite: form });
}
/**
 * The condition as written before the negation pass replaced `NOT (x op y)` by `x op' y`
 * (engine/optimizer.ts `negated`): PostgreSQL resolves the operator as written, so next/'s
 * lowering resolves this one first and its errors name it. A side table, as `rewriteSource`.
 */
export function negationSource(value: object): object | undefined {
  return get(value)?.negation;
}
export function rememberNegation(value: object, written: object): void {
  remember(value, { negation: written });
}
/** Clone hooks preserve call identity, record labels and exact field bindings together. */
export function copyJsonCall(source: object, target: object): void {
  const data = get(source);
  if (data !== undefined) sources.set(target, data);
}
