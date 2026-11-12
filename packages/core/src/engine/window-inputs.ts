/** Optional private typed-frontend extension. The published parser retains its own grammar. */
import type { Expression, WindowSpec } from "../plan/model.js";
export interface WindowInputs {
  readonly offsetExpression?: Expression;
  readonly fallbackExpression?: Expression;
  readonly offsetAlias?: string;
}
export type WindowExpression = Extract<Expression, { kind: "window" }>;
interface Extension {
  read(node: object): WindowInputs;
  map<T extends object>(
    source: object,
    target: T,
    visit: (expression: Expression) => Expression,
  ): T;
  parse(name: "LAG" | "LEAD", args: readonly Expression[]): WindowExpression;
  complete(
    expression: WindowExpression,
    spec: WindowSpec,
    hide: (expression: Expression) => string,
  ): WindowSpec;
}
let extension: Extension | undefined;
const empty: WindowInputs = {};
export function registerWindowInputs(value: Extension): void {
  extension = value;
}
export function windowInputsOf(node: object): WindowInputs {
  return extension?.read(node) ?? empty;
}
export function mapWindowInputs<T extends object>(
  source: object,
  target: T,
  visit: (expression: Expression) => Expression,
): T {
  return extension?.map(source, target, visit) ?? target;
}
export function typedWindowShift(
  name: "LAG" | "LEAD",
  args: readonly Expression[],
): WindowExpression | undefined {
  return extension?.parse(name, args);
}
export function completeWindowInputs(
  expression: WindowExpression,
  spec: WindowSpec,
  hide: (expression: Expression) => string,
): WindowSpec {
  return extension?.complete(expression, spec, hide) ?? spec;
}
