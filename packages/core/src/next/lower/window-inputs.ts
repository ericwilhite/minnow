/** Private lag/lead grammar and clone-safe lazy inputs. No implementation ships in core's parser. */
import type { Expression, WindowSpec } from "../../plan/model.js";
import { isAggregateCall, mapChildExpressions } from "../../engine/query.js";
import {
  registerWindowInputs,
  type WindowInputs,
  type WindowExpression,
} from "../../engine/window-inputs.js";
const inputs = new WeakMap<object, WindowInputs>();
const empty: WindowInputs = {};

export function windowInputsOf(node: object): WindowInputs {
  return inputs.get(node) ?? empty;
}

export function rememberWindowInputs<T extends object>(node: T, value: WindowInputs): T {
  inputs.set(node, value);
  return node;
}

/** Every private clone/rewrite carries its hidden dependencies through the same visitor. */
export function mapWindowInputs<T extends object>(
  source: object,
  target: T,
  visit: (expression: Expression) => Expression,
): T {
  const value = inputs.get(source);
  if (value === undefined) return target;
  return rememberWindowInputs(target, {
    ...(value.offsetAlias === undefined ? {} : { offsetAlias: value.offsetAlias }),
    ...(value.offsetExpression === undefined
      ? {}
      : { offsetExpression: visit(value.offsetExpression) }),
    ...(value.fallbackExpression === undefined
      ? {}
      : { fallbackExpression: visit(value.fallbackExpression) }),
  });
}

function typedWindowShift(name: "LAG" | "LEAD", args: readonly Expression[]): WindowExpression {
  const argument = args[0];
  if (argument === undefined || argument.kind === "wildcard")
    throw new TypeError(`${name} requires a value argument`);
  const fallback = args[2];
  // CASE supplies R19's common-type context without evaluating its unselected arm.
  // Both forms list value before default, matching anycompatible's candidate order.
  const contextual = (selected: boolean): Expression => ({
    kind: "case",
    branches: [{ when: { kind: "literal", value: selected }, then: argument }],
    ...(fallback === undefined ? {} : { otherwise: fallback }),
  });
  const offset = args[1];
  const literalOffset =
    offset?.kind === "literal" &&
    typeof offset.value === "number" &&
    Number.isInteger(offset.value) &&
    (offset.exactText === undefined || !/[.eE]/u.test(offset.exactText)) &&
    offset.value >= -2147483648 &&
    offset.value <= 2147483647
      ? offset.value
      : undefined;
  return rememberWindowInputs(
    {
      kind: "window",
      name: name,
      partitionBy: [],
      orderBy: [],
      argument: fallback === undefined ? argument : contextual(true),
      ...(literalOffset === undefined ? {} : { offset: literalOffset }),
    },
    {
      ...(offset === undefined || literalOffset !== undefined
        ? {}
        : {
            offsetExpression:
              offset.kind === "parameter" ||
              (offset.kind === "literal" &&
                (offset.value === null || typeof offset.value === "string"))
                ? {
                    kind: "case" as const,
                    branches: [{ when: { kind: "literal" as const, value: true }, then: offset }],
                    otherwise: { kind: "literal" as const, value: 1 },
                  }
                : offset,
          }),
      ...(fallback === undefined ? {} : { fallbackExpression: contextual(false) }),
    },
  );
}

function completeWindowInputs(
  expression: WindowExpression,
  spec: WindowSpec,
  hide: (expression: Expression) => string,
): WindowSpec {
  const metadata = inputs.get(expression);
  if (metadata === undefined) return spec;
  // Only dependencies are eager. Scalar default work remains at the demanded output row.
  const defaultInput = (input: Expression): Expression =>
    input.kind === "column" || isAggregateCall(input)
      ? { kind: "column", reference: hide(input) }
      : mapChildExpressions(input, defaultInput);
  return rememberWindowInputs(spec, {
    ...(metadata.offsetExpression === undefined
      ? {}
      : { offsetAlias: hide(metadata.offsetExpression) }),
    ...(metadata.fallbackExpression === undefined
      ? {}
      : { fallbackExpression: defaultInput(metadata.fallbackExpression) }),
  });
}
registerWindowInputs({
  read: windowInputsOf,
  map: mapWindowInputs,
  parse: typedWindowShift,
  complete: completeWindowInputs,
});
