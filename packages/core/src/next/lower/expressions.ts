/**
 * Front-end expressions (plan/model.ts `Expression`, `Predicate`) to typed IR scalar
 * expressions. Names resolve through a `Scope`; types follow PostgreSQL (lower/coerce.ts).
 */
import type { CatalogView } from "../access/catalog.js";
import { NON_STRICT_FUNCTIONS, VOLATILE_FUNCTIONS } from "../ir/functions.js";
import type { Collation, ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import type { Expression, PredicateOperator } from "../../plan/model.js";
import { isAggregateCall } from "../../engine/query.js";
import { externalSqlDomainValue } from "../../engine/sql-domains.js";
import { timestampFromDate } from "../temporal.js";
import {
  coerce,
  commonType,
  resolveFunction,
  settle,
  type ParameterTypes,
  type Typed,
} from "./coerce.js";
import { PlanningError, unsupported } from "./errors.js";
import {
  boolLiteral,
  decimalLiteral,
  integerLiteral,
  parseDecimalText,
  textLiteral,
  timestampLiteral,
} from "./literals.js";

/** How names (and, in a grouped block, whole expressions) resolve. */
export interface Scope {
  /** A column reference as the front end writes it (`a`, `t.a`), or undefined if unknown. */
  column(reference: string): Typed | undefined;
  /**
   * A whole expression this scope already computes: a GROUP BY key or an aggregate of a grouped
   * block. Tried before the expression is lowered structurally.
   */
  match?(expression: Expression): Typed | undefined;
}

/** State shared by every expression of one statement. */
export interface ExpressionState {
  readonly parameters: ParameterTypes;
  readonly catalog: CatalogView;
  /** The statement clock's slot (allocated on first use). */
  clock(): number;
  volatile: boolean;
  sequences: boolean;
}

/** A typed value with the explicit collation a `COLLATE` gave it (C8). */
export type Collated = Typed & { readonly collation?: string };

const BOOL: PhysicalType = { kind: "bool" };
const TIMESTAMPTZ: PhysicalType = { kind: "timestamp", zoned: true };

/** Front-end scalar function → IR function (ir/functions.ts). */
const FUNCTION_NAMES: Readonly<Record<string, string>> = {
  ABS: "abs",
  ROUND: "round",
  TRUNC: "trunc",
  CEIL: "ceil",
  FLOOR: "floor",
  SIGN: "sign",
  SQRT: "sqrt",
  EXP: "exp",
  LN: "ln",
  POWER: "power",
  MOD: "mod",
  UPPER: "upper",
  LOWER: "lower",
  LENGTH: "length",
  OCTET_LENGTH: "octet_length",
  SUBSTR: "substr",
  TRIM: "btrim",
  BTRIM: "btrim",
  LTRIM: "ltrim",
  RTRIM: "rtrim",
  REPLACE: "replace",
  STRPOS: "strpos",
  LEFT: "left",
  RIGHT: "right",
  STARTS_WITH: "starts_with",
  COALESCE: "coalesce",
  NULLIF: "nullif",
  GREATEST: "greatest",
  LEAST: "least",
  DATE_TRUNC: "date_trunc",
  EXTRACT: "extract",
  RANDOM: "random",
  GEN_RANDOM_UUID: "gen_random_uuid",
};

const BINARY_FUNCTIONS: Readonly<Record<string, string>> = {
  "+": "add",
  "-": "sub",
  "*": "mul",
  "/": "div",
  "%": "mod",
};

/** The type a CAST target literal names (`"number-integer"` + `declaredTypeName`, …). */
export function castTargetType(target: Expression): PhysicalType {
  if (target.kind !== "literal" || typeof target.value !== "string")
    throw new PlanningError("Malformed CAST target", "42601");
  const value = target.value;
  switch (value) {
    case "number-integer":
      return target.declaredTypeName === "smallint"
        ? { kind: "int32", bits: 16 }
        : target.declaredTypeName === "integer"
          ? { kind: "int32", bits: 32 }
          : { kind: "int64" };
    case "number":
      return target.declaredTypeName === "real" ? { kind: "float32" } : { kind: "float64" };
    case "string": {
      const width = target.declaredLength;
      if (width === undefined) return { kind: "text" };
      return width.fixed
        ? { kind: "text", maxLength: width.length, fixed: true }
        : { kind: "text", maxLength: width.length };
    }
    case "boolean":
      return BOOL;
    case "datetime":
      return { kind: "timestamp", zoned: target.timestamp !== true };
    case "json":
      return { kind: "json", binary: false };
    case "jsonb":
      return { kind: "json", binary: true };
    case "uuid":
      return { kind: "uuid" };
    case "date":
      return { kind: "date" };
    case "time":
      return { kind: "time" };
    case "interval":
      return { kind: "interval" };
  }
  const numeric = /^numeric:(\d*):(\d*)$/u.exec(value);
  if (numeric !== null) {
    const [, precision = "", scale = ""] = numeric;
    return {
      kind: "decimal",
      precision: precision === "" ? null : Number(precision),
      scale: scale === "" ? null : Number(scale),
    };
  }
  throw unsupported("columnType", `CAST to ${value}`);
}

/** The literal of a front-end numeric constant: int4, int8 or numeric by magnitude. */
function numberLiteral(value: number, exactText: string | undefined, decimal: boolean): Typed {
  const text = exactText ?? String(value);
  if (!decimal && Number.isInteger(value) && /^[+-]?\d+$/u.test(text)) {
    const whole = BigInt(text);
    const expr =
      integerLiteral(whole, { kind: "int32", bits: 32 }) ??
      integerLiteral(whole, { kind: "int64" });
    if (expr !== null) return { expr, nullable: false, big: false };
  }
  const parsed = parseDecimalText(text);
  if (parsed === null) throw new PlanningError(`invalid numeric constant ${text}`, "22P02");
  return { expr: decimalLiteral(parsed.coefficient, parsed.scale), nullable: false, big: false };
}

function lowerLiteral(expression: Extract<Expression, { kind: "literal" }>): Typed {
  const value = expression.value;
  if (value === null)
    return { expr: textLiteral(""), nullable: true, big: false, pending: { kind: "null" } };
  if (typeof value === "boolean") return { expr: boolLiteral(value), nullable: false, big: false };
  if (typeof value === "number")
    return numberLiteral(value, expression.exactText, expression.decimal === true);
  if (value instanceof Date) {
    const zoned = expression.timestamp !== true || expression.zoned === true;
    return { expr: timestampLiteral(timestampFromDate(value), zoned), nullable: false, big: false };
  }
  if (expression.internalSqlValue === true && expression.sqlDomain !== undefined) {
    const text = String(externalSqlDomainValue(value));
    const domain = expression.sqlDomain;
    switch (domain.kind) {
      case "numeric": {
        const parsed = parseDecimalText(text);
        if (parsed !== null)
          return {
            expr: decimalLiteral(parsed.coefficient, parsed.scale),
            nullable: false,
            big: false,
          };
        break;
      }
      case "date":
      case "time":
      case "interval":
      case "uuid":
      case "json":
      case "jsonb": {
        const type: PhysicalType =
          domain.kind === "json" || domain.kind === "jsonb"
            ? { kind: "json", binary: domain.kind === "jsonb" }
            : { kind: domain.kind };
        return {
          expr: { op: "cast", input: textLiteral(text), type },
          nullable: false,
          big: false,
        };
      }
      default:
        break;
    }
    return {
      expr: textLiteral(text),
      nullable: false,
      big: false,
      pending: { kind: "text", text },
    };
  }
  return {
    expr: textLiteral(value),
    nullable: false,
    big: false,
    pending: { kind: "text", text: value },
  };
}

function strictNullable(args: readonly Typed[]): boolean {
  return args.some((arg) => arg.nullable);
}

/** A call of an IR function with its arguments coerced to the chosen overload. */
export function callFunction(
  fn: string,
  args: readonly Typed[],
  state: ExpressionState,
  nullable?: boolean,
): Typed {
  const resolved = resolveFunction(fn, args, state.parameters);
  const coerced = args.map((arg, index) =>
    coerce(arg, resolved.args[index] ?? resolved.result, "implicit", state.parameters),
  );
  if (VOLATILE_FUNCTIONS.has(fn)) state.volatile = true;
  const anyNullable = NON_STRICT_FUNCTIONS.has(fn)
    ? fn === "nullif" || coerced.every((arg) => arg.nullable)
    : strictNullable(coerced);
  return {
    expr: { op: "call", fn, args: coerced.map((arg) => arg.expr), type: resolved.result },
    nullable: nullable ?? anyNullable,
    big: resolved.result.kind === "int64" && coerced.some((arg) => arg.big),
  };
}

export function asBool(typed: Typed, state: ExpressionState): Typed {
  return coerce(typed, BOOL, "implicit", state.parameters);
}

/** The collation of compared operands: an explicit COLLATE on either side, else the default. */
function collationOf(...values: readonly Collated[]): Collation {
  for (const value of values) if (value.collation !== undefined) return value.collation;
  return null;
}

function unify(values: readonly Typed[], state: ExpressionState, what: string): Typed[] {
  const type = commonType(values, state.parameters, what);
  return values.map((value) => coerce(value, type, "implicit", state.parameters));
}

const COMPARISONS: Readonly<Record<string, "=" | "<>" | "<" | "<=" | ">" | ">=">> = {
  "=": "=",
  "!=": "<>",
  "<>": "<>",
  "<": "<",
  "<=": "<=",
  ">": ">",
  ">=": ">=",
};

/** A front-end condition (`left operator right`) as a bool expression. */
export function lowerCondition(
  operator: PredicateOperator,
  left: Expression,
  right: Expression,
  escape: string | undefined,
  scope: Scope,
  state: ExpressionState,
): Typed {
  const cmp = COMPARISONS[operator];
  if (cmp !== undefined) {
    const l = lowerExpression(left, scope, state);
    const r = lowerExpression(right, scope, state);
    const [a, b] = unify([l, r], state, "comparison") as [Typed, Typed];
    return {
      expr: {
        op: "compare",
        cmp,
        left: a.expr,
        right: b.expr,
        collation: collationOf(l, r),
        type: BOOL,
      },
      nullable: a.nullable || b.nullable,
      big: false,
    };
  }
  switch (operator) {
    case "IS NULL":
    case "IS NOT NULL": {
      const arg = settle(lowerExpression(left, scope, state), state.parameters);
      return {
        expr: { op: "isNull", negated: operator === "IS NOT NULL", arg: arg.expr, type: BOOL },
        nullable: false,
        big: false,
      };
    }
    case "IS DISTINCT FROM":
    case "IS NOT DISTINCT FROM": {
      const [a, b] = unify(
        [lowerExpression(left, scope, state), lowerExpression(right, scope, state)],
        state,
        "IS DISTINCT FROM",
      ) as [Typed, Typed];
      return {
        expr: {
          op: "distinctFrom",
          negated: operator === "IS NOT DISTINCT FROM",
          left: a.expr,
          right: b.expr,
          type: BOOL,
        },
        nullable: false,
        big: false,
      };
    }
    case "IS TRUE": {
      const arg = asBool(lowerExpression(left, scope, state), state);
      return {
        expr: {
          op: "distinctFrom",
          negated: true,
          left: arg.expr,
          right: boolLiteral(true),
          type: BOOL,
        },
        nullable: false,
        big: false,
      };
    }
    case "IN":
    case "NOT IN": {
      if (right.kind !== "list")
        throw unsupported(
          "subquery",
          `${operator} over ${right.kind === "subquery" ? "a subquery" : right.kind}`,
        );
      const arg = lowerExpression(left, scope, state);
      const items = right.items.map((item) => lowerExpression(item, scope, state));
      const [first, ...rest] = unify([arg, ...items], state, "IN");
      if (first === undefined) throw new PlanningError("IN needs a value", "42601");
      return {
        expr: {
          op: "inList",
          negated: operator === "NOT IN",
          arg: first.expr,
          list: rest.map((item) => item.expr),
          collation: collationOf(arg, ...items),
          type: BOOL,
        },
        nullable: first.nullable || rest.some((item) => item.nullable),
        big: false,
      };
    }
    case "LIKE":
    case "NOT LIKE":
    case "ILIKE":
    case "NOT ILIKE":
    case "SIMILAR TO":
    case "NOT SIMILAR TO": {
      const text: PhysicalType = { kind: "text" };
      const arg = coerce(lowerExpression(left, scope, state), text, "implicit", state.parameters);
      const pattern = coerce(
        lowerExpression(right, scope, state),
        text,
        "implicit",
        state.parameters,
      );
      return {
        expr: {
          op: "like",
          mode: operator.endsWith("ILIKE")
            ? "ilike"
            : operator.endsWith("SIMILAR TO")
              ? "similar"
              : "like",
          negated: operator.startsWith("NOT "),
          arg: arg.expr,
          pattern: pattern.expr,
          escape: escape ?? null,
          type: BOOL,
        },
        nullable: arg.nullable || pattern.nullable,
        big: false,
      };
    }
    default:
      throw unsupported("function", `the ${operator} operator`);
  }
}

function lowerCall(
  expression: Extract<Expression, { kind: "call" }>,
  scope: Scope,
  state: ExpressionState,
): Collated {
  const name = expression.name;
  const args = expression.arguments;
  switch (name) {
    case "CAST": {
      const [operand, target] = args;
      if (operand === undefined || target === undefined)
        throw new PlanningError("Malformed CAST", "42601");
      return coerce(
        lowerExpression(operand, scope, state),
        castTargetType(target),
        "explicit",
        state.parameters,
      );
    }
    case "MINNOW_COLLATE": {
      const [operand, collation] = args;
      if (
        operand === undefined ||
        collation?.kind !== "literal" ||
        typeof collation.value !== "string"
      )
        throw new PlanningError("Malformed COLLATE", "42601");
      const typed = coerce(
        lowerExpression(operand, scope, state),
        { kind: "text" },
        "implicit",
        state.parameters,
      );
      return {
        ...typed,
        collation: collation.value === "default" ? undefined : collation.value,
      } as Collated;
    }
    case "MINNOW_TIMESTAMP_TEXT":
    case "MINNOW_TO_CHAR_LOCAL": {
      // Rendering wrappers of the current engine: a zoneless timestamp already renders bare.
      const [operand] = args;
      if (operand === undefined) throw new PlanningError(`Malformed ${name}`, "42601");
      return lowerExpression(operand, scope, state);
    }
    case "CURRENT_TIMESTAMP":
      return {
        expr: { op: "param", slot: state.clock(), type: TIMESTAMPTZ },
        nullable: false,
        big: false,
      };
    case "CURRENT_DATE":
      return {
        expr: {
          op: "cast",
          input: { op: "param", slot: state.clock(), type: TIMESTAMPTZ },
          type: { kind: "date" },
        },
        nullable: false,
        big: false,
      };
    case "LOCALTIME":
      return {
        expr: {
          op: "cast",
          input: { op: "param", slot: state.clock(), type: TIMESTAMPTZ },
          type: { kind: "time" },
        },
        nullable: false,
        big: false,
      };
    case "NEXTVAL":
    case "CURRVAL": {
      const [nameArg] = args;
      if (nameArg?.kind !== "literal" || typeof nameArg.value !== "string")
        throw unsupported("function", `${name} of a computed sequence name`);
      const sequenceName = nameArg.value;
      const sequence = state.catalog.sequences.find((candidate) => candidate.name === sequenceName);
      if (sequence === undefined)
        throw new PlanningError(`relation "${sequenceName}" does not exist`, "42P01");
      state.sequences = true;
      if (name === "NEXTVAL") state.volatile = true;
      const id = integerLiteral(BigInt(sequence.id), { kind: "int64" });
      if (id === null) throw new PlanningError("Sequence id out of range", "XX000");
      return {
        expr: {
          op: "call",
          fn: name === "NEXTVAL" ? "nextval" : "currval",
          args: [id],
          type: { kind: "int64" },
        },
        nullable: false,
        big: true,
      };
    }
  }
  if (isAggregateCall(expression))
    throw new PlanningError(
      `aggregate function ${name.toLowerCase()} is not allowed here`,
      "42803",
    );
  const fn = FUNCTION_NAMES[name];
  if (fn === undefined) throw unsupported("function", name.toLowerCase());
  const lowered = args.map((arg) => lowerExpression(arg, scope, state));
  if (fn === "date_trunc" || fn === "extract") {
    // The unit or field is text; PostgreSQL types the constant as text.
    const [unit, ...rest] = lowered;
    if (unit === undefined) throw new PlanningError(`${fn} needs a field`, "42601");
    return callFunction(
      fn,
      [coerce(unit, { kind: "text" }, "implicit", state.parameters), ...rest],
      state,
    );
  }
  return callFunction(fn, lowered, state);
}

/** One front-end expression as a typed IR expression (pending values stay pending). */
export function lowerExpression(
  expression: Expression,
  scope: Scope,
  state: ExpressionState,
): Collated {
  const matched = scope.match?.(expression);
  if (matched !== undefined) return matched;
  switch (expression.kind) {
    case "literal":
      return lowerLiteral(expression);
    case "parameter": {
      const fixed = state.parameters.type(expression.index);
      if (fixed === null)
        return {
          expr: textLiteral(""),
          nullable: true,
          big: false,
          pending: { kind: "param", index: expression.index },
        };
      return {
        expr: { op: "param", slot: expression.index, type: fixed },
        nullable: true,
        big: fixed.kind === "int64",
      };
    }
    case "column": {
      const resolved = scope.column(expression.reference);
      if (resolved === undefined)
        throw new PlanningError(`column "${expression.reference}" does not exist`, "42703");
      return resolved;
    }
    case "wildcard":
      throw new PlanningError("* is not allowed here", "42601");
    case "binary": {
      const left = lowerExpression(expression.left, scope, state);
      const right = lowerExpression(expression.right, scope, state);
      if (expression.operator === "||") {
        const text: PhysicalType = { kind: "text" };
        const asText = (value: Typed) =>
          coerce(
            value,
            text,
            value.pending !== undefined ? "implicit" : "explicit",
            state.parameters,
          );
        return callFunction("textcat", [asText(left), asText(right)], state);
      }
      const fn = BINARY_FUNCTIONS[expression.operator];
      if (fn === undefined) throw unsupported("function", `the ${expression.operator} operator`);
      return callFunction(fn, [left, right], state);
    }
    case "call":
      return lowerCall(expression, scope, state);
    case "condition":
      return lowerCondition(
        expression.operator,
        expression.left,
        expression.right,
        expression.escape,
        scope,
        state,
      );
    case "logical": {
      const args: ScalarExpr[] = [];
      let nullable = false;
      const collect = (part: Expression): void => {
        if (part.kind === "logical" && part.operator === expression.operator) {
          collect(part.left);
          collect(part.right);
          return;
        }
        const typed = asBool(lowerExpression(part, scope, state), state);
        nullable ||= typed.nullable;
        args.push(typed.expr);
      };
      collect(expression);
      return { expr: { op: expression.operator, args, type: BOOL }, nullable, big: false };
    }
    case "not": {
      const operand = asBool(lowerExpression(expression.operand, scope, state), state);
      return {
        expr: { op: "not", arg: operand.expr, type: BOOL },
        nullable: operand.nullable,
        big: false,
      };
    }
    case "case": {
      const whens = expression.branches.map((branch) =>
        asBool(lowerExpression(branch.when, scope, state), state),
      );
      const thens = expression.branches.map((branch) => lowerExpression(branch.then, scope, state));
      const otherwise =
        expression.otherwise === undefined
          ? null
          : lowerExpression(expression.otherwise, scope, state);
      const results = unify(otherwise === null ? thens : [...thens, otherwise], state, "CASE");
      const values = otherwise === null ? results : results.slice(0, -1);
      const last = otherwise === null ? null : (results.at(-1) ?? null);
      const type = results[0]?.expr.type ?? { kind: "text" };
      return {
        expr: {
          op: "case",
          branches: whens.map((when, index) => ({
            when: when.expr,
            then: values[index]?.expr ?? when.expr,
          })),
          otherwise: last?.expr ?? null,
          type,
        },
        nullable: last === null || results.some((result) => result.nullable),
        big: type.kind === "int64" && results.some((result) => result.big),
      };
    }
    case "list":
      throw new PlanningError("A value list is allowed only after IN", "42601");
    case "subquery":
    case "exists":
      throw unsupported("subquery", "subqueries (milestone M3)");
    case "window":
      throw unsupported("window", "window functions (milestone M3)");
    case "fts":
      throw unsupported("fullTextSearch", "full-text search");
  }
}
