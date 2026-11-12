/**
 * Type resolution as PostgreSQL does it, over the IR's function table (ir/functions.ts): typed
 * expressions, untyped constants and placeholders, implicit / assignment / explicit coercion,
 * common types (CASE, COALESCE, comparison operands) and overload choice.
 */
import {
  explicitCastAllowed,
  FUNCTIONS,
  IMPLICIT_CASTS,
  PREFERRED_TYPES,
  physicalTypeOfSql,
  sqlTypeOf,
  type Overload,
  type SqlType,
} from "../ir/functions.js";
import type { ScalarExpr } from "../ir/ir.js";
import { samePhysicalType, type PhysicalType } from "../types.js";
import { PlanningError } from "./errors.js";
import {
  boolLiteral,
  decimalLiteral,
  floatLiteral,
  integerLiteral,
  integerLiteralValue,
  nullLiteral,
  parseDecimalText,
  textLiteral,
} from "./literals.js";

/**
 * A lowered expression. `pending` marks a value whose type the context decides (PostgreSQL's
 * `unknown`): a string constant, a bare NULL, or a placeholder not yet typed by its position.
 * Its `expr` is a placeholder (text) until `coerce` or `settle` fixes it.
 */
export interface Typed {
  readonly expr: ScalarExpr;
  readonly nullable: boolean;
  /** Renders as a JS bigint (ruling R17.1; lower/result.ts). */
  readonly big: boolean;
  readonly pending?:
    | { readonly kind: "text"; readonly text: string }
    | { readonly kind: "null" }
    | { readonly kind: "param"; readonly index: number };
}

/** Placeholder types by `$n` index, fixed by the first position that types each one. */
export class ParameterTypes {
  readonly #types: Array<PhysicalType | null>;
  constructor(count: number, fixed: ReadonlyArray<PhysicalType | null> = []) {
    this.#types = Array.from({ length: count }, (_, index) => fixed[index] ?? null);
  }
  get count(): number {
    return this.#types.length;
  }
  type(index: number): PhysicalType | null {
    return this.#types[index] ?? null;
  }
  fix(index: number, type: PhysicalType): PhysicalType {
    if (index >= this.#types.length)
      throw new PlanningError(`There is no parameter $${String(index + 1)}`, "42P02");
    const current = this.#types[index] ?? null;
    if (current !== null) return current;
    this.#types[index] = type;
    return type;
  }
  /** Every placeholder's type; one nothing fixed is `text`. */
  settled(): PhysicalType[] {
    return this.#types.map((type) => type ?? { kind: "text" });
  }
}

/** The type with its modifiers dropped (numeric(p, s) → numeric, varchar(n) → text). */
export function baseType(type: PhysicalType): PhysicalType {
  if (type.kind === "decimal") return { kind: "decimal", precision: null, scale: null };
  if (type.kind === "text") return { kind: "text" };
  return type;
}

function sameBase(left: PhysicalType, right: PhysicalType): boolean {
  return samePhysicalType(baseType(left), baseType(right));
}

export function canImplicitlyCast(from: PhysicalType, to: PhysicalType): boolean {
  if (sameBase(from, to)) return true;
  const source = sqlTypeOf(from);
  const target = sqlTypeOf(to);
  return source !== null && target !== null && (IMPLICIT_CASTS[source] ?? []).includes(target);
}

const NUMERIC_KINDS = new Set(["int32", "int64", "float32", "float64", "decimal"]);

/** PostgreSQL's assignment context: implicit casts, numeric narrowing, anything to text. */
function canAssign(from: PhysicalType, to: PhysicalType): boolean {
  if (canImplicitlyCast(from, to)) return true;
  if (NUMERIC_KINDS.has(from.kind) && NUMERIC_KINDS.has(to.kind)) return true;
  if (to.kind === "text") return true;
  if (from.kind === "timestamp" && (to.kind === "date" || to.kind === "time")) return true;
  if (from.kind === "json" && to.kind === "json") return true;
  return false;
}

export type CoercionContext = "implicit" | "assignment" | "explicit";

function typeName(type: PhysicalType): string {
  return sqlTypeOf(type) ?? type.kind;
}

/**
 * A pending constant as a constant of `type`: folded when lowering knows the value exactly, else
 * cast by the executor in `context` (an assignment never truncates: 22001, review finding 4).
 */
function constantOf(text: string, type: PhysicalType, context: CoercionContext): ScalarExpr {
  const assignment = context === "assignment";
  switch (type.kind) {
    case "json":
      if (!type.binary) {
        // Syntax validation only; retain duplicates, number spellings and whitespace.
        try {
          JSON.parse(text);
        } catch {
          throw new PlanningError("invalid input syntax for type json", "22P02");
        }
        return textLiteral(text, type);
      }
      return castTo(textLiteral(text), type, assignment);
    case "text":
      return type.maxLength === undefined
        ? textLiteral(text, type)
        : castTo(textLiteral(text), type, assignment);
    case "bool": {
      const word = text.trim().toLowerCase();
      if (["t", "true", "y", "yes", "on", "1"].includes(word)) return boolLiteral(true);
      if (["f", "false", "n", "no", "off", "0"].includes(word)) return boolLiteral(false);
      throw new PlanningError(`invalid input syntax for type boolean: "${text}"`, "22P02");
    }
    case "int32":
    case "int64": {
      if (!/^\s*[+-]?\d+\s*$/u.test(text))
        throw new PlanningError(
          `invalid input syntax for type ${typeName(type)}: "${text}"`,
          "22P02",
        );
      const literal = integerLiteral(BigInt(text.trim()), type);
      if (literal === null)
        throw new PlanningError(
          `value "${text}" is out of range for type ${typeName(type)}`,
          "22003",
        );
      return literal;
    }
    case "decimal": {
      const parsed = parseDecimalText(text);
      // NaN / Infinity and declared scales go through the executor's cast.
      if (parsed === null || type.scale !== null)
        return castTo(textLiteral(text), type, assignment);
      return decimalLiteral(parsed.coefficient, parsed.scale, type);
    }
    default:
      return castTo(textLiteral(text), type, assignment);
  }
}

function castTo(expr: ScalarExpr, type: PhysicalType, assignment = false): ScalarExpr {
  return assignment
    ? { op: "cast", input: expr, type, assignment: true }
    : { op: "cast", input: expr, type };
}

/**
 * `typed` as a value of exactly `type`, or a `PlanningError` (42804) when the context does not
 * allow the cast. Pending constants become constants of `type`; a pending placeholder takes
 * `type` (its first typing position, as in PostgreSQL). Integer constants are folded when they
 * fit, so keys stay constants.
 */
export function coerce(
  typed: Typed,
  type: PhysicalType,
  context: CoercionContext,
  parameters: ParameterTypes,
): Typed {
  const pending = typed.pending;
  const int64 = type.kind === "int64";
  if (
    pending === undefined &&
    type.kind === "json" &&
    !type.binary &&
    typed.expr.op === "literal" &&
    typed.expr.type.kind === "text" &&
    typed.expr.value.validity === null &&
    typed.expr.value.data.layout === "varlen"
  ) {
    const data = typed.expr.value.data;
    const text = new TextDecoder().decode(
      data.bytes.subarray(data.offsets[0] ?? 0, data.offsets[1] ?? 0),
    );
    return { expr: constantOf(text, type, context), nullable: typed.nullable, big: false };
  }
  // R17.1: a bigint parameter or an explicit CAST AS BIGINT renders bigint; an implicit
  // widening keeps the operand's rendering; nothing else does.
  if (pending?.kind === "null") return { expr: nullLiteral(type), nullable: true, big: false };
  if (pending?.kind === "text")
    return {
      expr: constantOf(pending.text, type, context),
      nullable: false,
      big: int64 && context === "explicit",
    };
  if (pending?.kind === "param") {
    const fixed = parameters.fix(pending.index, type);
    const param: Typed = {
      expr: { op: "param", slot: pending.index, type: fixed },
      nullable: true,
      big: fixed.kind === "int64",
    };
    return samePhysicalType(fixed, type) ? param : coerce(param, type, context, parameters);
  }
  const from = typed.expr.type;
  if (samePhysicalType(from, type))
    // R17.1: CAST AS BIGINT renders bigint even over an int8 (`count(*)::bigint`, finding 8).
    return int64 && context === "explicit" && !typed.big ? { ...typed, big: true } : typed;
  // Dropping a modifier for a comparison is no cast: a VARCHAR(40) value is a text value.
  if (context === "implicit" && sameBase(from, type) && samePhysicalType(type, baseType(type)))
    return typed;
  const allowed =
    context === "explicit"
      ? explicitCastAllowed(from, type)
      : context === "assignment"
        ? canAssign(from, type)
        : canImplicitlyCast(from, type);
  if (!allowed && context === "explicit")
    throw new PlanningError(`cannot cast type ${typeName(from)} to ${typeName(type)}`, "42846");
  if (!allowed)
    throw new PlanningError(
      `${context === "assignment" ? "column is of type" : "cannot use"} ${typeName(type)} but expression is of type ${typeName(from)}`,
      "42804",
    );
  const big = int64 && (typed.big || context === "explicit");
  // Fold an integer constant into an integer, numeric or float type it fits exactly.
  const value = integerLiteralValue(typed.expr);
  if (value !== null) {
    if (
      type.kind === "int64" ||
      type.kind === "int32" ||
      (type.kind === "decimal" && type.scale === null)
    ) {
      const folded = integerLiteral(value, type);
      if (folded !== null) return { expr: folded, nullable: false, big };
    } else if (
      (type.kind === "float64" || type.kind === "float32") &&
      Number.isSafeInteger(Number(value))
    )
      return { expr: floatLiteral(Number(value), type), nullable: false, big: false };
  }
  return {
    expr: castTo(typed.expr, type, context === "assignment"),
    nullable: typed.nullable,
    big,
  };
}

/** A pending value with no context left: text, or the placeholder's fixed type. */
export function settle(typed: Typed, parameters: ParameterTypes): Typed {
  if (typed.pending === undefined) return typed;
  if (typed.pending.kind === "param") {
    const type = parameters.type(typed.pending.index) ?? { kind: "text" };
    return coerce(typed, type, "implicit", parameters);
  }
  return coerce(typed, { kind: "text" }, "implicit", parameters);
}

/**
 * The common type of several values (PostgreSQL's UNION / CASE resolution): pending values
 * adopt it; all pending is text. Throws 42804 when two types cannot be matched.
 */
export function commonType(
  values: readonly Typed[],
  parameters: ParameterTypes,
  what: string,
): PhysicalType {
  let candidate: PhysicalType | null = null;
  for (const value of values) {
    if (value.pending !== undefined) {
      if (value.pending.kind === "param") {
        const fixed = parameters.type(value.pending.index);
        if (fixed === null) continue;
        candidate ??= fixed;
      }
      continue;
    }
    const type = value.expr.type;
    if (candidate === null) {
      candidate = type;
      continue;
    }
    if (samePhysicalType(candidate, type)) continue;
    if (sameBase(candidate, type)) {
      candidate = baseType(type);
      continue;
    }
    const up = canImplicitlyCast(candidate, type);
    const down = canImplicitlyCast(type, candidate);
    if (up && !down) candidate = type;
    else if (!down)
      throw new PlanningError(
        `${what} types ${typeName(candidate)} and ${typeName(type)} cannot be matched`,
        "42804",
      );
  }
  return candidate ?? { kind: "text" };
}

export interface Resolved {
  readonly fn: string;
  /** The overload's argument types, one per argument (variadic expanded). */
  readonly args: readonly PhysicalType[];
  readonly result: PhysicalType;
}

function argSql(value: Typed, parameters: ParameterTypes): SqlType | "unknown" | null {
  if (value.pending !== undefined) {
    if (value.pending.kind !== "param") return "unknown";
    const fixed = parameters.type(value.pending.index);
    return fixed === null ? "unknown" : sqlTypeOf(fixed);
  }
  return sqlTypeOf(value.expr.type);
}

function expand(overload: Overload, arity: number): ReadonlyArray<SqlType | "T" | "any"> | null {
  if (overload.variadic === true) {
    if (arity < overload.args.length) return null;
    const last = overload.args.at(-1) ?? "T";
    return [
      ...overload.args,
      ...Array<SqlType | "T" | "any">(arity - overload.args.length).fill(last),
    ];
  }
  return overload.args.length === arity ? overload.args : null;
}

/** PostgreSQL's type categories (`pg_type.typcategory`) of the table's types. */
function categoryOf(type: SqlType): string {
  switch (type) {
    case "int2":
    case "int4":
    case "int8":
    case "float4":
    case "float8":
    case "numeric":
      return "N";
    case "text":
      return "S";
    case "date":
    case "time":
    case "timestamp":
    case "timestamptz":
      return "D";
    case "interval":
      return "T";
    case "bool":
      return "B";
    default:
      return type;
  }
}

interface Candidate {
  readonly overload: Overload;
  readonly params: readonly SqlType[];
}

/**
 * Chooses the overload of `fn` for `args` by PostgreSQL's func_select_candidate over the table's
 * types (typeconv-func.html): an exact match; for a binary call with one unknown argument, the
 * overload taking the known type twice; else among the candidates reachable by implicit casts,
 * the most exact matches, then the most preferred types where a cast is needed, then for each
 * unknown argument the string category if a candidate accepts it, else the one category every
 * candidate accepts; finally, when every known argument has one type, the candidate taking that
 * type at the unknown positions. Throws 42883 when nothing fits and 42725 when more than one
 * candidate is left (`'1' + '2'`, as PostgreSQL).
 */
export function resolveFunction(
  fn: string,
  args: readonly Typed[],
  parameters: ParameterTypes,
): Resolved {
  const overloads = FUNCTIONS[fn];
  if (overloads === undefined) throw new PlanningError(`function ${fn} does not exist`, "42883");
  const kinds = args.map((arg) => argSql(arg, parameters));
  const expanded = overloads.flatMap((overload) => {
    const params = expand(overload, args.length);
    return params === null ? [] : [{ overload, params }];
  });
  const heterogeneous = expanded.find(
    ({ params }) => params.length > 0 && params.every((param) => param === "any"),
  );
  if (heterogeneous !== undefined) {
    if (heterogeneous.overload.unknown === "error" && kinds.includes("unknown"))
      throw new PlanningError(
        "could not determine polymorphic type because input has type unknown",
        "42804",
      );
    const types = args.map((arg) => settle(arg, parameters).expr.type);
    const binary = (
      heterogeneous.overload.jsonResult === "values"
        ? types.filter((_, i) => i % 2 === 1)
        : heterogeneous.overload.jsonResult === "arguments"
          ? types
          : []
    ).some((type) => type.kind === "json" && type.binary);
    return {
      fn,
      args: types,
      result: binary
        ? { kind: "json", binary: true }
        : physicalTypeOfSql(heterogeneous.overload.result as SqlType),
    };
  }
  // A type-variable overload (coalesce, greatest…) takes the arguments' common type. A
  // zero-argument overload is an ordinary exact match (review finding 5).
  const generic = expanded.find(
    ({ params }) => params.length > 0 && params.every((param) => param === "T"),
  );
  if (generic !== undefined) {
    const type = commonType(args, parameters, fn);
    return { fn, args: args.map(() => type), result: type };
  }
  if (kinds.includes(null))
    throw new PlanningError(`function ${fn} does not accept these argument types`, "42883");
  const candidates: Candidate[] = expanded.flatMap(({ overload, params }) =>
    params.includes("T") || params.includes("any")
      ? []
      : [{ overload, params: params as readonly SqlType[] }],
  );
  const resolved = (chosen: Candidate): Resolved => ({
    fn,
    args: chosen.params.map((param) => physicalTypeOfSql(param)),
    result: physicalTypeOfSql(chosen.overload.result as SqlType),
  });
  const exact = candidates.find((candidate) =>
    candidate.params.every((param, index) => kinds[index] === param),
  );
  if (exact !== undefined) return resolved(exact);
  const known = kinds.filter((kind): kind is SqlType => kind !== "unknown" && kind !== null);
  if (args.length === 2 && known.length === 1) {
    const twice = candidates.find((candidate) =>
      candidate.params.every((param) => param === known[0]),
    );
    if (twice !== undefined) return resolved(twice);
  }
  let viable = candidates.filter((candidate) =>
    candidate.params.every((param, index) => {
      const kind = kinds[index];
      return (
        kind === "unknown" ||
        kind === param ||
        (kind !== null && kind !== undefined && (IMPLICIT_CASTS[kind] ?? []).includes(param))
      );
    }),
  );
  if (viable.length === 0)
    throw new PlanningError(`function ${fn}(${kinds.join(", ")}) does not exist`, "42883");
  const keepMost = (score: (candidate: Candidate) => number): void => {
    const best = Math.max(...viable.map(score));
    viable = viable.filter((candidate) => score(candidate) === best);
  };
  keepMost((candidate) => candidate.params.filter((param, index) => kinds[index] === param).length);
  keepMost(
    (candidate) =>
      candidate.params.filter(
        (param, index) => kinds[index] !== param && PREFERRED_TYPES.has(param),
      ).length,
  );
  for (const [index, kind] of kinds.entries()) {
    if (kind !== "unknown" || viable.length <= 1) continue;
    const categories = new Set(
      viable.map((candidate) => categoryOf(candidate.params[index] ?? "text")),
    );
    const chosen = categories.has("S")
      ? "S"
      : categories.size === 1
        ? [...categories][0]
        : undefined;
    if (chosen === undefined) break;
    viable = viable.filter((candidate) => categoryOf(candidate.params[index] ?? "text") === chosen);
    const preferred = viable.filter((candidate) =>
      PREFERRED_TYPES.has(candidate.params[index] ?? "text"),
    );
    if (preferred.length > 0) viable = preferred;
  }
  if (viable.length > 1 && known.length > 0 && known.every((kind) => kind === known[0])) {
    const same = viable.filter((candidate) =>
      candidate.params.every((param, index) => kinds[index] !== "unknown" || param === known[0]),
    );
    if (same.length === 1) viable = same;
  }
  const [only] = viable;
  if (only === undefined || viable.length > 1)
    throw new PlanningError(`function ${fn}(${kinds.join(", ")}) is not unique`, "42725");
  return resolved(only);
}
