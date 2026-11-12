/**
 * The scalar functions and operators of the IR: every `call.fn` lowering may emit, with its
 * overloads (additive, ruling R17.5). Lowering resolves a SQL call against this table
 * (PostgreSQL's func_select_candidate: lower/coerce.ts `resolveFunction`) and emits the chosen
 * overload's argument types exactly, inserting `cast` nodes. The executor implements a kernel per
 * overload and refuses any other (`UnsupportedPlanError`, stage "execute"). The table is data so
 * both sides read one list; growing it is additive. Ruling R19: this file is the one authority for
 * names, overloads, casts (implicit and explicit), strictness, volatility and preferred types;
 * the executor binds kernels to its entries.
 *
 * ## Semantics shared by every entry
 * - **Strict** unless listed in `NON_STRICT_FUNCTIONS`: any NULL argument gives NULL, without
 *   evaluating the kernel (so `1 / NULL` is NULL, not an error).
 * - **Errors are PostgreSQL's**: integer overflow 22003, division by zero 22012, invalid input
 *   22P02, value out of range 22008 for dates, and so on, at the type of the chosen overload
 *   (`int2 + int2` overflows at 32,767).
 * - **Volatile** functions (`VOLATILE_FUNCTIONS`) are evaluated once per row, in input order,
 *   and make the plan `volatile` (ir.ts `PhysicalPlan.volatile`).
 * - **Type variables.** `"T"` in an overload is one type shared by every `"T"` argument and the
 *   result (any type, including enums and arrays); lowering picks it as PostgreSQL picks a common
 *   type (UNION / CASE resolution) and casts each argument to it.
 * - **Heterogeneous arguments.** `"any"` keeps each input type independently (PostgreSQL's
 *   VARIADIC "any"); it never unifies JSON constructor members. Unknowns settle to text
 *   unless the overload requires a known polymorphic input (`unknown: "error"`).
 * - Comparison, boolean logic, NULL tests, CASE, IN lists and LIKE are IR nodes, not calls.
 *   `compare` and `inList` take operands of one base type (lowering casts to the common type);
 *   type modifiers may differ and are ignored: a VARCHAR(2) column compares with a `text`
 *   constant by value, never by truncating the constant (review B6-p2a finding 1), and
 *   NUMERIC(10, 2) with unconstrained NUMERIC by value. A `keyLookup` or `indexRange` key may
 *   likewise differ from its column only in modifiers. So no comparison overloads are listed.
 *
 * ## Casts (`cast` nodes)
 * Exactly the pairs `explicitCastAllowed` accepts; any other explicit CAST is 42846 at lowering
 * (`CAST(true AS date)`, `CAST(1 AS date)`, `CAST(1::int8 AS bool)`, as PostgreSQL). Semantics
 * are PostgreSQL's (review finding 9):
 * - float4/float8 → integer rounds to nearest, **ties to even** (`rint`: 2.5 → 2, 3.5 → 4,
 *   −2.5 → −2); numeric → integer and numeric → numeric(p, s) round half away from zero
 *   (2.5 → 3); out of range is 22003; text is parsed by the type's input function (22P02);
 * - `CAST` to VARCHAR(n) / CHAR(n) truncates; `cast.assignment` raises 22001 instead (ir.ts).
 * Implicit casts (applied without a CAST in the SQL) are `IMPLICIT_CASTS`; assignment casts
 * (values written to a column) additionally narrow among numeric types and accept any type into
 * text.
 */
import type { AggregateFn } from "./ir.js";
import { samePhysicalType, type PhysicalType } from "../types.js";

/** The SQL type names the table uses (PostgreSQL's), one per physical type family. */
export type SqlType =
  | "bool"
  | "int2"
  | "int4"
  | "int8"
  | "float4"
  | "float8"
  | "numeric"
  | "text"
  | "date"
  | "time"
  | "timestamp"
  | "timestamptz"
  | "interval"
  | "json"
  | "jsonb"
  | "uuid"
  | "bytea";

export interface Overload {
  readonly args: ReadonlyArray<SqlType | "T" | "any">;
  readonly result: SqlType | "T";
  /** The last argument repeats (`coalesce(T, T, …)`): one or more. */
  readonly variadic?: true;
  /** SQL/JSON defaults to JSONB if a value argument is JSONB; build_* stays fixed. */
  readonly jsonResult?: "values" | "arguments";
  /** Polymorphic single-value conversion cannot infer an unknown input. */
  readonly unknown?: "error";
}

const numericTypes = ["int2", "int4", "int8", "float4", "float8", "numeric"] as const;
const same = (types: readonly SqlType[], arity: number): Overload[] =>
  types.map((type) => ({ args: Array<SqlType>(arity).fill(type), result: type }));

/** `fn` → overloads. Names are PostgreSQL's function names, or the operator's name below. */
export const FUNCTIONS: Readonly<Record<string, readonly Overload[]>> = {
  // PostgreSQL VARIADIC "any": independent argument types, including zero arguments.
  json_object: [
    { args: [], result: "json" },
    { args: ["any"], result: "json", variadic: true, jsonResult: "values" },
  ],
  jsonb_build_object: [
    { args: [], result: "jsonb" },
    { args: ["any"], result: "jsonb", variadic: true },
  ],
  jsonb_build_array: [
    { args: [], result: "jsonb" },
    { args: ["any"], result: "jsonb", variadic: true },
  ],
  to_jsonb: [{ args: ["any"], result: "jsonb", unknown: "error" }],
  json_build_object: [
    { args: [], result: "json" },
    { args: ["any"], result: "json", variadic: true },
  ],
  json_build_array: [
    { args: [], result: "json" },
    { args: ["any"], result: "json", variadic: true },
  ],
  json_array: [
    { args: [], result: "json" },
    { args: ["any"], result: "json", variadic: true, jsonResult: "arguments" },
  ],
  to_json: [{ args: ["any"], result: "json", unknown: "error" }],
  // SRFs bind through this table too; execution uses a relational series node.
  generate_series: [
    { args: ["int4", "int4"], result: "int4" },
    { args: ["int8", "int8"], result: "int8" },
    { args: ["numeric", "numeric"], result: "numeric" },
    { args: ["int4", "int4", "int4"], result: "int4" },
    { args: ["int8", "int8", "int8"], result: "int8" },
    { args: ["numeric", "numeric", "numeric"], result: "numeric" },
    { args: ["timestamp", "timestamp", "interval"], result: "timestamp" },
    { args: ["timestamptz", "timestamptz", "interval"], result: "timestamptz" },
  ],
  // Operators: `+ - * / %`, unary `-`, and `||`.
  json_get: [
    { args: ["json", "text"], result: "json" },
    { args: ["json", "int4"], result: "json" },
    { args: ["jsonb", "text"], result: "jsonb" },
    { args: ["jsonb", "int4"], result: "jsonb" },
  ],
  json_get_text: [
    { args: ["json", "text"], result: "text" },
    { args: ["json", "int4"], result: "text" },
    { args: ["jsonb", "text"], result: "text" },
    { args: ["jsonb", "int4"], result: "text" },
  ],
  add: [
    ...same(numericTypes, 2),
    { args: ["date", "int4"], result: "date" },
    { args: ["int4", "date"], result: "date" },
    { args: ["date", "interval"], result: "timestamp" },
    { args: ["timestamp", "interval"], result: "timestamp" },
    { args: ["timestamptz", "interval"], result: "timestamptz" },
    { args: ["time", "interval"], result: "time" },
    { args: ["interval", "interval"], result: "interval" },
  ],
  sub: [
    ...same(numericTypes, 2),
    { args: ["date", "date"], result: "int4" },
    { args: ["date", "int4"], result: "date" },
    { args: ["date", "interval"], result: "timestamp" },
    { args: ["timestamp", "timestamp"], result: "interval" },
    { args: ["timestamptz", "timestamptz"], result: "interval" },
    { args: ["timestamp", "interval"], result: "timestamp" },
    { args: ["timestamptz", "interval"], result: "timestamptz" },
    { args: ["time", "time"], result: "interval" },
    { args: ["interval", "interval"], result: "interval" },
  ],
  mul: [
    ...same(numericTypes, 2),
    { args: ["interval", "float8"], result: "interval" },
    { args: ["float8", "interval"], result: "interval" },
  ],
  /** Integer division truncates toward zero (PostgreSQL's int `/`). */
  div: [...same(numericTypes, 2), { args: ["interval", "float8"], result: "interval" }],
  mod: same(["int2", "int4", "int8", "numeric"], 2),
  neg: [...same(numericTypes, 1), { args: ["interval"], result: "interval" }],
  /** `||` on text (PostgreSQL's textcat); a non-text operand is cast to text first. */
  textcat: [{ args: ["text", "text"], result: "text" }],

  // Mathematical functions.
  abs: same(numericTypes, 1),
  round: [
    { args: ["float8"], result: "float8" },
    { args: ["numeric"], result: "numeric" },
    { args: ["numeric", "int4"], result: "numeric" },
  ],
  trunc: [
    { args: ["float8"], result: "float8" },
    { args: ["numeric"], result: "numeric" },
    { args: ["numeric", "int4"], result: "numeric" },
  ],
  ceil: same(["float8", "numeric"], 1),
  floor: same(["float8", "numeric"], 1),
  sign: same(["float8", "numeric"], 1),
  sqrt: same(["float8", "numeric"], 1),
  exp: same(["float8", "numeric"], 1),
  ln: same(["float8", "numeric"], 1),
  power: same(["float8", "numeric"], 2),

  // String functions.
  lower: [{ args: ["text"], result: "text" }],
  upper: [{ args: ["text"], result: "text" }],
  /** Characters (code points), not bytes. */
  length: [{ args: ["text"], result: "int4" }],
  octet_length: [{ args: ["text"], result: "int4" }],
  substr: [
    { args: ["text", "int4"], result: "text" },
    { args: ["text", "int4", "int4"], result: "text" },
  ],
  btrim: [
    { args: ["text"], result: "text" },
    { args: ["text", "text"], result: "text" },
  ],
  ltrim: [
    { args: ["text"], result: "text" },
    { args: ["text", "text"], result: "text" },
  ],
  rtrim: [
    { args: ["text"], result: "text" },
    { args: ["text", "text"], result: "text" },
  ],
  replace: [{ args: ["text", "text", "text"], result: "text" }],
  strpos: [{ args: ["text", "text"], result: "int4" }],
  left: [{ args: ["text", "int4"], result: "text" }],
  right: [{ args: ["text", "int4"], result: "text" }],
  starts_with: [{ args: ["text", "text"], result: "bool" }],

  // Conditional expressions (non-strict, see `NON_STRICT_FUNCTIONS`).
  coalesce: [{ args: ["T"], result: "T", variadic: true }],
  nullif: [{ args: ["T", "T"], result: "T" }],
  greatest: [{ args: ["T"], result: "T", variadic: true }],
  least: [{ args: ["T"], result: "T", variadic: true }],

  // Date and time. The unit or field is a text constant, as in PostgreSQL.
  date_trunc: [
    { args: ["text", "timestamp"], result: "timestamp" },
    { args: ["text", "timestamptz"], result: "timestamptz" },
    { args: ["text", "interval"], result: "interval" },
  ],
  /** EXTRACT(field FROM x): numeric, as PostgreSQL 14+ returns it. */
  extract: [
    { args: ["text", "date"], result: "numeric" },
    { args: ["text", "time"], result: "numeric" },
    { args: ["text", "timestamp"], result: "numeric" },
    { args: ["text", "timestamptz"], result: "numeric" },
    { args: ["text", "interval"], result: "numeric" },
  ],

  /** DATE_PART keeps PostgreSQL's float8 result, separately from EXTRACT's NUMERIC. */
  date_part: [
    { args: ["text", "date"], result: "float8" },
    { args: ["text", "time"], result: "float8" },
    { args: ["text", "timestamp"], result: "float8" },
    { args: ["text", "timestamptz"], result: "float8" },
    { args: ["text", "interval"], result: "float8" },
  ],

  // Sequences: the sequence id is an int8 constant (access/catalog.ts `SequenceDef.id`).
  nextval: [{ args: ["int8"], result: "int8" }],
  currval: [{ args: ["int8"], result: "int8" }],

  // Volatile generators.
  random: [{ args: [], result: "float8" }],
  gen_random_uuid: [{ args: [], result: "uuid" }],
};

/**
 * Functions that see NULL arguments: `coalesce` (first non-null), `greatest` / `least` (ignore
 * nulls; NULL only when every argument is), `nullif` (NULL first argument gives NULL; NULL
 * second gives the first). `nullif`'s result type is PostgreSQL's: the left operand type of the
 * resolved `=`, which is the first argument's type when both are integers or both floats (cross-
 * type `=` exists) and their common type otherwise; lowering casts back to the first argument's
 * type in the integer and float cases, so the call itself is always `nullif(T, T)`.
 */
export const NON_STRICT_FUNCTIONS: ReadonlySet<string> = new Set([
  "json_build_object",
  "jsonb_build_object",
  "json_object",
  "json_build_array",
  "jsonb_build_array",
  "json_array",
  "coalesce",
  "greatest",
  "least",
  "nullif",
]);

/** Evaluated once per row in input order; the plan is `volatile`. */
export const VOLATILE_FUNCTIONS: ReadonlySet<string> = new Set([
  "nextval",
  "random",
  "gen_random_uuid",
]);

/**
 * PostgreSQL's implicit casts among the table's types (`pg_cast` with castcontext `i`), as
 * from → to. Lowering inserts them to match an overload or a common type; anything else needs an
 * explicit CAST in the SQL.
 */
export const IMPLICIT_CASTS: Readonly<Partial<Record<SqlType, readonly SqlType[]>>> = {
  int2: ["int4", "int8", "numeric", "float4", "float8"],
  int4: ["int8", "numeric", "float4", "float8"],
  int8: ["numeric", "float4", "float8"],
  numeric: ["float4", "float8"],
  float4: ["float8"],
  date: ["timestamp", "timestamptz"],
  timestamp: ["timestamptz"],
  time: ["interval"],
  json: ["jsonb"],
};

const NUMERIC_TYPES: readonly SqlType[] = ["int2", "int4", "int8", "float4", "float8", "numeric"];

/** Explicit casts beyond numeric ↔ numeric and anything ↔ text (`pg_cast` among these types). */
export const EXPLICIT_CASTS: Readonly<Partial<Record<SqlType, readonly SqlType[]>>> = {
  bool: ["int4"],
  int4: ["bool"],
  date: ["timestamp", "timestamptz"],
  timestamp: ["date", "time", "timestamptz"],
  timestamptz: ["date", "time", "timestamp"],
  time: ["interval"],
  interval: ["time"],
  json: ["jsonb"],
  jsonb: ["json"],
};

/**
 * Whether `CAST(from AS to)` exists: the same base type, numeric ↔ numeric, to or from text
 * (PostgreSQL's I/O conversion casts), the pairs in `EXPLICIT_CASTS`, enums ↔ text, and arrays
 * whose elements cast. Anything else is 42846.
 */
export function explicitCastAllowed(from: PhysicalType, to: PhysicalType): boolean {
  if (from.kind === to.kind && from.kind !== "array") return true;
  if (from.kind === "text" || to.kind === "text") return true;
  if (from.kind === "array" && to.kind === "array")
    return explicitCastAllowed(from.element, to.element);
  const source = sqlTypeOf(from);
  const target = sqlTypeOf(to);
  if (source === null || target === null) return false;
  if (NUMERIC_TYPES.includes(source) && NUMERIC_TYPES.includes(target)) return true;
  return (EXPLICIT_CASTS[source] ?? []).includes(target);
}

/**
 * The preferred type of each category (`pg_type.typispreferred`), which wins ties in overload
 * resolution and common-type selection.
 */
export const PREFERRED_TYPES: ReadonlySet<SqlType> = new Set([
  "float8",
  "text",
  "timestamptz",
  "interval",
  "bool",
]);

/** The SQL type of a physical type, or null for types the table names only through `"T"`. */
export function sqlTypeOf(type: PhysicalType): SqlType | null {
  switch (type.kind) {
    case "bool":
      return "bool";
    case "int32":
      return type.bits === 16 ? "int2" : "int4";
    case "int64":
      return "int8";
    case "float32":
      return "float4";
    case "float64":
      return "float8";
    case "decimal":
      return "numeric";
    case "text":
      return "text";
    case "date":
      return "date";
    case "time":
      return "time";
    case "timestamp":
      return type.zoned ? "timestamptz" : "timestamp";
    case "interval":
      return "interval";
    case "json":
      return type.binary ? "jsonb" : "json";
    case "uuid":
      return "uuid";
    case "bytes":
      return "bytea";
    case "enum":
    case "array":
    case "locator":
      return null;
  }
}

/** The physical type an overload's SQL type stands for (numeric unconstrained, text unbounded). */
export function physicalTypeOfSql(type: SqlType): PhysicalType {
  switch (type) {
    case "bool":
      return { kind: "bool" };
    case "int2":
      return { kind: "int32", bits: 16 };
    case "int4":
      return { kind: "int32", bits: 32 };
    case "int8":
      return { kind: "int64" };
    case "float4":
      return { kind: "float32" };
    case "float8":
      return { kind: "float64" };
    case "numeric":
      return { kind: "decimal", precision: null, scale: null };
    case "text":
      return { kind: "text" };
    case "date":
      return { kind: "date" };
    case "time":
      return { kind: "time" };
    case "timestamp":
      return { kind: "timestamp", zoned: false };
    case "timestamptz":
      return { kind: "timestamp", zoned: true };
    case "interval":
      return { kind: "interval" };
    case "json":
      return { kind: "json", binary: false };
    case "jsonb":
      return { kind: "json", binary: true };
    case "uuid":
      return { kind: "uuid" };
    case "bytea":
      return { kind: "bytes" };
  }
}

/** An aggregate's argument pattern: a SQL type, `"T"` (any type), or T bound to an enum/array. */
export type AggregateArg = SqlType | "T" | "anyenum" | "anyarray";

/** One overload of an aggregate (ir.ts `AggregateFn`). */
export interface AggregateOverload {
  readonly args: readonly AggregateArg[];
  /** Ordered-set aggregates: the type of the one WITHIN GROUP (ORDER BY …) key. */
  readonly within?: AggregateArg;
  /**
   * The result: a SQL type, T, an array of T, or an exact physical type where the IR fixes a
   * modifier (`sum(int8)` is numeric with scale 0, R17.2).
   */
  readonly result: SqlType | "T" | "T[]" | PhysicalType;
}

/** The types `min` and `max` order (PostgreSQL's btree opclasses with a min/max aggregate). */
const MIN_MAX_TYPES: readonly SqlType[] = [
  ...numericTypes,
  "text",
  "date",
  "time",
  "timestamp",
  "timestamptz",
  "interval",
  "bytea",
];

/**
 * Every aggregate the IR names, with its overloads (ruling R19: the one authority, read by
 * lowering's typing and by the plan checker). PostgreSQL's signatures, except where a ruling
 * fixes Minnow's: `avg` of integers and floats is double precision (C11), and `sum(int8)` is
 * numeric with scale 0 (R17.2). There are no `min` / `max` over bool, json, jsonb or uuid, as
 * in PostgreSQL (42883).
 */
export const AGGREGATES: Readonly<Record<AggregateFn, readonly AggregateOverload[]>> = {
  count_star: [{ args: [], result: "int8" }],
  count: [{ args: ["T"], result: "int8" }],
  sum: [
    { args: ["int2"], result: "int8" },
    { args: ["int4"], result: "int8" },
    { args: ["int8"], result: { kind: "decimal", precision: null, scale: 0 } },
    { args: ["numeric"], result: "numeric" },
    { args: ["float4"], result: "float4" },
    { args: ["float8"], result: "float8" },
    { args: ["interval"], result: "interval" },
  ],
  avg: [
    ...(["int2", "int4", "int8", "float4", "float8"] as const).map((type) => ({
      args: [type],
      result: "float8" as const,
    })),
    { args: ["numeric"], result: "numeric" },
    { args: ["interval"], result: "interval" },
  ],
  min: [
    ...MIN_MAX_TYPES.map((type) => ({ args: [type], result: type })),
    { args: ["anyenum"], result: "T" },
    { args: ["anyarray"], result: "T" },
  ],
  max: [
    ...MIN_MAX_TYPES.map((type) => ({ args: [type], result: type })),
    { args: ["anyenum"], result: "T" },
    { args: ["anyarray"], result: "T" },
  ],
  bool_and: [{ args: ["bool"], result: "bool" }],
  bool_or: [{ args: ["bool"], result: "bool" }],
  string_agg: [
    { args: ["text", "text"], result: "text" },
    { args: ["bytea", "bytea"], result: "bytea" },
  ],
  array_agg: [{ args: ["T"], result: "T[]" }],
  json_agg: [{ args: ["T"], result: "json" }],
  percentile_cont: [
    { args: ["float8"], within: "float8", result: "float8" },
    { args: ["float8"], within: "interval", result: "interval" },
  ],
  percentile_disc: [{ args: ["float8"], within: "T", result: "T" }],
  mode: [{ args: [], within: "T", result: "T" }],
  single_value: [{ args: ["T"], result: "T" }],
};

/** Base types for overload matching: numeric modifiers and text widths are ignored. */
function aggregateBase(type: PhysicalType): PhysicalType {
  if (type.kind === "decimal") return { kind: "decimal", precision: null, scale: null };
  if (type.kind === "text") return { kind: "text" };
  return type;
}

/**
 * The result type of `fn` over arguments (and a WITHIN GROUP key) of these types, by the
 * overload they match exactly (lowering casts to an overload's types first), or null when no
 * overload takes them.
 */
export function aggregateResult(
  fn: AggregateFn,
  args: readonly PhysicalType[],
  within?: PhysicalType,
): PhysicalType | null {
  const same = (a: PhysicalType, b: PhysicalType) =>
    samePhysicalType(aggregateBase(a), aggregateBase(b));
  for (const overload of AGGREGATES[fn]) {
    if (overload.args.length !== args.length) continue;
    if ((overload.within === undefined) !== (within === undefined)) continue;
    let bound: PhysicalType | undefined;
    const binds = (pattern: AggregateArg, actual: PhysicalType): boolean => {
      if (pattern === "T" || pattern === "anyenum" || pattern === "anyarray") {
        if (pattern === "anyenum" && actual.kind !== "enum") return false;
        if (pattern === "anyarray" && actual.kind !== "array") return false;
        bound ??= actual;
        return same(bound, actual);
      }
      return sqlTypeOf(actual) === pattern;
    };
    if (!overload.args.every((pattern, index) => binds(pattern, args[index] ?? { kind: "bool" })))
      continue;
    if (overload.within !== undefined && within !== undefined && !binds(overload.within, within))
      continue;
    const result = overload.result;
    if (result === "T") {
      if (bound !== undefined) return bound;
      continue;
    }
    if (result === "T[]") {
      if (bound !== undefined) return { kind: "array", element: bound };
      continue;
    }
    return typeof result === "string" ? physicalTypeOfSql(result) : result;
  }
  return null;
}

/**
 * The argument type a one-argument aggregate call resolves to, as PostgreSQL's
 * func_select_candidate does: the argument's own type when an overload takes it, else the one
 * overload argument it implicitly casts to (`sum(time)` is `sum(interval)`), else null (42883).
 * Several reachable overloads resolve to the preferred type of the argument's category, if one.
 */
export function aggregateArgument(fn: AggregateFn, arg: PhysicalType): PhysicalType | null {
  if (aggregateResult(fn, [arg]) !== null) return arg;
  const source = sqlTypeOf(arg);
  if (source === null) return null;
  const reachable = new Set(IMPLICIT_CASTS[source] ?? []);
  const targets = [
    ...new Set(
      AGGREGATES[fn].flatMap((overload) => {
        const [only] = overload.args;
        return overload.args.length === 1 &&
          overload.within === undefined &&
          only !== undefined &&
          only !== "T" &&
          only !== "anyenum" &&
          only !== "anyarray" &&
          reachable.has(only)
          ? [only]
          : [];
      }),
    ),
  ];
  const preferred = targets.filter((type) => PREFERRED_TYPES.has(type));
  const target =
    targets.length === 1 ? targets[0] : preferred.length === 1 ? preferred[0] : undefined;
  return target === undefined ? null : physicalTypeOfSql(target);
}
