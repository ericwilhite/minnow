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
import type { PhysicalType } from "../types.js";

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
  readonly args: ReadonlyArray<SqlType | "T">;
  readonly result: SqlType | "T";
  /** The last argument repeats (`coalesce(T, T, …)`): one or more. */
  readonly variadic?: true;
}

const numericTypes = ["int2", "int4", "int8", "float4", "float8", "numeric"] as const;
const same = (types: readonly SqlType[], arity: number): Overload[] =>
  types.map((type) => ({ args: Array<SqlType>(arity).fill(type), result: type }));

/** `fn` → overloads. Names are PostgreSQL's function names, or the operator's name below. */
export const FUNCTIONS: Readonly<Record<string, readonly Overload[]>> = {
  // Operators: `+ - * / %`, unary `-`, and `||`.
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
