/**
 * Kernel bindings onto ir/functions.ts (ruling R19): that table is the one authority for function
 * names, overloads and casts; this file says, for each of its entries, which kernel implements it
 * or that the executor refuses it, with a construct key. No second list of functions.
 *
 * - `FUNCTION_BINDINGS[fn]` is either one binding for every overload of `fn`, or one per overload,
 *   keyed by `overloadKey` (`"int4,int4"`, `"T…"` for a variadic `T`). bindings.test.ts fails when
 *   an entry of `FUNCTIONS` has no binding, when a binding names no entry, and when a kernel
 *   binding does not compile and evaluate.
 * - A kernel over a type variable `"T"` lists the physical kinds it takes (`kinds`); any other `T`
 *   is refused with the same construct key.
 * - Casts: every pair `explicitCastAllowed` accepts (between the table's SQL types) has a kernel
 *   in `compileCast` or a refusal in `CAST_REFUSALS`; the test checks both directions.
 * - A refusal is `UnsupportedPlanError` with `stage: "execute"` and the binding's `construct` key:
 *   `"function"` (lowering's key for an overload with no kernel, lower/errors.ts) or `"cast"`.
 */
import { FUNCTIONS, type Overload, type SqlType, sqlTypeOf } from "../../ir/functions.js";
import type { PhysicalKind, PhysicalType } from "../../types.js";
import { UnsupportedPlanError } from "../errors.js";

/** The call kernels compile.ts dispatches to. */
export type CallKernel =
  | "arith"
  | "neg"
  | "coalesce"
  | "nullif"
  | "numericRound"
  | "dateTrunc"
  | "temporalSub"
  | "calendarAdd"
  | "intervalExtract"
  | "json";

export type Binding =
  | {
      readonly kernel: CallKernel;
      /** For overloads over `"T"`: the physical kinds the kernel takes (others are refused). */
      readonly kinds?: ReadonlySet<PhysicalKind>;
    }
  | { readonly refused: string; readonly construct: "function" };

/** One binding per overload of a function, keyed by `overloadKey`. */
export interface PerOverload {
  readonly overloads: Readonly<Record<string, Binding>>;
}
const per = (overloads: Record<string, Binding>): PerOverload => ({ overloads });

const refuse = (reason: string): Binding => ({ refused: reason, construct: "function" });

/** The key of an overload in a per-overload binding map. */
export function overloadKey(overload: Overload): string {
  return overload.args.join(",") + (overload.variadic === true ? "…" : "");
}

const NUMERIC: readonly SqlType[] = ["int2", "int4", "int8", "float4", "float8", "numeric"];
const each = (
  types: readonly SqlType[],
  arity: number,
  binding: Binding,
): Record<string, Binding> =>
  Object.fromEntries(types.map((type) => [Array<SqlType>(arity).fill(type).join(","), binding]));

const ARITH: Binding = { kernel: "arith" };
const DATETIME = refuse("date, time and interval arithmetic (temporal kernels come later)");
const INTERVAL = refuse("interval arithmetic (temporal kernels come later)");

/** Kinds `compare`, `distinctFrom`, `inList` and `nullif` take. */
export const COMPARABLE_KINDS: ReadonlySet<PhysicalKind> = new Set<PhysicalKind>([
  "bool",
  "int32",
  "int64",
  "float64",
  "float32",
  "decimal",
  "date",
  "time",
  "timestamp",
  "text",
  "bytes",
  "uuid",
]);

/** Kinds CASE and COALESCE can produce. */
export const CASE_RESULT_KINDS: ReadonlySet<PhysicalKind> = new Set<PhysicalKind>([
  ...COMPARABLE_KINDS,
  "json",
  "enum",
  "interval",
]);

export const FUNCTION_BINDINGS: Readonly<Record<string, Binding | PerOverload>> = {
  json_get: per({
    "json,text": { kernel: "json" },
    "json,int4": { kernel: "json" },
    "jsonb,text": refuse("JSONB canonical extraction"),
    "jsonb,int4": refuse("JSONB canonical extraction"),
  }),
  json_get_text: per({
    "json,text": { kernel: "json" },
    "json,int4": { kernel: "json" },
    "jsonb,text": refuse("JSONB canonical extraction"),
    "jsonb,int4": refuse("JSONB canonical extraction"),
  }),
  generate_series: refuse("set-returning GENERATE_SERIES requires its relational operator"),
  add: per({
    ...each(NUMERIC, 2, ARITH),
    "date,int4": DATETIME,
    "int4,date": DATETIME,
    "date,interval": { kernel: "calendarAdd" },
    "timestamp,interval": { kernel: "calendarAdd" },
    "timestamptz,interval": { kernel: "calendarAdd" },
    "time,interval": DATETIME,
    "interval,interval": INTERVAL,
  }),
  sub: per({
    ...each(NUMERIC, 2, ARITH),
    "date,date": { kernel: "temporalSub" },
    "date,int4": DATETIME,
    "date,interval": { kernel: "calendarAdd" },
    "timestamp,timestamp": { kernel: "temporalSub" },
    "timestamptz,timestamptz": { kernel: "temporalSub" },
    "timestamp,interval": { kernel: "calendarAdd" },
    "timestamptz,interval": { kernel: "calendarAdd" },
    "time,time": DATETIME,
    "interval,interval": INTERVAL,
  }),
  mul: per({
    ...each(NUMERIC, 2, ARITH),
    "interval,float8": INTERVAL,
    "float8,interval": INTERVAL,
  }),
  div: per({
    ...each(NUMERIC, 2, ARITH),
    "interval,float8": INTERVAL,
  }),
  mod: per({
    ...each(["int2", "int4", "int8", "numeric"], 2, ARITH),
  }),
  neg: per({ ...each(NUMERIC, 1, { kernel: "neg" }), interval: INTERVAL }),
  textcat: refuse("text concatenation (text kernels come next)"),

  abs: refuse("mathematical functions (come next)"),
  round: per({
    float8: refuse("float8 ROUND (comes next)"),
    numeric: refuse("one-argument numeric ROUND (comes next)"),
    "numeric,int4": { kernel: "numericRound" },
  }),
  trunc: refuse("mathematical functions (come next)"),
  ceil: refuse("mathematical functions (come next)"),
  floor: refuse("mathematical functions (come next)"),
  sign: refuse("mathematical functions (come next)"),
  sqrt: refuse("mathematical functions (come next)"),
  exp: refuse("mathematical functions (come next)"),
  ln: refuse("mathematical functions (come next)"),
  power: refuse("mathematical functions (come next)"),

  lower: refuse("string functions (text kernels come next)"),
  upper: refuse("string functions (text kernels come next)"),
  length: refuse("string functions (text kernels come next)"),
  octet_length: refuse("string functions (text kernels come next)"),
  substr: refuse("string functions (text kernels come next)"),
  btrim: refuse("string functions (text kernels come next)"),
  ltrim: refuse("string functions (text kernels come next)"),
  rtrim: refuse("string functions (text kernels come next)"),
  replace: refuse("string functions (text kernels come next)"),
  strpos: refuse("string functions (text kernels come next)"),
  left: refuse("string functions (text kernels come next)"),
  right: refuse("string functions (text kernels come next)"),
  starts_with: refuse("string functions (text kernels come next)"),

  coalesce: { kernel: "coalesce", kinds: CASE_RESULT_KINDS },
  nullif: { kernel: "nullif", kinds: COMPARABLE_KINDS },
  greatest: refuse("greatest (comes with the text kernels)"),
  least: refuse("least (comes with the text kernels)"),

  date_trunc: per({
    "text,timestamp": { kernel: "dateTrunc" },
    "text,timestamptz": { kernel: "dateTrunc" },
    "text,interval": refuse("interval DATE_TRUNC (calendar interval kernels come later)"),
  }),
  extract: per({
    "text,interval": { kernel: "intervalExtract" },
    ...Object.fromEntries(
      ["date", "time", "timestamp", "timestamptz"].map((type) => [
        `text,${type}`,
        refuse("date/time EXTRACT (calendar kernels come later)"),
      ]),
    ),
  }),

  date_part: per({
    "text,interval": { kernel: "intervalExtract" },
    ...Object.fromEntries(
      ["date", "time", "timestamp", "timestamptz"].map((type) => [
        `text,${type}`,
        refuse("date/time DATE_PART (calendar kernels come later)"),
      ]),
    ),
  }),

  nextval: refuse("sequences (come with DML)"),
  currval: refuse("sequences (come with DML)"),
  random: refuse("volatile generators (come with DML)"),
  gen_random_uuid: refuse("volatile generators (come with DML)"),
};

/**
 * Explicit casts (`explicitCastAllowed`) with no kernel yet, as "from→to" over SQL types; every
 * other allowed pair has one in cast.ts. `*` stands for any type of the table; implemented
 * temporal output casts are exceptions selected by castRefusal before its wildcard.
 */
export const CAST_REFUSALS: ReadonlyMap<string, string> = new Map([
  ["*→text", "casts to text (output functions come with the text kernels)"],
  ["text→*", "casts from text (input functions come with the text kernels)"],
  ["bool→int4", "bool ↔ int4 casts"],
  ["int4→bool", "bool ↔ int4 casts"],
  ["timestamp→time", "casts to time"],
  ["timestamptz→time", "casts to time"],
  ["time→interval", "time ↔ interval casts"],
  ["interval→time", "time ↔ interval casts"],
  ["json→jsonb", "json ↔ jsonb casts"],
  ["jsonb→json", "json ↔ jsonb casts"],
]);

/** Why the cast `from → to` is refused, or null when cast.ts implements it. */
export function castRefusal(from: SqlType, to: SqlType): string | null {
  if (from === to) return null;
  if ((from === "text" && to === "json") || (from === "json" && to === "text")) return null;
  if (to === "text")
    return from === "date" || from === "timestamp" || from === "timestamptz" || from === "interval"
      ? null
      : (CAST_REFUSALS.get("*→text") ?? null);
  if (from === "text") return CAST_REFUSALS.get("text→*") ?? null;
  return CAST_REFUSALS.get(`${from}→${to}`) ?? null;
}

/** The SQL type `type` stands for in an overload slot (`T` takes any). */
function matches(slot: SqlType | "T", type: PhysicalType): boolean {
  return slot === "T" || sqlTypeOf(type) === slot;
}

/**
 * The overload of `fn` the argument types select, and its binding. Throws `UnsupportedPlanError`
 * for a function or overload outside ir/functions.ts and for a refused binding.
 */
export function bindCall(
  fn: string,
  args: readonly PhysicalType[],
): Extract<Binding, { kernel: CallKernel }> {
  const overloads = FUNCTIONS[fn];
  const signature = `${fn}(${args.map((type) => sqlTypeOf(type) ?? type.kind).join(", ")})`;
  if (overloads === undefined)
    throw new UnsupportedPlanError(`the function ${fn}, which ir/functions.ts does not list`, {
      construct: "function",
    });
  const overload = overloads.find((candidate) => {
    const slots = candidate.args;
    // Every "T" argument is one type (lowering casts them to it).
    const variables = args.filter((_, k) => (slots[k] ?? slots[slots.length - 1]) === "T");
    if (variables.some((type) => type.kind !== variables[0]?.kind)) return false;
    if (candidate.variadic === true) {
      const last = slots[slots.length - 1];
      if (last === undefined || args.length < slots.length) return false;
      return args.every((type, k) => matches(slots[k] ?? last, type));
    }
    return (
      slots.length === args.length &&
      slots.every((slot, k) => args[k] !== undefined && matches(slot, args[k]))
    );
  });
  if (overload === undefined)
    throw new UnsupportedPlanError(`${signature}: no such overload in ir/functions.ts`, {
      construct: "function",
    });
  const entry = FUNCTION_BINDINGS[fn];
  const binding =
    entry === undefined
      ? undefined
      : "overloads" in entry
        ? entry.overloads[overloadKey(overload)]
        : entry;
  if (binding === undefined)
    throw new UnsupportedPlanError(`${signature}: no kernel binding`, { construct: "function" });
  if ("refused" in binding)
    throw new UnsupportedPlanError(`${signature}: ${binding.refused}`, {
      construct: binding.construct,
    });
  if (binding.kinds !== undefined) {
    const variable = args.find(
      (_, k) => (overload.args[k] ?? overload.args[overload.args.length - 1]) === "T",
    );
    if (variable !== undefined && !binding.kinds.has(variable.kind))
      throw new UnsupportedPlanError(`${signature} over ${variable.kind}`, {
        construct: "function",
      });
  }
  return binding;
}
