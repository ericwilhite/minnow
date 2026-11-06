/**
 * ir/functions.ts is the authority (R19); bindings.ts binds its entries. This suite fails when an
 * entry has neither a kernel nor an explicit refusal with a construct key, when a binding names
 * no entry, when a kernel binding does not compile and evaluate, and when a refusal is not the
 * shared `UnsupportedPlanError` (stage "execute", its construct key).
 */
import { describe, expect, it } from "vitest";
import {
  FUNCTIONS,
  explicitCastAllowed,
  physicalTypeOfSql,
  type Overload,
  type SqlType,
} from "../../ir/functions.js";
import type { ScalarExpr } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import { allocateFlat, type FlatVector } from "../../vector/vector.js";
import { UnsupportedPlanError } from "../errors.js";
import {
  CAST_REFUSALS,
  COMPARABLE_KINDS,
  FUNCTION_BINDINGS,
  castRefusal,
  overloadKey,
  type Binding,
} from "./bindings.js";
import { BOOL, batchOf, cmp, col, compileFor, decimals, numbers, texts } from "./test-support.js";

const SQL_TYPES: readonly SqlType[] = [
  "bool",
  "int2",
  "int4",
  "int8",
  "float4",
  "float8",
  "numeric",
  "text",
  "date",
  "time",
  "timestamp",
  "timestamptz",
  "interval",
  "json",
  "jsonb",
  "uuid",
  "bytea",
];
const ENUM: PhysicalType = { kind: "enum", name: "e", labels: ["x", "y"] };

/** A two-row vector of `type` whose values no kernel or cast of the table raises on. */
function sample(type: PhysicalType): FlatVector {
  switch (type.kind) {
    case "bool":
      return numbers(type, [1, 0]);
    case "int32":
    case "int64":
    case "float64":
    case "float32":
    case "date":
    case "time":
    case "timestamp":
      return numbers(type, [6, 3]);
    case "decimal":
      return decimals(type, [
        [6n, 0],
        [3n, 1],
      ]);
    case "text":
    case "bytes":
    case "json":
      return texts(["a", "b"], type);
    case "enum":
      return texts(["x", "y"], type);
    default: {
      const vector = allocateFlat(type, 2);
      vector.length = 2;
      return vector;
    }
  }
}

/** The argument types of one call of `overload`, with `T` as `variable`. */
function argumentTypes(overload: Overload, variable: PhysicalType): PhysicalType[] {
  const slots = overload.variadic === true ? [...overload.args, ...overload.args] : overload.args;
  return slots.map((slot) => (slot === "T" ? variable : physicalTypeOfSql(slot)));
}

function callOf(fn: string, overload: Overload, types: readonly PhysicalType[]): ScalarExpr {
  const result = overload.result === "T" ? types[0] : physicalTypeOfSql(overload.result);
  return {
    op: "call",
    fn,
    args: types.map((type, k) => col(k, type)),
    type: result ?? BOOL,
  };
}

function bindingOf(fn: string, overload: Overload): Binding | undefined {
  const entry = FUNCTION_BINDINGS[fn];
  if (entry === undefined) return undefined;
  return "overloads" in entry ? entry.overloads[overloadKey(overload)] : entry;
}

function refusedWith(run: () => unknown, construct: string): void {
  try {
    run();
  } catch (error) {
    expect(error).toBeInstanceOf(UnsupportedPlanError);
    expect((error as UnsupportedPlanError).stage).toBe("execute");
    expect((error as UnsupportedPlanError).construct).toBe(construct);
    return;
  }
  throw new Error("expected a refusal");
}

const ENTRIES = Object.entries(FUNCTIONS).flatMap(([fn, overloads]) =>
  overloads.map((overload) => [`${fn}(${overloadKey(overload)})`, fn, overload] as const),
);

describe("kernel bindings onto ir/functions.ts (R19)", () => {
  it("binds every function and overload of the table, and nothing else", () => {
    const unbound = ENTRIES.filter(([, fn, overload]) => bindingOf(fn, overload) === undefined);
    expect(unbound.map(([name]) => name)).toEqual([]);
    const stale: string[] = [];
    for (const [fn, entry] of Object.entries(FUNCTION_BINDINGS)) {
      const overloads = FUNCTIONS[fn];
      if (overloads === undefined) {
        stale.push(fn);
        continue;
      }
      if ("overloads" in entry)
        for (const key of Object.keys(entry.overloads))
          if (!overloads.some((overload) => overloadKey(overload) === key))
            stale.push(`${fn}(${key})`);
    }
    expect(stale).toEqual([]);
  });

  it.each(ENTRIES)("%s: a kernel that evaluates, or a refusal", (_name, fn, overload) => {
    const binding = bindingOf(fn, overload);
    if (binding === undefined) throw new Error("unbound (see the first test)");
    const variables: PhysicalType[] = overload.args.includes("T")
      ? [...SQL_TYPES.map(physicalTypeOfSql), ENUM]
      : [BOOL];
    for (const variable of variables) {
      const types = argumentTypes(overload, variable);
      const run = () => {
        const compiled = compileFor(callOf(fn, overload, types), types);
        compiled.evaluate(batchOf(types.map(sample)), null);
      };
      if ("refused" in binding) refusedWith(run, binding.construct);
      else if (binding.kinds !== undefined && !binding.kinds.has(variable.kind))
        refusedWith(run, "function");
      else expect(run).not.toThrow();
    }
  });

  it.each(
    SQL_TYPES.flatMap((from) =>
      SQL_TYPES.filter((to) =>
        explicitCastAllowed(physicalTypeOfSql(from), physicalTypeOfSql(to)),
      ).map((to) => [`${from} → ${to}`, from, to] as const),
    ),
  )("cast %s: a kernel that evaluates, or a refusal", (_name, from, to) => {
    const input = sample(physicalTypeOfSql(from));
    const run = () => {
      const compiled = compileFor(
        { op: "cast", input: col(0, input.type), type: physicalTypeOfSql(to) },
        [input.type],
      );
      compiled.evaluate(batchOf([input]), null);
    };
    if (castRefusal(from, to) === null) expect(run).not.toThrow();
    else refusedWith(run, "cast");
  });

  it("refuses only casts the table allows (no stale refusal)", () => {
    for (const key of CAST_REFUSALS.keys()) {
      const [from, to] = key.split("→") as [string, string];
      const pairs =
        from === "*"
          ? SQL_TYPES.map((type) => [type, to as SqlType] as const)
          : to === "*"
            ? SQL_TYPES.map((type) => [from as SqlType, type] as const)
            : [[from as SqlType, to as SqlType] as const];
      expect(
        pairs.some(
          ([a, b]) => a !== b && explicitCastAllowed(physicalTypeOfSql(a), physicalTypeOfSql(b)),
        ),
        key,
      ).toBe(true);
    }
  });

  it("refuses a bounded text cast until the text kernels (no silent non-truncation)", () => {
    const input = texts(["abc"]);
    refusedWith(
      () =>
        compileFor(
          { op: "cast", input: col(0, input.type), type: { kind: "text", maxLength: 2 } },
          [input.type],
        ),
      "cast",
    );
  });

  it.each([...COMPARABLE_KINDS])("compare over %s", (kind) => {
    const type =
      kind === "enum"
        ? ENUM
        : (SQL_TYPES.map(physicalTypeOfSql).find((candidate) => candidate.kind === kind) ?? ENUM);
    const input = sample(type);
    const compiled = compileFor(cmp("<", col(0, input.type), col(1, input.type)), [
      input.type,
      input.type,
    ]);
    expect(() => compiled.evaluate(batchOf([input, input]), null)).not.toThrow();
  });

  it("refuses functions and overloads outside the table", () => {
    refusedWith(
      () => compileFor({ op: "call", fn: "no_such_fn", args: [], type: BOOL }, []),
      "function",
    );
    // IS TRUE is lowered to distinctFrom; the kernel names of old are not functions.
    refusedWith(
      () => compileFor({ op: "call", fn: "is_true", args: [col(0, BOOL)], type: BOOL }, [BOOL]),
      "function",
    );
    const float = physicalTypeOfSql("float8");
    refusedWith(
      () =>
        compileFor({ op: "call", fn: "mod", args: [col(0, float), col(1, float)], type: float }, [
          float,
          float,
        ]),
      "function",
    );
  });
});
