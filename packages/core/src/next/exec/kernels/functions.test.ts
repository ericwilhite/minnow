/**
 * ir/functions.ts promises kernels to lowering: every entry must compile and evaluate here, and
 * a function outside it must be refused as unsupported.
 */
import { describe, expect, it } from "vitest";
import { CASTS, COMPARABLE_KINDS, SCALAR_FUNCTIONS, findFunction } from "./kernel-table.js";
import type { ScalarExpr } from "../../ir/ir.js";
import type { PhysicalKind, PhysicalType } from "../../types.js";
import { allocateFlat, type FlatVector } from "../../vector/vector.js";
import { UnsupportedPlanError } from "../errors.js";
import { BOOL, batchOf, cmp, col, compileFor, decimals, numbers, texts } from "./test-support.js";

/** A representative type and a two-row vector of it. */
function sample(kind: PhysicalKind): FlatVector {
  switch (kind) {
    case "bool":
      return numbers(BOOL, [1, 0]);
    case "int32":
      return numbers({ kind: "int32", bits: 32 }, [6, 3]);
    case "int64":
    case "time":
    case "timestamp":
      return numbers(
        kind === "int64" ? { kind } : kind === "time" ? { kind } : { kind, zoned: false },
        [6, 3],
      );
    case "float64":
    case "float32":
      return numbers({ kind }, [6, 3]);
    case "date":
      return numbers({ kind }, [6, 3]);
    case "decimal":
      return decimals({ kind, precision: null, scale: null }, [
        [6n, 0],
        [3n, 1],
      ]);
    case "text":
    case "bytes":
    case "json":
      return texts(["a", "b"], kind === "json" ? { kind, binary: true } : { kind });
    case "enum":
      return texts(["x", "y"], { kind, name: "e", labels: ["x", "y"] });
    default: {
      const type: PhysicalType =
        kind === "uuid" ? { kind } : kind === "interval" ? { kind } : { kind: "bool" };
      const vector = allocateFlat(type, 2);
      vector.length = 2;
      return vector;
    }
  }
}

describe("ir/functions.ts is backed by kernels", () => {
  it.each(SCALAR_FUNCTIONS.map((s) => [`${s.fn}(${s.args.join(", ")})`, s] as const))(
    "%s",
    (_name, signature) => {
      const inputs = signature.args.map((kind) => sample(kind));
      const args = (signature.variadic ? [inputs[0], inputs[0]] : inputs) as FlatVector[];
      const expr: ScalarExpr = {
        op: "call",
        fn: signature.fn,
        args: args.map((_, k) => col(k, args[k]?.type ?? BOOL)),
        type: args[0]?.type ?? BOOL,
      };
      const compiled = compileFor(
        expr,
        args.map((arg) => arg.type),
      );
      expect(() => compiled.evaluate(batchOf(args), null)).not.toThrow();
    },
  );

  it.each(CASTS.map(([from, to]) => [`${from} → ${to}`, from, to] as const))(
    "cast %s",
    (_name, from, to) => {
      const input = sample(from);
      const target = sample(to).type;
      const compiled = compileFor({ op: "cast", input: col(0, input.type), type: target }, [
        input.type,
      ]);
      expect(() => compiled.evaluate(batchOf([input]), null)).not.toThrow();
    },
  );

  it.each(COMPARABLE_KINDS)("compare %s", (kind) => {
    const input = sample(kind);
    const compiled = compileFor(cmp("<", col(0, input.type), col(1, input.type)), [
      input.type,
      input.type,
    ]);
    expect(() => compiled.evaluate(batchOf([input, input]), null)).not.toThrow();
  });

  it("finds overloads by argument kinds and refuses the rest", () => {
    expect(findFunction("add", ["int64", "int64"])?.result).toBe("int64");
    expect(findFunction("coalesce", ["text", "text", "text"])?.result).toBe("text");
    expect(findFunction("mod", ["float64", "float64"])).toBeUndefined();
    expect(() => compileFor({ op: "call", fn: "no_such_fn", args: [], type: BOOL }, [])).toThrow(
      UnsupportedPlanError,
    );
  });
});
