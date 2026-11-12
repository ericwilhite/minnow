/**
 * Compiles an IR scalar expression (ir/ir.ts `ScalarExpr`) into a tree of prebuilt kernels
 * (no code generation). One compiled tree serves one pipeline instance: its output buffers are
 * allocated here, once, from the context's arena (a grant), and reused for every batch.
 *
 * - Columns return the input batch's own vector (no copy). Literals and parameters are constants;
 *   parameter slots are read at evaluation time from `context.params`, so a plan compiled once
 *   serves every execution that binds new values into the same slots.
 * - Function names and overloads are ir/functions.ts's (R19); bindings.ts maps each overload to
 *   a kernel or a refusal. Anything else is `UnsupportedPlanError` here, never a wrong answer
 *   later.
 */
import { compileLike } from "./like.js";
import { compileIntervalExtract } from "./interval-extract.js";
import { compileCalendarAdd } from "./calendar-add.js";
import type { ScalarExpr } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import { samePhysicalType } from "../../types.js";
import {
  allocateFlat,
  type Batch,
  type FlatVector,
  type Selection,
  type Vector,
} from "../../vector/vector.js";
import { UnsupportedPlanError } from "../errors.js";
import { compileArithmetic, compileNegate, type ArithOp } from "./arith.js";
import { bindCall } from "./bindings.js";
import { compileCase, compileCoalesce } from "./case.js";
import { compileTemporalSubtract } from "./temporal-subtract.js";
import { compileDateTrunc } from "./date-trunc.js";
import { compileJson } from "./json.js";
import { compileNumericRound } from "./round.js";
import { compileCast } from "./cast.js";
import { compileCompare, compileDistinctFrom } from "./compare.js";
import { compileInList, compileJunction, compileNot, compileTest } from "./logic.js";
import type { Compiled, OutputArena } from "./vectors.js";

/** Parameter slot values, filled per execution (`BoundParameters.slots` plus init plans and clock). */
export interface ParameterSlots {
  slots: readonly FlatVector[];
}

export interface CompileContext {
  readonly arena: OutputArena;
  readonly params: ParameterSlots;
  /** Types of the input's columns, by position. */
  readonly input: readonly PhysicalType[];
  /** Prepared-plan overload decisions only; compiled buffers remain instance-local. */
  readonly resolveCall?: CallResolver;
}

export type CallResolver = (
  fn: string,
  args: readonly PhysicalType[],
) => ReturnType<typeof bindCall>;

class ColumnNode implements Compiled {
  constructor(
    readonly type: PhysicalType,
    readonly index: number,
  ) {}
  evaluate(batch: Batch): Vector {
    const column = batch.columns[this.index];
    if (column === undefined) throw new RangeError(`The batch has no column ${String(this.index)}`);
    return column;
  }
}

class RetypeNode implements Compiled {
  constructor(
    readonly type: PhysicalType,
    readonly input: Compiled,
  ) {}
  evaluate(batch: Batch, rows: Selection | null): Vector {
    return { ...this.input.evaluate(batch, rows), type: this.type };
  }
}

class ConstantNode implements Compiled {
  readonly #vector: { form: "constant"; type: PhysicalType; length: number; value: FlatVector };
  constructor(
    readonly type: PhysicalType,
    value: FlatVector,
  ) {
    this.#vector = { form: "constant", type, length: 0, value };
  }
  evaluate(batch: Batch): Vector {
    this.#vector.length = batch.length;
    return this.#vector;
  }
}

class ParamNode implements Compiled {
  readonly #vector: { form: "constant"; type: PhysicalType; length: number; value: FlatVector };
  constructor(
    readonly type: PhysicalType,
    readonly slot: number,
    readonly params: ParameterSlots,
  ) {
    this.#vector = { form: "constant", type, length: 0, value: allocateFlat(type, 1) };
  }
  evaluate(batch: Batch): Vector {
    const value = this.params.slots[this.slot];
    if (value === undefined)
      throw new RangeError(`Parameter slot ${String(this.slot)} is not bound`);
    this.#vector.value = value;
    this.#vector.length = batch.length;
    return this.#vector;
  }
}

export function compileExpr(expr: ScalarExpr, context: CompileContext): Compiled {
  const { arena } = context;
  const sub = (child: ScalarExpr) => compileExpr(child, context);
  switch (expr.op) {
    case "column": {
      const input = context.input[expr.index];
      if (input === undefined)
        throw new RangeError(`Column ${String(expr.index)} is outside the input`);
      if (!samePhysicalType(input, expr.type) && input.kind !== expr.type.kind)
        throw new TypeError(
          `Column ${String(expr.index)} is ${input.kind}, the plan says ${expr.type.kind}`,
        );
      return new ColumnNode(expr.type, expr.index);
    }
    case "literal":
      return new ConstantNode(expr.type, expr.value);
    case "param":
      return new ParamNode(expr.type, expr.slot, context.params);
    case "cast":
      if (expr.jsonCanonical) return new RetypeNode(expr.type, sub(expr.input));
      return compileCast(expr.type, sub(expr.input), arena);
    case "compare":
      return compileCompare(expr.cmp, sub(expr.left), sub(expr.right), expr.collation, arena);
    case "distinctFrom":
      return compileDistinctFrom(expr.negated, sub(expr.left), sub(expr.right), arena);
    case "and":
    case "or":
      return compileJunction(expr.op === "and", expr.args.map(sub), arena);
    case "not":
      return compileNot(sub(expr.arg), arena);
    case "isNull":
      return compileTest(
        sub(expr.arg),
        expr.negated ? [true, true, false] : [false, false, true],
        arena,
      );
    case "case":
      return compileCase(
        expr.type,
        expr.branches.map((branch) => ({ when: sub(branch.when), then: sub(branch.then) })),
        expr.otherwise === null ? null : sub(expr.otherwise),
        arena,
      );
    case "inList":
      if (expr.collation !== null && expr.collation !== "C" && expr.collation !== "default")
        throw new UnsupportedPlanError(`IN under collation ${expr.collation}`);
      return compileInList(
        expr.negated,
        sub(expr.arg),
        expr.list.map(sub),
        expr.list.every((item) => item.op === "literal" || item.op === "param"),
        arena,
      );
    case "like":
      return compileLike(
        expr.mode,
        expr.negated,
        sub(expr.arg),
        sub(expr.pattern),
        expr.escape,
        expr.collation,
        arena,
      );
    case "call":
      return compileCall(expr, context);
  }
}

function compileCall(expr: Extract<ScalarExpr, { op: "call" }>, context: CompileContext): Compiled {
  const { arena } = context;
  const args = expr.args.map((arg) => compileExpr(arg, context));
  const binding = (context.resolveCall ?? bindCall)(
    expr.fn,
    args.map((arg) => arg.type),
  );
  switch (binding.kernel) {
    case "json":
      return compileJson(expr.type, expr.fn, args, arena);
    case "arith": {
      const [left, right] = args;
      if (args.length !== 2 || left === undefined || right === undefined)
        throw new TypeError(`${expr.fn} takes two arguments`);
      return compileArithmetic(expr.fn as ArithOp, expr.type, left, right, arena);
    }
    case "neg": {
      const [arg] = args;
      if (arg === undefined) throw new TypeError("neg takes one argument");
      return compileNegate(expr.type, arg, arena);
    }
    case "intervalExtract": {
      const [field, input] = args;
      if (field === undefined || input === undefined)
        throw new TypeError("EXTRACT takes two arguments");
      return compileIntervalExtract(expr.type, field, input, arena);
    }
    case "calendarAdd": {
      const [left, right] = args;
      if (left === undefined || right === undefined)
        throw new TypeError("Calendar arithmetic takes two arguments");
      return compileCalendarAdd(expr.type, expr.fn, left, right, arena);
    }
    case "temporalSub": {
      const [left, right] = args;
      if (left === undefined || right === undefined)
        throw new TypeError("Temporal subtraction takes two arguments");
      return compileTemporalSubtract(expr.type, left, right, arena);
    }
    case "dateTrunc": {
      const [field, input] = args;
      if (field === undefined || input === undefined)
        throw new TypeError("DATE_TRUNC takes two arguments");
      return compileDateTrunc(expr.type, field, input, arena);
    }
    case "numericRound": {
      const [input, digits] = args;
      if (input === undefined || digits === undefined)
        throw new TypeError("numeric ROUND takes two arguments");
      return compileNumericRound(expr.type, input, digits, arena);
    }
    case "coalesce":
      return compileCoalesce(expr.type, args, arena);
    case "nullif": {
      const [a, b] = args;
      if (a === undefined || b === undefined) throw new TypeError("nullif takes two arguments");
      const nullValue = allocateFlat(expr.type, 1);
      nullValue.length = 1;
      nullValue.validity = new Uint32Array(1);
      return compileCase(
        expr.type,
        [
          {
            when: compileCompare("=", a, b, null, arena),
            then: new ConstantNode(expr.type, nullValue),
          },
        ],
        a,
        arena,
      );
    }
  }
}

/** Evaluates a compiled predicate and writes the rows where it is true (not null) into `out`. */
export function selectTrue(
  predicate: Compiled,
  batch: Batch,
  rows: Selection | null,
  out: Selection,
): Selection {
  const value = predicate.evaluate(batch, rows);
  const count = rows === null ? batch.length : rows.count;
  let kept = 0;
  const target = out.rows;
  if (value.form === "flat") {
    const values = (value.data as { values: Uint8Array }).values;
    const validity = value.validity;
    if (rows === null) {
      // Branch-free: write every row, advance only past the true ones.
      if (validity === null)
        for (let i = 0; i < count; i++) {
          target[kept] = i;
          kept += values[i] ?? 0;
        }
      else
        for (let i = 0; i < count; i++)
          if (((validity[i >>> 5] ?? 0) & (1 << (i & 31))) !== 0) {
            target[kept] = i;
            kept += values[i] ?? 0;
          }
    } else {
      const source = rows.rows;
      for (let k = 0; k < count; k++) {
        const i = source[k] ?? 0;
        if (validity === null || ((validity[i >>> 5] ?? 0) & (1 << (i & 31))) !== 0) {
          target[kept] = i;
          kept += values[i] ?? 0;
        }
      }
    }
  } else if (value.form === "constant") {
    const truth =
      (value.value.validity === null || ((value.value.validity[0] ?? 0) & 1) !== 0) &&
      (value.value.data as { values: Uint8Array }).values[0] === 1;
    if (truth)
      for (let k = 0; k < count; k++) target[kept++] = rows === null ? k : (rows.rows[k] ?? 0);
  } else {
    const entries = (value.dictionary.data as { values: Uint8Array }).values;
    for (let k = 0; k < count; k++) {
      const i = rows === null ? k : (rows.rows[k] ?? 0);
      if (value.validity !== null && ((value.validity[i >>> 5] ?? 0) & (1 << (i & 31))) === 0)
        continue;
      if (entries[value.codes[i] ?? 0] === 1) target[kept++] = i;
    }
  }
  out.count = kept;
  return out;
}
