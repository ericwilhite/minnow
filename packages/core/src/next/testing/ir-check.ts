/**
 * Structural type checks of a physical plan, for tests: every column reference is in range and
 * of its input column's type, every projection and VALUES row produces its declared output
 * types, every call's argument types are an overload of `ir/functions.ts` (R19), every explicit
 * cast exists, and `modify` inputs have the shapes access/ requires. Parameter reads, init plans
 * and apply bindings are checked against the plan's slot types; window calls against their
 * signatures and frames; work-table scans against the recursion that binds them. Types are
 * compared modulo modifiers (VARCHAR(n) widths, NUMERIC precision and scale) where
 * ir/functions.ts allows it. Throws a `TypeError` naming the first violation.
 */
import type { CatalogView } from "../access/catalog.js";
import {
  AGGREGATES,
  aggregateResult,
  explicitCastAllowed,
  FUNCTIONS,
  sqlTypeOf,
} from "../ir/functions.js";
import type {
  AggregateFn,
  OutputColumn,
  PhysicalPlan,
  Rel,
  ScalarExpr,
  SortKey,
  WindowCall,
} from "../ir/ir.js";
import { samePhysicalType, type PhysicalType } from "../types.js";

function base(type: PhysicalType): PhysicalType {
  if (type.kind === "decimal") return { kind: "decimal", precision: null, scale: null };
  if (type.kind === "text") return { kind: "text" };
  return type;
}

const sameBase = (a: PhysicalType, b: PhysicalType) => samePhysicalType(base(a), base(b));

function fail(where: string, message: string): never {
  throw new TypeError(`${where}: ${message}`);
}

/** What the plan being checked declares: slot types, and the work tables in scope. */
interface Context {
  readonly parameters: readonly PhysicalType[];
  readonly workTables: Map<number, readonly OutputColumn[]>;
  /** Slots filled for the whole plan: placeholders, the clock, init plans. */
  readonly global: ReadonlySet<number>;
  /** Slots of the `apply` bindings enclosing the expression being checked. */
  readonly bound: Set<number>;
  /** Init-plan slots read, collected while one init plan's body is checked (for cycles). */
  reads: Set<number> | null;
}

// checkPlan is synchronous: the plan under check, set for the duration of one call.
let context: Context = {
  parameters: [],
  workTables: new Map(),
  global: new Set(),
  bound: new Set(),
  reads: null,
};

/** A slot's declared type (writes and reads). */
function checkSlotType(slot: number, type: PhysicalType, where: string): void {
  const declared = context.parameters[slot];
  if (declared === undefined)
    fail(where, `slot ${String(slot)} of ${String(context.parameters.length)}`);
  if (!sameBase(declared, type))
    fail(where, `slot ${String(slot)} is ${declared.kind}, read as ${type.kind}`);
}

/** A read of a slot: of its type, and filled where it is read (globally or by an enclosing apply). */
function checkSlot(slot: number, type: PhysicalType, where: string): void {
  checkSlotType(slot, type, where);
  if (!context.global.has(slot) && !context.bound.has(slot))
    fail(where, `slot ${String(slot)} is read where nothing fills it`);
  context.reads?.add(slot);
}

/** A LIMIT / OFFSET bound: a non-negative integer, or a filled bigint slot. */
function checkBound(bound: unknown, where: string): void {
  if (bound === null) return;
  const shape = bound as { kind?: unknown; value?: unknown; slot?: unknown };
  if (shape.kind === "const" && Number.isInteger(shape.value) && (shape.value as number) >= 0)
    return;
  if (shape.kind === "param" && typeof shape.slot === "number") {
    checkSlot(shape.slot, INT8_TYPE, where);
    return;
  }
  fail(where, `malformed bound ${JSON.stringify(bound)}`);
}

const INT8_TYPE: PhysicalType = { kind: "int64" };

/** An input column position: an integer within `[0, width)`. */
function checkPosition(index: number, width: number, where: string, what: string): void {
  if (!Number.isInteger(index) || index < 0 || index >= width)
    fail(where, `${what} ${String(index)} of ${String(width)}`);
}

/** Output columns equal to the input's (types modulo modifiers). */
function checkSameColumns(
  input: readonly OutputColumn[],
  output: readonly OutputColumn[],
  where: string,
): void {
  if (input.length !== output.length) fail(where, "output width differs from its input");
  for (const [index, column] of output.entries())
    if (!sameBase(column.type, at(input, index).type))
      fail(where, `output ${String(index)} of another type than its input`);
}

const INT8: PhysicalType = { kind: "int64" };
const INT4: PhysicalType = { kind: "int32", bits: 32 };
const FLOAT8: PhysicalType = { kind: "float64" };

const ORDERED_SET: ReadonlySet<string> = new Set(["percentile_cont", "percentile_disc", "mode"]);

/**
 * An aggregate's argument and result types: exactly an overload of ir/functions.ts `AGGREGATES`
 * (R19), for every aggregate. An ordered-set aggregate's WITHIN GROUP key is its one ORDER BY.
 */
function checkAggregateSignature(
  fn: string,
  args: readonly ScalarExpr[],
  orderBy: readonly SortKey[],
  type: PhysicalType,
  where: string,
): void {
  if (!Object.hasOwn(AGGREGATES, fn)) fail(where, `unknown aggregate ${fn}`);
  const aggregate = fn as AggregateFn;
  const ordered = ORDERED_SET.has(fn);
  if (ordered && orderBy.length !== 1)
    fail(where, `aggregate ${fn}: WITHIN GROUP takes one ORDER BY key`);
  const signature = `${fn}(${args.map((arg) => arg.type.kind).join(", ")})`;
  const expected = aggregateResult(
    aggregate,
    args.map((arg) => arg.type),
    ordered ? orderBy[0]?.expr.type : undefined,
  );
  if (expected === null) fail(where, `aggregate ${signature}: no such overload`);
  const exact = expected.kind === "decimal" && expected.scale !== null;
  if (exact ? type.kind !== "decimal" || type.scale !== expected.scale : !sameBase(type, expected))
    fail(where, `aggregate ${signature}: returns ${expected.kind}, not ${type.kind}`);
}

/** A window call's argument and result types (lower/windows.ts, PostgreSQL's signatures). */
function checkWindowCall(call: WindowCall, orderBy: readonly SortKey[], where: string): void {
  const args = call.args.map((arg) => arg.type);
  const [first] = args;
  const expect = (ok: boolean, what: string) => {
    if (!ok) fail(where, `window ${call.fn}: ${what}`);
  };
  switch (call.fn) {
    case "row_number":
    case "rank":
    case "dense_rank":
      expect(args.length === 0 && sameBase(call.type, INT8), "() → bigint");
      break;
    case "percent_rank":
    case "cume_dist":
      expect(args.length === 0 && sameBase(call.type, FLOAT8), "() → double precision");
      break;
    case "ntile":
      expect(
        args.length === 1 &&
          first !== undefined &&
          sameBase(first, INT4) &&
          sameBase(call.type, INT4),
        "(integer) → integer",
      );
      break;
    case "lag":
    case "lead":
      expect(
        args.length >= 2 &&
          args.length <= 3 &&
          first !== undefined &&
          sameBase(first, call.type) &&
          sameBase(args[1] ?? FLOAT8, INT4) &&
          (args[2] === undefined || sameBase(args[2], call.type)),
        "(T, integer[, T]) → T",
      );
      break;
    case "first_value":
    case "last_value":
      expect(args.length === 1 && first !== undefined && sameBase(first, call.type), "(T) → T");
      break;
    case "nth_value":
      expect(
        args.length === 2 &&
          first !== undefined &&
          sameBase(first, call.type) &&
          sameBase(args[1] ?? FLOAT8, INT4),
        "(T, integer) → T",
      );
      break;
    case "count_star":
    case "count":
    case "min":
    case "max":
    case "sum":
    case "avg":
      checkAggregateSignature(call.fn, call.args, [], call.type, `${where}: window`);
      break;
    default:
      fail(where, `unknown window function ${call.fn}`);
  }
  const frame = call.frame;
  if (frame === null) return;
  for (const bound of [frame.start, frame.end]) {
    const offset = bound.offset;
    const offsetKind = bound.kind === "preceding" || bound.kind === "following";
    if (offsetKind !== (offset !== null)) fail(where, `frame bound ${bound.kind} offset`);
    if (offset === null) continue;
    checkExpr(offset, [], where);
    if (frame.unit !== "range") {
      if (!sameBase(offset.type, INT8)) fail(where, `${frame.unit} offset is not bigint`);
      continue;
    }
    const [key, ...more] = orderBy;
    if (key === undefined || more.length > 0)
      fail(where, "RANGE with an offset needs exactly one ORDER BY key");
    const ordered = key.expr.type.kind;
    const temporal = ordered === "timestamp" || ordered === "date" || ordered === "time";
    if (!(temporal ? offset.type.kind === "interval" : sameBase(offset.type, key.expr.type)))
      fail(where, `RANGE offset ${offset.type.kind} over a ${ordered} key`);
  }
}

function checkExpr(expr: ScalarExpr, input: readonly OutputColumn[], where: string): void {
  switch (expr.op) {
    case "column": {
      const column = input[expr.index];
      if (column === undefined)
        fail(where, `column ${String(expr.index)} of ${String(input.length)}`);
      if (!sameBase(column.type, expr.type))
        fail(
          where,
          `column ${String(expr.index)} is ${column.type.kind}, read as ${expr.type.kind}`,
        );
      return;
    }
    case "literal":
      if (!samePhysicalType(expr.value.type, expr.type))
        fail(where, "literal vector of another type");
      return;
    case "param":
      checkSlot(expr.slot, expr.type, where);
      return;
    case "cast":
      checkExpr(expr.input, input, where);
      if (!explicitCastAllowed(expr.input.type, expr.type))
        fail(where, `cast ${expr.input.type.kind} → ${expr.type.kind} does not exist`);
      return;
    case "call": {
      for (const arg of expr.args) checkExpr(arg, input, where);
      const overloads = FUNCTIONS[expr.fn];
      if (overloads === undefined) fail(where, `unknown function ${expr.fn}`);
      const kinds = expr.args.map((arg) => sqlTypeOf(arg.type));
      const result = sqlTypeOf(expr.type);
      const matches = overloads.some((overload) => {
        const params =
          overload.variadic === true
            ? [
                ...overload.args,
                ...Array<string>(Math.max(0, kinds.length - overload.args.length)).fill(
                  overload.args.at(-1) ?? "T",
                ),
              ]
            : overload.args;
        if (params.length !== kinds.length) return false;
        const variable = expr.args[params.indexOf("T")]?.type;
        return (
          params.every((param, index) =>
            param === "T"
              ? variable !== undefined && sameBase(expr.args[index]?.type ?? variable, variable)
              : param === kinds[index],
          ) &&
          (overload.result === "T"
            ? variable !== undefined && sameBase(variable, expr.type)
            : overload.result === result)
        );
      });
      if (!matches)
        fail(where, `${expr.fn}(${kinds.join(", ")}) → ${String(result)} is no overload`);
      return;
    }
    case "compare":
    case "distinctFrom":
      checkExpr(expr.left, input, where);
      checkExpr(expr.right, input, where);
      if (!sameBase(expr.left.type, expr.right.type)) fail(where, `${expr.op} over two types`);
      return;
    case "and":
    case "or":
      for (const arg of expr.args) checkExpr(arg, input, where);
      return;
    case "not":
    case "isNull":
      checkExpr(expr.arg, input, where);
      return;
    case "case":
      for (const branch of expr.branches) {
        checkExpr(branch.when, input, where);
        checkExpr(branch.then, input, where);
        if (!sameBase(branch.then.type, expr.type)) fail(where, "CASE branch of another type");
      }
      if (expr.otherwise !== null) checkExpr(expr.otherwise, input, where);
      return;
    case "inList":
      checkExpr(expr.arg, input, where);
      for (const item of expr.list) {
        checkExpr(item, input, where);
        if (!sameBase(item.type, expr.arg.type)) fail(where, "IN list of another type");
      }
      return;
    case "like":
      checkExpr(expr.arg, input, where);
      checkExpr(expr.pattern, input, where);
      return;
  }
}

function checkOutput(
  exprs: readonly ScalarExpr[],
  output: readonly OutputColumn[],
  where: string,
): void {
  if (exprs.length !== output.length)
    fail(where, `${String(exprs.length)} expressions, ${String(output.length)} outputs`);
  for (const [index, expr] of exprs.entries()) {
    const column = output[index];
    if (column !== undefined && !sameBase(expr.type, column.type))
      fail(where, `output ${String(index)} is ${column.type.kind}, computed as ${expr.type.kind}`);
  }
}

function checkRel(rel: Rel, catalog: CatalogView): void {
  const where = `${rel.op}#${String(rel.id)}`;
  switch (rel.op) {
    case "scan":
      if (rel.filter !== null) checkExpr(rel.filter, rel.output, where);
      return;
    case "values":
      for (const row of rel.rows) {
        for (const cell of row) checkExpr(cell, [], where);
        checkOutput(row, rel.output, where);
      }
      return;
    case "filter":
      checkRel(rel.input, catalog);
      checkExpr(rel.predicate, rel.input.output, where);
      if (rel.predicate.type.kind !== "bool") fail(where, "predicate is not boolean");
      checkSameColumns(rel.input.output, rel.output, where);
      return;
    case "distinct":
      checkRel(rel.input, catalog);
      checkSameColumns(rel.input.output, rel.output, where);
      return;
    case "fetch": {
      checkRel(rel.input, catalog);
      checkPosition(rel.locator, rel.input.output.length, where, "locator");
      if (at(rel.input.output, rel.locator).type.kind !== "locator")
        fail(where, "fetch locator is not a locator");
      const table = catalog.table(rel.tableId);
      if (table === undefined) fail(where, "unknown table");
      for (const id of rel.columns)
        if (!table.columns.some((column) => column.id === id))
          fail(where, `unknown column ${String(id)}`);
      return;
    }
    case "project":
      checkRel(rel.input, catalog);
      for (const expr of rel.exprs) checkExpr(expr, rel.input.output, where);
      checkOutput(rel.exprs, rel.output, where);
      return;
    case "aggregate": {
      checkRel(rel.input, catalog);
      const input = rel.input.output;
      for (const index of rel.groupBy) checkPosition(index, input.length, where, "group key");
      for (const set of rel.groupingSets ?? [])
        for (const member of set)
          checkPosition(member, rel.groupBy.length, where, "grouping set member");
      for (const call of rel.aggregates) {
        for (const arg of call.args) checkExpr(arg, input, where);
        if (call.filter !== null) {
          checkExpr(call.filter, input, where);
          if (call.filter.type.kind !== "bool") fail(where, "aggregate FILTER is not boolean");
        }
        for (const key of call.orderBy) checkExpr(key.expr, input, where);
        checkAggregateSignature(call.fn, call.args, call.orderBy, call.type, where);
      }
      if (rel.output.length !== rel.groupBy.length + rel.aggregates.length)
        fail(where, "output width");
      for (const [position, index] of rel.groupBy.entries())
        if (!sameBase(at(rel.output, position).type, at(input, index).type))
          fail(where, `group key output ${String(position)} of another type`);
      for (const [position, call] of rel.aggregates.entries())
        if (!sameBase(at(rel.output, rel.groupBy.length + position).type, call.type))
          fail(where, `aggregate output ${String(position)} of another type`);
      return;
    }
    case "sort":
    case "limit":
      checkRel(rel.input, catalog);
      if (rel.op === "sort")
        for (const key of rel.keys) checkExpr(key.expr, rel.input.output, where);
      checkBound(rel.limit, where);
      checkBound(rel.offset, where);
      checkSameColumns(rel.input.output, rel.output, where);
      return;
    case "modify": {
      checkRel(rel.input, catalog);
      const table = catalog.table(rel.tableId);
      if (table === undefined) fail(where, "unknown table");
      const row = table.columns.map((column) => ({
        name: column.name,
        type: column.type,
        nullable: column.nullable,
      }));
      const input = rel.input.output;
      if (rel.kind === "insert" || rel.kind === "upsert") {
        if (input.length !== row.length) fail(where, "insert input is not the table row");
        for (const [index, column] of row.entries())
          if (!samePhysicalType(input[index]?.type ?? { kind: "text" }, column.type))
            fail(where, `insert column ${column.name} has another type`);
      }
      if (rel.kind === "update") {
        if (input[0]?.type.kind !== "locator") fail(where, "update input lacks the locator");
        for (const [index, id] of rel.assigned.entries()) {
          const column = table.columns.find((candidate) => candidate.id === id);
          if (
            column === undefined ||
            !samePhysicalType(input[index + 1]?.type ?? { kind: "text" }, column.type)
          )
            fail(where, `assigned column ${String(id)} has another type`);
        }
      }
      if (rel.kind === "delete" && input[0]?.type.kind !== "locator")
        fail(where, "delete input lacks the locator");
      if (rel.onConflict !== null) {
        const reads = [...row, ...row];
        for (const assignment of rel.onConflict.assignments) {
          checkExpr(assignment.expr, reads, where);
          const column = table.columns.find((candidate) => candidate.id === assignment.column);
          if (column === undefined || !samePhysicalType(assignment.expr.type, column.type))
            fail(
              where,
              `conflict assignment of column ${String(assignment.column)} has another type`,
            );
          reads.push({ name: column.name, type: column.type, nullable: true });
        }
        if (rel.onConflict.where !== null) checkExpr(rel.onConflict.where, [...row, ...row], where);
      }
      const extra =
        (rel.passthrough ?? 0) === 0 ? [] : input.slice(input.length - (rel.passthrough ?? 0));
      for (const expr of rel.returning ?? []) checkExpr(expr, [...row, ...extra], where);
      if (rel.kind === "merge") {
        const merge = rel.merge;
        if (merge === undefined) fail(where, "merge without its spec");
        if (input[merge.sourceColumns]?.type.kind !== "locator")
          fail(where, "merge input lacks the target locator");
        for (const [index, column] of row.entries())
          if (
            !samePhysicalType(
              input[merge.sourceColumns + 1 + index]?.type ?? { kind: "text" },
              column.type,
            )
          )
            fail(where, `merge target column ${column.name} has another type`);
        for (const action of merge.actions) {
          if (action.condition !== null) checkExpr(action.condition, input, where);
          const reads = [...input];
          if (action.action.kind === "update") {
            for (const [index, value] of action.action.values.entries()) {
              checkExpr(value, reads, where);
              const column = table.columns.find(
                (candidate) =>
                  candidate.id ===
                  (action.action as { assigned: readonly number[] }).assigned[index],
              );
              if (column === undefined || !samePhysicalType(value.type, column.type))
                fail(where, "merge update value of another type");
              reads.push({ name: column.name, type: column.type, nullable: true });
            }
          }
          if (action.action.kind === "insert") {
            if (action.action.values.length !== row.length)
              fail(where, "merge insert is not the table row");
            for (const [index, value] of action.action.values.entries()) {
              checkExpr(value, reads, where);
              const column = row[index] as OutputColumn;
              if (!samePhysicalType(value.type, column.type))
                fail(where, `merge insert column ${column.name} of another type`);
              reads.push(column);
            }
          }
        }
      }
      return;
    }
    case "keyLookup":
    case "indexRange": {
      const keys =
        rel.op === "keyLookup"
          ? rel.keys.flat()
          : [...(rel.lower?.keys ?? []), ...(rel.upper?.keys ?? [])];
      for (const key of keys) checkExpr(key, [], where);
      const table = catalog.table(rel.tableId);
      const index = table?.indexes.find((candidate) => candidate.id === rel.indexId);
      if (table === undefined || index === undefined) fail(where, "unknown table or index");
      const tuples =
        rel.op === "keyLookup" ? rel.keys : [rel.lower?.keys ?? [], rel.upper?.keys ?? []];
      for (const tuple of tuples)
        for (const [position, key] of tuple.entries()) {
          const column = table.columns.find(
            (candidate) => candidate.id === index.columnIds[position],
          );
          if (column === undefined || !sameBase(key.type, column.type))
            fail(where, `key ${String(position)} of another type`);
        }
      return;
    }
    case "join": {
      checkRel(rel.left, catalog);
      checkRel(rel.right, catalog);
      if (rel.leftKeys.length !== rel.rightKeys.length) fail(where, "key counts differ");
      for (const [index, key] of rel.leftKeys.entries()) {
        checkExpr(key, rel.left.output, where);
        const other = at(rel.rightKeys, index);
        checkExpr(other, rel.right.output, where);
        if (!sameBase(key.type, other.type)) fail(where, `key ${String(index)} compares two types`);
      }
      const both = [...rel.left.output, ...rel.right.output];
      if (rel.residual !== null) checkExpr(rel.residual, both, where);
      if (rel.filter !== undefined && rel.filter !== null) {
        if (rel.kind !== "mark") fail(where, "filter on a non-mark join");
        checkExpr(rel.filter, both, where);
      }
      const width =
        rel.kind === "semi" || rel.kind === "anti"
          ? rel.left.output.length
          : rel.kind === "mark"
            ? rel.left.output.length + 1
            : both.length;
      if (rel.output.length !== width)
        fail(where, `output width ${String(rel.output.length)}, expected ${String(width)}`);
      return;
    }
    case "apply": {
      checkRel(rel.outer, catalog);
      for (const binding of rel.bindings) {
        checkExpr(binding.expr, rel.outer.output, where);
        checkSlotType(binding.slot, binding.expr.type, where);
      }
      // The bindings fill their slots for the inner relation only.
      const added = rel.bindings
        .map((binding) => binding.slot)
        .filter((slot) => !context.bound.has(slot));
      for (const slot of added) context.bound.add(slot);
      try {
        checkRel(rel.inner, catalog);
      } finally {
        for (const slot of added) context.bound.delete(slot);
      }
      if (rel.quantified !== null) {
        checkExpr(rel.quantified.test, rel.outer.output, where);
        const inner = rel.inner.output[0];
        if (inner === undefined || !sameBase(inner.type, rel.quantified.test.type))
          fail(where, "quantified test of another type");
      }
      if (rel.output.length !== rel.outer.output.length + 1) fail(where, "apply output width");
      return;
    }
    case "window":
      checkRel(rel.input, catalog);
      for (const index of rel.partitionBy)
        checkPosition(index, rel.input.output.length, where, "partition key");
      for (const key of rel.orderBy) checkExpr(key.expr, rel.input.output, where);
      for (const call of rel.functions) {
        for (const arg of call.args) checkExpr(arg, rel.input.output, where);
        checkWindowCall(call, rel.orderBy, where);
      }
      if (rel.output.length !== rel.input.output.length + rel.functions.length)
        fail(where, "window output width");
      for (const [index, column] of rel.output.entries()) {
        const type =
          index < rel.input.output.length
            ? at(rel.input.output, index).type
            : at(rel.functions, index - rel.input.output.length).type;
        if (!sameBase(column.type, type)) fail(where, `window output ${String(index)} type`);
      }
      return;
    case "setOp":
      for (const input of rel.inputs) {
        checkRel(input, catalog);
        if (input.output.length !== rel.output.length) fail(where, "set operation widths differ");
        for (const [index, column] of input.output.entries())
          if (!sameBase(column.type, at(rel.output, index).type))
            fail(where, `set operation column ${String(index)} of another type`);
      }
      return;
    case "recursive": {
      if (context.workTables.has(rel.workTable)) fail(where, "work table bound twice");
      checkRel(rel.base, catalog);
      // Only the step reads the work table: the rows of the previous iteration.
      context.workTables.set(rel.workTable, rel.output);
      try {
        checkRel(rel.step, catalog);
      } finally {
        context.workTables.delete(rel.workTable);
      }
      for (const side of [rel.base, rel.step]) {
        if (side.output.length !== rel.output.length) fail(where, "recursive width differs");
        for (const [index, column] of side.output.entries())
          if (!sameBase(column.type, at(rel.output, index).type))
            fail(where, `recursive column ${String(index)} of another type`);
      }
      return;
    }
    case "workTableScan": {
      const bound = context.workTables.get(rel.workTable);
      if (bound === undefined) fail(where, "work table outside its recursive step");
      if (bound.length !== rel.output.length) fail(where, "work table width");
      for (const [index, column] of rel.output.entries())
        if (!sameBase(column.type, at(bound, index).type))
          fail(where, `work table column ${String(index)} of another type`);
      return;
    }
    default:
      return;
  }
}

/** Checks `plan` against `catalog` (see the module comment). */
export function checkPlan(plan: PhysicalPlan, catalog: CatalogView): void {
  const saved = context;
  const global = new Set<number>();
  context = {
    parameters: plan.parameters,
    workTables: new Map(),
    global,
    bound: new Set(),
    reads: null,
  };
  try {
    if (plan.placeholderCount > plan.parameters.length) fail("plan", "placeholders past the slots");
    for (let slot = 0; slot < plan.placeholderCount; slot += 1) global.add(slot);
    if (plan.clockSlot !== null) {
      checkSlotType(plan.clockSlot, { kind: "timestamp", zoned: true }, "clock");
      if (plan.clockSlot < plan.placeholderCount) fail("clock", "slot is a placeholder");
      global.add(plan.clockSlot);
    }
    const initSlots = new Set<number>();
    for (const init of plan.initPlans) {
      const where = `init plan for slot ${String(init.slot)}`;
      if (init.slot < plan.placeholderCount || init.slot === plan.clockSlot)
        fail(where, "slot is a placeholder or the clock");
      if (initSlots.has(init.slot)) fail(where, "slot filled twice");
      initSlots.add(init.slot);
      global.add(init.slot);
    }
    // Every declared slot has exactly one producer: a placeholder, the clock, an init plan or
    // an apply binding (which fills it for its inner relation only, checked where it is read).
    const producers = new Map<number, number>();
    for (const slot of global) producers.set(slot, 1);
    const visitApplies = (value: unknown): void => {
      if (value === null || typeof value !== "object") return;
      if (Array.isArray(value)) {
        for (const item of value) visitApplies(item);
        return;
      }
      const node = value as { op?: unknown; bindings?: ReadonlyArray<{ slot: number }> };
      if (node.op === "apply")
        for (const binding of node.bindings ?? [])
          producers.set(binding.slot, (producers.get(binding.slot) ?? 0) + 1);
      for (const child of Object.values(node)) visitApplies(child);
    };
    visitApplies(plan.root);
    for (const init of plan.initPlans) visitApplies(init.plan);
    for (let slot = 0; slot < plan.parameters.length; slot += 1) {
      const count = producers.get(slot) ?? 0;
      if (count !== 1)
        fail(
          `slot ${String(slot)}`,
          count === 0 ? "nothing fills it" : `${String(count)} producers fill it`,
        );
    }
    for (const slot of producers.keys())
      if (slot >= plan.parameters.length) fail(`slot ${String(slot)}`, "is not declared");
    // Init plans: one column of their slot's type, reading only plan-wide slots, no cycle.
    const reads = new Map<number, Set<number>>();
    for (const init of plan.initPlans) {
      const where = `init plan for slot ${String(init.slot)}`;
      context.reads = new Set();
      checkRel(init.plan, catalog);
      reads.set(init.slot, new Set([...context.reads].filter((slot) => initSlots.has(slot))));
      context.reads = null;
      const [only, ...more] = init.plan.output;
      if (only === undefined || more.length > 0) fail(where, "not one column");
      checkSlotType(init.slot, only.type, where);
    }
    const state = new Map<number, "visiting" | "done">();
    const visit = (slot: number): void => {
      if (state.get(slot) === "done") return;
      if (state.get(slot) === "visiting")
        fail(`init plan for slot ${String(slot)}`, "reads itself");
      state.set(slot, "visiting");
      for (const next of reads.get(slot) ?? []) visit(next);
      state.set(slot, "done");
    };
    for (const slot of initSlots) visit(slot);
    checkRel(plan.root, catalog);
    checkOutput(
      plan.columns.map((column) => ({ op: "literal", type: column.type }) as ScalarExpr),
      plan.root.output,
      "plan columns",
    );
  } finally {
    context = saved;
  }
}

/** `items[index]`, which the plan's shape guarantees exists. */
function at<T>(items: ArrayLike<T>, index: number): T {
  const item = items[index];
  if (item === undefined) throw new Error(`item ${String(index)} is missing`);
  return item;
}
