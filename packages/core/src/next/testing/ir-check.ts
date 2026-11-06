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
import { explicitCastAllowed, FUNCTIONS, sqlTypeOf } from "../ir/functions.js";
import type { OutputColumn, PhysicalPlan, Rel, ScalarExpr, SortKey, WindowCall } from "../ir/ir.js";
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
}

// checkPlan is synchronous: the plan under check, set for the duration of one call.
let context: Context = { parameters: [], workTables: new Map() };

function checkSlot(slot: number, type: PhysicalType, where: string): void {
  const declared = context.parameters[slot];
  if (declared === undefined)
    fail(where, `slot ${String(slot)} of ${String(context.parameters.length)}`);
  if (!sameBase(declared, type))
    fail(where, `slot ${String(slot)} is ${declared.kind}, read as ${type.kind}`);
}

const INT8: PhysicalType = { kind: "int64" };
const INT4: PhysicalType = { kind: "int32", bits: 32 };
const FLOAT8: PhysicalType = { kind: "float64" };

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
      expect(args.length === 0 && sameBase(call.type, INT8), "() → bigint");
      break;
    case "count":
      expect(args.length === 1 && sameBase(call.type, INT8), "(T) → bigint");
      break;
    case "min":
    case "max":
      expect(args.length === 1 && first !== undefined && sameBase(first, call.type), "(T) → T");
      break;
    case "sum":
    case "avg":
      expect(args.length === 1, "one argument");
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
      return;
    case "project":
      checkRel(rel.input, catalog);
      for (const expr of rel.exprs) checkExpr(expr, rel.input.output, where);
      checkOutput(rel.exprs, rel.output, where);
      return;
    case "aggregate":
      checkRel(rel.input, catalog);
      for (const call of rel.aggregates)
        for (const arg of call.args) checkExpr(arg, rel.input.output, where);
      if (rel.output.length !== rel.groupBy.length + rel.aggregates.length)
        fail(where, "output width");
      return;
    case "sort":
    case "limit":
      checkRel(rel.input, catalog);
      if (rel.op === "sort")
        for (const key of rel.keys) checkExpr(key.expr, rel.input.output, where);
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
    case "apply":
      checkRel(rel.outer, catalog);
      checkRel(rel.inner, catalog);
      for (const binding of rel.bindings) {
        checkExpr(binding.expr, rel.outer.output, where);
        checkSlot(binding.slot, binding.expr.type, where);
      }
      if (rel.quantified !== null) {
        checkExpr(rel.quantified.test, rel.outer.output, where);
        const inner = rel.inner.output[0];
        if (inner === undefined || !sameBase(inner.type, rel.quantified.test.type))
          fail(where, "quantified test of another type");
      }
      if (rel.output.length !== rel.outer.output.length + 1) fail(where, "apply output width");
      return;
    case "window":
      checkRel(rel.input, catalog);
      for (const index of rel.partitionBy)
        if (index >= rel.input.output.length) fail(where, "partition key out of range");
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
      for (const side of [rel.base, rel.step])
        for (const [index, column] of side.output.entries())
          if (!sameBase(column.type, rel.output[index]?.type ?? column.type))
            fail(where, `recursive column ${String(index)} of another type`);
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
  context = { parameters: plan.parameters, workTables: new Map() };
  try {
    if (plan.placeholderCount > plan.parameters.length) fail("plan", "placeholders past the slots");
    if (plan.clockSlot !== null) {
      checkSlot(plan.clockSlot, { kind: "timestamp", zoned: true }, "clock");
      if (plan.clockSlot < plan.placeholderCount) fail("clock", "slot is a placeholder");
    }
    const initSlots = new Set<number>();
    for (const init of plan.initPlans) {
      const where = `init plan for slot ${String(init.slot)}`;
      if (init.slot < plan.placeholderCount || init.slot === plan.clockSlot)
        fail(where, "slot is a placeholder or the clock");
      if (initSlots.has(init.slot)) fail(where, "slot filled twice");
      initSlots.add(init.slot);
      checkRel(init.plan, catalog);
      const [only, ...more] = init.plan.output;
      if (only === undefined || more.length > 0) fail(where, "not one column");
      checkSlot(init.slot, only.type, where);
    }
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
