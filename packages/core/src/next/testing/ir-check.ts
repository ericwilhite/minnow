/**
 * Structural type checks of a physical plan, for tests: every column reference is in range and
 * of its input column's type, every projection and VALUES row produces its declared output
 * types, every call's argument types are an overload of `ir/functions.ts` (R19), every explicit
 * cast exists, and `modify` inputs have the shapes access/ requires. Types are compared modulo
 * modifiers (VARCHAR(n) widths, NUMERIC precision and scale) where ir/functions.ts allows it.
 * Throws a `TypeError` naming the first violation.
 */
import type { CatalogView } from "../access/catalog.js";
import { explicitCastAllowed, FUNCTIONS, sqlTypeOf } from "../ir/functions.js";
import type { OutputColumn, PhysicalPlan, Rel, ScalarExpr } from "../ir/ir.js";
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
      for (const binding of rel.bindings) checkExpr(binding.expr, rel.outer.output, where);
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
      for (const call of rel.functions)
        for (const arg of call.args) checkExpr(arg, rel.input.output, where);
      if (rel.output.length !== rel.input.output.length + rel.functions.length)
        fail(where, "window output width");
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
    case "recursive":
      checkRel(rel.base, catalog);
      checkRel(rel.step, catalog);
      for (const side of [rel.base, rel.step])
        for (const [index, column] of side.output.entries())
          if (!sameBase(column.type, rel.output[index]?.type ?? column.type))
            fail(where, `recursive column ${String(index)} of another type`);
      return;
    default:
      return;
  }
}

/** Checks `plan` against `catalog` (see the module comment). */
export function checkPlan(plan: PhysicalPlan, catalog: CatalogView): void {
  checkRel(plan.root, catalog);
  checkOutput(
    plan.columns.map((column) => ({ op: "param", slot: 0, type: column.type })),
    plan.root.output,
    "plan columns",
  );
}

/** `items[index]`, which the plan's shape guarantees exists. */
function at<T>(items: ArrayLike<T>, index: number): T {
  const item = items[index];
  if (item === undefined) throw new Error(`item ${String(index)} is missing`);
  return item;
}
