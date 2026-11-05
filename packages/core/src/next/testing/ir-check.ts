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
      for (const expr of rel.returning ?? []) checkExpr(expr, row, where);
      return;
    }
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
