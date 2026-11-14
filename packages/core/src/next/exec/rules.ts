/**
 * The executor's row rules for the write path (ruling R1): CHECK constraints and stored generated
 * columns, compiled into kernels that satisfy access/modify.ts `TableRules`. The write path calls
 * them for every row a statement writes, including rows its foreign-key actions (cascades,
 * SET NULL, rekey follow) write in other tables.
 *
 * ## Who compiles what
 * - The catalog stores conditions and generation expressions as SQL text (access/catalog.ts). The
 *   executor never calls the front end, so lowering turns them into IR expressions
 *   (`TableRuleExpressions`) and the owner passes those here.
 * - Expressions are positional over the table's columns in catalog order (`TableDef.columns`):
 *   `{ op: "column", index: i }` is column i, which is the layout of every batch the rules see.
 * - The owner caches only the immutable validated program per catalog identity/table id.
 *   Each statement binds it with `bindTableRules` (row-rules.ts), or supplies the additive fourth
 *   execution context here, and hands the bound hooks to `WriteTransaction.statement({ rules })`.
 *   The binding, buffers and grant stay alive through statement.end(), including FK actions.
 *
 * ## Semantics
 * - `check` evaluates each CHECK over the active rows; FALSE fails and NULL passes (SQL). Checks
 *   that read none of `columnIds` (including regenerated IDs) may be skipped. The result is the first failing row in physical
 *   order, and for that row the first failing constraint in catalog order.
 * - `generate` recomputes, in place, every stored generated column that depends on `columnIds`,
 *   in catalog order (a generated column cannot read another one, as in PostgreSQL).
 * - Both are synchronous kernels; execution buffers are charged before allocation at binding
 *   time and growth. Programs retain no execution buffers. Errors inside an expression (division by zero, overflow) are the kernels'
 *   SQL errors (exec/errors.ts) and fail the statement.
 */
import type { TableDef } from "../access/catalog.js";
import type { TableRules } from "../access/modify.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { TableRuleExpressions } from "../ir/rules.js";
import { samePhysicalType } from "../types.js";
import type { Batch } from "../vector/vector.js";
import { rememberRuleProgram, type CompiledCheck } from "./rule-program.js";
import { bindTableRules, type BoundTableRules, type RowRuleContext } from "./row-rules.js";
import { UnsupportedPlanError } from "./errors.js";

/** The lowered forms of a table's rule expressions: defined once in ir/rules.ts (lowering builds them). */
export type { TableRuleExpressions } from "../ir/rules.js";

export class RuleCompileError extends TypeError {
  readonly code = "MINNOW_RULE_COMPILE";
  constructor(tableId: number, message: string) {
    super(`Table ${String(tableId)}: ${message}`);
    this.name = "RuleCompileError";
  }
}

function columnsRead(expr: ScalarExpr, into: Set<number>): Set<number> {
  switch (expr.op) {
    case "column":
      into.add(expr.index);
      break;
    case "literal":
    case "param":
      break;
    case "call":
    case "and":
    case "or":
      for (const arg of expr.args) columnsRead(arg, into);
      break;
    case "cast":
      columnsRead(expr.input, into);
      break;
    case "compare":
    case "distinctFrom":
      columnsRead(expr.left, into);
      columnsRead(expr.right, into);
      break;
    case "not":
    case "isNull":
      columnsRead(expr.arg, into);
      break;
    case "case":
      for (const branch of expr.branches) {
        columnsRead(branch.when, into);
        columnsRead(branch.then, into);
      }
      if (expr.otherwise !== null) columnsRead(expr.otherwise, into);
      break;
    case "inList":
      columnsRead(expr.arg, into);
      for (const item of expr.list) columnsRead(item, into);
      break;
    case "like":
      columnsRead(expr.arg, into);
      columnsRead(expr.pattern, into);
      break;
  }
  return into;
}

function hasParam(expr: ScalarExpr): boolean {
  if (expr.op === "param") return true;
  switch (expr.op) {
    case "call":
    case "and":
    case "or":
      return expr.args.some(hasParam);
    case "cast":
      return hasParam(expr.input);
    case "compare":
    case "distinctFrom":
      return hasParam(expr.left) || hasParam(expr.right);
    case "not":
    case "isNull":
      return hasParam(expr.arg);
    case "case":
      return (
        expr.branches.some((branch) => hasParam(branch.when) || hasParam(branch.then)) ||
        (expr.otherwise !== null && hasParam(expr.otherwise))
      );
    case "inList":
      return hasParam(expr.arg) || expr.list.some(hasParam);
    case "like":
      return hasParam(expr.arg) || hasParam(expr.pattern);
    default:
      return false;
  }
}

const NO_RULES_GENERATE = (): void => undefined;

/**
 * Validates and compiles a table's rules at one catalog version. Throws `RuleCompileError` when
 * the expressions do not match the table (a missing or extra check or generated column, a column
 * index out of range, a wrong result type, a parameter, a generated column reading another).
 * A table with no checks and no generated columns gets rules that do nothing and cost nothing.
 */
export function compileTableRules(
  table: TableDef,
  catalogVersion: number,
  expressions: TableRuleExpressions,
): TableRules;
export function compileTableRules(
  table: TableDef,
  catalogVersion: number,
  expressions: TableRuleExpressions,
  context: RowRuleContext,
): BoundTableRules;
export function compileTableRules(
  table: TableDef,
  catalogVersion: number,
  expressions: TableRuleExpressions,
  context?: RowRuleContext,
): TableRules {
  const fail = (message: string): never => {
    throw new RuleCompileError(table.id, `${message} (catalog version ${String(catalogVersion)})`);
  };
  const columns = table.columns;
  const validate = (expr: ScalarExpr, what: string): Set<number> => {
    if (hasParam(expr)) fail(`${what} uses a parameter slot`);
    const read = columnsRead(expr, new Set());
    for (const index of read)
      if (!(index >= 0 && index < columns.length)) fail(`${what} reads column ${String(index)}`);
    return read;
  };

  const byCheck = new Map(expressions.checks.map((entry) => [entry.checkId, entry.condition]));
  if (byCheck.size !== expressions.checks.length) fail("a check is given twice");
  const checks: CompiledCheck[] = table.checks.map((def) => {
    const condition = byCheck.get(def.id);
    if (condition === undefined) return fail(`check ${String(def.id)} has no expression`);
    if (condition.type.kind !== "bool") fail(`check ${String(def.id)} is not boolean`);
    const reads = new Set<number>();
    for (const index of validate(condition, `check ${String(def.id)}`)) {
      const column = columns[index];
      if (column !== undefined) reads.add(column.id);
    }
    return { def, reads, condition };
  });
  if (byCheck.size !== table.checks.length) fail("an expression names no check of the table");

  const generatedColumns = new Set(
    columns.filter((column) => column.generatedSql !== null).map((column) => column.id),
  );
  const byColumn = new Map(expressions.generated.map((entry) => [entry.columnId, entry.expr]));
  if (byColumn.size !== expressions.generated.length) fail("a generated column is given twice");
  for (const [columnId, expr] of byColumn) {
    if (!generatedColumns.has(columnId)) fail(`column ${String(columnId)} is not generated`);
    const column = columns.find((candidate) => candidate.id === columnId);
    if (column !== undefined && !samePhysicalType(column.type, expr.type))
      fail(`generated column ${String(columnId)} has the wrong type`);
    for (const index of validate(expr, `generated column ${String(columnId)}`))
      if (generatedColumns.has(columns[index]?.id ?? -1))
        fail(`generated column ${String(columnId)} reads a generated column`);
  }
  for (const columnId of generatedColumns)
    if (!byColumn.has(columnId)) fail(`generated column ${String(columnId)} has no expression`);

  if (checks.length === 0 && byColumn.size === 0) {
    const noRules: TableRules = { check: () => null, generate: NO_RULES_GENERATE };
    return context ? bindTableRules(noRules, context.charge, context) : noRules;
  }

  const generated = columns.flatMap((column, index) => {
    const expr = byColumn.get(column.id);
    return expr === undefined
      ? []
      : [
          {
            columnId: column.id,
            index,
            expr,
            reads: new Set([...columnsRead(expr, new Set())].map((i) => columns[i]?.id ?? -1)),
          },
        ];
  });
  const rules: TableRules = {
    check(_batch: Batch, columnIds: readonly number[] | null) {
      if (
        checks.length === 0 ||
        (generated.length === 0 &&
          !checks.some(
            (check) =>
              check.reads.size === 0 ||
              columnIds === null ||
              columnIds.some((id) => check.reads.has(id)),
          ))
      )
        return null;
      throw new UnsupportedPlanError("row rules require an execution binding", {
        construct: "rowRules",
      });
    },
    generate(_batch, columnIds) {
      if (!generated.some((g) => g.reads.size === 0 || columnIds.some((id) => g.reads.has(id))))
        return;
      throw new UnsupportedPlanError("row rules require an execution binding", {
        construct: "rowRules",
      });
    },
  };
  rememberRuleProgram(rules, { table, checks, generated });
  return context ? bindTableRules(rules, context.charge, context) : rules;
}
