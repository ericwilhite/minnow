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
 * - The owner caches the result per (table id, catalog version) and hands
 *   `(tableId) => TableRules` to `WriteTransaction.statement({ rules })`.
 *
 * ## Semantics
 * - `check` evaluates each CHECK over the active rows; FALSE fails and NULL passes (SQL). Checks
 *   that read none of `columnIds` are skipped. The result is the first failing row in physical
 *   order, and for that row the first failing constraint in catalog order.
 * - `generate` recomputes, in place, every stored generated column that depends on `columnIds`,
 *   in catalog order (a generated column cannot read another one, as in PostgreSQL).
 * - Both are synchronous kernels with no allocation per row; their scratch is sized at compile
 *   time for one batch. Errors inside an expression (division by zero, overflow) are the kernels'
 *   SQL errors (exec/errors.ts) and fail the statement.
 */
import type { CheckDef, TableDef } from "../access/catalog.js";
import type { TableRules } from "../access/modify.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { TableRuleExpressions } from "../ir/rules.js";
import { samePhysicalType } from "../types.js";
import type { Batch } from "../vector/vector.js";
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

interface CompiledCheck {
  readonly def: CheckDef;
  /** Catalog column ids it reads. */
  readonly reads: ReadonlySet<number>;
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
    return { def, reads };
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

  if (checks.length === 0 && byColumn.size === 0)
    return { check: () => null, generate: NO_RULES_GENERATE };

  // Expression kernels are Phase 2: the interface is fixed, evaluation is not implemented yet.
  return {
    check(_batch: Batch, columnIds: readonly number[] | null) {
      const relevant =
        columnIds === null
          ? checks
          : checks.filter((check) => columnIds.some((id) => check.reads.has(id)));
      if (relevant.length === 0) return null;
      throw new UnsupportedPlanError("CHECK evaluation (Phase 2)");
    },
    generate() {
      throw new UnsupportedPlanError("generated columns (Phase 2)");
    },
  };
}
