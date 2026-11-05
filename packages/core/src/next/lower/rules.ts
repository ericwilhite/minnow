/**
 * A table's rule expressions (CHECK, generated columns, defaults) lowered from the catalog's SQL
 * text: `Lowerer.lowerTableRules` for the executor's `compileTableRules` (R14.3), and the same
 * expressions inlined into insert and update plans.
 */
import type { CatalogView, ColumnDef, TableDef } from "../access/catalog.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { TableRuleExpressions } from "../ir/rules.js";
import {
  compileCheckExpression,
  compileDefaultExpression,
  compileWithDeclaredTypes,
  expressionColumnNames,
} from "../../engine/query.js";
import type { Expression } from "../../plan/model.js";
import { coerce, ParameterTypes, type Typed } from "./coerce.js";
import { PlanningError } from "./errors.js";
import { asBool, lowerExpression, type ExpressionState, type Scope } from "./expressions.js";
import { integerLiteral, nullLiteral } from "./literals.js";

/** A scope over one row of `table` whose column `i` (catalog order) is `values(i)`. */
export function rowScope(
  table: TableDef,
  qualifiers: readonly string[],
  values: (position: number, def: ColumnDef) => Typed,
): Scope {
  return {
    column: (reference) => {
      const dot = reference.indexOf(".");
      if (dot !== -1 && !qualifiers.includes(reference.slice(0, dot))) return undefined;
      const name = dot === -1 ? reference : reference.slice(dot + 1);
      const position = table.columns.findIndex((def) => def.name === name);
      const def = table.columns[position];
      return def === undefined ? undefined : values(position, def);
    },
  };
}

/** The scope of a complete table row laid out in catalog order (rules, RETURNING). */
export function tableRowScope(
  table: TableDef,
  qualifiers: readonly string[] = [table.name],
): Scope {
  return rowScope(table, qualifiers, (position, def) => ({
    expr: { op: "column", index: position, type: def.type },
    nullable: def.nullable,
    big: def.type.kind === "int64",
  }));
}

const parsedRules = new Map<string, Expression>();

/** Parses stored rule text once (the text is the front end's normalized SQL). */
function parseRule(sql: string, check: string | null): Expression {
  const key = `${check === null ? "e" : "c"}\u0000${sql}`;
  let parsed = parsedRules.get(key);
  if (parsed === undefined) {
    parsed = compileWithDeclaredTypes(() =>
      check === null ? compileDefaultExpression(sql) : compileCheckExpression(sql, check),
    );
    if (parsedRules.size > 4096) parsedRules.clear();
    parsedRules.set(key, parsed);
  }
  return parsed;
}

/** The column names a stored expression reads. */
export function ruleColumns(sql: string): string[] {
  return expressionColumnNames(parseRule(sql, null)).map((name) => name.split(".").at(-1) ?? name);
}

/** The default of `def` (assignment-cast to its type), or NULL. Reads no column. */
export function defaultValue(def: ColumnDef, state: ExpressionState): Typed {
  if (def.identitySequence !== null) {
    const id = integerLiteral(BigInt(def.identitySequence), { kind: "int64" });
    if (id === null) throw new PlanningError("sequence id out of range", "XX000");
    state.volatile = true;
    state.sequences = true;
    const next: Typed = {
      expr: { op: "call", fn: "nextval", args: [id], type: { kind: "int64" } },
      nullable: false,
      big: true,
    };
    return coerce(next, def.type, "assignment", state.parameters);
  }
  if (def.defaultSql === null) return { expr: nullLiteral(def.type), nullable: true, big: false };
  const noColumns: Scope = {
    column: (reference) => {
      throw new PlanningError(`cannot use column reference "${reference}" in a default`, "0A000");
    },
  };
  const typed = lowerExpression(parseRule(def.defaultSql, null), noColumns, state);
  return coerce(typed, def.type, "assignment", state.parameters);
}

/** A stored generated column's value over `scope` (the row it belongs to), cast to its type. */
export function generatedValue(def: ColumnDef, scope: Scope, state: ExpressionState): Typed {
  if (def.generatedSql === null)
    throw new PlanningError(`column ${def.name} is not generated`, "XX000");
  return coerce(
    lowerExpression(parseRule(def.generatedSql, null), scope, state),
    def.type,
    "assignment",
    state.parameters,
  );
}

/** A CHECK condition over `scope`, as bool. */
export function checkCondition(
  sql: string,
  name: string,
  scope: Scope,
  state: ExpressionState,
): Typed {
  return asBool(lowerExpression(parseRule(sql, name), scope, state), state);
}

/**
 * The rules state: no placeholders; a default that reads the statement clock (`now()`) reads
 * parameter slot 0 (ir/rules.ts).
 */
function rulesState(catalog: CatalogView): ExpressionState {
  return {
    parameters: new ParameterTypes(0),
    catalog,
    clock: () => 0,
    volatile: false,
    sequences: false,
  };
}

export function lowerTableRules(table: TableDef, catalog: CatalogView): TableRuleExpressions {
  const scope = tableRowScope(table);
  const state = rulesState(catalog);
  const checks = table.checks.map((check) => ({
    checkId: check.id,
    condition: checkCondition(check.conditionSql, check.name, scope, state).expr,
  }));
  const generated: Array<{ columnId: number; expr: ScalarExpr }> = [];
  const defaults: Array<{ columnId: number; expr: ScalarExpr }> = [];
  for (const def of table.columns) {
    if (def.generatedSql !== null)
      generated.push({ columnId: def.id, expr: generatedValue(def, scope, state).expr });
    else if (def.defaultSql !== null || def.identitySequence !== null)
      defaults.push({ columnId: def.id, expr: defaultValue(def, state).expr });
  }
  return { checks, generated, defaults };
}
