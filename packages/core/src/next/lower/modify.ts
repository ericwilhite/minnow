/**
 * INSERT, UPDATE, DELETE and batch writes as `modify` plans (ir.ts; access/modify.ts shapes):
 * - insert: rows in catalog order, every value assignment-cast to its column, omitted and DEFAULT
 *   columns filled with their defaults, then stored generated columns computed over the row;
 * - update: the target's locator and the assigned values (plus generated columns that read an
 *   assigned column), found by key lookup or scan;
 * - delete: the target's locators;
 * - RETURNING over the affected table row.
 * UPDATE … FROM and DELETE … USING join the target with their sources (lower/from.ts; a row
 * matched twice is updated once, `onRepeat: "first"`); MERGE is `lowerMerge`.
 */
import type { ColumnDef, IndexDef, TableDef } from "../access/catalog.js";
import type { OutputColumn, Rel, ScalarExpr } from "../ir/ir.js";
import {
  isDefaultInsertValue,
  isDeferredInsertExpression,
  isUpdateDefaultAssignment,
  type CompiledStatement,
  type InsertValue,
  type MutationSources,
  type ReturningItem,
} from "../../engine/query.js";
import type { Expression, JoinPlan, Predicate, TableSource } from "../../plan/model.js";
import { crossJoinPlan } from "../../plan/model.js";
import { coerce, settle, type Typed } from "./coerce.js";
import { PlanningError, unsupported } from "./errors.js";
import { asBool, lowerCondition, lowerExpression, type Scope } from "./expressions.js";
import { parseQuery, prepareQuery } from "./frontend.js";
import { lowerFrom, SOURCE_STRIDE, sourceOf, virtual, type FromClause } from "./from.js";
import {
  mapColumns,
  outputColumns,
  type PlanBuilder,
  type RelColumn,
  type TableRead,
} from "./plan.js";
import { lowerBlock } from "./query.js";
import { defaultValue, generatedValue, rowScope, ruleColumns, tableRowScope } from "./rules.js";
import { nullLiteral } from "./literals.js";

type Insert = Extract<CompiledStatement, { kind: "insert" }>;
type Update = Extract<CompiledStatement, { kind: "update" }>;
type Delete = Extract<CompiledStatement, { kind: "delete" }>;
type Merge = Extract<CompiledStatement, { kind: "merge" }>;

export interface LoweredWrite {
  readonly rel: Rel;
  /** RETURNING columns (empty without RETURNING). */
  readonly columns: readonly RelColumn[];
  readonly tableId: number;
}

const column = (index: number, def: { readonly type: ColumnDef["type"] }): ScalarExpr => ({
  op: "column",
  index,
  type: def.type,
});

function targetTable(name: string, builder: PlanBuilder): TableDef {
  const table = builder.catalog.tableByName(name);
  if (table === undefined) throw new PlanningError(`relation "${name}" does not exist`, "42P01");
  return table;
}

function columnNamed(table: TableDef, name: string): ColumnDef {
  const def = table.columns.find((candidate) => candidate.name === name);
  if (def === undefined)
    throw new PlanningError(`column "${name}" of relation "${table.name}" does not exist`, "42703");
  return def;
}

/** An INSERT value of the compiled statement as a front-end expression. */
function insertExpression(value: InsertValue): Expression {
  if (isDeferredInsertExpression(value)) return value.expression;
  if (
    typeof value === "object" &&
    value !== null &&
    !(value instanceof Date) &&
    "parameter" in value
  )
    return { kind: "parameter", index: value.parameter };
  if (typeof value === "string" && value.startsWith("\u0000minnow-domain:")) {
    const kind = value.slice("\u0000minnow-domain:".length).split(":")[0];
    if (
      kind === "numeric" ||
      kind === "date" ||
      kind === "time" ||
      kind === "interval" ||
      kind === "uuid"
    )
      return { kind: "literal", value, internalSqlValue: true, sqlDomain: { kind } };
  }
  return { kind: "literal", value: value as Exclude<InsertValue, object> | Date | null };
}

/**
 * The RETURNING list over the affected table row: `*`, column names, or expression items.
 */
function returningList(
  statement: { readonly returning?: string[] | "*"; readonly returningItems?: ReturningItem[] },
  table: TableDef,
  alias: string | undefined,
  builder: PlanBuilder,
): { exprs: ScalarExpr[]; columns: RelColumn[] } | null {
  const scope = tableRowScope(table, [table.name, ...(alias === undefined ? [] : [alias])]);
  const typedItems: Array<{ name: string; typed: Typed }> = [];
  if (statement.returningItems !== undefined) {
    for (const item of statement.returningItems)
      typedItems.push({
        name: item.alias,
        typed: settle(lowerExpression(item.expression, scope, builder.state), builder.parameters),
      });
  } else if (statement.returning === "*") {
    for (const [position, def] of table.columns.entries())
      typedItems.push({
        name: def.name,
        typed: {
          expr: column(position, def),
          nullable: def.nullable,
          big: def.type.kind === "int64",
        },
      });
  } else if (statement.returning !== undefined) {
    for (const name of statement.returning) {
      const typed = scope.column(name);
      if (typed === undefined) throw new PlanningError(`column "${name}" does not exist`, "42703");
      typedItems.push({ name, typed });
    }
  } else return null;
  return {
    exprs: typedItems.map((item) => item.typed.expr),
    columns: typedItems.map(({ name, typed }) => ({
      name,
      type: typed.expr.type,
      nullable: typed.nullable,
      big: typed.big,
    })),
  };
}

/**
 * Completes rows of the target table: `supplied(def, position)` gives a value for a column the
 * statement names (or undefined), then defaults, then stored generated columns over the row.
 * Returns a projection over `input` in catalog order.
 */
function completeRows(
  table: TableDef,
  input: Rel,
  supplied: (def: ColumnDef) => Typed | undefined,
  builder: PlanBuilder,
): Rel {
  const base = table.columns.map((def): Typed => {
    // A placeholder of the column's own type; the next projection computes it (finding 6).
    if (def.generatedSql !== null)
      return { expr: nullLiteral(def.type), nullable: true, big: false };
    return supplied(def) ?? defaultValue(def, builder.state);
  });
  const output: OutputColumn[] = table.columns.map((def) => ({
    name: def.name,
    type: def.type,
    nullable: def.nullable,
  }));
  const first: Rel = {
    op: "project",
    id: builder.id(),
    input,
    exprs: base.map((value) => value.expr),
    output,
  };
  if (!table.columns.some((def) => def.generatedSql !== null)) return first;
  const scope = tableRowScope(table);
  const exprs = table.columns.map((def, position) =>
    def.generatedSql === null
      ? column(position, def)
      : generatedValue(def, scope, builder.state).expr,
  );
  return { op: "project", id: builder.id(), input: first, exprs, output };
}

function arbitersFor(table: TableDef, columns: readonly string[]): IndexDef[] {
  const wanted = new Set(columns.map((name) => columnNamed(table, name).id));
  return table.indexes.filter(
    (index) =>
      index.unique &&
      index.columnIds.length === wanted.size &&
      index.columnIds.every((id) => wanted.has(id)),
  );
}

/**
 * The stored generated columns that read an assigned column, computed over the new row: an
 * assigned column reads `assignedValue(def)`, the value already materialized once (an input
 * column of the next projection, or an earlier ON CONFLICT assignment), never its expression
 * again, so `SET x = nextval('s')` advances the sequence once (review finding 3); any other
 * column reads `current`. In catalog order.
 */
function dependentGenerated(
  table: TableDef,
  assigned: ReadonlySet<number>,
  assignedValue: (def: ColumnDef) => Typed,
  current: (position: number, def: ColumnDef) => Typed,
  qualifiers: readonly string[],
  builder: PlanBuilder,
): Array<{ readonly def: ColumnDef; readonly value: Typed }> {
  const assignedNames = new Set(
    table.columns.filter((def) => assigned.has(def.id)).map((def) => def.name),
  );
  const newRow = rowScope(table, qualifiers, (position, def) =>
    assigned.has(def.id) ? assignedValue(def) : current(position, def),
  );
  return table.columns.flatMap((def) =>
    def.generatedSql !== null &&
    ruleColumns(def.generatedSql).some((name) => assignedNames.has(name))
      ? [{ def, value: generatedValue(def, newRow, builder.state) }]
      : [],
  );
}

/**
 * ON CONFLICT assignments in IR order: the assigned values, then the generated columns that read
 * them, each reading an earlier assignment `j` as column `2n + j` (ir.ts "What modify's
 * expressions read").
 */
function conflictAssignments(
  table: TableDef,
  assigned: ReadonlyArray<{ readonly def: ColumnDef; readonly value: Typed }>,
  qualifiers: readonly string[],
  builder: PlanBuilder,
): Array<{ readonly column: number; readonly expr: ScalarExpr }> {
  const width = table.columns.length;
  const order = new Map(assigned.map((entry, index) => [entry.def.id, index]));
  const dependents = dependentGenerated(
    table,
    new Set(order.keys()),
    (def) => ({
      expr: column(2 * width + (order.get(def.id) ?? 0), def),
      nullable: true,
      big: def.type.kind === "int64",
    }),
    (position, def) => ({
      expr: column(position, def),
      nullable: def.nullable,
      big: def.type.kind === "int64",
    }),
    qualifiers,
    builder,
  );
  return [...assigned, ...dependents].map(({ def, value }) => ({
    column: def.id,
    expr: value.expr,
  }));
}

function generatedAssignmentError(def: ColumnDef): PlanningError {
  return new PlanningError(`column "${def.name}" can only be updated to DEFAULT`, "428C9");
}

export function lowerInsert(statement: Insert, builder: PlanBuilder): LoweredWrite {
  const table = targetTable(statement.table, builder);
  const names =
    statement.columns.length > 0 ? statement.columns : table.columns.map((def) => def.name);
  const targets = names.map((name) => columnNamed(table, name));
  if (new Set(targets.map((def) => def.id)).size !== targets.length)
    throw new PlanningError("column specified more than once", "42701");
  const state = builder.state;
  const noColumns: Scope = { column: () => undefined };
  let rows: Rel;
  let supplied: (def: ColumnDef) => Typed | undefined;
  if (statement.query !== undefined) {
    const query = lowerBlock(prepareQuery(statement.query, builder.catalog), builder);
    if (query.columns.length !== targets.length)
      throw new PlanningError("INSERT has more target columns than expressions", "42601");
    rows = query.rel;
    supplied = (def) => {
      const position = targets.indexOf(def);
      const source = query.columns[position];
      if (position === -1 || source === undefined) return undefined;
      return coerce(
        {
          expr: { op: "column", index: position, type: source.type },
          nullable: source.nullable,
          big: source.big,
        },
        def.type,
        "assignment",
        builder.parameters,
      );
    };
  } else {
    const valueRows: ReadonlyArray<readonly InsertValue[]> =
      statement.defaultValues === true
        ? [targets.map(() => ({ default: true as const }))]
        : statement.rows;
    const cells = valueRows.map((row) => {
      if (row.length !== targets.length)
        throw new PlanningError(
          `INSERT has more ${row.length > targets.length ? "expressions" : "target columns"} than ${row.length > targets.length ? "target columns" : "expressions"}`,
          "42601",
        );
      return targets.map((def, index): Typed => {
        const value = row[index] ?? null;
        if (isDefaultInsertValue(value)) return defaultValue(def, state);
        if (def.generatedSql !== null)
          throw new PlanningError(
            `cannot insert a non-DEFAULT value into column "${def.name}"`,
            "428C9",
          );
        return coerce(
          lowerExpression(insertExpression(value), noColumns, state),
          def.type,
          "assignment",
          builder.parameters,
        );
      });
    });
    const output: OutputColumn[] = targets.map((def, index) => ({
      name: def.name,
      type: def.type,
      nullable: cells.some((row) => row[index]?.nullable ?? true),
    }));
    rows = {
      op: "values",
      id: builder.id(),
      rows: cells.map((row) => row.map((cell) => cell.expr)),
      output,
    };
    supplied = (def) => {
      const position = targets.indexOf(def);
      return position === -1
        ? undefined
        : { expr: column(position, def), nullable: true, big: false };
    };
  }
  for (const def of targets)
    if (def.generatedSql !== null && statement.query !== undefined)
      throw new PlanningError(
        `cannot insert a non-DEFAULT value into column "${def.name}"`,
        "428C9",
      );
  const input = completeRows(
    table,
    rows,
    (def) => (def.generatedSql === null ? supplied(def) : undefined),
    builder,
  );
  const returning = returningList(statement, table, statement.alias, builder);
  const onConflict =
    statement.onConflict === undefined ? null : conflictClause(statement, table, targets, builder);
  return {
    rel: {
      op: "modify",
      id: builder.id(),
      kind: onConflict === null ? "insert" : "upsert",
      tableId: table.id,
      input,
      assigned: [],
      onRepeat: "error",
      onConflict,
      returning: returning?.exprs ?? null,
      output: outputColumns(returning?.columns ?? []),
    },
    columns: returning?.columns ?? [],
    tableId: table.id,
  };
}

function conflictClause(
  statement: Insert,
  table: TableDef,
  targets: readonly ColumnDef[],
  builder: PlanBuilder,
): NonNullable<Extract<Rel, { op: "modify" }>["onConflict"]> {
  const clause = statement.onConflict;
  if (clause === undefined) throw new PlanningError("no ON CONFLICT", "XX000");
  const arbiters =
    clause.anyTarget === true
      ? []
      : arbitersFor(table, clause.columns ?? [clause.column]).map((index) => index.id);
  if (clause.anyTarget !== true && arbiters.length === 0)
    throw new PlanningError(
      "there is no unique or exclusion constraint matching the ON CONFLICT specification",
      "42P10",
    );
  const indexId = arbiters[0] ?? -1;
  if (clause.action === "nothing")
    return { arbiters, indexId, action: "nothing", assignments: [], where: null };
  // Existing row is columns 0 … n−1, EXCLUDED is n … 2n−1 (ir.ts "What modify's expressions read").
  const width = table.columns.length;
  const qualifiers = [table.name, ...(statement.alias === undefined ? [] : [statement.alias])];
  const existing = (position: number, def: ColumnDef): Typed => ({
    expr: column(position, def),
    nullable: def.nullable,
    big: def.type.kind === "int64",
  });
  const own = rowScope(table, qualifiers, existing);
  const excluded = rowScope(table, ["EXCLUDED", "excluded"], (position, def) => ({
    expr: column(width + position, def),
    nullable: def.nullable,
    big: def.type.kind === "int64",
  }));
  // An unqualified name is both the target's and EXCLUDED's column: PostgreSQL's 42702 (finding 7).
  const scope: Scope = {
    column: (reference) => {
      if (!reference.includes(".") && own.column(reference) !== undefined)
        throw new PlanningError(`column reference "${reference}" is ambiguous`, "42702");
      return excluded.column(reference) ?? own.column(reference);
    },
  };
  const assigned: Array<{ def: ColumnDef; value: Typed }> = [];
  const pairs =
    clause.action === "replace"
      ? targets.map((def) => ({
          column: def.name,
          expression: { kind: "column" as const, reference: `EXCLUDED.${def.name}` },
        }))
      : (clause.assignments ?? []);
  for (const pair of pairs) {
    const def = columnNamed(table, pair.column);
    if (def.generatedSql !== null) throw generatedAssignmentError(def);
    const value = isUpdateDefaultAssignment(pair.expression)
      ? defaultValue(def, builder.state)
      : coerce(
          lowerExpression(pair.expression, scope, builder.state),
          def.type,
          "assignment",
          builder.parameters,
        );
    if (assigned.some((entry) => entry.def.id === def.id))
      throw new PlanningError(`multiple assignments to same column "${def.name}"`, "42601");
    assigned.push({ def, value });
  }
  const where =
    clause.where === undefined
      ? null
      : asBool(lowerExpression(clause.where, scope, builder.state), builder.state).expr;
  return {
    arbiters,
    indexId,
    action: "update",
    assignments: conflictAssignments(table, assigned, qualifiers, builder),
    where,
  };
}

/**
 * The target rows of an UPDATE or DELETE, with the locator: a single-table read (key lookup,
 * index range or scan; lower/access-path.ts), or the target joined with UPDATE … FROM / DELETE …
 * USING sources (lower/from.ts orders the joins). Expressions are typed against the clause's
 * virtual columns (`scope`) and placed by `place` once `build` ran.
 */
interface TargetRows {
  readonly from: FromClause;
  readonly scope: Scope;
  readonly read: TableRead;
  readonly locator: number;
  readonly targetSource: number;
  build(): { readonly rel: Rel; readonly place: (expr: ScalarExpr) => ScalarExpr };
}

function targetRows(
  table: TableDef,
  alias: string | undefined,
  predicates: readonly Predicate[],
  sources: MutationSources | undefined,
  builder: PlanBuilder,
): TargetRows {
  const base: TableSource = { table: table.name, alias: alias ?? table.name };
  const joins: JoinPlan[] =
    sources === undefined ? [] : [crossJoinPlan(sources.base), ...sources.joins];
  const from = lowerFrom(base, joins, builder, { locators: [0], lowerBlock });
  const read = from.tableRead(0);
  if (read === undefined)
    throw new PlanningError(`relation "${table.name}" is not a table`, "42809");
  const where = [...predicates, ...(sources?.predicates ?? [])].map((predicate) =>
    asBool(
      lowerCondition(
        predicate.operator,
        predicate.left,
        predicate.right,
        predicate.escape,
        from.scope,
        builder.state,
      ),
      builder.state,
    ),
  );
  return {
    from,
    scope: from.scope,
    read,
    locator: from.locator(0),
    targetSource: from.sources[0] ?? -1,
    build: () => {
      const unit = from.build(where);
      const positions = new Map(unit.layout.map((index, position) => [index, position]));
      return {
        rel: unit.rel,
        place: (expr) =>
          mapColumns(expr, (index) => {
            const position = positions.get(index);
            if (position === undefined)
              throw new PlanningError(`column ${String(index)} vanished`, "XX000");
            return position;
          }),
      };
    },
  };
}

/**
 * RETURNING of an UPDATE or DELETE: target columns read the affected table row (positions in
 * catalog order); any other name (UPDATE … FROM / DELETE … USING sources) is a passthrough
 * input column, `n + p` (ir.ts "What modify's expressions read").
 */
function returningWithSources(
  statement: { readonly returning?: string[] | "*"; readonly returningItems?: ReturningItem[] },
  table: TableDef,
  target: TargetRows,
  builder: PlanBuilder,
): { returning: { exprs: ScalarExpr[]; columns: RelColumn[] } | null; passthrough: Typed[] } {
  const passthrough: Typed[] = [];
  const width = table.columns.length;
  const scope: Scope = {
    column: (reference) => {
      const typed = target.scope.column(reference);
      if (typed?.expr.op !== "column") return typed;
      const index = typed.expr.index;
      if (sourceOf(index) === target.targetSource) {
        const columnId = target.read.columns[index - target.targetSource * SOURCE_STRIDE];
        const position = table.columns.findIndex((def) => def.id === columnId);
        const def = table.columns[position];
        if (def === undefined) throw new PlanningError(`column "${reference}" vanished`, "XX000");
        return {
          expr: column(position, def),
          nullable: def.nullable,
          big: def.type.kind === "int64",
        };
      }
      passthrough.push(typed);
      return { ...typed, expr: column(width + passthrough.length - 1, typed.expr) };
    },
  };
  const items: Array<{ name: string; typed: Typed }> = [];
  if (statement.returningItems !== undefined) {
    for (const item of statement.returningItems)
      items.push({
        name: item.alias,
        typed: settle(lowerExpression(item.expression, scope, builder.state), builder.parameters),
      });
  } else if (statement.returning === "*") {
    for (const [position, def] of table.columns.entries())
      items.push({
        name: def.name,
        typed: {
          expr: column(position, def),
          nullable: def.nullable,
          big: def.type.kind === "int64",
        },
      });
  } else if (statement.returning !== undefined) {
    for (const name of statement.returning) {
      const typed = scope.column(name);
      if (typed === undefined) throw new PlanningError(`column "${name}" does not exist`, "42703");
      items.push({ name, typed });
    }
  } else return { returning: null, passthrough };
  return {
    returning: {
      exprs: items.map((item) => item.typed.expr),
      columns: items.map(({ name, typed }) => ({
        name,
        type: typed.expr.type,
        nullable: typed.nullable,
        big: typed.big,
      })),
    },
    passthrough,
  };
}

export function lowerUpdate(statement: Update, builder: PlanBuilder): LoweredWrite {
  const table = targetTable(statement.table, builder);
  const qualifiers = [table.name, ...(statement.alias === undefined ? [] : [statement.alias])];
  const target = targetRows(table, statement.alias, statement.predicates, statement.from, builder);
  const scope = target.scope;
  const assigned: Array<{ def: ColumnDef; value: Typed }> = [];
  for (const assignment of statement.assignments) {
    const def = columnNamed(table, assignment.column);
    if (assigned.some((entry) => entry.def.id === def.id))
      throw new PlanningError(`multiple assignments to same column "${def.name}"`, "42601");
    if (def.generatedSql !== null && !isUpdateDefaultAssignment(assignment.expression))
      throw generatedAssignmentError(def);
    if (def.generatedSql !== null) continue;
    const value = isUpdateDefaultAssignment(assignment.expression)
      ? defaultValue(def, builder.state)
      : coerce(
          lowerExpression(assignment.expression, scope, builder.state),
          def.type,
          "assignment",
          builder.parameters,
        );
    assigned.push({ def, value });
  }
  // Generated columns read the assigned values as columns of a first projection
  // [locator, assigned…, current target values…, RETURNING passthrough…] (review B6-p2a finding 3).
  const order = new Map(assigned.map((entry, index) => [entry.def.id, index]));
  const current: Typed[] = [];
  const dependents = dependentGenerated(
    table,
    new Set(order.keys()),
    (def) => ({ expr: column(1 + (order.get(def.id) ?? 0), def), nullable: true, big: false }),
    (_position, def) => {
      const resolved =
        scope.column(`${qualifiers[qualifiers.length - 1] ?? table.name}.${def.name}`) ??
        scope.column(def.name);
      if (resolved === undefined) throw new PlanningError(`column ${def.name} vanished`, "XX000");
      current.push(resolved);
      return { ...resolved, expr: column(1 + assigned.length + current.length - 1, def) };
    },
    qualifiers,
    builder,
  );
  const { returning, passthrough } = returningWithSources(statement, table, target, builder);
  const built = target.build();
  const locatorType = { kind: "locator" as const, tableId: table.id };
  const locator: OutputColumn = { name: "(locator)", type: locatorType, nullable: false };
  const assignedOutput = (def: ColumnDef): OutputColumn => ({
    name: def.name,
    type: def.type,
    nullable: true,
  });
  const extra = (typed: Typed, index: number): OutputColumn => ({
    name: `(passthrough ${String(index + 1)})`,
    type: typed.expr.type,
    nullable: typed.nullable,
  });
  const first: Rel = {
    op: "project",
    id: builder.id(),
    input: built.rel,
    exprs: [
      built.place({ op: "column", index: target.locator, type: locatorType }),
      ...assigned.map((entry) => built.place(entry.value.expr)),
      ...current.map((typed) => built.place(typed.expr)),
      ...passthrough.map((typed) => built.place(typed.expr)),
    ],
    output: [
      locator,
      ...assigned.map((entry) => assignedOutput(entry.def)),
      ...current.map((typed, index) => extra(typed, index)),
      ...passthrough.map(extra),
    ],
  };
  const input: Rel =
    dependents.length === 0 && current.length === 0
      ? first
      : {
          op: "project",
          id: builder.id(),
          input: first,
          exprs: [
            { op: "column", index: 0, type: locatorType },
            ...assigned.map((entry, index) => column(1 + index, entry.def)),
            ...dependents.map((entry) => entry.value.expr),
            ...passthrough.map((typed, index) =>
              column(1 + assigned.length + current.length + index, { type: typed.expr.type }),
            ),
          ],
          output: [
            locator,
            ...[...assigned, ...dependents].map((entry) => assignedOutput(entry.def)),
            ...passthrough.map(extra),
          ],
        };
  return {
    rel: {
      op: "modify",
      id: builder.id(),
      kind: "update",
      tableId: table.id,
      input,
      assigned: [...assigned, ...dependents].map((entry) => entry.def.id),
      // A target row joined to several source rows is updated once, from the first (PostgreSQL).
      onRepeat: statement.from === undefined ? "error" : "first",
      onConflict: null,
      returning: returning?.exprs ?? null,
      ...(passthrough.length === 0 ? {} : { passthrough: passthrough.length }),
      output: outputColumns(returning?.columns ?? []),
    },
    columns: returning?.columns ?? [],
    tableId: table.id,
  };
}

export function lowerDelete(statement: Delete, builder: PlanBuilder): LoweredWrite {
  const table = targetTable(statement.table, builder);
  if (statement.truncate === true) return lowerTruncate(table, builder);
  const target = targetRows(table, statement.alias, statement.predicates, statement.from, builder);
  const { returning, passthrough } = returningWithSources(statement, table, target, builder);
  const built = target.build();
  const locatorType = { kind: "locator" as const, tableId: table.id };
  const input: Rel = {
    op: "project",
    id: builder.id(),
    input: built.rel,
    exprs: [
      built.place({ op: "column", index: target.locator, type: locatorType }),
      ...passthrough.map((typed) => built.place(typed.expr)),
    ],
    output: [
      { name: "(locator)", type: locatorType, nullable: false },
      ...passthrough.map((typed, index) => ({
        name: `(passthrough ${String(index + 1)})`,
        type: typed.expr.type,
        nullable: typed.nullable,
      })),
    ],
  };
  return {
    rel: {
      op: "modify",
      id: builder.id(),
      kind: "delete",
      tableId: table.id,
      input,
      assigned: [],
      onRepeat: statement.from === undefined ? "error" : "first",
      onConflict: null,
      returning: returning?.exprs ?? null,
      ...(passthrough.length === 0 ? {} : { passthrough: passthrough.length }),
      output: outputColumns(returning?.columns ?? []),
    },
    columns: returning?.columns ?? [],
    tableId: table.id,
  };
}

/**
 * MERGE (ir.ts `MergeSpec`): the source LEFT JOIN the target on the match condition, laid out as
 * [source columns…, target locator, target row in catalog order]. Each WHEN clause is a
 * `MergeAction` in order; values are assignment-cast to their columns, and values of earlier
 * columns are read as input columns (`width + i`, ir.ts), so generated columns never evaluate an
 * assigned value twice. A target row matched by two source rows is 21000 (`onRepeat: "error"`).
 */
export function lowerMerge(statement: Merge, builder: PlanBuilder): LoweredWrite {
  const table = targetTable(statement.table, builder);
  const source: TableSource =
    statement.source.sql !== undefined
      ? {
          table: "(merge source)",
          alias: statement.source.alias,
          derived: prepareQuery(parseQuery(statement.source.sql), builder.catalog),
        }
      : builder.catalog.tableByName(statement.source.table ?? "") !== undefined
        ? { table: statement.source.table ?? "", alias: statement.source.alias }
        : {
            table: "(merge source)",
            alias: statement.source.alias,
            derived: prepareQuery(
              parseQuery(`SELECT * FROM "${(statement.source.table ?? "").replaceAll('"', '""')}"`),
              builder.catalog,
            ),
          };
  const targetJoin: JoinPlan = {
    table: table.name,
    alias: statement.alias,
    kind: "left",
    left: { kind: "literal", value: null },
    right: { kind: "literal", value: null },
    on: statement.on,
  };
  const from = lowerFrom(source, [targetJoin], builder, {
    locators: [1],
    fullRows: [0, 1],
    lowerBlock,
  });
  const scope = from.scope;
  const state = builder.state;
  const sourceColumns = from.columnsOf(0);
  const read = from.tableRead(1);
  if (read === undefined) throw new PlanningError("MERGE target is not a table", "42809");
  const targetSource = from.sources[1] ?? -1;
  const width = sourceColumns.length + 1 + table.columns.length;
  // Input layout: source columns, locator, target row (catalog order).
  const layout: number[] = [
    ...sourceColumns.map((entry) => entry.index),
    from.locator(1),
    ...table.columns.map((def) => virtual(targetSource, read.use(def))),
  ];
  const condition = (expression: Expression | undefined): ScalarExpr | null =>
    expression === undefined ? null : asBool(lowerExpression(expression, scope, state), state).expr;
  const typedActions = statement.branches.map((branch) => {
    const when = condition(branch.condition);
    if (branch.when === "matched") {
      if (branch.action.kind === "delete")
        return { when: "matched" as const, condition: when, action: { kind: "delete" as const } };
      const assigned: Array<{ def: ColumnDef; value: Typed }> = [];
      for (const assignment of branch.action.assignments) {
        const def = columnNamed(table, assignment.column);
        if (def.generatedSql !== null) throw generatedAssignmentError(def);
        const value = isUpdateDefaultAssignment(assignment.expression)
          ? defaultValue(def, state)
          : coerce(
              lowerExpression(assignment.expression, scope, state),
              def.type,
              "assignment",
              builder.parameters,
            );
        assigned.push({ def, value });
      }
      const order = new Map(assigned.map((entry, index) => [entry.def.id, index]));
      const dependents = dependentGenerated(
        table,
        new Set(order.keys()),
        (def) => ({
          expr: column(width + (order.get(def.id) ?? 0), def),
          nullable: true,
          big: false,
        }),
        (position, def) => ({
          expr: column(sourceColumns.length + 1 + position, def),
          nullable: true,
          big: def.type.kind === "int64",
        }),
        [table.name, statement.alias],
        builder,
      );
      return {
        when: "matched" as const,
        condition: when,
        action: {
          kind: "update" as const,
          assigned: [...assigned, ...dependents].map((entry) => entry.def.id),
          values: [
            ...assigned.map((entry) => ({ virtual: true, expr: entry.value.expr })),
            ...dependents.map((entry) => ({ virtual: false, expr: entry.value.expr })),
          ],
        },
      };
    }
    const names =
      branch.action.columns.length > 0
        ? branch.action.columns
        : table.columns.map((def) => def.name);
    const supplied = new Map<number, Typed>();
    for (const [index, name] of names.entries()) {
      const def = columnNamed(table, name);
      const expression = branch.action.values[index];
      if (expression === undefined)
        throw new PlanningError("INSERT has more target columns than expressions", "42601");
      if (def.generatedSql !== null && !isUpdateDefaultAssignment(expression))
        throw new PlanningError(
          `cannot insert a non-DEFAULT value into column "${def.name}"`,
          "428C9",
        );
      if (def.generatedSql !== null) continue;
      supplied.set(
        def.id,
        isUpdateDefaultAssignment(expression)
          ? defaultValue(def, state)
          : coerce(
              lowerExpression(expression, scope, state),
              def.type,
              "assignment",
              builder.parameters,
            ),
      );
    }
    const values: Array<{ virtual: boolean; expr: ScalarExpr }> = [];
    for (const [position, def] of table.columns.entries()) {
      if (def.generatedSql === null) {
        values.push({
          virtual: true,
          expr: (supplied.get(def.id) ?? defaultValue(def, state)).expr,
        });
        continue;
      }
      // An earlier column is read as an input column; a later one only if it is not volatile.
      const row = rowScope(table, [table.name, statement.alias], (other, otherDef) => {
        if (other < position)
          return { expr: column(width + other, otherDef), nullable: true, big: false };
        throw unsupported(
          "merge",
          `generated column ${def.name} reads the later column ${otherDef.name}`,
        );
      });
      values.push({ virtual: false, expr: generatedValue(def, row, state).expr });
    }
    return {
      when: "notMatched" as const,
      condition: when,
      action: { kind: "insert" as const, values },
    };
  });
  const built = from.build([]);
  const positions = new Map(built.layout.map((index, position) => [index, position]));
  const toInput = new Map(layout.map((index, position) => [index, position]));
  const place = (expr: ScalarExpr) =>
    mapColumns(expr, (index) => {
      const position = positions.get(index);
      if (position === undefined)
        throw new PlanningError(`column ${String(index)} vanished`, "XX000");
      return position;
    });
  const asInput = (expr: ScalarExpr) =>
    mapColumns(expr, (index) => {
      const position = toInput.get(index);
      if (position === undefined)
        throw new PlanningError(`MERGE reads column ${String(index)} outside its input`, "XX000");
      return position;
    });
  const locatorType = { kind: "locator" as const, tableId: table.id };
  const inputColumns: OutputColumn[] = [
    ...sourceColumns.map((entry) => ({
      name: entry.name,
      type: entry.typed.expr.type,
      nullable: entry.typed.nullable,
    })),
    { name: "(locator)", type: locatorType, nullable: true },
    ...table.columns.map((def) => ({ name: def.name, type: def.type, nullable: true })),
  ];
  const input: Rel = {
    op: "project",
    id: builder.id(),
    input: built.rel,
    exprs: layout.map((index, position) =>
      place({ op: "column", index, type: inputColumns[position]?.type ?? locatorType }),
    ),
    output: inputColumns,
  };
  const actions = typedActions.map((action) => {
    const cond = action.condition === null ? null : asInput(action.condition);
    if (action.action.kind === "delete")
      return { when: "matched" as const, condition: cond, action: { kind: "delete" as const } };
    const values = action.action.values.map((value) =>
      value.virtual ? asInput(value.expr) : value.expr,
    );
    return action.action.kind === "update"
      ? {
          when: "matched" as const,
          condition: cond,
          action: { kind: "update" as const, assigned: action.action.assigned, values },
        }
      : {
          when: "notMatched" as const,
          condition: cond,
          action: { kind: "insert" as const, values },
        };
  });
  return {
    rel: {
      op: "modify",
      id: builder.id(),
      kind: "merge",
      tableId: table.id,
      input,
      assigned: [],
      onRepeat: "error",
      onConflict: null,
      returning: null,
      merge: { sourceColumns: sourceColumns.length, actions },
      output: [],
    },
    columns: [],
    tableId: table.id,
  };
}

/**
 * TRUNCATE: the `truncate` modify kind (ir.ts), which removes every row. A table another table's
 * foreign key references is refused as PostgreSQL refuses it without CASCADE (0A000).
 */
function lowerTruncate(table: TableDef, builder: PlanBuilder): LoweredWrite {
  const referenced = builder.catalog.tables.find(
    (other) =>
      other.id !== table.id && other.foreignKeys.some((key) => key.parentTableId === table.id),
  );
  if (referenced !== undefined)
    throw new PlanningError(
      `cannot truncate a table referenced in a foreign key constraint: "${referenced.name}" references "${table.name}"`,
      "0A000",
    );
  builder.read(table);
  return {
    rel: {
      op: "modify",
      id: builder.id(),
      kind: "truncate",
      tableId: table.id,
      input: { op: "values", id: builder.id(), rows: [], output: [] },
      assigned: [],
      onRepeat: "error",
      onConflict: null,
      returning: null,
      output: [],
    },
    columns: [],
    tableId: table.id,
  };
}

/**
 * A client batch write (`Lowerer.lowerWrite`): an `input` relation of the named columns, typed
 * as the table's columns, completed like INSERT. Upsert conflicts on the primary key and updates
 * the supplied non-key columns. Update and delete batches need a lookup join of the client rows
 * against the key index, which the IR does not have yet (`batchWrite`).
 */
export function lowerBatchWrite(
  shape: {
    readonly table: string;
    readonly op: "insert" | "upsert" | "update" | "delete";
    readonly names: readonly string[];
  },
  builder: PlanBuilder,
): LoweredWrite {
  if (shape.op === "update" || shape.op === "delete")
    throw unsupported("batchWrite", `${shape.op} batches (they need a lookup join)`);
  const table = targetTable(shape.table, builder);
  const targets = shape.names.map((name) => columnNamed(table, name));
  for (const def of targets)
    if (def.generatedSql !== null)
      throw new PlanningError(
        `cannot insert a non-DEFAULT value into column "${def.name}"`,
        "428C9",
      );
  const rows: Rel = {
    op: "input",
    id: builder.id(),
    output: targets.map((def) => ({ name: def.name, type: def.type, nullable: true })),
  };
  const input = completeRows(
    table,
    rows,
    (def) => {
      const position = targets.indexOf(def);
      return position === -1
        ? undefined
        : { expr: column(position, def), nullable: true, big: false };
    },
    builder,
  );
  let onConflict: Extract<Rel, { op: "modify" }>["onConflict"] = null;
  if (shape.op === "upsert") {
    const primary = table.indexes.find((index) => index.id === table.primaryKey);
    if (primary === undefined)
      throw new PlanningError(`table "${table.name}" has no primary key to upsert on`, "42P10");
    const width = table.columns.length;
    const supplied = table.columns.flatMap((def, position) =>
      targets.includes(def) && !primary.columnIds.includes(def.id)
        ? [{ def, value: { expr: column(width + position, def), nullable: true, big: false } }]
        : [],
    );
    onConflict = {
      arbiters: [primary.id],
      indexId: primary.id,
      action: "update",
      // The supplied columns, then the generated columns that read them (finding 6).
      assignments: conflictAssignments(table, supplied, [table.name], builder),
      where: null,
    };
  }
  return {
    rel: {
      op: "modify",
      id: builder.id(),
      kind: onConflict === null ? "insert" : "upsert",
      tableId: table.id,
      input,
      assigned: [],
      onRepeat: "error",
      onConflict,
      returning: null,
      output: [],
    },
    columns: [],
    tableId: table.id,
  };
}

/** For CREATE TABLE AS: the INSERT … SELECT text that fills the new table. */
export function insertSelectText(
  table: string,
  columns: readonly string[],
  querySql: string,
): string {
  const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
  return `INSERT INTO ${quote(table)} (${columns.map(quote).join(", ")}) ${querySql}`;
}
