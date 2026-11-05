/**
 * INSERT, UPDATE, DELETE and batch writes as `modify` plans (ir.ts; access/modify.ts shapes):
 * - insert: rows in catalog order, every value assignment-cast to its column, omitted and DEFAULT
 *   columns filled with their defaults, then stored generated columns computed over the row;
 * - update: the target's locator and the assigned values (plus generated columns that read an
 *   assigned column), found by key lookup or scan;
 * - delete: the target's locators;
 * - RETURNING over the affected table row.
 * UPDATE … FROM, DELETE … USING and MERGE are milestone M3 (`UnsupportedPlanError`).
 */
import type { ColumnDef, IndexDef, TableDef } from "../access/catalog.js";
import type { OutputColumn, Rel, ScalarExpr } from "../ir/ir.js";
import {
  isDefaultInsertValue,
  isDeferredInsertExpression,
  isUpdateDefaultAssignment,
  type CompiledStatement,
  type InsertValue,
  type ReturningItem,
} from "../../engine/query.js";
import type { Expression, Predicate } from "../../plan/model.js";
import { coerce, settle, type Typed } from "./coerce.js";
import { PlanningError, unsupported } from "./errors.js";
import { asBool, lowerCondition, lowerExpression, type Scope } from "./expressions.js";
import { prepareQuery } from "./frontend.js";
import {
  andOf,
  findKeyAccess,
  lowerBlock,
  outputColumns,
  TableRead,
  type PlanBuilder,
  type RelColumn,
} from "./query.js";
import { defaultValue, generatedValue, rowScope, ruleColumns, tableRowScope } from "./rules.js";
import { nullLiteral } from "./literals.js";

type Insert = Extract<CompiledStatement, { kind: "insert" }>;
type Update = Extract<CompiledStatement, { kind: "update" }>;
type Delete = Extract<CompiledStatement, { kind: "delete" }>;

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

/** The target rows of an UPDATE or DELETE: key lookup or scan with the locator, filtered. */
function targetRows(
  table: TableDef,
  alias: string | undefined,
  predicates: readonly Predicate[],
  builder: PlanBuilder,
  typeMore: (scope: Scope) => void,
): { rel: Rel; locator: number; scope: Scope; read: TableRead } {
  const read = new TableRead(table, alias ?? table.name);
  const scope = read.scope();
  const conjuncts = predicates.map((predicate) =>
    asBool(
      lowerCondition(
        predicate.operator,
        predicate.left,
        predicate.right,
        predicate.escape,
        scope,
        builder.state,
      ),
      builder.state,
    ),
  );
  typeMore(scope);
  builder.read(table);
  return {
    rel: keyedOrScanned(read, conjuncts, predicates, builder),
    locator: read.columns.length,
    scope,
    read,
  };
}

/** A table read with its locator: keyLookup when a unique key is fixed, else scan + filter. */
function keyedOrScanned(
  read: TableRead,
  conjuncts: readonly Typed[],
  predicates: readonly Predicate[],
  builder: PlanBuilder,
): Rel {
  const access = findKeyAccess(read, predicates, builder);
  const output = read.output(true);
  if (access !== null) {
    const lookup: Rel = {
      op: "keyLookup",
      id: builder.id(),
      tableId: read.table.id,
      indexId: access.index.id,
      keys: access.keys,
      columns: [...read.columns],
      emitLocator: true,
      output,
    };
    const residual = andOf(conjuncts.filter((_, position) => !access.used.has(position)));
    return residual === null
      ? lookup
      : { op: "filter", id: builder.id(), input: lookup, predicate: residual, output };
  }
  return {
    op: "scan",
    id: builder.id(),
    tableId: read.table.id,
    columns: [...read.columns],
    filter: andOf(conjuncts),
    runtimeFilters: [],
    emitLocator: true,
    output,
  };
}

export function lowerUpdate(statement: Update, builder: PlanBuilder): LoweredWrite {
  if (statement.from !== undefined) throw unsupported("join", "UPDATE … FROM (milestone M3)");
  const table = targetTable(statement.table, builder);
  const qualifiers = [table.name, ...(statement.alias === undefined ? [] : [statement.alias])];
  const assigned: Array<{ def: ColumnDef; value: Typed }> = [];
  let dependents: Array<{ readonly def: ColumnDef; readonly value: Typed }> = [];
  // Generated columns read the assigned values as columns of a first projection
  // [locator, assigned…, every target column]: positions 1 + j and 1 + k + scan position.
  const passthrough = (scanPosition: number) => 1 + assigned.length + scanPosition;
  const target = targetRows(table, statement.alias, statement.predicates, builder, (scope) => {
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
    const order = new Map(assigned.map((entry, index) => [entry.def.id, index]));
    dependents = dependentGenerated(
      table,
      new Set(order.keys()),
      (def) => ({ expr: column(1 + (order.get(def.id) ?? 0), def), nullable: true, big: false }),
      (_position, def) => {
        const resolved = scope.column(def.name);
        if (resolved?.expr.op !== "column")
          throw new PlanningError(`column ${def.name} vanished`, "XX000");
        return { ...resolved, expr: column(passthrough(resolved.expr.index), def) };
      },
      qualifiers,
      builder,
    );
  });
  const locatorType = { kind: "locator" as const, tableId: table.id };
  const locator: OutputColumn = { name: "(locator)", type: locatorType, nullable: false };
  const assignedOutput = (def: ColumnDef): OutputColumn => ({
    name: def.name,
    type: def.type,
    nullable: true,
  });
  const scanned = target.rel.output.slice(0, target.read.columns.length);
  let input: Rel = {
    op: "project",
    id: builder.id(),
    input: target.rel,
    exprs: [
      { op: "column", index: target.locator, type: locatorType },
      ...assigned.map((entry) => entry.value.expr),
      ...(dependents.length === 0
        ? []
        : scanned.map((output, position): ScalarExpr => ({
            op: "column",
            index: position,
            type: output.type,
          }))),
    ],
    output: [
      locator,
      ...assigned.map((entry) => assignedOutput(entry.def)),
      ...(dependents.length === 0 ? [] : scanned),
    ],
  };
  if (dependents.length > 0)
    input = {
      op: "project",
      id: builder.id(),
      input,
      exprs: [
        { op: "column", index: 0, type: locatorType },
        ...assigned.map((entry, index) => column(1 + index, entry.def)),
        ...dependents.map((entry) => entry.value.expr),
      ],
      output: [locator, ...[...assigned, ...dependents].map((entry) => assignedOutput(entry.def))],
    };
  const returning = returningList(statement, table, statement.alias, builder);
  return {
    rel: {
      op: "modify",
      id: builder.id(),
      kind: "update",
      tableId: table.id,
      input,
      assigned: [...assigned, ...dependents].map((entry) => entry.def.id),
      onRepeat: "error",
      onConflict: null,
      returning: returning?.exprs ?? null,
      output: outputColumns(returning?.columns ?? []),
    },
    columns: returning?.columns ?? [],
    tableId: table.id,
  };
}

export function lowerDelete(statement: Delete, builder: PlanBuilder): LoweredWrite {
  if (statement.from !== undefined) throw unsupported("join", "DELETE … USING (milestone M3)");
  const table = targetTable(statement.table, builder);
  if (statement.truncate === true) return lowerTruncate(table, builder);
  const target = targetRows(table, statement.alias, statement.predicates, builder, () => undefined);
  const locatorType = { kind: "locator" as const, tableId: table.id };
  const input: Rel = {
    op: "project",
    id: builder.id(),
    input: target.rel,
    exprs: [{ op: "column", index: target.locator, type: locatorType }],
    output: [{ name: "(locator)", type: locatorType, nullable: false }],
  };
  const returning = returningList(statement, table, statement.alias, builder);
  return {
    rel: {
      op: "modify",
      id: builder.id(),
      kind: "delete",
      tableId: table.id,
      input,
      assigned: [],
      onRepeat: "error",
      onConflict: null,
      returning: returning?.exprs ?? null,
      output: outputColumns(returning?.columns ?? []),
    },
    columns: returning?.columns ?? [],
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
 * the supplied non-key columns. Update and delete batches need a lookup join (milestone M3).
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
    throw unsupported("batchWrite", `${shape.op} batches (milestone M3)`);
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
