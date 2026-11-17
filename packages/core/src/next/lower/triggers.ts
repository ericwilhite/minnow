/** CREATE-time executable dependencies, frozen OLD/NEW types and body assignment legality.
 * The shipped trigger grammar is the contract, not PostgreSQL CREATE FUNCTION syntax.
 */
import type { CatalogChange, CatalogView, ColumnDef, TableDef } from "../access/catalog.js";
import {
  validateTriggerCatalog,
  TRIGGER_SYSTEM_PREFIX,
  type TriggerBody,
  type TriggerColumn,
} from "../access/trigger-def.js";
import type { CompiledStatement } from "../../engine/query.js";
import { isDefaultInsertValue, isDeferredInsertExpression } from "../../engine/query.js";
import type { PhysicalType } from "../types.js";
import type { LoweredModify, LoweringContext } from "./lowerer.js";
import { parseStatement } from "./frontend.js";
import { PlanningError, unsupported } from "./errors.js";
import { ruleReferences } from "./rules.js";
import { expressionObjects } from "./expression-objects.js";
type Create = Extract<CompiledStatement, { kind: "create-trigger" }>;
const column = (table: TableDef, name: string): ColumnDef => {
  const c = table.columns.find((c) => c.name === name);
  if (!c) throw new PlanningError(`column "${name}" does not exist`, "42703");
  return c;
};
const ref = (c: ColumnDef): TriggerColumn => ({ columnId: c.id, name: c.name, type: c.type });
const hasDefault = (c: ColumnDef) =>
  c.nullable || c.generatedSql !== null || c.defaultSql !== null || c.identitySequence !== null;
export function createTriggerChange(
  statement: Create,
  context: LoweringContext,
  compile: (sql: string, types: readonly PhysicalType[]) => LoweredModify,
): CatalogChange {
  const catalog = context.catalog,
    owner = catalog.tableByName(statement.table),
    input = statement.trigger;
  if (!owner) throw new PlanningError(`relation "${statement.table}" does not exist`, "42P01");
  if (input.name.startsWith(TRIGGER_SYSTEM_PREFIX))
    throw new PlanningError(`trigger name "${input.name}" uses a reserved system prefix`, "42939");
  if (catalog.triggers.some((t) => t.name === input.name))
    throw new PlanningError(`trigger "${input.name}" already exists`, "42710");
  const statements = input.statements.map((body): TriggerBody => {
    const bindings = body.bindings.map((binding) => {
      if (binding.source === "old" && input.event === "insert")
        throw new PlanningError("INSERT triggers have no OLD row", "42703");
      if (binding.source === "new" && input.event === "delete")
        throw new PlanningError("DELETE triggers have no NEW row", "42703");
      return { ...ref(column(owner, binding.column)), source: binding.source };
    });
    const parsed = parseStatement(body.sql);
    if (parsed.kind !== "insert" && parsed.kind !== "update" && parsed.kind !== "delete")
      throw unsupported("trigger", "a non-DML trigger body");
    const target = catalog.tableByName(parsed.table);
    if (!target) throw new PlanningError(`relation "${parsed.table}" does not exist`, "42P01");
    if (target.id === owner.id)
      throw new PlanningError("A trigger cannot write to its own table", "0A000");
    if (parsed.kind === "insert" ? target.primaryKey !== null : target.primaryKey === null)
      throw unsupported(
        "trigger",
        parsed.kind === "insert"
          ? "trigger INSERT into a keyed table"
          : "trigger mutation of a keyless table",
      );
    const columns = new Map<number, TriggerColumn>(),
      insertColumnIds: number[] = [],
      defaults = new Set<number>(),
      assignedColumnIds: number[] = [];
    const use = (name: string) => {
      const c = column(target, name);
      columns.set(c.id, ref(c));
      return c;
    };
    let implicitArity: number | null = null;
    if (parsed.kind === "insert") {
      if (parsed.query || parsed.onConflict || parsed.returning)
        throw unsupported("trigger", "a trigger INSERT with query/ON CONFLICT/RETURNING");
      const names =
        parsed.defaultValues === true
          ? []
          : parsed.columns.length
            ? parsed.columns
            : target.columns.map((c) => c.name);
      if (parsed.defaultValues !== true && !parsed.columns.length)
        implicitArity = target.columns.length;
      const targets = names.map(use);
      for (const target of targets) insertColumnIds.push(target.id);
      if (new Set(insertColumnIds).size !== insertColumnIds.length)
        throw new PlanningError("column specified more than once", "42701");
      for (const row of parsed.rows) {
        if (row.length !== targets.length)
          throw new PlanningError("Trigger body INSERT must match its target columns", "42601");
        row.forEach((value, i) => {
          const c = targets[i];
          if (!c) throw new PlanningError("missing trigger INSERT column", "XX000");
          if (isDefaultInsertValue(value)) {
            defaults.add(c.id);
            if (!hasDefault(c))
              throw new PlanningError(
                `Trigger body INSERT uses DEFAULT without a default: ${c.name}`,
                "23502",
              );
          } else if (c.generatedSql !== null)
            throw new PlanningError(
              `Trigger body INSERT assigns generated column: ${c.name}`,
              "428C9",
            );
          // The lowerer below resolves/type-checks deferred expressions, including exact names.
          if (isDeferredInsertExpression(value))
            for (const r of ruleReferences(value.expression))
              throw new PlanningError(
                `Trigger body INSERT cannot read target column: ${r.name}`,
                "42703",
              );
        });
      }
      for (const c of target.columns)
        if (!insertColumnIds.includes(c.id) && !hasDefault(c))
          throw new PlanningError(
            `Trigger body INSERT omits a non-nullable column without a default: ${c.name}`,
            "23502",
          );
    } else {
      if (parsed.from !== undefined || parsed.returning !== undefined)
        throw unsupported("trigger", "a trigger mutation with other sources or RETURNING");
      if (parsed.kind === "update")
        for (const a of parsed.assignments) {
          const c = use(a.column);
          assignedColumnIds.push(c.id);
          if (c.generatedSql !== null)
            throw new PlanningError(
              `Trigger body UPDATE assigns generated column: ${c.name}`,
              "428C9",
            );
          for (const r of ruleReferences(a.expression)) {
            if (r.qualifier !== undefined && r.qualifier !== target.name)
              throw new PlanningError(`column "${r.qualifier}.${r.name}" does not exist`, "42703");
            use(r.name);
          }
        }
      for (const p of parsed.predicates)
        for (const e of [p.left, p.right])
          for (const r of ruleReferences(e)) {
            if (r.qualifier !== undefined && r.qualifier !== target.name)
              throw new PlanningError(`column "${r.qualifier}.${r.name}" does not exist`, "42703");
            use(r.name);
          }
    }
    const plan = compile(
      body.sql,
      bindings.map((b) => b.type),
    );
    if (
      plan.parameters.length !== bindings.length ||
      plan.writes.length !== 1 ||
      plan.writes[0] !== target.id
    )
      throw new PlanningError("Trigger body bindings/target changed during lowering", "XX000");
    return {
      sql: body.sql,
      kind: parsed.kind,
      targetTableId: target.id,
      targetName: target.name,
      bindings,
      columns: [...columns.values()].sort((a, b) => a.columnId - b.columnId),
      insertColumnIds,
      defaultColumnIds: [...defaults].sort((a, b) => a - b),
      implicitArity,
      assignedColumnIds,
      ...expressionObjects(parsed, catalog),
    };
  });
  return {
    kind: "createTrigger",
    trigger: {
      name: input.name,
      tableId: owner.id,
      event: input.event,
      timing: input.timing,
      dialectVersion: 1,
      statements,
    },
  };
}
/** Fail dependent DDL before execution as well as during catalog/WAL validation. */
export function protectTriggerChange(change: CatalogChange | null, catalog: CatalogView): void {
  if (
    !change ||
    !catalog.triggers.length ||
    change.kind === "createTrigger" ||
    change.kind === "dropTrigger"
  )
    return;
  let tables = catalog.tables,
    triggers = catalog.triggers,
    types = catalog.types,
    sequences = catalog.sequences;
  if (change.kind === "dropTable") {
    tables = tables.filter((t) => t.id !== change.tableId);
    triggers = triggers.filter((t) => t.tableId !== change.tableId);
  }
  if (change.kind === "dropType") types = types.filter((t) => t.id !== change.typeId);
  if (change.kind === "dropSequence")
    sequences = sequences.filter((s) => s.id !== change.sequenceId);
  if (change.kind === "alterTable") {
    const old = catalog.table(change.tableId);
    if (!old) return;
    const candidate = {
      ...old,
      name: change.table.name,
      columns: change.table.columns.map((c) => ({
        id: c.id ?? -1,
        name: c.name,
        type: c.type,
        declared: c.declared,
        nullable: c.nullable,
        defaultSql: c.defaultSql,
        generatedSql: c.generatedSql,
        missing: c.missing,
        identitySequence: c.identity ? -1 : c.identitySequence,
      })),
    };
    tables = tables.map((t) => (t.id === old.id ? candidate : t));
  }
  try {
    validateTriggerCatalog({ tables, triggers, types, sequences });
  } catch (e) {
    if (e instanceof Error && "code" in e && e.code === "MINNOW_STORE_CORRUPT")
      throw new PlanningError(
        `cannot change catalog: a stored trigger depends on it (${e.message})`,
        "2BP01",
      );
    throw e;
  }
}
