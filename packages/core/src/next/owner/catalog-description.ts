/** Project immutable catalog facts without leaking private records, vectors or mutation capability. */
import type { CatalogView } from "../access/catalog.js";
import type { PhysicalType } from "../types.js";
import { sqlTypeName } from "../lower/result.js";
import { stringBytes, OBJECT_OVERHEAD_BYTES } from "../memory/governor.js";
import type { CatalogDescription } from "./inspection.js";
export function describeCatalog(catalog: CatalogView): CatalogDescription {
  return {
    version: 1,
    catalogVersion: catalog.version,
    tables: catalog.tables.map((table) => ({
      id: table.id,
      name: table.name,
      schemaVersion: table.schemaVersion,
      managed: table.managed === true,
      primaryKey: table.primaryKey,
      columns: table.columns.map((column) => ({
        id: column.id,
        name: column.name,
        sqlType: sqlTypeName(column.type),
        typeId: column.typeId ?? null,
        autoIncrement: column.autoIncrement === true,
        nullable: column.nullable,
        defaultSql: column.defaultSql,
        generatedSql: column.generatedSql,
        identity:
          column.identitySequence === null
            ? null
            : { sequenceId: column.identitySequence, always: column.identityAlways === true },
        enumValues: column.allowedValues
          ? [...column.allowedValues]
          : column.type.kind === "enum"
            ? [...column.type.labels]
            : null,
      })),
      indexes: table.indexes.map((index) => ({
        id: index.id,
        name: index.name,
        columnIds: [...index.columnIds],
        directions: index.columnIds.map((_, i) => index.directions?.[i] ?? "asc"),
        unique: index.unique,
        kind: index.kind,
        role: index.role,
        state: index.invalid === true ? "invalid" : "ready",
      })),
      checks: table.checks.map((check) => ({
        id: check.id,
        name: check.name,
        sql: check.conditionSql,
        columnIds: [...check.columnIds],
      })),
      foreignKeys: table.foreignKeys.map((key) => ({
        id: key.id,
        name: key.name,
        columnIds: [...key.columnIds],
        parentTableId: key.parentTableId,
        parentIndexId: key.parentIndexId,
        parentName: catalog.table(key.parentTableId)?.name ?? key.parentName ?? null,
        supportIndexId: key.supportIndexId,
        onDelete: key.onDelete,
        onUpdate: key.onUpdate,
        enforced: key.enforced,
      })),
    })),
    views: catalog.views.map((view) => ({
      id: view.id,
      name: view.name,
      sql: view.bodySql,
      managed: view.managed === true,
      columns: (view.outputs ?? []).map((column) => ({
        name: column.name,
        sqlType: sqlTypeName(column.type),
        nullable: column.nullable,
      })),
      dependencies: (view.dependencies ?? []).map((dep) => ({
        ...dep,
        columnIds: [...dep.columnIds],
      })),
    })),
    types: catalog.types.map((type) => ({
      id: type.id,
      name: type.name,
      labels: [...type.labels],
    })),
    sequences: catalog.sequences.map((sequence) => ({
      id: sequence.id,
      name: sequence.name,
      start: sequence.start.toString(),
      increment: sequence.increment.toString(),
      min: sequence.min.toString(),
      max: sequence.max.toString(),
      cycle: sequence.cycle,
      ownedBy: sequence.ownedBy === null ? null : { ...sequence.ownedBy },
    })),
    fts: catalog.fts.map((def) => ({
      id: def.id,
      tableId: def.tableId,
      columnId: def.columnId,
      tokenizerVersion: def.tokenizerVersion,
      rendererVersion: def.rendererVersion,
      scorerVersion: def.scorerVersion,
      configVersion: def.config.version,
    })),
    triggers: catalog.triggers.map((trigger) => ({
      id: trigger.id,
      name: trigger.name,
      tableId: trigger.tableId,
      timing: trigger.timing,
      event: trigger.event,
      statements: trigger.statements.map((statement) => ({
        sql: statement.sql,
        bindings: statement.bindings.map((binding) => ({
          source: binding.source,
          columnId: binding.columnId,
          name: binding.name,
        })),
      })),
    })),
  };
}

/** Two metadata copies under the governor's object/string accounting, never row/missing vectors. */
export function catalogDescriptionBytes(catalog: CatalogView): number {
  let size = 0;
  const record = (fields: number) => {
    size += OBJECT_OVERHEAD_BYTES + 8 * fields;
  };
  const array = (count: number) => {
    size += OBJECT_OVERHEAD_BYTES + 8 * count;
  };
  const text = (value: string) => {
    size += stringBytes(value);
  };
  const type = (value: PhysicalType) => {
    text(sqlTypeName(value));
  };
  record(8);
  array(catalog.tables.length);
  for (const table of catalog.tables) {
    record(9);
    text(table.name);
    array(table.columns.length);
    for (const column of table.columns) {
      record(10);
      text(column.name);
      type(column.type);
      if (column.defaultSql !== null) text(column.defaultSql);
      if (column.generatedSql !== null) text(column.generatedSql);
      if (column.identitySequence !== null) record(2);
      const labels =
        column.allowedValues ?? (column.type.kind === "enum" ? column.type.labels : null);
      if (labels !== null) {
        array(labels.length);
        for (const label of labels) text(label);
      }
    }
    array(table.indexes.length);
    for (const index of table.indexes) {
      record(8);
      text(index.name);
      text(index.kind);
      text(index.role);
      text(index.invalid ? "invalid" : "ready");
      array(index.columnIds.length);
      array(index.columnIds.length);
      for (let i = 0; i < index.columnIds.length; i++) text(index.directions?.[i] ?? "asc");
    }
    array(table.checks.length);
    for (const check of table.checks) {
      record(4);
      text(check.name);
      text(check.conditionSql);
      array(check.columnIds.length);
    }
    array(table.foreignKeys.length);
    for (const key of table.foreignKeys) {
      record(10);
      text(key.name);
      const parent = catalog.table(key.parentTableId)?.name ?? key.parentName;
      if (parent !== undefined) text(parent);
      text(key.onDelete);
      text(key.onUpdate);
      array(key.columnIds.length);
    }
  }
  array(catalog.views.length);
  for (const view of catalog.views) {
    record(6);
    text(view.name);
    text(view.bodySql);
    array(view.outputs?.length ?? 0);
    for (const column of view.outputs ?? []) {
      record(3);
      text(column.name);
      type(column.type);
    }
    array(view.dependencies?.length ?? 0);
    for (const dependency of view.dependencies ?? []) {
      record(3);
      text(dependency.kind);
      array(dependency.columnIds.length);
    }
  }
  array(catalog.types.length);
  for (const def of catalog.types) {
    record(3);
    text(def.name);
    array(def.labels.length);
    for (const label of def.labels) text(label);
  }
  array(catalog.sequences.length);
  for (const def of catalog.sequences) {
    record(8);
    text(def.name);
    // Sequence values are validated int64: four decimal strings, at most twenty characters each.
    size += 4 * (24 + 2 * 20);
    if (def.ownedBy !== null) record(2);
  }
  array(catalog.fts.length);
  size += catalog.fts.length * (OBJECT_OVERHEAD_BYTES + 8 * 7);
  array(catalog.triggers.length);
  for (const trigger of catalog.triggers) {
    record(6);
    text(trigger.name);
    text(trigger.timing);
    text(trigger.event);
    array(trigger.statements.length);
    for (const statement of trigger.statements) {
      record(2);
      text(statement.sql);
      array(statement.bindings.length);
      for (const binding of statement.bindings) {
        record(3);
        text(binding.source);
        text(binding.name);
      }
    }
  }
  return 2 * size;
}
