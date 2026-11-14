/**
 * An in-memory `CatalogView` (access/catalog.ts) that applies `CatalogChange`s: lowering's
 * tests, and a stand-in for the write path's catalog until that exists (milestone M1). It assigns
 * ids as the contract says (never reused) and bumps `version` on every change. It does not
 * validate data (NOT NULL over existing rows, type changes): that is the write path's job.
 */
import { validateTriggerCatalog, type TriggerDef } from "../access/trigger-def.js";
import type {
  CatalogChange,
  CatalogView,
  CheckDef,
  ColumnDef,
  ForeignKeyDef,
  IndexDef,
  SequenceDef,
  TableDef,
  TableInput,
  ViewDef,
  TypeDef,
} from "../access/catalog.js";
import { CatalogChangeError } from "../access/catalog.js";
import { resolveDependencies, tableUsesObject } from "../access/catalog-dependencies.js";
import { ownedSequenceName } from "../access/catalog-names.js";
import type { PhysicalType } from "../types.js";

/** keys/key-encoding.ts `indexKeyKind` without the import lowering's layer forbids: enums are equality-only. */
const indexKeyKind = (type: PhysicalType): IndexDef["kind"] =>
  type.kind === "enum" ? "equality" : "ordered";

class Snapshot implements CatalogView {
  readonly fts = [];
  constructor(
    readonly version: number,
    readonly tables: readonly TableDef[],
    readonly sequences: readonly SequenceDef[],
    readonly views: readonly ViewDef[],
    readonly types: readonly TypeDef[] = [],
    readonly triggers: readonly TriggerDef[] = [],
  ) {}
  table(tableId: number): TableDef | undefined {
    return this.tables.find((table) => table.id === tableId);
  }
  tableByName(name: string): TableDef | undefined {
    return this.tables.find((table) => table.name === name);
  }
  sequence(sequenceId: number): SequenceDef | undefined {
    return this.sequences.find((sequence) => sequence.id === sequenceId);
  }
  type(id: number): TypeDef | undefined {
    return this.types.find((type) => type.id === id);
  }
  typeByName(name: string): TypeDef | undefined {
    return this.types.find((type) => type.name === name);
  }
}

export class MemoryCatalog {
  #nextId = 1;
  #view: CatalogView = new Snapshot(1, [], [], []);

  get view(): CatalogView {
    return this.#view;
  }

  #id(): number {
    return this.#nextId++;
  }

  #nameTaken(name: string): boolean {
    const view = this.#view;
    return (
      view.tableByName(name) !== undefined ||
      view.views.some((entry) => entry.name === name) ||
      view.sequences.some((entry) => entry.name === name) ||
      view.tables.some((table) => table.indexes.some((index) => index.name === name))
    );
  }

  #table(input: TableInput, previous: TableDef | null): TableDef {
    const id = previous?.id ?? this.#id();
    const columns: ColumnDef[] = input.columns.map(
      ({ identity: _identity, defaultDependencies: _d, generatedDependencies: _g, ...column }) => {
        void _identity;
        void _d;
        void _g;
        return { ...column, id: column.id ?? this.#id() };
      },
    );
    const ids = columns.map((column) => column.id);
    input.columns.forEach((source, i) => {
      const column = columns[i];
      if (!column) throw new CatalogChangeError("Missing column");
      columns[i] = {
        ...column,
        ...(source.defaultDependencies === undefined
          ? {}
          : { defaultDependencies: resolveDependencies(source.defaultDependencies, ids) }),
        ...(source.generatedDependencies === undefined
          ? {}
          : { generatedDependencies: resolveDependencies(source.generatedDependencies, ids) }),
      };
    });
    const columnId = (position: number): number => {
      const column = columns[position];
      if (column === undefined)
        throw new CatalogChangeError(`No column at position ${String(position)}`);
      return column.id;
    };
    const kindOf = (ids: readonly number[]): IndexDef["kind"] => {
      for (const columnId of ids) {
        const type = columns.find((column) => column.id === columnId)?.type;
        if (type !== undefined && indexKeyKind(type) === "equality") return "equality";
      }
      return "ordered";
    };
    const indexes: IndexDef[] = [];
    const keep =
      previous?.indexes.filter(
        (index) => index.role === "secondary" || index.role === "foreignKey",
      ) ?? [];
    let primaryKey: number | null = null;
    if (input.primaryKey !== null) {
      const ids = input.primaryKey.map(columnId);
      const existing = previous?.indexes.find((index) => index.id === previous.primaryKey);
      const indexId = existing?.id ?? this.#id();
      indexes.push({
        id: indexId,
        name: `${input.name}_pkey`,
        tableId: id,
        columnIds: ids,
        unique: true,
        kind: kindOf(ids),
        role: "primary",
      });
      primaryKey = indexId;
    }
    for (const unique of input.unique) {
      const ids = unique.columns.map(columnId);
      indexes.push({
        id: unique.id ?? this.#id(),
        name: unique.name,
        tableId: id,
        columnIds: ids,
        unique: true,
        kind: kindOf(ids),
        role: "unique",
      });
    }
    const liveIds = new Set(columns.map((column) => column.id));
    for (const index of keep)
      if (index.columnIds.every((columnId) => liveIds.has(columnId))) indexes.push(index);
    // R17.7: "self" names the table being defined; "primary" and { unique } name a key of the
    // parent (this table, or the one `parentTableId` names), as the write path's catalog does.
    const foreignKeys: ForeignKeyDef[] = input.foreignKeys.map((key) => {
      const self = key.parentTableId === "self";
      const parent = self ? undefined : this.#view.table(key.parentTableId);
      if (!self && parent === undefined)
        throw new CatalogChangeError(`Foreign key ${key.name} names a missing table`);
      const reference = key.parentIndexId;
      const parentIndexes = parent?.indexes ?? indexes;
      const found =
        typeof reference === "number"
          ? reference
          : reference === "primary"
            ? parent === undefined
              ? primaryKey
              : parent.primaryKey
            : parentIndexes.find((index) => index.name === reference.unique)?.id;
      if (found === undefined || found === null)
        throw new CatalogChangeError(
          `Foreign key ${key.name} names no key of ${parent?.name ?? input.name}`,
        );
      return {
        id: key.id ?? this.#id(),
        name: key.name,
        columnIds: key.columns.map(columnId),
        parentTableId: self ? id : (parent?.id ?? 0),
        parentIndexId: found,
        supportIndexId: null,
        onDelete: key.onDelete,
        onUpdate: key.onUpdate ?? "follow",
        enforced: key.enforced,
      };
    });
    const checks: CheckDef[] = input.checks.map((check) => ({
      id: check.id ?? this.#id(),
      name: check.name,
      conditionSql: check.conditionSql,
      columnIds: check.columns.map(columnId),
      ...(check.dependencies === undefined
        ? {}
        : { dependencies: resolveDependencies(check.dependencies, ids) }),
    }));
    return {
      id,
      name: input.name,
      schemaVersion: (previous?.schemaVersion ?? 0) + 1,
      ...(input.managed === undefined ? {} : { managed: input.managed }),
      columns,
      primaryKey,
      indexes,
      foreignKeys,
      checks,
    };
  }

  /** Applies one change; returns the id of what it created (or null). */
  apply(change: CatalogChange): { readonly id: number | null } {
    const view = this.#view;
    let tables = view.tables;
    let sequences = view.sequences;
    let views = view.views;
    let types = view.types;
    let triggers = view.triggers;
    let created: number | null = null;
    switch (change.kind) {
      case "createTrigger": {
        if (triggers.some((t) => t.name === change.trigger.name))
          throw new CatalogChangeError("Trigger exists");
        const def = { ...change.trigger, id: this.#id() };
        triggers = [...triggers, def];
        created = def.id;
        break;
      }
      case "dropTrigger":
        if (!triggers.some((t) => t.id === change.triggerId))
          throw new CatalogChangeError("Unknown trigger");
        triggers = triggers.filter((t) => t.id !== change.triggerId);
        break;
      case "createTable": {
        if (this.#nameTaken(change.table.name))
          throw new CatalogChangeError(`${change.table.name} exists`);
        const initial = this.#table(change.table, null);
        const columns = initial.columns.map((column, i) => {
          const identity = change.table.columns[i]?.identity;
          if (!identity) return column;
          if (column.type.kind !== "int32" && column.type.kind !== "int64")
            throw new CatalogChangeError("Identity needs an integer");
          const min =
            column.type.kind === "int64"
              ? -(1n << 63n)
              : column.type.bits === 16
                ? -32768n
                : -2147483648n;
          const max =
            column.type.kind === "int64"
              ? (1n << 63n) - 1n
              : column.type.bits === 16
                ? 32767n
                : 2147483647n;
          const id = this.#id();
          sequences = [
            ...sequences,
            {
              id,
              name: ownedSequenceName(
                initial.name,
                column.name,
                (name) =>
                  this.#nameTaken(name) ||
                  sequences.some((entry) => entry.name === name) ||
                  initial.indexes.some((entry) => entry.name === name),
              ),
              start: identity.start,
              increment: identity.increment,
              min: identity.increment > 0n ? 1n : min,
              max: identity.increment > 0n ? max : -1n,
              cycle: false,
              ownedBy: { tableId: initial.id, columnId: column.id },
            },
          ];
          return {
            ...column,
            identitySequence: id,
            defaultDependencies: { dialectVersion: 1, columnIds: [], sequenceIds: [id] },
          };
        });
        const table = { ...initial, columns };
        tables = [...tables, table];
        created = table.id;
        break;
      }
      case "alterTable": {
        const previous = view.table(change.tableId);
        if (previous === undefined)
          throw new CatalogChangeError(`No table ${String(change.tableId)}`);
        const table = this.#table(change.table, previous);
        tables = tables.map((entry) => (entry.id === table.id ? table : entry));
        break;
      }
      case "dropTable":
        triggers = triggers.filter((t) => t.tableId !== change.tableId);
        if (view.table(change.tableId) === undefined) throw new CatalogChangeError("No such table");
        tables = tables.filter((table) => table.id !== change.tableId);
        sequences = sequences.filter((sequence) => sequence.ownedBy?.tableId !== change.tableId);
        break;
      case "createIndex": {
        if (this.#nameTaken(change.index.name))
          throw new CatalogChangeError(`${change.index.name} exists`);
        const table = view.table(change.index.tableId);
        if (table === undefined) throw new CatalogChangeError("No such table");
        const kind = change.index.columnIds.some((columnId) => {
          const type = table.columns.find((column) => column.id === columnId)?.type;
          return type !== undefined && indexKeyKind(type) === "equality";
        })
          ? ("equality" as const)
          : ("ordered" as const);
        const index: IndexDef = { ...change.index, id: this.#id(), kind };
        tables = tables.map((entry) =>
          entry.id === table.id ? { ...entry, indexes: [...entry.indexes, index] } : entry,
        );
        created = index.id;
        break;
      }
      case "dropIndex":
        tables = tables.map((table) => ({
          ...table,
          indexes: table.indexes.filter((index) => index.id !== change.indexId),
        }));
        break;
      case "createSequence": {
        const sequence: SequenceDef = { ...change.sequence, id: this.#id() };
        sequences = [...sequences, sequence];
        created = sequence.id;
        break;
      }
      case "alterSequence":
        sequences = sequences.map((entry) =>
          entry.id === change.sequence.id ? change.sequence : entry,
        );
        break;
      case "dropSequence": {
        const sequence = sequences.find((sequence) => sequence.id === change.sequenceId);
        if (
          sequence?.ownedBy != null ||
          tables.some((table) => tableUsesObject(table, "sequence", change.sequenceId)) ||
          views.some((view) =>
            view.dependencies?.some(
              (dep) => dep.kind === "sequence" && dep.objectId === change.sequenceId,
            ),
          )
        )
          throw new CatalogChangeError("Sequence in use");
        sequences = sequences.filter((entry) => entry.id !== change.sequenceId);
        break;
      }
      case "createView": {
        if (this.#nameTaken(change.view.name))
          throw new CatalogChangeError(`${change.view.name} exists`);
        const created_: ViewDef = { ...change.view, id: this.#id() };
        views = [...views, created_];
        created = created_.id;
        break;
      }
      case "replaceView":
        if (!views.some((view) => view.id === change.view.id))
          throw new CatalogChangeError("No such view");
        views = views.map((view) => (view.id === change.view.id ? change.view : view));
        break;
      case "createType": {
        if (types.some((type) => type.name === change.type.name))
          throw new CatalogChangeError("Type exists");
        const type = { ...change.type, id: this.#id() };
        types = [...types, type];
        created = type.id;
        break;
      }
      case "dropType":
        if (
          tables.some((table) =>
            tableUsesObject(
              table,
              "type",
              change.typeId,
              types.find((type) => type.id === change.typeId)?.name,
            ),
          ) ||
          views.some((view) =>
            view.dependencies?.some((dep) => dep.kind === "type" && dep.objectId === change.typeId),
          )
        )
          throw new CatalogChangeError("Type in use");
        types = types.filter((type) => type.id !== change.typeId);
        break;
      case "dropView":
        views = views.filter((entry) => entry.id !== change.viewId);
        break;
    }
    validateTriggerCatalog({ tables, triggers, types, sequences });
    this.#view = new Snapshot(view.version + 1, tables, sequences, views, types, triggers);
    return { id: created };
  }
}
