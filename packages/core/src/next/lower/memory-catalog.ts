/**
 * An in-memory `CatalogView` (access/catalog.ts) that applies `CatalogChange`s: lowering's
 * tests, and a stand-in for the write path's catalog until that exists (milestone M1). It assigns
 * ids as the contract says (never reused) and bumps `version` on every change. It does not
 * validate data (NOT NULL over existing rows, type changes): that is the write path's job.
 */
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
import type { PhysicalType } from "../types.js";

/** keys/key-encoding.ts `indexKeyKind` without the import lowering's layer forbids: enums are equality-only. */
const indexKeyKind = (type: PhysicalType): IndexDef["kind"] =>
  type.kind === "enum" ? "equality" : "ordered";

class Snapshot implements CatalogView {
  constructor(
    readonly version: number,
    readonly tables: readonly TableDef[],
    readonly sequences: readonly SequenceDef[],
    readonly views: readonly ViewDef[],
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
  // Named types (R17.7): the in-memory catalog defines none.
  readonly types: readonly TypeDef[] = [];
  type(): TypeDef | undefined {
    return undefined;
  }
  typeByName(): TypeDef | undefined {
    return undefined;
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
    const columns: ColumnDef[] = input.columns.map((column) => ({
      ...column,
      id: column.id ?? this.#id(),
    }));
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
        enforced: key.enforced,
      };
    });
    const checks: CheckDef[] = input.checks.map((check) => ({
      id: check.id ?? this.#id(),
      name: check.name,
      conditionSql: check.conditionSql,
      columnIds: check.columns.map(columnId),
    }));
    return {
      id,
      name: input.name,
      schemaVersion: (previous?.schemaVersion ?? 0) + 1,
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
    let created: number | null = null;
    switch (change.kind) {
      case "createTable": {
        if (this.#nameTaken(change.table.name))
          throw new CatalogChangeError(`${change.table.name} exists`);
        const table = this.#table(change.table, null);
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
        if (view.table(change.tableId) === undefined) throw new CatalogChangeError("No such table");
        tables = tables.filter((table) => table.id !== change.tableId);
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
      case "dropSequence":
        sequences = sequences.filter((entry) => entry.id !== change.sequenceId);
        break;
      case "createView": {
        if (this.#nameTaken(change.view.name))
          throw new CatalogChangeError(`${change.view.name} exists`);
        const created_: ViewDef = { ...change.view, id: this.#id() };
        views = [...views, created_];
        created = created_.id;
        break;
      }
      case "dropView":
        views = views.filter((entry) => entry.id !== change.viewId);
        break;
    }
    this.#view = new Snapshot(view.version + 1, tables, sequences, views);
    return { id: created };
  }
}
