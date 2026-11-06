/**
 * The catalog the write path keeps (access/catalog.ts): immutable `Catalog` values, the DDL
 * validation that produces the next one, and the logged form of every change, so replay applies
 * exactly what the commit applied (format/replay.ts "ordered effects").
 *
 * - `Catalog.change(change, version)` validates a `CatalogChange` against this catalog and returns
 *   the next catalog plus the `LoggedChange`s that reproduce it. Replay calls `Catalog.apply` with
 *   the logged changes; the live path calls the same function, so the two cannot diverge.
 * - Ids come from counters stored in the catalog itself, so they are never reused, even across a
 *   rolled back transaction (the counters advance only in committed catalogs: a rolled back DDL
 *   reuses nothing a durable structure names, because nothing durable named it).
 * - `version` is assigned by the caller from a write-path-wide counter that never goes back.
 */
import {
  CatalogChangeError,
  type CatalogChange,
  type CatalogView,
  type CheckDef,
  type ColumnDef,
  type ForeignKeyDef,
  type IndexDef,
  type SequenceDef,
  type TableDef,
  type TableInput,
  type TypeDef,
  type ViewDef,
} from "../access/catalog.js";
import { indexKeyKind } from "../keys/key-encoding.js";
import {
  INT16_MAX,
  INT16_MIN,
  INT32_MAX,
  INT32_MIN,
  INT64_MAX,
  INT64_MIN,
  samePhysicalType,
} from "../types.js";
import type { PhysicalType } from "../types.js";
import { decodeJson, encodeJson } from "./bytes.js";

export interface CatalogCounters {
  readonly nextTableId: number;
  readonly nextColumnId: number;
  readonly nextIndexId: number;
  readonly nextConstraintId: number;
  readonly nextSequenceId: number;
  readonly nextViewId: number;
  readonly nextTypeId: number;
}

const FIRST_COUNTERS: CatalogCounters = {
  nextTableId: 1,
  nextColumnId: 1,
  nextIndexId: 1,
  nextConstraintId: 1,
  nextSequenceId: 1,
  nextViewId: 1,
  nextTypeId: 1,
};

/** A change as the log carries it: every id resolved, nothing left to validate. */
export type LoggedChange =
  | {
      readonly op: "createTable";
      readonly table: TableDef;
      readonly sequences: readonly SequenceDef[];
    }
  | { readonly op: "dropTable"; readonly tableId: number }
  | { readonly op: "createIndex"; readonly index: IndexDef }
  | { readonly op: "dropIndex"; readonly tableId: number; readonly indexId: number }
  | { readonly op: "putSequence"; readonly sequence: SequenceDef }
  | { readonly op: "dropSequence"; readonly sequenceId: number }
  | { readonly op: "createView"; readonly view: ViewDef }
  | { readonly op: "dropView"; readonly viewId: number }
  | { readonly op: "createType"; readonly type: TypeDef }
  | { readonly op: "dropType"; readonly typeId: number };

interface CatalogData {
  readonly version: number;
  readonly counters: CatalogCounters;
  readonly tables: readonly TableDef[];
  readonly sequences: readonly SequenceDef[];
  readonly views: readonly ViewDef[];
  readonly types: readonly TypeDef[];
}

function intRange(type: PhysicalType): { min: bigint; max: bigint } {
  if (type.kind === "int32")
    return type.bits === 16
      ? { min: BigInt(INT16_MIN), max: BigInt(INT16_MAX) }
      : { min: BigInt(INT32_MIN), max: BigInt(INT32_MAX) };
  if (type.kind === "int64") return { min: INT64_MIN, max: INT64_MAX };
  throw new CatalogChangeError(`An identity column must be an integer, not ${type.kind}`);
}

export class Catalog implements CatalogView {
  readonly version: number;
  readonly counters: CatalogCounters;
  readonly tables: readonly TableDef[];
  readonly sequences: readonly SequenceDef[];
  readonly views: readonly ViewDef[];
  readonly types: readonly TypeDef[];
  readonly #tables: ReadonlyMap<number, TableDef>;
  readonly #tablesByName: ReadonlyMap<string, TableDef>;
  readonly #sequences: ReadonlyMap<number, SequenceDef>;
  readonly #types: ReadonlyMap<number, TypeDef>;

  constructor(data: CatalogData) {
    this.version = data.version;
    this.counters = data.counters;
    this.tables = data.tables;
    this.sequences = data.sequences;
    this.views = data.views;
    this.types = data.types;
    this.#tables = new Map(data.tables.map((table) => [table.id, table]));
    this.#tablesByName = new Map(data.tables.map((table) => [table.name, table]));
    this.#sequences = new Map(data.sequences.map((sequence) => [sequence.id, sequence]));
    this.#types = new Map(data.types.map((type) => [type.id, type]));
  }

  static empty(): Catalog {
    return new Catalog({
      version: 0,
      counters: FIRST_COUNTERS,
      tables: [],
      sequences: [],
      views: [],
      types: [],
    });
  }

  table(tableId: number): TableDef | undefined {
    return this.#tables.get(tableId);
  }

  tableByName(name: string): TableDef | undefined {
    return this.#tablesByName.get(name);
  }

  sequence(sequenceId: number): SequenceDef | undefined {
    return this.#sequences.get(sequenceId);
  }

  type(typeId: number): TypeDef | undefined {
    return this.#types.get(typeId);
  }

  typeByName(name: string): TypeDef | undefined {
    return this.types.find((type) => type.name === name);
  }

  index(indexId: number): IndexDef | undefined {
    for (const table of this.tables) {
      const found = table.indexes.find((index) => index.id === indexId);
      if (found !== undefined) return found;
    }
    return undefined;
  }

  #data(): CatalogData {
    return {
      version: this.version,
      counters: this.counters,
      tables: this.tables,
      sequences: this.sequences,
      views: this.views,
      types: this.types,
    };
  }

  /** Same content at another version (a transaction's view after a rollback, for instance). */
  withVersion(version: number): Catalog {
    return new Catalog({ ...this.#data(), version });
  }

  // ── Validation: CatalogChange → logged changes ───────────────────────────────────────────

  /** Validates `change` and returns the next catalog, the logged changes, and the new object's id. */
  change(
    change: CatalogChange,
    version: number,
  ): {
    readonly catalog: Catalog;
    readonly logged: readonly LoggedChange[];
    readonly id: number | null;
  } {
    const logged = this.#plan(change);
    const next = logged.reduce<Catalog>((catalog, entry) => catalog.apply(entry, version), this);
    const id = createdId(logged[0] ?? null);
    return { catalog: next, logged, id };
  }

  #nameTaken(name: string): boolean {
    return this.#tablesByName.has(name) || this.views.some((view) => view.name === name);
  }

  #plan(change: CatalogChange): LoggedChange[] {
    switch (change.kind) {
      case "createTable":
        return this.#planCreateTable(change.table);
      case "alterTable":
        throw new CatalogChangeError("ALTER TABLE is not supported by the write path yet");
      case "dropTable": {
        const table = this.#requireTable(change.tableId);
        for (const other of this.tables)
          for (const fk of other.foreignKeys)
            if (fk.parentTableId === table.id && other.id !== table.id && fk.enforced)
              throw new CatalogChangeError(
                `Table ${table.name} is referenced by a foreign key of ${other.name}`,
              );
        return [{ op: "dropTable", tableId: table.id }];
      }
      case "createIndex": {
        const input = change.index;
        const table = this.#requireTable(input.tableId);
        if (table.indexes.some((index) => index.name === input.name))
          throw new CatalogChangeError(`Index ${input.name} already exists`);
        const index = this.#indexDef(
          table,
          this.counters.nextIndexId,
          input.name,
          input.columnIds,
          input.unique,
          input.role,
        );
        return [{ op: "createIndex", index }];
      }
      case "dropIndex": {
        const table = this.tables.find((candidate) =>
          candidate.indexes.some((index) => index.id === change.indexId),
        );
        if (table === undefined)
          throw new CatalogChangeError(`Index ${String(change.indexId)} does not exist`);
        if (table.primaryKey === change.indexId)
          throw new CatalogChangeError("A primary key's index cannot be dropped");
        for (const other of this.tables)
          for (const fk of other.foreignKeys)
            if (fk.parentIndexId === change.indexId || fk.supportIndexId === change.indexId)
              throw new CatalogChangeError(`Index ${String(change.indexId)} serves a foreign key`);
        return [{ op: "dropIndex", tableId: table.id, indexId: change.indexId }];
      }
      case "createSequence": {
        if (this.sequences.some((sequence) => sequence.name === change.sequence.name))
          throw new CatalogChangeError(`Sequence ${change.sequence.name} already exists`);
        const sequence = { ...change.sequence, id: this.counters.nextSequenceId };
        validateSequence(sequence);
        return [{ op: "putSequence", sequence }];
      }
      case "alterSequence": {
        if (!this.#sequences.has(change.sequence.id))
          throw new CatalogChangeError(`Sequence ${String(change.sequence.id)} does not exist`);
        validateSequence(change.sequence);
        return [{ op: "putSequence", sequence: change.sequence }];
      }
      case "dropSequence": {
        const sequence = this.#sequences.get(change.sequenceId);
        if (sequence === undefined)
          throw new CatalogChangeError(`Sequence ${String(change.sequenceId)} does not exist`);
        if (sequence.ownedBy !== null)
          throw new CatalogChangeError(`Sequence ${sequence.name} belongs to a column`);
        return [{ op: "dropSequence", sequenceId: sequence.id }];
      }
      case "createView": {
        if (this.#nameTaken(change.view.name))
          throw new CatalogChangeError(`Relation ${change.view.name} already exists`);
        return [{ op: "createView", view: { ...change.view, id: this.counters.nextViewId } }];
      }
      case "dropView": {
        if (!this.views.some((view) => view.id === change.viewId))
          throw new CatalogChangeError(`View ${String(change.viewId)} does not exist`);
        return [{ op: "dropView", viewId: change.viewId }];
      }
      case "createType": {
        if (this.typeByName(change.type.name) !== undefined)
          throw new CatalogChangeError(`Type ${change.type.name} already exists`);
        if (new Set(change.type.labels).size !== change.type.labels.length)
          throw new CatalogChangeError(`Type ${change.type.name} repeats a label`);
        return [{ op: "createType", type: { ...change.type, id: this.counters.nextTypeId } }];
      }
      case "dropType": {
        const type = this.#types.get(change.typeId);
        if (type === undefined)
          throw new CatalogChangeError(`Type ${String(change.typeId)} does not exist`);
        for (const table of this.tables)
          for (const column of table.columns)
            if (column.type.kind === "enum" && column.type.name === type.name)
              throw new CatalogChangeError(
                `Type ${type.name} is used by ${table.name}.${column.name}`,
              );
        return [{ op: "dropType", typeId: type.id }];
      }
    }
  }

  #requireTable(tableId: number): TableDef {
    const table = this.#tables.get(tableId);
    if (table === undefined)
      throw new CatalogChangeError(`Table ${String(tableId)} does not exist`);
    return table;
  }

  #indexDef(
    table: Pick<TableDef, "id" | "columns">,
    id: number,
    name: string,
    columnIds: readonly number[],
    unique: boolean,
    role: IndexDef["role"],
  ): IndexDef {
    if (columnIds.length === 0) throw new CatalogChangeError(`Index ${name} has no columns`);
    let kind: IndexDef["kind"] = "ordered";
    for (const columnId of columnIds) {
      const column = table.columns.find((candidate) => candidate.id === columnId);
      if (column === undefined)
        throw new CatalogChangeError(`Index ${name} names unknown column ${String(columnId)}`);
      if (indexKeyKind(column.type) === "equality") kind = "equality";
    }
    return { id, name, tableId: table.id, columnIds, unique, kind, role };
  }

  #planCreateTable(input: TableInput): LoggedChange[] {
    if (this.#nameTaken(input.name))
      throw new CatalogChangeError(`Relation ${input.name} already exists`);
    const counters = { ...this.counters };
    const tableId = counters.nextTableId;
    const names = new Set<string>();
    const primary = new Set(input.primaryKey ?? []);
    const sequences: SequenceDef[] = [];
    const columns: ColumnDef[] = input.columns.map((column, position) => {
      if (names.has(column.name)) throw new CatalogChangeError(`Column ${column.name} is repeated`);
      names.add(column.name);
      const id = counters.nextColumnId++;
      let identitySequence = column.identitySequence;
      if (column.identity !== undefined && column.identity !== null) {
        const range = intRange(column.type);
        const increment = column.identity.increment;
        const sequence: SequenceDef = {
          id: counters.nextSequenceId++,
          name: `${input.name}_${column.name}_seq`,
          increment,
          min: increment > 0n ? 1n : range.min,
          max: increment > 0n ? range.max : -1n,
          start: column.identity.start,
          cycle: false,
          ownedBy: { tableId, columnId: id },
        };
        validateSequence(sequence);
        sequences.push(sequence);
        identitySequence = sequence.id;
      }
      if (column.type.kind === "enum" && column.type.labels.length === 0)
        throw new CatalogChangeError(`Column ${column.name} has an enum type with no labels`);
      return {
        id,
        name: column.name,
        type: column.type,
        declared: column.declared,
        // A primary key's columns are NOT NULL (PostgreSQL).
        nullable: column.nullable && !primary.has(position),
        defaultSql: column.defaultSql,
        generatedSql: column.generatedSql,
        identitySequence,
        missing: null,
      };
    });
    const at = (position: number, what: string): ColumnDef => {
      const column = columns[position];
      if (column === undefined)
        throw new CatalogChangeError(`${what} names column position ${String(position)}`);
      return column;
    };
    const shape = { id: tableId, columns };
    const indexes: IndexDef[] = [];
    let primaryKey: number | null = null;
    if (input.primaryKey !== null) {
      const index = this.#indexDef(
        shape,
        counters.nextIndexId++,
        `${input.name}_pkey`,
        input.primaryKey.map((position) => at(position, "The primary key").id),
        true,
        "primary",
      );
      indexes.push(index);
      primaryKey = index.id;
    }
    const uniqueByName = new Map<string, IndexDef>();
    for (const unique of input.unique) {
      if (indexes.some((index) => index.name === unique.name))
        throw new CatalogChangeError(`Constraint ${unique.name} is repeated`);
      const index = this.#indexDef(
        shape,
        counters.nextIndexId++,
        unique.name,
        unique.columns.map((position) => at(position, `Unique ${unique.name}`).id),
        true,
        "unique",
      );
      indexes.push(index);
      uniqueByName.set(unique.name, index);
    }
    const foreignKeys: ForeignKeyDef[] = [];
    for (const input_ of input.foreignKeys) {
      const columnIds = input_.columns.map(
        (position) => at(position, `Foreign key ${input_.name}`).id,
      );
      let parentTableId: number;
      let parentIndex: IndexDef;
      let parentColumns: readonly ColumnDef[];
      if (input_.parentTableId === "self") {
        parentTableId = tableId;
        const reference = input_.parentIndexId;
        const found =
          reference === "primary"
            ? indexes.find((index) => index.id === primaryKey)
            : typeof reference === "object"
              ? uniqueByName.get(reference.unique)
              : indexes.find((index) => index.id === reference);
        if (found === undefined)
          throw new CatalogChangeError(`Foreign key ${input_.name} names no key of ${input.name}`);
        parentIndex = found;
        parentColumns = columns;
      } else {
        const parent = this.#requireTable(input_.parentTableId);
        parentTableId = parent.id;
        const reference = input_.parentIndexId;
        const found =
          reference === "primary"
            ? parent.indexes.find((index) => index.id === parent.primaryKey)
            : typeof reference === "object"
              ? parent.indexes.find((index) => index.name === reference.unique)
              : parent.indexes.find((index) => index.id === reference);
        if (found === undefined)
          throw new CatalogChangeError(`Foreign key ${input_.name} names no key of ${parent.name}`);
        parentIndex = found;
        parentColumns = parent.columns;
      }
      if (!parentIndex.unique)
        throw new CatalogChangeError(`Foreign key ${input_.name} must reference a unique key`);
      if (parentIndex.columnIds.length !== columnIds.length)
        throw new CatalogChangeError(`Foreign key ${input_.name} has the wrong number of columns`);
      parentIndex.columnIds.forEach((parentColumnId, k) => {
        const parentColumn = parentColumns.find((column) => column.id === parentColumnId);
        const child = columns.find((column) => column.id === columnIds[k]);
        if (
          parentColumn === undefined ||
          child === undefined ||
          !samePhysicalType(parentColumn.type, child.type)
        )
          throw new CatalogChangeError(
            `Foreign key ${input_.name} column types differ from the key`,
          );
      });
      let supportIndexId: number | null = null;
      if (input_.enforced) {
        const covering = indexes.find(
          (index) =>
            index.columnIds.length >= columnIds.length &&
            columnIds.every((columnId, k) => index.columnIds[k] === columnId),
        );
        if (covering !== undefined) supportIndexId = covering.id;
        else {
          const index = this.#indexDef(
            shape,
            counters.nextIndexId++,
            `${input.name}_${input_.name}_fkey_idx`,
            columnIds,
            false,
            "foreignKey",
          );
          indexes.push(index);
          supportIndexId = index.id;
        }
      }
      foreignKeys.push({
        id: counters.nextConstraintId++,
        name: input_.name,
        columnIds,
        parentTableId,
        parentIndexId: parentIndex.id,
        supportIndexId,
        onDelete: input_.onDelete,
        enforced: input_.enforced,
      });
    }
    const checks: CheckDef[] = input.checks.map((check) => ({
      id: counters.nextConstraintId++,
      name: check.name,
      conditionSql: check.conditionSql,
      columnIds: check.columns.map((position) => at(position, `Check ${check.name}`).id),
    }));
    const table: TableDef = {
      id: tableId,
      name: input.name,
      schemaVersion: 1,
      columns,
      primaryKey,
      indexes,
      foreignKeys,
      checks,
    };
    return [{ op: "createTable", table, sequences }];
  }

  // ── Application: logged change → next catalog (live and replay) ───────────────────────────

  apply(change: LoggedChange, version: number): Catalog {
    const data = this.#data();
    const counters = { ...data.counters };
    const bump = (key: keyof CatalogCounters, used: number) => {
      counters[key] = Math.max(counters[key], used + 1);
    };
    let { tables, sequences, views, types } = data;
    switch (change.op) {
      case "createTable": {
        const table = change.table;
        if (this.#tables.has(table.id))
          throw new CatalogChangeError(`Table ${String(table.id)} exists`);
        bump("nextTableId", table.id);
        for (const column of table.columns) bump("nextColumnId", column.id);
        for (const index of table.indexes) bump("nextIndexId", index.id);
        for (const fk of table.foreignKeys) bump("nextConstraintId", fk.id);
        for (const check of table.checks) bump("nextConstraintId", check.id);
        for (const sequence of change.sequences) bump("nextSequenceId", sequence.id);
        tables = [...tables, table];
        sequences = [...sequences, ...change.sequences];
        break;
      }
      case "dropTable":
        tables = tables.filter((table) => table.id !== change.tableId);
        sequences = sequences.filter((sequence) => sequence.ownedBy?.tableId !== change.tableId);
        break;
      case "createIndex":
        bump("nextIndexId", change.index.id);
        tables = tables.map((table) =>
          table.id === change.index.tableId
            ? { ...table, indexes: [...table.indexes, change.index] }
            : table,
        );
        break;
      case "dropIndex":
        tables = tables.map((table) =>
          table.id === change.tableId
            ? { ...table, indexes: table.indexes.filter((index) => index.id !== change.indexId) }
            : table,
        );
        break;
      case "putSequence":
        bump("nextSequenceId", change.sequence.id);
        sequences = [
          ...sequences.filter((sequence) => sequence.id !== change.sequence.id),
          change.sequence,
        ];
        break;
      case "dropSequence":
        sequences = sequences.filter((sequence) => sequence.id !== change.sequenceId);
        break;
      case "createView":
        bump("nextViewId", change.view.id);
        views = [...views, change.view];
        break;
      case "dropView":
        views = views.filter((view) => view.id !== change.viewId);
        break;
      case "createType":
        bump("nextTypeId", change.type.id);
        types = [...types, change.type];
        break;
      case "dropType":
        types = types.filter((type) => type.id !== change.typeId);
        break;
    }
    return new Catalog({ version, counters, tables, sequences, views, types });
  }

  // ── Stored form ────────────────────────────────────────────────────────────────────────────

  encode(): Uint8Array {
    return encodeJson(this.#data());
  }

  static decode(bytes: Uint8Array): Catalog {
    return new Catalog(decodeJson(bytes) as CatalogData);
  }
}

function createdId(change: LoggedChange | null): number | null {
  if (change === null) return null;
  switch (change.op) {
    case "createTable":
      return change.table.id;
    case "createIndex":
      return change.index.id;
    case "putSequence":
      return change.sequence.id;
    case "createView":
      return change.view.id;
    case "createType":
      return change.type.id;
    default:
      return null;
  }
}

function validateSequence(sequence: SequenceDef): void {
  if (sequence.increment === 0n) throw new CatalogChangeError("A sequence's increment cannot be 0");
  if (sequence.min > sequence.max)
    throw new CatalogChangeError("A sequence's minimum exceeds its maximum");
  if (sequence.start < sequence.min || sequence.start > sequence.max)
    throw new CatalogChangeError("A sequence's start lies outside its bounds");
}

export function encodeLogged(change: LoggedChange): Uint8Array {
  return encodeJson(change);
}

export function decodeLogged(bytes: Uint8Array): LoggedChange {
  return decodeJson(bytes) as LoggedChange;
}
