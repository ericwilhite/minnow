import {
  validateCatalogType,
  validateCatalogDeclared,
  validateCatalogMissing,
} from "./catalog-values.js";
import type { ByteBudget } from "../storage/extents.js";
import { ParseBudgetError } from "../format/manifest-codec.js";
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
import { CATALOG_CHANGE_FIELDS, DURABLE_CODECS, requireTag } from "../durable-format.js";
import { StoreCorruptionError } from "../storage/contract.js";
import type { BufferAllocator } from "./commit-buffers.js";
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
  ENUM_LABEL_LIMIT,
  ENUM_LABEL_BYTES,
  samePhysicalType,
} from "../types.js";
import type { PhysicalType } from "../types.js";
import { resolveDependencies, tableUsesObject } from "../access/catalog-dependencies.js";
import { ownedSequenceName } from "../access/catalog-names.js";
import {
  decodeCatalogStream,
  encodeCatalogStream,
  encodeCatalogChange,
  decodeCatalogChange,
  type CatalogStreamData,
} from "./catalog-stream.js";
import {
  validateTriggerDef,
  validateTriggerCatalog,
  type TriggerDef,
} from "../access/trigger-def.js";
import { validateFtsDef, type FtsDef } from "../access/fts-def.js";
import { DurableFormatError } from "../durable-format.js";
import { retainCatalogExtensions, UnsupportedCatalogError } from "./catalog-records.js";
import { validateRelationalCatalog } from "./catalog-relationships.js";
import { utf8Length } from "../vector/varlen.js";

export interface CatalogCounters {
  readonly nextTableId: number;
  readonly nextColumnId: number;
  readonly nextIndexId: number;
  readonly nextConstraintId: number;
  readonly nextSequenceId: number;
  readonly nextViewId: number;
  readonly nextTypeId: number;
  readonly nextFtsId: number;
  readonly nextTriggerId: number;
}

const FIRST_COUNTERS: CatalogCounters = {
  nextTableId: 1,
  nextColumnId: 1,
  nextIndexId: 1,
  nextConstraintId: 1,
  nextSequenceId: 1,
  nextViewId: 1,
  nextTypeId: 1,
  nextFtsId: 1,
  nextTriggerId: 1,
};

/** A change as the log carries it: every id resolved, nothing left to validate. */
export type CatalogOperation =
  | {
      readonly op: "createTable";
      readonly table: TableDef;
      readonly sequences: readonly SequenceDef[];
    }
  | {
      readonly op: "alterTable";
      readonly table: TableDef;
      readonly sequences: readonly SequenceDef[];
    }
  | { readonly op: "dropTable"; readonly tableId: number }
  | { readonly op: "createIndex"; readonly index: IndexDef }
  | { readonly op: "invalidateIndex"; readonly tableId: number; readonly indexId: number }
  | { readonly op: "dropIndex"; readonly tableId: number; readonly indexId: number }
  | { readonly op: "putSequence"; readonly sequence: SequenceDef }
  | { readonly op: "dropSequence"; readonly sequenceId: number }
  | { readonly op: "createView"; readonly view: ViewDef }
  | { readonly op: "dropView"; readonly viewId: number }
  | { readonly op: "replaceView"; readonly view: ViewDef }
  | { readonly op: "createType"; readonly type: TypeDef }
  | { readonly op: "dropType"; readonly typeId: number }
  | { readonly op: "createFts"; readonly fts: FtsDef }
  | { readonly op: "upsertFts"; readonly fts: FtsDef }
  | { readonly op: "dropFts"; readonly ftsId: number }
  | { readonly op: "createTrigger"; readonly trigger: TriggerDef }
  | { readonly op: "dropTrigger"; readonly triggerId: number };

/** Every WAL operation binds its predecessor and its live-assigned resulting version. */
export type LoggedChange = CatalogOperation & {
  readonly expectedVersion: number;
  readonly version: number;
};

type CatalogData = CatalogStreamData;

function intRange(type: PhysicalType): { min: bigint; max: bigint } {
  if (type.kind === "int32")
    return type.bits === 16
      ? { min: BigInt(INT16_MIN), max: BigInt(INT16_MAX) }
      : { min: BigInt(INT32_MIN), max: BigInt(INT32_MAX) };
  if (type.kind === "int64") return { min: INT64_MIN, max: INT64_MAX };
  throw new CatalogChangeError(`An identity column must be an integer, not ${type.kind}`);
}

/** JSON input can carry fields outside IndexDef's compile-time unions. */
function validateIndexPolicy(index: {
  readonly unique: boolean;
  readonly columnIds: readonly number[];
  readonly invalid?: unknown;
  readonly keyLimit?: unknown;
}): void {
  if (index.invalid !== undefined && (index.invalid !== true || index.unique))
    throw new StoreCorruptionError("Invalid unique-index availability flag");
  if (index.keyLimit !== undefined && index.keyLimit !== "scalar" && index.keyLimit !== "tuple")
    throw new StoreCorruptionError("Unknown index key-limit policy");
  if (index.keyLimit === "scalar" && index.columnIds.length !== 1)
    throw new StoreCorruptionError("A scalar index key-limit policy requires one column");
}

export class Catalog implements CatalogView {
  readonly version: number;
  readonly counters: CatalogCounters;
  readonly tables: readonly TableDef[];
  readonly sequences: readonly SequenceDef[];
  readonly views: readonly ViewDef[];
  readonly types: readonly TypeDef[];
  readonly fts: readonly FtsDef[];
  readonly triggers: readonly TriggerDef[];
  readonly wireMetadata?: unknown;
  readonly #tables: ReadonlyMap<number, TableDef>;
  readonly #tablesByName: ReadonlyMap<string, TableDef>;
  readonly #sequences: ReadonlyMap<number, SequenceDef>;
  readonly #types: ReadonlyMap<number, TypeDef>;

  constructor(data: CatalogData) {
    for (const table of data.tables) for (const index of table.indexes) validateIndexPolicy(index);
    this.wireMetadata = data.wireMetadata;
    this.version = data.version;
    this.counters = data.counters;
    this.tables = data.tables;
    this.sequences = data.sequences;
    this.views = data.views;
    this.types = data.types;
    this.fts = data.fts;
    this.triggers = data.triggers;
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
      fts: [],
      triggers: [],
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
      wireMetadata: this.wireMetadata,
      version: this.version,
      counters: this.counters,
      tables: this.tables,
      sequences: this.sequences,
      views: this.views,
      types: this.types,
      fts: this.fts,
      triggers: this.triggers,
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
    let expectedVersion = this.version;
    const logged = this.#plan(change).map((operation) => {
      const entry = { ...operation, expectedVersion, version };
      expectedVersion = version;
      return entry;
    });
    const next = logged.reduce<Catalog>((catalog, entry) => catalog.apply(entry, version), this);
    try {
      validateStoredCatalog(next.#data());
      validateRelationalCatalog(next.#data());
    } catch (error) {
      if (error instanceof StoreCorruptionError) throw new CatalogChangeError(error.message);
      throw error;
    }
    const id = createdId(logged[0] ?? null);
    return { catalog: next, logged, id };
  }

  #nameTaken(name: string): boolean {
    return this.#tablesByName.has(name) || this.views.some((view) => view.name === name);
  }

  #plan(change: CatalogChange): CatalogOperation[] {
    switch (change.kind) {
      case "createTrigger":
        if (this.triggers.some((t) => t.name === change.trigger.name))
          throw new CatalogChangeError(`Trigger ${change.trigger.name} already exists`);
        return [
          { op: "createTrigger", trigger: { ...change.trigger, id: this.counters.nextTriggerId } },
        ];
      case "dropTrigger":
        if (!this.triggers.some((t) => t.id === change.triggerId))
          throw new CatalogChangeError("Unknown trigger identity");
        return [{ op: "dropTrigger", triggerId: change.triggerId }];
      case "createFts":
        return [{ op: "createFts", fts: { ...change.fts, id: this.counters.nextFtsId } }];
      case "upsertFts":
        return [{ op: "upsertFts", fts: change.fts }];
      case "dropFts":
        if (!this.fts.some((def) => def.id === change.ftsId))
          throw new CatalogChangeError("Unknown FTS definition");
        return [{ op: "dropFts", ftsId: change.ftsId }];
      case "createTable":
        return this.#planCreateTable(change.table);
      case "alterTable":
        return this.#planAlterTable(change.tableId, change.table);
      case "dropTable": {
        const table = this.#requireTable(change.tableId);
        this.#requireUnreferenced("table", table.id);
        if (
          this.triggers.some(
            (t) => t.tableId !== table.id && t.statements.some((b) => b.targetTableId === table.id),
          )
        )
          throw new CatalogChangeError(`Table ${table.name} is a trigger target`);
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
        if (input.keyLimit === "scalar" && input.columnIds.length !== 1)
          throw new CatalogChangeError("A scalar index requires one column");
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
        return [
          {
            op: "createIndex",
            index: {
              ...index,
              ...(input.keyLimit === undefined ? {} : { keyLimit: input.keyLimit }),
              ...(input.directions === undefined ? {} : { directions: input.directions }),
            },
          },
        ];
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
        this.#requireUnreferenced("sequence", sequence.id);
        if (
          this.tables.some((table) =>
            table.columns.some((column) =>
              column.defaultDependencies?.sequenceIds.includes(sequence.id),
            ),
          )
        )
          throw new CatalogChangeError(`Sequence ${sequence.name} is used by a default`);
        return [{ op: "dropSequence", sequenceId: sequence.id }];
      }
      case "createView": {
        if (this.#nameTaken(change.view.name))
          throw new CatalogChangeError(`Relation ${change.view.name} already exists`);
        return [{ op: "createView", view: { ...change.view, id: this.counters.nextViewId } }];
      }
      case "replaceView": {
        const before = this.views.find((view) => view.id === change.view.id);
        if (before === undefined)
          throw new CatalogChangeError("The view to replace does not exist");
        if (before.name !== change.view.name)
          throw new CatalogChangeError("View replacement cannot rename the view");
        if (before.outputs !== undefined) {
          const outputs = change.view.outputs;
          if (
            outputs === undefined ||
            outputs.length < before.outputs.length ||
            before.outputs.some((output, i) => {
              const next = outputs.at(i);
              return next?.name !== output.name || !samePhysicalType(next.type, output.type);
            })
          )
            throw new CatalogChangeError(
              "View replacement cannot change existing output names or types",
            );
        }
        return [{ op: "replaceView", view: { ...before, ...change.view } }];
      }
      case "dropView": {
        if (!this.views.some((view) => view.id === change.viewId))
          throw new CatalogChangeError(`View ${String(change.viewId)} does not exist`);
        this.#requireUnreferenced("view", change.viewId);
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
        this.#requireUnreferenced("type", type.id);
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

  #requireUnreferenced(kind: "table" | "view" | "type" | "sequence", objectId: number): void {
    if (kind === "type" || kind === "sequence") {
      const name = kind === "type" ? this.type(objectId)?.name : undefined;
      const table = this.tables.find((table) => tableUsesObject(table, kind, objectId, name));
      if (table)
        throw new CatalogChangeError(`${kind} ${String(objectId)} is used by table ${table.name}`);
    }
    const view = this.views.find((view) =>
      view.dependencies?.some((dep) => dep.kind === kind && dep.objectId === objectId),
    );
    if (view !== undefined)
      throw new CatalogChangeError(`${kind} ${String(objectId)} is used by view ${view.name}`);
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

  #planAlterTable(tableId: number, input: TableInput): CatalogOperation[] {
    const before = this.#requireTable(tableId);
    const seen = new Set<number>();
    for (const column of input.columns)
      if (column.id !== undefined) {
        const prior = before.columns.find((entry) => entry.id === column.id);
        if (!prior || seen.has(column.id))
          throw new CatalogChangeError("ALTER names an unknown or repeated column identity");
        seen.add(column.id);
        if (
          prior.name !== column.name &&
          (before.checks.some((check) => check.columnIds.includes(column.id ?? -1)) ||
            before.columns.some((other) =>
              other.generatedDependencies?.columnIds.includes(column.id ?? -1),
            ) ||
            this.views.some((view) =>
              view.dependencies?.some(
                (dep) =>
                  dep.kind === "table" &&
                  dep.objectId === tableId &&
                  dep.columnIds.includes(column.id ?? -1),
              ),
            ))
        )
          throw new CatalogChangeError(
            "Cannot rename a column used by a stored expression or view",
          );
        if (!samePhysicalType(prior.type, column.type))
          throw new CatalogChangeError("ALTER column type requires a rewrite");
        if (prior.generatedSql !== column.generatedSql)
          throw new CatalogChangeError("Changing a generated expression requires a rewrite");
        if (prior.identityAlways !== column.identityAlways)
          throw new CatalogChangeError("ALTER cannot change identity generation mode");
        if (column.identitySequence !== prior.identitySequence && column.identitySequence !== null)
          throw new CatalogChangeError("ALTER cannot adopt another column's sequence");
        if (column.identity != null && prior.identitySequence !== null)
          throw new CatalogChangeError("ALTER cannot replace an owned sequence");
        if (
          column.allowedValues !== undefined &&
          (prior.allowedValues === undefined ||
            prior.allowedValues.some((value) => !column.allowedValues?.includes(value)))
        )
          throw new CatalogChangeError("ALTER cannot narrow a closed text set");
        if (prior.missing !== column.missing && (prior.missing !== null || column.missing !== null))
          throw new CatalogChangeError("ALTER cannot replace a frozen missing value");
      }
    const currentPrimary =
      before.indexes.find((index) => index.id === before.primaryKey)?.columnIds ?? [];
    const nextPrimary = input.primaryKey?.map((position) => input.columns[position]?.id) ?? [];
    if (
      currentPrimary.length !== nextPrimary.length ||
      currentPrimary.some((id, i) => id !== nextPrimary[i])
    )
      throw new CatalogChangeError("Changing a primary key requires a rebuild");
    for (const column of before.columns)
      if (!seen.has(column.id)) {
        if (
          before.indexes.some((index) => index.columnIds.includes(column.id)) ||
          before.checks.some((check) => check.columnIds.includes(column.id)) ||
          before.foreignKeys.some((key) => key.columnIds.includes(column.id)) ||
          before.columns.some((other) =>
            other.generatedDependencies?.columnIds.includes(column.id),
          ) ||
          this.views.some((view) =>
            view.dependencies?.some(
              (dep) =>
                dep.kind === "table" &&
                dep.objectId === tableId &&
                dep.columnIds.includes(column.id),
            ),
          )
        )
          throw new CatalogChangeError(`Column ${column.name} is used by another object`);
      }
    // Existing constraints are stable; adding/changing row constraints needs data validation.
    if (
      input.unique.length !== before.indexes.filter((index) => index.role === "unique").length ||
      input.unique.some(
        (key) =>
          !before.indexes.some(
            (index) =>
              index.id === key.id &&
              index.name === key.name &&
              index.columnIds.length === key.columns.length &&
              index.columnIds.every((id, i) => id === input.columns[key.columns[i] ?? -1]?.id),
          ),
      )
    )
      throw new CatalogChangeError("Changing UNIQUE constraints requires an index build");
    if (
      input.checks.length !== before.checks.length ||
      input.checks.some(
        (check) =>
          !before.checks.some(
            (prior) => prior.id === check.id && prior.conditionSql === check.conditionSql,
          ),
      )
    )
      throw new CatalogChangeError("Changing CHECK constraints requires data validation");
    if (
      input.foreignKeys.some(
        (key) =>
          key.enforced &&
          !before.foreignKeys.some(
            (prior) =>
              prior.id === key.id &&
              prior.enforced &&
              prior.parentTableId === key.parentTableId &&
              prior.parentIndexId === key.parentIndexId &&
              prior.onDelete === key.onDelete &&
              prior.onUpdate === (key.onUpdate ?? "follow") &&
              prior.columnIds.length === key.columns.length &&
              prior.columnIds.every((id, i) => id === input.columns[key.columns[i] ?? -1]?.id),
          ),
      )
    )
      throw new CatalogChangeError("Changing enforced foreign keys requires data validation");
    const operations = this.#planCreateTable(input, before);
    for (const op of operations)
      if (op.op === "createTable") {
        try {
          validateTriggerCatalog({
            ...this.#data(),
            tables: this.tables.map((t) => (t.id === before.id ? op.table : t)),
            sequences: [
              ...this.sequences.filter(
                (s) =>
                  s.ownedBy?.tableId !== before.id ||
                  op.table.columns.some((c) => c.identitySequence === s.id),
              ),
              ...op.sequences,
            ],
          });
        } catch (error) {
          if (error instanceof StoreCorruptionError) throw new CatalogChangeError(error.message);
          throw error;
        }
      }
    return operations.map((operation) =>
      operation.op === "createTable" ? { ...operation, op: "alterTable" as const } : operation,
    );
  }

  #planCreateTable(input: TableInput, previous?: TableDef): CatalogOperation[] {
    if (this.#nameTaken(input.name) && previous?.name !== input.name)
      throw new CatalogChangeError(`Relation ${input.name} already exists`);
    const counters = { ...this.counters };
    const tableId = previous?.id ?? counters.nextTableId;
    const names = new Set<string>();
    const primary = new Set(input.primaryKey ?? []);
    const sequences: SequenceDef[] = [];
    const columns: ColumnDef[] = input.columns.map((column, position) => {
      if (names.has(column.name)) throw new CatalogChangeError(`Column ${column.name} is repeated`);
      names.add(column.name);
      const id =
        previous !== undefined && column.id !== undefined ? column.id : counters.nextColumnId++;
      let identitySequence = column.identitySequence;
      if (column.identity !== undefined && column.identity !== null) {
        const range =
          column.autoIncrement && column.type.kind === "float64"
            ? { min: 1n, max: BigInt(Number.MAX_SAFE_INTEGER) }
            : intRange(column.type);
        const increment = column.identity.increment;
        const sequence: SequenceDef = {
          id: counters.nextSequenceId++,
          name: ownedSequenceName(
            input.name,
            column.name,
            (name) =>
              this.#nameTaken(name) ||
              this.sequences.some((sequence) => sequence.name === name) ||
              this.tables.some((table) => table.indexes.some((index) => index.name === name)) ||
              sequences.some((sequence) => sequence.name === name),
          ),
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
        ...(column.autoIncrement === true ? { autoIncrement: true as const } : {}),
        ...(column.identityAlways === true ? { identityAlways: true as const } : {}),
        missing: previous === undefined ? null : column.missing,
        ...(column.typeId === undefined ? {} : { typeId: column.typeId }),
        ...(column.allowedValues === undefined ? {} : { allowedValues: column.allowedValues }),
      };
    });
    const at = (position: number, what: string): ColumnDef => {
      const column = columns[position];
      if (column === undefined)
        throw new CatalogChangeError(`${what} names column position ${String(position)}`);
      return column;
    };
    const ids = columns.map((column) => column.id);
    input.columns.forEach((source, position) => {
      const column = at(position, "Expression dependencies");
      columns[position] = {
        ...column,
        ...(source.defaultDependencies === undefined
          ? {}
          : { defaultDependencies: resolveDependencies(source.defaultDependencies, ids) }),
        ...(source.generatedDependencies === undefined
          ? {}
          : { generatedDependencies: resolveDependencies(source.generatedDependencies, ids) }),
        ...(column.identitySequence === null
          ? {}
          : {
              defaultDependencies: {
                dialectVersion: 1,
                columnIds: [],
                sequenceIds: [column.identitySequence],
              },
            }),
      };
    });
    const shape = { id: tableId, columns };
    const indexes: IndexDef[] = [];
    let primaryKey: number | null = null;
    if (input.primaryKey !== null) {
      const index = this.#indexDef(
        shape,
        previous?.primaryKey ?? counters.nextIndexId++,
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
        (previous !== undefined ? unique.id : undefined) ?? counters.nextIndexId++,
        unique.name,
        unique.columns.map((position) => at(position, `Unique ${unique.name}`).id),
        true,
        "unique",
      );
      indexes.push(index);
      uniqueByName.set(unique.name, index);
    }
    for (const index of previous?.indexes ?? [])
      if (index.role === "secondary" || index.role === "foreignKey") indexes.push(index);
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
        id: (previous !== undefined ? input_.id : undefined) ?? counters.nextConstraintId++,
        name: input_.name,
        columnIds,
        parentTableId,
        parentIndexId: parentIndex.id,
        supportIndexId,
        onDelete: input_.onDelete,
        onUpdate: input_.onUpdate ?? "follow",
        enforced: input_.enforced,
      });
    }
    const checks: CheckDef[] = input.checks.map((check) => ({
      id: (previous !== undefined ? check.id : undefined) ?? counters.nextConstraintId++,
      name: check.name,
      conditionSql: check.conditionSql,
      columnIds: check.columns.map((position) => at(position, `Check ${check.name}`).id),
      ...(check.dependencies === undefined
        ? {}
        : { dependencies: resolveDependencies(check.dependencies, ids) }),
    }));
    const table: TableDef = {
      id: tableId,
      name: input.name,
      schemaVersion: previous === undefined ? 1 : previous.schemaVersion + 1,
      ...(input.managed === undefined ? {} : { managed: input.managed }),
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
    if (
      change.expectedVersion !== this.version ||
      change.version !== version ||
      version < this.version
    )
      throw new StoreCorruptionError("A logged catalog change has the wrong predecessor/version");
    const data = this.#data();
    const counters = { ...data.counters };
    retainCatalogExtensions(data.counters, counters);
    const bump = (key: keyof CatalogCounters, used: number) => {
      counters[key] = Math.max(counters[key], used + 1);
    };
    let { tables, sequences, views, types, fts, triggers } = data;
    switch (change.op) {
      case "createTrigger": {
        const def = change.trigger;
        validateTriggerDef(def);
        if (
          def.id < counters.nextTriggerId ||
          def.id >= Number.MAX_SAFE_INTEGER ||
          triggers.some((t) => t.name === def.name)
        )
          throw new StoreCorruptionError("Invalid trigger identity/name allocation");
        bump("nextTriggerId", def.id);
        triggers = [...triggers, def];
        break;
      }
      case "dropTrigger":
        if (!triggers.some((t) => t.id === change.triggerId))
          throw new StoreCorruptionError("Unknown dropped trigger identity");
        triggers = triggers.filter((t) => t.id !== change.triggerId);
        break;
      case "createFts":
      case "upsertFts": {
        const def = change.fts;
        validateFtsDef(def);
        const old = fts.find((entry) => entry.id === def.id);
        const column = this.table(def.tableId)?.columns.find((c) => c.id === def.columnId);
        if (
          !column ||
          (old &&
            (change.op === "createFts" ||
              old.tableId !== def.tableId ||
              old.columnId !== def.columnId)) ||
          (!old && def.id < counters.nextFtsId) ||
          fts.some(
            (entry) =>
              entry.id !== def.id &&
              entry.tableId === def.tableId &&
              entry.columnId === def.columnId,
          )
        )
          throw new StoreCorruptionError("Invalid FTS identity or column binding");
        if (def.id >= Number.MAX_SAFE_INTEGER) throw new StoreCorruptionError("FTS id exhausted");
        bump("nextFtsId", def.id);
        fts = [...fts.filter((entry) => entry.id !== def.id), def].sort((a, b) => a.id - b.id);
        break;
      }
      case "dropFts":
        if (!fts.some((entry) => entry.id === change.ftsId))
          throw new StoreCorruptionError("Unknown FTS id");
        fts = fts.filter((entry) => entry.id !== change.ftsId);
        break;
      case "createTable":
      case "alterTable": {
        const table = change.table;
        if (
          change.op === "alterTable" &&
          this.table(table.id)?.schemaVersion !== table.schemaVersion - 1
        )
          throw new StoreCorruptionError("A catalog ALTER skips its table schema version");
        if (change.op === "createTable" && this.#tables.has(table.id))
          throw new CatalogChangeError(`Table ${String(table.id)} exists`);
        bump("nextTableId", table.id);
        for (const column of table.columns) bump("nextColumnId", column.id);
        for (const index of table.indexes) bump("nextIndexId", index.id);
        for (const fk of table.foreignKeys) bump("nextConstraintId", fk.id);
        for (const check of table.checks) bump("nextConstraintId", check.id);
        for (const sequence of change.sequences) bump("nextSequenceId", sequence.id);
        tables =
          change.op === "createTable"
            ? [...tables, table]
            : tables.map((prior) => (prior.id === table.id ? table : prior));
        if (change.op === "alterTable")
          sequences = sequences.filter(
            (sequence) =>
              sequence.ownedBy?.tableId !== table.id ||
              table.columns.some((column) => column.identitySequence === sequence.id),
          );
        sequences = [...sequences, ...change.sequences];
        break;
      }
      case "dropTable":
        triggers = triggers.filter((t) => t.tableId !== change.tableId);
        fts = fts.filter((def) => def.tableId !== change.tableId);
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
      case "invalidateIndex":
        if (
          this.table(change.tableId)?.indexes.find((index) => index.id === change.indexId)
            ?.unique !== false
        )
          throw new CatalogChangeError("Only a nonunique index can be invalidated");
        tables = tables.map((table) =>
          table.id === change.tableId
            ? {
                ...table,
                indexes: table.indexes.map((index) =>
                  index.id === change.indexId ? { ...index, invalid: true as const } : index,
                ),
              }
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
      case "replaceView":
        views = views.map((view) => (view.id === change.view.id ? change.view : view));
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
    validateTriggerCatalog({ tables, triggers, types, sequences });
    return new Catalog({
      wireMetadata: this.wireMetadata,
      version,
      counters,
      tables,
      sequences,
      views,
      types,
      fts,
      triggers,
    });
  }

  // ── Stored form ────────────────────────────────────────────────────────────────────────────

  encode(): Uint8Array {
    return encodeCatalogStream(this.#data());
  }

  static decode(bytes: Uint8Array): Catalog {
    return Catalog.fromStored(decodeCatalogStream(bytes));
  }

  /** Validation shared by native pages and the portable logical stream. */
  static fromStored(data: CatalogStreamData, budget?: ByteBudget): Catalog {
    // Temporary graph-validation IDs/names are distinct from the CatalogView's existing
    // resident definitions. Admit before constructing the temporary sets and maps.
    let entries =
      data.tables.length +
      data.sequences.length +
      data.views.length +
      data.types.length +
      data.fts.length +
      data.triggers.length;
    for (const trigger of data.triggers)
      for (const body of trigger.statements)
        entries +=
          1 +
          body.bindings.length +
          body.columns.length +
          body.typeIds.length +
          body.sequenceIds.length +
          body.insertColumnIds.length +
          body.defaultColumnIds.length +
          body.assignedColumnIds.length;
    for (const table of data.tables)
      entries +=
        table.columns.length +
        table.indexes.length +
        table.checks.length +
        table.foreignKeys.length;
    for (const table of data.tables) {
      for (const column of table.columns)
        entries +=
          (column.defaultDependencies?.columnIds.length ?? 0) +
          (column.defaultDependencies?.sequenceIds.length ?? 0) +
          (column.defaultDependencies?.typeIds?.length ?? 0) +
          (column.generatedDependencies?.columnIds.length ?? 0) +
          (column.generatedDependencies?.sequenceIds.length ?? 0) +
          (column.generatedDependencies?.typeIds?.length ?? 0) +
          (column.allowedValues?.length ?? 0);
      for (const condition of table.checks)
        entries +=
          (condition.dependencies?.columnIds.length ?? 0) +
          (condition.dependencies?.sequenceIds.length ?? 0) +
          (condition.dependencies?.typeIds?.length ?? 0);
    }
    for (const view of data.views)
      entries +=
        (view.outputs?.length ?? 0) +
        (view.dependencies?.reduce((n, dep) => n + 1 + dep.columnIds.length, 0) ?? 0) +
        (view.sourceSchemas?.reduce((n, source) => n + 1 + source.columnIds.length, 0) ?? 0);
    // Enum duplicate checks own one temporary Set at a time. Prepay its largest
    // label table, not the sum across definitions/columns visited sequentially.
    let labels = 0;
    for (const type of data.types)
      if (Array.isArray(type.labels)) labels = Math.max(labels, type.labels.length);
    for (const table of data.tables)
      for (const column of table.columns) labels = Math.max(labels, enumLabelCount(column.type));
    const scratch = 1024 + entries * 256 + labels * 64;
    if (budget && !budget.allocate(scratch)) throw new ParseBudgetError(scratch);
    try {
      validateStoredCatalog(data);
      validateRelationalCatalog(data);
      return new Catalog(data);
    } catch (error) {
      if (
        error instanceof StoreCorruptionError ||
        error instanceof UnsupportedCatalogError ||
        error instanceof DurableFormatError
      )
        throw error;
      throw new StoreCorruptionError(`A stored catalog is malformed: ${String(error)}`);
    } finally {
      budget?.free(scratch);
    }
  }
}

/** Sizing precedes semantic validation: do not trust the asserted physical-type shape. */
function enumLabelCount(value: unknown): number {
  for (let depth = 0; depth <= 8; depth++) {
    if (typeof value !== "object" || value === null || !("kind" in value)) return 0;
    if (value.kind === "enum")
      return "labels" in value && Array.isArray(value.labels) ? value.labels.length : 0;
    if (value.kind !== "array" || !("element" in value)) return 0;
    value = value.element;
  }
  return 0;
}

/** Validate the complete stored graph, never a transient drop/recreate during ALTER replay. */
function validateStoredCatalog(data: CatalogData): void {
  const check = (ok: boolean, message: string) => {
    if (!ok) throw new StoreCorruptionError(message);
  };
  const ids = (values: ReadonlyArray<{ readonly id: number }>, counter: number, name: string) => {
    const seen = new Set<number>();
    for (const value of values) {
      check(
        value.id > 0 && value.id < counter && !seen.has(value.id),
        `Invalid/duplicate stored ${name} id`,
      );
      seen.add(value.id);
    }
    check(counter >= 1, `Invalid stored ${name} counter`);
  };
  const names = (values: ReadonlyArray<{ readonly name: string }>) => {
    const seen = new Set<string>();
    for (const value of values) {
      check(!seen.has(value.name), "Duplicate stored catalog name");
      seen.add(value.name);
    }
  };
  ids(data.tables, data.counters.nextTableId, "table");
  ids(
    data.tables.flatMap((t) => t.columns),
    data.counters.nextColumnId,
    "column",
  );
  ids(
    data.tables.flatMap((t) => t.indexes),
    data.counters.nextIndexId,
    "index",
  );
  ids(
    data.tables.flatMap((t) => [...t.checks, ...t.foreignKeys]),
    data.counters.nextConstraintId,
    "constraint",
  );
  ids(data.sequences, data.counters.nextSequenceId, "sequence");
  ids(data.views, data.counters.nextViewId, "view");
  ids(data.types, data.counters.nextTypeId, "type");
  ids(data.fts, data.counters.nextFtsId, "FTS");
  ids(data.triggers, data.counters.nextTriggerId, "trigger");
  for (const trigger of data.triggers)
    for (const body of trigger.statements)
      for (const ref of [...body.bindings, ...body.columns]) validateCatalogType(ref.type);
  validateTriggerCatalog(data);
  const bindings = new Set<string>();
  let previousFtsId = 0;
  for (const def of data.fts) {
    validateFtsDef(def);
    const key = `${String(def.tableId)}:${String(def.columnId)}`;
    check(
      def.id > previousFtsId &&
        !bindings.has(key) &&
        data.tables.some(
          (table) => table.id === def.tableId && table.columns.some((c) => c.id === def.columnId),
        ),
      "Invalid stored FTS order or column binding",
    );
    previousFtsId = def.id;
    bindings.add(key);
  }
  names([...data.tables, ...data.views]);
  names(data.sequences);
  names(data.types);
  const tables = new Map(data.tables.map((t) => [t.id, t]));
  const sequences = new Set(data.sequences.map((s) => s.id));
  for (const table of data.tables) {
    validateStoredTable(table);
    check(table.schemaVersion > 0, "Invalid stored schema version");
    names(table.columns);
    names(table.indexes);
    names([...table.foreignKeys, ...table.checks]);
    const columns = new Set(table.columns.map((c) => c.id));
    for (const column of table.columns)
      check(
        column.identitySequence === null || sequences.has(column.identitySequence),
        "Missing stored identity sequence",
      );
    for (const index of table.indexes)
      check(
        index.tableId === table.id &&
          index.columnIds.length > 0 &&
          index.columnIds.every((id) => columns.has(id)),
        "Invalid stored index references",
      );
    check(
      table.primaryKey === null ||
        table.indexes.some((i) => i.id === table.primaryKey && i.unique && i.role === "primary"),
      "Missing stored primary key",
    );
    for (const condition of table.checks)
      check(
        condition.columnIds.every((id) => columns.has(id)),
        "Invalid stored check dependency",
      );
    for (const fk of table.foreignKeys) {
      const parentTable = tables.get(fk.parentTableId);
      const parent = parentTable?.indexes.find((i) => i.id === fk.parentIndexId);
      check(
        fk.columnIds.length > 0 && fk.columnIds.every((id) => columns.has(id)),
        "Invalid stored foreign-key columns",
      );
      // Informational relationships survive the deliberate drop of their parent (shipped contract).
      // An id outside the allocated namespace was never a valid parent, even informationally.
      check(
        fk.parentTableId < data.counters.nextTableId &&
          fk.parentIndexId < data.counters.nextIndexId,
        "Unknown stored foreign-key identity",
      );
      if (fk.enforced || parentTable !== undefined)
        check(
          parent !== undefined &&
            parent.unique &&
            parent.columnIds.length === fk.columnIds.length &&
            parent.columnIds.every((id, position) => {
              const p = parentTable?.columns.find((c) => c.id === id);
              const c = table.columns.find((c) => c.id === fk.columnIds[position]);
              return p !== undefined && c !== undefined && samePhysicalType(p.type, c.type);
            }),
          "Invalid stored foreign-key references",
        );
      const support = table.indexes.find((i) => i.id === fk.supportIndexId);
      check(!fk.enforced || support !== undefined, "Missing stored foreign-key support index");
      check(
        fk.supportIndexId === null ||
          (support !== undefined &&
            support.columnIds.length >= fk.columnIds.length &&
            fk.columnIds.every((id, position) => support.columnIds[position] === id)),
        "Invalid stored foreign-key support columns",
      );
    }
  }
  for (const sequence of data.sequences) {
    validateSequence(sequence);
    check(
      sequence.ownedBy === null ||
        tables
          .get(sequence.ownedBy.tableId)
          ?.columns.some((c) => c.id === sequence.ownedBy?.columnId) === true,
      "Invalid stored sequence owner",
    );
  }
  for (const type of data.types)
    check(
      type.labels.length <= ENUM_LABEL_LIMIT &&
        type.labels.every((label) => utf8Length(label) <= ENUM_LABEL_BYTES) &&
        new Set(type.labels).size === type.labels.length,
      "Invalid stored enum labels",
    );
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
    case "createTrigger":
      return change.trigger.id;
    case "createFts":
      return change.fts.id;
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

export function encodeLogged(change: LoggedChange, allocator?: BufferAllocator): Uint8Array {
  return encodeCatalogChange(change, allocator);
}

function positiveId(id: number): void {
  if (!Number.isSafeInteger(id) || id <= 0)
    throw new StoreCorruptionError("Invalid stored catalog id");
}
function validateStoredIndex(index: IndexDef): void {
  positiveId(index.id);
  positiveId(index.tableId);
  if (!index.columnIds.length) throw new StoreCorruptionError("Empty stored index key");
  if (
    index.directions !== undefined &&
    (index.directions.length !== index.columnIds.length || index.directions.length === 0)
  )
    throw new StoreCorruptionError("Invalid stored index directions");
  for (const direction of index.directions ?? [])
    requireTag(direction, ["asc", "desc"], "index direction", StoreCorruptionError);
  index.columnIds.forEach(positiveId);
  if (index.keyLimit !== undefined)
    requireTag(
      index.keyLimit,
      DURABLE_CODECS.indexKeyLimit,
      "index key limit",
      StoreCorruptionError,
    );
  if (index.invalid !== undefined)
    requireTag(
      index.invalid,
      DURABLE_CODECS.presentFlag,
      "invalid index flag",
      StoreCorruptionError,
    );
  requireTag(index.unique, DURABLE_CODECS.jsonBoolean, "stored unique flag", StoreCorruptionError);
  requireTag(index.kind, DURABLE_CODECS.indexKind, "stored index kind", StoreCorruptionError);
  requireTag(index.role, DURABLE_CODECS.indexRole, "stored index role", StoreCorruptionError);
}
function validateStoredTable(table: TableDef): void {
  positiveId(table.id);
  positiveId(table.schemaVersion);
  if (table.primaryKey !== null) positiveId(table.primaryKey);
  for (const condition of table.checks) {
    positiveId(condition.id);
    condition.columnIds.forEach(positiveId);
  }
  for (const column of table.columns) {
    positiveId(column.id);
    if (column.identitySequence !== null) positiveId(column.identitySequence);
    validateCatalogType(column.type);
    validateCatalogDeclared(column.declared, column.type);
    requireTag(
      column.nullable,
      DURABLE_CODECS.jsonBoolean,
      "stored nullable flag",
      StoreCorruptionError,
    );
    // Wire discriminator validation only; physicalTypeOf remains the sole SQL-type mapping.
    requireTag(
      column.declared.type,
      DURABLE_CODECS.simpleSql,
      "stored SQL type",
      StoreCorruptionError,
    );
    if (column.declared.sqlDomain !== undefined)
      requireTag(
        column.declared.sqlDomain.kind,
        DURABLE_CODECS.sqlDomain,
        "stored SQL domain",
        StoreCorruptionError,
      );
    if (column.declared.integer !== undefined)
      requireTag(
        column.declared.integer,
        DURABLE_CODECS.presentFlag,
        "stored integer flag",
        StoreCorruptionError,
      );
    if (column.declared.withoutTimeZone !== undefined)
      requireTag(
        column.declared.withoutTimeZone,
        DURABLE_CODECS.presentFlag,
        "stored timezone flag",
        StoreCorruptionError,
      );
    if (column.declared.declaredLength !== undefined)
      requireTag(
        column.declared.declaredLength.fixed,
        DURABLE_CODECS.jsonBoolean,
        "stored fixed-width flag",
        StoreCorruptionError,
      );
    const missing = column.missing;
    if (missing !== null) {
      validateCatalogMissing(missing, column.type);
      requireTag(
        missing.form,
        DURABLE_CODECS.vectorForm,
        "stored missing vector form",
        StoreCorruptionError,
      );
      validateCatalogType(missing.type);
      requireTag(
        missing.data.layout,
        DURABLE_CODECS.columnLayout,
        "stored missing layout",
        StoreCorruptionError,
      );
      if (missing.data.layout === "int64")
        requireTag(
          missing.data.values.repr,
          DURABLE_CODECS.vectorIntegerRepr,
          "stored integer representation",
          StoreCorruptionError,
        );
      if (missing.data.layout === "decimal")
        requireTag(
          missing.data.coefficients.repr,
          DURABLE_CODECS.vectorDecimalRepr,
          "stored coefficient representation",
          StoreCorruptionError,
        );
      if (missing.data.layout === "interval")
        requireTag(
          missing.data.micros.repr,
          DURABLE_CODECS.vectorIntegerRepr,
          "stored interval representation",
          StoreCorruptionError,
        );
    }
  }
  for (const index of table.indexes) validateStoredIndex(index);
  for (const foreignKey of table.foreignKeys) {
    positiveId(foreignKey.id);
    positiveId(foreignKey.parentTableId);
    positiveId(foreignKey.parentIndexId);
    if (foreignKey.supportIndexId !== null) positiveId(foreignKey.supportIndexId);
    foreignKey.columnIds.forEach(positiveId);
    requireTag(
      foreignKey.enforced,
      DURABLE_CODECS.jsonBoolean,
      "stored foreign-key enforcement flag",
      StoreCorruptionError,
    );
    requireTag(
      foreignKey.onDelete,
      DURABLE_CODECS.foreignKeyAction,
      "stored foreign-key action",
      StoreCorruptionError,
    );
    requireTag(
      foreignKey.onUpdate,
      DURABLE_CODECS.foreignKeyUpdateAction,
      "stored foreign-key update action",
      StoreCorruptionError,
    );
  }
}

export function decodeLogged(bytes: Uint8Array): LoggedChange {
  try {
    const value = decodeCatalogChange(bytes);
    requireTag(
      value.op,
      DURABLE_CODECS.catalogOperation,
      "catalog operation",
      StoreCorruptionError,
    );
    if (
      !Number.isSafeInteger(value.expectedVersion) ||
      value.expectedVersion < 0 ||
      !Number.isSafeInteger(value.version) ||
      value.version < 1 ||
      value.version < value.expectedVersion
    )
      throw new StoreCorruptionError("Invalid logged catalog version precondition");
    const fields = CATALOG_CHANGE_FIELDS[value.op];
    for (const name of fields)
      if (!Object.hasOwn(value, name))
        throw new StoreCorruptionError(`Missing logged catalog field ${name}`);
    for (const name of Object.keys(value))
      if (
        name !== "op" &&
        name !== "expectedVersion" &&
        name !== "version" &&
        !(fields as readonly string[]).includes(name)
      )
        throw new StoreCorruptionError(`Unexpected logged catalog field ${name}`);
    const record = value as unknown as Record<string, unknown>;
    for (const name of [
      "tableId",
      "indexId",
      "sequenceId",
      "viewId",
      "typeId",
      "ftsId",
      "triggerId",
    ] as const) {
      const id = record[name];
      if (name in record && !(typeof id === "number" && id > 0))
        throw new StoreCorruptionError(`Invalid logged catalog id ${name}`);
    }
    if (value.op === "createTrigger") {
      validateTriggerDef(value.trigger);
      for (const body of value.trigger.statements)
        for (const ref of [...body.bindings, ...body.columns]) validateCatalogType(ref.type);
    }
    if (value.op === "createFts" || value.op === "upsertFts") validateFtsDef(value.fts);
    if (value.op === "createTable" || value.op === "alterTable") validateStoredTable(value.table);
    if (value.op === "createIndex") validateStoredIndex(value.index);
    if (value.op === "putSequence") {
      positiveId(value.sequence.id);
      validateSequence(value.sequence);
    }
    if (value.op === "createTable" || value.op === "alterTable")
      for (const sequence of value.sequences) {
        positiveId(sequence.id);
        validateSequence(sequence);
      }
    if (value.op === "createView" || value.op === "replaceView") positiveId(value.view.id);
    if (value.op === "createType") positiveId(value.type.id);
    return value;
  } catch (error) {
    if (
      error instanceof StoreCorruptionError ||
      error instanceof UnsupportedCatalogError ||
      error instanceof DurableFormatError
    )
      throw error;
    throw new StoreCorruptionError(`A logged catalog change is malformed: ${String(error)}`);
  }
}
