/**
 * The catalog as the new core stores and serves it (owner: write path, track B3). Lowering reads
 * it to resolve names and compile defaults, checks and generated columns; the executor reads
 * column types and index definitions; the write path enforces what it declares.
 *
 * ## Rules
 * - **Plain, immutable data.** A `CatalogView` never changes; a DDL statement produces a new one.
 *   Its `version` increases with every change, committed or not, and is never reused (a rolled
 *   back DDL burns its version), so `(statement text, version)` is a safe plan-cache key
 *   (ir/ir.ts `PhysicalPlan.catalogVersion`).
 * - **Stable ids.** Table, column, index, constraint and sequence ids are never reused, so a
 *   rename changes names only and stored data refers to ids. Columns are in catalog order
 *   (`TableDef.columns`), which is the order of `TableWriter.insert` batches.
 * - **Expressions are SQL text.** Defaults, CHECK conditions, generated columns and view bodies
 *   are stored as the front end's normalized SQL text; lowering compiles them (cached per
 *   version). The write path never evaluates an expression: it calls the executor's
 *   `TableRules` (modify.ts).
 * - **Indexes are scalar-only** (keys/key-encoding.ts `indexKeyKind`): DDL refuses indexes,
 *   primary keys, unique constraints and foreign keys over arrays or JSON with
 *   `UnindexableTypeError`. An enum index is equality-only (`IndexDef.kind`).
 * - **Every enforced foreign key has a supporting index** on the referencing columns (role
 *   `"foreignKey"`, created with the constraint unless an index with those leading columns
 *   exists), so a parent's delete or rekey finds its children by index probe, never by a scan.
 * - **Named types** (ruling R17.7): `CREATE TYPE … AS ENUM` is a `TypeDef`; a column of that type
 *   has an enum `PhysicalType` with the same name and labels. A type a column uses cannot be
 *   dropped.
 * - **Owned identity sequences** (R17.7): a `TableInput` column with `identity` gets a sequence
 *   created with the table, owned by that column (`SequenceDef.ownedBy`, `identitySequence`), and
 *   dropped with it.
 * - **Self-referencing foreign keys** (R17.7): `parentTableId: "self"` names the table being
 *   created; its `parentIndexId` is then `"primary"` or `{ unique: name }` of the same input.
 */
import type { SqlTypeDescriptor, PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";

export interface CatalogView {
  readonly version: number;
  readonly tables: readonly TableDef[];
  readonly sequences: readonly SequenceDef[];
  readonly views: readonly ViewDef[];
  /** Named types (ruling R17.7). */
  readonly types: readonly TypeDef[];
  type(typeId: number): TypeDef | undefined;
  typeByName(name: string): TypeDef | undefined;
  /** By id; undefined when absent (dropped ids are never reused). */
  table(tableId: number): TableDef | undefined;
  /** Case-sensitive lookup of a normalized name (the front end normalizes identifiers). */
  tableByName(name: string): TableDef | undefined;
  sequence(sequenceId: number): SequenceDef | undefined;
}

export interface TableDef {
  readonly id: number;
  readonly name: string;
  /** Increases with every ALTER of this table (format/replay.ts checks frames against it). */
  readonly schemaVersion: number;
  /** Live columns in catalog order (dropped columns are absent; their ids are not reused). */
  readonly columns: readonly ColumnDef[];
  /** The primary key's index, or null for a table without one. */
  readonly primaryKey: number | null;
  /** Every index of the table: primary key, unique constraints, secondary and FK-supporting. */
  readonly indexes: readonly IndexDef[];
  readonly foreignKeys: readonly ForeignKeyDef[];
  readonly checks: readonly CheckDef[];
}

export interface ColumnDef {
  readonly id: number;
  readonly name: string;
  readonly type: PhysicalType;
  /** The declared SQL type, kept for the front end (DECISIONS.md C5: widths are mandatory). */
  readonly declared: SqlTypeDescriptor;
  readonly nullable: boolean;
  /** Default expression (SQL text), or null. Applied by lowering, never by the write path. */
  readonly defaultSql: string | null;
  /** Stored generated column expression (SQL text), or null. */
  readonly generatedSql: string | null;
  /** The sequence behind SERIAL / IDENTITY / AUTOINCREMENT, or null. */
  readonly identitySequence: number | null;
  /**
   * The value of rows stored before the column existed (ALTER TABLE ADD COLUMN with a constant
   * default; PostgreSQL's `attmissingval`): a length-1 flat vector, null via its validity. Null
   * when such rows read as SQL NULL.
   */
  readonly missing: FlatVector | null;
}

export interface IndexDef {
  readonly id: number;
  readonly name: string;
  readonly tableId: number;
  /** Key columns in key order. Keys are equality keys of these columns' physical types. */
  readonly columnIds: readonly number[];
  readonly unique: boolean;
  /** `ordered`: equality and ranges in key-byte order; `equality`: lookups only (enum). */
  readonly kind: "ordered" | "equality";
  readonly role: "primary" | "unique" | "secondary" | "foreignKey";
}

export interface ForeignKeyDef {
  readonly id: number;
  readonly name: string;
  /** Referencing columns of this table, in the parent key's order. */
  readonly columnIds: readonly number[];
  readonly parentTableId: number;
  /** The parent's primary-key (or unique) index the reference names. */
  readonly parentIndexId: number;
  /** The supporting index on `columnIds` (see "Rules"); null when `enforced` is false. */
  readonly supportIndexId: number | null;
  readonly onDelete: "restrict" | "cascade" | "setNull";
  /**
   * A parent rekey (UPDATE of its key) always rewrites the reference in the same statement
   * (today's behaviour; PostgreSQL's ON UPDATE CASCADE).
   */
  readonly enforced: boolean;
}

export interface CheckDef {
  readonly id: number;
  readonly name: string;
  /** The condition (SQL text) over this table's columns. False fails; NULL passes. */
  readonly conditionSql: string;
  /** Columns the condition reads, so an UPDATE that assigns none of them skips the check. */
  readonly columnIds: readonly number[];
}

export interface SequenceDef {
  readonly id: number;
  readonly name: string;
  readonly increment: bigint;
  readonly min: bigint;
  readonly max: bigint;
  readonly start: bigint;
  readonly cycle: boolean;
  /** The column this sequence belongs to (SERIAL / IDENTITY), or null. */
  readonly ownedBy: { readonly tableId: number; readonly columnId: number } | null;
}

/** `CREATE TYPE name AS ENUM (labels…)` (ruling R17.7). */
export interface TypeDef {
  readonly id: number;
  readonly name: string;
  readonly labels: readonly string[];
}

export interface ViewDef {
  readonly id: number;
  readonly name: string;
  readonly bodySql: string;
}

/**
 * A DDL request (`Statement.changeCatalog`). Ids in a `create*` are assigned by the write path;
 * the input omits them. ALTER is expressed as the table's next definition: the write path diffs
 * it, validates what the diff needs (existing rows against a new NOT NULL, CHECK, UNIQUE or
 * foreign key; type changes are refused until a rewrite path exists), converts memtable rows,
 * and bumps `schemaVersion`.
 */
export type CatalogChange =
  | { readonly kind: "createTable"; readonly table: TableInput }
  | { readonly kind: "alterTable"; readonly tableId: number; readonly table: TableInput }
  | { readonly kind: "dropTable"; readonly tableId: number }
  | { readonly kind: "createIndex"; readonly index: Omit<IndexDef, "id" | "kind"> }
  | { readonly kind: "dropIndex"; readonly indexId: number }
  | { readonly kind: "createSequence"; readonly sequence: Omit<SequenceDef, "id"> }
  | { readonly kind: "alterSequence"; readonly sequence: SequenceDef }
  | { readonly kind: "dropSequence"; readonly sequenceId: number }
  | { readonly kind: "createView"; readonly view: Omit<ViewDef, "id"> }
  | { readonly kind: "dropView"; readonly viewId: number }
  | { readonly kind: "createType"; readonly type: Omit<TypeDef, "id"> }
  | { readonly kind: "dropType"; readonly typeId: number };

/**
 * A table definition as DDL supplies it. Existing columns, indexes and constraints keep their ids
 * (an entry with an `id` is the same object); entries without one are new.
 */
export interface TableInput {
  readonly name: string;
  readonly columns: ReadonlyArray<
    Omit<ColumnDef, "id"> & {
      readonly id?: number;
      /**
       * An owned identity sequence to create with the table (SERIAL, IDENTITY, AUTOINCREMENT;
       * ruling R17.7). Then `identitySequence` is ignored and set by the write path.
       */
      readonly identity?: { readonly start: bigint; readonly increment: bigint } | null;
    }
  >;
  /** Primary key columns (by position in `columns`), or null. */
  readonly primaryKey: readonly number[] | null;
  readonly unique: ReadonlyArray<{
    readonly id?: number;
    readonly name: string;
    readonly columns: readonly number[];
  }>;
  readonly foreignKeys: ReadonlyArray<
    Omit<
      ForeignKeyDef,
      "id" | "columnIds" | "supportIndexId" | "parentTableId" | "parentIndexId"
    > & {
      readonly id?: number;
      readonly columns: readonly number[];
      /** `"self"`: the table this input defines (ruling R17.7). */
      readonly parentTableId: number | "self";
      /** For `"self"`: this input's `"primary"` key or one of its `unique` constraints by name. */
      readonly parentIndexId: number | "primary" | { readonly unique: string };
    }
  >;
  readonly checks: ReadonlyArray<
    Omit<CheckDef, "id" | "columnIds"> & {
      readonly id?: number;
      readonly columns: readonly number[];
    }
  >;
}

/** A DDL request the catalog refuses (name clash, unknown id, unsupported change). */
export class CatalogChangeError extends Error {
  readonly code = "MINNOW_CATALOG_CHANGE";
  constructor(message: string) {
    super(message);
    this.name = "CatalogChangeError";
  }
}
