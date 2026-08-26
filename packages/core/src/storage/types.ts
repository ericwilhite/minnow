import { dateIsoString, dateMilliseconds } from "../date-value.js";
import {
  assertWellFormedString,
  MAX_BLOCK_ROW_COUNT,
  MAX_STORED_BLOCK_BYTE_LENGTH,
} from "../block-format/index.js";

export const storeNames = [
  "catalog",
  "manifests",
  "segments",
  "blocks",
  "transactions",
  "leases",
  "statistics",
  "temp",
  "gc",
] as const;

export const MAX_MANIFEST_CHANGED_TABLE_IDS = 1_024;

export function canonicalManifestChangedTableIds(ids: readonly string[]): string[] {
  if (ids.length > MAX_MANIFEST_CHANGED_TABLE_IDS) {
    throw new RangeError(
      `Manifest changed-table IDs cannot exceed ${String(MAX_MANIFEST_CHANGED_TABLE_IDS)}`,
    );
  }
  const canonical = [...new Set(ids.map((id) => validateStorageId(id, "Changed table ID")))].sort();
  if (canonical.length > MAX_MANIFEST_CHANGED_TABLE_IDS) {
    throw new RangeError(
      `Manifest changed-table IDs cannot exceed ${String(MAX_MANIFEST_CHANGED_TABLE_IDS)}`,
    );
  }
  return canonical;
}

export function validateCanonicalManifestChangedTableIds(ids: readonly string[]): string[] {
  const canonical = canonicalManifestChangedTableIds(ids);
  if (canonical.length !== ids.length || canonical.some((id, index) => id !== ids[index])) {
    throw new TypeError("Manifest changed-table IDs must be unique and lexically sorted");
  }
  return canonical;
}

/**
 * The bounded manifest record every commit publishes. Block membership is paged separately;
 * reading this record must never materialize a database-sized ID array.
 */
export interface ManifestSummary {
  version: number;
  previousVersion: number | null;
  createdAt: string;
  /** Exact live payload membership cardinality at this version. */
  liveBlockCount: number;
  /** Exact sum of stored payload bytes in the live membership at this version. */
  liveBlockBytes: number;
  /** Table IDs whose logical content changed; empty means compaction or another logical no-op. */
  changedTableIds: string[];
  /** A pruned descriptor remains readable for commit reconciliation but cannot be pinned. */
  prunedAt?: string;
  /**
   * Commit-local maintenance hints, not persisted in manifest history. A ready full-text
   * column reports its durable delta-tail length so the committing engine can rebuild before
   * metadata grows with every later commit, even when nobody searches the column again.
   */
  ftsDeltaCounts?: Array<{ tableId: string; columnId: string; count: number }>;
}

/** Public/cold manifest inspection is bounded metadata only. */
export type Manifest = ManifestSummary;

/**
 * One payload's bounded manifest-membership interval. The block is visible at version `v` iff
 * `addedVersion <= v && (removedVersion === null || v < removedVersion)`. `byteLength` and the
 * payload bytes are immutable while this record exists; `removedVersion`, when present, is
 * strictly greater than `addedVersion`.
 *
 * A retired ID cannot be added again while this provenance record or a transaction/job that
 * names it is retained. Once every readable/pinned version in the interval and every recovery
 * root is gone, garbage collection removes payload and provenance together; no immortal ID
 * tombstone is required.
 */
export interface ManifestBlockRecord {
  readonly blockId: string;
  readonly byteLength: number;
  /** CRC-32 of the exact stored payload, computed once before the first durable write. */
  readonly checksum: number;
  readonly addedVersion: number;
  readonly removedVersion: number | null;
}

export interface ListManifestBlockPageInput {
  readonly version: number;
  readonly afterBlockId: string | null;
  readonly limit: number;
}

export interface ListRetiredManifestBlockPageInput {
  /** Includes records retired at or before this published version. */
  readonly removedThroughVersion: number;
  readonly afterBlockId: string | null;
  readonly limit: number;
}

export interface ManifestBlockPage {
  readonly records: ReadonlyArray<Pick<ManifestBlockRecord, "blockId" | "byteLength" | "checksum">>;
  readonly nextCursor: string | null;
}

/** Input for adapter-internal manifest construction; not a public store mutation. */
export interface CreateManifestInput {
  changedTableIds: readonly string[];
  expectedVersion: number | null;
  liveBlockCount: number;
  liveBlockBytes: number;
  createdAt?: string;
}

/**
 * The stored manifest is the same bounded summary returned publicly. Exact block membership is
 * represented once by `ManifestBlockRecord` intervals; checkpoint/delta arrays are deliberately
 * absent from the v1 layout.
 */
export type StoredManifestRecord = ManifestSummary;

/**
 * One atomic user-table drop. The store compares both catalog and manifest revisions, derives
 * every block that the table's segments still contribute to that exact manifest, publishes one
 * successor without those blocks, and removes all table-owned metadata in the same durable
 * step. Stored block payloads remain for lease-aware collection.
 */
export interface DropTableInput {
  tableId: string;
  expectedTableRevision: number;
  expectedManifestVersion: number | null;
  /** Catalog epoch of the complete dependency proof used to authorize this drop. */
  expectedCatalogEpoch: number;
  committedAt: string;
}

/** One atomic column retirement; see `CatalogStore.dropTableColumn`. */
export interface DropTableColumnInput {
  tableId: string;
  columnId: string;
  expectedTableRevision: number;
  expectedManifestVersion: number | null;
  /** Catalog epoch of the complete dependency proof used to authorize this drop. */
  expectedCatalogEpoch: number;
  committedAt: string;
}

export const simpleDataTypes = ["boolean", "number", "string", "datetime"] as const;
export type SimpleDataType = (typeof simpleDataTypes)[number];

/** PostgreSQL logical domains layered over the four stable physical block encodings. */
export type SqlDomain =
  | { kind: "numeric"; precision?: number; scale?: number }
  | { kind: "json" | "jsonb" | "uuid" | "date" | "time" | "interval" }
  | { kind: "array"; element: string }
  | { kind: "enum"; name: string; values: string[] };

/** Validates the logical metadata shared by SQL DDL, the schema DSL, and restored catalogs. */
export function validateSqlDomain(domain: SqlDomain, context: string): SqlDomain {
  if (domain.kind === "numeric") {
    const { precision, scale } = domain;
    if (
      (precision !== undefined &&
        (!Number.isSafeInteger(precision) || precision < 1 || precision > 100_000)) ||
      (scale !== undefined && (!Number.isSafeInteger(scale) || scale < 0 || scale > 100_000)) ||
      (precision !== undefined && scale !== undefined && scale > precision)
    ) {
      throw new TypeError(`Invalid NUMERIC domain metadata: ${context}`);
    }
  }
  if (
    domain.kind === "array" &&
    (domain.element.trim().length === 0 || domain.element.trim() !== domain.element)
  ) {
    throw new TypeError(`ARRAY element type must be a trimmed non-empty name: ${context}`);
  }
  if (domain.kind === "enum") {
    if (domain.name.trim().length === 0 || domain.name.trim() !== domain.name) {
      throw new TypeError(`Enum type name must be a trimmed non-empty name: ${context}`);
    }
    validateEnumValues(domain.values, domain.name);
  }
  return structuredClone(domain);
}

/**
 * Declarative SQL write-time default. The spec is structured-clone-safe because it crosses the
 * worker boundary and persists in the catalog. Expressions are parsed and type-checked by the
 * engine before the catalog is changed, then evaluated once for every omitted insert slot.
 */
export type ColumnDefault =
  | { kind: "literal"; value: boolean | number | string | Date }
  | { kind: "expression"; sql: string }
  | { kind: "autoincrement" };

export interface TableColumnRecord {
  id: string;
  name: string;
  type: SimpleDataType;
  /**
   * SQL INTEGER/SMALLINT/BIGINT columns use the number physical type, but only accept exact
   * JavaScript safe integers. Absent for the public `number` type and SQL floating-point types.
   * Keeping the domain in catalog metadata prevents a declared integer from silently rounding
   * before it reaches storage while preserving the released Float64 block encoding.
   */
  integer?: true;
  /** SQL-level semantics for a value physically encoded in this column's primitive type. */
  sqlDomain?: SqlDomain;
  nullable: boolean;
  /** Fills omitted or SQL `DEFAULT` slots at insert time; explicit NULL is never replaced. */
  defaultValue?: ColumnDefault;
  /**
   * What rows written before this column existed read as, instead of NULL.
   *
   * A column added by a migration has no blocks in older segments. Those rows would otherwise
   * read NULL forever, which is why adding a non-nullable column was impossible. Substituting
   * this value at read time makes the addition meaningful without rewriting a single stored
   * byte — the segments are untouched, and compaction folds the value in whenever it next
   * rewrites them. It is frozen when the column is added: a generator runs once, at migration
   * time, so every reader of a given row agrees.
   *
   * Spelled out rather than imported: storage sits below the engine, and NULL is the absence
   * this replaces, so it is not one of the options.
   */
  backfill?: boolean | number | string | Date;
  /**
   * String columns only: the closed set of values writes must draw from. Physically the column
   * stays a plain string column; the set is write-time validation metadata, so widening it (or
   * dropping it) is catalog-only while narrowing it is rejected by migration planning.
   */
  enumValues?: string[];
  /** Engine-generated row-addressing column; stored and indexed, never exposed as SQL schema. */
  hidden?: true;
}

export const MAX_TABLE_COLUMNS = 1_024;
export const MAX_ENUM_VALUES = 4_096;
export const MAX_SECONDARY_INDEXES = 1_024;
export const MAX_TABLE_TRIGGERS = 256;
export const MAX_TRIGGER_STATEMENTS = 256;
const VALID_TRIGGER_EVENTS: readonly unknown[] = ["insert", "update", "delete"];
const VALID_TRIGGER_TIMINGS: readonly unknown[] = ["before", "after"];
const VALID_TRIGGER_BINDING_SOURCES: readonly unknown[] = ["new", "old"];
export const MAX_TABLE_CONSTRAINTS = 1_024;
export const MAX_TABLE_RECORD_CHARACTERS = 1_048_576;
export const MAX_TABLE_RECORD_ENTRIES = 65_536;

/**
 * The single authority on which enum declarations are legal, shared by the schema DSL's
 * `column.enum()` and the engine's `createTable`: at least one value, every value a non-empty
 * string, no duplicates. Returns a defensive copy.
 */
export function validateEnumValues(values: readonly string[], context: string): string[] {
  if (values.length === 0) {
    throw new TypeError(`An enum needs at least one value: ${context}`);
  }
  if (values.length > MAX_ENUM_VALUES) {
    throw new RangeError(`An enum cannot exceed ${String(MAX_ENUM_VALUES)} values: ${context}`);
  }
  const seen = new Set<string>();
  for (const value of values) {
    if (typeof value !== "string" || value.length === 0) {
      throw new TypeError(`Enum values must be non-empty strings: ${context}`);
    }
    if (seen.has(value)) {
      throw new TypeError(`Duplicate enum value: ${context} has "${value}" twice`);
    }
    seen.add(value);
  }
  return [...values];
}

/**
 * Validates the catalog invariants owned by a table's column list. Storage adapters call this
 * for ordinary catalog writes and before restoring snapshots or checkpoints, so malformed
 * metadata cannot enter through a less common persistence path.
 */
export function validateTableColumns(columns: readonly TableColumnRecord[]): void {
  const runtimeColumns: unknown = columns;
  if (!Array.isArray(runtimeColumns) || columns.length === 0) {
    throw new TypeError("A table needs at least one column");
  }
  if (columns.length > MAX_TABLE_COLUMNS) {
    throw new RangeError(`A table cannot exceed ${String(MAX_TABLE_COLUMNS)} columns`);
  }
  const runtimeEntries: readonly unknown[] = runtimeColumns;
  for (const entry of runtimeEntries) {
    if (!hasValidColumnPrimitives(entry)) {
      throw new TypeError("Table columns have invalid primitive metadata");
    }
    const column = entry;
    validateStorageId(column.id, "Column ID");
    validateCatalogName(column.name, "Column name");
    const integer: unknown = column.integer;
    if (integer !== undefined && (integer !== true || column.type !== "number")) {
      throw new TypeError(`Integer domain requires a number column: ${column.name}`);
    }
    if (column.sqlDomain !== undefined && column.type !== "string") {
      throw new TypeError(`SQL domain columns must use string storage: ${column.name}`);
    }
    if (column.sqlDomain !== undefined) validateSqlDomain(column.sqlDomain, column.name);
    if (column.integer === true && column.sqlDomain !== undefined) {
      throw new TypeError(
        `A column cannot have both integer and SQL-domain metadata: ${column.name}`,
      );
    }
    if (column.enumValues !== undefined && column.sqlDomain !== undefined) {
      throw new TypeError(
        `A column cannot have both enum restrictions and a SQL domain: ${column.name}`,
      );
    }
    if (column.enumValues !== undefined) {
      if (column.type !== "string") {
        throw new TypeError(`Enum restrictions require a string column: ${column.name}`);
      }
      validateEnumValues(column.enumValues, column.name);
    }
    if (column.defaultValue !== undefined) {
      validateColumnDefault(
        {
          name: column.name,
          type: column.type,
          nullable: column.nullable,
          isUniqueKey: true,
          ...(column.integer === undefined ? {} : { integer: column.integer }),
          ...(column.sqlDomain === undefined ? {} : { sqlDomain: column.sqlDomain }),
          ...(column.enumValues === undefined ? {} : { enumValues: column.enumValues }),
        },
        column.defaultValue,
      );
    }
    if (column.backfill !== undefined) {
      const value = column.backfill;
      const validType =
        column.type === "datetime"
          ? value instanceof Date && Number.isFinite(dateMilliseconds(value))
          : typeof value === column.type;
      if (
        !validType ||
        (typeof value === "number" && !Number.isFinite(value)) ||
        (column.integer === true && !Number.isSafeInteger(value)) ||
        (column.enumValues !== undefined &&
          typeof value === "string" &&
          !column.enumValues.includes(value))
      ) {
        throw new TypeError(`Invalid backfill value: ${column.name}`);
      }
    }
  }
  const ids = new Set(columns.map(({ id }) => id));
  const names = new Set(columns.map(({ name }) => name));
  if (ids.size !== columns.length || names.size !== columns.length) {
    throw new TypeError("Table columns must have unique IDs and names");
  }
}

function hasValidColumnPrimitives(value: unknown): value is TableColumnRecord {
  if (value === null || typeof value !== "object") return false;
  const column = value as Record<string, unknown>;
  return (
    typeof column.id === "string" &&
    column.id.length > 0 &&
    typeof column.name === "string" &&
    column.name.length > 0 &&
    simpleDataTypes.some((type) => type === column.type) &&
    typeof column.nullable === "boolean" &&
    (column.hidden === undefined || column.hidden === true)
  );
}

/**
 * The single authority on which default declarations are legal, shared by the schema DSL's
 * `table()` and the engine's `createTable` so the two entry points (and the wire path between
 * them) can never drift. Storage owns structural and literal validation; the engine additionally
 * parses and type-checks SQL expressions before catalog mutation.
 */
export function validateColumnDefault(
  column: {
    name: string;
    type: SimpleDataType;
    integer?: true;
    sqlDomain?: SqlDomain;
    nullable: boolean;
    isUniqueKey: boolean;
    enumValues?: readonly string[];
  },
  defaultValue: ColumnDefault,
): void {
  switch (defaultValue.kind) {
    case "autoincrement":
      if (column.type !== "number") {
        throw new TypeError(`Auto-increment requires a number column: ${column.name}`);
      }
      if (!column.isUniqueKey) {
        throw new TypeError(`Auto-increment requires the unique key column: ${column.name}`);
      }
      return;
    case "expression":
      if (
        typeof defaultValue.sql !== "string" ||
        defaultValue.sql.length === 0 ||
        defaultValue.sql.trim() !== defaultValue.sql
      ) {
        throw new TypeError(`Default SQL must be a trimmed non-empty expression: ${column.name}`);
      }
      return;
    case "literal": {
      const value = defaultValue.value;
      const numericDomain = column.sqlDomain?.kind === "numeric";
      const correctType =
        column.type === "datetime"
          ? value instanceof Date && Number.isFinite(dateMilliseconds(value))
          : numericDomain
            ? typeof value === "number" || typeof value === "string"
            : typeof value === column.type;
      if (!correctType) {
        throw new TypeError(`Default literal must be a ${column.type}: ${column.name}`);
      }
      if (typeof value === "number" && !Number.isFinite(value)) {
        throw new TypeError(`Default literal must be finite: ${column.name}`);
      }
      if (column.integer === true && !Number.isSafeInteger(value)) {
        throw new TypeError(`Default literal must be a safe integer: ${column.name}`);
      }
      if (
        column.enumValues !== undefined &&
        typeof value === "string" &&
        !column.enumValues.includes(value)
      ) {
        throw new TypeError(`Default must be one of the enum values: ${column.name}`);
      }
      if (
        column.sqlDomain?.kind === "enum" &&
        typeof value === "string" &&
        !column.sqlDomain.values.includes(value)
      ) {
        throw new TypeError(`Default must be one of the enum values: ${column.name}`);
      }
      return;
    }
    // Storage restoration and untyped wire callers can still supply malformed catalog data.
    default:
      throw new TypeError(
        `Unknown default kind: ${String((defaultValue as { kind?: unknown }).kind)}`,
      );
  }
}

export type FtsColumnIndexState = "building" | "ready" | "invalid";

/**
 * One column's persisted full-text index declaration. The index is a pruning accelerator, never
 * ground truth: readers use it only in state "ready" with a matching tokenizer version, and the
 * scan re-verifies every candidate, so a stale or missing index costs speed, not correctness.
 */
export interface FtsColumnIndexRecord {
  storage: "fts-chunks-v1";
  tokenizerVersion: number;
  state: FtsColumnIndexState;
  /** Manifest version the base build covers; commit deltas above it merge at read time. */
  buildFromVersion: number;
}

export type SecondaryIndexState = "building" | "ready" | "invalid";

export type SecondaryIndexDirection = "asc" | "desc";

/**
 * One durable secondary index. The physical postings live in the same bounded, immutable
 * base-plus-delta substrate as full-text postings, under `storageColumnId`; keeping the storage
 * identity separate from the catalog column IDs lets one postings generation represent a
 * composite key.
 *
 * Postings are a pruning accelerator, never truth. A keyed table stores a deterministic hash of
 * its immutable unique key (collisions only add false positives); an append-only keyless table
 * stores its hidden row ID. Every scan re-evaluates the SQL predicate against the row.
 */
export interface SecondaryIndexRecord {
  name: string;
  /** First indexed column, repeated for constant-time scalar catalog access. */
  columnId: string;
  /** Ordered key columns. */
  columnIds: string[];
  /** Declared key direction per column. */
  directions: SecondaryIndexDirection[];
  /** SQL UNIQUE: enforced through an atomic, separately namespaced membership set. */
  unique?: true;
  /** The membership set has been seeded and must be enforced, independent of postings state. */
  uniqueEnforced?: true;
  /** Prefix-free composite encoding used by every v1 secondary index. */
  termEncoding: "tuple-v1";
  storage: "postings-v1";
  storageColumnId: string;
  locator: "row-id" | "key-hash-v1";
  state: SecondaryIndexState;
  /** Identifies the builder allowed to publish a `building` record; omitted otherwise. */
  buildId?: string;
  /** Manifest version the base build covers; commit deltas above it merge at read time. */
  buildFromVersion: number;
}

/**
 * Whether a secondary-index replacement changes the contract a staged writer must honor.
 * Physical posting-build state is deliberately excluded: postings are only a reverified pruning
 * accelerator. Index identity, key shape, and UNIQUE enforcement are structural.
 */
export function secondaryIndexWriteContractChanged(
  previous: Readonly<Record<string, SecondaryIndexRecord>> | null | undefined,
  next: Readonly<Record<string, SecondaryIndexRecord>> | null | undefined,
): boolean {
  const previousEntries = Object.entries(previous ?? {}).sort(([left], [right]) =>
    left.localeCompare(right),
  );
  const nextEntries = Object.entries(next ?? {}).sort(([left], [right]) =>
    left.localeCompare(right),
  );
  if (previousEntries.length !== nextEntries.length) return true;
  return previousEntries.some(([id, left], position) => {
    const rightEntry = nextEntries[position];
    if (rightEntry?.[0] !== id) return true;
    const right = rightEntry[1];
    return (
      left.name !== right.name ||
      left.columnId !== right.columnId ||
      left.columnIds.length !== right.columnIds.length ||
      left.columnIds.some((columnId, index) => columnId !== right.columnIds[index]) ||
      left.directions.length !== right.directions.length ||
      left.directions.some((direction, index) => direction !== right.directions[index]) ||
      left.unique !== right.unique ||
      left.uniqueEnforced !== right.uniqueEnforced ||
      left.storageColumnId !== right.storageColumnId ||
      left.locator !== right.locator
    );
  });
}

export interface TableForeignKeyRecord {
  name: string;
  columns: string[];
  parentTable: string;
  parentColumns: string[];
  onDelete: "restrict" | "cascade" | "set null";
  /** False records relationship metadata without write/delete enforcement. Absent means true. */
  enforced?: boolean;
}

export interface TableRecord {
  id: string;
  name: string;
  columns: TableColumnRecord[];
  uniqueKeyColumnId?: string;
  /** Declared PRIMARY KEY columns. More than one uses the hidden scalar row-addressing key. */
  primaryKeyColumnIds?: string[];
  uniqueKeyLookupReady?: boolean;
  /** Full-text index state per column ID. Writers that see this emit commit deltas. */
  ftsColumns?: Record<string, FtsColumnIndexRecord>;
  /** Durable secondary indexes keyed by stable index ID. */
  secondaryIndexes?: Record<string, SecondaryIndexRecord>;
  /** BEFORE/AFTER triggers on this table, fired by the committing writer in its transaction. */
  triggers?: TriggerRecord[];
  /** FOREIGN KEY constraints. Column tuples are always explicit, including scalar keys. */
  foreignKeys?: TableForeignKeyRecord[];
  /**
   * Row-level CHECK constraints (E141-06), each the text of a boolean expression over this
   * table's own columns. Text rather than a compiled form because the record crosses the worker
   * boundary and IndexedDB; the writer compiles it and evaluates it against every row it writes.
   */
  checks?: Array<{ name: string; sql: string }>;
  /**
   * True when `migrate()` created this table from a schema declaration, which makes the schema
   * authoritative over it: dropping the declaration may drop the table. `CREATE TABLE` records
   * false explicitly.
   */
  managed: boolean;
  /**
   * A view rather than a table: the query text it stands for, and no segments of its own. The
   * `columns` are the query's inferred output schema, so a view answers the same catalog
   * questions a table does — what a reader can select, and of what type.
   */
  view?: {
    sql: string;
    /**
     * True when `migrate()` created this view from a schema declaration. `CREATE VIEW` records
     * false explicitly.
     */
    managed: boolean;
  };
  /** Internal catalog object backing CREATE TYPE ... AS ENUM; never exposed as a table. */
  enumType?: { name: string; values: string[] };
  /** Internal catalog object backing a durable PostgreSQL sequence. */
  sequence?: { name: string; start: number; columnId: string };
  createdAt: string;
  /** Compare-and-swap revision for catalog evolution. */
  revision: number;
}

/**
 * Validates one relationship against its child and parent records. This is shared by every
 * storage adapter and snapshot restore path so informational relationships cannot bypass the
 * same structural and domain checks as enforced foreign keys.
 */
export function validateTableForeignKey(
  childTable: TableRecord,
  key: TableForeignKeyRecord,
  parentTable: TableRecord,
): void {
  if (
    typeof key.name !== "string" ||
    key.name.length === 0 ||
    typeof key.parentTable !== "string" ||
    key.parentTable.length === 0 ||
    !Array.isArray(key.columns) ||
    !Array.isArray(key.parentColumns) ||
    key.columns.length === 0 ||
    key.columns.some((column) => typeof column !== "string" || column.length === 0) ||
    key.parentColumns.some((column) => typeof column !== "string" || column.length === 0) ||
    new Set(key.columns).size !== key.columns.length ||
    new Set(key.parentColumns).size !== key.parentColumns.length ||
    !["restrict", "cascade", "set null"].includes(key.onDelete) ||
    (key.enforced !== undefined && typeof key.enforced !== "boolean")
  ) {
    throw new TypeError("FOREIGN KEY metadata is invalid");
  }
  if (key.parentTable !== parentTable.name) {
    throw new TypeError(`FOREIGN KEY ${key.name} resolved to the wrong parent table`);
  }
  if (key.enforced === false && key.onDelete !== "restrict") {
    throw new TypeError(`Informational FOREIGN KEY ${key.name} cannot declare ON DELETE actions`);
  }
  const children = key.columns.map((name) =>
    childTable.columns.find((column) => column.name === name && column.hidden !== true),
  );
  if (children.some((column) => column === undefined)) {
    throw new TypeError(`FOREIGN KEY ${key.name} names a column this table does not have`);
  }
  const addressIds = parentTable.primaryKeyColumnIds?.length
    ? parentTable.primaryKeyColumnIds
    : parentTable.uniqueKeyColumnId === undefined
      ? []
      : [parentTable.uniqueKeyColumnId];
  const parents = addressIds.map((id) =>
    parentTable.columns.find((column) => column.id === id && column.hidden !== true),
  );
  const addressNames = parents.map((column) => column?.name ?? "");
  if (
    addressNames.length !== key.parentColumns.length ||
    addressNames.some((name, index) => name !== key.parentColumns[index])
  ) {
    throw new TypeError(`FOREIGN KEY ${key.name} must reference the parent primary or unique key`);
  }
  if (children.length !== parents.length) {
    throw new TypeError(
      `FOREIGN KEY ${key.name} has ${String(children.length)} child columns for ${String(parents.length)} parent columns`,
    );
  }
  children.forEach((child, index) => {
    const parent = parents[index];
    if (child === undefined || parent === undefined) {
      throw new TypeError(`FOREIGN KEY ${key.name} is missing a key column`);
    }
    if (child.type !== parent.type) {
      throw new TypeError(`FOREIGN KEY ${key.name} compares ${child.type} with ${parent.type}`);
    }
    if ((child.integer === true) !== (parent.integer === true)) {
      throw new TypeError(
        `FOREIGN KEY ${key.name} compares an integer domain with an approximate number domain`,
      );
    }
    if (JSON.stringify(child.sqlDomain ?? null) !== JSON.stringify(parent.sqlDomain ?? null)) {
      throw new TypeError(`FOREIGN KEY ${key.name} compares different SQL value domains`);
    }
    if (key.onDelete === "set null" && !child.nullable) {
      throw new TypeError(`FOREIGN KEY ${key.name} cannot SET NULL a NOT NULL column`);
    }
  });
}

const catalogRecordTextEncoder = new TextEncoder();

function durableRecordRetainedBytes(record: unknown, label: string): number {
  // Keep this byte accounting identical to the canonical record-wire JSON codec without
  // importing the toolkit codec back into this leaf contract module. Segment records always
  // contain bigint row envelopes, so plain JSON.stringify is not sufficient.
  let json: string | undefined;
  try {
    json = JSON.stringify(record);
  } catch {
    json = JSON.stringify(record, (_key, entry: unknown) => {
      if (typeof entry !== "bigint") return entry;
      if (entry < 0n || entry > MAX_ROW_ID_EXCLUSIVE_END) {
        throw new RangeError("Record bigint exceeds the unsigned 64-bit persisted range");
      }
      return { $n: entry.toString() };
    });
  }
  if (typeof json !== "string") throw new TypeError(`${label} is not JSON-serializable`);
  return catalogRecordTextEncoder.encode(json).byteLength;
}

/** Exact UTF-8 bytes used when the canonical record-wire JSON codec persists a table record. */
export function catalogRecordRetainedBytes(record: TableRecord): number {
  return durableRecordRetainedBytes(record, "Table record");
}

export function manifestRecordRetainedBytes(record: Manifest): number {
  return durableRecordRetainedBytes(record, "Manifest record");
}

/**
 * Admission charge for one manifest summary, including the exact canonical tombstone bytes that
 * later reclamation may add. Reserving them up front prevents quota deadlock at the byte ceiling.
 */
export function manifestRecordRetainedReservationBytes(record: Manifest): number {
  return manifestRecordRetainedBytes(
    record.prunedAt === undefined ? { ...record, prunedAt: "1970-01-01T00:00:00.000Z" } : record,
  );
}

export function segmentRecordRetainedBytes(record: SegmentRecord): number {
  return durableRecordRetainedBytes(record, "Segment record");
}

/** Hard catalog cardinality/text bounds shared by engine, adapters, checkpoints, and snapshots. */
export function validateTableRecordBounds(record: TableRecord): void {
  validateStorageId(record.id, "Table ID");
  validateCatalogName(record.name, "Table name");
  if (typeof record.managed !== "boolean") throw new TypeError("Table managed flag is required");
  if (!Number.isSafeInteger(record.revision) || record.revision < 0) {
    throw new RangeError("Table revision must be a non-negative safe integer");
  }
  if (record.view !== undefined && typeof record.view.managed !== "boolean") {
    throw new TypeError("View managed flag is required");
  }
  if ((record.primaryKeyColumnIds?.length ?? 0) > MAX_TABLE_COLUMNS) {
    throw new RangeError("A primary key names too many columns");
  }
  if (Object.keys(record.ftsColumns ?? {}).length > MAX_TABLE_COLUMNS) {
    throw new RangeError("A table has too many full-text columns");
  }
  if (Object.keys(record.secondaryIndexes ?? {}).length > MAX_SECONDARY_INDEXES) {
    throw new RangeError(
      `A table cannot exceed ${String(MAX_SECONDARY_INDEXES)} secondary indexes`,
    );
  }
  if ((record.triggers?.length ?? 0) > MAX_TABLE_TRIGGERS) {
    throw new RangeError(`A table cannot exceed ${String(MAX_TABLE_TRIGGERS)} triggers`);
  }
  const triggerIds = new Set<string>();
  const triggerNames = new Set<string>();
  const columnNames = new Set(record.columns.map((column) => column.name));
  for (const trigger of record.triggers ?? []) {
    validateStorageId(trigger.id, "Trigger ID");
    validateCatalogName(trigger.name, "Trigger name");
    if (!VALID_TRIGGER_EVENTS.includes(trigger.event)) {
      throw new TypeError(`Trigger event is invalid: ${trigger.name}`);
    }
    if (!VALID_TRIGGER_TIMINGS.includes(trigger.timing)) {
      throw new TypeError(`Trigger timing is invalid: ${trigger.name}`);
    }
    if (triggerIds.has(trigger.id)) {
      throw new TypeError(`Trigger ID already exists: ${trigger.id}`);
    }
    if (triggerNames.has(trigger.name)) {
      throw new TypeError(`Trigger already exists: ${trigger.name}`);
    }
    triggerIds.add(trigger.id);
    triggerNames.add(trigger.name);
    if (typeof trigger.createdAt !== "string") {
      throw new TypeError(`Trigger creation timestamp is invalid: ${trigger.name}`);
    }
    const triggerCreatedAt = Date.parse(trigger.createdAt);
    if (
      !Number.isFinite(triggerCreatedAt) ||
      dateIsoString(new Date(triggerCreatedAt)) !== trigger.createdAt
    ) {
      throw new TypeError(`Trigger creation timestamp is invalid: ${trigger.name}`);
    }
    if (!Array.isArray(trigger.statements) || trigger.statements.length === 0) {
      throw new TypeError(`A trigger needs at least one statement: ${trigger.name}`);
    }
    if (trigger.statements.length > MAX_TRIGGER_STATEMENTS) {
      throw new RangeError(
        `A trigger cannot exceed ${String(MAX_TRIGGER_STATEMENTS)} statements: ${trigger.name}`,
      );
    }
    for (const statement of trigger.statements) {
      if (typeof statement.sql !== "string" || statement.sql.length === 0) {
        throw new TypeError(`Trigger statement SQL is invalid: ${trigger.name}`);
      }
      if (!Array.isArray(statement.bindings)) {
        throw new TypeError(`Trigger statement bindings are invalid: ${trigger.name}`);
      }
      for (const binding of statement.bindings) {
        if (!VALID_TRIGGER_BINDING_SOURCES.includes(binding.source)) {
          throw new TypeError(`Trigger binding source is invalid: ${trigger.name}`);
        }
        validateCatalogName(binding.column, "Trigger binding column");
        if (!columnNames.has(binding.column)) {
          throw new TypeError(`Trigger binding names an unknown column: ${binding.column}`);
        }
        if (
          (trigger.event === "insert" && binding.source === "old") ||
          (trigger.event === "delete" && binding.source === "new")
        ) {
          throw new TypeError(`Trigger binding source is unavailable for ${trigger.event}`);
        }
      }
    }
  }
  const namedConstraints = [
    ...(record.foreignKeys ?? []).map((constraint) => constraint.name),
    ...(record.checks ?? []).map((constraint) => constraint.name),
    ...Object.values(record.secondaryIndexes ?? {})
      .filter((index) => index.unique === true)
      .map((index) => index.name),
  ];
  if (namedConstraints.length > MAX_TABLE_CONSTRAINTS) {
    throw new RangeError(
      `A table cannot exceed ${String(MAX_TABLE_CONSTRAINTS)} named constraints`,
    );
  }
  const constraintNames = new Set<string>();
  for (const key of record.foreignKeys ?? []) {
    if (key.enforced !== undefined && typeof key.enforced !== "boolean") {
      throw new TypeError(`FOREIGN KEY enforcement is invalid: ${key.name}`);
    }
  }
  for (const constraintName of namedConstraints) {
    validateCatalogName(constraintName, "Constraint name");
    if (constraintNames.has(constraintName)) {
      throw new TypeError(`Constraint already exists: ${constraintName}`);
    }
    constraintNames.add(constraintName);
  }
  if ((record.enumType?.values.length ?? 0) > MAX_ENUM_VALUES) {
    throw new RangeError("A catalog enum has too many values");
  }
  const stack: unknown[] = [record];
  const seen = new WeakSet();
  let characters = 0;
  let entries = 0;
  while (stack.length > 0) {
    const value = stack.pop();
    if (typeof value === "string") {
      assertWellFormedString(value, "Table record string");
      characters += value.length;
    } else if (Array.isArray(value)) {
      if (seen.has(value)) throw new TypeError("A table record cannot contain cycles or aliases");
      seen.add(value);
      entries += value.length;
      for (const item of value) stack.push(item);
    } else if (value instanceof Date) {
      if (!Number.isFinite(dateMilliseconds(value))) {
        throw new TypeError("A table record date is invalid");
      }
    } else if (value !== null && typeof value === "object") {
      if (seen.has(value)) throw new TypeError("A table record cannot contain cycles or aliases");
      seen.add(value);
      const fields = Object.entries(value);
      entries += fields.length;
      for (const [key, item] of fields) {
        assertWellFormedString(key, "Table record field name");
        characters += key.length;
        stack.push(item);
      }
    }
    if (!Number.isSafeInteger(entries) || entries > MAX_TABLE_RECORD_ENTRIES) {
      throw new RangeError(
        `A table record cannot exceed ${String(MAX_TABLE_RECORD_ENTRIES)} aggregate entries`,
      );
    }
    if (!Number.isSafeInteger(characters) || characters > MAX_TABLE_RECORD_CHARACTERS) {
      throw new RangeError(
        `A table record cannot exceed ${String(MAX_TABLE_RECORD_CHARACTERS)} modeled characters`,
      );
    }
  }
}

/** Validates the durable identities and ownership rules of one table's secondary indexes. */
export function validateSecondaryIndexes(record: TableRecord): void {
  validateTableRecordBounds(record);
  const columnIds = new Set(record.columns.map((column) => column.id));
  const names = new Set<string>();
  const storageIds = new Set(Object.keys(record.ftsColumns ?? {}));
  for (const [indexId, index] of Object.entries(record.secondaryIndexes ?? {})) {
    if (indexId.length === 0 || index.name.length === 0 || index.storageColumnId.length === 0) {
      throw new TypeError("Secondary-index IDs and names must be non-empty");
    }
    validateStorageId(indexId, "Secondary-index ID");
    validateCatalogName(index.name, "Secondary-index name");
    validateStorageId(index.storageColumnId, "Secondary-index storage ID");
    const indexedColumnIds = index.columnIds;
    if (
      indexedColumnIds.length === 0 ||
      indexedColumnIds[0] !== index.columnId ||
      new Set(indexedColumnIds).size !== indexedColumnIds.length ||
      indexedColumnIds.some((columnId) => !columnIds.has(columnId))
    ) {
      throw new TypeError(`Secondary index ${index.name} references an unknown column`);
    }
    const directions: readonly unknown[] = index.directions;
    const termEncoding: unknown = index.termEncoding;
    if (
      directions.length !== indexedColumnIds.length ||
      directions.some((direction) => direction !== "asc" && direction !== "desc") ||
      termEncoding !== "tuple-v1"
    ) {
      throw new TypeError(`Secondary index ${index.name} has invalid key metadata`);
    }
    if (index.uniqueEnforced === true && index.unique !== true) {
      throw new TypeError(`Secondary index ${index.name} enforces uniqueness without UNIQUE`);
    }
    if (names.has(index.name)) throw new TypeError(`Index already exists: ${index.name}`);
    names.add(index.name);
    if (storageIds.has(index.storageColumnId)) {
      throw new TypeError(`Secondary-index storage ID is already used: ${index.storageColumnId}`);
    }
    storageIds.add(index.storageColumnId);
    const expectedLocator = record.uniqueKeyColumnId === undefined ? "row-id" : "key-hash-v1";
    const storage: unknown = index.storage;
    if (storage !== "postings-v1" || index.locator !== expectedLocator) {
      throw new TypeError(`Secondary index ${index.name} has incompatible storage metadata`);
    }
    const state: unknown = index.state;
    if (
      (state !== "building" && state !== "ready" && state !== "invalid") ||
      !Number.isSafeInteger(index.buildFromVersion) ||
      index.buildFromVersion < -1 ||
      (index.state === "building") !== (index.buildId !== undefined)
    ) {
      throw new TypeError(`Secondary index ${index.name} has invalid build metadata`);
    }
  }
}

/** Ordered catalog column IDs for one canonical secondary-index record. */
export function secondaryIndexColumnIds(index: SecondaryIndexRecord): readonly string[] {
  return index.columnIds;
}

/** Declared directions for one canonical secondary-index record. */
export function secondaryIndexDirections(
  index: SecondaryIndexRecord,
): readonly SecondaryIndexDirection[] {
  return index.directions;
}

/** Unique-membership namespace owned by one physical secondary index. */
export function secondaryUniqueKeyNamespace(tableId: string, indexId: string): string {
  return `${tableId}\u0000secondary-index\u0000${indexId}`;
}

/**
 * One row trigger: catalog-persisted on its table record so the catalog epoch makes it
 * visible to every tab immediately, and executed by the committing writer inside the same
 * transaction as the triggering write — the write and its derivations publish atomically.
 */
export interface TriggerRecord {
  /** Immutable durable identity. Names may be reused only after this trigger is gone. */
  id: string;
  name: string;
  event: "insert" | "update" | "delete";
  /**
   * BEFORE and AFTER differ only in body staging order here: both fire in the committing
   * writer inside the triggering commit, so the pair exists for SQL portability, with
   * identical atomicity.
   */
  timing: "before" | "after";
  /** Body statements in order; each fires once per affected row. */
  statements: TriggerStatementRecord[];
  createdAt: string;
}

export interface TriggerStatementRecord {
  /** The body statement with every NEW.col / OLD.col reference rewritten to a placeholder. */
  sql: string;
  /** Placeholder bindings in order: which pseudo-row and column fills each parameter. */
  bindings: Array<{ source: "new" | "old"; column: string }>;
}

export class TableRecordConflictError extends Error {
  override readonly name = "TableRecordConflictError";

  constructor(
    readonly tableId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number | null,
  ) {
    super(
      `Table ${tableId} changed: expected revision ${String(expectedRevision)}, found ${String(actualRevision)}`,
    );
  }
}

/** A catalog drop lost a race with durable work that still owns records for the table. */
export class TableInUseError extends Error {
  override readonly name = "TableInUseError";

  constructor(
    readonly tableId: string,
    readonly ownerKind:
      | "transaction"
      | "compaction job"
      | "foreign key"
      | "pending foreign key"
      | "accelerator build",
    readonly ownerId: string,
  ) {
    super(`Cannot remove table ${tableId} while ${ownerKind} ${ownerId} is active`);
  }
}

/** Refuses a commit that would cross the persisted level-zero fragmentation safety ceiling. */
export class CompactionBacklogError extends Error {
  override readonly name = "CompactionBacklogError";

  constructor(
    readonly tableName: string,
    readonly levelZeroSegments: number,
    readonly limit: number,
  ) {
    super(
      `Table ${tableName} has ${String(levelZeroSegments)} level-zero segments; compactTable() must reduce it below the ${String(limit)}-segment safety limit before more writes`,
    );
  }
}

export type SegmentKind = "insert" | "upsert" | "update" | "delete" | "base";
/** Absolute visible level-zero ceiling; every commit that adds L0 must enforce this or lower. */
export const MAX_LEVEL_ZERO_SEGMENTS = 4_096;

/** Maps a contiguous segment-row run to its immutable hidden row IDs. */
export interface RowIdSpan {
  readonly rowStart: number;
  readonly rowCount: number;
  readonly rowIdStart: bigint;
}

export interface SegmentRecord {
  id: string;
  tableId: string;
  transactionId: string;
  rowCount: number;
  rowIdStart: bigint;
  rowIdEndExclusive: bigint;
  columnBlockIds: Record<string, string[]>;
  kind: SegmentKind;
  keyColumnId?: string;
  level: number;
  logicalOrder: number;
  /**
   * Staging position inside the owning transaction. Orders segments of one commit relative
   * to each other (an in-scope update must fold after the in-scope insert it patches).
   */
  commitOrdinal: number;
  /** Empty for a contiguous row-ID envelope; merged bases record every retained range. */
  rowIdSpans: readonly RowIdSpan[];
  /** Monotone policy ordinal for an immutable append-row-range level-two partition. */
  readonly partitionOrdinal?: number;
  createdAt: string;
}

export interface RowIdRange {
  start: bigint;
  endExclusive: bigint;
}

/** Largest persisted row ID/posting value; the wire format is canonical unsigned 64-bit. */
export const MAX_ROW_ID = (1n << 64n) - 1n;
/** One past `MAX_ROW_ID`, allowed only for exclusive range/counter ends. */
export const MAX_ROW_ID_EXCLUSIVE_END = 1n << 64n;
/** Auto-increment values are exposed as exact JavaScript numbers and therefore stop here. */
export const MAX_AUTO_INCREMENT_VALUE = BigInt(Number.MAX_SAFE_INTEGER);
export const MAX_AUTO_INCREMENT_EXCLUSIVE_END = MAX_AUTO_INCREMENT_VALUE + 1n;

export type LeaseKind = "reader" | "backup";
/** A live owner renews; an abandoned durable pin expires within one hour. */
export const MAX_LEASE_TTL_MS = 60 * 60 * 1_000;
/** Durable resource ceilings; adapters sweep expired owners before atomically refusing creation. */
export const MAX_ACTIVE_LEASES = 4_096;
export const MAX_ACTIVE_TEMP_OWNERS = 1_024;
export const MAX_TEMP_RUNS_PER_OWNER = 1_024;
export const MAX_TEMP_PAGES_PER_OWNER = 16_384;
export const MAX_TEMP_RUNS_TOTAL = 65_536;
export const MAX_TEMP_PAGES_TOTAL = 262_144;
export const MAX_TEMP_BYTES_PER_OWNER = 512 * 1024 * 1024;
export const MAX_TEMP_BYTES_TOTAL = 1024 * 1024 * 1024;
export const MAX_ACTIVE_COMPACTION_JOBS = 1_024;
export const MAX_ACTIVE_GARBAGE_COLLECTION_JOBS = 1;
export const MAX_ACTIVE_UNIQUE_KEY_BUILDS = 1_024;
export const MAX_ACTIVE_FTS_BASE_BUILDS = 128;
export const MAX_ACTIVE_SECONDARY_INDEX_BUILDS = 128;
export const MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL = 1024 * 1024 * 1024;
export const MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL = 16_777_216;
export const MAX_ACTIVE_TRANSACTIONS = 4_096;
export const MAX_GLOBAL_STAGED_ARTIFACT_BYTES = 512 * 1024 * 1024;
export const MAX_GLOBAL_STAGED_BLOCKS = 65_536;
export const MAX_GLOBAL_STAGED_SEGMENTS = 65_536;
/** Catalog enumeration remains bounded even for cold public inspection APIs. */
export const MAX_CATALOG_RECORDS = 4_096;
/** Total canonical UTF-8 record-wire bytes retained by the durable catalog. */
export const MAX_CATALOG_RETAINED_BYTES = 64 * 1024 * 1024;
export const MAX_MANIFEST_RECORDS = 65_536;
export const MAX_MANIFEST_RETAINED_BYTES = 64 * 1024 * 1024;
export const MAX_SEGMENT_RECORDS = 1_048_576;
export const MAX_SEGMENT_RETAINED_BYTES = 512 * 1024 * 1024;
/** Durable diagnostic/provenance tails are pruned before admitting more terminal records. */
export const MAX_TERMINAL_TRANSACTION_RECORDS = 65_536;
export const MAX_TERMINAL_COMPACTION_JOB_RECORDS = 4_096;
export const MAX_COMPLETED_GARBAGE_COLLECTION_JOB_RECORDS = 1_024;
/** An old renewable pin may not force unbounded history retention while writes continue. */
export const MAX_PINNED_MANIFEST_VERSION_LAG = 4_096;
export const MAX_PINNED_RETIRED_BLOCKS = 65_536;
export const MAX_PINNED_RETIRED_BYTES = 512 * 1024 * 1024;
/** Total obsolete physical history is refused before it can consume an origin indefinitely. */
export const MAX_RETIRED_HISTORY_BYTES = 1024 * 1024 * 1024;

/** A durable resource family reached its fixed corruption/growth safety ceiling. */
export class StorageResourceLimitError extends Error {
  override readonly name = "StorageResourceLimitError";

  constructor(
    readonly resource:
      | "lease"
      | "temp owner"
      | "temp run"
      | "temp page"
      | "temporary run total"
      | "temporary page total"
      | "temp owner byte"
      | "temporary byte"
      | "compaction job"
      | "garbage collection job"
      | "unique-key build"
      | "full-text build"
      | "secondary-index build"
      | "accelerator build byte"
      | "accelerator build entry"
      | "transaction"
      | "staged artifact byte"
      | "staged block"
      | "staged segment"
      | "catalog record"
      | "catalog byte"
      | "manifest record"
      | "manifest byte"
      | "segment record"
      | "segment byte"
      | "terminal transaction"
      | "terminal compaction job"
      | "completed garbage collection job"
      | "pinned manifest version lag"
      | "pinned retired block"
      | "pinned retired byte"
      | "snapshot accelerator byte"
      | "snapshot accelerator entry"
      | "retired history byte",
    readonly count: number,
    readonly limit: number,
  ) {
    super(
      `Storage resource ${resource} would reach ${String(count)}; the fixed safety limit is ${String(limit)}`,
    );
  }
}

export interface LeaseRecord {
  id: string;
  kind: LeaseKind;
  manifestVersion: number | null;
  ownerId: string;
  /** Issuance cutoff used to prove the initial expiry is bounded. */
  createdAt: string;
  expiresAt: string;
  revision: number;
}

export interface RenewLeaseInput {
  id: string;
  expectedRevision: number;
  /** A persisted expiry at or before this boundary is irrevocably expired. */
  expiresAtCutoff: string;
  expiresAt: string;
}

export interface MoveLeaseInput extends RenewLeaseInput {
  manifestVersion: number | null;
}

export const compactionJobStates = [
  "planned",
  "running",
  "ready",
  "published",
  "cancelled",
  "aborted",
] as const;
export type CompactionJobState = (typeof compactionJobStates)[number];

export interface CompactionJobCursor {
  sourceSegmentIndex: number;
  sourceBlockIndex: number;
}

export const compactionRewritePlanKinds = ["copy-v1", "rechunk-v1", "merge-v1"] as const;
export type CompactionRewritePlanKind = (typeof compactionRewritePlanKinds)[number];

export interface CopyCompactionRewritePlan {
  readonly kind: "copy-v1";
}

export interface RechunkCompactionSourceBlock {
  readonly blockId: string;
  readonly rowStart: number;
  readonly rowCount: number;
  /** Full persisted block byteLength, including the envelope and stored payload. */
  readonly storedBytes: number;
  /** Uncompressed encoded payload length from the immutable block header. */
  readonly encodedBytes: number;
  readonly checksum: number;
}

export interface RechunkCompactionSourceColumn {
  readonly columnId: string;
  readonly type: SimpleDataType;
  readonly sourceBlocks: readonly RechunkCompactionSourceBlock[];
}

export interface RechunkCompactionOutputWindow {
  readonly rowStart: number;
  readonly rowCount: number;
}

export const compactionOutputCompressions = ["raw", "gzip"] as const;
export type CompactionOutputCompression = (typeof compactionOutputCompressions)[number];

export interface RechunkCompactionRewritePlan {
  readonly kind: "rechunk-v1";
  readonly targetBlockBytes: number;
  readonly outputCompression: CompactionOutputCompression;
  readonly totalRows: number;
  readonly rowIdStart: bigint;
  readonly rowIdEndExclusive: bigint;
  readonly logicalOrder: number;
  readonly columns: readonly RechunkCompactionSourceColumn[];
  /** Shared row windows, emitted in output-window-major then column order. */
  readonly outputs: readonly RechunkCompactionOutputWindow[];
  /** Optional level-one publication layout; partitions tile the output without splitting windows. */
  readonly partitions?: readonly MergeOutputPartition[];
}

export interface MergeCompactionSourceBlock {
  readonly blockId: string;
  /** Row offset within the source segment column. */
  readonly rowStart: number;
  readonly rowCount: number;
  readonly storedBytes: number;
  readonly encodedBytes: number;
  readonly checksum: number;
}

export interface MergeCompactionSourceColumn {
  readonly columnId: string;
  readonly type: SimpleDataType;
  readonly sourceBlocks: readonly MergeCompactionSourceBlock[];
}

export interface MergeCompactionSourceSegment {
  readonly segmentId: string;
  readonly transactionId: string;
  readonly committedVersion: number;
  readonly kind: SegmentKind;
  readonly keyColumnId: string | null;
  readonly level: number;
  readonly logicalOrder: number;
  readonly rowCount: number;
  readonly rowIdStart: bigint;
  readonly rowIdEndExclusive: bigint;
  readonly rowIdSpans: readonly RowIdSpan[];
  readonly columns: readonly MergeCompactionSourceColumn[];
}

export interface MergeCompactionOutputSourceRange {
  /** Row offset within the canonical merged output. */
  readonly outputRowStart: number;
  readonly sourceBlockId: string;
  /** Row offset within sourceBlockId. */
  readonly sourceRowStart: number;
  readonly rowCount: number;
}

export interface MergeCompactionOutputColumn {
  readonly columnId: string;
  readonly type: SimpleDataType;
  readonly sourceRanges: readonly MergeCompactionOutputSourceRange[];
}

/**
 * One output segment of a partitioned physical rewrite: a contiguous run of the canonical
 * output, published as its own level-one partition under its own logical order.
 */
export interface MergeOutputPartition {
  /** Row offset within the canonical merged output. */
  readonly rowStart: number;
  readonly rowCount: number;
  /** The finite, non-negative published order; strictly increasing across partitions. */
  readonly logicalOrder: number;
}

/** An immutable logical replay result followed by a physical, output-driven rewrite. */
export interface MergeCompactionRewritePlan {
  readonly kind: "merge-v1";
  readonly targetBlockBytes: number;
  readonly outputCompression: CompactionOutputCompression;
  readonly keyColumnId: string;
  readonly totalRows: number;
  /** Bounding row-ID envelope; spans preserve gaps and output order. */
  readonly rowIdStart: bigint;
  readonly rowIdEndExclusive: bigint;
  readonly rowIdSpans: readonly RowIdSpan[];
  readonly logicalOrder: number;
  readonly sourceSegments: readonly MergeCompactionSourceSegment[];
  readonly columns: readonly MergeCompactionOutputColumn[];
  readonly outputs: readonly RechunkCompactionOutputWindow[];
  /**
   * How the output is split into published segments. Missing on plans that publish the whole
   * output as one segment under `logicalOrder`. When present, the partitions tile the output
   * contiguously, every output window lies inside one partition, and partition `i` publishes
   * as the job's output segment ID for `i === 0` and `${outputSegmentId}/${i}` after that.
   */
  readonly partitions?: readonly MergeOutputPartition[];
}

export type CompactionRewritePlan =
  CopyCompactionRewritePlan | RechunkCompactionRewritePlan | MergeCompactionRewritePlan;

/**
 * The next rechunk output to emit, ordered by output window and then column. A completed cursor
 * has outputIndex === outputs.length, columnIndex zero, and rowStart === totalRows.
 */
export interface CompactionOutputCursor {
  outputIndex: number;
  columnIndex: number;
  rowStart: number;
}

export interface CompactionJobRecord {
  id: string;
  tableId: string;
  sourceManifestVersion: number;
  sourceSegmentIds: string[];
  sourceBlockIds: string[];
  outputBlockIds: string[];
  cursor: CompactionJobCursor;
  processedRows: number;
  sourceStoredBytes: number;
  outputStoredBytes: number;
  logicalBytes: number;
  readonly rewritePlan: CompactionRewritePlan;
  /** Null for copy-v1; points at the next output for rechunk-v1. */
  outputCursor: CompactionOutputCursor | null;
  /** Immutable execution budget. Zero for copy-v1 jobs. */
  readonly memoryBudgetBytes: number;
  /** Immutable planner estimate. Zero for copy-v1 jobs. */
  readonly minimumMemoryBytes: number;
  /** Immutable stored bytes from newly promoted level-zero sources. */
  readonly level0SourceStoredBytes: number;
  /** Immutable stored bytes from the retained level-one anchor. */
  readonly anchorSourceStoredBytes: number;
  /** Output partition assigned by the append-row-range L2 policy. */
  readonly outputPartitionOrdinal?: number;
  /** Immutable maximum compaction output bytes per newly promoted L0 byte. */
  readonly maxWriteAmplification?: number;
  /** Immutable exact ceiling for all stored output blocks produced by this job. */
  readonly maximumOutputStoredBytes?: number;
  /** Immutable conservative full-block upper bound for the planned output. */
  readonly plannedOutputStoredBytesUpperBound?: number;
  /**
   * Immutable stored bytes already written by cancelled or aborted attempts at these same
   * sources. The persisted ceiling is reduced by this amount, so attempts share one lifetime
   * write-amplification budget.
   */
  readonly priorAttemptOutputStoredBytes?: number;
  peakWorkingBytes: number;
  outputLogicalBytes: number;
  targetLevel: number;
  state: CompactionJobState;
  transactionId: string | null;
  outputSegmentId: string | null;
  publishedVersion: number | null;
  revision: number;
  createdAt: string;
  updatedAt: string;
  error?: string;
}

export interface CompactionJobRecordUpdate {
  outputBlockIds?: readonly string[];
  cursor?: CompactionJobCursor;
  processedRows?: number;
  sourceStoredBytes?: number;
  outputStoredBytes?: number;
  logicalBytes?: number;
  outputCursor?: CompactionOutputCursor | null;
  peakWorkingBytes?: number;
  outputLogicalBytes?: number;
  state?: CompactionJobState;
  transactionId?: string | null;
  outputSegmentId?: string | null;
  publishedVersion?: number | null;
  updatedAt: string;
  error?: string | null;
}

/** Every segment ID owned by a compaction job, including merge partition outputs. */
export function compactionOutputSegmentIds(
  job: Pick<CompactionJobRecord, "outputSegmentId" | "rewritePlan">,
): string[] {
  const outputSegmentId = job.outputSegmentId;
  if (outputSegmentId === null) return [];
  const partitionCount =
    (job.rewritePlan.kind === "merge-v1" || job.rewritePlan.kind === "rechunk-v1") &&
    job.rewritePlan.partitions !== undefined
      ? job.rewritePlan.partitions.length
      : 1;
  return Array.from({ length: partitionCount }, (_, index) =>
    index === 0 ? outputSegmentId : `${outputSegmentId}/${String(index)}`,
  );
}

/**
 * Proves that a compaction transaction stages exactly the immutable job output. This is kept in
 * the storage contract (rather than only in the engine) so adapters reject forged, reordered, or
 * partially journaled output before publication and while loading durable state.
 */
export function assertCompactionOutputProvenance(
  job: CompactionJobRecord,
  table: TableRecord,
  transaction: Pick<TransactionRecord, "id" | "pendingBlockIds" | "pendingSegmentIds">,
  sourceSegments: readonly SegmentRecord[],
  outputSegments: readonly SegmentRecord[],
  options: { readonly allowOutputPrefix?: boolean } = {},
): void {
  if (job.transactionId !== transaction.id) {
    throw new Error(`Compaction job ${job.id} belongs to another transaction`);
  }
  const expectedSegmentIds = compactionOutputSegmentIds(job);
  const stagedExpectedSegmentIds = options.allowOutputPrefix
    ? expectedSegmentIds.slice(0, transaction.pendingSegmentIds.length)
    : expectedSegmentIds;
  assertExactOrderedIds(
    transaction.pendingSegmentIds,
    stagedExpectedSegmentIds,
    `Compaction job ${job.id} segment journal`,
  );
  assertExactOrderedIds(
    outputSegments.map((segment) => segment.id),
    stagedExpectedSegmentIds,
    `Compaction job ${job.id} output segments`,
  );
  assertExactOrderedIds(
    sourceSegments.map((segment) => segment.id),
    job.sourceSegmentIds,
    `Compaction job ${job.id} source segments`,
  );

  const plan = job.rewritePlan;
  if (plan.kind === "copy-v1") {
    const expectedOutputBlockIds = sourceSegments.flatMap((segment, segmentIndex) =>
      table.columns.flatMap((column, columnIndex) =>
        (segment.columnBlockIds[column.id] ?? []).map((_blockId, part) =>
          copyCompactionOutputBlockId(job.id, segmentIndex, columnIndex, part),
        ),
      ),
    );
    assertExactOrderedIds(
      job.outputBlockIds,
      expectedOutputBlockIds,
      `Compaction job ${job.id} output block plan`,
    );
  } else {
    if (
      table.columns.length !== plan.columns.length ||
      table.columns.some((column, index) => {
        const planned = plan.columns.at(index);
        if (planned?.columnId !== column.id) return true;
        return planned.type !== column.type;
      })
    ) {
      throw new Error(`Compaction job ${job.id} table schema differs from its rewrite plan`);
    }
    if (plan.kind === "merge-v1" && plan.keyColumnId !== table.uniqueKeyColumnId) {
      throw new Error(`Compaction job ${job.id} table key differs from its rewrite plan`);
    }
    const expectedOutputBlockIds = plan.outputs.flatMap((_output, outputIndex) =>
      plan.columns.map((_column, columnIndex) =>
        physicalCompactionOutputBlockId(job.id, outputIndex, columnIndex),
      ),
    );
    assertExactOrderedIds(
      job.outputBlockIds,
      expectedOutputBlockIds,
      `Compaction job ${job.id} output block plan`,
    );
  }
  assertExactOrderedIds(
    transaction.pendingBlockIds,
    job.outputBlockIds,
    `Compaction job ${job.id} block journal`,
  );

  const expectedSegments = expectedCompactionOutputSegments(job, table, sourceSegments);
  for (const [index, actual] of outputSegments.entries()) {
    const expected = expectedSegments[index];
    if (expected === undefined) {
      throw new Error(`Compaction job ${job.id} stages an unexpected output segment`);
    }
    assertCompactionSegmentFields(actual, expected, job.id);
  }
}

type CompactionOutputShape = Omit<SegmentRecord, "createdAt">;

function expectedCompactionOutputSegments(
  job: CompactionJobRecord,
  table: TableRecord,
  sourceSegments: readonly SegmentRecord[],
): CompactionOutputShape[] {
  const outputSegmentId = job.outputSegmentId;
  if (outputSegmentId === null) return [];
  const plan = job.rewritePlan;
  const common = {
    tableId: table.id,
    transactionId: job.transactionId ?? "",
    ...(table.uniqueKeyColumnId === undefined ? {} : { keyColumnId: table.uniqueKeyColumnId }),
    level: job.targetLevel,
    ...(job.outputPartitionOrdinal === undefined
      ? {}
      : { partitionOrdinal: job.outputPartitionOrdinal }),
  };
  if (plan.kind === "copy-v1") {
    const first = sourceSegments[0];
    const last = sourceSegments[sourceSegments.length - 1];
    if (first === undefined || last === undefined) {
      throw new Error(`Compaction job ${job.id} has no source rows`);
    }
    return [
      {
        id: outputSegmentId,
        ...common,
        rowCount: safeSum(
          sourceSegments.map((segment) => segment.rowCount),
          "Copy compaction output rows",
        ),
        rowIdStart: first.rowIdStart,
        rowIdEndExclusive: last.rowIdEndExclusive,
        columnBlockIds: Object.fromEntries(
          table.columns.map((column, columnIndex) => [
            column.id,
            sourceSegments.flatMap((segment, segmentIndex) =>
              (segment.columnBlockIds[column.id] ?? []).map((_blockId, part) =>
                copyCompactionOutputBlockId(job.id, segmentIndex, columnIndex, part),
              ),
            ),
          ]),
        ),
        kind: "insert",
        logicalOrder: Math.min(...sourceSegments.map((segment) => segment.logicalOrder)),
        commitOrdinal: 0,
        rowIdSpans: [],
      },
    ];
  }

  const partitions = plan.partitions ?? [
    { rowStart: 0, rowCount: plan.totalRows, logicalOrder: plan.logicalOrder },
  ];
  return partitions.map((partition, index) => {
    const rowIdSpans =
      plan.kind === "merge-v1"
        ? sliceCompactionRowIdSpans(plan.rowIdSpans, partition.rowStart, partition.rowCount)
        : [];
    const envelope =
      plan.kind === "merge-v1"
        ? compactionRowIdSpanEnvelope(rowIdSpans)
        : {
            start: plan.rowIdStart + BigInt(partition.rowStart),
            endExclusive: plan.rowIdStart + BigInt(partition.rowStart + partition.rowCount),
          };
    return {
      id: index === 0 ? outputSegmentId : `${outputSegmentId}/${String(index)}`,
      ...common,
      rowCount: partition.rowCount,
      rowIdStart: envelope.start,
      rowIdEndExclusive: envelope.endExclusive,
      columnBlockIds: Object.fromEntries(
        plan.columns.map((column, columnIndex) => [
          column.columnId,
          plan.outputs.flatMap((output, outputIndex) =>
            output.rowStart >= partition.rowStart &&
            output.rowStart + output.rowCount <= partition.rowStart + partition.rowCount
              ? [physicalCompactionOutputBlockId(job.id, outputIndex, columnIndex)]
              : [],
          ),
        ]),
      ),
      kind: plan.kind === "merge-v1" ? "base" : "insert",
      logicalOrder: partition.logicalOrder,
      commitOrdinal: index,
      rowIdSpans,
    };
  });
}

function assertCompactionSegmentFields(
  actual: SegmentRecord,
  expected: CompactionOutputShape,
  jobId: string,
): void {
  for (const field of [
    "id",
    "tableId",
    "transactionId",
    "rowCount",
    "rowIdStart",
    "rowIdEndExclusive",
    "kind",
    "keyColumnId",
    "level",
    "logicalOrder",
    "commitOrdinal",
    "partitionOrdinal",
  ] as const) {
    if (actual[field] !== expected[field]) {
      throw new Error(`Compaction job ${jobId} output ${actual.id} has invalid ${field}`);
    }
  }
  const expectedColumnIds = Object.keys(expected.columnBlockIds);
  assertExactOrderedIds(
    Object.keys(actual.columnBlockIds).sort(),
    [...expectedColumnIds].sort(),
    `Compaction job ${jobId} output ${actual.id} columns`,
  );
  for (const columnId of expectedColumnIds) {
    assertExactOrderedIds(
      actual.columnBlockIds[columnId] ?? [],
      expected.columnBlockIds[columnId] ?? [],
      `Compaction job ${jobId} output ${actual.id} column ${columnId}`,
    );
  }
  if (
    actual.rowIdSpans.length !== expected.rowIdSpans.length ||
    actual.rowIdSpans.some((span, index) => {
      const planned = expected.rowIdSpans[index];
      return (
        planned?.rowStart !== span.rowStart ||
        planned.rowCount !== span.rowCount ||
        planned.rowIdStart !== span.rowIdStart
      );
    })
  ) {
    throw new Error(`Compaction job ${jobId} output ${actual.id} has invalid row-ID spans`);
  }
}

function assertExactOrderedIds(
  actual: readonly string[],
  expected: readonly string[],
  label: string,
): void {
  if (
    actual.length !== expected.length ||
    actual.some((value, index) => value !== expected[index])
  ) {
    throw new Error(`${label} is not the canonical ordered sequence`);
  }
}

function physicalCompactionOutputBlockId(
  jobId: string,
  outputIndex: number,
  columnIndex: number,
): string {
  return [
    jobId,
    "rewrite",
    "window",
    String(outputIndex).padStart(8, "0"),
    "column",
    String(columnIndex).padStart(8, "0"),
  ].join("/");
}

function copyCompactionOutputBlockId(
  jobId: string,
  segmentIndex: number,
  columnIndex: number,
  part: number,
): string {
  return [
    jobId,
    "output",
    "segment",
    String(segmentIndex).padStart(6, "0"),
    "column",
    String(columnIndex).padStart(6, "0"),
    "part",
    String(part).padStart(6, "0"),
  ].join("/");
}

function sliceCompactionRowIdSpans(
  spans: readonly RowIdSpan[],
  rowStart: number,
  rowCount: number,
): RowIdSpan[] {
  const result: RowIdSpan[] = [];
  const rowEnd = rowStart + rowCount;
  for (const span of spans) {
    const spanEnd = span.rowStart + span.rowCount;
    if (spanEnd <= rowStart || span.rowStart >= rowEnd) continue;
    const start = Math.max(span.rowStart, rowStart);
    const end = Math.min(spanEnd, rowEnd);
    const next = {
      rowStart: start - rowStart,
      rowCount: end - start,
      rowIdStart: span.rowIdStart + BigInt(start - span.rowStart),
    };
    const previous = result[result.length - 1];
    if (
      previous !== undefined &&
      previous.rowStart + previous.rowCount === next.rowStart &&
      previous.rowIdStart + BigInt(previous.rowCount) === next.rowIdStart
    ) {
      result[result.length - 1] = {
        ...previous,
        rowCount: previous.rowCount + next.rowCount,
      };
    } else {
      result.push(next);
    }
  }
  return result;
}

function compactionRowIdSpanEnvelope(spans: readonly RowIdSpan[]): {
  start: bigint;
  endExclusive: bigint;
} {
  if (spans.length === 0) return { start: 0n, endExclusive: 0n };
  let start = spans[0]?.rowIdStart ?? 0n;
  let endExclusive = start + BigInt(spans[0]?.rowCount ?? 0);
  for (const span of spans.slice(1)) {
    if (span.rowIdStart < start) start = span.rowIdStart;
    const spanEnd = span.rowIdStart + BigInt(span.rowCount);
    if (spanEnd > endExclusive) endExclusive = spanEnd;
  }
  return { start, endExclusive };
}

export class CompactionJobConflictError extends Error {
  override readonly name = "CompactionJobConflictError";

  constructor(
    readonly jobId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number | null,
  ) {
    super(
      `Compaction job ${jobId} changed: expected revision ${String(expectedRevision)}, found ${String(actualRevision)}`,
    );
  }
}

export const garbageCollectionJobStates = ["planned", "running", "completed"] as const;
export type GarbageCollectionJobState = (typeof garbageCollectionJobStates)[number];

/**
 * How far a job has examined each of its candidate lists, in the order a step works through
 * them: manifests, then segments, then blocks, then transactions. Transactions come last so a
 * segment the same job reclaims has already released the transaction that wrote it.
 */
export interface GarbageCollectionCursor {
  manifestIndex: number;
  segmentIndex: number;
  blockIndex: number;
  transactionIndex: number;
}

export interface CreateGarbageCollectionJobInput {
  id: string;
  candidateManifestVersions: readonly number[];
  candidateSegmentIds: readonly string[];
  candidateBlockIds: readonly string[];
  /**
   * Transaction records the planner believes nothing needs any more: committed records below
   * the retained window with no segment owner, or aborted records after their pending artifacts
   * are gone. Optional, so a caller that only reclaims artifacts need not mention them. The
   * store decides for itself at step time.
   */
  candidateTransactionIds?: readonly string[];
  /** Fixed cutoff used to decide which persisted leases protect a manifest for this job. */
  leaseCutoff: string;
  createdAt: string;
  /** Present while bounded discovery is still paging durable metadata. */
  discovery?: GarbageCollectionDiscovery;
}

export type GarbageCollectionDiscoveryPhase =
  "manifests" | "manifest-blocks" | "segments" | "transactions" | "compactions" | "complete";

export interface GarbageCollectionDiscovery {
  phase: GarbageCollectionDiscoveryPhase;
  currentManifestVersion: number | null;
  retainAboveVersion: number;
  retainAfter: number;
  maxPlanningItems: number;
  manifestCursor: number | null;
  segmentCursor: string | null;
  transactionCursor: string | null;
  compactionCursor: string | null;
  visitedRecords: number;
  /**
   * When a bounded candidate array fills, reclamation completes this job and the next job
   * resumes this discovery phase instead of restarting at the beginning.
   */
  resumePhase?: Exclude<GarbageCollectionDiscoveryPhase, "complete"> | null;
  /** Older continuation resumed only after this job first discovers newly eligible manifests. */
  postManifestPhase?: Exclude<GarbageCollectionDiscoveryPhase, "complete" | "manifests"> | null;
  /** Exact within-record provenance offset; every named artifact is examined at most once. */
  artifactCursor?: GarbageCollectionArtifactCursor | null;
}

export interface GarbageCollectionArtifactCursor {
  family: "manifest" | "transaction" | "compaction";
  /** Decimal manifest version for `manifest`; durable record ID for the other families. */
  recordId: string;
  /** Exact lexical manifest-membership cursor; null for transaction/compaction arrays. */
  blockId: string | null;
  blockIndex: number;
  segmentIndex: number;
}

/** Absolute per-call bound for durable maintenance candidate arrays and storage transactions. */
export const MAX_MAINTENANCE_BATCH_ITEMS = 1_024;

export function boundedMaintenanceBatchItems(value: number, label: string): number {
  const normalized = positiveWholeNumber(value, label);
  if (normalized > MAX_MAINTENANCE_BATCH_ITEMS) {
    throw new RangeError(`${label} cannot exceed ${String(MAX_MAINTENANCE_BATCH_ITEMS)} items`);
  }
  return normalized;
}

export interface UpdateGarbageCollectionPlanningInput {
  jobId: string;
  expectedRevision: number;
  candidateManifestVersions?: readonly number[];
  candidateSegmentIds?: readonly string[];
  candidateBlockIds?: readonly string[];
  candidateTransactionIds?: readonly string[];
  discovery: GarbageCollectionDiscovery;
  updatedAt: string;
}

export interface GarbageCollectionJobRecord {
  id: string;
  candidateManifestVersions: number[];
  candidateSegmentIds: string[];
  candidateBlockIds: string[];
  candidateTransactionIds: string[];
  cursor: GarbageCollectionCursor;
  prunedManifestCount: number;
  alreadyPrunedManifestCount: number;
  retainedManifestCount: number;
  missingManifestCount: number;
  reclaimedSegmentCount: number;
  retainedSegmentCount: number;
  missingSegmentCount: number;
  reclaimedBlockCount: number;
  retainedBlockCount: number;
  missingBlockCount: number;
  reclaimedBlockBytes: number;
  reclaimedTransactionCount: number;
  retainedTransactionCount: number;
  missingTransactionCount: number;
  state: GarbageCollectionJobState;
  revision: number;
  leaseCutoff: string;
  createdAt: string;
  updatedAt: string;
  /** Omitted only for explicitly supplied bounded candidate jobs; no discovery scan is needed. */
  discovery?: GarbageCollectionDiscovery;
}

export interface RunGarbageCollectionStepInput {
  jobId: string;
  expectedRevision: number;
  maxItems: number;
  updatedAt: string;
}

export interface GarbageCollectionStepResult {
  job: GarbageCollectionJobRecord;
  prunedManifestVersions: number[];
  alreadyPrunedManifestVersions: number[];
  retainedManifestVersions: number[];
  missingManifestVersions: number[];
  reclaimedSegmentIds: string[];
  retainedSegmentIds: string[];
  missingSegmentIds: string[];
  reclaimedBlockIds: string[];
  retainedBlockIds: string[];
  missingBlockIds: string[];
  reclaimedBlockBytes: number;
  reclaimedTransactionIds: string[];
  retainedTransactionIds: string[];
  missingTransactionIds: string[];
}

export interface StoragePage<T, Cursor> {
  records: T[];
  nextCursor: Cursor | null;
}

export class GarbageCollectionJobConflictError extends Error {
  override readonly name = "GarbageCollectionJobConflictError";

  constructor(
    readonly jobId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number | null,
  ) {
    super(
      `Garbage collection job ${jobId} changed: expected revision ${String(expectedRevision)}, found ${String(actualRevision)}`,
    );
  }
}

export class SnapshotManifestMissingError extends Error {
  override readonly name = "SnapshotManifestMissingError";

  constructor(readonly version: number) {
    super(`Snapshot manifest is unavailable: ${String(version)}`);
  }
}

export interface GarbageCollectionStepAccounting {
  examinedManifestCount: number;
  prunedManifestCount: number;
  alreadyPrunedManifestCount: number;
  retainedManifestCount: number;
  missingManifestCount: number;
  examinedSegmentCount: number;
  reclaimedSegmentCount: number;
  retainedSegmentCount: number;
  missingSegmentCount: number;
  examinedBlockCount: number;
  reclaimedBlockCount: number;
  retainedBlockCount: number;
  missingBlockCount: number;
  reclaimedBlockBytes: number;
  examinedTransactionCount: number;
  reclaimedTransactionCount: number;
  retainedTransactionCount: number;
  missingTransactionCount: number;
  updatedAt: string;
}

export class LeaseConflictError extends Error {
  override readonly name = "LeaseConflictError";

  constructor(
    readonly leaseId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number | null,
  ) {
    super(
      `Lease ${leaseId} changed: expected revision ${String(expectedRevision)}, found ${String(actualRevision)}`,
    );
  }
}

/** A renewal/move cutoff reached the persisted expiry; expiry is irrevocable. */
export class LeaseExpiredError extends Error {
  override readonly name = "LeaseExpiredError";

  constructor(
    readonly leaseId: string,
    readonly expiresAt: string,
    readonly expiresAtCutoff: string,
  ) {
    super(`Lease ${leaseId} expired at ${expiresAt}`);
  }
}

/** Refuses a lease release from a different durable owner. */
export class LeaseOwnerConflictError extends Error {
  override readonly name = "LeaseOwnerConflictError";

  constructor(
    readonly leaseId: string,
    readonly expectedOwnerId: string,
    readonly actualOwnerId: string,
  ) {
    super(`Lease ${leaseId} belongs to ${actualOwnerId}, not release owner ${expectedOwnerId}`);
  }
}

export type TransactionStatus = "active" | "committed" | "aborted";

export interface TransactionRecord {
  id: string;
  /** Stable writer identity; liveness renewals never contend on the data revision. */
  ownerId: string;
  /** Durable writer liveness deadline. Expired active records are atomically aborted by GC. */
  expiresAt: string;
  snapshotVersion: number | null;
  pendingBlockIds: string[];
  pendingSegmentIds: string[];
  status: TransactionStatus;
  revision: number;
  startedAt: string;
  updatedAt: string;
  committedVersion: number | null;
  /**
   * Structural catalog epoch captured atomically with the transaction snapshot. Every active
   * transaction carries this storage-owned guard; commit rejects it if DDL changed the schema
   * used to prepare the staged artifacts. Terminal records drop the guard.
   */
  schemaEpochGuard?: number;
  /**
   * Catalog record owned exclusively by this active transaction. It is invisible to catalog
   * reads until commit; abort/expiry discards the reservation with the transaction.
   */
  pendingTable?: TableRecord;
  /** Next row id for the pending table, advanced atomically as insert artifacts stage. */
  pendingTableNextRowId?: bigint;
  /** Catalog epoch captured atomically when the pending table was admitted. */
  catalogEpochGuard?: number;
}

export interface AbortTransactionIfExpiredInput {
  transactionId: string;
  expectedOwnerId: string;
  expiresAtCutoff: string;
  updatedAt: string;
}

export interface RenewTransactionInput {
  transactionId: string;
  ownerId: string;
  expiresAtCutoff: string;
  expiresAt: string;
}

export interface TransactionRecordUpdate {
  snapshotVersion?: number | null;
  pendingBlockIds?: readonly string[];
  pendingSegmentIds?: readonly string[];
  status?: TransactionStatus;
  updatedAt: string;
  committedVersion?: number | null;
  /** Storage-owned advancement while staging rows for a pending table. */
  pendingTableNextRowId?: bigint;
}

export interface BeginTransactionInput {
  /**
   * Record to create; the store atomically stamps both `snapshotVersion` and
   * `schemaEpochGuard` from its current state.
   */
  record: Omit<TransactionRecord, "snapshotVersion" | "schemaEpochGuard">;
  /** Reserve this many row ids for the table in the same atomic step. */
  reserveRowIds?: { tableId: string; count: number };
  /**
   * Reserve auto-increment values for the column in the same atomic step, first bumping the
   * counter to at least `atLeast`. `count` may be 0 for a pure bump past explicit values.
   */
  reserveAutoIncrement?: { tableId: string; columnId: string; count: number; atLeast?: bigint };
  /**
   * Reserves one invisible catalog record and its initial row-id state in the same atomic step.
   * The expected epoch closes schema/FK races between planning and begin; commit rechecks it.
   */
  pendingTable?: {
    record: TableRecord;
    nextRowId: bigint;
    expectedCatalogEpoch: number;
  };
}

export interface BeginTransactionResult {
  record: TransactionRecord;
  rowIds?: RowIdRange;
  autoIncrementValues?: RowIdRange;
}

export interface StageTransactionArtifactsInput {
  transactionId: string;
  expectedRevision: number;
  blocks: readonly BlockWrite[];
  segments: readonly SegmentRecord[];
  updatedAt: string;
}

/** Maximum immutable payloads accepted by one atomic staging/WAL operation. */
export const MAX_TRANSACTION_STAGE_BLOCKS = 64;
/** Maximum segment records accepted by one atomic staging/WAL operation. */
export const MAX_TRANSACTION_STAGE_SEGMENTS = 64;
/**
 * Maximum block bytes accepted by one atomic staging/WAL operation. The limit still admits one
 * maximum-size physical block; callers split collections of smaller blocks into bounded calls.
 */
export const MAX_TRANSACTION_STAGE_BYTES = MAX_STORED_BLOCK_BYTE_LENGTH;
/** Durable journal ceilings prevent a forgotten or hostile transaction growing without bound. */
export const MAX_TRANSACTION_PENDING_BLOCKS = 4_096;
export const MAX_TRANSACTION_PENDING_SEGMENTS = 4_096;

/** Storage-boundary preflight shared by staging and single-shot write implementations. */
export function assertTransactionArtifactBatchLimits(
  blocks: readonly BlockWrite[],
  segments: readonly SegmentRecord[],
): void {
  if (blocks.length > MAX_TRANSACTION_STAGE_BLOCKS) {
    throw new RangeError(
      `Transaction artifact batch exceeds ${String(MAX_TRANSACTION_STAGE_BLOCKS)} blocks`,
    );
  }
  if (segments.length > MAX_TRANSACTION_STAGE_SEGMENTS) {
    throw new RangeError(
      `Transaction artifact batch exceeds ${String(MAX_TRANSACTION_STAGE_SEGMENTS)} segments`,
    );
  }
  let byteLength = 0;
  for (const block of blocks) {
    if (!(block.bytes instanceof Uint8Array)) {
      throw new TypeError("Transaction block bytes must be a Uint8Array");
    }
    byteLength += block.bytes.byteLength;
    if (!Number.isSafeInteger(byteLength) || byteLength > MAX_TRANSACTION_STAGE_BYTES) {
      throw new RangeError(
        `Transaction artifact batch exceeds ${String(MAX_TRANSACTION_STAGE_BYTES)} block bytes`,
      );
    }
  }
}

/** Preflight for the complete persisted journal after a bounded staging operation. */
export function assertTransactionArtifactJournalLimits(
  pendingBlockIds: readonly string[],
  pendingSegmentIds: readonly string[],
): void {
  if (pendingBlockIds.length > MAX_TRANSACTION_PENDING_BLOCKS) {
    throw new RangeError(
      `Transaction journal exceeds ${String(MAX_TRANSACTION_PENDING_BLOCKS)} pending blocks`,
    );
  }
  if (pendingSegmentIds.length > MAX_TRANSACTION_PENDING_SEGMENTS) {
    throw new RangeError(
      `Transaction journal exceeds ${String(MAX_TRANSACTION_PENDING_SEGMENTS)} pending segments`,
    );
  }
}

/**
 * Atomically rewinds one active transaction's artifact journal to an earlier set and removes
 * exactly the artifacts that are no longer reachable. Journals are canonically sorted, so an
 * adapter validates the retained and removed lists as a duplicate-free, disjoint, exact
 * partition of its current journal before changing records or bytes.
 */
export interface RollbackTransactionArtifactsInput {
  transactionId: string;
  expectedRevision: number;
  pendingBlockIds: readonly string[];
  pendingSegmentIds: readonly string[];
  removeBlockIds: readonly string[];
  removeSegmentIds: readonly string[];
  updatedAt: string;
}

export type StorageIntegrityMode = "metadata" | "full";

export interface StorageIntegrityIssue {
  readonly code: string;
  readonly location: string;
  readonly message: string;
}

export interface StorageIntegrityReport {
  readonly mode: StorageIntegrityMode;
  readonly ok: boolean;
  readonly checkedRecords: number;
  readonly checkedBlocks: number;
  readonly checkedBytes: number;
  /** Total findings; `issues` itself is bounded by `maxIssues`. */
  readonly issueCount: number;
  readonly issues: readonly StorageIntegrityIssue[];
}

export interface StorageStats {
  readonly backend: string;
  /** Bytes reachable as current database records or payloads. */
  readonly logicalBytes: number;
  /** Actual substrate allocation where available; IndexedDB cannot report this per database. */
  readonly physicalBytes: number | null;
  readonly liveBlockCount: number;
  /** Stored payloads outside the current manifest, including staged and retired bytes. */
  readonly obsoleteBlockCount: number;
  readonly liveBlockBytes: number;
  readonly obsoleteBlockBytes: number;
  readonly temporaryBytes: number;
  readonly walBytes: number | null;
  readonly checkpointBytes: number | null;
  readonly orphanBytes: number | null;
  readonly manifestCount: number;
  readonly transactionCount: number;
  readonly segmentCount: number;
  readonly maintenance?: {
    readonly degraded: boolean;
    readonly consecutiveFailures: number;
    readonly lastError: string | null;
    readonly walLimitBytes: number | null;
    /** Durable garbage awaiting safe physical deletion, when the substrate separates the two. */
    readonly cleanupDebtBytes: number | null;
    /** Refusal/backpressure ceiling for cleanup debt, or null when the substrate has no debt. */
    readonly cleanupLimitBytes: number | null;
  };
}

export interface InterruptedSnapshotImport {
  readonly identity: string;
  readonly version: number;
  readonly createdAt: string;
  readonly stagedBlockCount: number;
  readonly stagedBytes: number;
}

export interface InterruptedSnapshotImportAbortResult {
  readonly identity: string;
  readonly removedBlockCount: number;
  readonly removedBytes: number;
}

/** A snapshot session is renewable but can never pin storage for more than one hour unattended. */
export const MAX_SNAPSHOT_SESSION_TTL_MS = 60 * 60 * 1_000;

/** Snapshot v1 is an ordered bounded record stream; no database-sized collection is in header. */
export const SNAPSHOT_FRAME_KINDS = [
  "catalog-page",
  "segment-page",
  "transaction-page",
  "unique-page",
  "posting-page",
  "block",
] as const;
export type SnapshotFrameKind = (typeof SNAPSHOT_FRAME_KINDS)[number];
export const MAX_SNAPSHOT_FRAME_ITEMS = 1_024;
export const MAX_SNAPSHOT_METADATA_FRAME_BYTES = 4 * 1024 * 1024;
export const MAX_SNAPSHOT_FRAME_BATCH_ITEMS = 64;
export const MAX_SNAPSHOT_METADATA_BATCH_BYTES = 16 * 1024 * 1024;
/** One block frame is permitted; metadata frames still use MAX_SNAPSHOT_METADATA_BATCH_BYTES. */
export const MAX_SNAPSHOT_FRAME_BATCH_BYTES = MAX_STORED_BLOCK_BYTE_LENGTH + 8 * 1024;
/**
 * Framed restore builds accelerator generations in a disposable core before one atomic publish.
 * These aggregate ceilings keep that validation heap bounded until generations become a native
 * chunk-backed structure rather than one materialized in-memory collection.
 */
export const MAX_SNAPSHOT_IMPORT_ACCELERATOR_RETAINED_BYTES =
  MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL;
export const MAX_SNAPSHOT_IMPORT_ACCELERATOR_RETAINED_ENTRIES =
  MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL;

export interface SnapshotKindSummary {
  readonly frameCount: number;
  readonly itemCount: number;
  readonly storedBytes: number;
}

/** One table plus its durable allocation counters; memberships/postings are separate pages. */
export interface SnapshotCatalogItem {
  readonly kind: "table";
  readonly record: TableRecord;
  readonly nextRowId: bigint;
  readonly autoIncrement: ReadonlyArray<{ readonly columnId: string; readonly next: bigint }>;
}

export interface SnapshotSegmentItem {
  readonly kind: "segment";
  readonly record: SegmentRecord;
}

export interface SnapshotTransactionItem {
  readonly kind: "transaction";
  readonly record: TransactionRecord;
}

/** Descriptor precedes this generation's chunks; tokens are globally strict lexical order. */
export interface SnapshotUniqueGenerationItem {
  readonly kind: "unique-generation";
  readonly tableId: string;
  readonly indexId: string | null;
  readonly namespaceId: string;
  readonly generationId: string;
  readonly chunkCount: number;
  readonly tokenCount: number;
}

export interface SnapshotUniqueChunkItem {
  readonly kind: "unique-chunk";
  readonly namespaceId: string;
  readonly generationId: string;
  readonly ordinal: number;
  readonly keyTokens: readonly string[];
}

export type SnapshotUniqueItem = SnapshotUniqueGenerationItem | SnapshotUniqueChunkItem;

/**
 * Complete canonical postings generation at the captured version. Both full-text and scalar
 * secondary accelerators use this base format; deltas are merged during bounded export, so a
 * restore never needs pre-snapshot history.
 */
export interface SnapshotPostingGenerationItem {
  readonly kind: "posting-generation";
  readonly tableId: string;
  readonly ownerKind: "fts-column" | "secondary-index";
  readonly ownerId: string;
  readonly storageColumnId: string;
  readonly generationId: string;
  readonly coversVersion: number;
  readonly chunkCount: number;
  /** Exact safe sum of every posting frequency; secondary-index frequencies are always one. */
  readonly totalTokens: number;
}

export interface SnapshotPostingChunkItem {
  readonly kind: "posting-chunk";
  readonly storageColumnId: string;
  readonly generationId: string;
  readonly ordinal: number;
  readonly postings: readonly FtsPosting[];
}

export type SnapshotPostingItem = SnapshotPostingGenerationItem | SnapshotPostingChunkItem;

export type SnapshotMetadataItem =
  | SnapshotCatalogItem
  | SnapshotSegmentItem
  | SnapshotTransactionItem
  | SnapshotUniqueItem
  | SnapshotPostingItem;

/** Modeled heap retained by one framed accelerator chunk during atomic snapshot validation. */
export function snapshotAcceleratorItemRetainedUsage(
  item: SnapshotUniqueChunkItem | SnapshotPostingChunkItem,
): { bytes: number; entries: number } {
  if (item.kind === "unique-chunk") {
    return {
      bytes: uniqueKeyBuildChunkRetainedBytes(item.keyTokens),
      entries: item.keyTokens.length,
    };
  }
  let bytes = 0;
  let entries = 0;
  for (const posting of item.postings) {
    bytes = safeSum([bytes, 32 + posting.term.length * 2], "Snapshot posting retained bytes");
    entries = safeSum([entries, posting.rowIds.length], "Snapshot posting retained entries");
    // Bigint row IDs, term-frequency numbers, and the two array slots all remain live until the
    // generation is validated and promoted. The fixed model is intentionally conservative.
    bytes = safeSum([bytes, posting.rowIds.length * 48], "Snapshot posting retained bytes");
  }
  return { bytes, entries };
}

/** Shared arithmetic boundary used by streaming adapters and allocation-free cap tests. */
export function assertSnapshotImportAcceleratorUsage(bytes: number, entries: number): void {
  if (!Number.isSafeInteger(bytes) || bytes < 0) {
    throw new RangeError("Snapshot accelerator retained bytes are invalid");
  }
  if (!Number.isSafeInteger(entries) || entries < 0) {
    throw new RangeError("Snapshot accelerator retained entries are invalid");
  }
  if (bytes > MAX_SNAPSHOT_IMPORT_ACCELERATOR_RETAINED_BYTES) {
    throw new StorageResourceLimitError(
      "snapshot accelerator byte",
      bytes,
      MAX_SNAPSHOT_IMPORT_ACCELERATOR_RETAINED_BYTES,
    );
  }
  if (entries > MAX_SNAPSHOT_IMPORT_ACCELERATOR_RETAINED_ENTRIES) {
    throw new StorageResourceLimitError(
      "snapshot accelerator entry",
      entries,
      MAX_SNAPSHOT_IMPORT_ACCELERATOR_RETAINED_ENTRIES,
    );
  }
}

/**
 * Canonical compressed container header. It remains O(1) with database size: the six entries
 * below contain counts and byte totals only, never record descriptors, keys, or postings.
 */
export interface SnapshotFrameStreamHeader {
  readonly formatVersion: 1;
  readonly databaseVersion: number;
  readonly createdAt: string;
  readonly kinds: Readonly<Record<SnapshotFrameKind, SnapshotKindSummary>>;
}

/**
 * One exact body frame. Frames are globally contiguous by `sequence` and grouped in
 * `SNAPSHOT_FRAME_KINDS` order. Within kinds, tables/segments/transactions/blocks order by ID;
 * UNIQUE generations order by namespace (descriptor, then contiguous chunk ordinals), and
 * posting generations by storageColumnId using the same descriptor/chunk rule. Metadata
 * payloads use canonical adapter encoding and contain at
 * most MAX_SNAPSHOT_FRAME_ITEMS/MAX_SNAPSHOT_METADATA_FRAME_BYTES; a block frame contains one
 * ID plus one stored block and may use MAX_STORED_BLOCK_BYTE_LENGTH. `checksum` covers the exact
 * payload. Metadata payloads use the single core-owned snapshot wire codec; adapters must not
 * substitute substrate encodings. Lost-ack replay must additionally compare the complete
 * persisted bytes.
 */
export interface SnapshotFrame {
  readonly sequence: number;
  readonly kind: SnapshotFrameKind;
  readonly itemCount: number;
  /** Block ID for a block frame; null for core-wire-encoded metadata pages. */
  readonly key: string | null;
  readonly payload: Uint8Array;
  readonly checksum: number;
}

export interface SnapshotFrameFooter {
  readonly frameCount: number;
  readonly itemCount: number;
  readonly storedBytes: number;
  /** Rolling checksum over every canonical frame header and payload in sequence order. */
  readonly checksum: number;
}

export interface BeginSnapshotFrameExportInput {
  readonly ownerId: string;
  readonly createdAt: string;
  readonly expiresAt: string;
}

/**
 * Export begin atomically captures a frozen catalog/segment/transaction/accelerator generation
 * into bounded durable pages and records only an exact manifest version/cursor for blocks (it
 * must not copy block descriptors). It creates the backup lease in the same operation.
 * Catalog-only writes after begin cannot change the session.
 */
export interface SnapshotFrameExportSession {
  readonly sessionId: string;
  readonly ownerId: string;
  readonly expiresAt: string;
  readonly header: SnapshotFrameStreamHeader;
}

export interface ReadSnapshotExportFrameInput {
  readonly sessionId: string;
  readonly ownerId: string;
  readonly sequence: number;
  readonly expiresAtCutoff: string;
  readonly expiresAt: string;
}

export interface BeginSnapshotFrameImportInput {
  readonly identity: string;
  readonly ownerId: string;
  readonly createdAt: string;
  readonly expiresAt: string;
  readonly header: SnapshotFrameStreamHeader;
}

export interface SnapshotFrameImportSession {
  readonly identity: string;
  readonly ownerId: string;
  readonly version: number;
  readonly createdAt: string;
  readonly expiresAt: string;
  readonly nextSequence: number;
  readonly stagedBytes: number;
}

export interface RenewSnapshotFrameImportInput {
  readonly identity: string;
  readonly ownerId: string;
  readonly expiresAtCutoff: string;
  readonly expiresAt: string;
}

export interface AppendSnapshotImportFramesInput extends RenewSnapshotFrameImportInput {
  /** Bounded contiguous frames; replayed sequences compare complete persisted bytes. */
  readonly frames: readonly SnapshotFrame[];
}

export interface FinishSnapshotFrameImportInput {
  readonly identity: string;
  readonly ownerId: string;
  readonly expiresAtCutoff: string;
  readonly footer: SnapshotFrameFooter;
}

export interface CloseSnapshotExportInput {
  readonly sessionId: string;
  readonly ownerId: string;
}

export interface CancelSnapshotImportInput {
  readonly identity: string;
  readonly ownerId: string;
}

export class SnapshotImportConflictError extends Error {
  override readonly name = "SnapshotImportConflictError";

  constructor(
    readonly identity: string,
    readonly ownerId: string,
    message: string,
  ) {
    super(`Snapshot import ${identity} owned by ${ownerId}: ${message}`);
  }
}

/**
 * Persisted bytes belong to a recognized storage family, but not to the version this reader
 * supports. This is deliberately distinct from corruption: callers must not infer that repair,
 * recreation, or deletion is safe merely because versions differ.
 */
export class StorageFormatVersionError extends Error {
  override readonly name = "StorageFormatVersionError";

  constructor(
    readonly backend: string,
    readonly location: string,
    readonly actualVersion: number | null,
    readonly supportedVersion: number,
    readonly relation: "older" | "newer",
  ) {
    const actual =
      actualVersion === null ? "an unknown version" : `version ${String(actualVersion)}`;
    super(
      `${backend} storage format at ${location} uses ${actual}, which is ${relation} than ` +
        `the supported version ${String(supportedVersion)}`,
    );
  }
}

/** A native IndexedDB schema upgrade is waiting for another connection to close. */
export class IndexedDbSchemaUpgradeBlockedError extends Error {
  override readonly name = "IndexedDbSchemaUpgradeBlockedError";

  constructor(
    readonly databaseName: string,
    readonly oldVersion: number,
    readonly requestedVersion: number,
  ) {
    super(
      `IndexedDB schema upgrade for ${databaseName} from version ${String(oldVersion)} to ` +
        `${String(requestedVersion)} is blocked by another open connection; close older tabs and retry`,
    );
  }
}

/** Persisted metadata or payload failed an adapter's fail-closed integrity checks. */
export class StorageCorruptionError extends Error {
  override readonly name = "StorageCorruptionError";

  constructor(
    readonly backend: string,
    readonly location: string,
    message: string,
  ) {
    super(`${backend} storage corruption at ${location}: ${message}`);
  }
}

/**
 * A remote OPFS leader may have committed a mutation whose acknowledgement was lost.
 * Reconcile stable identities or revisions before retrying the named operation.
 */
export class OpfsUncertainOutcomeError extends Error {
  override readonly name = "OpfsUncertainOutcomeError";

  constructor(readonly method: string) {
    super(
      `The OPFS leader changed before ${method} was acknowledged; the mutation may have committed`,
    );
  }
}

/**
 * The commit input carries only the change: added blocks are the transaction's journaled pending
 * blocks, removals are the superseded ids. The store derives the published manifest from its
 * stored base, so commit cost scales with the delta rather than the database's total block count.
 */
export interface CommitTransactionInput {
  transactionId: string;
  changedTableIds?: readonly string[];
  expectedTransactionRevision: number;
  expectedManifestVersion: number | null;
  /**
   * Required when blocks are retired. The store atomically proves this ready compaction job is
   * owned by the transaction and that `removedBlockIds` exactly names its still-current,
   * unaliased source set. Ordinary commits cannot retire visible blocks.
   */
  compactionJobId?: string;
  removedBlockIds?: readonly string[];
  /**
   * Atomic post-commit fragmentation ceilings. Exactly one entry is mandatory for every table
   * receiving a pending level-zero segment, no unrelated entry is allowed, and no limit may
   * exceed `MAX_LEVEL_ZERO_SEGMENTS`.
   */
  levelZeroSegmentLimits?: ReadonlyArray<{ tableId: string; limit: number }>;
  /**
   * Per-table unique-key changes, in operation order. Multi-entry commits come from atomic
   * write scopes; entries for the same table apply sequentially, so in-scope conflicts
   * (inserting one key twice) fail exactly like cross-commit conflicts.
   */
  uniqueKeyChanges?: readonly UniqueKeyChanges[];
  /** Per-table full-text deltas; at most one entry per table. */
  ftsChanges?: readonly FtsChanges[];
  committedAt: string;
}

/**
 * The single-shot write: stage these blocks and segments and commit them, in one atomic storage
 * transaction. Carries the same commit change as `CommitTransactionInput`; the artifacts become
 * the transaction's journaled pending ids on the way through.
 */
export interface WriteTransactionInput extends Omit<
  CommitTransactionInput,
  "transactionId" | "expectedTransactionRevision"
> {
  /**
   * Which transaction publishes. `{ id, expectedRevision }` names an active transaction begun
   * earlier (typically with `beginTransaction`, when the artifacts depend on a row-id or
   * auto-increment reservation); its journal is extended and its revision compare-and-swapped
   * (`TransactionRecordConflictError`). `{ record }` begins the transaction in the same step —
   * a fresh record (revision 0, empty journal) the store pins at `expectedManifestVersion` —
   * so a write that needed no reservation costs one round trip in total. A fresh record carries
   * the `schemaEpochGuard` captured with its preparation; the store rejects a stale guard before
   * persisting any part of the write.
   */
  transaction:
    | { id: string; expectedRevision: number }
    | { record: Omit<TransactionRecord, "snapshotVersion"> };
  blocks: readonly BlockWrite[];
  segments: readonly SegmentRecord[];
}

export interface UniqueKeyChanges {
  tableId: string;
  keyTokens: readonly string[];
  requireAbsent: boolean;
  remove?: boolean;
}

/** Bounded calls used to seed an arbitrarily large UNIQUE namespace without heap materialization. */
export const MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK = 4_096;
export const MAX_UNIQUE_KEY_BUILD_CHUNK_BYTES = 4 * 1024 * 1024;
export const MAX_UNIQUE_KEY_BUILD_TTL_MS = 60 * 60 * 1_000;
export const MAX_UNIQUE_KEY_BUILD_STAGED_BYTES = 512 * 1024 * 1024;
export const MAX_UNIQUE_KEY_BUILD_STAGED_BYTES_TOTAL = 1024 * 1024 * 1024;

export interface UniqueKeyBuildRecord {
  buildId: string;
  tableId: string;
  indexId: string;
  namespaceId: string;
  ownerId: string;
  state: "active" | "completed";
  nextOrdinal: number;
  tokenCount: number;
  retainedBytes: number;
  expiresAt: string;
  createdAt: string;
  updatedAt: string;
  completedAt?: string;
}

export interface BeginUniqueKeyBuildInput {
  buildId: string;
  tableId: string;
  indexId: string;
  namespaceId: string;
  ownerId: string;
  expiresAt: string;
  createdAt: string;
}

export interface AppendUniqueKeyBuildChunkInput {
  buildId: string;
  ownerId: string;
  expiresAtCutoff: string;
  ordinal: number;
  keyTokens: readonly string[];
  updatedAt: string;
}

export interface FinishUniqueKeyBuildInput {
  buildId: string;
  ownerId: string;
  expiresAtCutoff: string;
  expectedTableRevision: number;
  expectedManifestVersion: number | null;
  chunkCount: number;
  coversVersion: number;
  completedAt: string;
}

export interface RenewUniqueKeyBuildInput {
  buildId: string;
  ownerId: string;
  expiresAtCutoff: string;
  expiresAt: string;
  updatedAt: string;
}

export interface AbortUniqueKeyBuildInput {
  buildId: string;
  ownerId: string;
  expiresAtCutoff: string;
}

/**
 * A bounded background fold of one already-enforced UNIQUE namespace. Begin freezes the exact
 * immutable base generation and delta prefix through `throughVersion`; later commits keep
 * appending canonical `(namespace, token, version)` add/remove records to a separate ordered
 * tail. Each step resolves at most one bounded lexical token window
 * into a new immutable generation. Finish swaps the generation and drops only the frozen tail
 * prefix atomically, so no commit copies database-sized membership and readers always see an
 * exact base+tail view.
 */
export interface UniqueKeyFoldRecord {
  foldId: string;
  tableId: string;
  indexId: string | null;
  namespaceId: string;
  ownerId: string;
  state: "active" | "completed";
  sourceGenerationId: string | null;
  outputGenerationId: string;
  throughVersion: number;
  afterToken: string | null;
  nextOrdinal: number;
  tokenCount: number;
  retainedBytes: number;
  createdAt: string;
  updatedAt: string;
  expiresAt: string;
  completedAt?: string;
}

export interface BeginUniqueKeyFoldInput {
  foldId: string;
  tableId: string;
  indexId: string | null;
  namespaceId: string;
  ownerId: string;
  outputGenerationId: string;
  expectedManifestVersion: number;
  createdAt: string;
  expiresAt: string;
}

export interface RunUniqueKeyFoldStepInput {
  foldId: string;
  ownerId: string;
  expiresAtCutoff: string;
  expiresAt: string;
  updatedAt: string;
  maxTokens: number;
}

export interface FinishUniqueKeyFoldInput {
  foldId: string;
  ownerId: string;
  expiresAtCutoff: string;
  expectedSourceGenerationId: string | null;
  expectedThroughVersion: number;
  chunkCount: number;
  completedAt: string;
}

export interface AbortUniqueKeyFoldInput {
  foldId: string;
  ownerId: string;
  expiresAtCutoff: string;
}

/** Validates one staged seed call before an adapter opens a transaction or WAL frame. */
export function uniqueKeyBuildChunkRetainedBytes(keyTokens: readonly string[]): number {
  if (keyTokens.length === 0 || keyTokens.length > MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK) {
    throw new RangeError(
      `A UNIQUE build chunk must contain 1-${String(MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK)} tokens`,
    );
  }
  const seen = new Set<string>();
  let bytes = 0;
  for (const token of keyTokens) {
    if (
      typeof token !== "string" ||
      token.length === 0 ||
      token.length > MAX_FTS_POSTING_TERM_CHARACTERS
    ) {
      throw new TypeError("A UNIQUE build token has invalid length");
    }
    assertWellFormedString(token, "UNIQUE build token");
    if (seen.has(token)) throw new UniqueKeyConflictError("UNIQUE build", token);
    seen.add(token);
    bytes = safeSum([bytes, 16 + token.length * 2], "UNIQUE build chunk bytes");
    if (bytes > MAX_UNIQUE_KEY_BUILD_CHUNK_BYTES) {
      throw new RangeError(
        `A UNIQUE build chunk cannot exceed ${String(MAX_UNIQUE_KEY_BUILD_CHUNK_BYTES)} retained bytes`,
      );
    }
  }
  return bytes;
}

export class UniqueKeyBuildConflictError extends Error {
  override readonly name = "UniqueKeyBuildConflictError";

  constructor(
    readonly buildId: string,
    readonly reason: string,
  ) {
    super(`UNIQUE build ${buildId} cannot continue: ${reason}`);
  }
}

/** One term's postings within a commit delta or base chunk: parallel rowId/tf arrays. */
export interface FtsPosting {
  term: string;
  rowIds: bigint[];
  tf: number[];
}

export const MAX_POSTING_BUILD_TTL_MS = 60 * 60 * 1_000;

export interface BeginPostingBuildInput {
  tableId: string;
  columnId: string;
  buildId: string;
  ownerId: string;
  createdAt: string;
  expiresAt: string;
}

export interface RenewPostingBuildInput {
  tableId: string;
  columnId: string;
  buildId: string;
  ownerId: string;
  expiresAtCutoff: string;
  expiresAt: string;
  updatedAt: string;
}

export interface AppendPostingBuildChunkInput extends RenewPostingBuildInput {
  ordinal: number;
  chunk: readonly FtsPosting[];
}

export interface FinishPostingBuildInput {
  tableId: string;
  columnId: string;
  buildId: string;
  ownerId: string;
  expiresAtCutoff: string;
  coversVersion: number;
  chunkCount: number;
  totalTokens: number;
  completedAt: string;
}

export interface AbortPostingBuildInput {
  tableId: string;
  columnId: string;
  buildId: string;
  ownerId: string;
  expiresAtCutoff: string;
}

export class PostingBuildConflictError extends Error {
  override readonly name = "PostingBuildConflictError";

  constructor(
    readonly buildId: string,
    readonly ownerId: string,
    readonly reason: string,
  ) {
    super(`Posting build ${buildId} owned by ${ownerId}: ${reason}`);
  }
}

/** One indexed column's contribution from one commit: postings for the commit's new rows. */
export interface FtsColumnDelta {
  columnId: string;
  /** Term-sorted postings; rowIds ascending within each term. */
  postings: FtsPosting[];
  /** Total tokens the commit's rows contribute to this column — feeds exact BM25 statistics. */
  totalTokens: number;
}

/**
 * A commit's full-text index deltas, applied atomically with the manifest publish. The store
 * also closes the stale-writer race here: a commit that adds segments to a table whose record
 * indexes a column in state "building" or "ready" without carrying that column's delta flips
 * the column to "invalid" (self-healing rebuild) rather than rejecting the data commit.
 */
export interface FtsChanges {
  tableId: string;
  columns: readonly FtsColumnDelta[];
}

/** In-memory and atomic-publish bounds for one transaction's accelerator/key deltas. */
export const MAX_TRANSACTION_COMMIT_DELTA_BYTES = 64 * 1024 * 1024;
export const MAX_TRANSACTION_COMMIT_DELTA_ENTRIES = 1_048_576;

export function transactionCommitDeltaRetainedBytes(
  uniqueKeyChanges: readonly UniqueKeyChanges[],
  ftsChanges: readonly FtsChanges[],
): { bytes: number; entries: number } {
  let bytes = 0;
  let entries = 0;
  const add = (amount: number): void => {
    bytes += amount;
    if (!Number.isSafeInteger(bytes) || bytes > MAX_TRANSACTION_COMMIT_DELTA_BYTES) {
      throw new RangeError(
        `Transaction commit deltas exceed ${String(MAX_TRANSACTION_COMMIT_DELTA_BYTES)} retained bytes`,
      );
    }
  };
  const addEntry = (): void => {
    entries += 1;
    if (entries > MAX_TRANSACTION_COMMIT_DELTA_ENTRIES) {
      throw new RangeError(
        `Transaction commit deltas exceed ${String(MAX_TRANSACTION_COMMIT_DELTA_ENTRIES)} entries`,
      );
    }
  };
  for (const change of uniqueKeyChanges) {
    add(48 + change.tableId.length * 2);
    for (const token of change.keyTokens) {
      addEntry();
      add(16 + token.length * 2);
    }
  }
  for (const change of ftsChanges) {
    add(32 + change.tableId.length * 2);
    for (const column of change.columns) {
      add(48 + column.columnId.length * 2);
      for (const posting of column.postings) {
        addEntry();
        add(32 + posting.term.length * 2);
        if (posting.rowIds.length !== posting.tf.length) {
          throw new TypeError("Full-text posting row and frequency counts differ");
        }
        entries += posting.rowIds.length;
        if (entries > MAX_TRANSACTION_COMMIT_DELTA_ENTRIES) {
          throw new RangeError(
            `Transaction commit deltas exceed ${String(MAX_TRANSACTION_COMMIT_DELTA_ENTRIES)} entries`,
          );
        }
        add(posting.rowIds.length * 16);
      }
    }
  }
  return { bytes, entries };
}

/** The per-term candidate row IDs a full-text index lookup returns, aligned with the query. */
export interface FtsCandidates {
  /** Per requested term: ascending unique row IDs whose indexed column contained the term. */
  rowIdsByTerm: bigint[][];
  /** The bounded read stopped before collecting every matching row; callers must scan. */
  overflow: boolean;
}

/** Aggregate row-id ceiling for one postings candidate read. Overflow falls back to a scan. */
export const MAX_FTS_CANDIDATE_ROW_IDS = 65_536;
/** Query-term cardinality shared by the parser and every postings adapter boundary. */
export const MAX_FTS_QUERY_TERMS = 32;
/** Retained metadata/value ceiling for one ordered postings read. Overflow falls back to a scan. */
export const MAX_FTS_ORDERED_READ_BYTES = 64 * 1024 * 1024;
/** Hard public mutation bounds for one streamed postings-build chunk. */
export const MAX_FTS_POSTINGS_PER_CHUNK = 65_536;
export const MAX_FTS_POSTING_ROW_IDS_PER_CHUNK = 1_048_576;
export const MAX_FTS_POSTING_TERM_CHARACTERS = 65_536;
/** Indexed string values are bounded; non-indexed block strings retain the physical block cap. */
export const MAX_INDEXED_STRING_CHARACTERS = 16_384;
/** Maximum normalized tokens retained from one indexed document. */
export const MAX_FTS_TOKENS_PER_DOCUMENT = 4_096;
/** Bounded catalog/TOC cardinality. Oversized accelerators invalidate and queries scan. */
export const MAX_FTS_BASE_CHUNKS = 4_096;
export const MAX_FTS_DELTA_CHUNKS = 128;

/** One exact/prefix term lookup or a lexicographic term range over a postings index. */
export type FtsPostingQuery =
  | { term: string; prefix: boolean }
  | {
      lower?: string;
      lowerInclusive?: boolean;
      upper?: string;
      upperInclusive?: boolean;
    };

/** Strict, bounded runtime validation shared by every adapter before any postings read. */
export function validateFtsPostingQueries(
  value: unknown,
  label = "Full-text query terms",
): asserts value is readonly FtsPostingQuery[] {
  if (!Array.isArray(value)) throw new TypeError(`${label} must be an array`);
  if (value.length > MAX_FTS_QUERY_TERMS) {
    throw new RangeError(`${label} cannot exceed ${String(MAX_FTS_QUERY_TERMS)} items`);
  }
  for (const queryValue of value as unknown[]) {
    if (typeof queryValue !== "object" || queryValue === null || Array.isArray(queryValue)) {
      throw new TypeError("Full-text posting query must be an object");
    }
    const query = queryValue as Record<string, unknown>;
    const keys = Object.keys(query);
    if ("term" in query) {
      if (
        keys.length !== 2 ||
        keys.some((key) => key !== "term" && key !== "prefix") ||
        typeof query.term !== "string" ||
        query.term.length === 0 ||
        query.term.length > MAX_FTS_POSTING_TERM_CHARACTERS ||
        typeof query.prefix !== "boolean"
      ) {
        throw new TypeError("Full-text exact/prefix query is invalid");
      }
      assertWellFormedString(query.term, "Full-text query term");
      continue;
    }
    if (
      keys.some(
        (key) =>
          key !== "lower" &&
          key !== "lowerInclusive" &&
          key !== "upper" &&
          key !== "upperInclusive",
      ) ||
      (query.lower !== undefined &&
        (typeof query.lower !== "string" ||
          query.lower.length > MAX_FTS_POSTING_TERM_CHARACTERS)) ||
      (query.upper !== undefined &&
        (typeof query.upper !== "string" ||
          query.upper.length > MAX_FTS_POSTING_TERM_CHARACTERS)) ||
      (query.lowerInclusive !== undefined && typeof query.lowerInclusive !== "boolean") ||
      (query.upperInclusive !== undefined && typeof query.upperInclusive !== "boolean")
    ) {
      throw new TypeError("Full-text range query is invalid");
    }
    if (typeof query.lower === "string") {
      assertWellFormedString(query.lower, "Full-text query lower bound");
    }
    if (typeof query.upper === "string") {
      assertWellFormedString(query.upper, "Full-text query upper bound");
    }
  }
}

/** Whether a stored term belongs to one exact, prefix, or range lookup. */
export function ftsPostingQueryMatches(term: string, query: FtsPostingQuery): boolean {
  if ("term" in query) return query.prefix ? term.startsWith(query.term) : term === query.term;
  if (query.lower !== undefined) {
    if (term < query.lower || (term === query.lower && query.lowerInclusive === false))
      return false;
  }
  if (query.upper !== undefined) {
    if (term > query.upper || (term === query.upper && query.upperInclusive === false))
      return false;
  }
  return true;
}

/**
 * Shared candidate-merge core for both stores: fetching chunks is store-specific, but the
 * term-match rule (exact, or prefix as a term range) and the sorted-unique row-id shape must
 * never drift between backends — pruning would silently differ per store.
 */
export function collectFtsCandidates(
  chunkLists: Iterable<readonly FtsPosting[]>,
  terms: readonly FtsPostingQuery[],
  maxRowIds = MAX_FTS_CANDIDATE_ROW_IDS,
): FtsCandidates {
  validateFtsPostingQueries(terms);
  if (!Number.isSafeInteger(maxRowIds) || maxRowIds < 1 || maxRowIds > MAX_FTS_CANDIDATE_ROW_IDS) {
    throw new RangeError(
      `Full-text candidate limit must be between 1 and ${String(MAX_FTS_CANDIDATE_ROW_IDS)}`,
    );
  }
  const sets = terms.map(() => new Set<bigint>());
  let retainedRowIds = 0;
  for (const postings of chunkLists) {
    for (const posting of postings) {
      for (let index = 0; index < terms.length; index += 1) {
        const term = terms[index];
        if (term === undefined) continue;
        const matches = ftsPostingQueryMatches(posting.term, term);
        if (!matches) continue;
        const set = sets[index];
        if (set !== undefined) {
          for (const rowId of posting.rowIds) {
            if (set.has(rowId)) continue;
            if (retainedRowIds === maxRowIds) {
              return { rowIdsByTerm: terms.map(() => []), overflow: true };
            }
            set.add(rowId);
            retainedRowIds += 1;
          }
        }
      }
    }
  }
  return {
    rowIdsByTerm: sets.map((set) =>
      [...set].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0)),
    ),
    overflow: false,
  };
}

/** Validates the fixed memory ceilings accepted by one ordered postings read. */
export function validateFtsOrderedReadLimits(
  maxRowIds = MAX_FTS_CANDIDATE_ROW_IDS,
  maxRetainedBytes = MAX_FTS_ORDERED_READ_BYTES,
): void {
  if (!Number.isSafeInteger(maxRowIds) || maxRowIds < 1 || maxRowIds > MAX_FTS_CANDIDATE_ROW_IDS) {
    throw new RangeError(
      `Ordered postings row limit must be between 1 and ${String(MAX_FTS_CANDIDATE_ROW_IDS)}`,
    );
  }
  if (
    !Number.isSafeInteger(maxRetainedBytes) ||
    maxRetainedBytes < 1 ||
    maxRetainedBytes > MAX_FTS_ORDERED_READ_BYTES
  ) {
    throw new RangeError(
      `Ordered postings byte limit must be between 1 and ${String(MAX_FTS_ORDERED_READ_BYTES)}`,
    );
  }
}

/**
 * K-way merges sorted base/delta chunks into canonical term order. Only one term's row map and
 * one cursor per chunk stay live beyond the returned postings, avoiding a second index-sized map.
 */
export function collectFtsPostings(chunkLists: Iterable<readonly FtsPosting[]>): FtsPosting[] {
  return mergeFtsPostings(chunkLists, Number.POSITIVE_INFINITY, Number.POSITIVE_INFINITY).postings;
}

/** Bounded ordered merge; overflow deliberately returns no partial authoritative answer. */
export function collectFtsPostingsBounded(
  chunkLists: Iterable<readonly FtsPosting[]>,
  maxRowIds = MAX_FTS_CANDIDATE_ROW_IDS,
  maxRetainedBytes = MAX_FTS_ORDERED_READ_BYTES,
): { postings: FtsPosting[]; overflow: boolean } {
  validateFtsOrderedReadLimits(maxRowIds, maxRetainedBytes);
  return mergeFtsPostings(chunkLists, maxRowIds, maxRetainedBytes);
}

function mergeFtsPostings(
  chunkLists: Iterable<readonly FtsPosting[]>,
  maxRowIds: number,
  maxRetainedBytes: number,
): { postings: FtsPosting[]; overflow: boolean } {
  interface Cursor {
    postings: readonly FtsPosting[];
    position: number;
    ordinal: number;
  }
  const heap: Cursor[] = [];
  const before = (left: Cursor, right: Cursor): boolean => {
    const leftTerm = left.postings[left.position]?.term ?? "";
    const rightTerm = right.postings[right.position]?.term ?? "";
    return leftTerm < rightTerm || (leftTerm === rightTerm && left.ordinal < right.ordinal);
  };
  const cursorAt = (position: number): Cursor => {
    const cursor = heap[position];
    if (cursor === undefined) throw new Error("Posting merge heap is inconsistent");
    return cursor;
  };
  const push = (cursor: Cursor): void => {
    heap.push(cursor);
    let child = heap.length - 1;
    while (child > 0) {
      const parent = (child - 1) >>> 1;
      const childCursor = cursorAt(child);
      const parentCursor = cursorAt(parent);
      if (!before(childCursor, parentCursor)) break;
      heap[parent] = childCursor;
      heap[child] = parentCursor;
      child = parent;
    }
  };
  const pop = (): Cursor | undefined => {
    const first = heap[0];
    const last = heap.pop();
    if (first === undefined || last === undefined) return first;
    if (heap.length === 0) return first;
    heap[0] = last;
    let parent = 0;
    for (;;) {
      const left = parent * 2 + 1;
      if (left >= heap.length) break;
      const right = left + 1;
      const child = right < heap.length && before(cursorAt(right), cursorAt(left)) ? right : left;
      const childCursor = cursorAt(child);
      const parentCursor = cursorAt(parent);
      if (!before(childCursor, parentCursor)) break;
      heap[parent] = childCursor;
      heap[child] = parentCursor;
      parent = child;
    }
    return first;
  };

  let ordinal = 0;
  for (const postings of chunkLists) {
    if (postings.length > 0) push({ postings, position: 0, ordinal });
    ordinal += 1;
  }
  const output: FtsPosting[] = [];
  let retainedRowIds = 0;
  let retainedBytes = 0;
  while (heap.length > 0) {
    const term = heap[0]?.postings[heap[0].position]?.term;
    if (term === undefined) break;
    const rows = new Map<bigint, number>();
    while (heap[0]?.postings[heap[0].position]?.term === term) {
      const cursor = pop();
      if (cursor === undefined) break;
      const posting = cursor.postings[cursor.position];
      if (posting === undefined) continue;
      posting.rowIds.forEach((rowId, index) => {
        const existing = rows.get(rowId);
        if (existing === undefined) {
          rows.set(rowId, posting.tf[index] ?? 1);
        } else {
          rows.set(rowId, Math.max(existing, posting.tf[index] ?? 1));
        }
      });
      cursor.position += 1;
      if (cursor.position < cursor.postings.length) push(cursor);
    }
    const nextRetainedRowIds = retainedRowIds + rows.size;
    const nextRetainedBytes = retainedBytes + 64 + term.length * 2 + rows.size * 32;
    if (nextRetainedRowIds > maxRowIds || nextRetainedBytes > maxRetainedBytes) {
      return { postings: [], overflow: true };
    }
    const ordered = [...rows].sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
    output.push({
      term,
      rowIds: ordered.map(([rowId]) => rowId),
      tf: ordered.map(([, frequency]) => frequency),
    });
    retainedRowIds = nextRetainedRowIds;
    retainedBytes = nextRetainedBytes;
  }
  return { postings: output, overflow: false };
}

/**
 * Shared stale-writer policy for both stores' commit steps: a commit that adds segments to a
 * table with active full-text columns but no covering delta flips the uncovered columns to
 * "invalid" (the index self-heals through a rebuild; the data commit itself always proceeds).
 * Returns the updated record, or undefined when nothing changes.
 */
export function invalidateUncoveredFtsColumns(
  record: TableRecord,
  coveredColumnIds: ReadonlySet<string>,
): TableRecord | undefined {
  const ftsColumns = record.ftsColumns;
  if (ftsColumns === undefined) return undefined;
  let invalidated = false;
  const next: Record<string, FtsColumnIndexRecord> = {};
  for (const [columnId, state] of Object.entries(ftsColumns)) {
    if (state.state !== "invalid" && !coveredColumnIds.has(columnId)) {
      next[columnId] = { ...state, state: "invalid" };
      invalidated = true;
    } else {
      next[columnId] = { ...state };
    }
  }
  if (!invalidated) return undefined;
  return {
    ...record,
    ftsColumns: next,
    revision: safeSum([record.revision, 1], "Table revision"),
  };
}

/**
 * Scalar-index half of the stale-writer rule. A writer that staged table data from catalog
 * metadata older than a newly building/ready index cannot provide its postings, so the commit
 * invalidates that index atomically. Readers then scan until a rebuild closes the gap.
 */
export function invalidateUncoveredSecondaryIndexes(
  record: TableRecord,
  coveredStorageColumnIds: ReadonlySet<string>,
): TableRecord | undefined {
  const indexes = record.secondaryIndexes;
  if (indexes === undefined) return undefined;
  let invalidated = false;
  const next: Record<string, SecondaryIndexRecord> = {};
  for (const [indexId, index] of Object.entries(indexes)) {
    if (index.state !== "invalid" && !coveredStorageColumnIds.has(index.storageColumnId)) {
      const { buildId: _abandonedBuild, ...invalid } = index;
      void _abandonedBuild;
      next[indexId] = { ...invalid, state: "invalid" };
      invalidated = true;
    } else {
      next[indexId] = { ...index };
    }
  }
  if (!invalidated) return undefined;
  return {
    ...record,
    secondaryIndexes: next,
    revision: safeSum([record.revision, 1], "Table revision"),
  };
}

/** Physical posting IDs a current catalog record authorizes a commit to write. */
export function activePostingStorageColumnIds(record: TableRecord): Set<string> {
  return new Set([
    ...Object.entries(record.ftsColumns ?? {}).flatMap(([columnId, index]) =>
      index.state === "invalid" ? [] : [columnId],
    ),
    ...Object.values(record.secondaryIndexes ?? {}).flatMap((index) =>
      index.state === "invalid" ? [] : [index.storageColumnId],
    ),
  ]);
}

export class UniqueKeyConflictError extends Error {
  override readonly name = "UniqueKeyConflictError";

  constructor(
    readonly tableId: string,
    readonly keyToken: string,
  ) {
    super(`Unique key already exists in table ${tableId}`);
  }
}

/** A writer prepared before an enforced UNIQUE index existed and therefore cannot enforce it. */
export class UniqueIndexCoverageError extends Error {
  override readonly name = "UniqueIndexCoverageError";

  constructor(
    readonly tableId: string,
    readonly indexName: string,
  ) {
    super(`Write did not cover enforced UNIQUE index ${indexName} on table ${tableId}`);
  }
}

export class WriteConflictError extends Error {
  override readonly name = "WriteConflictError";

  constructor(
    readonly expectedVersion: number | null,
    readonly actualVersion: number | null,
  ) {
    super(`Manifest changed: expected ${String(expectedVersion)}, found ${String(actualVersion)}`);
  }
}

/** Staged write artifacts were prepared against a structurally different catalog. */
export class SchemaConflictError extends Error {
  override readonly name = "SchemaConflictError";

  constructor(
    readonly expectedEpoch: number,
    readonly actualEpoch: number,
  ) {
    super(
      `Schema changed while the write was staged: expected epoch ${String(expectedEpoch)}, found ${String(actualEpoch)}`,
    );
  }
}

export class TransactionRecordConflictError extends Error {
  override readonly name = "TransactionRecordConflictError";

  constructor(
    readonly transactionId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number | null,
  ) {
    super(
      `Transaction ${transactionId} changed: expected revision ${String(expectedRevision)}, found ${String(actualRevision)}`,
    );
  }
}

export interface BlockWrite {
  id: string;
  bytes: Uint8Array;
}

export interface TempRunPage {
  ownerId: string;
  runId: string;
  pageIndex: number;
  bytes: Uint8Array;
}

/**
 * Atomically adopts an unpublished compaction output left by an aborted attempt. The adapter
 * verifies that `segment` exactly matches the stored immutable record apart from its owner,
 * that the old owner is the expected aborted revision, and that the replacement is the active
 * transaction linked to `compactionJobId`. It then retags the segment and moves its journal
 * membership from the old owner to the replacement while advancing both transaction revisions.
 */
export interface AdoptAbortedSegmentInput {
  segment: SegmentRecord;
  expectedAbortedTransactionId: string;
  expectedAbortedTransactionRevision: number;
  replacementTransactionId: string;
  expectedReplacementTransactionRevision: number;
  compactionJobId: string;
  updatedAt: string;
}

/** Hard storage-boundary limits for one scratch-page write. */
export const MAX_TEMP_RUN_PAGES_PER_BATCH = 64;
export const MAX_TEMP_RUN_PAGE_BYTES = MAX_STORED_BLOCK_BYTE_LENGTH;
export const MAX_TEMP_RUN_BATCH_BYTES = MAX_STORED_BLOCK_BYTE_LENGTH;

/**
 * Refuses an oversized scratch write before an adapter starts a transaction or appends a WAL
 * record. Query execution splits pages and batches to these limits, so the limits bound atomic
 * storage work without limiting the total size of a spill.
 */
export function assertTempRunPageBatchLimits(pages: readonly TempRunPage[]): void {
  if (pages.length > MAX_TEMP_RUN_PAGES_PER_BATCH) {
    throw new RangeError(
      `Temp run page batch exceeds ${String(MAX_TEMP_RUN_PAGES_PER_BATCH)} pages`,
    );
  }
  let totalBytes = 0;
  for (const page of pages) {
    if (!(page.bytes instanceof Uint8Array)) {
      throw new TypeError("Temp run page bytes must be a Uint8Array");
    }
    if (page.bytes.byteLength > MAX_TEMP_RUN_PAGE_BYTES) {
      throw new RangeError(`Temp run page exceeds ${String(MAX_TEMP_RUN_PAGE_BYTES)} bytes`);
    }
    totalBytes += page.bytes.byteLength;
    if (!Number.isSafeInteger(totalBytes)) {
      throw new RangeError("Temp run page batch bytes exceed the safe integer range");
    }
    if (totalBytes > MAX_TEMP_RUN_BATCH_BYTES) {
      throw new RangeError(`Temp run page batch exceeds ${String(MAX_TEMP_RUN_BATCH_BYTES)} bytes`);
    }
  }
}

export interface TempOwnerRecord {
  ownerId: string;
  createdAt: string;
  expiresAt: string;
  revision: number;
}

export const MAX_TEMP_OWNER_TTL_MS = 60 * 60 * 1_000;

export interface RenewTempOwnerInput {
  ownerId: string;
  expectedRevision: number;
  expiresAtCutoff: string;
  expiresAt: string;
}

export class TempOwnerConflictError extends Error {
  override readonly name = "TempOwnerConflictError";

  constructor(
    readonly ownerId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number | null,
  ) {
    super(
      `Temp owner ${ownerId} changed: expected revision ${String(expectedRevision)}, found ${String(actualRevision)}`,
    );
  }
}

/**
 * One coherent read of everything query preparation needs before touching blocks: the
 * current manifest version, the named table records, current segments of the found tables,
 * and the transaction records those segments reference. Stores that can produce this in one
 * atomic read collapse the sequential per-record round trips a prepare would otherwise issue.
 */
export interface QueryCatalogState {
  manifestVersion: number | null;
  /** Positional per requested name; undefined where the table does not exist. */
  tables: Array<TableRecord | undefined>;
  /** Current-manifest segments of the found tables, sorted by id. */
  segments: SegmentRecord[];
  /** Records for the segments' transaction ids; missing records are omitted. */
  transactions: TransactionRecord[];
  /**
   * The catalog epoch this state was read at, read in the same atomic storage transaction.
   * Present when the store maintains an epoch (see `getCatalogProbe`); callers may cache the
   * state and reuse it while a probe returns the same epoch.
   */
  catalogEpoch?: number;
}

/**
 * The change counters a reader or writer needs, read together in one atomic storage
 * transaction. `manifestVersion` moves on
 * every data commit. `catalogEpoch` moves on every catalog mutation — table creation, table
 * record updates (schema migration, full-text index stamps), and every manifest publish —
 * so an unchanged epoch proves cached catalog state is byte-identical to a fresh read.
 * Physical garbage collection does not move the epoch: it only deletes records that are
 * already invisible at every leased version, so cached state stays result-equivalent.
 * `schemaEpoch` is deliberately narrower: it moves only for structural catalog changes, so
 * writers can reject old-schema artifacts without making concurrent ordinary writes conflict.
 */
export interface CatalogProbe {
  manifestVersion: number | null;
  catalogEpoch: number;
  /** Advances only when structural catalog state changes, never for an ordinary data commit. */
  schemaEpoch: number;
}

/** Optional serializable guard for a catalog mutation whose proof read multiple records. */
export interface CatalogMutationOptions {
  /** Reject unless the complete catalog is still at this epoch. */
  expectedCatalogEpoch?: number;
}

/** Atomic replacement fields for one catalog record. */
export interface TableRecordUpdate extends CatalogMutationOptions {
  columns?: TableColumnRecord[];
  /** Replaces the complete relationship catalog. */
  foreignKeys?: TableRecord["foreignKeys"];
  /** Replaces the full-text index state map; null clears it. */
  ftsColumns?: Record<string, FtsColumnIndexRecord> | null;
  /** Replaces the secondary-index state map; null clears it. */
  secondaryIndexes?: Record<string, SecondaryIndexRecord> | null;
  /** Additional manifest CAS used when publishing a UNIQUE build from a stable snapshot. */
  expectedManifestVersion?: { value: number | null };
  /** Atomically seeds one secondary UNIQUE membership namespace while activating enforcement. */
  uniqueKeySeed?: { namespaceId: string; keyTokens: readonly string[] };
  /** Atomically advances a newly declared auto-increment column's durable counter. */
  autoIncrementSeed?: { columnId: string; atLeast: bigint };
  /** Replaces the trigger list; null clears it. */
  triggers?: TriggerRecord[] | null;
  /** Replaces a view definition in place; null converts the record back to a table. */
  view?: TableRecord["view"] | null;
}

/**
 * Bulk payload storage: immutable, opaque byte blobs keyed by structured ids.
 *
 * Blocks are written only through transaction staging, which creates their durable provenance
 * in the same atomic step and rejects duplicate ids without a partial write. Ids contain `/` separators
 * (`table/<uuid>/segment/<uuid>/part/000001`) and sort lexically; treat them as opaque keys.
 * Reads MUST return bytes the caller may mutate freely (a fresh copy or freshly deserialized
 * buffer), and writes MUST NOT alias the caller's buffer — the engine may reuse it.
 *
 * Published blocks are retired by superseding them in a commit; only the store's atomic
 * lease-aware collection step can physically delete payloads once no reader can reach them.
 */
/** Maximum manifest-membership ids accepted by one storage call. */
export const MAX_MANIFEST_BLOCK_PRESENCE_IDS = 1_024;
/** Maximum UTF-16 code units in any opaque storage identity. */
export const MAX_STORAGE_ID_CHARACTERS = 1_024;
/** Maximum UTF-16 code units in a persisted catalog name. */
export const MAX_CATALOG_NAME_CHARACTERS = 1_024;
/** Public adapter database-name bound; names commonly become substrate keys or path segments. */
export const MAX_STORAGE_DATABASE_NAME_CHARACTERS = 256;
/** Maximum positional items accepted by one public bulk-read operation. */
export const MAX_STORAGE_BULK_READ_ITEMS = 1_024;
/** Maximum aggregate payload bytes returned by one getBlocks call. */
export const MAX_BLOCK_READ_BATCH_BYTES = MAX_STORED_BLOCK_BYTE_LENGTH;

export class BlockReadBatchTooLargeError extends RangeError {
  override readonly name = "BlockReadBatchTooLargeError";

  constructor(
    readonly requestedBytes: number,
    readonly limitBytes = MAX_BLOCK_READ_BATCH_BYTES,
  ) {
    super(
      `Block read batch requires ${String(requestedBytes)} bytes; limit is ${String(limitBytes)}`,
    );
  }
}

export function assertStorageBulkReadItems(items: readonly unknown[], label: string): void {
  if (items.length > MAX_STORAGE_BULK_READ_ITEMS) {
    throw new RangeError(`${label} cannot exceed ${String(MAX_STORAGE_BULK_READ_ITEMS)} items`);
  }
}

export function validateStorageId(value: unknown, label = "Storage ID"): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${label} cannot be empty`);
  }
  if (value.length > MAX_STORAGE_ID_CHARACTERS) {
    throw new TypeError(`${label} exceeds ${String(MAX_STORAGE_ID_CHARACTERS)} characters`);
  }
  assertWellFormedString(value, label);
  return value;
}

export function validateCatalogName(value: unknown, label = "Catalog name"): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${label} cannot be empty`);
  }
  if (value.length > MAX_CATALOG_NAME_CHARACTERS) {
    throw new TypeError(`${label} exceeds ${String(MAX_CATALOG_NAME_CHARACTERS)} characters`);
  }
  assertWellFormedString(value, label);
  return value;
}

export function validateStorageDatabaseName(value: unknown): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError("Storage database name cannot be empty");
  }
  if (value.length > MAX_STORAGE_DATABASE_NAME_CHARACTERS) {
    throw new TypeError(
      `Storage database name exceeds ${String(MAX_STORAGE_DATABASE_NAME_CHARACTERS)} characters`,
    );
  }
  assertWellFormedString(value, "Storage database name");
  return value;
}

export interface BlockPayloadStore {
  getBlock(id: string): Promise<Uint8Array | undefined>;
  /** Positional per requested id; undefined where a block does not exist. */
  getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>>;
  /**
   * Reads one block only when it belongs to the exact readable manifest version. The membership
   * check and payload read are one storage operation: callers must never fetch a block first and
   * then race a separate manifest check. A missing/pruned manifest or non-member id returns
   * undefined. A readable member whose payload is missing or unreadable is corruption and MUST
   * throw StorageCorruptionError. Reader leases keep a selected version from being pruned while
   * it is in use.
   */
  readManifestBlock(version: number | null, id: string): Promise<Uint8Array | undefined>;
  /**
   * Positional membership in the exact readable manifest version. Implementations must bound the
   * input at MAX_MANIFEST_BLOCK_PRESENCE_IDS; callers window larger sets. A missing/pruned
   * manifest returns false for every id.
   */
  hasManifestBlocks(version: number | null, ids: readonly string[]): Promise<boolean[]>;
}

/**
 * The table catalog: schema records, the counters that keep writes collision-free, and
 * unique-key membership.
 *
 * Table mutations are compare-and-swap on the record's `revision` and MUST fail with
 * `TableRecordConflictError` — the exact exported class — on a mismatch. Catalog mutations
 * advance the catalog epoch (see `CatalogProbe`). Counter reservations (`reserveRowIds`,
 * `reserveAutoIncrement`) MUST be atomic and durable: two racing callers may never receive
 * overlapping ranges, across connections and across crashes. Reservations are never returned;
 * aborted transactions leave gaps.
 */
export interface CatalogStore {
  /** Fails on a duplicate id or name. Advances the catalog epoch. */
  addTable(record: TableRecord, options?: CatalogMutationOptions): Promise<void>;
  getTable(id: string): Promise<TableRecord | undefined>;
  getTableByName(name: string): Promise<TableRecord | undefined>;
  /** Sorted by table name; MAX_CATALOG_RECORDS makes this array globally bounded. */
  listTables(): Promise<TableRecord[]>;
  /**
   * Replaces catalog metadata atomically. When `columns` removes a column, the same operation
   * must also discard that column's full-text catalog entry, base chunks, and commit deltas.
   */
  updateTable(
    id: string,
    expectedRevision: number,
    update: TableRecordUpdate,
  ): Promise<TableRecord>;
  /**
   * Removes a segment-free catalog object such as a view. Must refuse when any segment names the
   * record; populated tables use `dropTable`, whose manifest retirement and catalog removal are
   * inseparable. Advances the catalog epoch and fails with `TableRecordConflictError` on a stale
   * revision.
   */
  removeTable(
    id: string,
    expectedRevision: number,
    options?: CatalogMutationOptions,
  ): Promise<void>;
  /**
   * Atomically retires all live table blocks, removes the catalog/segments/counters/indexes,
   * and publishes the successor manifest. A table revision mismatch throws
   * `TableRecordConflictError`; a manifest mismatch throws `WriteConflictError`; an active
   * transaction or nonterminal compaction owner throws `TableInUseError`. Any rejection leaves
   * the catalog, manifest, jobs, transaction journals, segments, counters, indexes, and payloads
   * unchanged. A success returns the one manifest summary it published.
   */
  dropTable(input: DropTableInput): Promise<ManifestSummary>;
  /**
   * Atomically removes one safe non-key column from catalog metadata and every table segment,
   * removes its posting accelerator, and publishes a successor retiring only removed column
   * blocks that no remaining segment reference owns. CAS/busy errors match `dropTable`; any
   * rejection changes nothing, payload bytes stay available to pinned historical readers, and
   * success returns the exactly one published manifest summary.
   */
  dropTableColumn(input: DropTableColumnInput): Promise<ManifestSummary>;
  reserveRowIds(tableId: string, count: number): Promise<RowIdRange>;
  /**
   * Atomically reserves `count` auto-increment values for the column, first bumping the
   * counter to at least `atLeast`. `count` may be 0 for a pure bump past explicit values.
   */
  reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast?: bigint,
  ): Promise<RowIdRange>;
  /** Which of the given key tokens already exist for the table, deduplicated and sorted. */
  getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]>;
  /**
   * Starts or takes over the exact catalog-owned UNIQUE builder. One active session per namespace
   * and MAX_ACTIVE_UNIQUE_KEY_BUILDS globally are enforced atomically. Repeating the same active
   * owner/header is idempotent; an expired owner is irrevocably replaced with an empty stage.
   */
  beginUniqueKeyBuild(input: BeginUniqueKeyBuildInput): Promise<UniqueKeyBuildRecord>;
  getUniqueKeyBuild(buildId: string): Promise<UniqueKeyBuildRecord | undefined>;
  /** Refuses at/past the cutoff, so a dead builder can never be resurrected. */
  renewUniqueKeyBuild(input: RenewUniqueKeyBuildInput): Promise<UniqueKeyBuildRecord>;
  /**
   * Appends the exact next ordinal. Same-ordinal lost-ack replay compares the persisted tokens
   * byte-for-byte; a changed replay or duplicate within/across chunks refuses without mutation.
   */
  appendUniqueKeyBuildChunk(input: AppendUniqueKeyBuildChunkInput): Promise<UniqueKeyBuildRecord>;
  /**
   * Atomically verifies the exact building catalog owner, table+manifest CAS, completed chunk
   * count, and then publishes both the durable membership namespace and ready catalog state.
   * A same-build completed retry is a no-op; no catalog/membership intermediate state exists.
   */
  finishUniqueKeyBuild(input: FinishUniqueKeyBuildInput): Promise<TableRecord>;
  /** Exact owner may abort; after expiry any caller may reclaim. Returns false when absent. */
  abortUniqueKeyBuild(input: AbortUniqueKeyBuildInput): Promise<boolean>;
  /** Starts or takes over a bounded base+tail membership fold; see `UniqueKeyFoldRecord`. */
  beginUniqueKeyFold?(input: BeginUniqueKeyFoldInput): Promise<UniqueKeyFoldRecord>;
  getUniqueKeyFold?(foldId: string): Promise<UniqueKeyFoldRecord | undefined>;
  /** Advances at most `maxTokens` lexical keys and renews the exact live owner. */
  runUniqueKeyFoldStep?(input: RunUniqueKeyFoldStepInput): Promise<UniqueKeyFoldRecord>;
  /** Atomically promotes the output generation and removes only the frozen delta prefix. */
  finishUniqueKeyFold?(input: FinishUniqueKeyFoldInput): Promise<UniqueKeyFoldRecord>;
  abortUniqueKeyFold?(input: AbortUniqueKeyFoldInput): Promise<boolean>;
}

/**
 * Versions and visibility: manifests (the set of live block ids at each version), segments
 * (which blocks belong to which table and rows), and the transaction records that stage and
 * publish them.
 *
 * This is where the whole consistency story lives. `commitTransaction` is THE atomic step of
 * the database: in one durable, all-or-nothing action it validates the transaction record's
 * revision and active status, verifies its structural `schemaEpochGuard`, compare-and-swaps the
 * current manifest version, publishes the
 * next manifest, finalizes the transaction's segments, applies unique-key changes (failing
 * with `UniqueKeyConflictError` on a `requireAbsent` violation), applies full-text deltas,
 * and flips the transaction record to committed. No intermediate state may ever be
 * observable, including after a crash at any moment. Schema conflicts MUST be
 * `SchemaConflictError`, version conflicts `WriteConflictError`, and revision conflicts
 * `TransactionRecordConflictError` — the exact
 * exported classes; the engine's retry and rebase loops match on them, and the worker client
 * rehydrates them by name across the thread boundary.
 */
export interface TransactionStore {
  getCurrentManifest(): Promise<Manifest | undefined>;
  /** The current version alone, without materializing the manifest's block list. */
  getCurrentManifestVersion(): Promise<number | null>;
  getManifest(version: number): Promise<Manifest | undefined>;
  /**
   * Exact ID-ordered membership at one historically published version. Implementations resolve
   * this from interval provenance without reconstructing or cloning a complete manifest. It must
   * remain pageable after the summary tombstone is removed so bounded maintenance can drain the
   * retired payload interval; callers separately use `getManifest`/leases to authorize reads.
   * A version newer than the current database returns an empty page. `limit` is storage-bounded.
   */
  listManifestBlockPage(input: ListManifestBlockPageInput): Promise<ManifestBlockPage>;
  /**
   * ID-ordered retired provenance, independent of bounded manifest-summary tombstones. This is
   * the durable garbage-discovery index: every record whose non-null `removedVersion` is at most
   * `removedThroughVersion` appears exactly once in a full cursor traversal.
   */
  listRetiredManifestBlockPage(
    input: ListRetiredManifestBlockPageInput,
  ): Promise<ManifestBlockPage>;
  listManifestPage(
    afterVersion: number | null,
    limit: number,
  ): Promise<StoragePage<Manifest, number>>;
  /**
   * Fails on a duplicate id; the record's snapshot version and pending ids must be valid. When
   * an active record omits `schemaEpochGuard`, the store stamps its current structural epoch in
   * the same atomic creation step. A supplied stale guard throws `SchemaConflictError`.
   */
  createTransaction(record: TransactionRecord): Promise<void>;
  getTransaction(id: string): Promise<TransactionRecord | undefined>;
  /** Positional per requested id; undefined where a record does not exist. */
  getTransactions(ids: readonly string[]): Promise<Array<TransactionRecord | undefined>>;
  listTransactionPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<TransactionRecord, string>>;
  /**
   * Compare-and-swap on `expectedRevision` (`TransactionRecordConflictError` on a mismatch).
   * Only active transactions may be updated, and only `commitTransaction` may mark one
   * committed.
   */
  updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord>;
  /**
   * Extends a matching active writer's durable deadline without changing its data revision.
   * Returns false after ownership is lost or the transaction becomes terminal.
   */
  renewTransaction(input: RenewTransactionInput): Promise<boolean>;
  /**
   * Atomically aborts an active transaction only when both its owner still matches and its
   * durable deadline is at or before the cutoff. A concurrent renewal wins by returning
   * undefined; a successful abort preserves artifact provenance and advances the data revision.
   */
  abortTransactionIfExpired(
    input: AbortTransactionIfExpiredInput,
  ): Promise<TransactionRecord | undefined>;
  /** Publishes the next version; the summary omits the block list, which commits never need. */
  commitTransaction(input: CommitTransactionInput): Promise<ManifestSummary>;
  getSegment(id: string): Promise<SegmentRecord | undefined>;
  /** Sorted by id, after the exclusive cursor; bounded for maintenance scans. */
  listSegmentPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<SegmentRecord, string>>;
  /** Table-indexed ID page; query/compaction paths never scan unrelated segment history. */
  listTableSegmentPage(
    tableId: string,
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<SegmentRecord, string>>;
  /**
   * Removes an unpublished segment left by the exact aborted owner and unjournals it from that
   * transaction in the same atomic step. Returns false when the segment is already absent.
   * Any owner mismatch, non-aborted/missing owner, readable-manifest block reference, active
   * transaction pending-segment reference, or nonterminal compaction reference rejects without
   * mutation. A block alias in another journal is safe: this operation deletes segment metadata,
   * never payload bytes.
   */
  removeAbortedSegment(id: string, expectedTransactionId: string): Promise<boolean>;
  /** See `AdoptAbortedSegmentInput`; returns the updated replacement transaction journal. */
  adoptAbortedSegment(input: AdoptAbortedSegmentInput): Promise<TransactionRecord>;
}

/**
 * Reader pins. A lease is a stored record with an expiry that protects one manifest version
 * (and every block it references) from garbage collection while a reader may still be using
 * it. Renewals are compare-and-swap on `revision` and fail with `LeaseConflictError`; an
 * expired lease simply stops protecting — there is no callback, which is what makes dead
 * tabs safe.
 */
export interface LeaseStore {
  /**
   * Fails on a duplicate id or unavailable manifest. Before enforcing MAX_ACTIVE_LEASES, an
   * adapter sweeps a bounded expired page; creation and the final count check are atomic.
   */
  createLease(record: LeaseRecord): Promise<void>;
  getLease(id: string): Promise<LeaseRecord | undefined>;
  /** Sorted by id; MAX_ACTIVE_LEASES makes this array globally bounded. */
  listLeases(): Promise<LeaseRecord[]>;
  /** Bounded expiry/id-ordered page containing only records expired at the fixed cutoff. */
  listExpiredLeasePage(
    expiresAtCutoff: string,
    afterCursor: string | null,
    limit: number,
  ): Promise<StoragePage<LeaseRecord, string>>;
  renewLease(input: RenewLeaseInput): Promise<LeaseRecord>;
  /** True when removed; false (without removing) when the lease has not yet expired. */
  removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean>;
  /**
   * Releases an existing lease only for its durable owner. A concurrent same-owner renewal may
   * still be removed safely; a different owner throws `LeaseOwnerConflictError` without change.
   * Returns false when the lease is already absent.
   */
  removeLease(input: { id: string; ownerId: string }): Promise<boolean>;
}

/**
 * Background-maintenance bookkeeping: the resumable job records that let compaction and
 * garbage collection survive a tab being closed, throttled, or killed mid-step. Job updates
 * are compare-and-swap on `revision` and fail with `CompactionJobConflictError` /
 * `GarbageCollectionJobConflictError`.
 *
 * `runGarbageCollectionStep` does real deletion and MUST be atomic: it re-verifies lease and
 * transaction pins inside the same storage transaction that prunes manifests and deletes
 * segments and blocks, advancing the job's cursors so an interrupted collection resumes
 * rather than restarts. Reclaimed manifests are first tombstoned (`prunedAt`);
 * `removePrunedManifestRecords` may then delete only the old prefix no readable delta chain
 * needs.
 */
export interface MaintenanceStore {
  /**
   * Atomically enforces one nonterminal job per table and MAX_ACTIVE_COMPACTION_JOBS globally;
   * terminal history does not count and is pruned separately.
   */
  createCompactionJob(record: CompactionJobRecord): Promise<void>;
  getCompactionJob(id: string): Promise<CompactionJobRecord | undefined>;
  /**
   * Sorted by createdAt, then id; `tableId` filters. The returned array is bounded by the
   * enforced active and terminal compaction-job record quotas above.
   */
  listCompactionJobs(tableId?: string): Promise<CompactionJobRecord[]>;
  listCompactionJobPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<CompactionJobRecord, string>>;
  updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ): Promise<CompactionJobRecord>;
  /**
   * Resolves a job that may be racing its own publication: already-terminal jobs return
   * unchanged, a job whose transaction committed is marked published, anything else is
   * cancelled and its active transaction aborted — atomically.
   */
  cancelCompactionJob(
    id: string,
    expectedRevision: number,
    cancelledAt: string,
  ): Promise<CompactionJobRecord>;
  /**
   * Removes only published, cancelled, or aborted diagnostics. Returns false when missing or
   * when an existing source/output block or segment would lose its last durable provenance/root;
   * collection can remove that payload first. Nonterminal removal throws without mutation.
   */
  removeCompactionJob(id: string): Promise<boolean>;
  /** Validates candidate provenance against persisted records before accepting the job. */
  createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord>;
  /** CAS-appends one bounded discovery page; no payload or catalog data is changed. */
  updateGarbageCollectionPlanning(
    input: UpdateGarbageCollectionPlanningInput,
  ): Promise<GarbageCollectionJobRecord>;
  getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined>;
  /**
   * Sorted by createdAt, then id. The returned array is bounded by the enforced active and
   * completed garbage-collection record quotas above.
   */
  listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]>;
  /** Bounded page sorted by id; maintenance paths use this instead of materializing history. */
  listGarbageCollectionJobPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<GarbageCollectionJobRecord, string>>;
  runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult>;
  /**
   * Removes at most `maxItems` obsolete summary tombstones while retaining the checkpoint prefix
   * readable deltas need. Retired block provenance, not the summary, is durable garbage
   * discovery; `runGarbageCollectionStep` removes each payload and its provenance atomically, so
   * deleting a summary cannot strand an undiscoverable block.
   */
  removePrunedManifestRecords(maxItems: number): Promise<number>;
  /** Removes only a completed diagnostic; planned/running collection is refused. */
  removeGarbageCollectionJob(id: string): Promise<void>;
}

/**
 * Full-text index persistence: per-column base chunks plus the per-commit deltas that
 * `commitTransaction` applies. The index is a pruning accelerator the scan re-verifies, so
 * losing a base costs a rebuild, never a wrong answer — which is why snapshots may restore
 * indexed columns as `invalid`.
 */
export interface FtsIndexStore {
  /** Removes every base chunk and commit delta owned by one column. */
  /**
   * Removes posting storage only when the current catalog no longer owns it as a ready/building
   * accelerator (absent or invalid is safe). Refusal leaves catalog and postings unchanged.
   */
  removeFtsColumn(tableId: string, columnId: string): Promise<void>;
  /**
   * Replaces one column's full-text base chunks (term-range partitioned, term-sorted within
   * each chunk) and deletes commit deltas the new base covers. The caller flips the catalog
   * state separately via updateTable; orphaned chunks from a lost race are overwritten by the
   * next build.
   */
  writeFtsBase(
    tableId: string,
    columnId: string,
    input: { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number },
  ): Promise<void>;
  /**
   * Per-term candidate row IDs from the base chunks plus every commit delta at or below
   * `upToVersion`, with the column's merged token total for exact BM25 statistics. Prefix
   * terms match the term range [term, term + "\uffff"). Reports the merged delta-chunk count
   * so callers can schedule a rebuild when the tail grows, whether a published base exists,
   * and the base's covered version —
   * a concurrent rebuild can publish a base ahead of a reader's snapshot, and a caller
   * needing snapshot-exact statistics must detect `coversVersion > upToVersion` and fall
   * back (candidates stay a safe superset either way).
   */
  readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: readonly FtsPostingQuery[],
    upToVersion: number,
    maxRowIds?: number,
  ): Promise<
    FtsCandidates & {
      deltaChunkCount: number;
      totalTokens: number;
      coversVersion: number;
      hasBase: boolean;
    }
  >;
  /**
   * Canonical term/posting order from the same snapshot-bounded base-plus-delta view. This is a
   * materialized cold path for ordered and covering scans; callers must not retain the result.
   */
  readFtsPostings(
    tableId: string,
    columnId: string,
    upToVersion: number,
    maxRowIds?: number,
    maxRetainedBytes?: number,
  ): Promise<{
    postings: FtsPosting[];
    /** The bounded merge stopped early; callers must scan and may rebuild the accelerator. */
    overflow: boolean;
    deltaChunkCount: number;
    coversVersion: number;
    hasBase: boolean;
  }>;
}

/**
 * Query spill: scratch pages a bounded-memory query writes when it exceeds its budget, plus
 * the owner leases that let any connection reclaim a dead owner's pages. Pages carry no
 * durability requirement whatsoever — losing them costs a query, never data — but owner
 * records are real records with the usual compare-and-swap (`TempOwnerConflictError`).
 */
export interface TempSpillStore {
  /** Atomically enforces the per-owner run/page ceilings before storing payload bytes. */
  putTempRunPage(page: TempRunPage): Promise<void>;
  /**
   * Optional: writes a batch of pages in one storage round trip. Callers fall back to
   * per-page writes when absent; implement it where per-call overhead is real (the IndexedDB
   * adapter pays one transaction per page otherwise).
   */
  putTempRunPages?(pages: readonly TempRunPage[]): Promise<void>;
  getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined>;
  removeTempRun(ownerId: string, runId: string): Promise<void>;
  /** Removes the owner record and every page under the owner. */
  removeTempOwner(ownerId: string): Promise<void>;
  /** Sweeps a bounded expired page before atomically enforcing MAX_ACTIVE_TEMP_OWNERS. */
  createTempOwner(record: TempOwnerRecord): Promise<void>;
  getTempOwner(ownerId: string): Promise<TempOwnerRecord | undefined>;
  renewTempOwner(input: RenewTempOwnerInput): Promise<TempOwnerRecord>;
  /** Sweeps pages too when it removes; owners found only via orphaned pages count as expired. */
  removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): Promise<boolean>;
  /** Owner ids from records and from orphaned pages alike, deduplicated, sorted, paged. */
  listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>>;
  /** Actual expired candidates only, ordered by expiry then owner id. */
  listExpiredTempOwnerPage(
    expiresAtCutoff: string,
    afterCursor: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>>;
}

/**
 * The complete storage contract: a database is `MinnowDatabase` plus one implementation of
 * this interface. The engine holds exactly one and talks to nothing else persistent, so
 * implementing it against a new substrate — React Native storage, an object store like R2,
 * the Node filesystem — yields a working database with no engine changes. The capability
 * interfaces above split the surface by concern; implement all of them (this type), and see
 * `/docs/storage/custom` for the guide and `runBlockStoreConformance` from
 * `@minnowdb/core/testing` for the referee.
 *
 * The rules every implementation must honor — the conformance kit checks each of them:
 *
 * - **Atomicity.** Every method is all-or-nothing, including after a crash at any moment.
 *   `commitTransaction` and `runGarbageCollectionStep` mutate several record families in one
 *   durable step. A local adapter method that resolves has happened; one that rejects has not
 *   happened. The narrow transport-loss exception is an OPFS follower call that throws
 *   `OpfsUncertainOutcomeError`: its prior leader may have committed the named mutation before
 *   losing the reply, so callers must inspect/reopen instead of blindly repeating it.
 * - **Conflicts are typed, by exact class.** Compare-and-swap failures throw the exported
 *   error classes (`WriteConflictError`, `TransactionRecordConflictError`,
 *   `TableRecordConflictError`, `LeaseConflictError`, `CompactionJobConflictError`,
 *   `GarbageCollectionJobConflictError`, `TempOwnerConflictError`, `UniqueKeyConflictError`,
 *   `SnapshotManifestMissingError`) — not subclasses, not wrappers. The engine's rebase loops
 *   match on them and the worker client rehydrates them by constructor name.
 * - **Platform errors pass through.** A quota refusal must escape as the browser's own
 *   `QuotaExceededError` `DOMException`, unwrapped, with everything committed beforehand
 *   intact and the same write succeeding once space frees — no reopen, no repair step.
 * - **Nothing is shared.** Returned records and bytes must be safe for the caller to mutate;
 *   received records and bytes must be copied or serialized before the call resolves.
 * - **Deterministic ordering.** List methods sort as documented on each capability interface;
 *   pagination cursors are stable under concurrent writes.
 * - **Optional means atomic.** Optional accelerator methods exist so an adapter that can do
 *   something in one atomic step may say so; callers trust a present method completely and fall
 *   back to safe sequential calls when it is absent. Never implement one as sequential calls in
 *   a trench coat.
 * - **Multiple connections are normal.** Several instances (tabs) may open one database.
 *   Readers must never block writers; competing writers must resolve through the typed
 *   conflicts. How is the adapter's business — storage transactions, a write-ahead log behind
 *   a leader, anything that keeps these rules true.
 * - **Records carry no adapter fields.** These types are the whole vocabulary between engine
 *   and store. Anything an adapter needs to remember about its own layout — key partitioning,
 *   format generations, file placements — lives in the adapter's own storage space, keyed
 *   however it likes, never as extra fields on the records it hands back.
 * - **Bigints are data.** Row ids, counters, and full-text posting ids are `bigint`; an
 *   adapter that serializes records needs an encoding for them.
 */
export interface BlockStore
  extends
    BlockPayloadStore,
    CatalogStore,
    TransactionStore,
    LeaseStore,
    MaintenanceStore,
    FtsIndexStore,
    TempSpillStore {
  /**
   * Bounded builder for a postings base. Chunks stage under `buildId`; finish swaps the complete
   * generation into view atomically and prunes covered deltas. Beginning another build for the
   * same physical column reclaims an abandoned generation, so tab death cannot leak one
   * generation per retry. Required because an index build must never materialize one
   * database-sized storage value as a fallback.
   */
  beginFtsBaseBuild(input: BeginPostingBuildInput): Promise<void>;
  renewFtsBaseBuild(input: RenewPostingBuildInput): Promise<void>;
  writeFtsBaseBuildChunk(input: AppendPostingBuildChunkInput): Promise<void>;
  finishFtsBaseBuild(input: FinishPostingBuildInput): Promise<void>;
  abortFtsBaseBuild(input: AbortPostingBuildInput): Promise<void>;
  /**
   * The current manifest version, catalog epoch, and structural schema epoch in one atomic read.
   * An unchanged manifest/catalog pair proves cached query state is current; the schema epoch is
   * the narrower write-serialization guard and does not move for ordinary commits.
   */
  getCatalogProbe(): Promise<CatalogProbe>;
  /**
   * Optional atomic query-catalog read. The returned tables, segments, transaction owners, and
   * probe epoch must come from one storage snapshot; engines use the sequential stable-epoch
   * fallback when an adapter does not provide it.
   */
  getQueryCatalogState?(names: readonly string[]): Promise<QueryCatalogState>;
  /**
   * Reads the current manifest and schema epoch, creates the transaction record pinned to both,
   * and optionally reserves row ids or an invisible pending table in one atomic storage
   * transaction. Required because these reservations cannot be emulated safely by sequential
   * calls.
   */
  beginTransaction(input: BeginTransactionInput): Promise<BeginTransactionResult>;
  /**
   * Required atomic staging boundary: saves blocks and segments and appends their ids to the
   * active transaction journal in one durable operation. It is equivalent to addBlocks +
   * addSegment(s) + one updateTransaction only when those changes commit atomically; there is no
   * sequential fallback because a crash could otherwise strand bytes or publish a dangling id.
   * A local refusal has no effect; the documented OPFS follower uncertain-outcome error means
   * the complete operation may have happened and must never be converted to an ordinary error.
   */
  stageTransactionArtifacts(input: StageTransactionArtifactsInput): Promise<TransactionRecord>;
  /**
   * Required atomic savepoint rewind. Compare-and-swaps the journal, validates retained and
   * removed ids as its exact duplicate-free partition, and removes only the unreachable removed
   * artifacts in the same durable operation. A refusal leaves both journal and bytes unchanged.
   */
  rollbackTransactionArtifacts(
    input: RollbackTransactionArtifactsInput,
  ): Promise<TransactionRecord>;
  /**
   * Optional: the single-shot write — begin (or continue) a transaction, stage its blocks and
   * segments, and commit, all in one atomic storage transaction. Must be exactly equivalent to
   * `stageTransactionArtifacts` followed by `commitTransaction` (preceded by `createTransaction`
   * at `expectedManifestVersion` when the input carries a fresh record): the same validation,
   * the same typed conflicts (`SchemaConflictError`, `WriteConflictError`, `TransactionRecordConflictError`,
   * `UniqueKeyConflictError`), the same finalized records afterwards — and nothing at all
   * written when any part refuses, including the fresh record. This is what lets a simple
   * write cost one durable storage commit instead of three; callers fall back to the sequence
   * when it is absent.
   */
  writeTransaction?(input: WriteTransactionInput): Promise<ManifestSummary>;
  /**
   * Optional: re-pins a lease to another manifest version and renews it, in one atomic step —
   * `createLease` at the new version plus `removeLease` of the old pin, as one round trip that
   * keeps the record and its id. Compare-and-swap on `expectedRevision` (`LeaseConflictError`);
   * the target version's manifest must be available (`SnapshotManifestMissingError`), and a refused
   * move leaves the lease exactly as it was. An already-expired pin cannot be moved or renewed,
   * and the new expiry is capped by `MAX_LEASE_TTL_MS`. The engine uses it to carry its shared
   * reader pin forward after each commit; callers fall back to create + remove when absent.
   */
  moveLease?(input: MoveLeaseInput): Promise<LeaseRecord>;
  /**
   * Native bounded snapshot v1. Built-in adapters implement this complete family together.
   * Export reads exactly one globally ordered frame at a time from a durable frozen generation;
   * close releases that generation and its lease. No method returns a database-sized array.
   */
  beginSnapshotFrameExport?(
    input: BeginSnapshotFrameExportInput,
  ): Promise<SnapshotFrameExportSession>;
  readSnapshotExportFrame?(input: ReadSnapshotExportFrameInput): Promise<SnapshotFrame | undefined>;
  closeSnapshotFrameExport?(input: CloseSnapshotExportInput): Promise<boolean>;
  /**
   * Native bounded import. Append atomically accepts only contiguous bounded frames and exact
   * lost-ack replay bytes. Finish validates header/footer counts, order, checksums, complete
   * catalog/segment/transaction/index cross-references, and atomically promotes all staged
   * generations; rejection leaves the current database unchanged.
   */
  beginSnapshotFrameImport?(
    input: BeginSnapshotFrameImportInput,
  ): Promise<SnapshotFrameImportSession>;
  renewSnapshotFrameImport?(
    input: RenewSnapshotFrameImportInput,
  ): Promise<SnapshotFrameImportSession>;
  appendSnapshotImportFrames?(
    input: AppendSnapshotImportFramesInput,
  ): Promise<SnapshotFrameImportSession>;
  finishSnapshotFrameImport?(input: FinishSnapshotFrameImportInput): Promise<void>;
  cancelSnapshotFrameImport?(
    input: CancelSnapshotImportInput,
  ): Promise<InterruptedSnapshotImportAbortResult>;
  /** Bounded, fail-closed verification of control records and optionally every live block. */
  checkIntegrity?(options?: {
    mode?: StorageIntegrityMode;
    maxIssues?: number;
  }): Promise<StorageIntegrityReport>;
  /** Explicit storage accounting; adapters do not run this on query or write hot paths. */
  getStorageStats?(): Promise<StorageStats>;
  /** Returns the durable marker for an unpublished snapshot import, if this adapter has one. */
  inspectInterruptedImport?(): Promise<InterruptedSnapshotImport | null>;
  /** Atomically abandons the named unpublished import and removes all staged state. */
  abortInterruptedImport?(identity: string): Promise<InterruptedSnapshotImportAbortResult>;
  /**
   * Optional: what this database's data occupies in its substrate, in bytes — the number an
   * application shows a user next to the quota, and what the benchmarks report.
   */
  getLogicalStorageBytes?(): Promise<number>;
  /**
   * Optional deterministic channel for commit hints between connections to the same durable
   * database. The engine creates the BroadcastChannel; stores only expose identity.
   */
  readonly liveQueryChannelName?: string;
  /**
   * Releases whatever the connection holds (open handles, channels, timers) without flushing
   * or deleting anything. Synchronous; safe to call twice. Data durability must never depend
   * on close being called — tabs die without warning.
   */
  close(): void;
}

export function createManifest(input: CreateManifestInput): Manifest {
  return {
    version:
      input.expectedVersion === null ? 0 : safeSum([input.expectedVersion, 1], "Manifest version"),
    previousVersion: input.expectedVersion,
    liveBlockCount: nonNegativeWholeNumber(input.liveBlockCount, "Manifest live block count"),
    liveBlockBytes: nonNegativeWholeNumber(input.liveBlockBytes, "Manifest live block bytes"),
    createdAt: input.createdAt ?? dateIsoString(new Date()),
    changedTableIds: canonicalManifestChangedTableIds(input.changedTableIds),
  };
}

/** Strictly clones the one canonical v1 segment shape. */
export function normalizeSegmentRecord(record: SegmentRecord): SegmentRecord {
  switch (record.kind) {
    case "insert":
    case "upsert":
    case "update":
    case "delete":
    case "base":
      break;
    default:
      throw new TypeError("Segment kind is invalid");
  }
  const level = nonNegativeWholeNumber(record.level, "Segment level");
  if (level > 2) throw new RangeError("Segment level must be between zero and two");
  nonNegativeFiniteNumber(record.logicalOrder, "Segment logical order");
  nonNegativeWholeNumber(record.commitOrdinal, "Segment commit ordinal");
  if (!Array.isArray(record.rowIdSpans)) {
    throw new TypeError("Segment row ID spans must be an array");
  }
  if (record.partitionOrdinal === undefined) {
    if (level === 2) throw new TypeError("A level-two segment requires a partition ordinal");
    return structuredClone(record);
  }

  const partitionOrdinal = nonNegativeWholeNumber(
    record.partitionOrdinal,
    "Segment partition ordinal",
  );
  if (record.level !== 2) {
    throw new TypeError("A partitioned segment must have explicit level two");
  }
  const kind = record.kind;
  if (kind !== "insert" && kind !== "base") {
    throw new TypeError("A partitioned segment must be an insert or a merged base");
  }
  const rowCount = positiveWholeNumber(record.rowCount, "Segment row count");
  if (kind === "insert") {
    // Append-row-range partition: one contiguous positive row-ID interval, no spans.
    if (record.rowIdSpans.length !== 0) {
      throw new TypeError("A partitioned segment cannot contain row ID spans");
    }
    if (typeof record.rowIdStart !== "bigint" || record.rowIdStart <= 0n) {
      throw new RangeError("Segment row ID start must be a positive bigint");
    }
    if (
      typeof record.rowIdEndExclusive !== "bigint" ||
      record.rowIdEndExclusive !== record.rowIdStart + BigInt(rowCount)
    ) {
      throw new RangeError("A partitioned segment must have a contiguous positive row ID envelope");
    }
    return structuredClone({ ...record, partitionOrdinal });
  }
  // Keyed multi-range partition: a merged full-row base whose live rows keep their original
  // ids, described by positive, sorted, non-overlapping spans that sum to the row count.
  if (record.rowIdSpans.length === 0) {
    throw new TypeError("A merged partitioned segment requires row ID spans");
  }
  let spanRows = 0;
  let previousEnd = 0n;
  for (const span of record.rowIdSpans as readonly RowIdSpan[]) {
    if (
      typeof span.rowIdStart !== "bigint" ||
      span.rowIdStart <= 0n ||
      !Number.isSafeInteger(span.rowCount) ||
      span.rowCount <= 0
    ) {
      throw new RangeError("A partitioned segment span must be a positive non-empty interval");
    }
    if (span.rowIdStart < previousEnd) {
      throw new RangeError("Partitioned segment spans must be sorted and non-overlapping");
    }
    previousEnd = span.rowIdStart + BigInt(span.rowCount);
    spanRows += span.rowCount;
  }
  if (spanRows !== rowCount) {
    throw new RangeError("Partitioned segment spans must cover exactly the row count");
  }
  return structuredClone({ ...record, partitionOrdinal });
}

export function updateTransactionRecord(
  record: TransactionRecord,
  update: TransactionRecordUpdate,
): TransactionRecord {
  const updated: TransactionRecord = {
    ...record,
    ...(update.snapshotVersion === undefined ? {} : { snapshotVersion: update.snapshotVersion }),
    ...(update.pendingBlockIds === undefined
      ? {}
      : {
          pendingBlockIds: orderedUniqueIds(update.pendingBlockIds, "Transaction pending block ID"),
        }),
    ...(update.pendingSegmentIds === undefined
      ? {}
      : {
          pendingSegmentIds: orderedUniqueIds(
            update.pendingSegmentIds,
            "Transaction pending segment ID",
          ),
        }),
    ...(update.status === undefined ? {} : { status: update.status }),
    ...(update.committedVersion === undefined ? {} : { committedVersion: update.committedVersion }),
    ...(update.pendingTableNextRowId === undefined
      ? {}
      : { pendingTableNextRowId: update.pendingTableNextRowId }),
    updatedAt: update.updatedAt,
    revision: safeSum([record.revision, 1], "Transaction revision"),
  };
  if (updated.status !== "active") {
    delete updated.pendingTable;
    delete updated.pendingTableNextRowId;
    delete updated.catalogEpochGuard;
    delete updated.schemaEpochGuard;
  }
  return updated;
}

export function createGarbageCollectionJobRecord(
  input: CreateGarbageCollectionJobInput,
): GarbageCollectionJobRecord {
  const candidateManifestVersions = uniqueWholeNumbers(
    input.candidateManifestVersions,
    "Garbage collection candidate manifest version",
  );
  const candidateSegmentIds = uniqueIds(
    input.candidateSegmentIds,
    "Garbage collection candidate segment ID",
    true,
  );
  const candidateBlockIds = uniqueIds(
    input.candidateBlockIds,
    "Garbage collection candidate block ID",
    true,
  );
  const candidateTransactionIds = uniqueIds(
    input.candidateTransactionIds ?? [],
    "Garbage collection candidate transaction ID",
    true,
  );
  const createdAt = validTimestamp(input.createdAt, "Garbage collection creation timestamp");
  const discovery =
    input.discovery === undefined
      ? undefined
      : normalizeGarbageCollectionDiscovery(input.discovery);
  const complete =
    (discovery === undefined || discovery.phase === "complete") &&
    candidateManifestVersions.length === 0 &&
    candidateSegmentIds.length === 0 &&
    candidateBlockIds.length === 0 &&
    candidateTransactionIds.length === 0;
  return {
    id: validateStorageId(input.id, "Garbage collection job ID"),
    candidateManifestVersions,
    candidateSegmentIds,
    candidateBlockIds,
    candidateTransactionIds,
    cursor: { manifestIndex: 0, segmentIndex: 0, blockIndex: 0, transactionIndex: 0 },
    prunedManifestCount: 0,
    alreadyPrunedManifestCount: 0,
    retainedManifestCount: 0,
    missingManifestCount: 0,
    reclaimedSegmentCount: 0,
    retainedSegmentCount: 0,
    missingSegmentCount: 0,
    reclaimedBlockCount: 0,
    retainedBlockCount: 0,
    missingBlockCount: 0,
    reclaimedBlockBytes: 0,
    reclaimedTransactionCount: 0,
    retainedTransactionCount: 0,
    missingTransactionCount: 0,
    state: complete ? "completed" : "planned",
    revision: 0,
    leaseCutoff: validTimestamp(input.leaseCutoff, "Garbage collection lease cutoff"),
    createdAt,
    updatedAt: createdAt,
    ...(discovery === undefined ? {} : { discovery }),
  };
}

export function normalizeGarbageCollectionDiscovery(
  discovery: GarbageCollectionDiscovery,
): GarbageCollectionDiscovery {
  const runtime: unknown = discovery;
  if (typeof runtime !== "object" || runtime === null) {
    throw new TypeError("Garbage collection discovery must be an object");
  }
  const phase: unknown = (runtime as { phase?: unknown }).phase;
  if (
    phase !== "manifests" &&
    phase !== "manifest-blocks" &&
    phase !== "segments" &&
    phase !== "transactions" &&
    phase !== "compactions" &&
    phase !== "complete"
  ) {
    throw new TypeError(`Invalid garbage collection discovery phase: ${String(phase)}`);
  }
  const stringCursor = (value: unknown, name: string): string | null =>
    value === null ? null : validateStorageId(value, name);
  const currentManifestVersion =
    discovery.currentManifestVersion === null
      ? null
      : nonNegativeWholeNumber(
          discovery.currentManifestVersion,
          "Garbage collection discovery manifest version",
        );
  const manifestCursor =
    discovery.manifestCursor === null
      ? null
      : nonNegativeWholeNumber(
          discovery.manifestCursor,
          "Garbage collection manifest discovery cursor",
        );
  if (!Number.isSafeInteger(discovery.retainAboveVersion)) {
    throw new RangeError("Garbage collection retained version floor must be a safe integer");
  }
  if (!Number.isSafeInteger(discovery.retainAfter)) {
    throw new RangeError("Garbage collection retained timestamp floor must be a safe integer");
  }
  const resumePhase: unknown = (runtime as { resumePhase?: unknown }).resumePhase;
  if (
    resumePhase !== undefined &&
    resumePhase !== null &&
    resumePhase !== "manifests" &&
    resumePhase !== "manifest-blocks" &&
    resumePhase !== "segments" &&
    resumePhase !== "transactions" &&
    resumePhase !== "compactions"
  ) {
    throw new TypeError("Invalid garbage collection resume phase");
  }
  const postManifestPhase: unknown = (runtime as { postManifestPhase?: unknown }).postManifestPhase;
  if (
    postManifestPhase !== undefined &&
    postManifestPhase !== null &&
    postManifestPhase !== "manifest-blocks" &&
    postManifestPhase !== "segments" &&
    postManifestPhase !== "transactions" &&
    postManifestPhase !== "compactions"
  ) {
    throw new TypeError("Invalid garbage collection post-manifest phase");
  }
  const artifactCursor: unknown = (runtime as { artifactCursor?: unknown }).artifactCursor;
  let normalizedArtifactCursor: GarbageCollectionArtifactCursor | null | undefined;
  if (artifactCursor !== undefined && artifactCursor !== null) {
    if (typeof artifactCursor !== "object") {
      throw new TypeError("Garbage collection artifact cursor must be an object");
    }
    const rawArtifactCursor = artifactCursor as Record<string, unknown>;
    if (
      rawArtifactCursor.family !== "manifest" &&
      rawArtifactCursor.family !== "transaction" &&
      rawArtifactCursor.family !== "compaction"
    ) {
      throw new TypeError(
        `Invalid garbage collection artifact cursor family: ${String(rawArtifactCursor.family)}`,
      );
    }
    normalizedArtifactCursor = {
      family: rawArtifactCursor.family,
      recordId: validateStorageId(
        rawArtifactCursor.recordId,
        "Garbage collection artifact cursor record ID",
      ),
      blockId:
        rawArtifactCursor.blockId === null
          ? null
          : validateStorageId(
              rawArtifactCursor.blockId,
              "Garbage collection artifact cursor block ID",
            ),
      blockIndex: nonNegativeWholeNumber(
        rawArtifactCursor.blockIndex,
        "Garbage collection artifact block cursor",
      ),
      segmentIndex: nonNegativeWholeNumber(
        rawArtifactCursor.segmentIndex,
        "Garbage collection artifact segment cursor",
      ),
    };
  } else {
    normalizedArtifactCursor = artifactCursor;
  }
  return {
    phase,
    currentManifestVersion,
    retainAboveVersion: discovery.retainAboveVersion,
    retainAfter: discovery.retainAfter,
    maxPlanningItems: boundedMaintenanceBatchItems(
      discovery.maxPlanningItems,
      "Garbage collection planning item limit",
    ),
    manifestCursor,
    segmentCursor: stringCursor(
      discovery.segmentCursor,
      "Garbage collection segment discovery cursor",
    ),
    transactionCursor: stringCursor(
      discovery.transactionCursor,
      "Garbage collection transaction discovery cursor",
    ),
    compactionCursor: stringCursor(
      discovery.compactionCursor,
      "Garbage collection compaction discovery cursor",
    ),
    visitedRecords: nonNegativeWholeNumber(
      discovery.visitedRecords,
      "Garbage collection discovery visited records",
    ),
    ...(resumePhase === undefined ? {} : { resumePhase }),
    ...(postManifestPhase === undefined ? {} : { postManifestPhase }),
    ...(normalizedArtifactCursor === undefined ? {} : { artifactCursor: normalizedArtifactCursor }),
  };
}

export function updateGarbageCollectionPlanningRecord(
  record: GarbageCollectionJobRecord,
  input: UpdateGarbageCollectionPlanningInput,
): GarbageCollectionJobRecord {
  const current = normalizeGarbageCollectionJobRecord(record);
  if (current.id !== input.jobId || current.revision !== input.expectedRevision) {
    throw new GarbageCollectionJobConflictError(
      input.jobId,
      input.expectedRevision,
      current.id === input.jobId ? current.revision : null,
    );
  }
  if (current.state !== "planned" || current.discovery?.phase === "complete") {
    throw new TypeError("Only a discovering planned garbage collection job can be updated");
  }
  const appendIds = (
    existing: readonly string[],
    additions: readonly string[] | undefined,
    label: string,
  ): string[] => {
    const normalized = uniqueIds(additions ?? [], label, true);
    const seen = new Set(existing);
    for (const id of normalized) {
      if (seen.has(id)) throw new TypeError(`${label} is already planned: ${id}`);
      seen.add(id);
    }
    return [...existing, ...normalized];
  };
  const candidateManifestVersions = [...current.candidateManifestVersions];
  const manifestSet = new Set(candidateManifestVersions);
  for (const version of uniqueWholeNumbers(
    input.candidateManifestVersions ?? [],
    "Garbage collection candidate manifest version",
  )) {
    if (manifestSet.has(version)) {
      throw new TypeError(`Garbage collection manifest is already planned: ${String(version)}`);
    }
    manifestSet.add(version);
    candidateManifestVersions.push(version);
  }
  candidateManifestVersions.sort((left, right) => left - right);
  const candidateSegmentIds = appendIds(
    current.candidateSegmentIds,
    input.candidateSegmentIds,
    "Garbage collection candidate segment ID",
  );
  const candidateBlockIds = appendIds(
    current.candidateBlockIds,
    input.candidateBlockIds,
    "Garbage collection candidate block ID",
  );
  const candidateTransactionIds = appendIds(
    current.candidateTransactionIds,
    input.candidateTransactionIds,
    "Garbage collection candidate transaction ID",
  );
  const discovery = normalizeGarbageCollectionDiscovery(input.discovery);
  const fixedDiscovery = current.discovery;
  if (
    discovery.currentManifestVersion !== fixedDiscovery?.currentManifestVersion ||
    discovery.retainAboveVersion !== fixedDiscovery.retainAboveVersion ||
    discovery.retainAfter !== fixedDiscovery.retainAfter ||
    discovery.maxPlanningItems !== fixedDiscovery.maxPlanningItems
  ) {
    throw new TypeError(
      "Garbage collection discovery snapshot, retention boundary, and item limit are immutable",
    );
  }
  if (
    candidateManifestVersions.length +
      candidateSegmentIds.length +
      candidateBlockIds.length +
      candidateTransactionIds.length >
    discovery.maxPlanningItems
  ) {
    throw new RangeError("Garbage collection planning candidates exceed the persisted limit");
  }
  const updated: GarbageCollectionJobRecord = {
    ...current,
    candidateManifestVersions,
    candidateSegmentIds,
    candidateBlockIds,
    candidateTransactionIds,
    discovery,
    state: "planned",
    revision: safeSum([current.revision, 1], "Garbage collection job revision"),
    updatedAt: validTimestamp(input.updatedAt, "Garbage collection update timestamp"),
  };
  if (garbageCollectionJobComplete(updated)) updated.state = "completed";
  return normalizeGarbageCollectionJobRecord(updated);
}

export function normalizeGarbageCollectionJobRecord(
  record: GarbageCollectionJobRecord,
): GarbageCollectionJobRecord {
  const candidateManifestVersions = uniqueWholeNumbers(
    record.candidateManifestVersions,
    "Garbage collection candidate manifest version",
  );
  const candidateSegmentIds = uniqueIds(
    record.candidateSegmentIds,
    "Garbage collection candidate segment ID",
    true,
  );
  const candidateBlockIds = uniqueIds(
    record.candidateBlockIds,
    "Garbage collection candidate block ID",
    true,
  );
  const candidateTransactionIds = uniqueIds(
    record.candidateTransactionIds,
    "Garbage collection candidate transaction ID",
    true,
  );
  const cursor = normalizeGarbageCollectionCursor(record.cursor);
  const discovery =
    record.discovery === undefined
      ? undefined
      : normalizeGarbageCollectionDiscovery(record.discovery);
  const normalized: GarbageCollectionJobRecord = {
    ...record,
    id: validateStorageId(record.id, "Garbage collection job ID"),
    candidateManifestVersions,
    candidateSegmentIds,
    candidateBlockIds,
    candidateTransactionIds,
    cursor,
    prunedManifestCount: nonNegativeWholeNumber(
      record.prunedManifestCount,
      "Garbage collection pruned manifest count",
    ),
    alreadyPrunedManifestCount: nonNegativeWholeNumber(
      record.alreadyPrunedManifestCount,
      "Garbage collection already-pruned manifest count",
    ),
    retainedManifestCount: nonNegativeWholeNumber(
      record.retainedManifestCount,
      "Garbage collection retained manifest count",
    ),
    missingManifestCount: nonNegativeWholeNumber(
      record.missingManifestCount,
      "Garbage collection missing manifest count",
    ),
    reclaimedSegmentCount: nonNegativeWholeNumber(
      record.reclaimedSegmentCount,
      "Garbage collection reclaimed segment count",
    ),
    retainedSegmentCount: nonNegativeWholeNumber(
      record.retainedSegmentCount,
      "Garbage collection retained segment count",
    ),
    missingSegmentCount: nonNegativeWholeNumber(
      record.missingSegmentCount,
      "Garbage collection missing segment count",
    ),
    reclaimedBlockCount: nonNegativeWholeNumber(
      record.reclaimedBlockCount,
      "Garbage collection reclaimed block count",
    ),
    retainedBlockCount: nonNegativeWholeNumber(
      record.retainedBlockCount,
      "Garbage collection retained block count",
    ),
    missingBlockCount: nonNegativeWholeNumber(
      record.missingBlockCount,
      "Garbage collection missing block count",
    ),
    reclaimedBlockBytes: nonNegativeWholeNumber(
      record.reclaimedBlockBytes,
      "Garbage collection reclaimed block bytes",
    ),
    reclaimedTransactionCount: nonNegativeWholeNumber(
      record.reclaimedTransactionCount,
      "Garbage collection reclaimed transaction count",
    ),
    retainedTransactionCount: nonNegativeWholeNumber(
      record.retainedTransactionCount,
      "Garbage collection retained transaction count",
    ),
    missingTransactionCount: nonNegativeWholeNumber(
      record.missingTransactionCount,
      "Garbage collection missing transaction count",
    ),
    state: garbageCollectionJobState(record.state),
    revision: nonNegativeWholeNumber(record.revision, "Garbage collection job revision"),
    leaseCutoff: validTimestamp(record.leaseCutoff, "Garbage collection lease cutoff"),
    createdAt: validTimestamp(record.createdAt, "Garbage collection creation timestamp"),
    updatedAt: validTimestamp(record.updatedAt, "Garbage collection update timestamp"),
    ...(discovery === undefined ? {} : { discovery }),
  };
  if (
    discovery !== undefined &&
    candidateManifestVersions.length +
      candidateSegmentIds.length +
      candidateBlockIds.length +
      candidateTransactionIds.length >
      discovery.maxPlanningItems
  ) {
    throw new RangeError("Garbage collection planning candidates exceed the persisted limit");
  }
  if (
    safeSum(
      [
        normalized.prunedManifestCount,
        normalized.alreadyPrunedManifestCount,
        normalized.retainedManifestCount,
        normalized.missingManifestCount,
      ],
      "Garbage collection examined manifest count",
    ) !== cursor.manifestIndex ||
    safeSum(
      [
        normalized.reclaimedSegmentCount,
        normalized.retainedSegmentCount,
        normalized.missingSegmentCount,
      ],
      "Garbage collection examined segment count",
    ) !== cursor.segmentIndex ||
    safeSum(
      [normalized.reclaimedBlockCount, normalized.retainedBlockCount, normalized.missingBlockCount],
      "Garbage collection examined block count",
    ) !== cursor.blockIndex ||
    safeSum(
      [
        normalized.reclaimedTransactionCount,
        normalized.retainedTransactionCount,
        normalized.missingTransactionCount,
      ],
      "Garbage collection examined transaction count",
    ) !== cursor.transactionIndex
  ) {
    throw new TypeError("Garbage collection cursor does not match its persisted accounting");
  }
  if (
    cursor.manifestIndex > candidateManifestVersions.length ||
    cursor.segmentIndex > candidateSegmentIds.length ||
    cursor.blockIndex > candidateBlockIds.length ||
    cursor.transactionIndex > candidateTransactionIds.length
  ) {
    throw new RangeError("Garbage collection cursor is outside its candidate selection");
  }
  const complete = garbageCollectionJobComplete(normalized);
  if ((normalized.state === "completed") !== complete) {
    throw new TypeError(
      complete
        ? "A finished garbage collection cursor requires completed state"
        : "A completed garbage collection job requires a finished cursor",
    );
  }
  if (
    normalized.state === "planned" &&
    (cursor.manifestIndex !== 0 ||
      cursor.segmentIndex !== 0 ||
      cursor.blockIndex !== 0 ||
      cursor.transactionIndex !== 0)
  ) {
    throw new TypeError("A planned garbage collection job cannot contain progress");
  }
  return structuredClone(normalized);
}

export function advanceGarbageCollectionJobRecord(
  record: GarbageCollectionJobRecord,
  accounting: GarbageCollectionStepAccounting,
): GarbageCollectionJobRecord {
  const current = normalizeGarbageCollectionJobRecord(record);
  if (current.discovery !== undefined && current.discovery.phase !== "complete") {
    throw new TypeError("Garbage collection discovery must complete before reclamation starts");
  }
  if (current.state === "completed") return current;
  const increments = {
    examinedManifestCount: nonNegativeWholeNumber(
      accounting.examinedManifestCount,
      "Garbage collection examined manifest increment",
    ),
    prunedManifestCount: nonNegativeWholeNumber(
      accounting.prunedManifestCount,
      "Garbage collection pruned manifest increment",
    ),
    alreadyPrunedManifestCount: nonNegativeWholeNumber(
      accounting.alreadyPrunedManifestCount,
      "Garbage collection already-pruned manifest increment",
    ),
    retainedManifestCount: nonNegativeWholeNumber(
      accounting.retainedManifestCount,
      "Garbage collection retained manifest increment",
    ),
    missingManifestCount: nonNegativeWholeNumber(
      accounting.missingManifestCount,
      "Garbage collection missing manifest increment",
    ),
    examinedSegmentCount: nonNegativeWholeNumber(
      accounting.examinedSegmentCount,
      "Garbage collection examined segment increment",
    ),
    reclaimedSegmentCount: nonNegativeWholeNumber(
      accounting.reclaimedSegmentCount,
      "Garbage collection reclaimed segment increment",
    ),
    retainedSegmentCount: nonNegativeWholeNumber(
      accounting.retainedSegmentCount,
      "Garbage collection retained segment increment",
    ),
    missingSegmentCount: nonNegativeWholeNumber(
      accounting.missingSegmentCount,
      "Garbage collection missing segment increment",
    ),
    examinedBlockCount: nonNegativeWholeNumber(
      accounting.examinedBlockCount,
      "Garbage collection examined block increment",
    ),
    reclaimedBlockCount: nonNegativeWholeNumber(
      accounting.reclaimedBlockCount,
      "Garbage collection reclaimed block increment",
    ),
    retainedBlockCount: nonNegativeWholeNumber(
      accounting.retainedBlockCount,
      "Garbage collection retained block increment",
    ),
    missingBlockCount: nonNegativeWholeNumber(
      accounting.missingBlockCount,
      "Garbage collection missing block increment",
    ),
    reclaimedBlockBytes: nonNegativeWholeNumber(
      accounting.reclaimedBlockBytes,
      "Garbage collection reclaimed block byte increment",
    ),
    examinedTransactionCount: nonNegativeWholeNumber(
      accounting.examinedTransactionCount,
      "Garbage collection examined transaction increment",
    ),
    reclaimedTransactionCount: nonNegativeWholeNumber(
      accounting.reclaimedTransactionCount,
      "Garbage collection reclaimed transaction increment",
    ),
    retainedTransactionCount: nonNegativeWholeNumber(
      accounting.retainedTransactionCount,
      "Garbage collection retained transaction increment",
    ),
    missingTransactionCount: nonNegativeWholeNumber(
      accounting.missingTransactionCount,
      "Garbage collection missing transaction increment",
    ),
  };
  if (
    increments.examinedManifestCount !==
      safeSum(
        [
          increments.prunedManifestCount,
          increments.alreadyPrunedManifestCount,
          increments.retainedManifestCount,
          increments.missingManifestCount,
        ],
        "Garbage collection manifest increment",
      ) ||
    increments.examinedSegmentCount !==
      safeSum(
        [
          increments.reclaimedSegmentCount,
          increments.retainedSegmentCount,
          increments.missingSegmentCount,
        ],
        "Garbage collection segment increment",
      ) ||
    increments.examinedBlockCount !==
      safeSum(
        [
          increments.reclaimedBlockCount,
          increments.retainedBlockCount,
          increments.missingBlockCount,
        ],
        "Garbage collection block increment",
      ) ||
    increments.examinedTransactionCount !==
      safeSum(
        [
          increments.reclaimedTransactionCount,
          increments.retainedTransactionCount,
          increments.missingTransactionCount,
        ],
        "Garbage collection transaction increment",
      )
  ) {
    throw new TypeError("Garbage collection step accounting is incomplete");
  }
  const cursor: GarbageCollectionCursor = {
    manifestIndex: safeSum(
      [current.cursor.manifestIndex, increments.examinedManifestCount],
      "Garbage collection manifest cursor",
    ),
    segmentIndex: safeSum(
      [current.cursor.segmentIndex, increments.examinedSegmentCount],
      "Garbage collection segment cursor",
    ),
    blockIndex: safeSum(
      [current.cursor.blockIndex, increments.examinedBlockCount],
      "Garbage collection block cursor",
    ),
    transactionIndex: safeSum(
      [current.cursor.transactionIndex, increments.examinedTransactionCount],
      "Garbage collection transaction cursor",
    ),
  };
  const updated: GarbageCollectionJobRecord = {
    ...current,
    cursor,
    prunedManifestCount: safeSum(
      [current.prunedManifestCount, increments.prunedManifestCount],
      "Garbage collection pruned manifest count",
    ),
    alreadyPrunedManifestCount: safeSum(
      [current.alreadyPrunedManifestCount, increments.alreadyPrunedManifestCount],
      "Garbage collection already-pruned manifest count",
    ),
    retainedManifestCount: safeSum(
      [current.retainedManifestCount, increments.retainedManifestCount],
      "Garbage collection retained manifest count",
    ),
    missingManifestCount: safeSum(
      [current.missingManifestCount, increments.missingManifestCount],
      "Garbage collection missing manifest count",
    ),
    reclaimedSegmentCount: safeSum(
      [current.reclaimedSegmentCount, increments.reclaimedSegmentCount],
      "Garbage collection reclaimed segment count",
    ),
    retainedSegmentCount: safeSum(
      [current.retainedSegmentCount, increments.retainedSegmentCount],
      "Garbage collection retained segment count",
    ),
    missingSegmentCount: safeSum(
      [current.missingSegmentCount, increments.missingSegmentCount],
      "Garbage collection missing segment count",
    ),
    reclaimedBlockCount: safeSum(
      [current.reclaimedBlockCount, increments.reclaimedBlockCount],
      "Garbage collection reclaimed block count",
    ),
    retainedBlockCount: safeSum(
      [current.retainedBlockCount, increments.retainedBlockCount],
      "Garbage collection retained block count",
    ),
    missingBlockCount: safeSum(
      [current.missingBlockCount, increments.missingBlockCount],
      "Garbage collection missing block count",
    ),
    reclaimedBlockBytes: safeSum(
      [current.reclaimedBlockBytes, increments.reclaimedBlockBytes],
      "Garbage collection reclaimed block bytes",
    ),
    reclaimedTransactionCount: safeSum(
      [current.reclaimedTransactionCount, increments.reclaimedTransactionCount],
      "Garbage collection reclaimed transaction count",
    ),
    retainedTransactionCount: safeSum(
      [current.retainedTransactionCount, increments.retainedTransactionCount],
      "Garbage collection retained transaction count",
    ),
    missingTransactionCount: safeSum(
      [current.missingTransactionCount, increments.missingTransactionCount],
      "Garbage collection missing transaction count",
    ),
    state: "running",
    revision: safeSum([current.revision, 1], "Garbage collection revision"),
    updatedAt: validTimestamp(accounting.updatedAt, "Garbage collection update timestamp"),
  };
  if (garbageCollectionJobComplete(updated)) updated.state = "completed";
  return normalizeGarbageCollectionJobRecord(updated);
}

export function normalizeCompactionJobRecord(record: CompactionJobRecord): CompactionJobRecord {
  const error: unknown = record.error;
  if (error !== undefined && typeof error !== "string") {
    throw new TypeError("Compaction job error must be a string");
  }
  const rewritePlan = normalizeCompactionRewritePlan(record.rewritePlan);
  const logicalBytes = nonNegativeWholeNumber(record.logicalBytes, "Compaction logical bytes");
  const sourceStoredBytes = nonNegativeWholeNumber(
    record.sourceStoredBytes,
    "Compaction source stored bytes",
  );
  const outputStoredBytes = nonNegativeWholeNumber(
    record.outputStoredBytes,
    "Compaction output stored bytes",
  );
  const sourceLevelStoredBytes = {
    level0SourceStoredBytes: nonNegativeWholeNumber(
      record.level0SourceStoredBytes,
      "Compaction level-zero source stored bytes",
    ),
    anchorSourceStoredBytes: nonNegativeWholeNumber(
      record.anchorSourceStoredBytes,
      "Compaction anchor source stored bytes",
    ),
  };
  if (
    safeSum(
      [
        sourceLevelStoredBytes.level0SourceStoredBytes,
        sourceLevelStoredBytes.anchorSourceStoredBytes,
      ],
      "Compaction source-level stored bytes",
    ) !== sourceStoredBytes
  ) {
    throw new TypeError("Compaction source-level stored bytes must equal source stored bytes");
  }
  const level2PolicyValues = [
    record.outputPartitionOrdinal,
    record.maxWriteAmplification,
    record.maximumOutputStoredBytes,
    record.plannedOutputStoredBytesUpperBound,
  ];
  const level2PolicyFieldCount = level2PolicyValues.filter((value) => value !== undefined).length;
  if (level2PolicyFieldCount !== 0 && level2PolicyFieldCount !== level2PolicyValues.length) {
    throw new TypeError("Append-row-range L2 compaction policy fields must be present together");
  }
  if (record.priorAttemptOutputStoredBytes !== undefined && level2PolicyFieldCount === 0) {
    throw new TypeError(
      "Compaction prior-attempt accounting requires the L2 compaction policy fields",
    );
  }
  let level2Policy:
    | Pick<
        CompactionJobRecord,
        | "outputPartitionOrdinal"
        | "maxWriteAmplification"
        | "maximumOutputStoredBytes"
        | "plannedOutputStoredBytesUpperBound"
        | "priorAttemptOutputStoredBytes"
      >
    | undefined;
  if (level2PolicyFieldCount !== 0) {
    if (rewritePlan.kind !== "rechunk-v1" && rewritePlan.kind !== "merge-v1") {
      throw new TypeError("L2 compaction requires a rechunk or merge plan");
    }
    if (record.targetLevel !== 2) {
      throw new TypeError("Append-row-range L2 compaction must target level two");
    }
    // Append-row-range promotions consume pure level-zero prefixes; keyed merge promotions may
    // also fold a retained level-one anchor, whose bytes never count toward the L0 ceiling.
    if (
      rewritePlan.kind === "rechunk-v1" &&
      (sourceLevelStoredBytes.level0SourceStoredBytes !== sourceStoredBytes ||
        sourceLevelStoredBytes.anchorSourceStoredBytes !== 0)
    ) {
      throw new TypeError("Append-row-range L2 compaction requires only level-zero source bytes");
    }
    const outputPartitionOrdinal = nonNegativeWholeNumber(
      record.outputPartitionOrdinal,
      "Compaction output partition ordinal",
    );
    const maxWriteAmplification = positiveFiniteNumber(
      record.maxWriteAmplification,
      "Compaction maximum write amplification",
    );
    const maximumOutputStoredBytes = positiveWholeNumber(
      record.maximumOutputStoredBytes,
      "Compaction maximum output stored bytes",
    );
    const plannedOutputStoredBytesUpperBound = positiveWholeNumber(
      record.plannedOutputStoredBytesUpperBound,
      "Compaction planned output stored byte upper bound",
    );
    const priorAttemptOutputStoredBytes =
      record.priorAttemptOutputStoredBytes === undefined
        ? undefined
        : nonNegativeWholeNumber(
            record.priorAttemptOutputStoredBytes,
            "Compaction prior-attempt output stored bytes",
          );
    const amplificationCeiling = floorWholeNumberProduct(
      sourceStoredBytes,
      maxWriteAmplification,
      "Compaction write amplification product",
    );
    // The persisted ceiling plus everything failed attempts already wrote must stay within
    // the amplification limit — attempts share one lifetime budget.
    if (maximumOutputStoredBytes + (priorAttemptOutputStoredBytes ?? 0) > amplificationCeiling) {
      throw new RangeError("Compaction output stored byte ceiling exceeds its amplification limit");
    }
    if (plannedOutputStoredBytesUpperBound > maximumOutputStoredBytes) {
      throw new RangeError("Compaction planned output exceeds its stored byte ceiling");
    }
    if (outputStoredBytes > plannedOutputStoredBytesUpperBound) {
      throw new RangeError("Compaction output stored bytes exceed their planned upper bound");
    }
    level2Policy = {
      outputPartitionOrdinal,
      maxWriteAmplification,
      maximumOutputStoredBytes,
      plannedOutputStoredBytesUpperBound,
      ...(priorAttemptOutputStoredBytes === undefined ? {} : { priorAttemptOutputStoredBytes }),
    };
  }
  const normalized: CompactionJobRecord = {
    ...record,
    id: validateStorageId(record.id, "Compaction job ID"),
    tableId: validateStorageId(record.tableId, "Compaction job table ID"),
    sourceManifestVersion: nonNegativeWholeNumber(
      record.sourceManifestVersion,
      "Compaction source manifest version",
    ),
    sourceSegmentIds:
      rewritePlan.kind === "copy-v1"
        ? uniqueIds(record.sourceSegmentIds, "Compaction source segment ID", false)
        : orderedUniqueIds(record.sourceSegmentIds, "Compaction source segment ID"),
    sourceBlockIds:
      rewritePlan.kind === "copy-v1"
        ? uniqueIds(record.sourceBlockIds, "Compaction source block ID", true)
        : orderedUniqueIds(record.sourceBlockIds, "Compaction source block ID").sort(),
    outputBlockIds:
      rewritePlan.kind === "copy-v1"
        ? uniqueIds(record.outputBlockIds, "Compaction output block ID", true)
        : orderedUniqueIds(record.outputBlockIds, "Compaction output block ID"),
    cursor: normalizeCompactionJobCursor(record.cursor),
    processedRows: nonNegativeWholeNumber(record.processedRows, "Compaction processed row count"),
    sourceStoredBytes,
    outputStoredBytes,
    logicalBytes,
    rewritePlan,
    outputCursor: normalizeCompactionOutputCursor(record.outputCursor, rewritePlan),
    memoryBudgetBytes: nonNegativeWholeNumber(record.memoryBudgetBytes, "Compaction memory budget"),
    minimumMemoryBytes: nonNegativeWholeNumber(
      record.minimumMemoryBytes,
      "Compaction minimum memory",
    ),
    ...sourceLevelStoredBytes,
    ...(level2Policy ?? {}),
    peakWorkingBytes: nonNegativeWholeNumber(
      record.peakWorkingBytes,
      "Compaction peak working bytes",
    ),
    outputLogicalBytes: nonNegativeWholeNumber(
      record.outputLogicalBytes,
      "Compaction output logical bytes",
    ),
    targetLevel: nonNegativeWholeNumber(record.targetLevel, "Compaction target level"),
    state: compactionJobState(record.state),
    transactionId: nullableId(record.transactionId, "Compaction transaction ID"),
    outputSegmentId: nullableId(record.outputSegmentId, "Compaction output segment ID"),
    publishedVersion:
      record.publishedVersion === null
        ? null
        : nonNegativeWholeNumber(record.publishedVersion, "Compaction published version"),
    revision: nonNegativeWholeNumber(record.revision, "Compaction job revision"),
    createdAt: nonEmptyString(record.createdAt, "Compaction creation timestamp"),
    updatedAt: nonEmptyString(record.updatedAt, "Compaction update timestamp"),
  };
  if (normalized.sourceSegmentIds.length === 0) {
    throw new TypeError("Compaction requires at least one source segment");
  }
  if (normalized.cursor.sourceSegmentIndex > normalized.sourceSegmentIds.length) {
    throw new RangeError("Compaction source segment cursor is outside the source selection");
  }
  if (
    normalized.cursor.sourceSegmentIndex === normalized.sourceSegmentIds.length &&
    normalized.cursor.sourceBlockIndex !== 0
  ) {
    throw new RangeError("A completed compaction cursor must start at block zero");
  }
  validateCompactionRewrite(normalized);
  validateCompactionJobState(normalized);
  return structuredClone(normalized);
}

export function updateCompactionJobRecord(
  record: CompactionJobRecord,
  update: CompactionJobRecordUpdate,
): CompactionJobRecord {
  const current = normalizeCompactionJobRecord(record);
  for (const field of [
    "rewritePlan",
    "memoryBudgetBytes",
    "minimumMemoryBytes",
    "level0SourceStoredBytes",
    "anchorSourceStoredBytes",
    "outputPartitionOrdinal",
    "maxWriteAmplification",
    "maximumOutputStoredBytes",
    "plannedOutputStoredBytesUpperBound",
    "priorAttemptOutputStoredBytes",
  ] as const) {
    if (Reflect.has(update, field)) {
      throw new TypeError(`Compaction ${field} is immutable`);
    }
  }
  if (isOutputDrivenCompactionPlan(current.rewritePlan)) {
    for (const field of ["cursor", "sourceStoredBytes", "logicalBytes"] as const) {
      if (Reflect.has(update, field)) {
        throw new TypeError(`Output-driven compaction ${field} is immutable`);
      }
    }
  }
  const mirrorCopyLogicalBytes =
    current.rewritePlan.kind === "copy-v1" &&
    update.logicalBytes !== undefined &&
    update.outputLogicalBytes === undefined;
  const updated: CompactionJobRecord = {
    ...current,
    ...(update.outputBlockIds === undefined ? {} : { outputBlockIds: [...update.outputBlockIds] }),
    ...(update.cursor === undefined ? {} : { cursor: update.cursor }),
    ...(update.processedRows === undefined ? {} : { processedRows: update.processedRows }),
    ...(update.sourceStoredBytes === undefined
      ? {}
      : { sourceStoredBytes: update.sourceStoredBytes }),
    ...(update.outputStoredBytes === undefined
      ? {}
      : { outputStoredBytes: update.outputStoredBytes }),
    ...(update.logicalBytes === undefined ? {} : { logicalBytes: update.logicalBytes }),
    ...(update.outputCursor === undefined ? {} : { outputCursor: update.outputCursor }),
    ...(update.peakWorkingBytes === undefined ? {} : { peakWorkingBytes: update.peakWorkingBytes }),
    ...(update.outputLogicalBytes === undefined
      ? mirrorCopyLogicalBytes
        ? { outputLogicalBytes: update.logicalBytes }
        : {}
      : { outputLogicalBytes: update.outputLogicalBytes }),
    ...(update.state === undefined ? {} : { state: update.state }),
    ...(update.transactionId === undefined ? {} : { transactionId: update.transactionId }),
    ...(update.outputSegmentId === undefined ? {} : { outputSegmentId: update.outputSegmentId }),
    ...(update.publishedVersion === undefined ? {} : { publishedVersion: update.publishedVersion }),
    updatedAt: update.updatedAt,
    revision: safeSum([current.revision, 1], "Compaction revision"),
  };
  if (update.error === null) delete updated.error;
  else if (update.error !== undefined) updated.error = update.error;
  const normalized = normalizeCompactionJobRecord(updated);
  validateCompactionJobTransition(current.state, normalized.state);
  validateCompactionJobProgress(current, normalized);
  return normalized;
}

function validateCompactionJobState(record: CompactionJobRecord): void {
  const plan = record.rewritePlan;
  if (plan.kind === "merge-v1") {
    if (plan.totalRows === 0 && record.outputSegmentId !== null) {
      throw new TypeError("An empty merge compaction cannot have an output segment");
    }
    if (plan.totalRows > 0 && record.outputSegmentId === null) {
      throw new TypeError("A non-empty merge compaction requires an output segment ID");
    }
  }
  if (record.state === "cancelled" && record.error !== undefined) {
    throw new TypeError("A cancelled compaction cannot contain an error");
  }
  if (record.state === "planned") {
    const isCopy = record.rewritePlan.kind === "copy-v1";
    const hasProgress =
      record.cursor.sourceSegmentIndex !== 0 ||
      record.cursor.sourceBlockIndex !== 0 ||
      record.outputBlockIds.length !== 0 ||
      record.processedRows !== 0 ||
      (isCopy && record.sourceStoredBytes !== 0) ||
      record.outputStoredBytes !== 0 ||
      (isCopy && record.logicalBytes !== 0) ||
      record.outputLogicalBytes !== 0 ||
      record.peakWorkingBytes !== 0;
    if (hasProgress || record.transactionId !== null || !isInitialOutputCursor(record)) {
      throw new TypeError("A planned compaction cannot contain transaction progress");
    }
  }
  if (record.state === "running" && record.transactionId === null) {
    throw new TypeError("A running compaction requires a transaction ID");
  }
  if (record.state === "ready" || record.state === "published") {
    if (
      record.transactionId === null ||
      (record.outputSegmentId === null && !(plan.kind === "merge-v1" && plan.totalRows === 0))
    ) {
      throw new TypeError(`${record.state} compaction requires its transaction and output segment`);
    }
    if (!hasCompletedCompactionCursor(record)) {
      throw new TypeError(`${record.state} compaction requires a completed cursor`);
    }
    if (record.outputBlockIds.length !== expectedCompactionOutputCount(record)) {
      throw new TypeError(`${record.state} compaction requires every output block`);
    }
    if (
      isOutputDrivenCompactionPlan(record.rewritePlan) &&
      expectedCompactionOutputCount(record) > 0 &&
      record.peakWorkingBytes < record.minimumMemoryBytes
    ) {
      throw new TypeError(`${record.state} compaction requires complete memory accounting`);
    }
  }
  if (record.state === "published") {
    if (record.publishedVersion === null) {
      throw new TypeError("A published compaction requires a manifest version");
    }
  } else if (record.publishedVersion !== null) {
    throw new TypeError("Only a published compaction can have a manifest version");
  }
}

function validateCompactionRewrite(record: CompactionJobRecord): void {
  const plan = record.rewritePlan;
  const outputLogicalBytes = record.outputLogicalBytes;
  const memoryBudgetBytes = record.memoryBudgetBytes;
  const minimumMemoryBytes = record.minimumMemoryBytes;
  const peakWorkingBytes = record.peakWorkingBytes;
  if (plan.kind === "copy-v1") {
    if (record.outputCursor !== null) {
      throw new TypeError("A copy compaction cannot have an output cursor");
    }
    if (memoryBudgetBytes !== 0 || minimumMemoryBytes !== 0 || peakWorkingBytes !== 0) {
      throw new TypeError("A copy compaction cannot have rechunk memory accounting");
    }
    if (outputLogicalBytes !== record.logicalBytes) {
      throw new TypeError("A copy compaction must preserve its logical byte count");
    }
    if (record.outputBlockIds.length > record.sourceBlockIds.length) {
      throw new RangeError(
        "Compaction output cannot contain more blocks than its source selection",
      );
    }
    return;
  }

  if (record.cursor.sourceSegmentIndex !== 0 || record.cursor.sourceBlockIndex !== 0) {
    throw new TypeError("An output-driven compaction does not use the source cursor");
  }
  const permitsZeroMinimum = plan.kind === "merge-v1" && plan.totalRows === 0;
  if (memoryBudgetBytes === 0 || (minimumMemoryBytes === 0 && !permitsZeroMinimum)) {
    throw new RangeError("An output-driven compaction requires a memory budget and minimum memory");
  }
  if (minimumMemoryBytes > memoryBudgetBytes) {
    throw new RangeError("Compaction minimum memory exceeds its memory budget");
  }
  if (peakWorkingBytes > memoryBudgetBytes) {
    throw new RangeError("Compaction peak working bytes exceed its memory budget");
  }

  const plannedBlocks =
    plan.kind === "rechunk-v1"
      ? plan.columns.flatMap((column) => column.sourceBlocks)
      : plan.sourceSegments.flatMap((segment) =>
          segment.columns.flatMap((column) => column.sourceBlocks),
        );
  const plannedBlockIds = plannedBlocks.map((block) => block.blockId);
  if (new Set(plannedBlockIds).size !== plannedBlockIds.length) {
    throw new TypeError("A planned source block can only appear once in its source layout");
  }
  const sortedPlannedIds = [...plannedBlockIds].sort();
  if (
    sortedPlannedIds.length !== record.sourceBlockIds.length ||
    sortedPlannedIds.some((id, index) => id !== record.sourceBlockIds[index])
  ) {
    throw new TypeError("The rewrite source layout must describe every selected source block");
  }
  const plannedStoredBytes = safeSum(
    plannedBlocks.map((block) => block.storedBytes),
    "Rechunk source stored bytes",
  );
  const plannedEncodedBytes = safeSum(
    plannedBlocks.map((block) => block.encodedBytes),
    "Rechunk source encoded bytes",
  );
  if (record.sourceStoredBytes !== plannedStoredBytes) {
    throw new TypeError("Source stored bytes must match the immutable rewrite layout");
  }
  if (record.logicalBytes !== plannedEncodedBytes) {
    throw new TypeError("Logical bytes must match the immutable rewrite layout");
  }

  if (plan.kind === "merge-v1") {
    if (
      plan.totalRows === 0 &&
      (record.outputBlockIds.length !== 0 ||
        record.outputStoredBytes !== 0 ||
        outputLogicalBytes !== 0 ||
        peakWorkingBytes !== 0 ||
        minimumMemoryBytes !== 0)
    ) {
      throw new TypeError("An empty merge compaction cannot contain physical output progress");
    }
    const plannedSegmentIds = plan.sourceSegments.map((segment) => segment.segmentId);
    if (
      plannedSegmentIds.length !== record.sourceSegmentIds.length ||
      plannedSegmentIds.some((id, index) => id !== record.sourceSegmentIds[index])
    ) {
      throw new TypeError(
        "Merge source layout must preserve every selected source segment in order",
      );
    }
  }

  const cursor = record.outputCursor;
  if (cursor === null) {
    throw new TypeError("An output-driven compaction requires an output cursor");
  }
  const completedOutputs = safeSum(
    [
      safeProduct(cursor.outputIndex, plan.columns.length, "Rechunk output cursor"),
      cursor.columnIndex,
    ],
    "Rechunk output cursor",
  );
  if (record.outputBlockIds.length !== completedOutputs) {
    throw new TypeError("Compaction output IDs must match the output cursor");
  }
  const expectedProcessedRows =
    cursor.outputIndex === plan.outputs.length
      ? plan.totalRows
      : (plan.outputs[cursor.outputIndex]?.rowStart ?? 0);
  if (record.processedRows !== expectedProcessedRows) {
    throw new TypeError("Processed rows must match completed output windows");
  }
}

function validateCompactionJobProgress(
  previous: CompactionJobRecord,
  next: CompactionJobRecord,
): void {
  for (const [label, previousValue, nextValue] of [
    ["processed rows", previous.processedRows, next.processedRows],
    ["source stored bytes", previous.sourceStoredBytes, next.sourceStoredBytes],
    ["output stored bytes", previous.outputStoredBytes, next.outputStoredBytes],
    ["logical bytes", previous.logicalBytes, next.logicalBytes],
    ["output logical bytes", previous.outputLogicalBytes, next.outputLogicalBytes],
    ["peak working bytes", previous.peakWorkingBytes, next.peakWorkingBytes],
  ] as const) {
    if (nextValue < previousValue) {
      throw new RangeError(`Compaction ${label} cannot decrease`);
    }
  }

  if (next.outputBlockIds.length < previous.outputBlockIds.length) {
    throw new TypeError("Compaction output block IDs cannot be removed");
  }
  if (isOutputDrivenCompactionPlan(previous.rewritePlan)) {
    if (previous.outputBlockIds.some((id, index) => next.outputBlockIds[index] !== id)) {
      throw new TypeError("Output block IDs are an append-only ordered checkpoint");
    }
    if (compactionOutputOrdinal(next) < compactionOutputOrdinal(previous)) {
      throw new RangeError("Output cursor cannot move backwards");
    }
    if (
      previous.rewritePlan.kind === "merge-v1" &&
      previous.outputSegmentId !== next.outputSegmentId
    ) {
      throw new TypeError("Merge output segment ID is immutable");
    }
  } else {
    const nextIds = new Set(next.outputBlockIds);
    if (previous.outputBlockIds.some((id) => !nextIds.has(id))) {
      throw new TypeError("Compaction output block IDs cannot be removed");
    }
    const previousCursor = previous.cursor;
    const nextCursor = next.cursor;
    if (
      nextCursor.sourceSegmentIndex < previousCursor.sourceSegmentIndex ||
      (nextCursor.sourceSegmentIndex === previousCursor.sourceSegmentIndex &&
        nextCursor.sourceBlockIndex < previousCursor.sourceBlockIndex)
    ) {
      throw new RangeError("Compaction source cursor cannot move backwards");
    }
  }
}

function isInitialOutputCursor(record: CompactionJobRecord): boolean {
  const plan = record.rewritePlan;
  if (plan.kind === "copy-v1") return record.outputCursor === null;
  const cursor = record.outputCursor;
  const initialRowStart = plan.outputs[0]?.rowStart ?? plan.totalRows;
  return (
    cursor?.outputIndex === 0 && cursor.columnIndex === 0 && cursor.rowStart === initialRowStart
  );
}

function hasCompletedCompactionCursor(record: CompactionJobRecord): boolean {
  const plan = record.rewritePlan;
  if (plan.kind === "copy-v1") {
    return record.cursor.sourceSegmentIndex === record.sourceSegmentIds.length;
  }
  const cursor = record.outputCursor;
  return (
    cursor?.outputIndex === plan.outputs.length &&
    cursor.columnIndex === 0 &&
    cursor.rowStart === plan.totalRows
  );
}

function expectedCompactionOutputCount(record: CompactionJobRecord): number {
  const plan = record.rewritePlan;
  return plan.kind === "copy-v1"
    ? record.sourceBlockIds.length
    : safeProduct(plan.outputs.length, plan.columns.length, "Rechunk output block count");
}

function compactionOutputOrdinal(record: CompactionJobRecord): number {
  const plan = record.rewritePlan;
  const cursor = record.outputCursor;
  if (!isOutputDrivenCompactionPlan(plan) || cursor === null) return 0;
  return safeSum(
    [
      safeProduct(cursor.outputIndex, plan.columns.length, "Rechunk output cursor"),
      cursor.columnIndex,
    ],
    "Rechunk output cursor",
  );
}

function isOutputDrivenCompactionPlan(
  plan: CompactionRewritePlan,
): plan is RechunkCompactionRewritePlan | MergeCompactionRewritePlan {
  return plan.kind === "rechunk-v1" || plan.kind === "merge-v1";
}

function validateCompactionJobTransition(
  previous: CompactionJobState,
  next: CompactionJobState,
): void {
  const allowed: Record<CompactionJobState, readonly CompactionJobState[]> = {
    planned: ["planned", "running", "cancelled", "aborted"],
    running: ["running", "ready", "published", "cancelled", "aborted"],
    ready: ["ready", "running", "published", "cancelled", "aborted"],
    published: ["published"],
    cancelled: ["cancelled"],
    aborted: ["aborted"],
  };
  if (!allowed[previous].includes(next)) {
    throw new TypeError(`Invalid compaction state transition: ${previous} to ${next}`);
  }
}

function normalizeCompactionJobCursor(value: unknown): CompactionJobCursor {
  if (typeof value !== "object" || value === null) {
    throw new TypeError("Compaction cursor must be an object");
  }
  return {
    sourceSegmentIndex: nonNegativeWholeNumber(
      Reflect.get(value, "sourceSegmentIndex"),
      "Compaction source segment cursor",
    ),
    sourceBlockIndex: nonNegativeWholeNumber(
      Reflect.get(value, "sourceBlockIndex"),
      "Compaction source block cursor",
    ),
  };
}

function normalizeCompactionRewritePlan(value: unknown): CompactionRewritePlan {
  if (typeof value !== "object" || value === null) {
    throw new TypeError("Compaction rewrite plan must be an object");
  }
  const kind: unknown = Reflect.get(value, "kind");
  if (kind === "copy-v1") return { kind: "copy-v1" };
  if (kind === "merge-v1") return normalizeMergeCompactionRewritePlan(value);
  if (kind !== "rechunk-v1") {
    throw new TypeError(`Invalid compaction rewrite plan: ${String(kind)}`);
  }

  const totalRows = positiveWholeNumber(Reflect.get(value, "totalRows"), "Rechunk total row count");
  const rowIdStart = nonNegativeBigInt(Reflect.get(value, "rowIdStart"), "Rechunk row ID start");
  const rowIdEndExclusive = nonNegativeBigInt(
    Reflect.get(value, "rowIdEndExclusive"),
    "Rechunk row ID end",
  );
  if (rowIdEndExclusive - rowIdStart !== BigInt(totalRows)) {
    throw new RangeError("Rechunk row ID range must match its total row count");
  }
  const columnsValue: unknown = Reflect.get(value, "columns");
  if (!Array.isArray(columnsValue) || columnsValue.length === 0) {
    throw new TypeError("A rechunk plan requires at least one source column");
  }
  const columns = columnsValue.map((column, index) =>
    normalizeRechunkSourceColumn(column, totalRows, index),
  );
  const columnIds = columns.map((column) => column.columnId);
  if (new Set(columnIds).size !== columnIds.length) {
    throw new TypeError("A rechunk plan cannot contain duplicate source columns");
  }

  const outputsValue: unknown = Reflect.get(value, "outputs");
  if (!Array.isArray(outputsValue) || outputsValue.length === 0) {
    throw new TypeError("A rechunk plan requires at least one output window");
  }
  const outputs = outputsValue.map((output, index) => {
    if (typeof output !== "object" || output === null) {
      throw new TypeError(`Rechunk output window ${String(index)} must be an object`);
    }
    return {
      rowStart: nonNegativeWholeNumber(
        Reflect.get(output, "rowStart"),
        `Rechunk output window ${String(index)} row start`,
      ),
      rowCount: blockRowCount(
        Reflect.get(output, "rowCount"),
        `Rechunk output window ${String(index)} row count`,
      ),
    };
  });
  validateContiguousRows(outputs, totalRows, "Rechunk output windows");
  const partitionsValue: unknown = Reflect.get(value, "partitions");
  const partitions =
    partitionsValue === undefined
      ? undefined
      : normalizeMergeOutputPartitions(partitionsValue, totalRows, outputs);

  return {
    kind: "rechunk-v1",
    targetBlockBytes: positiveWholeNumber(
      Reflect.get(value, "targetBlockBytes"),
      "Rechunk target block bytes",
    ),
    outputCompression: compactionOutputCompression(Reflect.get(value, "outputCompression")),
    totalRows,
    rowIdStart,
    rowIdEndExclusive,
    logicalOrder: nonNegativeFiniteNumber(
      Reflect.get(value, "logicalOrder"),
      "Rechunk logical order",
    ),
    columns,
    outputs,
    ...(partitions === undefined ? {} : { partitions }),
  };
}

function normalizeRechunkSourceColumn(
  value: unknown,
  totalRows: number,
  columnIndex: number,
): RechunkCompactionSourceColumn {
  if (typeof value !== "object" || value === null) {
    throw new TypeError(`Rechunk source column ${String(columnIndex)} must be an object`);
  }
  const sourceBlocksValue: unknown = Reflect.get(value, "sourceBlocks");
  if (!Array.isArray(sourceBlocksValue) || sourceBlocksValue.length === 0) {
    throw new TypeError(`Rechunk source column ${String(columnIndex)} requires source blocks`);
  }
  const sourceBlocks = sourceBlocksValue.map((block, blockIndex) => {
    if (typeof block !== "object" || block === null) {
      throw new TypeError(
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} must be an object`,
      );
    }
    return {
      blockId: validateStorageId(
        Reflect.get(block, "blockId"),
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} ID`,
      ),
      rowStart: nonNegativeWholeNumber(
        Reflect.get(block, "rowStart"),
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} row start`,
      ),
      rowCount: blockRowCount(
        Reflect.get(block, "rowCount"),
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} row count`,
      ),
      storedBytes: positiveWholeNumber(
        Reflect.get(block, "storedBytes"),
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} stored bytes`,
      ),
      encodedBytes: nonNegativeWholeNumber(
        Reflect.get(block, "encodedBytes"),
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} encoded bytes`,
      ),
      checksum: uint32(
        Reflect.get(block, "checksum"),
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} checksum`,
      ),
    };
  });
  validateContiguousRows(
    sourceBlocks,
    totalRows,
    `Rechunk source column ${String(columnIndex)} blocks`,
  );
  return {
    columnId: validateStorageId(
      Reflect.get(value, "columnId"),
      `Rechunk source column ${String(columnIndex)} ID`,
    ),
    type: simpleDataType(Reflect.get(value, "type")),
    sourceBlocks,
  };
}

function normalizeMergeCompactionRewritePlan(value: object): MergeCompactionRewritePlan {
  const totalRows = nonNegativeWholeNumber(
    Reflect.get(value, "totalRows"),
    "Merge total row count",
  );
  const rowIdStart = nonNegativeBigInt(Reflect.get(value, "rowIdStart"), "Merge row ID start");
  const rowIdEndExclusive = nonNegativeBigInt(
    Reflect.get(value, "rowIdEndExclusive"),
    "Merge row ID end",
  );
  const rowIdSpans = normalizeRowIdSpans(
    Reflect.get(value, "rowIdSpans"),
    totalRows,
    rowIdStart,
    rowIdEndExclusive,
    "Merge output row ID spans",
  );
  const keyColumnId = validateStorageId(Reflect.get(value, "keyColumnId"), "Merge key column ID");

  const sourceSegmentsValue: unknown = Reflect.get(value, "sourceSegments");
  if (!Array.isArray(sourceSegmentsValue) || sourceSegmentsValue.length === 0) {
    throw new TypeError("A merge plan requires at least one source segment");
  }
  const sourceSegments = sourceSegmentsValue.map((segment, index) =>
    normalizeMergeSourceSegment(segment, index),
  );
  const sourceSegmentIds = sourceSegments.map((segment) => segment.segmentId);
  if (new Set(sourceSegmentIds).size !== sourceSegmentIds.length) {
    throw new TypeError("A merge source segment can only appear once");
  }
  for (let index = 1; index < sourceSegments.length; index += 1) {
    const previous = sourceSegments[index - 1];
    const current = sourceSegments[index];
    if (
      previous !== undefined &&
      current !== undefined &&
      compareMergeSourceSegments(previous, current) >= 0
    ) {
      throw new TypeError("Merge source segments must use canonical logical order");
    }
  }

  const sourceBlocks = new Map<
    string,
    { columnId: string; type: SimpleDataType; rowCount: number }
  >();
  for (const segment of sourceSegments) {
    for (const column of segment.columns) {
      for (const block of column.sourceBlocks) {
        if (sourceBlocks.has(block.blockId)) {
          throw new TypeError("A merge source block can only appear once");
        }
        sourceBlocks.set(block.blockId, {
          columnId: column.columnId,
          type: column.type,
          rowCount: block.rowCount,
        });
      }
    }
  }

  const columnsValue: unknown = Reflect.get(value, "columns");
  if (!Array.isArray(columnsValue) || columnsValue.length === 0) {
    throw new TypeError("A merge plan requires at least one output column");
  }
  const columns = columnsValue.map((column, index) =>
    normalizeMergeOutputColumn(column, totalRows, sourceBlocks, index),
  );
  const columnIds = columns.map((column) => column.columnId);
  if (new Set(columnIds).size !== columnIds.length) {
    throw new TypeError("A merge plan cannot contain duplicate output columns");
  }
  if (!columnIds.includes(keyColumnId)) {
    throw new TypeError("A merge plan must output its key column");
  }
  validateMergeSourceShapes(sourceSegments, columns, keyColumnId);

  const outputsValue: unknown = Reflect.get(value, "outputs");
  if (!Array.isArray(outputsValue)) throw new TypeError("Merge outputs must be an array");
  if ((totalRows === 0) !== (outputsValue.length === 0)) {
    throw new TypeError("Merge output windows must be empty exactly when no rows survive");
  }
  const outputs = outputsValue.map((output, index) => {
    if (typeof output !== "object" || output === null) {
      throw new TypeError(`Merge output window ${String(index)} must be an object`);
    }
    return {
      rowStart: nonNegativeWholeNumber(
        Reflect.get(output, "rowStart"),
        `Merge output window ${String(index)} row start`,
      ),
      rowCount: blockRowCount(
        Reflect.get(output, "rowCount"),
        `Merge output window ${String(index)} row count`,
      ),
    };
  });
  validateContiguousRows(outputs, totalRows, "Merge output windows");

  const logicalOrder = nonNegativeFiniteNumber(
    Reflect.get(value, "logicalOrder"),
    "Merge logical order",
  );
  if (logicalOrder !== sourceSegments[0]?.logicalOrder) {
    throw new TypeError("Merge logical order must match its earliest source segment");
  }
  const partitionsValue: unknown = Reflect.get(value, "partitions");
  const partitions =
    partitionsValue === undefined
      ? undefined
      : normalizeMergeOutputPartitions(partitionsValue, totalRows, outputs);

  return {
    kind: "merge-v1",
    targetBlockBytes: positiveWholeNumber(
      Reflect.get(value, "targetBlockBytes"),
      "Merge target block bytes",
    ),
    outputCompression: compactionOutputCompression(Reflect.get(value, "outputCompression")),
    keyColumnId,
    totalRows,
    rowIdStart,
    rowIdEndExclusive,
    rowIdSpans,
    logicalOrder,
    sourceSegments,
    columns,
    outputs,
    ...(partitions === undefined ? {} : { partitions }),
  };
}

/**
 * Output partitions tile the merged output, carry strictly increasing logical orders, and
 * never split an output window: a window's blocks belong to exactly one published segment.
 */
function normalizeMergeOutputPartitions(
  value: unknown,
  totalRows: number,
  outputs: readonly RechunkCompactionOutputWindow[],
): MergeOutputPartition[] {
  if (!Array.isArray(value)) throw new TypeError("Merge output partitions must be an array");
  if ((totalRows === 0) !== (value.length === 0)) {
    throw new TypeError("Merge output partitions must be empty exactly when no rows survive");
  }
  const partitions = value.map((partition, index) => {
    if (typeof partition !== "object" || partition === null) {
      throw new TypeError(`Merge output partition ${String(index)} must be an object`);
    }
    const label = `Merge output partition ${String(index)}`;
    return {
      rowStart: nonNegativeWholeNumber(Reflect.get(partition, "rowStart"), `${label} row start`),
      rowCount: positiveWholeNumber(Reflect.get(partition, "rowCount"), `${label} row count`),
      logicalOrder: nonNegativeFiniteNumber(
        Reflect.get(partition, "logicalOrder"),
        `${label} logical order`,
      ),
    };
  });
  validateContiguousRows(partitions, totalRows, "Merge output partitions");
  for (let index = 1; index < partitions.length; index += 1) {
    const previous = partitions[index - 1];
    const current = partitions[index];
    if (
      previous !== undefined &&
      current !== undefined &&
      current.logicalOrder <= previous.logicalOrder
    ) {
      throw new RangeError("Merge output partitions must carry strictly increasing logical orders");
    }
  }
  let partitionIndex = 0;
  for (const output of outputs) {
    let partition = partitions[partitionIndex];
    while (partition !== undefined && output.rowStart >= partition.rowStart + partition.rowCount) {
      partitionIndex += 1;
      partition = partitions[partitionIndex];
    }
    if (
      partition === undefined ||
      output.rowStart < partition.rowStart ||
      output.rowStart + output.rowCount > partition.rowStart + partition.rowCount
    ) {
      throw new RangeError("Merge output windows cannot straddle output partitions");
    }
  }
  return partitions;
}

function normalizeMergeSourceSegment(
  value: unknown,
  segmentIndex: number,
): MergeCompactionSourceSegment {
  if (typeof value !== "object" || value === null) {
    throw new TypeError(`Merge source segment ${String(segmentIndex)} must be an object`);
  }
  const label = `Merge source segment ${String(segmentIndex)}`;
  const kind = segmentKind(Reflect.get(value, "kind"));
  const rowCount = positiveWholeNumber(Reflect.get(value, "rowCount"), `${label} row count`);
  const rowIdStart = nonNegativeBigInt(Reflect.get(value, "rowIdStart"), `${label} row ID start`);
  const rowIdEndExclusive = nonNegativeBigInt(
    Reflect.get(value, "rowIdEndExclusive"),
    `${label} row ID end`,
  );
  let rowIdSpans: RowIdSpan[];
  if (kind === "insert" || kind === "upsert" || kind === "base") {
    rowIdSpans = normalizeRowIdSpans(
      Reflect.get(value, "rowIdSpans"),
      rowCount,
      rowIdStart,
      rowIdEndExclusive,
      `${label} row ID spans`,
    );
  } else {
    const spans: unknown = Reflect.get(value, "rowIdSpans");
    if (!Array.isArray(spans) || spans.length !== 0) {
      throw new TypeError(`${label} mutation markers cannot own row IDs`);
    }
    if (rowIdStart !== 0n || rowIdEndExclusive !== 0n) {
      throw new TypeError(`${label} mutation marker row ID envelope must be empty`);
    }
    rowIdSpans = [];
  }

  const columnsValue: unknown = Reflect.get(value, "columns");
  if (!Array.isArray(columnsValue) || columnsValue.length === 0) {
    throw new TypeError(`${label} requires at least one source column`);
  }
  const columns = columnsValue.map((column, columnIndex) =>
    normalizeMergeSourceColumn(column, rowCount, segmentIndex, columnIndex),
  );
  const columnIds = columns.map((column) => column.columnId);
  if (new Set(columnIds).size !== columnIds.length) {
    throw new TypeError(`${label} cannot contain duplicate source columns`);
  }

  return {
    segmentId: validateStorageId(Reflect.get(value, "segmentId"), `${label} ID`),
    transactionId: validateStorageId(
      Reflect.get(value, "transactionId"),
      `${label} transaction ID`,
    ),
    committedVersion: nonNegativeWholeNumber(
      Reflect.get(value, "committedVersion"),
      `${label} committed version`,
    ),
    kind,
    keyColumnId: nullableId(Reflect.get(value, "keyColumnId"), `${label} key column ID`),
    level: nonNegativeWholeNumber(Reflect.get(value, "level"), `${label} level`),
    logicalOrder: nonNegativeFiniteNumber(
      Reflect.get(value, "logicalOrder"),
      `${label} logical order`,
    ),
    rowCount,
    rowIdStart,
    rowIdEndExclusive,
    rowIdSpans,
    columns,
  };
}

function normalizeMergeSourceColumn(
  value: unknown,
  segmentRowCount: number,
  segmentIndex: number,
  columnIndex: number,
): MergeCompactionSourceColumn {
  const label = `Merge source column ${String(segmentIndex)}:${String(columnIndex)}`;
  if (typeof value !== "object" || value === null) {
    throw new TypeError(`${label} must be an object`);
  }
  const sourceBlocksValue: unknown = Reflect.get(value, "sourceBlocks");
  if (!Array.isArray(sourceBlocksValue) || sourceBlocksValue.length === 0) {
    throw new TypeError(`${label} requires source blocks`);
  }
  const sourceBlocks = sourceBlocksValue.map((block, blockIndex) => {
    if (typeof block !== "object" || block === null) {
      throw new TypeError(`${label} block ${String(blockIndex)} must be an object`);
    }
    return {
      blockId: validateStorageId(Reflect.get(block, "blockId"), `${label} block ID`),
      rowStart: nonNegativeWholeNumber(Reflect.get(block, "rowStart"), `${label} block row start`),
      rowCount: blockRowCount(Reflect.get(block, "rowCount"), `${label} block row count`),
      storedBytes: positiveWholeNumber(
        Reflect.get(block, "storedBytes"),
        `${label} block stored bytes`,
      ),
      encodedBytes: nonNegativeWholeNumber(
        Reflect.get(block, "encodedBytes"),
        `${label} block encoded bytes`,
      ),
      checksum: uint32(Reflect.get(block, "checksum"), `${label} block checksum`),
    };
  });
  validateContiguousRows(sourceBlocks, segmentRowCount, `${label} blocks`);
  return {
    columnId: validateStorageId(Reflect.get(value, "columnId"), `${label} ID`),
    type: simpleDataType(Reflect.get(value, "type")),
    sourceBlocks,
  };
}

function normalizeMergeOutputColumn(
  value: unknown,
  totalRows: number,
  sourceBlocks: ReadonlyMap<string, { columnId: string; type: SimpleDataType; rowCount: number }>,
  columnIndex: number,
): MergeCompactionOutputColumn {
  const label = `Merge output column ${String(columnIndex)}`;
  if (typeof value !== "object" || value === null) {
    throw new TypeError(`${label} must be an object`);
  }
  const columnId = validateStorageId(Reflect.get(value, "columnId"), `${label} ID`);
  const type = simpleDataType(Reflect.get(value, "type"));
  const rangesValue: unknown = Reflect.get(value, "sourceRanges");
  if (!Array.isArray(rangesValue)) throw new TypeError(`${label} source ranges must be an array`);
  const sourceRanges = rangesValue.map((range, rangeIndex) => {
    if (typeof range !== "object" || range === null) {
      throw new TypeError(`${label} source range ${String(rangeIndex)} must be an object`);
    }
    const normalized: MergeCompactionOutputSourceRange = {
      outputRowStart: nonNegativeWholeNumber(
        Reflect.get(range, "outputRowStart"),
        `${label} source range output row start`,
      ),
      sourceBlockId: validateStorageId(
        Reflect.get(range, "sourceBlockId"),
        `${label} source range block ID`,
      ),
      sourceRowStart: nonNegativeWholeNumber(
        Reflect.get(range, "sourceRowStart"),
        `${label} source range block row start`,
      ),
      rowCount: blockRowCount(Reflect.get(range, "rowCount"), `${label} source range row count`),
    };
    const source = sourceBlocks.get(normalized.sourceBlockId);
    if (source === undefined) throw new TypeError(`${label} references an unknown source block`);
    if (source.columnId !== columnId || source.type !== type) {
      throw new TypeError(`${label} source range has the wrong column or type`);
    }
    if (
      safeSum([normalized.sourceRowStart, normalized.rowCount], `${label} source range rows`) >
      source.rowCount
    ) {
      throw new RangeError(`${label} source range is outside its source block`);
    }
    return normalized;
  });
  let outputRowStart = 0;
  for (let index = 0; index < sourceRanges.length; index += 1) {
    const range = sourceRanges[index];
    if (range === undefined) continue;
    if (range.outputRowStart !== outputRowStart) {
      throw new RangeError(`${label} source ranges must cover output rows contiguously`);
    }
    const previous = sourceRanges[index - 1];
    if (
      previous?.sourceBlockId === range.sourceBlockId &&
      previous.sourceRowStart + previous.rowCount === range.sourceRowStart
    ) {
      throw new TypeError(`${label} contains adjacent source ranges that must be coalesced`);
    }
    outputRowStart = safeSum([outputRowStart, range.rowCount], `${label} output row count`);
  }
  if (outputRowStart !== totalRows) {
    throw new RangeError(`${label} source ranges must cover every merged output row`);
  }
  return { columnId, type, sourceRanges };
}

function validateMergeSourceShapes(
  sourceSegments: readonly MergeCompactionSourceSegment[],
  outputColumns: readonly MergeCompactionOutputColumn[],
  keyColumnId: string,
): void {
  const outputIds = outputColumns.map((column) => column.columnId);
  const outputTypes = new Map(outputColumns.map((column) => [column.columnId, column.type]));
  for (const segment of sourceSegments) {
    if (segment.keyColumnId !== keyColumnId) {
      throw new TypeError(`Merge source segment ${segment.segmentId} has the wrong key column`);
    }
    const sourceIds = segment.columns.map((column) => column.columnId);
    if (segment.columns.some((column) => outputTypes.get(column.columnId) !== column.type)) {
      throw new TypeError(
        `Merge source segment ${segment.segmentId} has an unknown column or type`,
      );
    }
    const canonicalIds = outputIds.filter((id) => sourceIds.includes(id));
    if (canonicalIds.some((id, index) => sourceIds[index] !== id)) {
      throw new TypeError(`Merge source segment ${segment.segmentId} columns are not canonical`);
    }
    if (segment.kind === "insert" || segment.kind === "upsert" || segment.kind === "base") {
      if (
        sourceIds.length !== outputIds.length ||
        sourceIds.some((id, index) => outputIds[index] !== id)
      ) {
        throw new TypeError(`Merge ${segment.kind} segment must contain every output column`);
      }
    } else if (segment.kind === "delete") {
      if (sourceIds.length !== 1 || sourceIds[0] !== keyColumnId) {
        throw new TypeError("A merge delete segment must contain only its key column");
      }
    } else if (
      sourceIds.length < 2 ||
      !sourceIds.includes(keyColumnId) ||
      sourceIds.every((id) => id === keyColumnId)
    ) {
      throw new TypeError("A merge update segment requires its key and a changed column");
    }
  }
}

function normalizeRowIdSpans(
  value: unknown,
  totalRows: number,
  rowIdStart: bigint,
  rowIdEndExclusive: bigint,
  label: string,
): RowIdSpan[] {
  if (!Array.isArray(value)) throw new TypeError(`${label} must be an array`);
  const spans = value.map((span, index) => {
    if (typeof span !== "object" || span === null) {
      throw new TypeError(`${label} ${String(index)} must be an object`);
    }
    return {
      rowStart: nonNegativeWholeNumber(
        Reflect.get(span, "rowStart"),
        `${label} ${String(index)} row start`,
      ),
      rowCount: positiveWholeNumber(
        Reflect.get(span, "rowCount"),
        `${label} ${String(index)} row count`,
      ),
      rowIdStart: nonNegativeBigInt(
        Reflect.get(span, "rowIdStart"),
        `${label} ${String(index)} row ID start`,
      ),
    };
  });
  let rowStart = 0;
  for (let index = 0; index < spans.length; index += 1) {
    const span = spans[index];
    if (span === undefined) continue;
    if (span.rowStart !== rowStart) throw new RangeError(`${label} must cover rows contiguously`);
    const previous = spans[index - 1];
    if (
      previous !== undefined &&
      previous.rowIdStart + BigInt(previous.rowCount) === span.rowIdStart
    ) {
      throw new TypeError(`${label} contains adjacent spans that must be coalesced`);
    }
    rowStart = safeSum([rowStart, span.rowCount], `${label} row count`);
  }
  if (rowStart !== totalRows) throw new RangeError(`${label} must cover every row`);
  if (spans.length === 0) {
    if (totalRows !== 0 || rowIdStart !== 0n || rowIdEndExclusive !== 0n) {
      throw new RangeError(`${label} has an invalid empty row ID envelope`);
    }
    return spans;
  }
  const intervals = spans
    .map((span) => ({ start: span.rowIdStart, end: span.rowIdStart + BigInt(span.rowCount) }))
    .sort((left, right) => (left.start < right.start ? -1 : left.start > right.start ? 1 : 0));
  for (let index = 1; index < intervals.length; index += 1) {
    const previous = intervals[index - 1];
    const current = intervals[index];
    if (previous !== undefined && current !== undefined && current.start < previous.end) {
      throw new RangeError(`${label} cannot contain overlapping row IDs`);
    }
  }
  const minimum = intervals[0]?.start;
  const maximum = intervals[intervals.length - 1]?.end;
  if (minimum !== rowIdStart || maximum !== rowIdEndExclusive) {
    throw new RangeError(`${label} must match its row ID envelope`);
  }
  return spans;
}

function compareMergeSourceSegments(
  left: MergeCompactionSourceSegment,
  right: MergeCompactionSourceSegment,
): number {
  return (
    left.logicalOrder - right.logicalOrder ||
    left.committedVersion - right.committedVersion ||
    left.segmentId.localeCompare(right.segmentId)
  );
}

function normalizeCompactionOutputCursor(
  value: unknown,
  plan: CompactionRewritePlan,
): CompactionOutputCursor | null {
  if (plan.kind === "copy-v1") {
    if (value !== null) {
      throw new TypeError("A copy compaction cannot have an output cursor");
    }
    return null;
  }
  if (value === undefined) {
    throw new TypeError("An output-driven compaction requires an explicit output cursor");
  }
  if (typeof value !== "object" || value === null) {
    throw new TypeError("Rechunk output cursor must be an object");
  }
  const cursor: CompactionOutputCursor = {
    outputIndex: nonNegativeWholeNumber(
      Reflect.get(value, "outputIndex"),
      "Rechunk output cursor index",
    ),
    columnIndex: nonNegativeWholeNumber(
      Reflect.get(value, "columnIndex"),
      "Rechunk output cursor column index",
    ),
    rowStart: nonNegativeWholeNumber(
      Reflect.get(value, "rowStart"),
      "Rechunk output cursor row start",
    ),
  };
  if (cursor.outputIndex === plan.outputs.length) {
    if (cursor.columnIndex !== 0 || cursor.rowStart !== plan.totalRows) {
      throw new RangeError("A completed rechunk output cursor is not canonical");
    }
    return cursor;
  }
  const output = plan.outputs[cursor.outputIndex];
  if (output === undefined || cursor.columnIndex >= plan.columns.length) {
    throw new RangeError("Rechunk output cursor is outside the output plan");
  }
  if (cursor.rowStart !== output.rowStart) {
    throw new RangeError("Rechunk output cursor row start does not match its output window");
  }
  return cursor;
}

function validateContiguousRows(
  ranges: ReadonlyArray<{ rowStart: number; rowCount: number }>,
  totalRows: number,
  label: string,
): void {
  let rowStart = 0;
  for (const range of ranges) {
    if (range.rowStart !== rowStart) {
      throw new RangeError(`${label} must cover rows contiguously from zero`);
    }
    rowStart = safeSum([rowStart, range.rowCount], `${label} row count`);
  }
  if (rowStart !== totalRows) {
    throw new RangeError(`${label} must cover every planned row`);
  }
}

function compactionOutputCompression(value: unknown): CompactionOutputCompression {
  if (
    typeof value !== "string" ||
    !(compactionOutputCompressions as readonly string[]).includes(value)
  ) {
    throw new TypeError(`Invalid compaction output compression: ${String(value)}`);
  }
  return value as CompactionOutputCompression;
}

function simpleDataType(value: unknown): SimpleDataType {
  if (typeof value !== "string" || !(simpleDataTypes as readonly string[]).includes(value)) {
    throw new TypeError(`Invalid compaction column type: ${String(value)}`);
  }
  return value as SimpleDataType;
}

function segmentKind(value: unknown): SegmentKind {
  if (
    value !== "insert" &&
    value !== "upsert" &&
    value !== "update" &&
    value !== "delete" &&
    value !== "base"
  ) {
    throw new TypeError(`Invalid merge source segment kind: ${String(value)}`);
  }
  return value;
}

function compactionJobState(state: unknown): CompactionJobState {
  if (typeof state !== "string" || !(compactionJobStates as readonly string[]).includes(state)) {
    throw new TypeError(`Invalid compaction job state: ${String(state)}`);
  }
  return state as CompactionJobState;
}

function garbageCollectionJobState(state: unknown): GarbageCollectionJobState {
  if (
    typeof state !== "string" ||
    !(garbageCollectionJobStates as readonly string[]).includes(state)
  ) {
    throw new TypeError(`Invalid garbage collection job state: ${String(state)}`);
  }
  return state as GarbageCollectionJobState;
}

function normalizeGarbageCollectionCursor(value: unknown): GarbageCollectionCursor {
  if (typeof value !== "object" || value === null) {
    throw new TypeError("Garbage collection cursor must be an object");
  }
  return {
    manifestIndex: nonNegativeWholeNumber(
      Reflect.get(value, "manifestIndex"),
      "Garbage collection manifest cursor",
    ),
    segmentIndex: nonNegativeWholeNumber(
      Reflect.get(value, "segmentIndex"),
      "Garbage collection segment cursor",
    ),
    blockIndex: nonNegativeWholeNumber(
      Reflect.get(value, "blockIndex"),
      "Garbage collection block cursor",
    ),
    transactionIndex: nonNegativeWholeNumber(
      Reflect.get(value, "transactionIndex"),
      "Garbage collection transaction cursor",
    ),
  };
}

function garbageCollectionJobComplete(record: GarbageCollectionJobRecord): boolean {
  return (
    (record.discovery === undefined || record.discovery.phase === "complete") &&
    record.cursor.manifestIndex === record.candidateManifestVersions.length &&
    record.cursor.segmentIndex === record.candidateSegmentIds.length &&
    record.cursor.blockIndex === record.candidateBlockIds.length &&
    record.cursor.transactionIndex === record.candidateTransactionIds.length
  );
}

function uniqueIds(ids: unknown, label: string, sort: boolean): string[] {
  if (!Array.isArray(ids)) throw new TypeError(`${label}s must be an array`);
  const unique = [...new Set(ids.map((id: unknown) => validateStorageId(id, label)))];
  return sort ? unique.sort() : unique;
}

function orderedUniqueIds(ids: unknown, label: string): string[] {
  if (!Array.isArray(ids)) throw new TypeError(`${label}s must be an array`);
  const normalized = ids.map((id: unknown) => validateStorageId(id, label));
  if (new Set(normalized).size !== normalized.length) {
    throw new TypeError(`${label}s cannot contain duplicates`);
  }
  return normalized;
}

function uniqueWholeNumbers(values: unknown, label: string): number[] {
  if (!Array.isArray(values)) throw new TypeError(`${label}s must be an array`);
  return [...new Set(values.map((value: unknown) => nonNegativeWholeNumber(value, label)))].sort(
    (left, right) => left - right,
  );
}

function nullableId(id: unknown, label: string): string | null {
  return id === null ? null : validateStorageId(id, label);
}

function nonEmptyString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${label} cannot be empty`);
  }
  return value;
}

function validTimestamp(value: unknown, label: string): string {
  const timestamp = nonEmptyString(value, label);
  // Storage timestamps are internal coordination metadata, not SQL date values. Keeping the
  // canonical v1 form fixed at 24 UTF-8 bytes makes byte reservations exact (notably the
  // manifest-pruning tombstone) and avoids engine-dependent Date.parse spellings.
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(timestamp)) {
    throw new TypeError(`${label} must be canonical UTC ISO-8601`);
  }
  const milliseconds = Date.parse(timestamp);
  if (!Number.isFinite(milliseconds) || dateIsoString(new Date(milliseconds)) !== timestamp) {
    throw new TypeError(`${label} must be canonical UTC ISO-8601`);
  }
  return timestamp;
}

function nonNegativeWholeNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${label} must be a non-negative whole number`);
  }
  return value;
}

function nonNegativeBigInt(value: unknown, label: string): bigint {
  if (typeof value !== "bigint" || value < 0n) {
    throw new RangeError(`${label} must be a non-negative bigint`);
  }
  return value;
}

function positiveWholeNumber(value: unknown, label: string): number {
  const normalized = nonNegativeWholeNumber(value, label);
  if (normalized === 0) throw new RangeError(`${label} must be positive`);
  return normalized;
}

function positiveFiniteNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive finite number`);
  }
  return value;
}

function nonNegativeFiniteNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new RangeError(`${label} must be a non-negative finite number`);
  }
  return value;
}

function uint32(value: unknown, label: string): number {
  const normalized = nonNegativeWholeNumber(value, label);
  if (normalized > 0xffff_ffff) throw new RangeError(`${label} must fit in 32 bits`);
  return normalized;
}

function positiveUint32(value: unknown, label: string): number {
  const normalized = uint32(value, label);
  if (normalized === 0) throw new RangeError(`${label} must be positive`);
  return normalized;
}

function blockRowCount(value: unknown, label: string): number {
  const normalized = positiveUint32(value, label);
  if (normalized > MAX_BLOCK_ROW_COUNT) {
    throw new RangeError(`${label} exceeds the block format row limit`);
  }
  return normalized;
}

function safeSum(values: readonly number[], label: string): number {
  let total = 0;
  for (const value of values) {
    total += value;
    if (!Number.isSafeInteger(total)) throw new RangeError(`${label} exceeds the safe range`);
  }
  return total;
}

function safeProduct(left: number, right: number, label: string): number {
  const product = left * right;
  if (!Number.isSafeInteger(product)) throw new RangeError(`${label} exceeds the safe range`);
  return product;
}

/** Floors an integer-times-double product without rounding the binary double upward. */
export function floorWholeNumberProduct(left: number, right: number, label: string): number {
  if (!Number.isSafeInteger(left) || left < 0 || !Number.isFinite(right) || right < 0) {
    throw new RangeError(`${label} exceeds the safe range`);
  }
  const bytes = new ArrayBuffer(8);
  const view = new DataView(bytes);
  view.setFloat64(0, right, false);
  const high = view.getUint32(0, false);
  const low = view.getUint32(4, false);
  const exponentBits = (high >>> 20) & 0x7ff;
  const fraction = (BigInt(high & 0x000f_ffff) << 32n) | BigInt(low);
  const significand = exponentBits === 0 ? fraction : (1n << 52n) | fraction;
  const binaryExponent = exponentBits === 0 ? -1074 : exponentBits - 1023 - 52;
  let product = BigInt(left) * significand;
  if (binaryExponent >= 0) product <<= BigInt(binaryExponent);
  else product >>= BigInt(-binaryExponent);
  if (product > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new RangeError(`${label} exceeds the safe range`);
  }
  return Number(product);
}
