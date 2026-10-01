import { type WriterAdmission } from "./write-coordinator.js";
import {
  simpleDataTypes,
  type ColumnDefault,
  type ColumnGenerated,
  validateColumnDefault,
  validateEnumValues,
  type SecondaryIndexRecord,
  secondaryIndexColumnIds,
  MAX_CATALOG_NAME_CHARACTERS,
  type SimpleDataType,
  type SqlDomain,
  type TableColumnRecord,
  type TableRecord,
  TableInUseError,
  TableRecordConflictError,
  WriteConflictError,
} from "../storage/types.js";
import {
  DUAL_TABLE,
  compileCheckExpression,
  compileQuery,
  compileStatement,
  expressionColumnNames,
  inferBlockSchema,
  validateDefaultExpression,
  type CompiledQuery,
  type CompiledStatement,
  type ForeignKeyDefinition,
  type Expression,
  type SqlColumnSchema,
  type UniqueConstraintDefinition,
} from "./query.js";
import { normalizeSqlDomainValue } from "./sql-domains.js";
import { dateIsoString, dateMilliseconds } from "../date-value.js";
import { assertColumnDroppable, compileGeneratedColumnExpression } from "./schema.js";
import { UnknownTableError } from "./errors.js";
import { toCatalog } from "./catalog.js";
import type { BlockStore, ManifestSummary } from "../storage/types.js";

/** Unspellable SQL identifier used as the scalar locator for a declared composite primary key. */
const COMPOSITE_KEY_COLUMN_NAME = "\u0000minnow_primary_key";

export const ENUM_TYPE_PREFIX = "\u0000minnow_enum_type:";

export interface ColumnDefinition {
  name: string;
  type: SimpleDataType;
  /** SQL integer-domain guard; ordinary API number columns remain finite Float64 values. */
  integer?: true;
  /** PostgreSQL logical domain over the primitive storage type. */
  sqlDomain?: SqlDomain;
  nullable?: boolean;
  /** Fills omitted or SQL DEFAULT slots at insert time; never applied at read time. */
  defaultValue?: ColumnDefault;
  /** Stored expression over sibling columns; callers cannot assign this column. */
  generatedValue?: ColumnGenerated;
  /** String columns only: the closed set of values writes must draw from. */
  enumValues?: readonly string[];
  /** What rows written before this column existed read as, instead of NULL. */
  backfill?: boolean | number | string | Date;
}

export function normalizedColumnBackfill(
  column: Pick<ColumnDefinition, "name" | "type" | "integer" | "sqlDomain" | "enumValues">,
  value: boolean | number | string | Date,
): boolean | number | string | Date {
  if (column.sqlDomain !== undefined) {
    const normalized = normalizeSqlDomainValue(column.sqlDomain, value);
    if (normalized === null) throw new TypeError(`Backfill cannot be NULL: ${column.name}`);
    return normalized;
  }
  const valid =
    column.type === "datetime"
      ? value instanceof Date && Number.isFinite(dateMilliseconds(value))
      : column.type === "number"
        ? typeof value === "number" &&
          Number.isFinite(value) &&
          (column.integer !== true || Number.isSafeInteger(value))
        : typeof value === column.type;
  if (!valid) throw new TypeError(`Backfill does not fit column: ${column.name}`);
  if (
    column.enumValues !== undefined &&
    typeof value === "string" &&
    !column.enumValues.includes(value)
  ) {
    throw new TypeError(`Backfill must be one of the enum values: ${column.name}`);
  }
  return value;
}

export interface CreateTableInput {
  name: string;
  columns: readonly ColumnDefinition[];
  /** Row conditions every written row must satisfy (E141-06); each is a boolean SQL expression. */
  checks?: ReadonlyArray<{ name: string; sql: string }>;
  /** Marks the table as created from a schema, which lets a later migration drop it. */
  managed?: boolean;
  /** References to a parent PRIMARY/unique row-addressing key. */
  foreignKeys?: readonly ForeignKeyDefinition[];
  uniqueKey?: string;
  /** PostgreSQL composite PRIMARY KEY, backed by a hidden canonical tuple locator. */
  compositePrimaryKey?: readonly string[];
  /** Independently enforced PostgreSQL UNIQUE constraints. */
  uniqueConstraints?: readonly UniqueConstraintDefinition[];
}

/** Whether a view's body reads one table by name, at any depth of its query. */
function viewReadsTable(sql: string, table: string): boolean {
  try {
    return collectRealTableNames(compileQuery(sql)).includes(table);
  } catch {
    // A malformed stored dependency is corruption, not proof that destructive DDL is safe.
    return true;
  }
}

type QueryColumnShape = SqlColumnSchema;

export function catalogQuerySchemas(
  records: readonly TableRecord[],
): Map<string, QueryColumnShape[]> {
  return new Map(
    records.map((record) => [
      record.name,
      record.columns.map(({ name, type, integer, sqlDomain }) => ({
        name,
        type,
        ...(integer === true ? { integer: true as const } : {}),
        ...(sqlDomain === undefined ? {} : { sqlDomain }),
      })),
    ]),
  );
}

function querySchemasEqual(
  left: readonly QueryColumnShape[],
  right: readonly QueryColumnShape[],
): boolean {
  return (
    left.length === right.length &&
    left.every((column, index) => {
      const other = right[index];
      return (
        column.name === other?.name &&
        column.type === other.type &&
        column.integer === other.integer &&
        JSON.stringify(column.sqlDomain) === JSON.stringify(other.sqlDomain)
      );
    })
  );
}

export function assertDependentViewsKeepSchema(
  records: readonly TableRecord[],
  replaced: TableRecord,
  candidateSchemas: ReadonlyMap<string, QueryColumnShape[]>,
): void {
  for (const dependent of records) {
    if (
      dependent.id === replaced.id ||
      dependent.view === undefined ||
      !viewReadsTable(dependent.view.sql, replaced.name)
    ) {
      continue;
    }
    try {
      const inferred = inferBlockSchema(compileQuery(dependent.view.sql), candidateSchemas);
      if (!querySchemasEqual(inferred, dependent.columns)) {
        throw new TypeError("view output would change");
      }
    } catch {
      throw new TypeError(
        `Cannot replace ${replaced.name}: view ${dependent.name} depends on its current schema`,
      );
    }
  }
}

function assertViewDefinitionAcyclic(
  records: readonly TableRecord[],
  viewName: string,
  plan: CompiledQuery,
): void {
  const views = new Map(
    records.flatMap((record) =>
      record.view === undefined ? [] : ([[record.name, record.view.sql]] as const),
    ),
  );
  const reaches = (name: string, target: string, visited: Set<string>): boolean => {
    if (name === target) return true;
    if (visited.has(name)) return false;
    visited.add(name);
    const sql = views.get(name);
    if (sql === undefined) return false;
    let dependencies: string[];
    try {
      dependencies = collectRealTableNames(compileQuery(sql));
    } catch {
      throw new TypeError(`Stored view has an invalid definition: ${name}`);
    }
    return dependencies.some((dependency) => reaches(dependency, target, visited));
  };
  if (
    collectRealTableNames(plan).some((dependency) =>
      reaches(dependency, viewName, new Set<string>()),
    )
  ) {
    throw new TypeError(`View dependency cycle: ${viewName}`);
  }
}

export function triggerReferencesColumn(
  owner: TableRecord,
  trigger: NonNullable<TableRecord["triggers"]>[number],
  target: TableRecord,
  columnName: string,
  implicitInsertDepends = true,
): boolean {
  for (const statement of trigger.statements) {
    if (
      owner.id === target.id &&
      statement.bindings.some((binding) => binding.column === columnName)
    ) {
      return true;
    }
    let compiled: CompiledStatement;
    try {
      compiled = compileStatement(statement.sql);
    } catch {
      // A body that cannot be re-read cannot prove it is independent of the dropped column.
      return true;
    }
    if (
      (compiled.kind !== "insert" && compiled.kind !== "update" && compiled.kind !== "delete") ||
      compiled.table !== target.name
    ) {
      continue;
    }
    if (compiled.kind === "insert") {
      if (
        (compiled.columns.length === 0 && implicitInsertDepends) ||
        compiled.columns.includes(columnName)
      ) {
        return true;
      }
      continue;
    }
    const references = new Set<string>();
    for (const predicate of compiled.predicates) {
      for (const expression of [predicate.left, predicate.right]) {
        for (const name of expressionColumnNames(expression)) {
          references.add(name.split(".").at(-1) ?? name);
        }
      }
    }
    if (compiled.kind === "update") {
      for (const assignment of compiled.assignments) {
        references.add(assignment.column);
        for (const name of expressionColumnNames(assignment.expression)) {
          references.add(name.split(".").at(-1) ?? name);
        }
      }
    }
    if (references.has(columnName)) return true;
  }
  return false;
}

export function validateName(name: string, kind: string): string {
  const trimmed = name.trim();
  if (trimmed.length === 0) throw new TypeError(`${kind} name cannot be empty`);
  if (trimmed.length > MAX_CATALOG_NAME_CHARACTERS) {
    throw new TypeError(`${kind} name exceeds ${String(MAX_CATALOG_NAME_CHARACTERS)} characters`);
  }
  return trimmed;
}

export function collectRealTableNames(plan: CompiledQuery): string[] {
  const names = new Set<string>();
  const excluded = new Set<string>();
  const walkExpression = (expression: Expression): void => {
    if (expression.kind === "subquery" || expression.kind === "exists") {
      walk(expression.block);
      return;
    }
    if (expression.kind === "binary" || expression.kind === "condition") {
      walkExpression(expression.left);
      walkExpression(expression.right);
    } else if (expression.kind === "logical") {
      walkExpression(expression.left);
      walkExpression(expression.right);
    } else if (expression.kind === "not") walkExpression(expression.operand);
    else if (expression.kind === "case") {
      for (const branch of expression.branches) {
        walkExpression(branch.when);
        walkExpression(branch.then);
      }
      if (expression.otherwise !== undefined) walkExpression(expression.otherwise);
    } else if (expression.kind === "call") expression.arguments.forEach(walkExpression);
    else if (expression.kind === "list") expression.items.forEach(walkExpression);
  };
  const walk = (block: CompiledQuery): void => {
    for (const source of [block.base, ...block.joins]) {
      if (source.union !== undefined) source.union.blocks.forEach(walk);
      else if (source.windowed !== undefined) walk(source.windowed.block);
      else if (source.recursive !== undefined) {
        // The self-reference is bound per iteration, never loaded from storage.
        excluded.add(source.recursive.reference);
        walk(source.recursive.base);
        walk(source.recursive.step);
      } else if (source.derived === undefined) {
        if (source.table !== DUAL_TABLE) names.add(source.table);
      } else walk(source.derived);
    }
    for (const item of block.select) walkExpression(item.expression);
    block.groupBy.forEach(walkExpression);
    for (const join of block.joins) {
      if (join.on !== undefined) walkExpression(join.on);
    }
    for (const predicate of [...block.predicates, ...block.having]) {
      walkExpression(predicate.left);
      walkExpression(predicate.right);
    }
    for (const order of block.orderBy) walkExpression(order.expression);
  };
  walk(plan);
  for (const name of excluded) names.delete(name);
  return [...names];
}

interface CatalogMutationOptions {
  readonly store: BlockStore;
  readonly maxCommitRetries: number;
  readonly createId: () => string;
  readonly now: () => Date;
  readonly foreground: <T>(run: () => Promise<T>) => Promise<T>;
  readonly cancelCompactions: (tableId: string) => Promise<void>;
  readonly afterCommit: (manifest: ManifestSummary) => void;
  readonly invalidatePlans: () => void;
  readonly forgetColumn: (table: TableRecord, column: TableColumnRecord) => void;
  readonly forgetTable: (table: TableRecord) => void;
}
/** Catalog changes own their dependency proofs and bounded CAS retries. The caller holds
 * the writer admission; adapters still publish each transition atomically. */
export class CatalogMutations {
  readonly droppingTables = new Set<string>();
  readonly droppingFtsColumns = new Set<string>();
  readonly #foreground: CatalogMutationOptions["foreground"];
  readonly #store: CatalogMutationOptions["store"];
  readonly #createId: CatalogMutationOptions["createId"];
  readonly #now: CatalogMutationOptions["now"];
  readonly #invalidatePlans: CatalogMutationOptions["invalidatePlans"];
  readonly #maxCommitRetries: CatalogMutationOptions["maxCommitRetries"];
  readonly #cancelCompactions: CatalogMutationOptions["cancelCompactions"];
  readonly #afterCommit: CatalogMutationOptions["afterCommit"];
  readonly #forgetColumn: CatalogMutationOptions["forgetColumn"];
  readonly #forgetTable: CatalogMutationOptions["forgetTable"];
  constructor(options: CatalogMutationOptions) {
    this.#foreground = options.foreground;
    this.#store = options.store;
    this.#createId = options.createId;
    this.#now = options.now;
    this.#invalidatePlans = options.invalidatePlans;
    this.#maxCommitRetries = options.maxCommitRetries;
    this.#cancelCompactions = options.cancelCompactions;
    this.#afterCommit = options.afterCommit;
    this.#forgetColumn = options.forgetColumn;
    this.#forgetTable = options.forgetTable;
  }

  async createTable(_admission: WriterAdmission, input: CreateTableInput): Promise<void> {
    return this.#foreground(async () => {
      const name = validateName(input.name, "Table");
      if (input.columns.length === 0) throw new TypeError("A table needs at least one column");
      const enumDomains = new Map<string, SqlDomain>();
      for (const column of input.columns) {
        if (column.sqlDomain?.kind !== "enum" || column.sqlDomain.values.length > 0) continue;
        const record = await this.#store.getTableByName(
          `${ENUM_TYPE_PREFIX}${column.sqlDomain.name}`,
        );
        if (record?.enumType === undefined) {
          throw new TypeError(`Unsupported column type: ${column.sqlDomain.name}`);
        }
        enumDomains.set(column.name, {
          kind: "enum",
          name: record.enumType.name,
          values: [...record.enumType.values],
        });
      }
      const names = new Set<string>();
      const columns: TableColumnRecord[] = input.columns.map((column) => {
        const columnName = validateName(column.name, "Column");
        if (names.has(columnName)) throw new TypeError(`Duplicate column: ${columnName}`);
        names.add(columnName);
        if (!simpleDataTypes.includes(column.type)) {
          throw new TypeError(`Unsupported data type: ${column.type}`);
        }
        if (column.enumValues !== undefined && column.type !== "string") {
          throw new TypeError(`Enum values require a string column: ${columnName}`);
        }
        const sqlDomain = enumDomains.get(column.name) ?? column.sqlDomain;
        return {
          id: this.#createId(),
          name: columnName,
          type: column.type,
          ...(column.integer === true ? { integer: true } : {}),
          ...(sqlDomain === undefined ? {} : { sqlDomain: structuredClone(sqlDomain) }),
          nullable: column.nullable ?? false,
          ...(column.defaultValue === undefined ? {} : { defaultValue: column.defaultValue }),
          ...(column.generatedValue === undefined
            ? {}
            : { generatedValue: structuredClone(column.generatedValue) }),
          ...(column.enumValues === undefined
            ? {}
            : { enumValues: validateEnumValues(column.enumValues, columnName) }),
          ...(column.backfill === undefined
            ? {}
            : {
                backfill: normalizedColumnBackfill(
                  {
                    name: columnName,
                    type: column.type,
                    ...(column.integer === true ? { integer: true as const } : {}),
                    ...(sqlDomain === undefined ? {} : { sqlDomain }),
                    ...(column.enumValues === undefined ? {} : { enumValues: column.enumValues }),
                  },
                  column.backfill,
                ),
              }),
        };
      });
      if (input.uniqueKey !== undefined && input.compositePrimaryKey !== undefined) {
        throw new TypeError("A table cannot declare both scalar and composite primary keys");
      }
      const compositePrimaryColumns = (input.compositePrimaryKey ?? []).map((columnName) => {
        const column = columns.find((candidate) => candidate.name === columnName);
        if (column === undefined) {
          throw new TypeError(`PRIMARY KEY column not found: ${columnName}`);
        }
        return column;
      });
      if (
        input.compositePrimaryKey !== undefined &&
        (compositePrimaryColumns.length < 2 ||
          new Set(compositePrimaryColumns.map((column) => column.id)).size !==
            compositePrimaryColumns.length)
      ) {
        throw new TypeError("A composite PRIMARY KEY needs at least two distinct columns");
      }
      for (const column of compositePrimaryColumns) {
        if (column.nullable) throw new TypeError(`PRIMARY KEY cannot be nullable: ${column.name}`);
      }
      const hiddenCompositeKey =
        compositePrimaryColumns.length === 0
          ? undefined
          : ({
              id: this.#createId(),
              name: COMPOSITE_KEY_COLUMN_NAME,
              type: "string",
              nullable: false,
              hidden: true,
            } satisfies TableColumnRecord);
      if (hiddenCompositeKey !== undefined) columns.push(hiddenCompositeKey);
      const uniqueKeyColumn =
        hiddenCompositeKey ??
        (input.uniqueKey === undefined
          ? undefined
          : columns.find((column) => column.name === input.uniqueKey));
      if (input.uniqueKey !== undefined && uniqueKeyColumn === undefined) {
        throw new TypeError(`Unique key column not found: ${input.uniqueKey}`);
      }
      if (uniqueKeyColumn?.nullable === true) {
        throw new TypeError(`Unique key cannot be nullable: ${uniqueKeyColumn.name}`);
      }
      for (const column of columns) {
        if (column.generatedValue !== undefined) {
          if (column.defaultValue !== undefined || column.backfill !== undefined) {
            throw new TypeError(
              `A generated column cannot also have a default or backfill: ${name}.${column.name}`,
            );
          }
          compileGeneratedColumnExpression(name, column.name, column.generatedValue.sql, columns);
          if (column === uniqueKeyColumn || compositePrimaryColumns.includes(column)) {
            throw new TypeError(
              `Generated columns cannot be row-addressing keys: ${name}.${column.name}`,
            );
          }
        }
        if (column.defaultValue !== undefined) {
          validateColumnDefault(
            { ...column, isUniqueKey: column === uniqueKeyColumn },
            column.defaultValue,
          );
          if (column.defaultValue.kind === "literal" && column.sqlDomain !== undefined) {
            normalizeSqlDomainValue(column.sqlDomain, column.defaultValue.value);
          }
          if (column.defaultValue.kind === "expression") {
            validateDefaultExpression(column.defaultValue.sql, {
              name: `${name}.${column.name}`,
              type: column.type,
              ...(column.sqlDomain === undefined ? {} : { sqlDomain: column.sqlDomain }),
            });
          }
        }
      }
      const constraintNames = new Set<string>();
      const claimConstraintName = (rawName: string): string => {
        const constraintName = validateName(rawName, "Constraint");
        if (constraintNames.has(constraintName)) {
          throw new TypeError(`Constraint already exists: ${constraintName}`);
        }
        constraintNames.add(constraintName);
        return constraintName;
      };
      const foreignKeyNames = (input.foreignKeys ?? []).map((key) => claimConstraintName(key.name));
      const checkNames = (input.checks ?? []).map((check) => claimConstraintName(check.name));
      const uniqueConstraintNames = (input.uniqueConstraints ?? []).map((constraint) =>
        claimConstraintName(constraint.name),
      );
      const foreignKeys = await Promise.all(
        (input.foreignKeys ?? []).map(async (key, keyIndex) => {
          if (key.enforced === false && key.onDelete !== "restrict") {
            throw new TypeError(
              `Informational FOREIGN KEY ${key.name} cannot declare ON DELETE actions`,
            );
          }
          const childNames = key.columns ?? [key.column];
          const children = childNames.map((columnName) => {
            const column = columns.find((candidate) => candidate.name === columnName);
            if (column === undefined) {
              throw new TypeError(
                `FOREIGN KEY ${key.name} names a column this table has no: ${columnName}`,
              );
            }
            return column;
          });
          // Self-references are allowed, and then the parent is this very table, which does not
          // exist yet — its own declaration is the authority on the key.
          const parent =
            key.parentTable === name
              ? undefined
              : await this.#store.getTableByName(key.parentTable);
          if (key.parentTable !== name && parent === undefined) {
            throw new TypeError(
              `FOREIGN KEY ${key.name} references a table that does not exist: ${key.parentTable}`,
            );
          }
          const parentColumns = parent === undefined ? columns : parent.columns;
          const parentPrimaryIds =
            parent === undefined
              ? compositePrimaryColumns.length > 0
                ? compositePrimaryColumns.map((column) => column.id)
                : uniqueKeyColumn === undefined
                  ? []
                  : [uniqueKeyColumn.id]
              : (parent.primaryKeyColumnIds ??
                (parent.uniqueKeyColumnId === undefined ? [] : [parent.uniqueKeyColumnId]));
          const parentPrimary = parentPrimaryIds
            .map((columnId) => parentColumns.find((column) => column.id === columnId))
            .filter(
              (column): column is TableColumnRecord => column !== undefined && !column.hidden,
            );
          if (parentPrimary.length === 0) {
            throw new TypeError(
              `FOREIGN KEY ${key.name} references a table with no unique key: ${key.parentTable}`,
            );
          }
          const requestedParentNames =
            key.parentColumns ??
            (key.parentColumn === undefined
              ? parentPrimary.map((column) => column.name)
              : [key.parentColumn]);
          if (
            requestedParentNames.length !== parentPrimary.length ||
            requestedParentNames.some(
              (columnName, index) => columnName !== parentPrimary[index]?.name,
            )
          ) {
            throw new TypeError(
              `FOREIGN KEY ${key.name} must reference ${key.parentTable}'s primary key (${parentPrimary.map((column) => column.name).join(", ")})`,
            );
          }
          if (children.length !== parentPrimary.length) {
            throw new TypeError(
              `FOREIGN KEY ${key.name} has ${String(children.length)} child columns for ${String(parentPrimary.length)} parent columns`,
            );
          }
          children.forEach((child, index) => {
            const parentKey = parentPrimary[index];
            if (parentKey === undefined) {
              throw new TypeError(`FOREIGN KEY ${key.name} is missing a parent key column`);
            }
            if (child.type !== parentKey.type) {
              throw new TypeError(
                `FOREIGN KEY ${key.name} compares ${child.type} with ${parentKey.type}`,
              );
            }
            if ((child.integer === true) !== (parentKey.integer === true)) {
              throw new TypeError(
                `FOREIGN KEY ${key.name} compares an integer domain with an approximate number domain`,
              );
            }
            if (
              JSON.stringify(child.sqlDomain ?? null) !==
              JSON.stringify(parentKey.sqlDomain ?? null)
            ) {
              throw new TypeError(`FOREIGN KEY ${key.name} compares different SQL value domains`);
            }
            if (key.onDelete === "set null" && !child.nullable) {
              throw new TypeError(`FOREIGN KEY ${key.name} cannot SET NULL a NOT NULL column`);
            }
          });
          const parentNames = parentPrimary.map((column) => column.name);
          return {
            name: foreignKeyNames[keyIndex] ?? key.name,
            columns: childNames,
            parentTable: key.parentTable,
            parentColumns: parentNames,
            onDelete: key.onDelete,
            ...(key.enforced === false ? { enforced: false } : {}),
          };
        }),
      );
      const checks = (input.checks ?? []).map((check, checkIndex) => {
        // Compiling here means a constraint the engine could never evaluate is refused at
        // definition rather than on the first write.
        const referencedColumns = expressionColumnNames(
          compileCheckExpression(check.sql, check.name),
        );
        for (const reference of referencedColumns) {
          const pieces = reference.split(".");
          const columnName = pieces.at(-1) ?? reference;
          const qualifier = pieces.length > 1 ? pieces.slice(0, -1).join(".") : undefined;
          if (qualifier !== undefined && qualifier !== name) {
            throw new TypeError(`CHECK ${check.name} references another table: ${reference}`);
          }
          if (!columns.some((column) => column.name === columnName && !column.hidden)) {
            throw new TypeError(`CHECK ${check.name} names an unknown column: ${columnName}`);
          }
        }
        return { name: checkNames[checkIndex] ?? check.name, sql: check.sql };
      });
      const tableId = this.#createId();
      const currentVersion = (await this.#store.getCurrentManifest())?.version ?? -1;
      const secondaryIndexes: Record<string, SecondaryIndexRecord> = {};
      for (const [constraintIndex, constraint] of (input.uniqueConstraints ?? []).entries()) {
        const indexName = uniqueConstraintNames[constraintIndex] ?? constraint.name;
        const indexedColumns = constraint.columns.map((columnName) => {
          const column = columns.find(
            (candidate) => candidate.name === columnName && !candidate.hidden,
          );
          if (column === undefined) {
            throw new TypeError(`UNIQUE ${indexName} names an unknown column: ${columnName}`);
          }
          return column;
        });
        if (
          indexedColumns.length === 0 ||
          new Set(indexedColumns.map((column) => column.id)).size !== indexedColumns.length
        ) {
          throw new TypeError(`UNIQUE ${indexName} needs distinct columns`);
        }
        const indexId = this.#createId();
        secondaryIndexes[indexId] = {
          name: indexName,
          columnId: indexedColumns[0]?.id ?? "",
          columnIds: indexedColumns.map((column) => column.id),
          directions: indexedColumns.map(() => "asc" as const),
          unique: true,
          uniqueEnforced: true,
          termEncoding: "tuple-v2",
          storage: "postings-v1",
          storageColumnId: this.#createId(),
          locator: uniqueKeyColumn === undefined ? "row-id" : "key-hash-v1",
          state: "ready",
          buildFromVersion: currentVersion,
        };
      }
      await this.#store.addTable({
        id: tableId,
        name,
        columns,
        revision: 0,
        managed: input.managed === true,
        ...(foreignKeys.length === 0 ? {} : { foreignKeys }),
        ...(checks.length === 0 ? {} : { checks }),
        ...(uniqueKeyColumn === undefined ? {} : { uniqueKeyColumnId: uniqueKeyColumn.id }),
        ...(uniqueKeyColumn === undefined ? {} : { uniqueKeyLookupReady: true }),
        ...(compositePrimaryColumns.length === 0
          ? {}
          : { primaryKeyColumnIds: compositePrimaryColumns.map((column) => column.id) }),
        ...(Object.keys(secondaryIndexes).length === 0 ? {} : { secondaryIndexes }),
        createdAt: dateIsoString(this.#now()),
      });
    });
  }

  async createView(
    _admission: WriterAdmission,
    name: string,
    sql: string,
    options: { orReplace?: boolean; managed?: boolean },
  ): Promise<void> {
    return this.#foreground(async () => {
      const viewName = validateName(name, "View");
      const plan = compileQuery(sql);
      let pinnedExistingId: string | null | undefined;
      for (let attempt = 0; ; attempt += 1) {
        const proof = await this.readProof();
        const existing = proof.records.find((record) => record.name === viewName);
        const existingId = existing?.id ?? null;
        if (pinnedExistingId === undefined) pinnedExistingId = existingId;
        else if (pinnedExistingId !== existingId) {
          throw new TableRecordConflictError(viewName, 0, existing?.revision ?? null);
        }
        if (existing !== undefined) {
          if (existing.view === undefined) throw new TypeError(`Table already exists: ${viewName}`);
          if (options.orReplace !== true) throw new TypeError(`View already exists: ${viewName}`);
        }
        assertViewDefinitionAcyclic(proof.records, viewName, plan);
        const schemas = catalogQuerySchemas(proof.records);
        const inferred = inferBlockSchema(plan, schemas);
        if (inferred.length === 0) {
          throw new TypeError(`A view needs at least one column: ${viewName}`);
        }
        if (existing !== undefined) {
          schemas.set(viewName, inferred);
          assertDependentViewsKeepSchema(proof.records, existing, schemas);
        }
        const columns = inferred.map((column, index) => {
          const previous = existing?.columns[index];
          return {
            id:
              previous !== undefined && querySchemasEqual([column], [previous])
                ? previous.id
                : this.#createId(),
            name: column.name,
            type: column.type,
            ...(column.integer === true ? { integer: true as const } : {}),
            ...(column.sqlDomain === undefined ? {} : { sqlDomain: column.sqlDomain }),
            nullable: true,
          };
        });
        const view = { sql, managed: options.managed === true };
        try {
          if (existing !== undefined) {
            await this.#store.updateTable(existing.id, existing.revision, {
              columns,
              ftsColumns: null,
              secondaryIndexes: null,
              triggers: null,
              view,
              expectedCatalogEpoch: proof.catalogEpoch,
            });
          } else {
            await this.#store.addTable(
              {
                id: this.#createId(),
                name: viewName,
                columns,
                view,
                managed: false,
                revision: 0,
                createdAt: dateIsoString(this.#now()),
              },
              { expectedCatalogEpoch: proof.catalogEpoch },
            );
          }
          this.#invalidatePlans();
          return;
        } catch (error) {
          if (!(error instanceof TableRecordConflictError) || attempt >= this.#maxCommitRetries) {
            throw error;
          }
          if (existing !== undefined) {
            const current = await this.#store.getTable(existing.id);
            if (current?.revision !== existing.revision) throw error;
          }
        }
      }
    });
  }

  async dropView(
    _admission: WriterAdmission,
    name: string,
    options: { ifExists?: boolean },
  ): Promise<boolean> {
    return this.#foreground(async () => {
      let pinnedId: string | undefined;
      for (let attempt = 0; ; attempt += 1) {
        const proof = await this.readProof();
        const record = proof.records.find((candidate) => candidate.name === name);
        if (record?.view === undefined) {
          if (pinnedId !== undefined) {
            throw new TableRecordConflictError(pinnedId, 0, record?.revision ?? null);
          }
          if (record !== undefined) throw new TypeError(`Not a view: ${name}`);
          if (options.ifExists === true) return false;
          throw new Error(`View not found: ${name}`);
        }
        pinnedId ??= record.id;
        if (record.id !== pinnedId) {
          throw new TableRecordConflictError(pinnedId, 0, record.revision);
        }
        const dependent = proof.records.find(
          (candidate) =>
            candidate.id !== record.id &&
            candidate.view !== undefined &&
            viewReadsTable(candidate.view.sql, record.name),
        );
        if (dependent !== undefined) {
          throw new TypeError(`Cannot drop ${record.name}: view ${dependent.name} reads it`);
        }
        try {
          await this.#store.removeTable(record.id, record.revision, {
            expectedCatalogEpoch: proof.catalogEpoch,
          });
          this.#invalidatePlans();
          return true;
        } catch (error) {
          if (!(error instanceof TableRecordConflictError) || attempt >= this.#maxCommitRetries) {
            throw error;
          }
        }
      }
    });
  }

  /** Reads all catalog records under one epoch proof, retrying only the bounded read window. */
  async readProof(): Promise<{
    catalogEpoch: number;
    manifestVersion: number | null;
    records: TableRecord[];
  }> {
    for (let attempt = 0; ; attempt += 1) {
      const before = await this.#store.getCatalogProbe();
      const records = await this.#store.listTables();
      const after = await this.#store.getCatalogProbe();
      if (
        before.catalogEpoch === after.catalogEpoch &&
        before.manifestVersion === after.manifestVersion
      ) {
        return {
          catalogEpoch: before.catalogEpoch,
          manifestVersion: before.manifestVersion,
          records,
        };
      }
      if (attempt >= this.#maxCommitRetries) {
        throw new TableRecordConflictError("catalog", before.catalogEpoch, after.catalogEpoch);
      }
    }
  }

  async dropColumn(
    _admission: WriterAdmission,
    tableName: string,
    columnName: string,
    options: { ifExists?: boolean },
  ): Promise<boolean> {
    return this.#foreground(async () => {
      let pinnedTableId: string | undefined;
      let pinnedColumnId: string | undefined;
      for (let attempt = 0; ; attempt += 1) {
        const catalogProof = await this.#store.getCatalogProbe();
        const records = await this.#store.listTables();
        if ((await this.#store.getCatalogProbe()).catalogEpoch !== catalogProof.catalogEpoch) {
          if (attempt >= this.#maxCommitRetries) {
            throw new TableRecordConflictError(tableName, 0, null);
          }
          continue;
        }
        const table = records.find((record) => record.name === tableName);
        if (table === undefined || table.view !== undefined) {
          if (pinnedTableId !== undefined) {
            throw new TableRecordConflictError(pinnedTableId, 0, table?.revision ?? null);
          }
          if (table?.view !== undefined) throw new TypeError(`${tableName} is a view, not a table`);
          if (options.ifExists === true) return false;
          throw new UnknownTableError(tableName);
        }
        pinnedTableId ??= table.id;
        if (table.id !== pinnedTableId) {
          throw new TableRecordConflictError(pinnedTableId, 0, table.revision);
        }
        const column = table.columns.find((candidate) => candidate.name === columnName);
        if (column === undefined) {
          if (pinnedColumnId !== undefined) {
            throw new TableRecordConflictError(pinnedTableId, table.revision, table.revision);
          }
          if (options.ifExists === true) return false;
          throw new TypeError(`Column not found: ${tableName}.${columnName}`);
        }
        pinnedColumnId ??= column.id;
        if (column.id !== pinnedColumnId) {
          throw new TableRecordConflictError(pinnedTableId, table.revision, table.revision);
        }
        this.assertColumnDropSafe(records, table, column);
        const buildKey = `${table.id}/${column.id}`;
        this.droppingFtsColumns.add(buildKey);
        try {
          await this.#cancelCompactions(table.id);
          const manifest = await this.#store.dropTableColumn({
            tableId: table.id,
            columnId: column.id,
            expectedTableRevision: table.revision,
            expectedManifestVersion: await this.#store.getCurrentManifestVersion(),
            expectedCatalogEpoch: catalogProof.catalogEpoch,
            committedAt: dateIsoString(this.#now()),
          });
          this.#afterCommit(manifest);
          this.#forgetColumn(table, column);
          this.#invalidatePlans();
          return true;
        } catch (error) {
          if (
            (!(error instanceof TableRecordConflictError) &&
              !(error instanceof TableInUseError) &&
              !(error instanceof WriteConflictError)) ||
            attempt >= this.#maxCommitRetries
          ) {
            throw error;
          }
        } finally {
          this.droppingFtsColumns.delete(buildKey);
        }
      }
    });
  }

  /** Refuses every catalog edge a dropped column would leave dangling. */
  assertColumnDropSafe(
    records: readonly TableRecord[],
    table: TableRecord,
    column: TableColumnRecord,
  ): void {
    if (table.columns.length === 1) {
      throw new TypeError(`The last column cannot be dropped: ${table.name}.${column.name}`);
    }
    const catalogTable = toCatalog(records).tables.find(
      (candidate) => candidate.name === table.name,
    );
    const catalogColumn = catalogTable?.columns.find((candidate) => candidate.id === column.id);
    if (catalogTable === undefined || catalogColumn === undefined) {
      throw new Error(`Catalog column disappeared while dropping: ${table.name}.${column.name}`);
    }
    assertColumnDroppable(catalogTable, catalogColumn);
    const dependentIndex = Object.values(table.secondaryIndexes ?? {}).find((index) =>
      secondaryIndexColumnIds(index).includes(column.id),
    );
    if (dependentIndex !== undefined) {
      throw new TypeError(
        `Cannot drop ${table.name}.${column.name}: index ${dependentIndex.name} depends on it`,
      );
    }

    for (const owner of records) {
      if (owner.view !== undefined && viewReadsTable(owner.view.sql, table.name)) {
        const schemas = new Map(
          records.map((record) => [
            record.name,
            (record.id === table.id
              ? record.columns.filter((candidate) => candidate.id !== column.id)
              : record.columns
            ).map(({ name, type, integer, sqlDomain }) => ({
              name,
              type,
              ...(integer === true ? { integer: true as const } : {}),
              ...(sqlDomain === undefined ? {} : { sqlDomain }),
            })),
          ]),
        );
        try {
          const inferred = inferBlockSchema(compileQuery(owner.view.sql), schemas);
          const stored = owner.columns.map(({ name, type, integer, sqlDomain }) => ({
            name,
            type,
            ...(integer === true ? { integer: true as const } : {}),
            ...(sqlDomain === undefined ? {} : { sqlDomain }),
          }));
          if (JSON.stringify(inferred) !== JSON.stringify(stored)) {
            throw new TypeError("view output would change");
          }
        } catch {
          throw new TypeError(
            `Cannot drop ${table.name}.${column.name}: view ${owner.name} reads it`,
          );
        }
      }
      for (const trigger of owner.triggers ?? []) {
        if (triggerReferencesColumn(owner, trigger, table, column.name)) {
          throw new TypeError(
            `Cannot drop ${table.name}.${column.name}: trigger ${trigger.name} references it`,
          );
        }
      }
    }
  }

  async dropTable(
    _admission: WriterAdmission,
    tableName: string,
    options: { ifExists?: boolean },
  ): Promise<boolean> {
    return this.#foreground(async () => {
      let pinnedTableId: string | undefined;
      for (let attempt = 0; ; attempt += 1) {
        const catalogProof = await this.#store.getCatalogProbe();
        const records = await this.#store.listTables();
        if ((await this.#store.getCatalogProbe()).catalogEpoch !== catalogProof.catalogEpoch) {
          if (attempt >= this.#maxCommitRetries) {
            throw new TableRecordConflictError(tableName, 0, null);
          }
          continue;
        }
        const table = records.find((record) => record.name === tableName);
        if (table === undefined) {
          if (pinnedTableId !== undefined) {
            throw new TableRecordConflictError(pinnedTableId, 0, null);
          }
          if (options.ifExists === true) return false;
          throw new UnknownTableError(tableName);
        }
        pinnedTableId ??= table.id;
        if (table.id !== pinnedTableId) {
          throw new TableRecordConflictError(pinnedTableId, 0, table.revision);
        }
        if (table.view !== undefined) throw new TypeError(`${tableName} is a view; use DROP VIEW`);
        // Anything that would be left pointing at the table refuses the drop. The catalog-epoch
        // CAS below makes this complete-catalog proof serializable with concurrent DDL.
        for (const owner of records) {
          if (owner.id === table.id) continue;
          if (owner.view !== undefined && viewReadsTable(owner.view.sql, table.name)) {
            throw new TypeError(`Cannot drop ${table.name}: view ${owner.name} reads it`);
          }
          for (const key of owner.foreignKeys ?? []) {
            if (key.parentTable === table.name) {
              throw new TypeError(
                `Cannot drop ${table.name}: foreign key ${key.name} on ${owner.name} references it`,
              );
            }
          }
          for (const trigger of owner.triggers ?? []) {
            const writesHere = trigger.statements.some((triggerStatement) => {
              const compiled = compileStatement(triggerStatement.sql);
              return (
                (compiled.kind === "insert" ||
                  compiled.kind === "update" ||
                  compiled.kind === "delete") &&
                compiled.table === table.name
              );
            });
            if (writesHere) {
              throw new TypeError(
                `Cannot drop ${table.name}: trigger ${trigger.name} on ${owner.name} writes to it`,
              );
            }
          }
        }
        this.droppingTables.add(table.id);
        try {
          await this.#cancelCompactions(table.id);
          const manifest = await this.#store.dropTable({
            tableId: table.id,
            expectedTableRevision: table.revision,
            expectedManifestVersion: await this.#store.getCurrentManifestVersion(),
            expectedCatalogEpoch: catalogProof.catalogEpoch,
            committedAt: dateIsoString(this.#now()),
          });
          this.#afterCommit(manifest);
          this.#forgetTable(table);
          this.#invalidatePlans();
          return true;
        } catch (error) {
          if (
            (!(error instanceof TableRecordConflictError) &&
              !(error instanceof TableInUseError) &&
              !(error instanceof WriteConflictError)) ||
            attempt >= this.#maxCommitRetries
          ) {
            throw error;
          }
        } finally {
          this.droppingTables.delete(table.id);
        }
      }
    });
  }
}
