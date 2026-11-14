/** Stage3's accepted logical catalog subset. Validate raw fields BEFORE Catalog's maps can
 * hide duplicate identities or unknown fields, and BEFORE acquiring/fencing the target.
 * MCAT performs full native validation; this phase additionally restricts load semantics.
 */
import { SnapshotScopeError } from "./snapshot-export.js";
import { snapshotCheck } from "./snapshot-stage.js";
function record(value: unknown): Record<string, unknown> {
  snapshotCheck(
    typeof value === "object" && value !== null && !Array.isArray(value),
    "Invalid snapshot catalog object",
  );
  return value as Record<string, unknown>;
}
function fields(value: Record<string, unknown>, allowed: readonly string[]): void {
  for (const key of Object.keys(value))
    if (!allowed.includes(key))
      throw new SnapshotScopeError(`Unsupported snapshot catalog field ${key}`);
  for (const key of allowed)
    snapshotCheck(Object.hasOwn(value, key), `Missing snapshot catalog field ${key}`);
}
function list(value: unknown): readonly unknown[] {
  snapshotCheck(Array.isArray(value), "Invalid snapshot catalog list");
  return value;
}
function integer(value: unknown, zero = false): number {
  snapshotCheck(
    typeof value === "number" && Number.isSafeInteger(value) && value >= (zero ? 0 : 1),
    "Invalid snapshot catalog integer",
  );
  return value;
}
function name(value: unknown, seen: Set<string>): void {
  snapshotCheck(
    typeof value === "string" && value.length > 0 && !seen.has(value),
    "Duplicate/invalid snapshot catalog name",
  );
  seen.add(value);
}
export function validateSnapshotCatalog(value: unknown): void {
  const data = record(value);
  fields(data, [
    "version",
    "counters",
    "tables",
    "sequences",
    "views",
    "types",
    "fts",
    "triggers",
    "wireMetadata",
  ]);
  integer(data.version, true);
  const counters = record(data.counters),
    counterKeys = [
      "nextTableId",
      "nextColumnId",
      "nextIndexId",
      "nextConstraintId",
      "nextSequenceId",
      "nextViewId",
      "nextTypeId",
      "nextFtsId",
      "nextTriggerId",
    ];
  fields(counters, counterKeys);
  for (const key of counterKeys) integer(counters[key]);
  const used = new Map<string, Set<number>>(counterKeys.map((key) => [key, new Set<number>()]));
  const id = (value: unknown, key: string) => {
    const n = integer(value),
      seen = used.get(key);
    snapshotCheck(
      seen !== undefined && !seen.has(n) && integer(counters[key]) > n,
      "Duplicate snapshot catalog id or counter below its allocation floor",
    );
    seen.add(n);
    return n;
  };
  const sequences = list(data.sequences),
    views = list(data.views),
    types = list(data.types);
  // TODO(B6/B2 stage4): carry trigger/FTS/managed semantics or refuse, never silently drop.
  if (list(data.triggers).length || list(data.fts).length)
    throw new SnapshotScopeError("Stage3 trigger/FTS snapshot reconstruction is unsupported");
  // TODO(B6): complete sequence/view semantics through the shared MCAT grammar.
  if (sequences.length || views.length)
    throw new SnapshotScopeError("Stage3 snapshot sequences/views are unsupported");
  const typeNames = new Map<string, readonly unknown[]>();
  const names = new Set<string>();
  for (const item of types) {
    const type = record(item);
    fields(type, ["id", "name", "labels"]);
    id(type.id, "nextTypeId");
    name(type.name, names);
    const labels = list(type.labels),
      seen = new Set<string>();
    for (const label of labels) {
      snapshotCheck(
        typeof label === "string" && !seen.has(label),
        "Duplicate/invalid snapshot enum label",
      );
      seen.add(label);
    }
    typeNames.set(type.name as string, labels);
  }
  const tableNames = new Set<string>();
  for (const item of list(data.tables)) {
    const table = record(item);
    fields(table, [
      "id",
      "name",
      "schemaVersion",
      "columns",
      "primaryKey",
      "indexes",
      "foreignKeys",
      "checks",
    ]);
    id(table.id, "nextTableId");
    name(table.name, tableNames);
    integer(table.schemaVersion);
    if (
      table.primaryKey !== null ||
      list(table.indexes).length ||
      list(table.foreignKeys).length ||
      list(table.checks).length
    )
      throw new SnapshotScopeError("Stage3 snapshot indexes/constraints are unsupported");
    const columnNames = new Set<string>();
    for (const item of list(table.columns)) {
      const column = record(item);
      fields(column, [
        "id",
        "name",
        "type",
        "declared",
        "nullable",
        "defaultSql",
        "generatedSql",
        "identitySequence",
        "missing",
      ]);
      id(column.id, "nextColumnId");
      name(column.name, columnNames);
      snapshotCheck(typeof column.nullable === "boolean", "Invalid snapshot nullable flag");
      for (const key of ["defaultSql", "generatedSql"])
        snapshotCheck(
          column[key] === null || typeof column[key] === "string",
          "Invalid snapshot expression text",
        );
      snapshotCheck(
        column.identitySequence === null,
        "Unresolved snapshot identity sequence reference",
      );
      if (column.missing !== null)
        throw new SnapshotScopeError("Stage3 missing/backfill vectors are unsupported");
      const validateType = (value: unknown, depth: number): void => {
        snapshotCheck(depth <= 16, "Snapshot physical type nesting too deep");
        const type = record(value);
        snapshotCheck(typeof type.kind === "string", "Invalid snapshot physical type");
        const extras: Record<string, readonly string[]> = {
          int32: ["bits"],
          decimal: ["precision", "scale"],
          text: ["maxLength", "fixed"],
          json: ["binary"],
          timestamp: ["zoned"],
          enum: ["name", "labels"],
          array: ["element"],
        };
        const simple = [
          "bool",
          "int64",
          "float32",
          "float64",
          "date",
          "time",
          "interval",
          "bytes",
          "uuid",
        ];
        snapshotCheck(
          simple.includes(type.kind) || Object.hasOwn(extras, type.kind),
          "Unsupported snapshot physical type",
        );
        for (const key of Object.keys(type))
          if (key !== "kind" && !(extras[type.kind] ?? []).includes(key))
            throw new SnapshotScopeError(`Unsupported snapshot physical field ${key}`);
        if (type.kind === "enum") {
          const labels = list(type.labels),
            definition = typeNames.get(type.name as string);
          snapshotCheck(definition !== undefined, "Unresolved snapshot enum definition");
          snapshotCheck(
            labels.length === definition.length &&
              labels.every((label, i) => label === definition[i]),
            "Unresolved snapshot enum definition",
          );
        } else if (type.kind === "array") validateType(type.element, depth + 1);
      };
      validateType(column.type, 0);
    }
  }
}
