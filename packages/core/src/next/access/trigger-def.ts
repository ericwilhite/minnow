/** Portable trigger programs. SQL text is dialect-versioned; bindings/dependencies use stable IDs.
 * Body syntax follows the shipped runner; 1.0 firing order follows PostgreSQL names/frames.
 */
import type { CatalogView, TableDef } from "./catalog.js";
import type { PhysicalType } from "../types.js";
import { samePhysicalType } from "../types.js";
import { DurableFormatError } from "../durable-format.js";
import { StoreCorruptionError } from "../storage/contract.js";
export type TriggerEvent = "insert" | "update" | "delete";
export type TriggerTiming = "before" | "after";
/** Resolved (case-sensitive) identifiers only; unquoted SQL names are folded before this. */
export const TRIGGER_SYSTEM_PREFIX = "RI_ConstraintTrigger_";
export interface TriggerColumn {
  readonly columnId: number;
  readonly name: string;
  readonly type: PhysicalType;
}
export interface TriggerBinding extends TriggerColumn {
  readonly source: "new" | "old";
}
export interface TriggerBody {
  /** NEW/OLD occurrences are numbered placeholders, in textual occurrence order. */
  readonly sql: string;
  readonly kind: TriggerEvent;
  readonly targetTableId: number;
  readonly targetName: string;
  readonly bindings: readonly TriggerBinding[];
  /** All explicitly referenced target columns (assignment, predicates and INSERT list). */
  readonly columns: readonly TriggerColumn[];
  /** Explicit INSERT target IDs, in VALUES order; empty for DEFAULT VALUES. */
  readonly insertColumnIds: readonly number[];
  /** INSERT columns receiving explicit DEFAULT (not omitted columns). */
  readonly defaultColumnIds: readonly number[];
  /** Positional INSERT arity, otherwise null; later column additions must not rebind it. */
  readonly implicitArity: number | null;
  readonly assignedColumnIds: readonly number[];
  readonly typeIds: readonly number[];
  readonly sequenceIds: readonly number[];
}
export interface TriggerDef {
  readonly id: number;
  readonly name: string;
  readonly tableId: number;
  readonly event: TriggerEvent;
  readonly timing: TriggerTiming;
  readonly dialectVersion: number;
  readonly statements: readonly TriggerBody[];
}
const check = (yes: boolean, message: string): void => {
  if (!yes) throw new StoreCorruptionError(`Invalid trigger ${message}`);
};
const definitionFields = [
  "id",
  "name",
  "tableId",
  "event",
  "timing",
  "dialectVersion",
  "statements",
];
const bodyFields = [
  "sql",
  "kind",
  "targetTableId",
  "targetName",
  "bindings",
  "columns",
  "insertColumnIds",
  "defaultColumnIds",
  "implicitArity",
  "assignedColumnIds",
  "typeIds",
  "sequenceIds",
];
function fields(value: object, names: readonly string[]): void {
  check(
    Object.keys(value).every((k) => names.includes(k)),
    "unexpected semantic field",
  );
}
const positive = (n: unknown): n is number =>
  typeof n === "number" && Number.isSafeInteger(n) && n > 0;
const event = (s: unknown) => s === "insert" || s === "update" || s === "delete";
/** Structural validation also runs on WAL operations before their catalog references are applied. */
export function validateTriggerDef(def: TriggerDef): void {
  fields(def, definitionFields);
  check(positive(def.id) && positive(def.tableId), "identity");
  check(typeof def.name === "string" && def.name.length > 0, "name");
  check(!def.name.startsWith(TRIGGER_SYSTEM_PREFIX), "reserved system name");
  check(event(def.event) && ["before", "after"].includes(def.timing), "event/timing");
  check(positive(def.dialectVersion), "dialect version");
  if (def.dialectVersion !== 1)
    throw new DurableFormatError(`Unsupported trigger SQL dialect ${String(def.dialectVersion)}`);
  check(Array.isArray(def.statements) && def.statements.length > 0, "empty body");
  for (const body of def.statements) {
    fields(body, bodyFields);
    check(typeof body.sql === "string" && body.sql.length > 0 && event(body.kind), "statement");
    check(
      positive(body.targetTableId) &&
        body.targetTableId !== def.tableId &&
        typeof body.targetName === "string" &&
        body.targetName.length > 0,
      "target",
    );
    check(
      Array.isArray(body.bindings) &&
        Array.isArray(body.columns) &&
        Array.isArray(body.insertColumnIds) &&
        Array.isArray(body.defaultColumnIds) &&
        Array.isArray(body.assignedColumnIds) &&
        Array.isArray(body.typeIds) &&
        Array.isArray(body.sequenceIds),
      "dependency lists",
    );
    for (const col of [...body.bindings, ...body.columns])
      check(
        positive(col.columnId) && typeof col.name === "string" && col.name.length > 0,
        "column dependency",
      );
    for (const binding of body.bindings) {
      fields(binding, ["columnId", "name", "type", "source"]);
      check(
        (binding.source === "old" && def.event !== "insert") ||
          (binding.source === "new" && def.event !== "delete"),
        "OLD/NEW binding",
      );
    }
    for (const col of body.columns) fields(col, ["columnId", "name", "type"]);
    check(
      new Set(body.columns.map((c) => c.columnId)).size === body.columns.length,
      "duplicate target dependency",
    );
    check(
      new Set(body.insertColumnIds).size === body.insertColumnIds.length &&
        body.insertColumnIds.every(positive),
      "INSERT targets",
    );
    check(
      new Set(body.defaultColumnIds).size === body.defaultColumnIds.length &&
        body.defaultColumnIds.every((id) => body.insertColumnIds.includes(id)),
      "DEFAULT targets",
    );
    check(
      body.implicitArity === null ||
        (Number.isSafeInteger(body.implicitArity) && body.implicitArity >= 0),
      "positional INSERT arity",
    );
    for (const ids of [body.assignedColumnIds, body.typeIds, body.sequenceIds])
      check(new Set(ids).size === ids.length && ids.every(positive), "expression dependency IDs");
    if (body.kind !== "update")
      check(body.assignedColumnIds.length === 0, "non-UPDATE assignments");
    if (body.kind !== "insert")
      check(
        body.insertColumnIds.length === 0 &&
          body.defaultColumnIds.length === 0 &&
          body.implicitArity === null,
        "mutation INSERT metadata",
      );
  }
}
function columns(table: TableDef, refs: readonly TriggerColumn[]): void {
  for (const ref of refs) {
    const col = table.columns.find((c) => c.id === ref.columnId);
    check(
      col?.name === ref.name && samePhysicalType(col.type, ref.type),
      "column identity/type dependency",
    );
  }
}
/** Complete graph checks run for live DDL candidates and ordinary reopen, never just firing. */
export function validateTriggerCatalog(
  catalog: Pick<CatalogView, "tables" | "triggers" | "types" | "sequences">,
): void {
  const names = new Set<string>();
  let prior = 0;
  for (const def of catalog.triggers) {
    validateTriggerDef(def);
    check(def.id > prior && !names.has(def.name), "order or duplicate name");
    prior = def.id;
    names.add(def.name);
    const owner = catalog.tables.find((t) => t.id === def.tableId);
    check(owner !== undefined, "owner table");
    if (!owner) continue;
    for (const body of def.statements) {
      columns(owner, body.bindings);
      const target = catalog.tables.find((t) => t.id === body.targetTableId);
      check(target?.name === body.targetName, "target table identity");
      if (!target) continue;
      columns(target, body.columns);
      check(
        body.typeIds.every((id) => catalog.types.some((t) => t.id === id)) &&
          body.sequenceIds.every((id) => catalog.sequences.some((s) => s.id === id)),
        "expression object reference",
      );
      check(
        body.assignedColumnIds.every(
          (id) =>
            body.columns.some((c) => c.columnId === id) &&
            target.columns.some((c) => c.id === id && c.generatedSql === null),
        ),
        "UPDATE generated assignment",
      );
      check(
        body.kind === "insert" ? target.primaryKey === null : target.primaryKey !== null,
        "target key policy",
      );
      if (body.kind !== "insert") continue;
      const provided = new Set(body.insertColumnIds);
      check(
        body.insertColumnIds.every((id) => body.columns.some((c) => c.columnId === id)),
        "INSERT column dependency",
      );
      check(
        body.implicitArity === null ||
          (target.columns.length === body.implicitArity &&
            target.columns.every((c, i) => c.id === body.insertColumnIds[i])),
        "positional INSERT schema",
      );
      for (const col of target.columns) {
        const fallback =
          col.generatedSql !== null || col.defaultSql !== null || col.identitySequence !== null;
        if (!provided.has(col.id) || body.defaultColumnIds.includes(col.id))
          check(col.nullable || fallback, "required INSERT column without default");
      }
    }
  }
}
