import type { QueryRow, QueryValue } from "@minnowdb/core";
import type { ConfirmRequest } from "../confirm.js";
import type { EditableTarget } from "../target.js";
import { describeValue } from "../values.js";
import type { ColumnInfo, TableInfo } from "./catalog.js";

/**
 * Why a table cannot be edited, or undefined when it can. The engine keys updates and deletes by
 * the unique key and refuses them outright without one, so a keyless table is browsable only —
 * which the panel says up front rather than discovering at save time. A view is browsable only
 * for a different reason: it has no rows of its own.
 */
export function editingBlockedReason(
  table: TableInfo,
  canWrite: boolean,
  targetSupportsWrites: boolean,
): string | undefined {
  if (!canWrite) return "Writes are turned off in these devtools.";
  if (!targetSupportsWrites) return "This target has no write API.";
  if (table.view !== undefined) {
    return `${table.name} is a view. Its rows come from a query, so there is nothing here to edit.`;
  }
  if (table.uniqueKey === undefined) {
    return `${table.name} has no unique key, so rows cannot be updated or deleted. Inserts still work.`;
  }
  return undefined;
}

/** The key value identifying a row, or undefined when the table has no key to identify it by. */
export function rowKey(table: TableInfo, row: QueryRow): QueryValue | undefined {
  if (table.uniqueKey === undefined) return undefined;
  return row[table.uniqueKey] ?? undefined;
}

export interface CellEdit {
  table: TableInfo;
  column: ColumnInfo;
  key: QueryValue;
  from: QueryValue;
  to: QueryValue;
}

/**
 * Writes go through the keyed batch API rather than generated SQL. It is exact — one named key,
 * one named column — it reports how many rows it actually touched, and it sidesteps the explorer's
 * day-only SQL literal encoder, so a datetime is written to the millisecond.
 */
export async function applyCellEdit(target: EditableTarget, edit: CellEdit): Promise<number> {
  const result = await target.updateBatch(edit.table.name, {
    keys: [edit.key],
    changes: { [edit.column.name]: [edit.to] },
  });
  return result.updatedRowCount;
}

export async function applyDelete(
  target: EditableTarget,
  table: TableInfo,
  key: QueryValue,
): Promise<number> {
  const result = await target.deleteBatch(table.name, { keys: [key] });
  return result.deletedRowCount;
}

export async function applyDeleteMany(
  target: EditableTarget,
  table: TableInfo,
  keys: readonly QueryValue[],
): Promise<number> {
  const result = await target.deleteBatch(table.name, { keys: [...keys] });
  return result.deletedRowCount;
}

export async function applyInsert(
  target: EditableTarget,
  table: TableInfo,
  values: Record<string, QueryValue>,
): Promise<number> {
  const result = await target.insert(table.name, values);
  return result.rowCount;
}

/** Confirmation copy. Every change is spelled out before it runs, never summarised after. */
export function confirmCellEdit(edit: CellEdit): ConfirmRequest {
  return {
    title: `Update 1 row in ${edit.table.name}`,
    facts: [
      ["table", edit.table.name],
      ["key", `${edit.table.uniqueKey ?? "key"} = ${describeValue(edit.key)}`],
      [edit.column.name, `${describeValue(edit.from)} → ${describeValue(edit.to)}`],
      ["call", `updateBatch('${edit.table.name}', { keys, changes })`],
    ],
    confirmLabel: "Update row",
  };
}

export function confirmDelete(table: TableInfo, key: QueryValue, row: QueryRow): ConfirmRequest {
  // A short read of the row, so the confirmation identifies it by more than its key.
  const preview = table.columns
    .filter((column) => !column.isUniqueKey)
    .slice(0, 3)
    .map((column) => describeValue(row[column.name] ?? null))
    .join(" · ");
  return {
    title: `Delete 1 row from ${table.name}`,
    facts: [
      ["table", table.name],
      ["key", `${table.uniqueKey ?? "key"} = ${describeValue(key)}`],
      ...(preview.length > 0 ? ([["row", preview]] as Array<[string, string]>) : []),
      ["call", `deleteBatch('${table.name}', { keys })`],
    ],
    confirmLabel: "Delete row",
    destructive: true,
    warning: "Rows in other tables that reference this one are not touched.",
  };
}

export function confirmDeleteMany(table: TableInfo, keys: readonly QueryValue[]): ConfirmRequest {
  const shown = keys.slice(0, 5).map(describeValue).join(", ");
  return {
    title: `Delete ${String(keys.length)} rows from ${table.name}`,
    facts: [
      ["table", table.name],
      ["rows", String(keys.length)],
      ["keys", keys.length > 5 ? `${shown}, …` : shown],
      ["call", `deleteBatch('${table.name}', { keys })`],
    ],
    confirmLabel: `Delete ${String(keys.length)} rows`,
    destructive: true,
    warning: "Rows in other tables that reference these are not touched.",
  };
}

export function confirmInsert(
  table: TableInfo,
  values: Record<string, QueryValue>,
): ConfirmRequest {
  return {
    title: `Insert 1 row into ${table.name}`,
    facts: [
      ["table", table.name],
      ...Object.entries(values).map(
        ([column, value]) => [column, describeValue(value)] as [string, string],
      ),
      ["call", `insert('${table.name}', row)`],
    ],
    confirmLabel: "Insert row",
  };
}
