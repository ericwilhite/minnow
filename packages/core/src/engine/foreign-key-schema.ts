import type { TableColumnRecord, TableRecord } from "../storage/types.js";
import type { ForeignKeyDefinition } from "./query.js";

/** The schema needed to prove a reference, including parents a migration has yet to create. */
export type ReferenceTable = Pick<
  TableRecord,
  "name" | "columns" | "primaryKeyColumnIds" | "uniqueKeyColumnId"
>;

/** One validation path for SQL creation and the final catalog of a declarative migration. */
export async function normalizeForeignKeys(
  owner: ReferenceTable,
  keys: readonly ForeignKeyDefinition[],
  findParent: (name: string) => Promise<ReferenceTable | undefined>,
) {
  const { name, columns } = owner;
  return Promise.all(
    keys.map(async (key) => {
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
      const parent = key.parentTable === name ? undefined : await findParent(key.parentTable);
      if (key.parentTable !== name && parent === undefined) {
        throw new TypeError(
          `FOREIGN KEY ${key.name} references a table that does not exist: ${key.parentTable}`,
        );
      }
      const parentColumns = parent === undefined ? columns : parent.columns;
      const parentPrimaryIds =
        parent === undefined
          ? (owner.primaryKeyColumnIds ??
            (owner.uniqueKeyColumnId === undefined ? [] : [owner.uniqueKeyColumnId]))
          : (parent.primaryKeyColumnIds ??
            (parent.uniqueKeyColumnId === undefined ? [] : [parent.uniqueKeyColumnId]));
      const parentPrimary = parentPrimaryIds
        .map((columnId) => parentColumns.find((column) => column.id === columnId))
        .filter((column): column is TableColumnRecord => column !== undefined && !column.hidden);
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
        requestedParentNames.some((columnName, index) => columnName !== parentPrimary[index]?.name)
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
          JSON.stringify(child.sqlDomain ?? null) !== JSON.stringify(parentKey.sqlDomain ?? null)
        ) {
          throw new TypeError(`FOREIGN KEY ${key.name} compares different SQL value domains`);
        }
        if (key.onDelete === "set null" && !child.nullable) {
          throw new TypeError(`FOREIGN KEY ${key.name} cannot SET NULL a NOT NULL column`);
        }
      });
      const parentNames = parentPrimary.map((column) => column.name);
      return {
        name: key.name,
        columns: childNames,
        parentTable: key.parentTable,
        parentColumns: parentNames,
        onDelete: key.onDelete,
        ...(key.enforced === false ? { enforced: false } : {}),
      };
    }),
  );
}
