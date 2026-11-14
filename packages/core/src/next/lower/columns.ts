import type { CatalogView } from "../access/catalog.js";
/**
 * Column types from DDL: the front end's column definition, compiled under
 * `compileWithDeclaredTypes` (engine/query.ts), to the new catalog's physical type and declared
 * descriptor (access/catalog.ts `ColumnDef.type` / `.declared`). The one place lowering applies
 * DECISIONS.md C5: a numeric column without its declared width is a lowering bug, never int64 or
 * float64 by accident.
 */
import type { CompiledStatement } from "../../engine/query.js";
import {
  physicalTypeOf,
  requireDeclaredWidths,
  type PhysicalType,
  type SqlTypeDescriptor,
} from "../types.js";
import { PlanningError, unsupported } from "./errors.js";

/** A column definition as CREATE TABLE and ALTER TABLE ADD COLUMN compile it. */
export type FrontEndColumn = Extract<
  CompiledStatement,
  { kind: "create-table" }
>["columns"][number];

export interface DeclaredColumnType {
  readonly type: PhysicalType;
  readonly declared: SqlTypeDescriptor;
}

/**
 * The physical type and declared descriptor of one DDL column. Throws `TypeError` (from
 * `requireDeclaredWidths`) for a numeric column compiled without `compileWithDeclaredTypes`, and
 * `PlanningError` for an unknown named type, or `UnsupportedPlanError` for an array of an
 * element type the new core does not name. Named enums resolve against the supplied catalog.
 */
export function declaredColumnType(
  column: FrontEndColumn,
  context: string,
  catalog?: CatalogView,
): DeclaredColumnType {
  const declared: SqlTypeDescriptor = {
    type: column.type,
    ...(column.integer === true ? { integer: true as const } : {}),
    ...(column.sqlDomain === undefined ? {} : { sqlDomain: column.sqlDomain }),
    ...(column.withoutTimeZone === true ? { withoutTimeZone: true as const } : {}),
    ...(column.enumValues === undefined ? {} : { enumValues: column.enumValues }),
    ...(column.declaredTypeName === undefined ? {} : { declaredTypeName: column.declaredTypeName }),
    ...(column.declaredLength === undefined ? {} : { declaredLength: column.declaredLength }),
  };
  requireDeclaredWidths(declared, context);
  // Unknown SQL names arrive as enum descriptors; resolve them against the immutable catalog.
  if (declared.sqlDomain?.kind === "enum" && declared.sqlDomain.values.length === 0) {
    const named = catalog?.typeByName(declared.sqlDomain.name);
    if (named === undefined)
      throw new PlanningError(`type "${declared.sqlDomain.name}" does not exist`, "42704");
    const resolved = {
      ...declared,
      sqlDomain: { ...declared.sqlDomain, values: [...named.labels] },
    };
    return { type: physicalTypeOf(resolved), declared: resolved };
  }
  try {
    return { type: physicalTypeOf(declared), declared };
  } catch (error) {
    if (!(error instanceof TypeError) || declared.sqlDomain?.kind !== "array") throw error;
    throw unsupported("columnType", `column ${context}: ${error.message}`);
  }
}
