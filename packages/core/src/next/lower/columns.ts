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
import { unsupported } from "./errors.js";

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
 * `UnsupportedPlanError` for a named enum type (`CREATE TYPE … AS ENUM`, which the new catalog
 * cannot declare yet) or an array of an element type the new core does not name.
 */
export function declaredColumnType(column: FrontEndColumn, context: string): DeclaredColumnType {
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
  if (declared.sqlDomain?.kind === "enum" && declared.sqlDomain.values.length === 0)
    throw unsupported("enumType", `column ${context} has enum type ${declared.sqlDomain.name}`);
  try {
    return { type: physicalTypeOf(declared), declared };
  } catch (error) {
    if (!(error instanceof TypeError) || declared.sqlDomain?.kind !== "array") throw error;
    throw unsupported("columnType", `column ${context}: ${error.message}`);
  }
}
