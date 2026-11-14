/** Frozen backfill belongs to the catalog; a SQL NULL needs only one bounded scalar slot. */
import type { ColumnDef } from "../access/catalog.js";
import { allocateData, type ConstantVector, type FlatVector } from "../vector/vector.js";
export function missingScalar(column: ColumnDef): FlatVector {
  return (
    column.missing ?? {
      form: "flat",
      type: column.type,
      length: 1,
      validity: new Uint32Array(1),
      data: allocateData(column.type, 1),
    }
  );
}
export function missingVector(column: ColumnDef, length: number): ConstantVector {
  return { form: "constant", type: column.type, length, value: missingScalar(column) };
}
