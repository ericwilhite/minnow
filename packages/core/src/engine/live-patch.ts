import type { QueryResult, QueryRow } from "../plan/model.js";
import type { LiveQueryDelivery } from "./live.js";
import { copyDate } from "../date-value.js";

export type LiveQueryPatch =
  | { readonly type: "reset"; readonly result: QueryResult }
  | {
      readonly type: "patch";
      /** Each next position's previous position, or -1 for a changed/new row. */
      readonly retained: Int32Array;
      readonly changedRows: ReadonlyArray<{ readonly index: number; readonly row: QueryRow }>;
    };

export interface LiveQueryPatchOptions {
  onPatch(patch: LiveQueryPatch, delivery: LiveQueryDelivery): void;
  onError?(error: unknown): void;
  onComplete?(): void;
}

function copyRow(row: QueryRow): QueryRow {
  const copy = { ...row };
  for (const key of Object.keys(copy)) {
    const value = copy[key];
    if (value instanceof Date) copy[key] = copyDate(value);
  }
  return copy;
}

/** Copy only changed payloads when provenance is available; resets establish a fresh baseline. */
export function createLiveQueryPatch(
  result: QueryResult,
  delivery: LiveQueryDelivery,
): LiveQueryPatch {
  if (delivery.retained === undefined || delivery.initial)
    return {
      type: "reset",
      result: {
        columns: [...result.columns],
        columnDomains: structuredClone(result.columnDomains),
        rows: result.rows.map(copyRow),
      },
    };
  const changedRows: Array<{ index: number; row: QueryRow }> = [];
  for (let index = 0; index < result.rows.length; index += 1) {
    const row = result.rows[index];
    if (row !== undefined && (delivery.retained[index] ?? -1) < 0)
      changedRows.push({ index, row: copyRow(row) });
  }
  return { type: "patch", retained: new Int32Array(delivery.retained), changedRows };
}
