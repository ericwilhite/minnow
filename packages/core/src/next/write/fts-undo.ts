/** Accepted undo is the only source of native FTS mutation identity. */
import type { TransactionImpl } from "./transaction.js";
import type { BodyEffect } from "../format/log-frame.js";
export type FtsMutationEffect = Extract<BodyEffect, { kind: "ftsMutation" }>;
export function isFtsKeyedUndo(e: TransactionImpl["undo"][number]): boolean {
  return (
    e.kind === "memDelete" || e.kind === "groupDelete" || (e.kind === "rows" && e.ftsKeyed === true)
  );
}
/** Progress tokens let the streamed encoder/prepare paths slice the same exact traversal. */
export function* ftsMutationScan(tx: TransactionImpl): Generator<FtsMutationEffect | null> {
  for (const table of tx.catalog.tables) {
    for (let i = 0; i < tx.undo.length; i++) {
      if ((i & 1023) === 0) yield null;
      const e = tx.undo[i];
      if (e && isFtsKeyedUndo(e) && "tableId" in e && e.tableId === table.id) {
        yield { kind: "ftsMutation", tableId: table.id, schemaVersion: table.schemaVersion };
        break;
      }
    }
  }
}
