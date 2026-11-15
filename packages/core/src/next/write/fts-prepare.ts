/** Accepted append ranges -> bounded immutable FTS parts. No replay of producer expressions.
 * Each part owns a disjoint logical-document range; commit identity groups the parts atomically.
 * Sorting is per document (<=4096 tokens); the transaction is never collected in a JS map.
 * Nonresident/unsupported preparation invalidates the accelerator, never a legal row write.
 */
import type { FtsWriteRenderer } from "../access/fts-write.js";
import { compatibleFtsRenderer } from "./fts-renderer.js";
import { tokenize } from "../full-text.js";
import { writeFtsInsertDelta, FTS_APPEND_MINIMUM_BYTES } from "../format/fts-append.js";
import {
  FTS_JOURNAL_ENTRIES,
  FTS_JOURNAL_WORKSPACE_BYTES,
  writeFtsJournal,
  type FtsJournalEntry,
} from "../format/fts-journal.js";
import type { BlobRef, ByteBudget, ExtentWriter } from "../storage/extents.js";
import type { TransactionImpl } from "./transaction.js";
import { ALL } from "./transaction.js";
import { MEMORY_CHUNK_ROWS } from "./columns.js";
import { pauseIfDue } from "../task.js";

export const FTS_PREPARE_DESCRIPTOR_BYTES = 4096;
/** Optional complete step: refusal selects a compact invalidation, not another progress loan. */
export function ftsPrepareMinimum(renderer: FtsWriteRenderer): number {
  return (
    renderer.workspaceBytes +
    FTS_APPEND_MINIMUM_BYTES +
    FTS_JOURNAL_WORKSPACE_BYTES +
    FTS_JOURNAL_ENTRIES * FTS_PREPARE_DESCRIPTOR_BYTES +
    64 * 1024 +
    512 * 1024
  );
}
export function isFtsKeyedUndo(e: TransactionImpl["undo"][number]): boolean {
  return (
    e.kind === "memDelete" || e.kind === "groupDelete" || (e.kind === "rows" && e.ftsKeyed === true)
  );
}
export function hasFtsWrites(tx: TransactionImpl): boolean {
  return tx.undo.some(isFtsKeyedUndo) || hasFtsInserts(tx);
}
export function hasFtsInserts(tx: TransactionImpl): boolean {
  if (tx.catalog.fts.length === 0) return false;
  return tx.undo.some(
    (e) => e.kind === "rows" && e.count > 0 && tx.catalog.fts.some((d) => d.tableId === e.tableId),
  );
}
export async function prepareFtsInserts(
  tx: TransactionImpl,
  transaction: number,
  budget: ByteBudget,
  writer: ExtentWriter,
  renderer: FtsWriteRenderer | null,
  signal?: AbortSignal,
): Promise<BlobRef | null> {
  renderer = compatibleFtsRenderer(renderer);
  const rendererWorkspace = renderer?.workspaceBytes ?? 0;
  let previous: BlobRef | null = null,
    ordinal = 0;
  const entries: FtsJournalEntry[] = [];
  const flush = async () => {
    if (entries.length === 0) return;
    previous = await writeFtsJournal(
      { kind: "prepared", transaction, ordinal: ++ordinal, previous, entries },
      budget,
      writer,
      signal,
    );
    entries.length = 0;
  };
  const put = async (e: FtsJournalEntry) => {
    entries.push(e);
    if (entries.length === FTS_JOURNAL_ENTRIES) await flush();
  };
  // One durable table-wide keyed history, even if there is no FtsDef yet. Iterate existing
  // catalog/undo storage instead of allocating a transaction-sized notification set.
  for (const table of tx.catalog.tables) {
    let keyed = false;
    for (let i = 0; i < tx.undo.length; i++) {
      if ((i & 1023) === 0) await pauseIfDue(signal ? { signal } : undefined);
      const e = tx.undo[i];
      if (e && isFtsKeyedUndo(e) && "tableId" in e && e.tableId === table.id) {
        keyed = true;
        break;
      }
    }
    if (keyed)
      await put({
        kind: "keyed",
        tableId: table.id,
        schemaVersion: table.schemaVersion,
        ftsId: 0,
        columnId: 0,
        generation: 0,
        mutationEpoch: 0,
      });
  }
  for (const def of tx.catalog.fts) {
    const table = tx.catalog.table(def.tableId);
    if (!table) continue;
    if (tx.undo.some((e) => isFtsKeyedUndo(e) && "tableId" in e && e.tableId === table.id))
      continue;
    const column = table.columns.findIndex((c) => c.id === def.columnId);
    const identity = {
      tableId: def.tableId,
      schemaVersion: table.schemaVersion,
      ftsId: def.id,
      columnId: def.columnId,
      generation: 1,
      mutationEpoch: 0,
    };
    let invalid = false,
      touched = false;
    for (let i = 0; i < tx.undo.length; i++) {
      const entry = tx.undo[i];
      if (entry?.kind !== "rows" || entry.tableId !== def.tableId || entry.count === 0) continue;
      touched = true;
      const memtable = entry.memtable;
      // Schema changes or pressure staging may have retired the source chunk. Do not reread
      // producer expressions or guess identity. The atomic invalidation is the safe fallback.
      if (
        !renderer ||
        memtable.schemaVersion !== table.schemaVersion ||
        memtable.staged !== null ||
        memtable.published !== null ||
        column < 0
      ) {
        invalid = true;
        break;
      }
      for (let position = entry.before; position < entry.before + entry.count; position++) {
        await pauseIfDue(signal ? { signal } : undefined);
        if ((memtable.deletions.get(position) ?? Infinity) <= ALL) continue;
        let seen = false;
        for (let k = 0; k < i; k++) {
          const p = tx.undo[k];
          if (
            p?.kind === "rows" &&
            p.memtable === memtable &&
            position >= p.before &&
            position < p.before + p.count
          ) {
            seen = true;
            break;
          }
        }
        if (seen) continue;
        const chunk = memtable.chunks[column]?.[Math.floor(position / MEMORY_CHUNK_ROWS)];
        if (!chunk) {
          invalid = true;
          break;
        }
        // A capability may retire across awaited preparation IO. Never mix semantic versions
        // or use a newly enlarged workspace without its admission in this transaction.
        if (
          compatibleFtsRenderer(renderer) === null ||
          renderer.workspaceBytes !== rendererWorkspace
        ) {
          invalid = true;
          break;
        }
        // No await/admission while rendering a borrowed row. All token/sort storage is prepaid.
        let rendered: string | undefined, tokens: string[];
        try {
          rendered = renderer.render(chunk.view(), position % MEMORY_CHUNK_ROWS);
          tokens = rendered === undefined ? [] : tokenize(rendered);
        } catch (error) {
          if (!(error instanceof RangeError)) throw error;
          invalid = true;
          break;
        }
        tokens.sort();
        const id = memtable.firstRowId + position;
        async function* documents() {
          yield { id, length: tokens.length, renderable: rendered !== undefined };
        }
        async function* postings() {
          for (let k = 0; k < tokens.length;) {
            const term = tokens[k];
            if (term === undefined) throw new Error("Missing FTS token");
            let end = k + 1;
            while (tokens[end] === term) end++;
            yield { term, id, frequency: end - k };
            k = end;
          }
        }
        const delta = await writeFtsInsertDelta(documents(), postings(), budget, writer, signal);
        try {
          // The entry batch retains owned references after the append lease retires.
          await put({
            ...identity,
            kind: "insert",
            documents: delta.documents,
            terms: delta.terms,
          });
        } finally {
          delta.release();
        }
      }
      if (invalid) break;
    }
    // Also fence a change during the LAST payload/page await: invalidation dominates any
    // already-prepared parts, and is claimed atomically by the same row body.
    if (
      touched &&
      renderer !== null &&
      (compatibleFtsRenderer(renderer) === null || renderer.workspaceBytes !== rendererWorkspace)
    )
      invalid = true;
    if (touched && invalid) await put({ ...identity, kind: "invalidate" });
  }
  await flush();
  return previous;
}
