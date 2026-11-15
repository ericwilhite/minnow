/** Accepted append ranges -> bounded immutable FTS parts. No replay of producer expressions.
 * Each part owns a disjoint logical-document range; commit identity groups the parts atomically.
 * Sorting is per document (<=4096 tokens); the transaction is never collected in a JS map.
 * Nonresident/unsupported preparation invalidates the accelerator, never a legal row write.
 */
import type { FtsWriteRenderer } from "../access/fts-write.js";
import { compatibleFtsRenderer, stableFtsRenderer } from "./fts-renderer.js";
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
import { pauseIfDue, taskClock, type TaskWork } from "../task.js";

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
export { isFtsKeyedUndo } from "./fts-undo.js";
import { isFtsKeyedUndo } from "./fts-undo.js";
/** A fixed-size synchronous hint only. Unknown/expired work takes the sliced commit route. */
export function hasFtsWrites(tx: TransactionImpl): boolean {
  if (taskClock.due() || tx.catalog.fts.length > 64) return true;
  const limit = Math.min(1024, tx.undo.length);
  for (let i = 0; i < limit; i++) {
    if ((i & 63) === 0 && taskClock.due()) return true;
    const e = tx.undo[i];
    if (
      e &&
      (isFtsKeyedUndo(e) ||
        (e.kind === "rows" && e.count > 0 && tx.catalog.fts.some((d) => d.tableId === e.tableId)))
    )
      return true;
  }
  return limit < tx.undo.length;
}
/** One bounded traversal per table, not one whole-undo search per inserted range. */
export async function ftsTableEligibility(
  tx: TransactionImpl,
  tableId: number,
  work?: TaskWork,
): Promise<"keyed" | "append" | "empty"> {
  let inserted = false;
  for (let i = 0; i < tx.undo.length; i++) {
    if ((i & 255) === 0) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
    }
    const e = tx.undo[i];
    if (!e || !("tableId" in e) || e.tableId !== tableId) continue;
    if (isFtsKeyedUndo(e)) return "keyed";
    if (e.kind === "rows" && e.count > 0) inserted = true;
  }
  work?.signal?.throwIfAborted();
  return inserted ? "append" : "empty";
}
export async function hasFtsKeyedWrites(tx: TransactionImpl, work?: TaskWork): Promise<boolean> {
  for (let i = 0; i < tx.undo.length; i++) {
    if ((i & 255) === 0) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
    }
    const e = tx.undo[i];
    if (e && isFtsKeyedUndo(e)) return true;
  }
  return false;
}
export async function hasFtsInserts(tx: TransactionImpl, work?: TaskWork): Promise<boolean> {
  if (tx.catalog.fts.length === 0) return false;
  for (const table of tx.catalog.tables) {
    const pending = pauseIfDue(work);
    if (pending !== undefined) await pending;
    if (
      tx.catalog.fts.some((d) => d.tableId === table.id) &&
      (await ftsTableEligibility(tx, table.id, work)) === "append"
    )
      return true;
  }
  return false;
}
export interface FtsPreparation {
  readonly ref: BlobRef;
  /** A stale capability cannot make these parts usable: every touched binding is invalidated. */
  readonly invalidatesAll: boolean;
}
export async function prepareFtsInserts(
  tx: TransactionImpl,
  transaction: number,
  budget: ByteBudget,
  writer: ExtentWriter,
  renderer: FtsWriteRenderer | null,
  rendererWorkspace: number,
  signal?: AbortSignal,
): Promise<FtsPreparation | null> {
  renderer = compatibleFtsRenderer(renderer);
  let invalidatesAll = renderer === null;
  const chain: { ref: BlobRef | null } = { ref: null };
  let ordinal = 0;
  const entries: FtsJournalEntry[] = [];
  const flush = async () => {
    if (entries.length === 0) return;
    chain.ref = await writeFtsJournal(
      { kind: "prepared", transaction, ordinal: ++ordinal, previous: chain.ref, entries },
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
  // Keyed history is reference-free in the same WAL body; only payload pages need staging.
  const work = signal ? { signal } : undefined;
  for (const table of tx.catalog.tables) {
    if (
      !tx.catalog.fts.some((d) => d.tableId === table.id) ||
      (await ftsTableEligibility(tx, table.id, work)) !== "append"
    )
      continue;
    for (const def of tx.catalog.fts) {
      if (def.tableId !== table.id) continue;
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
        touched = false,
        lastDocument = 0;
      for (let i = 0; i < tx.undo.length; i++) {
        if ((i & 255) === 0) {
          const pending = pauseIfDue(work);
          if (pending !== undefined) await pending;
        }
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
          // Accepted append undo is chronological in logical row-id order. A scalar high-water
          // handles overlapping/coalesced ranges without re-reading every prior undo record.
          const id = memtable.firstRowId + position;
          if (id <= lastDocument) continue;
          lastDocument = id;
          const chunk = memtable.chunks[column]?.[Math.floor(position / MEMORY_CHUNK_ROWS)];
          if (!chunk) {
            invalid = true;
            break;
          }
          // A capability may retire across awaited preparation IO. Never mix semantic versions
          // or use a newly enlarged workspace without its admission in this transaction.
          if (!stableFtsRenderer(renderer, rendererWorkspace)) {
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
      if (touched && renderer !== null && !stableFtsRenderer(renderer, rendererWorkspace))
        invalid = true;
      if (touched && invalid) await put({ ...identity, kind: "invalidate" });
    }
  }
  await flush();
  // The final journal append is itself awaited preparation IO. A retirement there must
  // dominate ALL touched bindings, including ones completed before that last write.
  if (!invalidatesAll && !stableFtsRenderer(renderer, rendererWorkspace)) {
    invalidatesAll = true;
    for (const table of tx.catalog.tables) {
      if (
        !tx.catalog.fts.some((d) => d.tableId === table.id) ||
        (await ftsTableEligibility(tx, table.id, work)) !== "append"
      )
        continue;
      for (const def of tx.catalog.fts) {
        if (def.tableId !== table.id) continue;
        await put({
          kind: "invalidate",
          tableId: table.id,
          schemaVersion: table.schemaVersion,
          ftsId: def.id,
          columnId: def.columnId,
          generation: 1,
          mutationEpoch: 0,
        });
      }
    }
    await flush();
  }
  return chain.ref === null ? null : { ref: chain.ref, invalidatesAll };
}
