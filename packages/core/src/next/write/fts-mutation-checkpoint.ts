/** Inline invalidations become immutable journal history before global coverage advances. */
import type { BlobRef, ByteBudget, ExtentReader, ExtentWriter } from "../storage/extents.js";
import { ParseBudgetError } from "../format/manifest-codec.js";
import {
  readFtsJournal,
  writeFtsJournal,
  FTS_JOURNAL_ENTRIES,
  type FtsJournalEntry,
} from "../format/fts-journal.js";
import { LogCorruptionError } from "../format/log-frame.js";
import type { FtsMutations } from "./fts-mutations.js";
export async function checkpointFtsMutations(
  mutations: FtsMutations | null,
  after: number,
  through: number,
  transaction: number,
  previous: BlobRef | null,
  budget: ByteBudget,
  writer: ExtentWriter,
  reader: ExtentReader,
  signal?: AbortSignal,
): Promise<{ ref: BlobRef | null; lsn: number }> {
  if (mutations === null || through <= after) return { ref: previous, lsn: 0 };
  const charge = FTS_JOURNAL_ENTRIES * 512 + 512;
  if (!budget.allocate(charge)) throw new ParseBudgetError(charge);
  let entries: FtsJournalEntry[] = [];
  let ref = previous,
    ordinal = 0,
    lsn = 0;
  try {
    if (ref !== null) {
      const held = await readFtsJournal(ref, budget, reader, signal);
      try {
        if (held.value.kind !== "prepared" || held.value.transaction !== transaction)
          throw new LogCorruptionError("FTS mutation checkpoint has wrong preparation");
        ordinal = held.value.ordinal;
      } finally {
        held.release();
      }
    }
    const flush = async () => {
      if (entries.length === 0) return;
      ref = await writeFtsJournal(
        { kind: "prepared", transaction, ordinal: ++ordinal, previous: ref, entries },
        budget,
        writer,
        signal,
      );
      entries.length = 0;
    };
    for await (const e of mutations.pending(after, through, signal)) {
      // The commit LSN is a monotone epoch. It retains each table's exact invalidation
      // boundary even when several reference-free effects share one checkpoint page.
      entries.push({
        kind: "keyed",
        tableId: e.tableId,
        schemaVersion: e.schemaVersion,
        ftsId: 0,
        columnId: 0,
        generation: 0,
        mutationEpoch: e.lsn,
      });
      lsn = Math.max(lsn, e.lsn);
      if (entries.length === FTS_JOURNAL_ENTRIES) await flush();
    }
    await flush();
    return { ref, lsn };
  } finally {
    entries = [];
    budget.free(charge);
  }
}
