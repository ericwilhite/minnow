/** FTS checkpoint coverage is independent of the global catalog/ledger checkpoint.
 * Read the checked head once, not a history-sized map; its LSN covers every prior claim.
 */
import type { BlobRef, ByteBudget, ExtentReader } from "../storage/extents.js";
import { readFtsJournal } from "../format/fts-journal.js";
import { LogCorruptionError } from "../format/log-frame.js";
export async function ftsReplayCoverage(
  ref: BlobRef | null,
  budget: ByteBudget,
  reader: ExtentReader,
  signal?: AbortSignal,
): Promise<number> {
  if (ref === null) return 0;
  const held = await readFtsJournal(ref, budget, reader, signal);
  try {
    const p = held.value;
    if (p.kind !== "checkpoint")
      throw new LogCorruptionError("FTS coverage must name a checkpoint");
    return p.lsn;
  } finally {
    held.release();
  }
}
