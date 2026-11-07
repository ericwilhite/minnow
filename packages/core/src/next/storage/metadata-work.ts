/** Shared S-slab views for private sort/merge cursors. Taking the slab precedes the mutation
 * barrier. Only control/output have owned buffers; concurrent native trimming still fits W.
 */
import { METADATA_PAGE_BYTES } from "./contract.js";
import type { MetadataJournal } from "./metadata-journal.js";
import { DEFAULT_PAIR_SLOTS, type PairSlots } from "./metadata-extents.js";
export interface MetadataWork {
  /** Exclusive scope from PageCache.withReadSlab; caller keeps it until all phases finish. */
  readonly slab: Uint8Array;
}
export function pairPhase<T>(
  journal: MetadataJournal,
  io: MetadataWork | undefined,
  phase: "merge",
  work: (slots: PairSlots) => Promise<T>,
): Promise<T> {
  if (io === undefined) return journal.workspace.run(phase, () => work(DEFAULT_PAIR_SLOTS));
  return journal.workspace.run("spool", () => {
    const frames = Array.from({ length: 6 }, (_, at) =>
      io.slab.subarray(
        640 * 1024 + at * METADATA_PAGE_BYTES,
        640 * 1024 + (at + 1) * METADATA_PAGE_BYTES,
      ),
    );
    return journal.workspace.withReadFrames(frames, (first) =>
      work({ left: first, right: first + 1, output: 1 }),
    );
  });
}
