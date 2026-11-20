/** Optional synchronous consumer of an already selected scan step of at most BATCH_ROWS.
 * The source loads and validates every needed page before canConsume. False is permitted
 * only before any accumulator transition. consume cannot suspend, retain bytes or retry.
 * Future exact summary consumers can use pageEntry eligibility before demanding payload.
 */
import type { ChunkPages } from "./scan.js";

export interface ScanPageColumn {
  readonly pages: ChunkPages | null;
}
/** Composable column bindings only. No payload/selection references are retained.
 * One lookup per aggregate argument, never per value; existing projections implement it.
 */
export interface PageColumnMap {
  pageColumn(index: number): number;
}
export interface ScanPageConsumer {
  canConsumePages(
    columns: readonly ScanPageColumn[],
    rows: Uint16Array,
    count: number,
    columnsMap?: PageColumnMap,
  ): boolean;
  consumePages(
    columns: readonly ScanPageColumn[],
    rows: Uint16Array,
    count: number,
    columnsMap?: PageColumnMap,
  ): undefined;
}
/** Bounded transient descriptors: one page-range tuple, borrowed DataView and loop metadata;
 * no payload backing allocation. A conservative 256-byte credit precedes eligibility and
 * transitions, then returns after the complete step, including fallback and thrown errors.
 */
export const PAGE_CONSUMER_METADATA_BYTES = 256;
