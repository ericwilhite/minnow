/**
 * The memory governor contract: one cap for the whole engine owner, handed out as grants.
 *
 * ## Rules
 * 1. **One cap.** `capBytes` covers the page cache, operator state, I/O and output buffers,
 *    dictionaries, sketches and Bloom filters, and buffers lent to helpers. Default: a fraction of
 *    `navigator.deviceMemory` when reported, otherwise a fixed conservative value; the single
 *    public option overrides it. There are no other budgets.
 * 2. **Grant or spill, never refuse for size.** `Grant.tryGrow` either grants or returns false.
 *    A spillable operator that is refused spills (partitions, runs) and retries smaller; it never
 *    fails a query because of size. Only `SpillQuotaError` (disk) or cancellation fails a query.
 * 3. **Minimums are unconditional.** `reserveMinimum` always succeeds: the fixed cost an operator
 *    needs to make progress at all (one batch of buffers, ≤10 KB per column, the merge fan-in
 *    buffers). It counts against the cap and may exceed it; the governor then evicts the cache
 *    and asks spillable grants to shrink.
 * 4. **The cache yields first.** Before refusing any `tryGrow`, the governor evicts unpinned
 *    encoded pages from the page cache. Operators spill only after the cache is at its floor.
 * 5. **Bounded by batch, page or partition.** No reservation is ever sized by a table's or
 *    result's total row count.
 * 6. **Cooperative reclaim.** A grant's `onReclaimRequest` callback is how the governor asks a
 *    spillable operator to give memory back (spill a partition) at its next batch boundary.
 *
 * ## Accounting unit
 * Bytes of ArrayBuffer capacity (`byteLength` of what is allocated, not what is used), plus a
 * fixed `OBJECT_OVERHEAD_BYTES` per JS object an operator keeps per group, partition or run.
 * JS strings are not kept in operator state (text stays UTF-8), so they are not counted. The
 * governor accounts managed memory; it does not promise browser RSS (Gate D measures that).
 */
export const OBJECT_OVERHEAD_BYTES = 64;

export interface GrantOptions {
  /** Diagnostic label, e.g. "hash-aggregate#3". */
  readonly label: string;
  /** Can this consumer spill when refused? Non-spillable consumers may only reserve minimums. */
  readonly spillable: boolean;
}

export interface Grant {
  readonly label: string;
  /** Bytes currently held by this grant (minimums included). */
  readonly bytes: number;
  /** Always succeeds (rule 3). */
  reserveMinimum(bytes: number): void;
  /** Grows by `bytes` or returns false (rule 2). Only spillable grants may grow past minimums. */
  tryGrow(bytes: number): boolean;
  /** Returns bytes to the governor. */
  release(bytes: number): void;
  /** Rule 6: called when the governor wants `bytes` back; the operator spills at a boundary. */
  onReclaimRequest(callback: (bytes: number) => void): void;
  /** Releases everything. Idempotent. */
  close(): void;
}

/** The page cache's side of the governor (rule 4). */
export interface EvictableCache {
  /** Bytes held that could be evicted now. */
  readonly evictableBytes: number;
  /** Evicts at least `bytes` if possible; returns the bytes freed. */
  evict(bytes: number): number;
}

export interface MemoryGovernor {
  readonly capBytes: number;
  /** Bytes granted across all grants and the cache. */
  readonly usedBytes: number;
  /** Peak `usedBytes` since creation (tests assert the cap held). */
  readonly peakBytes: number;
  open(options: GrantOptions): Grant;
  registerCache(cache: EvictableCache): void;
}
