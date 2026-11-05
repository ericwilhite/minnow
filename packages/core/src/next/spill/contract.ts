/**
 * The spill service: engine-owned temporary space for sort runs, join partitions, aggregate
 * partitions and DISTINCT streams. One service serves every operator.
 *
 * ## Never in the recovery path
 * - Spill state is in memory only: no root, catalog, log or checkpoint names spill space, and no
 *   spill write is flushed for durability.
 * - The space is one long-lived file per database (OPFS: `spill`, its sync handle opened at
 *   acquisition with the store's files), never deleted or created per query: browsers do not
 *   return quota on truncate (Chromium, WebKit), so space is reused inside the file. At startup,
 *   after the write path has recovered (format/manifest.ts "Startup order"), the whole file is
 *   free (`SpillService.sweep`), whatever it holds;
 *   only its free tail may be truncated. A crash, kill, cancellation or timeout mid-spill can cost
 *   disk space until the next open, never correctness or the ability to open.
 * - There are no per-query caps on files, pages or runs. The limits are the platform quota
 *   (`SpillQuotaError`) and the memory governor.
 *
 * ## Files and pages
 * - A `SpillFile` is one query's append-only logical stream inside the shared space (a list of
 *   extents the service allocates). `append` writes one typed spill page and returns its
 *   `SpillPageRef`; `read` returns the page's bytes after checking its checksum.
 * - A spill page is a batch in vector layout (vector.ts), not JSON:
 *     header: magic u32 "MNSP" | query id u32 | run u32 | partition u32 | sequence u32 |
 *             row count u32 | column count u32 | flags u32
 *     per column: layout u8, physical-type tag, decimal tier and uniform scale (or the per-slot
 *             scale array), then validity words,
 *             data buffers (int128 limbs, wide decimals as length-prefixed two's complement),
 *             varlen offsets and bytes, display scales and specials, each 8-byte aligned
 *   Pages are uncompressed (spill is bandwidth-bound and short-lived). The codec belongs to the
 *   executor track and must round-trip every `VectorData` variant exactly, locators included.
 * - Implementations batch small appends into large sequential writes (≥256 KiB) and may read
 *   several refs with one platform call. Buffers are charged to the writing operator's grant.
 *
 * ## Lifetime
 * - A file belongs to one query (`queryId`). `close` frees its extents. Cancellation and errors
 *   close every file the query opened; a query is restarted, never resumed.
 * - Only the engine owner touches spill space. Helpers receive page bytes by transfer.
 */
export interface SpillPageRef {
  readonly offset: number;
  readonly length: number;
  /** xxHash32 (seed 0) of the page bytes. */
  readonly checksum: number;
}

export interface SpillFile {
  readonly queryId: number;
  /** Bytes appended so far. */
  readonly length: number;
  append(page: Uint8Array): Promise<SpillPageRef>;
  /** The page's bytes, verified; throws `SpillCorruptionError` on a checksum mismatch. */
  read(ref: SpillPageRef): Promise<Uint8Array>;
  /** Frees the file's extents for reuse. Idempotent. */
  close(): Promise<void>;
}

export interface SpillService {
  create(queryId: number): Promise<SpillFile>;
  /** Marks the whole spill space free. Called once on open, after the owner's fence. */
  sweep(): Promise<void>;
  /** Spill bytes currently held (diagnostics). */
  readonly bytesInUse: number;
}

export class SpillCorruptionError extends Error {
  readonly code = "MINNOW_SPILL_CORRUPT";
  constructor(message: string) {
    super(message);
    this.name = "SpillCorruptionError";
  }
}

/** The platform refused space for spill. The query fails; the database is unaffected. */
export class SpillQuotaError extends Error {
  readonly code = "MINNOW_SPILL_QUOTA";
  constructor(message: string) {
    super(message);
    this.name = "SpillQuotaError";
  }
}

export const SPILL_PAGE_MAGIC = 0x50534e4d; // "MNSP"
