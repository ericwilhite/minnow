/**
 * The extent space: how the format and write-path tracks place bytes in the store's data space,
 * publish them atomically with a root, read them back through one verified cache, and get the
 * space back when no root reaches it. Owned by the storage track; one per open database, in the
 * engine owner only. It knows extents and checksums, never pages, groups or tables.
 *
 * ```text
 *   writer = space.beginWrite("flush t7")      ─ one per intended publication
 *   b = writer.open(capacity)                  ─ reserve a contiguous extent
 *   refs = await b.append([page, page, …])     ─ blobs, back to back, checksummed by the space
 *   chunk = await b.seal()                     ─ trim the unused tail; readable from now on
 *   …more extents, metadata blobs (writer.write(parts) = open + append + seal)…
 *   gen = await space.publish(rootBytes, { writers: [writer], retired: [...] })
 *                                              ─ data flush, root slot, flush: atomic, durable
 *   or: await writer.abort()                   ─ everything it reserved is free again at once
 * ```
 *
 * ## Units
 * - An **extent** (`ExtentRef`) is the unit of allocation and reclaim: a contiguous range at a
 *   `DATA_ALIGN` offset, written once, never modified while any root or pin reaches it. A column
 *   chunk is one extent (format/manifest.ts). Several blobs may share an extent (a chunk's pages,
 *   or a chunk and its page directory); they are reclaimed together.
 * - A **blob** (`BlobRef`) is a checksummed byte range inside one extent: what a reader asks for
 *   (a page, a group root, a directory). The space computes every checksum it hands out.
 *
 * ## Writer lifecycle (`ExtentWriter`)
 * - `open` → `publishing` → `published`, or `open` → `aborted`. A writer publishes once, with
 *   every builder sealed. Space it holds is invisible to every root until its publish resolves; a
 *   crash before then leaves orphans that the next open's walk frees (no job records).
 * - Abort and discard free at once: no durable root reached that space, so no pin can.
 * - A rejected publish leaves its writers `indeterminate` and the space `failed` (see Publish).
 *
 * ## Publish (`ExtentSpace.publish`)
 * - Publishes run one at a time, in call order. Each one: every data write before it, then the
 *   root, become durable in that order (storage/contract.ts guarantee 1), then the next generation
 *   is current. It returns that generation.
 * - `retired` lists the extents the previous root reaches and the new root does not, each exactly
 *   once, as `ExtentRef`s the space handed out (any length that rounds to the same aligned size is
 *   accepted). Retiring space that is free, reserved or already retired is a `RangeError` before
 *   anything is written. Retired space is reused only when no pin of an older generation remains
 *   (`reclaim.ts`), and never before this publish is durable.
 * - **A rejected publish is indeterminate**: the root may or may not be durable. The space keeps
 *   the writers' extents and the retired extents allocated, sets `failed`, and refuses further
 *   writers and publishes with `SpaceFailedError`. Reads continue. The owner reopens: a new space
 *   walks whichever root is durable and frees the rest.
 *
 * ## Free space
 * - Rebuilt on open from the durable root: `[0, dataLength)` minus every extent `reachable(root)`
 *   lists. Crash orphans, aborted writers and unpublished extents are simply unreachable. Two
 *   different reachable extents that overlap, or one past the data length, fail the open with
 *   `StoreCorruptionError`.
 * - Kept in memory after that (free-space.ts): lowest-address first fit, freed runs coalesced.
 *   Allocation never fails for fragmentation; it grows the data space at its end. Quota surfaces as
 *   `StoreQuotaError` from the write that needed it.
 * - Freed space goes to `session.releaseData` (IndexedDB deletes the records; OPFS truncates only a
 *   free tail, because Chromium and WebKit do not return quota on truncate) and is reallocated only
 *   after that call resolves. The tail is returned only while no writer is open.
 * - Engine obligation (the space cannot check it): `retired` and `reachable` are exact. A
 *   reachable extent missing from `reachable` is reused and overwritten. `audit` checks both
 *   against a fresh walk; the write path's crash sweeps run it after every publish.
 *
 * ## Reads and the cache (`ExtentReader`)
 * - One cache of **encoded, verified blobs**, keyed by exact `(offset, length, checksum)`, charged
 *   to the memory governor through its cache handle (`memory/governor.ts` `registerCache`). The
 *   governor evicts it before any grant grows or any operator spills; it has no size dial.
 * - **Verify once.** A blob's xxHash32 is checked when its bytes come from storage; a mismatch is
 *   `StoreCorruptionError` naming the offset. Cached bytes are trusted.
 * - **Coalesced misses.** One `read` call plans its misses into slabs (slabs.ts: gaps ≤ 64 KiB,
 *   slabs ≤ 1 MiB), reads each slab with one platform call into one reused 1 MiB buffer
 *   (`readDataInto`), verifies, and copies each blob into its own exact-size buffer. A blob is never
 *   a view of a shared slab, so evicting it frees exactly its bytes.
 * - **Policy.** Segmented LRU: a blob read once enters a probation segment; a second read
 *   promotes it. A one-pass scan over a table larger than the cache churns only probation, so
 *   directories, roots, fences and hot pages stay. No hints, no dials.
 * - **Pins.** A lease pins its blobs until `release`; pinned blobs are not evictable. A blob the
 *   cache cannot admit (the governor has no room) is charged to the caller's `budget` for the
 *   lease's life. If the budget refuses too, `read` rejects with `ReadDeniedError(bytes)` and
 *   holds nothing: the caller borrows a progress loan of that size and retries.
 * - `peek` serves a cached blob synchronously, with no lease, for the warm point-read path
 *   (≤ 10 µs PK reads): the bytes are valid until the caller next yields. Cache buffers are never
 *   pooled or reused, so a peeked or leased array is never overwritten, only uncharged.
 * - Writes go around the cache. Freeing an extent drops its cached blobs in the same step, before
 *   the space can reallocate it.
 *
 * ## Memory
 * Fixed: one 1 MiB read slab and one 1 MiB write slab, charged through the cache handle at open
 * (open fails with `RangeError` under a cap too small for them). The free map, pin table and cache
 * index are small (O(free runs + pins + cached blobs)). Writers stage nothing else: `append`
 * copies small parts into the write slab and writes parts ≥ 256 KiB in place, so a chunk of 32
 * pages costs a few platform writes, not 32.
 *
 * ## Async, cancellation, errors
 * - Methods that touch storage are async; everything else (`beginWrite`, `open`, `pin`, `peek`,
 *   stats) is synchronous. On OPFS each async call does its I/O on open sync handles before its
 *   first await; on IndexedDB it is one transaction.
 * - `read` takes an `AbortSignal`; it rejects with the signal's reason between slabs and holds
 *   nothing. Writer work is cancelled by `abort`.
 * - `OwnershipLostError`, `StoreQuotaError` and `StoreCorruptionError` pass through from the
 *   session. After `OwnershipLostError` the space is `failed`.
 * - Misuse (appending past capacity, publishing an open builder, a foreign writer, using an
 *   aborted writer) is a `TypeError` or `RangeError`, thrown before any effect.
 *
 * ## Both backends fit
 * Everything above goes through `StoreSession` (`writeData`, `readDataInto`, `releaseData`,
 * `publish`): OPFS runs it as synchronous handle calls; IndexedDB as staged records, one
 * transaction per call, the epoch check inside each. The space never assumes a file.
 */
import type { StoreSession } from "./contract.js";
import type { RootPin } from "./reclaim.js";

export type { RootPin } from "./reclaim.js";

/** A contiguous byte range of the data space: the unit of allocation and reclaim. */
export interface ExtentRef {
  /** A multiple of `DATA_ALIGN`. */
  readonly offset: number;
  /** Bytes written (the reservation is this rounded up to `DATA_ALIGN`). */
  readonly length: number;
}

/** A checksummed byte range inside one extent: what readers ask for. */
export interface BlobRef {
  readonly offset: number;
  readonly length: number;
  /** xxHash32 (seed 0) of the referenced bytes. */
  readonly checksum: number;
}

/** An extent and the blobs appended to it, in append order. */
export interface WrittenExtent {
  readonly extent: ExtentRef;
  readonly blobs: readonly BlobRef[];
}

// ── Memory, structurally (storage sits below memory/; a `Grant` and the governor's cache handle
//    satisfy these as they are) ──────────────────────────────────────────────────────────────

/** A grant's suballocator (`memory/governor.ts` `Grant.allocate`/`free`). Never waits. */
export interface ByteBudget {
  allocate(bytes: number): boolean;
  free(bytes: number): void;
}

/** The cache's side of the governor (`MemoryGovernor.registerCache` returns this). */
export interface CacheMemory {
  charge(bytes: number): boolean;
  release(bytes: number): void;
}

/** What the governor calls on the cache (`EvictableCache`). */
export interface EvictableCacheLike {
  readonly evictableBytes: number;
  evict(bytes: number): number;
}

// ── Open ───────────────────────────────────────────────────────────────────────────────────────

/**
 * Every extent `root` reaches, supplied by the format track. It reads metadata through `reader`
 * (verified and cached). Duplicates are fine (shared extents). It must list every extent of every
 * structure the root reaches: catalog, group directories, group roots, chunks, deletion bitmaps,
 * statistics, index runs and their fences and Bloom filters.
 */
export type ReachableExtents = (
  root: Uint8Array,
  reader: ExtentReader,
) => AsyncIterable<ExtentRef> | Iterable<ExtentRef>;

export interface OpenSpaceOptions {
  readonly reachable: ReachableExtents;
  /** Registers the cache with the governor and returns its handle (`governor.registerCache`). */
  readonly registerCache: (cache: EvictableCacheLike) => CacheMemory;
  readonly signal?: AbortSignal;
}

/**
 * Opens the space over a session the owner holds: reads the durable root, walks it, builds the
 * free map, returns the free tail. The walk reads O(groups + index runs) metadata, coalesced.
 * Implemented in phase 2 as `openExtentSpace`.
 */
export type OpenExtentSpace = (
  session: StoreSession,
  options: OpenSpaceOptions,
) => Promise<ExtentSpace>;

// ── Reading ────────────────────────────────────────────────────────────────────────────────────

export interface ReadOptions {
  /** Charged for leased bytes the cache cannot hold, until `release`. */
  readonly budget: ByteBudget;
  readonly signal?: AbortSignal;
}

export interface ReadLease {
  /** Verified bytes, one per requested ref, in request order. Read-only; do not transfer. */
  readonly bytes: readonly Uint8Array[];
  /** Unpins the blobs and returns budget bytes. Idempotent. Do not use `bytes` after it. */
  release(): void;
}

export interface ExtentReader {
  /**
   * Verified bytes for `refs` (any order; duplicates share one array): cache hits, then the
   * misses in coalesced slabs. Readable: blobs of sealed extents, published or not.
   */
  read(refs: readonly BlobRef[], options: ReadOptions): Promise<ReadLease>;
  /** A cached blob, synchronously, or null. Valid until the caller yields; never pinned. */
  peek(ref: BlobRef): Uint8Array | null;
}

// ── Writing ────────────────────────────────────────────────────────────────────────────────────

export type WriterState = "open" | "publishing" | "published" | "aborted" | "indeterminate";

export interface ExtentWriter {
  readonly label: string;
  readonly state: WriterState;
  /** Sealed extents, in seal order. */
  readonly extents: readonly ExtentRef[];
  /** Bytes reserved by open builders plus bytes of sealed extents. */
  readonly bytes: number;
  /**
   * Reserves one contiguous extent of up to `capacity` bytes (> 0). Synchronous: it only
   * allocates. Fails only on misuse (writer not open).
   */
  open(capacity: number): ExtentBuilder;
  /** `open(total)`, `append(parts)`, `seal()` in one call. `parts` must not be empty. */
  write(parts: readonly Uint8Array[]): Promise<WrittenExtent>;
  /** Frees one of this writer's sealed extents before publication (an intermediate it dropped). */
  discard(extent: ExtentRef): Promise<void>;
  /**
   * Frees everything the writer reserved or sealed, after its in-flight writes settle. Idempotent
   * on an aborted writer; a `TypeError` once publishing has begun.
   */
  abort(): Promise<void>;
}

export interface ExtentBuilder {
  /** The reserved extent's offset (a multiple of `DATA_ALIGN`). */
  readonly offset: number;
  readonly capacity: number;
  /** Bytes appended so far. */
  readonly length: number;
  /**
   * Places `parts` back to back after the bytes already appended and returns one checksummed ref
   * per part (absolute offsets). The caller may reuse `parts`' buffers once it resolves. A
   * `RangeError` if they do not fit the remaining capacity (nothing is written). Refs become
   * readable at `seal`.
   */
  append(parts: readonly Uint8Array[]): Promise<BlobRef[]>;
  /**
   * Writes what is staged, trims the unused capacity back to free space, and returns the extent
   * and its blobs, or null when nothing was appended (the whole reservation is freed).
   */
  seal(): Promise<WrittenExtent | null>;
}

// ── The space ──────────────────────────────────────────────────────────────────────────────────

export interface PublishChange {
  /** Every writer whose extents the new root reaches: all of their sealed extents, no builder open. */
  readonly writers: readonly ExtentWriter[];
  /** Extents the previous durable root reaches and the new one does not, each exactly once. */
  readonly retired: readonly ExtentRef[];
}

export interface SpaceStats {
  /** The data space's allocated end (high-water mark). */
  readonly dataBytes: number;
  /** Free bytes below `dataBytes`. */
  readonly freeBytes: number;
  /** Reserved or sealed by writers not yet published or aborted. */
  readonly writerBytes: number;
  /** Retired but still reachable from a pinned older generation (garbage debt). */
  readonly waitingBytes: number;
  /** Freed and being released to the platform. */
  readonly releasingBytes: number;
  readonly cache: {
    readonly residentBytes: number;
    readonly pinnedBytes: number;
    readonly hits: number;
    readonly misses: number;
    /** Platform read calls and bytes they read (gaps included). */
    readonly platformReads: number;
    readonly bytesRead: number;
  };
}

export interface ExtentSpace extends ExtentReader {
  /** The durable root, or null for a new store. Replaced when a publish resolves. */
  readonly root: Uint8Array | null;
  /** The current generation (0 at open). */
  readonly generation: number;
  /** Set when a publish was rejected or ownership was lost; writes and publishes then refuse. */
  readonly failed: Error | null;
  readonly stats: SpaceStats;

  /** A new open writer. `SpaceFailedError` if the space failed. */
  beginWrite(label: string): ExtentWriter;
  /** Atomically and durably makes `root` current; returns its generation. See "Publish". */
  publish(root: Uint8Array, change: PublishChange): Promise<number>;
  /** Holds the current root's extents (and so its blobs) until released. Synchronous. */
  pin(): RootPin;
  /**
   * Walks the current root with `reachable` and checks the free map against it: every reachable
   * extent allocated and not waiting; every allocated extent reachable, held by a writer, waiting
   * on a pin, or being released. Throws `Error("Extent audit: …")` naming the first difference. Tests only.
   */
  audit(): Promise<void>;
  /** Drops the cache and returns its memory. Does not close the session. Idempotent. */
  close(): Promise<void>;
}

/** A publish was rejected (outcome unknown) or ownership was lost: reopen to continue writing. */
export class SpaceFailedError extends Error {
  readonly code = "MINNOW_SPACE_FAILED";
  constructor(cause: unknown) {
    super("The extent space failed after a rejected publish; reopen the store", { cause });
    this.name = "SpaceFailedError";
  }
}

/** Neither the cache nor the caller's budget could hold a read's bytes. Borrow, then retry. */
export class ReadDeniedError extends Error {
  readonly code = "MINNOW_READ_DENIED";
  constructor(readonly bytes: number) {
    super(`A read needs ${String(bytes)} bytes the budget does not have`);
    this.name = "ReadDeniedError";
  }
}
