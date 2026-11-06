/**
 * The extent space: how the format and write-path tracks place bytes in the store's data space,
 * publish them atomically with a root, read them back through one verified cache, and get the
 * space back when no root reaches it. Owned by the storage track; one per open database, in the
 * engine owner only. It knows extents and checksums, never pages, groups or tables.
 *
 * ```text
 *   writer = space.beginWrite("flush t7")      ─ one per intended publication
 *   b = writer.open(estimate)                  ─ reserve a contiguous extent
 *   pending = await b.append([page, page, …])  ─ blobs (≤ 1 MiB each), checksummed by the space
 *   b.extend(more)                             ─ more capacity: in place, or relocated at seal
 *   chunk = await b.seal()                     ─ the extent and its BlobRefs; readable from now on
 *   …more extents, metadata blobs (writer.write(parts) = open + append + seal)…
 *   gen = await space.publish(rootBytes, { writers: [writer], retired: [...] })
 *                                              ─ data flush, root slot, flush: atomic, durable
 *   or: await writer.abort()                   ─ everything it reserved is free again at once
 * ```
 *
 * ## The write path's storage dependency (lead ruling R7)
 * The write path (access/, track B3) reaches the data space only through this interface; it gets
 * no `readData` adapter and keeps no free map of its own:
 * - **Reads:** `read` (leased, verified, cached, budget-charged) and `peek` (synchronous cache hit,
 *   for the end-to-end synchronous point-read path of ruling R6). The root: `root`.
 * - **Writes:** writers for memtable flushes, index runs, merges, catalog and ledger blobs.
 * - **Publish:** `publish(root, { writers, retired })`, the only way a root changes. The log stays
 *   on the session (`appendLog`, `flushLog`, `resetLog`…): the write path resets the log only after
 *   the publish that covers it has resolved.
 * - **Snapshots:** `pin(label)` under each access/ snapshot; `stats` for backpressure (garbage
 *   debt, oldest pin).
 * - **Startup** (storage/contract.ts "Startup order", rulings R13/R16.6): fence → store session →
 *   extent space (walks the current and previous roots) → write path (the single log scan,
 *   truncation and replay) → spill sweep → ready. The space reads no log.
 *
 * ## Units
 * - An **extent** (`ExtentRef`) is the unit of allocation and reclaim: a contiguous range at a
 *   `DATA_ALIGN` offset, written once, never modified while any kept root or pin reaches it. A
 *   column chunk is one extent (format/manifest.ts). Several blobs may share an extent (a chunk's
 *   pages, or a chunk and its page directory); they are reclaimed together. Extents have no size
 *   cap.
 * - A **blob** (`BlobRef`) is a checksummed byte range inside one extent: what a reader asks for
 *   (a page, a group root, a directory). It carries its extent and its offset within it (ruling
 *   R15.1), so reclaim and the open walk take a blob's extent from the ref, never infer it from
 *   an address. Small blobs (metadata, small chunks) share packed extents by the format's packing
 *   rule (format/manifest.ts); they are reclaimed with their extent. The space computes every
 *   checksum it hands out.
 * - **Blobs are at most `MAX_BLOB_BYTES` (1 MiB, ruling R8).** A larger structure (a catalog, a
 *   long group list, an index run's entries) is split across several blobs by its format. So every
 *   blob fits one read slab, and no single read pins an unbounded buffer.
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
 *   anything is written.
 * - **Two-generation reclaim (ruling R8, reclaim.ts).** Space retired by the publish of generation
 *   g is reused only when the *next* publish (g+1) is durable **and** no pin below g remains.
 *   Until g+1 overwrites the other root slot, that slot holds g−1, the root a reopen falls back to
 *   if g's slot is lost; so the current and previous roots always read intact. The cost is that
 *   garbage waits one publish (`stats.waitingBytes`); the write path's idle maintenance publishes
 *   when that debt matters.
 * - **A rejected publish is indeterminate**: the root may or may not be durable. The space keeps
 *   the writers' extents and the retired extents allocated, sets `failed`, and refuses further
 *   writers and publishes with `SpaceFailedError`. Reads continue. The owner reopens: a new space
 *   walks whichever roots are durable and frees the rest.
 *
 * ## Free space
 * - Rebuilt on open from **both kept roots**: `[0, dataLength)` minus every extent
 *   `reachable(current)` or `reachable(previous)` lists (`StoreSession.readPreviousRoot`). Extents
 *   only the previous root reaches are retired at generation 0 and reused after the first publish.
 *   Crash orphans, aborted writers and unpublished extents are simply unreachable. Two different
 *   reachable extents that overlap, or one past the data length, fail the open with
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
 * - One cache of **encoded, verified blobs**, keyed by exact `(address, length, checksum)`, charged
 *   to the memory governor through its cache handle (`memory/governor.ts` `registerCache`). The
 *   governor evicts it before any grant grows or any operator spills; it has no size dial. Nothing
 *   decoded is cached here: decoded dictionaries and pages belong to the query that decoded them
 *   (ruling R5).
 * - **Verify once.** A blob's xxHash32 is checked when its bytes come from storage; a mismatch is
 *   `StoreCorruptionError` naming the offset. Cached bytes are trusted.
 * - **Bounded per call (ruling R8).** One `read` asks for at most `MAX_READ_BYTES` (4 MiB) of
 *   blobs in total; more is a `RangeError` before any I/O (callers batch).
 * - **Coalesced misses.** A call plans its misses into slabs (slabs.ts: gaps ≤ 64 KiB, slabs ≤
 *   1 MiB, and a slab never exceeds 1 MiB because no blob does), reads each slab with one platform
 *   call into one reused 1 MiB buffer (`readDataInto`), verifies, and copies each blob into its own
 *   exact-size buffer. A blob is never a view of a shared slab, so evicting it frees exactly its
 *   bytes.
 * - **Policy.** Segmented LRU: a blob read once enters a probation segment; a second read
 *   promotes it. A one-pass scan over a table larger than the cache churns only probation, so
 *   directories, roots, fences and hot pages stay. No hints, no dials.
 * - **Leases are charged to the reader (ruling R5).** Every byte a lease holds, cached or not, is
 *   charged to the caller's `budget` (its grant) until `release`; while any lease pins a blob, the
 *   cache does not charge it, and it is not evictable. So a slow reader's pins count against its
 *   own grant (spillable or not), never against the cache, and are bounded by `MAX_READ_BYTES` per
 *   call. If the budget refuses, `read` rejects with `ReadDeniedError(bytes)` and holds nothing:
 *   the caller borrows a progress loan of that size and retries. On release, the cache charges the
 *   blob again or drops it if the governor has no room.
 * - `peek` serves a cached blob synchronously, with no lease and no charge, for the warm
 *   point-read path (owner-side ≤ 10 µs, ruling R6): the bytes are valid until the caller next
 *   yields. Cache buffers are never pooled or reused, so a peeked or leased array is never
 *   overwritten, only uncharged.
 * - Writes go around the cache. Freeing an extent drops its cached blobs in the same step, before
 *   the space can reallocate it.
 *
 * ## Memory
 * Fixed: one 1 MiB read slab and one 1 MiB write slab, charged through the cache handle at open
 * (open fails with `RangeError` under a cap too small for them). The free map, pin table and cache
 * index are small (O(free runs + pins + cached blobs)). Writers stage nothing else: `append`
 * copies parts into the write slab, which one builder holds at a time, so a builder's consecutive
 * appends become writes of up to 1 MiB (a chunk of 32 pages costs one or two platform writes, not
 * 32). When another builder appends, the slab is written out and the partial last sector
 * (< 4 KiB, since the store writes at `DATA_ALIGN` offsets) is kept with its builder; so
 * builders that alternate page by page cost one platform write per page, plus at most one
 * rewritten sector per switch. That costs 2–9× a commit's flush time for a 200-column group on
 * Chromium and Firefox (measured on OPFS), so a writer that interleaves builders passes a
 * `budget` to `beginWrite`: each builder then stages up to 256 KiB of its own (charged to the
 * budget until its seal or abort) and writes its aligned prefix as that fills. Measured on OPFS
 * (200 columns × 8 pages of 32 KiB): own buffers bring Chromium and WebKit to parity with
 * sequential writes; Firefox stays ~18% over sequential (its per-call cost). Accepted (lead
 * ruling, 2026-11-05): larger per-column batching would break the page writer's ≤ 16 MiB
 * working-set target for 200 columns; revisit only if the browser perf lane shows flush cost
 * mattering end to end.
 *
 * ## Async, cancellation, errors
 * - Methods that touch storage are async; everything else (`beginWrite`, `open`, `pin`, `peek`,
 *   stats) is synchronous. On OPFS each async call does its I/O on open sync handles before its
 *   first await; on IndexedDB it is one transaction.
 * - `read` takes an `AbortSignal`; it rejects with the signal's reason between slabs and holds
 *   nothing. Writer work is cancelled by `abort`.
 * - `OwnershipLostError`, `StoreQuotaError` and `StoreCorruptionError` pass through from the
 *   session. After `OwnershipLostError` the space is `failed`.
 * - Misuse (a blob over `MAX_BLOB_BYTES`, appending past capacity, a read over `MAX_READ_BYTES`,
 *   publishing an open builder, a foreign writer, using an aborted writer) is a `TypeError` or
 *   `RangeError`, thrown before any effect.
 *
 * ## Both backends fit
 * Everything above goes through `StoreSession` (`writeData`, `readDataInto`, `releaseData`,
 * `publish`, `readRoot`, `readPreviousRoot`): OPFS runs it as synchronous handle calls; IndexedDB
 * as staged records, one transaction per call, the epoch check inside each. The space never
 * assumes a file.
 */
import type { StoreSession } from "./contract.js";
import type { RootPin } from "./reclaim.js";
import { MAX_SLAB } from "./slabs.js";

export type { RootPin } from "./reclaim.js";

/** The largest blob a writer accepts (one read slab). Larger structures span several blobs. */
export const MAX_BLOB_BYTES = MAX_SLAB;
/** The most blob bytes one `read` call may ask for (and so one lease may pin). */
export const MAX_READ_BYTES = 4 * MAX_SLAB;

/** A contiguous byte range of the data space: the unit of allocation and reclaim. */
export interface ExtentRef {
  /** A multiple of `DATA_ALIGN`. */
  readonly offset: number;
  /** Bytes written (the reservation is this rounded up to `DATA_ALIGN`). */
  readonly length: number;
}

/**
 * A checksummed byte range inside one extent: what readers ask for. Its data-space address is
 * `extent.offset + offset`; `offset + length ≤ extent.length` (a ref read from storage that breaks
 * this is `StoreCorruptionError`).
 */
export interface BlobRef {
  /** The extent the blob lives in: its unit of reclaim (ruling R15.1). */
  readonly extent: ExtentRef;
  /** Byte offset within `extent`. */
  readonly offset: number;
  readonly length: number;
  /** xxHash32 (seed 0) of the referenced bytes. */
  readonly checksum: number;
}

/** A blob appended to an unsealed builder: its offset within the extent it will be sealed into. */
export interface PendingBlob {
  readonly offset: number;
  readonly length: number;
  readonly checksum: number;
}

/** The data-space address of a blob's first byte. */
export function blobAddress(ref: BlobRef): number {
  return ref.extent.offset + ref.offset;
}

/** An extent and the blobs appended to it, in append order, each carrying `extent`. */
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
  /**
   * When the entry `evict` takes next was last used (`performance.now()` ms): the governor evicts
   * across several registered caches oldest first (`memory/governor.ts` rule 6).
   */
  readonly oldestUnusedAt?: number | undefined;
}

// ── Open ───────────────────────────────────────────────────────────────────────────────────────

/**
 * Everything `root` reaches, supplied by the format track: the extents, or the blobs (the space
 * takes `BlobRef.extent`; no address is mapped back to an extent). The space calls it for the
 * current root and then the previous one (most of whose metadata the first walk already cached).
 * It reads metadata through `reader` (verified, cached), charging its leases to `budget` (the
 * open's `OpenSpaceOptions.budget`). Duplicates are fine (shared and packed extents). It must
 * reach every structure the root reaches: catalog, group directories, group roots, chunks,
 * deletion bitmaps, statistics, index runs (staged ones included) and their fences and Bloom
 * filters.
 */
export type ReachableExtents = (
  root: Uint8Array,
  reader: ExtentReader,
  budget: ByteBudget,
) => AsyncIterable<ExtentRef | BlobRef> | Iterable<ExtentRef | BlobRef>;

export interface OpenSpaceOptions {
  readonly reachable: ReachableExtents;
  /** Charged for the walk's leases (the owner's startup grant); `audit` uses it too. */
  readonly budget: ByteBudget;
  /** Registers the cache with the governor and returns its handle (`governor.registerCache`). */
  readonly registerCache: (cache: EvictableCacheLike) => CacheMemory;
  readonly signal?: AbortSignal;
}

/**
 * Opens the space over a freshly opened store session, before the write path's log scan
 * (storage/contract.ts "Startup order"): reads the current and previous roots, walks both, builds
 * the free map, retires
 * what only the previous root reaches (`ReclaimLedger.retireAtOpen`), returns the free tail. The
 * walks read O(groups + index runs) metadata, coalesced. Nothing is written but the tail release.
 * Implemented by `openExtentSpace` (space.ts).
 */
export type OpenExtentSpace = (
  session: StoreSession,
  options: OpenSpaceOptions,
) => Promise<ExtentSpace>;

// ── Reading ────────────────────────────────────────────────────────────────────────────────────

export interface ReadOptions {
  /** Charged for every byte the lease holds, cached or not, until `release` (ruling R5). */
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
   * misses in coalesced slabs. At most `MAX_READ_BYTES` in total (`RangeError` otherwise).
   * Readable: blobs of sealed extents, published or not, and of extents any kept root or pin
   * reaches. `ReadDeniedError` when `budget` cannot hold the lease.
   */
  read(refs: readonly BlobRef[], options: ReadOptions): Promise<ReadLease>;
  /** A cached blob, synchronously, or null. Valid until the caller yields; never pinned. */
  peek(ref: BlobRef): Uint8Array | null;
  /**
   * The parsed descriptor a reader attached to `ref`'s cache entry (`attach`), or null when the
   * entry is not resident or has none. Optional: a reader without it parses on every load.
   */
  parsed?(ref: BlobRef): object | null;
  /**
   * Attaches a parsed descriptor of the entry's bytes (views into those same bytes, validated
   * structure) to `ref`'s resident cache entry, charging `bytes` (the descriptor's own size) in
   * that entry; it is dropped with the entry. False when the entry is not resident, already has
   * one, or the charge is refused. The descriptor must not copy the entry's bytes.
   */
  attach?(ref: BlobRef, parsed: object, bytes: number): boolean;
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
   * Reserves one contiguous extent of `capacity` bytes (> 0), first fit; `extend` adds more.
   * Synchronous: it only allocates. Fails only on misuse (writer not open).
   */
  open(capacity: number): ExtentBuilder;
  /**
   * `open(total)`, `append(parts)`, `seal()` in one call. `parts` must not be empty; each is at
   * most `MAX_BLOB_BYTES`.
   */
  write(parts: readonly Uint8Array[]): Promise<WrittenExtent>;
  /** Frees one of this writer's sealed extents before publication (an intermediate it dropped). */
  discard(extent: ExtentRef): Promise<void>;
  /**
   * Frees everything the writer reserved or sealed, after its in-flight writes settle. Idempotent
   * on an aborted writer; a `TypeError` once publishing has begun.
   */
  abort(): Promise<void>;
}

/**
 * One extent being written. Blob offsets are relative to the extent, so they hold however the
 * builder grows; the extent's address is fixed at `seal` (reservation.ts holds the bookkeeping).
 */
export interface ExtentBuilder {
  /** Bytes the builder may hold: the opening capacity plus every `extend`. */
  readonly capacity: number;
  /** Bytes appended so far. */
  readonly length: number;
  /**
   * Adds `bytes` of capacity, synchronously (ruling R15.2), so a writer can stream a chunk whose
   * size it only estimated (the page track's `GroupWriter`).
   * - Returns true when the space after the reservation was free and it grew in place: no cost.
   * - Returns false when it was not: a new segment is reserved elsewhere, appends continue into
   *   it (a blob may straddle the two), and `seal` relocates the extent into one contiguous range.
   *   Relocation costs one read and one rewrite of the extent's bytes through the space's 1 MiB
   *   write slab (no extra memory; ~2× the extent's I/O, about 3 ms per MiB on Firefox cold), and
   *   holds both ranges until it finishes. Reserve from page-statistics estimates so it stays rare;
   *   `stats.relocations` counts it.
   */
  extend(bytes: number): boolean;
  /**
   * Places `parts` back to back after the bytes already appended and returns one checksummed
   * pending blob per part (offsets within the extent). The caller may reuse `parts`' buffers once
   * it resolves. A `RangeError` if a part exceeds `MAX_BLOB_BYTES` or they do not fit the
   * remaining capacity (nothing is written: `extend` first). Blobs become readable at `seal`.
   */
  append(parts: readonly Uint8Array[]): Promise<PendingBlob[]>;
  /**
   * Writes what is staged, relocates if `extend` had to (see there), trims the unused capacity
   * back to free space, and returns the extent and its blobs as `BlobRef`s (in append order), or
   * null when nothing was appended (the whole reservation is freed).
   */
  seal(): Promise<WrittenExtent | null>;
}

// ── The space ──────────────────────────────────────────────────────────────────────────────────

/** Options of `beginWrite` (an additive change after the Phase 1 freeze). */
export interface WriteOptions {
  /**
   * Charged for each builder's own staging buffer (up to 256 KiB, no more than the builder's
   * capacity), returned at its seal or abort. A builder the budget refuses uses the shared slab.
   */
  readonly budget?: ByteBudget;
}

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
  /** Retired, waiting for the next publish or an older pin (garbage debt). */
  readonly waitingBytes: number;
  readonly pinCount: number;
  /** The longest-held pin, for backpressure diagnostics. */
  readonly oldestPin: { readonly label: string; readonly ageMs: number } | null;
  /** Freed and being released to the platform. */
  readonly releasingBytes: number;
  /** Builders `seal` had to relocate after an `extend` could not grow in place. */
  readonly relocations: number;
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
  /** The root before it (kept intact until the next publish), or null. */
  readonly previousRoot: Uint8Array | null;
  /** The current generation (0 at open). */
  readonly generation: number;
  /** Set when a publish was rejected or ownership was lost; writes and publishes then refuse. */
  readonly failed: Error | null;
  readonly stats: SpaceStats;

  /**
   * A new open writer. `SpaceFailedError` if the space failed. With `options.budget`, each of its
   * builders asks the budget for its own staging buffer (see "Memory").
   */
  beginWrite(label: string, options?: WriteOptions): ExtentWriter;
  /** Atomically and durably makes `root` current; returns its generation. See "Publish". */
  publish(root: Uint8Array, change: PublishChange): Promise<number>;
  /**
   * Holds the current root's extents (and so its blobs) until released. Synchronous; `label`
   * names the holder in `stats.oldestPin`.
   */
  pin(label?: string): RootPin;
  /**
   * Walks the current and previous roots with `reachable` and checks the free map against them:
   * every extent either root reaches is allocated (the previous root's only as waiting); every
   * allocated extent is reachable, held by a writer, waiting, or being released. Throws `Error("Extent audit: …")` naming the first difference. Tests only.
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

/** The caller's budget could not hold a read's lease. Borrow a loan of `bytes`, then retry. */
export class ReadDeniedError extends Error {
  readonly code = "MINNOW_READ_DENIED";
  constructor(readonly bytes: number) {
    super(`A read needs ${String(bytes)} bytes the budget does not have`);
    this.name = "ReadDeniedError";
  }
}
