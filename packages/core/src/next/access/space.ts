/**
 * The slice of the storage track's extent space (B1, storage/extents.ts on `rebuild/storage`
 * 6b98ee1) that the access contracts name, stated structurally because that module is not on the
 * contracts branch yet. B1's `ExtentSpace`, `ExtentReader`, `ExtentWriter` and `RootPin`
 * satisfy these as they are. When B1 lands in the contracts, this file becomes
 * `export type { … } from "../storage/extents.js"` with no other change.
 *
 * The write path never reads the data space directly and never keeps its own free-space map: it
 * reads through the space's verified cache, writes through its writers, publishes through it, and
 * pins roots through it (R7). Every blob it writes is at most `MAX_BLOB_BYTES` (1 MiB, R8), so
 * large structures it owns (the catalog, a table's group list, the ledger, an index run's entries
 * and Bloom partitions) are split across blobs by their format, and one read leases at most B1's
 * `MAX_READ_BYTES`.
 */
import type { BlobRef, ExtentRef } from "../format/manifest.js";

/** A grant's suballocator (memory/governor.ts `Grant` satisfies it). Never waits. */
export interface ByteBudget {
  allocate(bytes: number): boolean;
  free(bytes: number): void;
}

export interface ReadLease {
  /** Verified bytes, one per requested ref, in request order. Read-only. */
  readonly bytes: readonly Uint8Array[];
  /** Unpins the blobs and returns budget bytes. Idempotent. */
  release(): void;
}

/** Largest blob the space writes (B1 `MAX_BLOB_BYTES`). */
export const MAX_BLOB_BYTES = 1024 * 1024;

/** Verified, cached reads (B1 `ExtentReader`). */
export interface SpaceReader {
  /** Every leased byte is charged to `budget` until release; `ReadDeniedError` if it refuses. */
  read(
    refs: readonly BlobRef[],
    options: { readonly budget: ByteBudget; readonly signal?: AbortSignal },
  ): Promise<ReadLease>;
  /** A cached blob, synchronously, or null. Valid until the caller yields. */
  peek(ref: BlobRef): Uint8Array | null;
}

/** Holds one root generation's extents until released (B1 `RootPin`). */
export interface SpacePin {
  readonly generation: number;
  readonly released: boolean;
  release(): void;
}

/** The writer side the write path uses (B1 `ExtentWriter`; builders as B1 defines them). */
export interface SpaceWriter {
  readonly label: string;
  readonly extents: readonly ExtentRef[];
  write(parts: readonly Uint8Array[]): Promise<{
    readonly extent: ExtentRef;
    readonly blobs: readonly BlobRef[];
  }>;
  abort(): Promise<void>;
}

/** The slice of B1 `SpaceStats` the write path's backpressure and idle maintenance read. */
export interface SpaceStatsSlice {
  /** Allocated end of the data space. */
  readonly dataBytes: number;
  readonly freeBytes: number;
  /** Retired, waiting for the next publish or an older pin (garbage debt). */
  readonly waitingBytes: number;
  readonly pinCount: number;
  readonly oldestPin: { readonly label: string; readonly ageMs: number } | null;
}

/** The space as the write path holds it (B1 `ExtentSpace`). */
export interface Space extends SpaceReader {
  /** The durable root bytes, or null for a new store. */
  readonly root: Uint8Array | null;
  /** The root before it, kept intact until the next publish (B1 two-generation reclaim). */
  readonly previousRoot: Uint8Array | null;
  readonly generation: number;
  readonly failed: Error | null;
  readonly stats: SpaceStatsSlice;
  beginWrite(label: string): SpaceWriter;
  /**
   * Durably makes `root` current. `retired`: extents the previous root reaches and this one does
   * not, each exactly once (the write path's obligation; reuse waits for the next durable publish
   * and every older pin: B1's two-generation reclaim).
   */
  publish(
    root: Uint8Array,
    change: { readonly writers: readonly SpaceWriter[]; readonly retired: readonly ExtentRef[] },
  ): Promise<number>;
  /** `label` names the holder in `stats.oldestPin`. */
  pin(label?: string): SpacePin;
}
