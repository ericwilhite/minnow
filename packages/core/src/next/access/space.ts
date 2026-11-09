/**
 * The storage track's extent space (storage/extents.ts) under the names the access contracts use.
 * Phase 1 stated this slice structurally because B1's module was not on the contracts branch; it
 * is now, so these are B1's own types (as the Phase 1 note said they would become).
 *
 * The write path never reads the data space directly and never keeps its own free-space map: it
 * reads through the space's verified cache, writes through its writers, publishes through it, and
 * pins roots through it (R7). Every blob it writes is at most `MAX_BLOB_BYTES` (1 MiB, R8), so
 * large structures it owns (the catalog, a table's group list, the ledger, an index run's entries
 * and Bloom partitions) are split across blobs by their format, and one read leases at most B1's
 * `MAX_READ_BYTES`.
 */
import type {
  ExtentReader,
  ExtentSpace,
  ExtentWriter,
  RootPin,
  SpaceStats,
} from "../storage/extents.js";

export type { ByteBudget, ReadLease } from "../storage/extents.js";
export { MAX_BLOB_BYTES, MAX_READ_BYTES, ReadDeniedError } from "../storage/extents.js";

/** Verified, cached reads (B1 `ExtentReader`). */
export type SpaceReader = ExtentReader;
/** Holds one root generation's extents until released (B1 `RootPin`). */
export type SpacePin = RootPin;
/** B1 `ExtentWriter`. */
export type SpaceWriter = ExtentWriter;
/** B1 `SpaceStats`. */
export type SpaceStatsSlice = SpaceStats;
/** The space as the write path holds it (B1 `ExtentSpace`). */
export type Space = ExtentSpace;
