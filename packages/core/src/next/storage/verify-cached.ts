/**
 * A test-build guard for the cache's trust in its bytes (review B2-p2e #2). The extent space and
 * the readers above it treat cached, verified bytes as immutable: a page is validated once per
 * cache residency, and a lease hands out the cache's own arrays. Nothing enforces that in
 * production (it costs a hash per release). With `MINNOW_VERIFY_CACHED_BYTES=1` in the
 * environment (vitest.config.ts sets it for every suite), a lease release and an eviction hash
 * the bytes again and throw if a consumer wrote into them.
 *
 * The flag is read once, at module load, through `globalThis` (no `process` in a browser), so
 * with it off the guard is one branch on a constant per release.
 */
import { xxHash32 } from "../hash/xxhash32.js";

const environment = (globalThis as { process?: { env?: Record<string, string | undefined> } })
  .process?.env;

/** Whether cached bytes are re-verified (test builds only). */
export const VERIFY_CACHED_BYTES = environment?.MINNOW_VERIFY_CACHED_BYTES === "1";

/** A consumer wrote into bytes the cache hands out as immutable. */
export class CachedBytesWrittenError extends Error {
  readonly code = "MINNOW_CACHED_BYTES_WRITTEN";
  constructor(where: string) {
    super(`Cached bytes changed after verification (${where}): a consumer wrote into a lease`);
    this.name = "CachedBytesWrittenError";
  }
}

/** Throws `CachedBytesWrittenError` unless `bytes` still hash to `checksum`. */
export function verifyCached(bytes: Uint8Array, checksum: number, where: string): void {
  if (bytes.length > 0 && xxHash32(bytes) !== checksum) throw new CachedBytesWrittenError(where);
}
