/** Parsed directories belong to one database owner, not one query or address.
 * All immutable identity fields participate; restore creates a new owner/cache.
 */
import { BlobCache, fullBlobKey } from "../format/blob-cache.js";
import { metadataCachesFor } from "../format/metadata-reader.js";
import type { MemoryGovernor } from "../memory/governor.js";
import type { ExtentReader } from "../storage/extents.js";
import type { ChunkDirectory } from "./directory.js";

export function createDirectoryCache(governor: MemoryGovernor): BlobCache<ChunkDirectory> {
  return new BlobCache<ChunkDirectory>((cache) => governor.registerCache(cache), fullBlobKey);
}

/** Resolve before a read-step wraps the pin's reader. Queries borrow the owner's
 * cache and never establish a separate unbounded validation registry.
 */
export function directoryCacheFor(pages: ExtentReader): BlobCache<ChunkDirectory> | undefined {
  return metadataCachesFor(pages)?.directories;
}
