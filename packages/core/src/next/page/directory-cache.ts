/** Query-scoped immutable directories, carried only by page-layer reader facades. */
import { BlobCache, fullBlobKey } from "../format/blob-cache.js";
import type { CacheMemory, EvictableCacheLike, ExtentReader } from "../storage/extents.js";
import type { ChunkDirectory } from "./directory.js";

export class QueryDirectoryCache extends BlobCache<ChunkDirectory> {
  constructor(register: (cache: EvictableCacheLike) => CacheMemory) {
    super(register, fullBlobKey);
  }
}
const DIRECTORIES = Symbol("query directory facts");
type DirectoryReader = ExtentReader & { readonly [DIRECTORIES]?: QueryDirectoryCache };
export function queryDirectories(reader: ExtentReader): QueryDirectoryCache | undefined {
  return (reader as DirectoryReader)[DIRECTORIES];
}
export function withQueryDirectories(
  reader: ExtentReader,
  directories: QueryDirectoryCache,
): ExtentReader {
  const wrapped: DirectoryReader = {
    [DIRECTORIES]: directories,
    peek: (ref, options) => reader.peek(ref, options),
    read: (refs, options) => reader.read(refs, options),
    parsed: (ref) => reader.parsed?.(ref) ?? null,
    attach: (ref, parsed, bytes, of) => reader.attach?.(ref, parsed, bytes, of) ?? false,
  };
  return wrapped;
}
