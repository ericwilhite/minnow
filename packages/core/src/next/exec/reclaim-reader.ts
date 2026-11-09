/** Retry a refused lease after cooperative reclaim, inside ChunkReader's original load. */
import { ReadDeniedError, type SpaceReader } from "../access/space.js";
import type { Grant } from "../memory/governor.js";

export function reclaimingReader(
  pages: SpaceReader,
  grant: Grant,
  signal?: AbortSignal,
): SpaceReader {
  return {
    peek: (ref, options) => pages.peek(ref, options),
    parsed: (ref) => pages.parsed?.(ref) ?? null,
    attach: (ref, parsed, bytes, of) => pages.attach?.(ref, parsed, bytes, of) ?? false,
    async read(refs, options) {
      for (;;) {
        signal?.throwIfAborted();
        try {
          return await pages.read(refs, options);
        } catch (error) {
          if (!(error instanceof ReadDeniedError)) throw error;
          if (!(await grant.makeRoom?.(error.bytes, options.signal ?? signal))) throw error;
        }
      }
    },
  };
}
