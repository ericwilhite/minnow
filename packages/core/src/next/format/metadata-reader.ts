/**
 * Reads the manifest's metadata blobs through the extent space, decoded once and shared:
 * group roots, group directory pages and directory indexes (manifest-codec.ts), each with an
 * optional `BlobCache` (blob-cache.ts; ruling R15.7 extended to the metadata a snapshot reads).
 *
 * - **Synchronous when cached (R6).** A call returns its value, not a Promise, when the decoded
 *   value is in its cache, or when the blob is resident in the space (`peek`) and only needs a
 *   parse; otherwise it reads (a lease, verified by the space) and parses.
 * - **Held, charged, released.** Every result is a `Held` value: its parse charge (or, on a cache
 *   hit, the entry's size) is on the caller's budget until `release`, which also unpins the
 *   cache entry. A value obtained through a cache stays valid after release only while its
 *   holder keeps no reference: callers keep the `Held` for as long as they use the value.
 * - **One owner per hold.** A failed parse returns its charge and admits nothing.
 *
 * It satisfies reachable.ts `ManifestReader` (bind a budget with `manifestReader`).
 */
import type { BlobRef, ByteBudget, ExtentReader } from "../storage/extents.js";
import type { BlobCache } from "./blob-cache.js";
import {
  decodeGroupIndex,
  decodeGroupPage,
  decodeGroupRoot,
  ParseBudgetError,
  type GroupPageEntry,
  type Parsed,
} from "./manifest-codec.js";
import type { GroupRef, GroupRoot } from "./manifest.js";
import type { Held, ManifestReader } from "./reachable.js";

export type MaybePromise<T> = T | Promise<T>;

export interface MetadataCaches {
  readonly groupRoots?: BlobCache<GroupRoot>;
  readonly groupPages?: BlobCache<readonly GroupRef[]>;
  readonly groupIndexes?: BlobCache<readonly GroupPageEntry[]>;
}

type Decoder<T> = (bytes: Uint8Array, budget: ByteBudget) => Parsed<T>;

export class MetadataReader {
  readonly #space: ExtentReader;
  readonly #caches: MetadataCaches;

  constructor(space: ExtentReader, caches: MetadataCaches = {}) {
    this.#space = space;
    this.#caches = caches;
  }

  groupRoot(ref: BlobRef, budget: ByteBudget): MaybePromise<Held<GroupRoot>> {
    return this.#read(ref, budget, decodeGroupRoot, this.#caches.groupRoots);
  }

  groupPage(ref: BlobRef, budget: ByteBudget): MaybePromise<Held<readonly GroupRef[]>> {
    return this.#read(ref, budget, decodeGroupPage, this.#caches.groupPages);
  }

  groupIndex(ref: BlobRef, budget: ByteBudget): MaybePromise<Held<readonly GroupPageEntry[]>> {
    return this.#read(ref, budget, decodeGroupIndex, this.#caches.groupIndexes);
  }

  #read<T>(
    ref: BlobRef,
    budget: ByteBudget,
    decode: Decoder<T>,
    cache: BlobCache<T> | undefined,
  ): MaybePromise<Held<T>> {
    const hit = cache?.pin(ref) ?? null;
    if (cache !== undefined && hit !== null) {
      if (!budget.allocate(hit.size)) {
        cache.unpin(ref);
        throw new ParseBudgetError(hit.size);
      }
      return held(hit.value, () => {
        budget.free(hit.size);
        cache.unpin(ref);
      });
    }
    const parse = (bytes: Uint8Array): Held<T> => {
      const parsed = decode(bytes, budget);
      if (cache === undefined) return held(parsed.value, () => budget.free(parsed.charge));
      const shared = cache.admit(ref, parsed.value, parsed.charge);
      return held(shared, () => {
        budget.free(parsed.charge);
        cache.unpin(ref);
      });
    };
    const resident = this.#space.peek(ref);
    if (resident !== null) return parse(resident);
    return this.#space.read([ref], { budget }).then((lease) => {
      try {
        return parse(lease.bytes[0] ?? new Uint8Array(0));
      } finally {
        lease.release();
      }
    });
  }
}

function held<T>(value: T, release: () => void): Held<T> {
  let released = false;
  return {
    value,
    release: () => {
      if (released) return;
      released = true;
      release();
    },
  };
}

/** A `ManifestReader` (reachable.ts) over `reader`, charging `budget`. */
export function manifestReader(reader: MetadataReader, budget: ByteBudget): ManifestReader {
  return {
    groupIndex: async (ref) => reader.groupIndex(ref, budget),
    groupPage: async (ref) => reader.groupPage(ref, budget),
    groupRoot: async (ref) => reader.groupRoot(ref, budget),
  };
}
