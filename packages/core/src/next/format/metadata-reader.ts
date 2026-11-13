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
import {
  decodeCatalogRoot,
  decodeCatalogPage,
  type CatalogRoot,
  type CatalogPage,
} from "./catalog-pages.js";
import type { BlobRef, ByteBudget, ExtentReader } from "../storage/extents.js";
import type { BlobCache } from "./blob-cache.js";
import {
  decodeGroupIndex,
  decodeGroupPage,
  decodeGroupRoot,
  tryDecodeGroupRootView,
  optionalParseAllocation,
  type GroupPageEntry,
  type Parsed,
} from "./manifest-codec.js";
import type { GroupRef, GroupRoot } from "./manifest.js";
import type { Held, ManifestReader } from "./reachable.js";

export type MaybePromise<T> = T | Promise<T>;

export interface MetadataCaches {
  /** Owner opt-in: complete validation with private selective materialization. */
  readonly validatedRoots?: boolean;
  readonly groupRoots?: BlobCache<GroupRoot>;
  readonly groupPages?: BlobCache<readonly GroupRef[]>;
  readonly groupIndexes?: BlobCache<readonly GroupPageEntry[]>;
}

// The owner already registers/clears these caches. This weak binding adds no second cache,
// and neither a query nor a snapshot controls their lifetime. Different spaces are isolated.
const ownerCaches = new WeakMap<ExtentReader, MetadataCaches>();
export function bindMetadataCaches(space: ExtentReader, caches: MetadataCaches): () => void {
  if (ownerCaches.has(space)) throw new Error("Metadata caches already bound to this owner");
  ownerCaches.set(space, caches);
  return () => {
    if (ownerCaches.get(space) === caches) ownerCaches.delete(space);
  };
}
export function metadataCachesFor(space: ExtentReader): MetadataCaches | undefined {
  return ownerCaches.get(space);
}

type Decoder<T> = (bytes: Uint8Array, budget: ByteBudget) => Parsed<T>;

export class MetadataReader {
  readonly #space: ExtentReader;
  readonly #caches: MetadataCaches;

  constructor(space: ExtentReader, caches: MetadataCaches = {}) {
    this.#space = space;
    this.#caches = caches;
  }

  catalogRoot(ref: BlobRef, budget: ByteBudget): MaybePromise<Held<CatalogRoot>> {
    return this.#read(ref, budget, decodeCatalogRoot, undefined);
  }
  catalogPage(ref: BlobRef, budget: ByteBudget): MaybePromise<Held<CatalogPage>> {
    return this.#read(ref, budget, decodeCatalogPage, undefined);
  }

  groupRoot(ref: BlobRef, budget: ByteBudget): MaybePromise<Held<GroupRoot>> {
    return this.#read(
      ref,
      budget,
      decodeGroupRoot,
      this.#caches.groupRoots,
      this.#caches.validatedRoots === true ? tryDecodeGroupRootView : undefined,
    );
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
    optionalDecode?: (bytes: Uint8Array, budget: ByteBudget) => Parsed<T> | null,
  ): MaybePromise<Held<T>> {
    const hit = cache?.pin(ref) ?? null;
    if (cache !== undefined && hit !== null) {
      const owner = cache;
      if (optionalParseAllocation(budget, hit.size)) {
        return held(hit.value, () => {
          budget.free(hit.size);
          owner.unpin(ref);
        });
      }
      cache.unpin(ref);
      // A larger optional representation must not raise the ordinary read minimum.
      cache = undefined;
      optionalDecode = undefined;
    }
    const parse = (bytes: Uint8Array): Held<T> => {
      const optional = optionalDecode?.(bytes, budget);
      const parsed = optional ?? decode(bytes, budget);
      const charge = parsed.charge;
      // Only one representation/charge is admitted per key. A declined optional build is
      // uncached, avoiding a concurrent eager/view race with different resident sizes.
      if (cache === undefined || (optionalDecode !== undefined && optional == null))
        return held(parsed.value, () => budget.free(charge));
      const owner = cache;
      const shared = owner.admit(ref, parsed.value, charge);
      return held(shared, () => {
        budget.free(charge);
        owner.unpin(ref);
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
    catalogRoot: async (ref) => reader.catalogRoot(ref, budget),
    catalogPage: async (ref) => reader.catalogPage(ref, budget),
    groupIndex: async (ref) => reader.groupIndex(ref, budget),
    groupPage: async (ref) => reader.groupPage(ref, budget),
    groupRoot: async (ref) => reader.groupRoot(ref, budget),
  };
}
