/**
 * Shared helpers for the extent space suites: a JSON root that lists blobs or extents, its
 * `reachable` walk, counting budgets and cache memory, and opening a space over a backend.
 * Test support only; nothing in the engine imports it.
 */
import type { StorageBackend, StoreSession } from "./contract.js";
import type {
  BlobRef,
  ByteBudget,
  CacheMemory,
  EvictableCacheLike,
  ExtentRef,
  ExtentSpace,
  ReachableExtents,
} from "./extents.js";
import { openExtentSpace } from "./space.js";

/** `items[index]`, which must exist. */
export function at<T>(items: readonly T[], index: number): T {
  const item = items[index];
  if (item === undefined) throw new RangeError(`No item at ${String(index)}`);
  return item;
}

/** Byte equality, fast on large arrays (the matcher's `toEqual` diffs element by element). */
/**
 * The first offset where `actual` and `expected` differ (the shorter length when one is a prefix
 * of the other), or -1 when they are equal. Four bytes a step where both views are aligned: a
 * deep `toEqual` walks byte arrays element by element (14.2 s for the extent-space interleaving
 * test, B2), and coverage counts every loop iteration.
 */
export function firstDifference(actual: Uint8Array, expected: Uint8Array): number {
  const length = Math.min(actual.length, expected.length);
  let index = 0;
  if (actual.byteOffset % 4 === 0 && expected.byteOffset % 4 === 0) {
    const words = length >>> 2;
    const a = new Int32Array(actual.buffer, actual.byteOffset, words);
    const b = new Int32Array(expected.buffer, expected.byteOffset, words);
    let word = 0;
    while (word < words && a[word] === b[word]) word += 1;
    index = word << 2;
  }
  while (index < length && actual[index] === expected[index]) index += 1;
  return index === length && actual.length === expected.length ? -1 : index;
}

export function sameBytes(left: Uint8Array | undefined, right: Uint8Array | undefined): boolean {
  return left !== undefined && right !== undefined && firstDifference(left, right) === -1;
}

/**
 * Asserts that `actual` holds exactly `expected`, naming the first differing offset on failure.
 * Throws a plain Error (this module is also loaded by the browser worker, without vitest).
 */
export function expectBytes(
  actual: Uint8Array | null | undefined,
  expected: Uint8Array,
  label = "bytes",
): void {
  if (actual === undefined || actual === null)
    throw new Error(`${label}: expected ${String(expected.length)} bytes, got ${String(actual)}`);
  const at = firstDifference(actual, expected);
  if (at === -1) return;
  throw new Error(
    `${label}: ${String(actual.length)} bytes against ${String(expected.length)} expected; first difference at offset ${String(at)} (${String(actual[at])} vs ${String(expected[at])})`,
  );
}

/** `length` bytes of a pattern derived from `seed` (period 256: built once, then doubled). */
export function bytesOf(length: number, seed: number): Uint8Array {
  const bytes = new Uint8Array(length);
  const period = Math.min(length, 256);
  for (let index = 0; index < period; index++) bytes[index] = (seed * 131 + index * 7) & 0xff;
  for (let filled = period; filled < length; filled *= 2)
    bytes.copyWithin(filled, 0, Math.min(filled, length - filled));
  return bytes;
}

/** A root that names `blobs` (and, optionally, bare extents). */
export function rootOf(blobs: readonly BlobRef[], extents: readonly ExtentRef[] = []): Uint8Array {
  return new TextEncoder().encode(JSON.stringify({ blobs, extents }));
}

export function blobsOf(root: Uint8Array): BlobRef[] {
  return (JSON.parse(new TextDecoder().decode(root)) as { blobs?: BlobRef[] }).blobs ?? [];
}

/** The reachable walk of `rootOf` roots: every blob (its extent) and bare extent. */
export const jsonReachable: ReachableExtents = function* (root) {
  const parsed = JSON.parse(new TextDecoder().decode(root)) as {
    blobs?: BlobRef[];
    extents?: ExtentRef[];
  };
  yield* parsed.blobs ?? [];
  yield* parsed.extents ?? [];
};

export interface CountingBudget extends ByteBudget {
  used: number;
  readonly limit: number;
}

export function countingBudget(limit: number): CountingBudget {
  const budget = {
    used: 0,
    limit,
    allocate(bytes: number) {
      if (budget.used + bytes > limit) return false;
      budget.used += bytes;
      return true;
    },
    free(bytes: number) {
      budget.used -= bytes;
    },
  };
  return budget;
}

export interface CountingMemory extends CacheMemory {
  used: number;
  readonly limit: number;
  cache: EvictableCacheLike | null;
}

export function countingMemory(limit: number): CountingMemory {
  const memory = {
    used: 0,
    limit,
    cache: null as EvictableCacheLike | null,
    charge(bytes: number) {
      if (memory.used + bytes > limit) return false;
      memory.used += bytes;
      return true;
    },
    release(bytes: number) {
      memory.used -= bytes;
    },
  };
  return memory;
}

export async function openOver(
  backend: StorageBackend,
  options: {
    reuseLock?: boolean;
    memory?: CountingMemory;
    wrap?: (session: StoreSession) => StoreSession;
  } = {},
): Promise<{ space: ExtentSpace; session: StoreSession; budget: CountingBudget }> {
  const lease = await backend.acquire({ steal: options.reuseLock === true, timeoutMs: 2_000 });
  const opened = await backend.open(lease);
  const session = options.wrap?.(opened) ?? opened;
  const memory = options.memory ?? countingMemory(Infinity);
  const budget = countingBudget(Infinity);
  const space = await openExtentSpace(session, {
    reachable: jsonReachable,
    budget,
    registerCache: (cache) => {
      memory.cache ??= cache;
      return memory;
    },
  });
  return { space, session, budget };
}
