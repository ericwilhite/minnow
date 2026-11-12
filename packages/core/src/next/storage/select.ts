/**
 * Which backend a database gets (the ownership track calls this in the owner worker).
 *
 * ## Order (review B1-p2c findings 2 and 3)
 * 1. **Probe OPFS.** `navigator.storage.getDirectory()`, even without sync access handles.
 *    Only the refusals the private modes produce mean "no OPFS here" (`REFUSALS`: Firefox private
 *    browsing `SecurityError`, Safari private browsing `UnknownError`, `NotSupportedError`), as does
 *    a context without the API. Sync handles are required to select OPFS, not to inspect it.
 *    Anything else (quota, an I/O error, a bug) propagates: falling back
 *    would open an empty IndexedDB store over a database that exists in OPFS.
 * 2. **Refuse pre-reset data** (PROPOSAL "Refuse old data cleanly"), before anything is created:
 *    the old engine's OPFS directory `minnowdb/<name>` (its own segment encoding), or an IndexedDB
 *    database under the bare name holding the old engine's object stores. Either is a
 *    `LegacyDataError` naming what was found; the owner surfaces it, and deleting old data is an
 *    explicit call the application makes.
 * 3. **Never hide a database.** Choosing OPFS while this name's IndexedDB store exists (made when
 *    OPFS was refused) is a `StoreConflictError`, not a silent switch to an empty store.
 * 4. **Open:** OPFS `minnow-core/<encoded name>/`, else IndexedDB `minnow-core:<name>`, else memory
 *    (Safari Lockdown Mode has neither; nothing survives the owner).
 *
 * The probes create nothing: OPFS lookups use no `create`, and the IndexedDB probe aborts the
 * upgrade a missing database would need, which leaves no database behind.
 */
import type { StorageBackend } from "./contract.js";
import type { LockManagerLike } from "./fence.js";
import { openIndexedDb } from "./idb-like.js";
import { createIndexedDbBackend, type IndexedDbBackendOptions } from "./indexeddb.js";
import { indexedDbStores, LegacyDataError, refuseLegacyIndexedDb } from "./legacy-data.js";
import { MemoryBackend } from "./memory-store.js";
import { createOpfsBackend, webLocks, type OpfsDirectoryLike } from "./opfs.js";

export { LegacyDataError } from "./legacy-data.js";

export const OPFS_NAMESPACE = "minnow-core";
export const INDEXEDDB_PREFIX = "minnow-core:";
/** The pre-reset engine's layout. */
export const LEGACY_OPFS_NAMESPACE = "minnowdb";
/** OPFS refusals that mean "this context has no OPFS" (observed in the private modes). */
export const REFUSALS: ReadonlySet<string> = new Set([
  "SecurityError",
  "UnknownError",
  "NotSupportedError",
]);

/** The database exists in a store other than the one this context would open. */
export class StoreConflictError extends Error {
  readonly code = "MINNOW_STORE_CONFLICT";
  constructor(readonly location: string) {
    super(
      `This context would open OPFS, but the database exists in IndexedDB ("${location}"); refusing to open an empty store over it`,
    );
    this.name = "StoreConflictError";
  }
}

interface DirectoryHandleLike {
  getDirectoryHandle(name: string, options?: { create?: boolean }): Promise<DirectoryHandleLike>;
}

/** What the chooser looks at; `globalThis` in a worker. Tests pass their own. */
export interface StoragePlatform {
  readonly navigator?: {
    readonly storage?: { getDirectory?: () => Promise<unknown> };
    readonly locks?: unknown;
  };
  readonly indexedDB?: IDBFactory;
  readonly FileSystemFileHandle?: { readonly prototype: object };
}

export interface ChosenBackend {
  readonly kind: "opfs" | "indexeddb" | "memory";
  readonly backend: StorageBackend;
  /** Why OPFS was not used (the platform's refusal, or "unsupported"), when it was not. */
  readonly opfsRefused: string | null;
}

const errorName = (error: unknown): string =>
  typeof error === "object" && error !== null && "name" in error ? String(error.name) : "";

/** The pre-reset engine's OPFS path segment for a database name (its `encodeSegment`). */
export function encodeLegacySegment(segment: string): string {
  if (segment.length === 0) return "%";
  let encoded = "";
  for (const char of segment) {
    if (/^[A-Za-z0-9._-]$/u.test(char) && !(encoded.length === 0 && char === ".")) {
      encoded += char;
      continue;
    }
    for (const byte of new TextEncoder().encode(char))
      encoded += `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
  }
  return encoded;
}

/** A directory's child, or null when it does not exist (never created). */
async function child(
  parent: DirectoryHandleLike,
  name: string,
): Promise<DirectoryHandleLike | null> {
  try {
    return await parent.getDirectoryHandle(name);
  } catch (error) {
    if (errorName(error) === "NotFoundError") return null;
    throw error;
  }
}

export async function chooseBackend(
  name: string,
  platform: StoragePlatform = globalThis,
  locks: LockManagerLike = webLocks(platform.navigator?.locks as Parameters<typeof webLocks>[0]),
  accounting: Pick<IndexedDbBackendOptions, "memory" | "registerCache"> = {},
): Promise<ChosenBackend> {
  // 1. The OPFS probe: only a recognized refusal (or no API) means "no OPFS".
  let root: DirectoryHandleLike | null = null;
  let opfsRefused: string | null = "unsupported";
  const getDirectory = platform.navigator?.storage?.getDirectory;
  const syncHandles =
    platform.FileSystemFileHandle !== undefined &&
    "createSyncAccessHandle" in platform.FileSystemFileHandle.prototype;
  if (getDirectory !== undefined)
    try {
      root = (await getDirectory.call(platform.navigator?.storage)) as DirectoryHandleLike;
      if (syncHandles) opfsRefused = null;
    } catch (error) {
      if (!REFUSALS.has(errorName(error))) throw error;
      opfsRefused = errorName(error);
    }
  const factory = platform.indexedDB;

  // 2. Pre-reset data, anywhere this context can see, before anything is created.
  if (root !== null) {
    const legacy = await child(root, LEGACY_OPFS_NAMESPACE);
    const segment = encodeLegacySegment(name);
    if (legacy !== null && (await child(legacy, segment)) !== null)
      throw new LegacyDataError("opfs", `${LEGACY_OPFS_NAMESPACE}/${segment}`);
  }
  if (factory !== undefined) await refuseLegacyIndexedDb(factory, name);

  // 3–4. OPFS, unless that would hide this database's IndexedDB store; else IndexedDB; else memory.
  if (root !== null && syncHandles) {
    if (
      factory !== undefined &&
      (await indexedDbStores(factory, `${INDEXEDDB_PREFIX}${name}`)) !== null
    )
      throw new StoreConflictError(`${INDEXEDDB_PREFIX}${name}`);
    const namespace = await root.getDirectoryHandle(OPFS_NAMESPACE, { create: true });
    const directory = await namespace.getDirectoryHandle(encodeURIComponent(name), {
      create: true,
    });
    return {
      kind: "opfs",
      backend: createOpfsBackend({
        directory: directory as unknown as OpfsDirectoryLike,
        name,
        locks,
      }),
      opfsRefused: null,
    };
  }
  if (factory !== undefined)
    return {
      kind: "indexeddb",
      backend: createIndexedDbBackend({
        ...accounting,
        database: () => openIndexedDb(factory, `${INDEXEDDB_PREFIX}${name}`),
        name,
        locks,
      }),
      opfsRefused,
    };
  return {
    kind: "memory",
    backend: new MemoryBackend({ locks, name: `minnow:${name}` }),
    opfsRefused,
  };
}
