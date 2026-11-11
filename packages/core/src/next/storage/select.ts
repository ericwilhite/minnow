/**
 * Which backend a database gets (the ownership track calls this in the owner worker).
 *
 * ## Order (review B1-p2c findings 2 and 3)
 * 1. **Probe OPFS.** `navigator.storage.getDirectory()` in a context with sync access handles.
 *    Only the refusals the private modes produce mean "no OPFS here" (`REFUSALS`: Firefox private
 *    browsing `SecurityError`, Safari private browsing `UnknownError`, `NotSupportedError`), as does
 *    a context without the API. Anything else (quota, an I/O error, a bug) propagates: falling back
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
import { MemoryBackend } from "./memory-store.js";
import { createOpfsBackend, webLocks, type OpfsDirectoryLike } from "./opfs.js";

export const OPFS_NAMESPACE = "minnow-core";
export const INDEXEDDB_PREFIX = "minnow-core:";
/** The pre-reset engine's layout. */
export const LEGACY_OPFS_NAMESPACE = "minnowdb";
const LEGACY_IDB_STORES = ["catalog", "manifests", "blocks"] as const;
/** OPFS refusals that mean "this context has no OPFS" (observed in the private modes). */
export const REFUSALS: ReadonlySet<string> = new Set([
  "SecurityError",
  "UnknownError",
  "NotSupportedError",
]);

/** Data of the pre-reset engine exists under this name. */
export class LegacyDataError extends Error {
  readonly code = "MINNOW_LEGACY_DATA";
  constructor(
    readonly store: "opfs" | "indexeddb",
    readonly location: string,
  ) {
    super(
      `A database from before the storage reset exists at ${store === "opfs" ? "OPFS" : "IndexedDB"} "${location}"; this version cannot read it. Export it with the old version or delete it explicitly.`,
    );
    this.name = "LegacyDataError";
  }
}

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
    if (errorName(error) === "NotFoundError" || errorName(error) === "TypeMismatchError")
      return null;
    throw error;
  }
}

/**
 * The object store names of the IndexedDB database `name`, or null when there is none. A
 * missing database's creation is aborted inside its upgrade, so nothing is left behind.
 */
function idbStores(factory: IDBFactory, name: string): Promise<string[] | null> {
  return new Promise((resolve, reject) => {
    let missing = false;
    const request = factory.open(name);
    request.onupgradeneeded = () => {
      missing = true;
      request.transaction?.abort();
    };
    request.onsuccess = () => {
      const database = request.result;
      const stores = Array.from(database.objectStoreNames as ArrayLike<string>);
      database.close();
      resolve(stores);
    };
    request.onerror = (event) => {
      if (missing) {
        event.preventDefault();
        resolve(null);
      } else reject(request.error ?? new Error(`Cannot inspect IndexedDB ${name}`));
    };
    request.onblocked = () => {
      reject(new Error(`IndexedDB ${name} is blocked by another connection`));
    };
  });
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
  if (getDirectory !== undefined && syncHandles)
    try {
      root = (await getDirectory.call(platform.navigator?.storage)) as DirectoryHandleLike;
      opfsRefused = null;
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
  if (factory !== undefined) {
    const stores = await idbStores(factory, name);
    if (stores !== null && LEGACY_IDB_STORES.every((store) => stores.includes(store)))
      throw new LegacyDataError("indexeddb", name);
  }

  // 3–4. OPFS, unless that would hide this database's IndexedDB store; else IndexedDB; else memory.
  if (root !== null) {
    if (factory !== undefined && (await idbStores(factory, `${INDEXEDDB_PREFIX}${name}`)) !== null)
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
