/**
 * Which backend a database gets (the ownership track calls this in the owner worker):
 * - **OPFS** when this context can open sync access handles: a dedicated worker whose
 *   `navigator.storage.getDirectory()` works. Chromium (incognito too), Firefox and Safari 16.4+.
 * - **IndexedDB** when OPFS is absent or refused: Safari private browsing (`getDirectory` throws
 *   `UnknownError`), Firefox private browsing (`SecurityError`), or a context without sync handles.
 *   Its data lives as long as the browser keeps the private session's IndexedDB.
 * - **Memory** when neither exists (Safari Lockdown Mode has no IndexedDB): nothing survives the
 *   owner. The caller tells the application.
 *
 * Layout: OPFS `minnow-core/<encoded name>/`, IndexedDB `minnow-core:<name>`; deliberately apart
 * from the pre-reset engine's `minnowdb/` directory and bare IndexedDB names, so the owner can
 * detect old data and refuse it by name.
 */
import type { StorageBackend } from "./contract.js";
import type { LockManagerLike } from "./fence.js";
import { openIndexedDb } from "./idb-like.js";
import { createIndexedDbBackend } from "./indexeddb.js";
import { MemoryBackend } from "./memory-store.js";
import { createOpfsBackend, webLocks, type OpfsDirectoryLike } from "./opfs.js";

export const OPFS_NAMESPACE = "minnow-core";
export const INDEXEDDB_PREFIX = "minnow-core:";

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
  /** Why OPFS was not used (the platform's error name), when it was not. */
  readonly opfsRefused: string | null;
}

interface DirectoryHandleLike {
  getDirectoryHandle(name: string, options?: { create?: boolean }): Promise<DirectoryHandleLike>;
}

export async function chooseBackend(
  name: string,
  platform: StoragePlatform = globalThis,
  locks: LockManagerLike = webLocks(platform.navigator?.locks as Parameters<typeof webLocks>[0]),
): Promise<ChosenBackend> {
  let opfsRefused: string | null = "unsupported";
  const getDirectory = platform.navigator?.storage?.getDirectory;
  const syncHandles =
    platform.FileSystemFileHandle !== undefined &&
    "createSyncAccessHandle" in platform.FileSystemFileHandle.prototype;
  if (getDirectory !== undefined && syncHandles)
    try {
      const root = (await getDirectory.call(platform.navigator?.storage)) as DirectoryHandleLike;
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
    } catch (error) {
      opfsRefused = error instanceof Error ? error.name : String(error);
    }
  const factory = platform.indexedDB;
  if (factory !== undefined)
    return {
      kind: "indexeddb",
      backend: createIndexedDbBackend({
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
