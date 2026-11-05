/**
 * The OPFS backend (phase 2 implements `createOpfsBackend`): the file store (file-store.ts) over
 * real `FileSystemSyncAccessHandle`s, fenced by them.
 *
 * ## Files
 * One directory per database, holding five long-lived files, each with one sync handle opened at
 * acquisition and kept open until the lease is released:
 * - `log`, `data`, `root-0`, `root-1` (file-store.ts);
 * - `spill`: the scratch file the spill service uses (`StorageBackend.scratch`).
 * No file is created or removed after the first acquisition: Chromium and WebKit keep quota
 * charged after a truncate (300 MiB written then truncated still counts 384 MiB), WebKit fails
 * opens after ~252 handles per origin (bug 293955), and every
 * `createSyncAccessHandle` is async, costs 0.2–12 ms, and waits behind a busy page on WebKit.
 *
 * ## Acquisition (`storage/fence.ts` `acquireFenced`)
 * 1. The Web Lock `minnow:<db>` (`navigator.locks`, adapted to `LockManagerLike`).
 * 2. Every file's sync handle, opened in a fixed order with bounded retry: the lock frees
 *    2 ms–2 s before a dead owner's handles. Busy is `NoModificationAllowedError` (Chromium,
 *    Firefox) or `InvalidStateError` (WebKit). Handles opened before a busy one are closed before
 *    the retry. `mode` is not passed: Firefox and WebKit ignore it and Chromium's non-default
 *    modes do not exclude.
 * 3. On lock loss (`AbortError` on the held request) the lease closes every handle at once: the
 *    handle, not the lock, is what stops a hung owner.
 *
 * ## Errors
 * - `QuotaExceededError` from any write or truncate, and `InvalidStateError` from a *write* whose
 *   ownership guard passed (WebKit's disk-full, bug 327929), become `StoreQuotaError`. Matched by
 *   name, never by class (the subclass with `quota`/`requested` ships only in Chrome 138+).
 * - A short read inside the file is `StoreCorruptionError`.
 *
 * ## Not here
 * Page lifecycle (`pagehide` terminate, `freeze` step-down) belongs to the ownership track. The
 * engine never touches OPFS outside these handles, and clients never open these files: any
 * other opener would block the owner.
 */
import type { LockManagerLike } from "./fence.js";
import type { SyncFile } from "./contract.js";

/** `FileSystemFileHandle`, as far as the backend uses it. */
export interface OpfsFileHandleLike {
  createSyncAccessHandle(): Promise<SyncFile>;
}

/** `FileSystemDirectoryHandle`, as far as the backend uses it. */
export interface OpfsDirectoryLike {
  getFileHandle(name: string, options?: { create?: boolean }): Promise<OpfsFileHandleLike>;
}

export interface OpfsBackendOptions {
  /** The database's own directory (under `navigator.storage.getDirectory()`). */
  readonly directory: OpfsDirectoryLike;
  /** The database name; the lock is `minnow:<name>`. */
  readonly name: string;
  readonly locks: LockManagerLike;
}

/** The files of one database directory, in acquisition order. */
export const OPFS_FILE_NAMES = ["log", "data", "root-0", "root-1", "spill"] as const;
