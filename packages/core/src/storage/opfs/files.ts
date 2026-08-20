/**
 * Path encoding and file IO for the OPFS store.
 *
 * Names that come from callers (database names, temp owner and run ids) become one
 * conservatively percent-encoded entry name each. Encoding is bijective — `[A-Za-z0-9_-]` and
 * non-leading `.` pass through, everything else becomes `%XX` per UTF-8 byte — so names
 * round-trip exactly and none can collide with or escape into another's path.
 */

import type { SyncFileHandle } from "../toolkit/sync-file.js";

const SAFE_SEGMENT_CHARS = /^[A-Za-z0-9._-]$/;

export function encodeSegment(segment: string): string {
  if (segment.length === 0) return "%";
  let encoded = "";
  for (const char of segment) {
    if (SAFE_SEGMENT_CHARS.test(char) && !(encoded.length === 0 && char === ".")) {
      encoded += char;
      continue;
    }
    for (const byte of new TextEncoder().encode(char)) {
      encoded += `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
    }
  }
  return encoded;
}

export function decodeSegment(encoded: string): string {
  if (encoded === "%") return "";
  const bytes: number[] = [];
  for (let index = 0; index < encoded.length;) {
    const char = encoded[index] ?? "";
    if (char === "%") {
      bytes.push(Number.parseInt(encoded.slice(index + 1, index + 3), 16));
      index += 3;
      continue;
    }
    for (const byte of new TextEncoder().encode(char)) bytes.push(byte);
    index += char.length;
  }
  return new TextDecoder().decode(new Uint8Array(bytes));
}

export interface WriteFileOptions {
  /** `flush()` before close — the strict-durability path. */
  flush?: boolean;
}

export interface ReadFileOptions {
  /**
   * A locked file is one a writer currently holds open. For control files that is "not yet
   * committed" and reads as absent; for block files it is a bug worth surfacing, so the
   * default rethrows.
   */
  lockedMeansAbsent?: boolean;
}

/**
 * A directory-handle cache plus the file operations the store needs. Handles are cached per
 * path — a bulk write of one segment's blocks resolves its directory chain once — and the
 * cache is invalidated on deletes underneath it.
 */
export class OpfsTree {
  readonly #root: FileSystemDirectoryHandle;
  readonly #directories = new Map<string, FileSystemDirectoryHandle>();

  constructor(root: FileSystemDirectoryHandle) {
    this.#root = root;
  }

  async getDirectory(path: readonly string[], create: boolean): Promise<FileSystemDirectoryHandle> {
    if (path.length === 0) return this.#root;
    const key = path.join("/");
    const cached = this.#directories.get(key);
    if (cached !== undefined) return cached;
    const parent = await this.getDirectory(path.slice(0, -1), create);
    const directory = await retryTransient(() =>
      parent.getDirectoryHandle(path[path.length - 1] ?? "", { create }),
    );
    this.#directories.set(key, directory);
    return directory;
  }

  /** Drops cached handles at and under a path — call after deleting a directory. */
  invalidate(pathPrefix: readonly string[]): void {
    const prefix = pathPrefix.join("/");
    for (const key of [...this.#directories.keys()]) {
      if (key === prefix || key.startsWith(`${prefix}/`)) this.#directories.delete(key);
    }
  }

  /** `undefined` when the file (or any parent) does not exist. */
  async readFile(
    path: readonly string[],
    options: ReadFileOptions = {},
  ): Promise<Uint8Array | undefined> {
    try {
      return await retryTransient(async () => {
        const directory = await this.getDirectory(path.slice(0, -1), false);
        const handle = await directory.getFileHandle(path[path.length - 1] ?? "");
        const file = await handle.getFile();
        return new Uint8Array(await file.arrayBuffer());
      });
    } catch (error) {
      if (isDomError(error, "NotFoundError") || isDomError(error, "TypeMismatchError")) {
        return undefined;
      }
      // Browsers disagree on how a read racing a writer or a delete surfaces: Firefox refuses
      // at getFile() with NoModificationAllowedError, Chromium and WebKit hand out the File
      // and then fail its arrayBuffer() with NotReadableError — and WebKit sometimes refuses
      // with InvalidStateError instead. All mean the same thing for a checksummed control
      // file: not committed at this instant.
      if (
        options.lockedMeansAbsent === true &&
        (isDomError(error, "NoModificationAllowedError") ||
          isDomError(error, "NotReadableError") ||
          isDomError(error, "InvalidStateError"))
      ) {
        return undefined;
      }
      throw error;
    }
  }

  /**
   * Writes a whole file through an exclusive sync handle: truncate, write, optional flush,
   * close. Quota errors escape unwrapped. The handle is always closed, so a failed write never
   * leaves the file locked — only torn, which readers detect by checksum.
   */
  async writeFile(
    path: readonly string[],
    bytes: Uint8Array,
    options: WriteFileOptions = {},
  ): Promise<void> {
    await retryTransient(async () => {
      const handle = await this.openHandle(path, { create: true });
      try {
        handle.truncate(0);
        handle.write(bytes, { at: 0 });
        if (options.flush === true) handle.flush();
      } finally {
        handle.close();
      }
    });
  }

  /**
   * An exclusive sync handle; callers own the read-verify-write protocol and the close.
   * Declared as the toolkit's `SyncFileHandle` — the browser handle satisfies it, and the
   * platform-specific type lives in the WebWorker lib, which not every consumer loads.
   */
  async openHandle(path: readonly string[], options: { create: boolean }): Promise<SyncFileHandle> {
    return retryTransient(async () => {
      const directory = await this.getDirectory(path.slice(0, -1), options.create);
      const handle = await directory.getFileHandle(path[path.length - 1] ?? "", {
        create: options.create,
      });
      return handle.createSyncAccessHandle();
    });
  }

  /** True when the file existed. Absence is not an error — deletes are idempotent. */
  async deleteFile(path: readonly string[]): Promise<boolean> {
    try {
      await retryTransient(async () => {
        const directory = await this.getDirectory(path.slice(0, -1), false);
        await directory.removeEntry(path[path.length - 1] ?? "");
      });
      return true;
    } catch (error) {
      if (isDomError(error, "NotFoundError")) return false;
      throw error;
    }
  }

  /** Removes a whole subtree; missing is fine. */
  async deleteTree(path: readonly string[]): Promise<void> {
    try {
      await retryTransient(async () => {
        const directory = await this.getDirectory(path.slice(0, -1), false);
        await directory.removeEntry(path[path.length - 1] ?? "", { recursive: true });
      });
    } catch (error) {
      if (!isDomError(error, "NotFoundError")) throw error;
    }
    this.invalidate(path);
  }

  /** Names of the entries directly inside a directory; `[]` when it does not exist. */
  async listNames(path: readonly string[]): Promise<string[]> {
    try {
      return await retryTransient(async () => {
        const directory = await this.getDirectory(path, false);
        const names: string[] = [];
        for await (const name of directory.keys()) names.push(name);
        return names;
      });
    } catch (error) {
      if (isDomError(error, "NotFoundError") || isDomError(error, "TypeMismatchError")) {
        return [];
      }
      throw error;
    }
  }

  /** Every file under a directory, with sizes — the open-time block index walk. */
  async walkFiles(path: readonly string[]): Promise<Array<{ path: string[]; size: number }>> {
    const results: Array<{ path: string[]; size: number }> = [];
    let root: FileSystemDirectoryHandle;
    try {
      root = await this.getDirectory(path, false);
    } catch (error) {
      if (isDomError(error, "NotFoundError")) return results;
      throw error;
    }
    const walk = async (directory: FileSystemDirectoryHandle, prefix: string[]): Promise<void> => {
      for await (const [name, entry] of directory) {
        if (entry.kind === "file") {
          try {
            const file = await (entry as FileSystemFileHandle).getFile();
            results.push({ path: [...prefix, name], size: file.size });
          } catch (error) {
            // A file another instance is writing or deleting this instant is not part of any
            // committed state; skip it rather than failing the whole walk.
            if (
              !isDomError(error, "NotFoundError") &&
              !isDomError(error, "NoModificationAllowedError") &&
              !isDomError(error, "NotReadableError")
            ) {
              throw error;
            }
          }
        } else {
          await walk(entry as FileSystemDirectoryHandle, [...prefix, name]);
        }
      }
    };
    await walk(root, []);
    return results;
  }
}

export function isDomError(error: unknown, name: string): boolean {
  return error instanceof DOMException && error.name === name;
}

/**
 * WebKit's OPFS backend occasionally fails an operation with `UnknownError` and a message that
 * literally says "for an unknown transient reason" — observed under nothing more exotic than
 * opening and closing handles in quick succession. The declared response to a declared
 * transient failure is a bounded retry; anything else, or persistence past the budget,
 * propagates.
 */
async function retryTransient<T>(operation: () => Promise<T>): Promise<T> {
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      if (isDomError(error, "UnknownError") && attempt < 20) {
        await new Promise((resolve) =>
          setTimeout(resolve, Math.min(50, 2 ** Math.min(attempt, 6))),
        );
        continue;
      }
      throw error;
    }
  }
}
