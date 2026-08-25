/**
 * Path encoding and file IO for the OPFS store.
 *
 * Names that come from callers (database names, temp owner and run ids) become one
 * conservatively percent-encoded entry name each. Encoding is bijective — `[A-Za-z0-9_-]` and
 * non-leading `.` pass through, everything else becomes `%XX` per UTF-8 byte — so names
 * round-trip exactly and none can collide with or escape into another's path.
 */

import { assertWellFormedString } from "../../block-format/unicode.js";
import { writeFully, type SyncFileHandle } from "../toolkit/sync-file.js";

const SAFE_SEGMENT_CHARS = /^[A-Za-z0-9._-]$/;
const UPPERCASE_HEX_BYTE = /^[0-9A-F]{2}$/;
const segmentTextEncoder = new TextEncoder();
const segmentTextDecoder = new TextDecoder("utf-8", { fatal: true });
/** Conservative cross-browser/filesystem ceiling for one already-encoded OPFS entry name. */
export const MAX_OPFS_ENCODED_SEGMENT_CHARACTERS = 240;
/** Directory handles have no close operation, so bound retained wrappers under path churn. */
export const MAX_OPFS_DIRECTORY_HANDLE_CACHE_ENTRIES = 128;
/** Internal layouts are shallow; reject adversarial/custom paths before recursive resolution. */
export const MAX_OPFS_PATH_SEGMENTS = 16;

function assertEncodedSegmentLength(length: number): void {
  if (length > MAX_OPFS_ENCODED_SEGMENT_CHARACTERS) {
    throw new RangeError(
      `Encoded OPFS path segment exceeds ${String(MAX_OPFS_ENCODED_SEGMENT_CHARACTERS)} characters`,
    );
  }
}

export function encodeSegment(segment: string): string {
  assertWellFormedString(segment, "OPFS path segment");
  if (segment.length === 0) return "%";
  let encoded = "";
  for (const char of segment) {
    if (SAFE_SEGMENT_CHARS.test(char) && !(encoded.length === 0 && char === ".")) {
      encoded += char;
      assertEncodedSegmentLength(encoded.length);
      continue;
    }
    for (const byte of segmentTextEncoder.encode(char)) {
      encoded += `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
      assertEncodedSegmentLength(encoded.length);
    }
  }
  return encoded;
}

export function decodeSegment(encoded: string): string {
  assertEncodedSegmentLength(encoded.length);
  if (encoded === "%") return "";
  if (encoded.length === 0) throw new Error("Invalid OPFS path segment encoding");
  const bytes: number[] = [];
  for (let index = 0; index < encoded.length;) {
    const char = encoded[index] ?? "";
    if (char === "%") {
      const hex = encoded.slice(index + 1, index + 3);
      if (!UPPERCASE_HEX_BYTE.test(hex)) {
        throw new Error("Invalid OPFS path segment encoding");
      }
      bytes.push(Number.parseInt(hex, 16));
      index += 3;
      continue;
    }
    if (!SAFE_SEGMENT_CHARS.test(char)) {
      throw new Error("Invalid OPFS path segment encoding");
    }
    bytes.push(char.charCodeAt(0));
    index += 1;
  }
  let decoded: string;
  try {
    decoded = segmentTextDecoder.decode(new Uint8Array(bytes));
  } catch {
    throw new Error("Invalid OPFS path segment encoding");
  }
  if (encodeSegment(decoded) !== encoded) {
    throw new Error("Non-canonical OPFS path segment encoding");
  }
  return decoded;
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
  /** Refuse before `arrayBuffer()` allocates when the file exceeds this exact byte ceiling. */
  maxBytes?: number;
}

function validatePath(path: readonly string[]): void {
  if (path.length > MAX_OPFS_PATH_SEGMENTS) {
    throw new RangeError(`OPFS path exceeds ${String(MAX_OPFS_PATH_SEGMENTS)} segments`);
  }
  for (const segment of path) {
    if (segment.length === 0 || segment.length > MAX_OPFS_ENCODED_SEGMENT_CHARACTERS) {
      throw new RangeError("OPFS path contains an invalid segment");
    }
  }
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

  /** @internal Hardening-test visibility for the cache's fixed-memory invariant. */
  get directoryCacheSizeForTests(): number {
    return this.#directories.size;
  }

  async getDirectory(path: readonly string[], create: boolean): Promise<FileSystemDirectoryHandle> {
    validatePath(path);
    if (path.length === 0) return this.#root;
    const key = path.join("/");
    const cached = this.#directories.get(key);
    if (cached !== undefined) {
      // Map insertion order is the LRU order. Refresh without retaining a second reference.
      this.#directories.delete(key);
      this.#directories.set(key, cached);
      return cached;
    }
    const parent = await this.getDirectory(path.slice(0, -1), create);
    const directory = await retryTransient(() =>
      parent.getDirectoryHandle(path[path.length - 1] ?? "", { create }),
    );
    this.#directories.set(key, directory);
    if (this.#directories.size > MAX_OPFS_DIRECTORY_HANDLE_CACHE_ENTRIES) {
      const oldest = this.#directories.keys().next().value;
      if (oldest !== undefined) this.#directories.delete(oldest);
    }
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
    validatePath(path);
    if (
      options.maxBytes !== undefined &&
      (!Number.isSafeInteger(options.maxBytes) || options.maxBytes < 0)
    ) {
      throw new RangeError("OPFS read byte limit must be a non-negative safe integer");
    }
    try {
      return await retryTransient(async () => {
        const directory = await this.getDirectory(path.slice(0, -1), false);
        const handle = await directory.getFileHandle(path[path.length - 1] ?? "");
        const file = await handle.getFile();
        if (options.maxBytes !== undefined && file.size > options.maxBytes) {
          throw new RangeError(`OPFS file exceeds the ${String(options.maxBytes)}-byte read limit`);
        }
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

  /** File length without materializing its contents; `undefined` when the path is absent. */
  async fileSize(path: readonly string[]): Promise<number | undefined> {
    validatePath(path);
    try {
      return await retryTransient(async () => {
        const directory = await this.getDirectory(path.slice(0, -1), false);
        const handle = await directory.getFileHandle(path[path.length - 1] ?? "");
        return (await handle.getFile()).size;
      });
    } catch (error) {
      if (isDomError(error, "NotFoundError") || isDomError(error, "TypeMismatchError")) {
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
    validatePath(path);
    await retryTransient(async () => {
      const handle = await this.openHandle(path, { create: true });
      try {
        handle.truncate(0);
        writeFully(handle, bytes, 0, `writing ${path.join("/")}`);
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
    validatePath(path);
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
    validatePath(path);
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
    validatePath(path);
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

  /** Names directly inside a directory, streamed without retaining the directory cardinality. */
  async *iterateNames(path: readonly string[]): AsyncGenerator<string> {
    validatePath(path);
    let directory: FileSystemDirectoryHandle;
    try {
      directory = await this.getDirectory(path, false);
    } catch (error) {
      if (isDomError(error, "NotFoundError") || isDomError(error, "TypeMismatchError")) {
        return;
      }
      throw error;
    }
    for await (const name of directory.keys()) yield name;
  }

  /** Every file under a directory, streamed with O(path depth) retained memory. */
  async *walkFiles(path: readonly string[]): AsyncGenerator<{ path: string[]; size: number }> {
    validatePath(path);
    let root: FileSystemDirectoryHandle;
    try {
      root = await this.getDirectory(path, false);
    } catch (error) {
      if (isDomError(error, "NotFoundError")) return;
      throw error;
    }
    const walk = async function* (
      directory: FileSystemDirectoryHandle,
      prefix: string[],
    ): AsyncGenerator<{ path: string[]; size: number }> {
      for await (const [name, entry] of directory) {
        if (entry.kind === "file") {
          try {
            const file = await (entry as FileSystemFileHandle).getFile();
            yield { path: [...prefix, name], size: file.size };
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
          yield* walk(entry as FileSystemDirectoryHandle, [...prefix, name]);
        }
      }
    };
    yield* walk(root, []);
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
