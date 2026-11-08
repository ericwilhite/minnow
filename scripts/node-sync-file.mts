/**
 * `SyncFile` (packages/core/src/next/storage/contract.ts) over Node's synchronous file API, so
 * the real file store, extent space and page cache run over files on disk in benchmarks (as
 * OPFS sync access handles do in a browser). Benchmark support only; nothing ships it.
 */
import {
  closeSync,
  fstatSync,
  ftruncateSync,
  mkdirSync,
  openSync,
  readSync,
  writeSync,
} from "node:fs";
import { join } from "node:path";
import type { SyncFile } from "../packages/core/src/next/storage/contract.ts";
import type { FileSet } from "../packages/core/src/next/storage/file-store.ts";

export class NodeSyncFile implements SyncFile {
  readonly #fd: number;
  constructor(path: string, create: boolean) {
    this.#fd = openSync(path, create ? "w+" : "r+");
  }
  read(buffer: Uint8Array, options: { at: number }): number {
    return readSync(this.#fd, buffer, 0, buffer.length, options.at);
  }
  write(buffer: Uint8Array, options: { at: number }): number {
    return writeSync(this.#fd, buffer, 0, buffer.length, options.at);
  }
  truncate(size: number): void {
    ftruncateSync(this.#fd, size);
  }
  getSize(): number {
    return fstatSync(this.#fd).size;
  }
  /** Durability is not what the benchmarks measure: the OS page cache keeps the bytes. */
  flush(): void {
    return undefined;
  }
  close(): void {
    closeSync(this.#fd);
  }
}

/**
 * The store's files in `directory` (created empty when `create`): log, data, both root slots, and
 * the extent space's owner-private metadata pair, named as OPFS names them.
 */
export function openFileSet(directory: string, create: boolean): FileSet {
  mkdirSync(directory, { recursive: true });
  const file = (name: string): NodeSyncFile => new NodeSyncFile(join(directory, name), create);
  return {
    log: file("log"),
    data: file("data"),
    roots: [file("root-0"), file("root-1")],
    // Reconstructible scratch the extent space resets on every open: always (re)created, so a
    // fixture built before these files existed still opens.
    metadata: new NodeSyncFile(join(directory, "space-meta"), true),
    metadataIndex: new NodeSyncFile(join(directory, "space-meta-index"), true),
  };
}

/** JSON that keeps bigints and byte arrays (column statistics). */
export const jsonReplacer = (_key: string, value: unknown): unknown =>
  typeof value === "bigint"
    ? { $bigint: value.toString() }
    : value instanceof Uint8Array
      ? { $bytes: Buffer.from(value).toString("base64") }
      : value;

export const jsonReviver = (_key: string, value: unknown): unknown => {
  if (value !== null && typeof value === "object") {
    const record = value as Record<string, unknown>;
    if (typeof record.$bigint === "string") return BigInt(record.$bigint);
    if (typeof record.$bytes === "string")
      return new Uint8Array(Buffer.from(record.$bytes, "base64"));
  }
  return value;
};
