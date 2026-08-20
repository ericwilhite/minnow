/**
 * An in-memory Origin Private File System for Node tests: enough of
 * `FileSystemDirectoryHandle` / `FileSystemFileHandle` / `FileSystemSyncAccessHandle` for the
 * OPFS block store, with the semantics that make OPFS interesting to test against:
 *
 * - **Exclusive sync-access locks.** One open handle per file across every directory handle on
 *   the same shim — a second `createSyncAccessHandle` throws `NoModificationAllowedError`,
 *   exactly the cross-tab arbitration the store's command log builds on. `getFile` and
 *   `removeEntry` on a locked file also refuse, the strictest reading of the underspecified
 *   parts of the spec, so a store that passes here never depends on browser-specific leniency.
 * - **Real DOMException identities.** `NotFoundError`, `TypeMismatchError`,
 *   `InvalidModificationError`, `NoModificationAllowedError`, and injected
 *   `QuotaExceededError`s are actual `DOMException`s, since error identity is part of the
 *   storage contract.
 * - **Injectable write faults.** `setWriteFault` throws from inside sync writes, creates, or
 *   flushes — how the quota and crash suites produce a failure at an exact byte boundary.
 *
 * For applications, `MemoryBlockStore` remains the right store in Node tests. This shim is for
 * adapter authors: it hosts `OpfsBlockStore` (pass `root`) or anything built on the storage
 * toolkit's `SyncFileHandle` surface, in plain vitest, with crashes and quota failures on tap.
 */

interface FileNode {
  kind: "file";
  name: string;
  bytes: Uint8Array;
  lockHolder: ShimSyncAccessHandle | null;
}

interface DirectoryNode {
  kind: "directory";
  name: string;
  children: Map<string, FileNode | DirectoryNode>;
}

export type WriteFault = (path: string, phase: "create" | "write" | "flush") => void;

export class MemoryOpfs {
  readonly #rootNode: DirectoryNode = { kind: "directory", name: "", children: new Map() };
  #writeFault: WriteFault | null = null;

  /** The tree's root, shaped like `navigator.storage.getDirectory()`'s result. */
  get root(): FileSystemDirectoryHandle {
    return new ShimDirectoryHandle(
      this,
      this.#rootNode,
      "",
    ) as unknown as FileSystemDirectoryHandle;
  }

  /** Installs (or clears, with `null`) a fault thrown from inside write paths. */
  setWriteFault(fault: WriteFault | null): void {
    this.#writeFault = fault;
  }

  maybeFault(path: string, phase: "create" | "write" | "flush"): void {
    this.#writeFault?.(path, phase);
  }

  /** Test-side peek: the file's current bytes, or undefined. Bypasses locks on purpose. */
  readFileBytes(path: string): Uint8Array | undefined {
    const node = this.#find(path);
    return node?.kind === "file" ? node.bytes.slice() : undefined;
  }

  /** Test-side poke: writes bytes directly, creating parents. Bypasses locks on purpose. */
  writeFileBytes(path: string, bytes: Uint8Array): void {
    const segments = path.split("/").filter((segment) => segment.length > 0);
    const name = segments.pop();
    if (name === undefined) throw new Error("A file path cannot be empty");
    let directory = this.#rootNode;
    for (const segment of segments) {
      let child = directory.children.get(segment);
      if (child === undefined) {
        child = { kind: "directory", name: segment, children: new Map() };
        directory.children.set(segment, child);
      }
      if (child.kind !== "directory") throw new Error(`Not a directory: ${segment}`);
      directory = child;
    }
    const existing = directory.children.get(name);
    if (existing?.kind === "directory") throw new Error(`Not a file: ${path}`);
    directory.children.set(name, {
      kind: "file",
      name,
      bytes: bytes.slice(),
      lockHolder: existing?.lockHolder ?? null,
    });
  }

  #find(path: string): FileNode | DirectoryNode | undefined {
    let node: FileNode | DirectoryNode = this.#rootNode;
    for (const segment of path.split("/").filter((segment) => segment.length > 0)) {
      if (node.kind !== "directory") return undefined;
      const child = node.children.get(segment);
      if (child === undefined) return undefined;
      node = child;
    }
    return node;
  }
}

function notFound(name: string): DOMException {
  return new DOMException(
    `A requested file or directory could not be found: ${name}`,
    "NotFoundError",
  );
}

function typeMismatch(name: string): DOMException {
  return new DOMException(`The path is not of the requested type: ${name}`, "TypeMismatchError");
}

function locked(name: string): DOMException {
  return new DOMException(
    `Access handles cannot be created if there is another open access handle: ${name}`,
    "NoModificationAllowedError",
  );
}

class ShimDirectoryHandle {
  readonly kind = "directory" as const;
  readonly #shim: MemoryOpfs;
  readonly #node: DirectoryNode;
  readonly #path: string;

  constructor(shim: MemoryOpfs, node: DirectoryNode, path: string) {
    this.#shim = shim;
    this.#node = node;
    this.#path = path;
  }

  get name(): string {
    return this.#node.name;
  }

  #childPath(name: string): string {
    return this.#path.length === 0 ? name : `${this.#path}/${name}`;
  }

  async getFileHandle(name: string, options?: { create?: boolean }): Promise<ShimFileHandle> {
    validateEntryName(name);
    const existing = this.#node.children.get(name);
    if (existing === undefined) {
      if (options?.create !== true) throw notFound(name);
      this.#shim.maybeFault(this.#childPath(name), "create");
      const node: FileNode = { kind: "file", name, bytes: new Uint8Array(0), lockHolder: null };
      this.#node.children.set(name, node);
      return new ShimFileHandle(this.#shim, node, this.#childPath(name));
    }
    if (existing.kind !== "file") throw typeMismatch(name);
    return new ShimFileHandle(this.#shim, existing, this.#childPath(name));
  }

  async getDirectoryHandle(
    name: string,
    options?: { create?: boolean },
  ): Promise<ShimDirectoryHandle> {
    validateEntryName(name);
    const existing = this.#node.children.get(name);
    if (existing === undefined) {
      if (options?.create !== true) throw notFound(name);
      this.#shim.maybeFault(this.#childPath(name), "create");
      const node: DirectoryNode = { kind: "directory", name, children: new Map() };
      this.#node.children.set(name, node);
      return new ShimDirectoryHandle(this.#shim, node, this.#childPath(name));
    }
    if (existing.kind !== "directory") throw typeMismatch(name);
    return new ShimDirectoryHandle(this.#shim, existing, this.#childPath(name));
  }

  async removeEntry(name: string, options?: { recursive?: boolean }): Promise<void> {
    validateEntryName(name);
    const existing = this.#node.children.get(name);
    if (existing === undefined) throw notFound(name);
    if (existing.kind === "file") {
      if (existing.lockHolder !== null) throw locked(name);
    } else if (options?.recursive !== true) {
      if (existing.children.size > 0) {
        throw new DOMException(`The directory is not empty: ${name}`, "InvalidModificationError");
      }
    } else {
      assertSubtreeUnlocked(existing, name);
    }
    this.#node.children.delete(name);
  }

  async isSameEntry(other: unknown): Promise<boolean> {
    return other instanceof ShimDirectoryHandle && other.#node === this.#node;
  }

  async resolve(): Promise<string[] | null> {
    return null;
  }

  async *entries(): AsyncIterableIterator<[string, ShimFileHandle | ShimDirectoryHandle]> {
    // Snapshot first: callers may mutate while iterating, as with the real API.
    for (const [name, node] of [...this.#node.children.entries()]) {
      if (this.#node.children.get(name) !== node) continue;
      yield [
        name,
        node.kind === "file"
          ? new ShimFileHandle(this.#shim, node, this.#childPath(name))
          : new ShimDirectoryHandle(this.#shim, node, this.#childPath(name)),
      ];
    }
  }

  async *keys(): AsyncIterableIterator<string> {
    for await (const [name] of this.entries()) yield name;
  }

  async *values(): AsyncIterableIterator<ShimFileHandle | ShimDirectoryHandle> {
    for await (const [, handle] of this.entries()) yield handle;
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<[string, ShimFileHandle | ShimDirectoryHandle]> {
    return this.entries();
  }
}

function assertSubtreeUnlocked(node: DirectoryNode, name: string): void {
  for (const child of node.children.values()) {
    if (child.kind === "file") {
      if (child.lockHolder !== null) throw locked(`${name}/${child.name}`);
    } else {
      assertSubtreeUnlocked(child, `${name}/${child.name}`);
    }
  }
}

function validateEntryName(name: string): void {
  if (name.length === 0 || name === "." || name === ".." || name.includes("/")) {
    throw new TypeError(`Entry name is invalid: ${name}`);
  }
}

class ShimFileHandle {
  readonly kind = "file" as const;
  readonly #shim: MemoryOpfs;
  readonly #node: FileNode;
  readonly #path: string;

  constructor(shim: MemoryOpfs, node: FileNode, path: string) {
    this.#shim = shim;
    this.#node = node;
    this.#path = path;
  }

  get name(): string {
    return this.#node.name;
  }

  async getFile(): Promise<File> {
    if (this.#node.lockHolder !== null) throw locked(this.#node.name);
    return new File([this.#node.bytes.slice()], this.#node.name);
  }

  async createSyncAccessHandle(): Promise<ShimSyncAccessHandle> {
    if (this.#node.lockHolder !== null) throw locked(this.#node.name);
    const handle = new ShimSyncAccessHandle(this.#shim, this.#node, this.#path);
    this.#node.lockHolder = handle;
    return handle;
  }

  async isSameEntry(other: unknown): Promise<boolean> {
    return other instanceof ShimFileHandle && other.#node === this.#node;
  }
}

class ShimSyncAccessHandle {
  readonly #shim: MemoryOpfs;
  readonly #node: FileNode;
  readonly #path: string;
  #closed = false;

  constructor(shim: MemoryOpfs, node: FileNode, path: string) {
    this.#shim = shim;
    this.#node = node;
    this.#path = path;
  }

  #assertOpen(): void {
    if (this.#closed) {
      throw new DOMException("The access handle is closed", "InvalidStateError");
    }
  }

  getSize(): number {
    this.#assertOpen();
    return this.#node.bytes.byteLength;
  }

  read(buffer: ArrayBufferView, options?: { at?: number }): number {
    this.#assertOpen();
    const at = options?.at ?? 0;
    const target = new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    const source = this.#node.bytes.subarray(at, at + target.byteLength);
    target.set(source);
    return source.byteLength;
  }

  write(buffer: ArrayBufferView, options?: { at?: number }): number {
    this.#assertOpen();
    this.#shim.maybeFault(this.#path, "write");
    const at = options?.at ?? 0;
    const source = new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    const end = at + source.byteLength;
    if (end > this.#node.bytes.byteLength) {
      const grown = new Uint8Array(end);
      grown.set(this.#node.bytes);
      this.#node.bytes = grown;
    }
    this.#node.bytes.set(source, at);
    return source.byteLength;
  }

  truncate(size: number): void {
    this.#assertOpen();
    this.#shim.maybeFault(this.#path, "write");
    if (size <= this.#node.bytes.byteLength) {
      this.#node.bytes = this.#node.bytes.slice(0, size);
      return;
    }
    const grown = new Uint8Array(size);
    grown.set(this.#node.bytes);
    this.#node.bytes = grown;
  }

  flush(): void {
    this.#assertOpen();
    this.#shim.maybeFault(this.#path, "flush");
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#node.lockHolder === this) this.#node.lockHolder = null;
  }
}
