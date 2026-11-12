/** Benchmark fixtures only: copy a quiescent MemoryBackend's complete file image.
 * Restore before opening the ordinary core. No product backup/upgrade API is added.
 */
import { createHash } from "node:crypto";
import { mkdir, open, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type {
  OwnerLease,
  StorageBackend,
  SyncFile,
} from "../../packages/core/dist/next/storage/contract.js";

interface ImageFile {
  name: string;
  size: number;
  capacity: number;
  sha256: string;
}
interface Image {
  version: 1;
  revision: string;
  files: ImageFile[];
}
interface MemoryFiles {
  log: SyncFile;
  data: SyncFile;
  roots: readonly SyncFile[];
  metadata?: SyncFile;
  metadataIndex?: SyncFile;
}
function filesOf(lease: OwnerLease): ReadonlyMap<string, SyncFile> {
  if (!("files" in lease)) throw new TypeError("A benchmark image requires MemoryBackend");
  const files = lease.files as MemoryFiles;
  const entries: Array<[string, SyncFile]> = [
    ["log", files.log],
    ["data", files.data],
  ];
  files.roots.forEach((file, i) => entries.push([`root${String(i)}`, file]));
  if (files.metadata !== undefined) entries.push(["metadata", files.metadata]);
  if (files.metadataIndex !== undefined) entries.push(["metadata-index", files.metadataIndex]);
  return new Map(entries);
}
const check = (signal: AbortSignal | undefined): void => signal?.throwIfAborted();
const capacityFor = (size: number): number =>
  size === 0 ? 0 : Math.max(4096, 2 ** Math.ceil(Math.log2(size)));

/** Install before the first open. MemoryFile doubles on growth and retains capacity on
 * truncate; capturing sizes alone could silently shrink the benchmark's live allocation.
 * Stop after quiescing the writer, before capturing/reopening. No read/engine method changes.
 */
export function monitorNativeCapacity(backend: StorageBackend) {
  let latest: OwnerLease | undefined,
    stopped = false;
  const descriptor = Object.getOwnPropertyDescriptor(backend, "acquire");
  const acquire = backend.acquire.bind(backend),
    capacities = new Map<string, number>();
  const undo: Array<() => void> = [];
  const track = (name: string, size: number) =>
    capacities.set(name, Math.max(capacities.get(name) ?? 0, capacityFor(size)));
  backend.acquire = async (options) => {
    const lease = await acquire(options);
    latest = lease;
    for (const [name, file] of filesOf(lease)) {
      track(name, file.getSize());
      const writeDescriptor = Object.getOwnPropertyDescriptor(file, "write"),
        truncateDescriptor = Object.getOwnPropertyDescriptor(file, "truncate");
      const write = file.write.bind(file),
        truncate = file.truncate.bind(file);
      file.write = (buffer, options) => {
        const n = write(buffer, options);
        track(name, options.at + n);
        return n;
      };
      file.truncate = (size) => {
        truncate(size);
        track(name, size);
      };
      undo.push(() => {
        if (writeDescriptor === undefined) Reflect.deleteProperty(file, "write");
        else Object.defineProperty(file, "write", writeDescriptor);
        if (truncateDescriptor === undefined) Reflect.deleteProperty(file, "truncate");
        else Object.defineProperty(file, "truncate", truncateDescriptor);
      });
    }
    return lease;
  };
  return {
    capacities: capacities as ReadonlyMap<string, number>,
    get lease(): OwnerLease {
      if (latest === undefined) throw new Error("No native benchmark lease has been acquired");
      return latest;
    },
    stop() {
      if (stopped) return new Map(capacities);
      stopped = true;
      if (descriptor === undefined) Reflect.deleteProperty(backend, "acquire");
      else Object.defineProperty(backend, "acquire", descriptor);
      for (const restore of undo) restore();
      undo.length = 0;
      return new Map(capacities);
    },
  };
}

/** Caller must quiesce the writer first and keep it quiescent throughout this copy.
 * Borrow the existing lease without releasing it: capturing the committed log tail before
 * close preserves the original input rather than forcing a final checkpoint/row reorder.
 * The output directory must not exist; publish its manifest only after all files complete.
 */
export async function captureNativeLease(
  lease: OwnerLease,
  directory: string,
  revision: string,
  signal?: AbortSignal,
  capacities?: ReadonlyMap<string, number>,
): Promise<Image> {
  check(signal);
  lease.signal.throwIfAborted();
  await mkdir(directory);
  try {
    const image: Image = { version: 1, revision, files: [] };
    const buffer = new Uint8Array(1 << 20);
    for (const [name, source] of filesOf(lease)) {
      const size = source.getSize(),
        hash = createHash("sha256");
      const output = await open(join(directory, name + ".bin"), "wx");
      try {
        for (let at = 0; at < size;) {
          check(signal);
          lease.signal.throwIfAborted();
          const n = source.read(buffer.subarray(0, Math.min(buffer.length, size - at)), { at });
          if (n <= 0) throw new Error("A native image source was truncated");
          hash.update(buffer.subarray(0, n));
          for (let written = 0; written < n;) {
            const result = await output.write(buffer, written, n - written, at + written);
            if (result.bytesWritten <= 0) throw new Error("A native image write made no progress");
            written += result.bytesWritten;
          }
          at += n;
        }
      } finally {
        await output.close();
      }
      image.files.push({
        name,
        size,
        capacity: capacities?.get(name) ?? capacityFor(size),
        sha256: hash.digest("hex"),
      });
    }
    check(signal);
    lease.signal.throwIfAborted();
    await writeFile(join(directory, "manifest.pending"), JSON.stringify(image));
    await rename(join(directory, "manifest.pending"), join(directory, "manifest.json"));
    return image;
  } catch (error) {
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}

/** Convenience for a closed core. Active-core captures use its already owned lease above. */
export async function captureNativeImage(
  backend: StorageBackend,
  directory: string,
  revision: string,
  signal?: AbortSignal,
  capacities?: ReadonlyMap<string, number>,
): Promise<Image> {
  check(signal);
  const lease = await backend.acquire();
  try {
    return await captureNativeLease(lease, directory, revision, signal, capacities);
  } finally {
    await lease.release();
  }
}

/** Restore into an unopened backend. A failed restore releases its lease and never opens
 * the core; retry overwrites every slot. Each file's full size and hash are checked.
 */
export async function restoreNativeImage(
  backend: StorageBackend,
  directory: string,
  revision: string,
  signal?: AbortSignal,
): Promise<Image> {
  check(signal);
  const image = JSON.parse(await readFile(join(directory, "manifest.json"), "utf8")) as Image;
  if (image.version !== 1 || image.revision !== revision || !Array.isArray(image.files))
    throw new TypeError("Native benchmark image version/revision mismatch");
  const lease = await backend.acquire();
  try {
    check(signal);
    const targets = filesOf(lease);
    if (
      image.files.length !== targets.size ||
      new Set(image.files.map((file) => file.name)).size !== targets.size ||
      image.files.some(
        (file) =>
          !targets.has(file.name) ||
          !Number.isSafeInteger(file.size) ||
          file.size < 0 ||
          !Number.isSafeInteger(file.capacity) ||
          file.capacity < file.size ||
          (file.capacity !== 0 &&
            (file.capacity < 4096 || !Number.isInteger(Math.log2(file.capacity)))) ||
          !/^[a-f0-9]{64}$/u.test(file.sha256),
      )
    )
      throw new TypeError("Invalid native benchmark image inventory");
    const buffer = new Uint8Array(1 << 20);
    for (const file of image.files) {
      check(signal);
      const target = targets.get(file.name);
      if (target === undefined) throw new TypeError("Missing native image target");
      const input = await open(join(directory, file.name + ".bin"), "r");
      try {
        if ((await input.stat()).size !== file.size)
          throw new Error("Native benchmark image length mismatch");
        target.truncate(file.capacity);
        target.truncate(file.size);
        const hash = createHash("sha256");
        for (let at = 0; at < file.size;) {
          check(signal);
          const { bytesRead } = await input.read(
            buffer,
            0,
            Math.min(buffer.length, file.size - at),
            at,
          );
          check(signal);
          if (bytesRead <= 0) throw new Error("A native image read made no progress");
          hash.update(buffer.subarray(0, bytesRead));
          for (let written = 0; written < bytesRead;) {
            const n = target.write(buffer.subarray(written, bytesRead), { at: at + written });
            if (n <= 0) throw new Error("A native restore write made no progress");
            written += n;
          }
          at += bytesRead;
        }
        if (hash.digest("hex") !== file.sha256)
          throw new Error("Native benchmark image hash mismatch");
        target.flush();
      } finally {
        await input.close();
      }
    }
    return image;
  } finally {
    await lease.release();
  }
}
