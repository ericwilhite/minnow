/**
 * How the other tracks call the extent space (extents.ts). Compile-checked by `tsc`; nothing
 * imports this module and it does no I/O at import. Byte layouts here are placeholders: the
 * format track owns chunk, directory and root encodings.
 */
import type { StoreSession } from "./contract.js";
import {
  ReadDeniedError,
  type BlobRef,
  type ByteBudget,
  type ExtentRef,
  type ExtentSpace,
  type OpenExtentSpace,
  type ReachableExtents,
} from "./extents.js";

/** Ownership track: open the space once the lease and session are held. */
export async function openSpace(
  openExtentSpace: OpenExtentSpace,
  session: StoreSession,
  governor: { registerCache: Parameters<OpenExtentSpace>[1]["registerCache"] },
  reachable: ReachableExtents,
): Promise<ExtentSpace> {
  // The walk frees every crash orphan; nothing else is swept.
  return openExtentSpace(session, {
    reachable,
    registerCache: (cache) => governor.registerCache(cache),
  });
}

/** Format track: `reachable` walks the root through the space's verified, cached reader. */
export const reachableExample: ReachableExtents = async function* (root, reader) {
  const groupDirectories: BlobRef[] = decodeRootGroupDirectories(root);
  const budget: ByteBudget = { allocate: () => true, free: () => undefined };
  for (const directory of groupDirectories) {
    yield extentOf(directory);
    const lease = await reader.read([directory], { budget });
    try {
      for (const chunk of decodeChunkExtents(lease.bytes[0] ?? new Uint8Array(0))) yield chunk;
    } finally {
      lease.release();
    }
  }
};

/**
 * Write path + format: a memtable flush writes one group (a chunk extent per column holding its
 * pages and then its page directory), a group root, and a new groups list, then publishes a root
 * that names them and retires the old groups list.
 */
export async function flushGroup(
  space: ExtentSpace,
  columns: ReadonlyArray<{ readonly pages: readonly Uint8Array[]; readonly bound: number }>,
  previousGroupsList: ExtentRef | null,
): Promise<number> {
  const writer = space.beginWrite("flush table 7");
  try {
    const chunks: Array<{ extent: ExtentRef; pages: BlobRef[]; directory: BlobRef }> = [];
    for (const column of columns) {
      // Reserve an upper bound, append pages and then the directory, trim the rest at seal.
      const builder = writer.open(column.bound);
      const pages = await builder.append(column.pages);
      const [directory] = await builder.append([encodeDirectory(builder.offset, pages)]);
      const sealed = await builder.seal();
      if (sealed === null || directory === undefined) throw new Error("empty chunk");
      chunks.push({ extent: sealed.extent, pages, directory });
    }
    const { blobs: groupRoot } = await writer.write([encodeGroupRoot(chunks)]);
    const { blobs: groupsList } = await writer.write([encodeGroupsList(groupRoot)]);
    return await space.publish(encodeRoot(groupsList), {
      writers: [writer],
      retired: previousGroupsList === null ? [] : [previousGroupsList],
    });
  } catch (error) {
    // A rejected publish leaves the writer indeterminate (abort refuses); anything earlier aborts.
    if (writer.state === "open") await writer.abort();
    throw error;
  }
}

/**
 * Write path (maintenance): a merge pins the root it reads, writes its output, and retires its
 * inputs in the publish that drops them. Cancellation aborts the writer.
 */
export async function mergeGroups(
  space: ExtentSpace,
  inputs: readonly ExtentRef[],
  inputPages: readonly BlobRef[],
  budget: ByteBudget,
  signal: AbortSignal,
): Promise<void> {
  const pin = space.pin();
  const writer = space.beginWrite("merge table 7");
  try {
    const lease = await space.read(inputPages, { budget, signal });
    let merged: Uint8Array;
    try {
      merged = concatenate(lease.bytes);
    } finally {
      lease.release();
    }
    const { blobs } = await writer.write([merged]);
    signal.throwIfAborted();
    await space.publish(encodeRoot(blobs), { writers: [writer], retired: inputs });
  } catch (error) {
    if (writer.state === "open") await writer.abort();
    throw error;
  } finally {
    // The inputs become reusable once this (the last older pin) is released.
    pin.release();
  }
}

/**
 * Executor: decode pages under the operator's grant; on `ReadDeniedError` borrow a progress loan
 * of the missing size and retry once.
 */
export async function decodePages(
  space: ExtentSpace,
  pages: readonly BlobRef[],
  grant: ByteBudget & { borrow(bytes: number): Promise<{ repay(): void }> },
  decode: (page: Uint8Array) => void,
): Promise<void> {
  let loan: { repay(): void } | null = null;
  try {
    for (;;) {
      try {
        const lease = await space.read(pages, { budget: grant });
        try {
          for (const page of lease.bytes) decode(page);
        } finally {
          lease.release();
        }
        return;
      } catch (error) {
        if (!(error instanceof ReadDeniedError) || loan !== null) throw error;
        loan = await grant.borrow(error.bytes);
      }
    }
  } finally {
    loan?.repay();
  }
}

/** Key index: a warm point read stays synchronous; a miss falls back to `read`. */
export async function lookupFence(
  space: ExtentSpace,
  fences: BlobRef,
  budget: ByteBudget,
  search: (fences: Uint8Array) => number,
): Promise<number> {
  const cached = space.peek(fences);
  if (cached !== null) return search(cached);
  const lease = await space.read([fences], { budget });
  try {
    return search(lease.bytes[0] ?? new Uint8Array(0));
  } finally {
    lease.release();
  }
}

// Placeholder codecs (format track).
declare function decodeRootGroupDirectories(root: Uint8Array): BlobRef[];
declare function decodeChunkExtents(directory: Uint8Array): ExtentRef[];
declare function extentOf(blob: BlobRef): ExtentRef;
declare function encodeDirectory(chunkOffset: number, pages: readonly BlobRef[]): Uint8Array;
declare function encodeGroupRoot(chunks: ReadonlyArray<{ extent: ExtentRef }>): Uint8Array;
declare function encodeGroupsList(groupRoot: readonly BlobRef[]): Uint8Array;
declare function encodeRoot(blobs: readonly BlobRef[]): Uint8Array;
declare function concatenate(parts: readonly Uint8Array[]): Uint8Array;
