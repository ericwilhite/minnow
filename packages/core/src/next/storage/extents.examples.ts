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

/**
 * Ownership track: the startup order (storage/contract.ts "Startup order", ruling R7). The
 * callbacks stand in for the other tracks' entry points.
 */
export async function startOwner(
  acquireAndOpen: () => Promise<StoreSession>,
  recoverLog: (session: StoreSession) => Promise<void>,
  openExtentSpace: OpenExtentSpace,
  governor: { registerCache: Parameters<OpenExtentSpace>[1]["registerCache"] },
  startupGrant: ByteBudget,
  reachable: ReachableExtents,
  openWritePath: (space: ExtentSpace, session: StoreSession) => Promise<void>,
  sweepSpill: () => Promise<void>,
): Promise<ExtentSpace> {
  const session = await acquireAndOpen(); // 1–2: fence, then session
  await recoverLog(session); // 3: scan and truncate the log; no data-space writes yet
  // 4: walk the current and previous roots; every crash orphan is free, nothing else is swept.
  const space = await openExtentSpace(session, {
    reachable,
    budget: startupGrant,
    registerCache: (cache) => governor.registerCache(cache),
  });
  await openWritePath(space, session); // 5: memtable replay over the space
  await sweepSpill(); // 6: then serve requests
  return space;
}

/** Format track: `reachable` walks the root through the space's verified, cached reader. */
export const reachableExample: ReachableExtents = async function* (root, reader, budget) {
  const groupDirectories: BlobRef[] = decodeRootGroupDirectories(root);
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
 * that names them and retires the old groups list. Every part is ≤ MAX_BLOB_BYTES (1 MiB): a
 * longer groups list would be split across several blobs.
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
  const pin = space.pin("merge table 7");
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
    // The inputs become reusable once this (the last older pin) is released and the next publish
    // after this one is durable (two-generation reclaim).
    pin.release();
  }
}

/**
 * Executor (through access/ snapshots): decode pages under the operator's grant, which is charged
 * for every leased byte until release; on `ReadDeniedError` borrow a progress loan of that size and
 * retry once. A batch's pages stay under MAX_READ_BYTES per call.
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

/** Key index: a warm point read stays synchronous end to end (ruling R6); a miss falls back to `read`. */
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
