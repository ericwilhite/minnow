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
  type PendingBlob,
  type ReachableExtents,
} from "./extents.js";

/**
 * Ownership track: the startup order (storage/contract.ts "Startup order", rulings R13/R16.6).
 * The callbacks stand in for the other tracks' entry points.
 */
export async function startOwner(
  acquireAndOpen: () => Promise<StoreSession>,
  openExtentSpace: OpenExtentSpace,
  governor: { registerCache: Parameters<OpenExtentSpace>[1]["registerCache"] },
  startupGrant: ByteBudget,
  reachable: ReachableExtents,
  openWritePath: (space: ExtentSpace, session: StoreSession) => Promise<void>,
  sweepSpill: () => Promise<void>,
): Promise<ExtentSpace> {
  const session = await acquireAndOpen(); // 1–2: fence, then store session
  // 3: walk the current and previous roots; every crash orphan is free. No log is read here.
  const space = await openExtentSpace(session, {
    reachable,
    budget: startupGrant,
    registerCache: (cache) => governor.registerCache(cache),
  });
  await openWritePath(space, session); // 4: the single log scan, truncation and replay
  await sweepSpill(); // 5: then ready (6)
  return space;
}

/**
 * Format track: `reachable` walks the root through the space's verified, cached reader and yields
 * the blobs it reaches; each carries its extent, so nothing maps an address back to an extent.
 */
export const reachableExample: ReachableExtents = async function* (root, reader, budget) {
  for (const directory of decodeRootGroupDirectories(root)) {
    yield directory;
    const lease = await reader.read([directory], { budget });
    try {
      yield* decodeChunkBlobs(lease.bytes[0] ?? new Uint8Array(0));
    } finally {
      lease.release();
    }
  }
};

/**
 * Write path + format: a memtable flush writes one group (a chunk extent per column holding its
 * pages and then its page directory), a group root, and a new groups list, then publishes a root
 * that names them and retires the old groups list. Each chunk is streamed page by page into a
 * builder reserved from an estimate and extended when the estimate falls short (ruling R15.2).
 * Every part is ≤ MAX_BLOB_BYTES (1 MiB): a longer groups list would be split across blobs.
 */
export async function flushGroup(
  space: ExtentSpace,
  columns: ReadonlyArray<{ readonly pages: Iterable<Uint8Array>; readonly estimate: number }>,
  previousGroupsList: ExtentRef | null,
): Promise<number> {
  const writer = space.beginWrite("flush table 7");
  try {
    const chunks: Array<{ extent: ExtentRef; blobs: readonly BlobRef[] }> = [];
    for (const column of columns) {
      const builder = writer.open(column.estimate);
      const pending: PendingBlob[] = [];
      for (const page of column.pages) {
        const short = builder.length + page.length - builder.capacity;
        // Grow by at least a quarter, so a bad estimate costs few extends and at most one move.
        if (short > 0) builder.extend(Math.max(short, builder.capacity >> 2));
        pending.push(...(await builder.append([page])));
      }
      const directory = encodeDirectory(pending); // offsets within the chunk: stable at seal
      if (builder.length + directory.length > builder.capacity)
        builder.extend(builder.length + directory.length - builder.capacity);
      await builder.append([directory]);
      const sealed = await builder.seal();
      if (sealed === null) throw new Error("empty chunk");
      chunks.push(sealed);
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
declare function decodeChunkBlobs(directory: Uint8Array): BlobRef[];
declare function encodeDirectory(pages: readonly PendingBlob[]): Uint8Array;
declare function encodeGroupRoot(chunks: ReadonlyArray<{ extent: ExtentRef }>): Uint8Array;
declare function encodeGroupsList(groupRoot: readonly BlobRef[]): Uint8Array;
declare function encodeRoot(blobs: readonly BlobRef[]): Uint8Array;
declare function concatenate(parts: readonly Uint8Array[]): Uint8Array;
