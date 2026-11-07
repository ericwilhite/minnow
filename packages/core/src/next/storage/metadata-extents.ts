/**
 * Bounded pair streams over the private journal; no per-extent objects, index or run array.
 * Preparation does not authorize ownership. The caller revalidates immutable current/new
 * lists and supplied sealed writers under the transition barrier after preparation waits.
 * The complement of a lossy free map is never a source of ownership proof.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import { DATA_ALIGN, StoreCorruptionError } from "./contract.js";
import type { ExtentRef } from "./extents.js";
import {
  MetadataJournal,
  METADATA_PAYLOAD_BYTES,
  type ChainCursor,
  type ChainWriter,
} from "./metadata-journal.js";

const ENTRY = 16;
const CAPACITY = METADATA_PAYLOAD_BYTES / ENTRY;
type Duplicates = "exact" | "refuse";
function coordinates(offset: number, length: number): void {
  if (
    !Number.isSafeInteger(offset) ||
    offset < 0 ||
    offset % DATA_ALIGN !== 0 ||
    !Number.isSafeInteger(length) ||
    length <= 0 ||
    !Number.isSafeInteger(offset + length) ||
    !Number.isSafeInteger(offset + Math.ceil(length / DATA_ALIGN) * DATA_ALIGN)
  )
    throw new StoreCorruptionError("Invalid private extent coordinates");
}
async function check(work?: TaskWork): Promise<void> {
  const pending = pauseIfDue(work);
  if (pending !== undefined) await pending;
}

/** One mutable scalar pair, valid until the next next() call; the page stays with its cursor. */
export class ExtentPairs {
  offset = 0;
  length = 0;
  #view: DataView | null = null;
  #at = 0;
  #closed = false;
  #last = -1;
  #lastLength = 0;
  constructor(
    readonly cursor: ChainCursor,
    readonly ordered = false,
  ) {
    if (cursor.descriptor.length % ENTRY !== 0)
      throw new StoreCorruptionError("Truncated private extent pair");
  }
  async next(work?: TaskWork): Promise<boolean> {
    if (this.#closed) return false;
    this.cursor.journal.workspace.assertFrame(this.cursor.frame);
    if (this.#view === null || this.#at === this.#view.byteLength) {
      const bytes = await this.cursor.next(work);
      if (bytes === null) {
        this.#closed = true;
        this.#view = null;
        return false;
      }
      this.#view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      this.#at = 0;
    }
    this.offset = this.#view.getFloat64(this.#at, true);
    this.length = this.#view.getFloat64(this.#at + 8, true);
    this.#at += ENTRY;
    coordinates(this.offset, this.length);
    if (
      this.ordered &&
      this.offset < this.#last + Math.ceil(this.#lastLength / DATA_ALIGN) * DATA_ALIGN
    )
      throw new StoreCorruptionError("Unordered or repeated private extent input");
    this.#last = this.offset;
    this.#lastLength = this.length;
    return true;
  }
  async close(): Promise<void> {
    this.#closed = true;
    this.#view = null;
    await this.cursor.close();
  }
}

/** A malformed pair chain cannot leak the cursor's reference or frame slot. */
async function openPairs(
  journal: MetadataJournal,
  handle: number,
  slot: number,
  ordered = false,
): Promise<ExtentPairs> {
  const cursor = await journal.cursor(handle, slot);
  try {
    return new ExtentPairs(cursor, ordered);
  } catch (error) {
    await cursor.close();
    throw error;
  }
}
async function cleanup(
  a: ExtentPairs | null,
  b: ExtentPairs | null,
  writer: ChainWriter | null,
  failed: boolean,
): Promise<void> {
  let cleanupFailed = false,
    reason: unknown;
  for (const close of [() => a?.close(), () => b?.close(), () => writer?.abort()]) {
    try {
      await close();
    } catch (error) {
      if (!cleanupFailed) {
        cleanupFailed = true;
        reason = error;
      }
    }
  }
  if (!failed && cleanupFailed) throw reason;
}

/** One small prepaid header buffer, reused across every entry. */
export class ExtentPairWriter {
  readonly #pair = new Uint8Array(ENTRY);
  readonly #view = new DataView(this.#pair.buffer);
  #offset = -1;
  #length = 0;
  constructor(
    readonly writer: ChainWriter,
    readonly ordered: boolean,
    readonly duplicates: Duplicates = "refuse",
  ) {}
  async append(offset: number, length: number, work?: TaskWork): Promise<void> {
    coordinates(offset, length);
    if (this.ordered && this.#offset >= 0) {
      if (offset === this.#offset && length === this.#length && this.duplicates === "exact") return;
      if (offset < this.#offset + Math.ceil(this.#length / DATA_ALIGN) * DATA_ALIGN)
        throw new StoreCorruptionError("Repeated, overlapping or descending private extent pair");
    }
    this.#view.setFloat64(0, offset, true);
    this.#view.setFloat64(8, length, true);
    await this.writer.append(this.#pair, work);
    this.#offset = offset;
    this.#length = length;
  }
}

/** Source phase (slot 1 or 2). Copies inputs incrementally, never [...input] before a slice. */
export async function spoolExtents(
  journal: MetadataJournal,
  input: Iterable<ExtentRef> | AsyncIterable<ExtentRef>,
  slot: number,
  work?: TaskWork,
): Promise<number> {
  const writer = await journal.writer(slot),
    output = new ExtentPairWriter(writer, false);
  let produced = 0;
  try {
    let count = 0;
    await check(work);
    for await (const extent of input) {
      if (count++ % 128 === 0) await check(work);
      await output.append(extent.offset, extent.length, work);
    }
    await check(work);
    produced = await writer.finish();
    await check(work);
    return produced;
  } catch (error) {
    if (produced !== 0) {
      try {
        await journal.release(produced);
      } catch {
        /* Preserve the original failure. */
      }
    }
    try {
      await writer.abort();
    } catch {
      /* Native failure already failed the journal. */
    }
    throw error;
  }
}

/** In-place heapsort over one admitted chunk, checking the shared task clock every 128 steps. */
async function sortChunk(values: Float64Array, count: number, work?: TaskWork): Promise<void> {
  const compare = (a: number, b: number) =>
    (values[a * 2] ?? 0) - (values[b * 2] ?? 0) ||
    (values[a * 2 + 1] ?? 0) - (values[b * 2 + 1] ?? 0);
  const swap = (a: number, b: number) => {
    const offset = values[a * 2] ?? 0,
      length = values[a * 2 + 1] ?? 0;
    values[a * 2] = values[b * 2] ?? 0;
    values[a * 2 + 1] = values[b * 2 + 1] ?? 0;
    values[b * 2] = offset;
    values[b * 2 + 1] = length;
  };
  const sift = (root: number, end: number) => {
    while (root * 2 + 1 < end) {
      let child = root * 2 + 1;
      if (child + 1 < end && compare(child, child + 1) < 0) child++;
      if (compare(root, child) >= 0) break;
      swap(root, child);
      root = child;
    }
  };
  for (let at = Math.floor(count / 2) - 1; at >= 0; at--) {
    if (at % 128 === 0) await check(work);
    sift(at, count);
  }
  for (let at = count - 1; at > 0; at--) {
    if (at % 128 === 0) await check(work);
    swap(0, at);
    sift(0, at);
  }
  await check(work);
}

/** Merge phase: two immutable input cursors (1/2), one output (3), journal control (0). */
export async function mergeExtents(
  journal: MetadataJournal,
  left: number,
  right: number,
  mode: "union" | "difference",
  work?: TaskWork,
  duplicates: Duplicates = "exact",
): Promise<number> {
  let a: ExtentPairs | null = null,
    b: ExtentPairs | null = null,
    writer: ChainWriter | null = null;
  let failed = false,
    produced = 0;
  try {
    a = await openPairs(journal, left, 1, true);
    b = await openPairs(journal, right, 2, true);
    writer = await journal.writer(3);
    const output = new ExtentPairWriter(writer, true, duplicates);
    let hasA = await a.next(work),
      hasB = await b.next(work),
      steps = 0;
    while (hasA || hasB) {
      if (steps++ % 128 === 0) await check(work);
      if (hasA && (!hasB || a.offset < b.offset)) {
        await output.append(a.offset, a.length, work);
        hasA = await a.next(work);
      } else if (hasB && (!hasA || b.offset < a.offset)) {
        if (mode === "union") await output.append(b.offset, b.length, work);
        hasB = await b.next(work);
      } else {
        if (a.length !== b.length)
          throw new StoreCorruptionError("Roots disagree on private extent length");
        if (mode === "union") {
          if (duplicates === "refuse")
            throw new StoreCorruptionError("Repeated private extent pair");
          await output.append(a.offset, a.length, work);
        }
        hasA = await a.next(work);
        hasB = await b.next(work);
      }
    }
    await check(work);
    produced = await writer.finish();
    await check(work);
    writer = null;
    return produced;
  } catch (error) {
    failed = true;
    if (produced !== 0) {
      try {
        await journal.release(produced);
      } catch {
        /* Preserve the original failure. */
      }
    }
    throw error;
  } finally {
    await cleanup(a, b, writer, failed);
  }
}

/** Caller owns input; returned chain has its own reference. No resident array of sorted runs. */
export async function sortExtents(
  journal: MetadataJournal,
  input: number,
  duplicates: Duplicates,
  work?: TaskWork,
): Promise<number> {
  let head = 0,
    tail = 0,
    runs = 0,
    nextHead = 0,
    nextTail: number;
  let pairs: ExtentPairs | null = null;
  const clear = async (first: number) => {
    while (first !== 0) {
      await check(); // Cleanup is sliced but does not inherit the cancelled signal.
      const next = (await journal.describe(first)).next;
      await journal.release(first);
      first = next;
    }
  };
  try {
    pairs = await openPairs(journal, input, 1);
    const frame = journal.workspace.claim(2);
    try {
      const values = new Float64Array(frame.buffer, frame.byteOffset, CAPACITY * 2);
      let more = await pairs.next(work);
      do {
        let count = 0;
        while (more && count < CAPACITY) {
          if (count % 128 === 0) await check(work);
          values[count * 2] = pairs.offset;
          values[count * 2 + 1] = pairs.length;
          count++;
          more = await pairs.next(work);
        }
        await sortChunk(values, count, work);
        const writer = await journal.writer(3),
          output = new ExtentPairWriter(writer, true, duplicates);
        let handle: number;
        try {
          for (let at = 0; at < count; at++) {
            if (at % 128 === 0) await check(work);
            await output.append(values[at * 2] ?? 0, values[at * 2 + 1] ?? 0, work);
          }
          handle = await writer.finish();
        } catch (error) {
          try {
            await writer.abort();
          } catch {
            /* Preserve the original native failure. */
          }
          throw error;
        }
        if (tail !== 0) await journal.link(tail, handle);
        else head = handle;
        tail = handle;
        runs++;
      } while (more);
    } finally {
      journal.workspace.returnFrame(frame);
    }
    await pairs.close();
    pairs = null;
    while (runs > 1) {
      let nextRuns = 0;
      nextHead = 0;
      nextTail = 0;
      while (head !== 0) {
        await check(work);
        const left = head,
          right = (await journal.describe(left)).next;
        let handle: number;
        if (right === 0) {
          handle = left;
          head = 0;
        } else {
          const rest = (await journal.describe(right)).next;
          handle = await mergeExtents(journal, left, right, "union", work, duplicates);
          head = rest;
          await journal.release(left);
          await journal.release(right);
        }
        await journal.link(handle, 0);
        if (nextTail !== 0) await journal.link(nextTail, handle);
        else nextHead = handle;
        nextTail = handle;
        nextRuns++;
      }
      head = nextHead;
      runs = nextRuns;
      nextHead = 0;
    }
    await check(work);
    return head;
  } catch (error) {
    try {
      await clear(head);
    } catch {
      /* Failed journal is discarded on reopen. */
    }
    try {
      await clear(nextHead);
    } catch {
      /* Still release the input cursor below. */
    }
    await cleanup(pairs, null, null, true);
    pairs = null;
    throw error;
  } finally {
    await pairs?.close();
  }
}
