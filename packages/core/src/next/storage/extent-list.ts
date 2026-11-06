/**
 * Ruling R25: each published root references a paged, offset-sorted list of the exact extents it
 * reaches, so startup rebuilds free space from the current and previous roots' lists (two reads
 * of O(extents / LIST_PAGE_EXTENTS) pages) instead of walking every group root.
 *
 * ## The list
 * - **Pages.** A list is an ordered array of pages (`ExtentListPage`), each a blob in the data
 *   space holding a run of extents in strictly increasing offset order; every extent of a page is
 *   below the next page's `first`. A page holds at most `LIST_PAGE_EXTENTS` extents (64 KiB).
 * - **What it names.** Every extent the root reaches except the extents the list's own pages live
 *   in: the root names those directly (its list field), so a reader yields them from the field.
 * - **Page bytes.** `MXL1` (u32 LE), version 1 (u16), reserved (u16), count (u32), reserved (u32),
 *   then `count` × (offset, length) as float64 LE (exact safe integers). The page's `BlobRef`
 *   carries its checksum; decoding also checks the count, the first offset and the order against
 *   the root's entry, so a page from another list or another version is refused.
 *
 * ## Keeping it (`ExtentListKeeper`, the write path's `ExtentDeltaSink`)
 * Each publication's delta (`added`, `released`) touches only the pages whose offset ranges it
 * falls in: those are read, edited, re-split (or merged with a small neighbour) and written as new
 * blobs through the publication's own writer; every other page is shared with the previous list
 * by reference. The replaced pages' extents, once no page of the new list lives in them, are
 * retired by the same publication (R8: held while the previous root or a pin reaches them). What
 * `prepare` builds is provisional: `commit` installs it, `abort` drops it. A keeper is built from
 * the durable root's list at open, never from earlier callbacks (a sink that failed mid-publication
 * is rebuilt by reopening).
 */
import { pauseIfDue, sortInSlices, type TaskWork } from "../task.js";
import { StoreCorruptionError } from "./contract.js";
import {
  MAX_READ_BYTES,
  ReadDeniedError,
  type BlobRef,
  type ByteBudget,
  type ExtentReader,
  type ExtentRef,
  type ExtentWriter,
} from "./extents.js";

/** Extents per list page at most (16 bytes each: 64 KiB of entries). */
export const LIST_PAGE_EXTENTS = 4096;

const MAGIC = 0x314c584d; // "MXL1"
const VERSION = 1;
const HEADER = 16;
const ENTRY = 16;

/** One page of a root's extent list, as the root names it. */
export interface ExtentListPage {
  readonly blob: BlobRef;
  /** Offset of the page's first extent. */
  readonly first: number;
  /** Extents in the page (≥ 1). */
  readonly count: number;
}

/** A root's extent list: its pages in offset order. */
export type ExtentList = readonly ExtentListPage[];

export function encodeListPage(extents: readonly ExtentRef[]): Uint8Array {
  const bytes = new Uint8Array(HEADER + extents.length * ENTRY);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, MAGIC, true);
  view.setUint16(4, VERSION, true);
  view.setUint32(8, extents.length, true);
  extents.forEach((extent, index) => {
    view.setFloat64(HEADER + index * ENTRY, extent.offset, true);
    view.setFloat64(HEADER + index * ENTRY + 8, extent.length, true);
  });
  return bytes;
}

/** The extents of `page` (verified bytes `bytes`), checked against the root's entry for it. */
export function decodeListPage(bytes: Uint8Array, page: ExtentListPage): ExtentRef[] {
  const fail = (why: string): never => {
    throw new StoreCorruptionError(`Extent list page at ${String(page.first)}: ${why}`);
  };
  if (bytes.length < HEADER) fail("too short");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(0, true) !== MAGIC) fail("not an extent list page");
  if (view.getUint16(4, true) !== VERSION)
    fail(`unknown version ${String(view.getUint16(4, true))}`);
  const count = view.getUint32(8, true);
  if (count !== page.count)
    fail(`holds ${String(count)} extents, the root says ${String(page.count)}`);
  if (count < 1 || count > LIST_PAGE_EXTENTS) fail(`holds ${String(count)} extents`);
  if (bytes.length !== HEADER + count * ENTRY) fail("has the wrong length");
  const extents: ExtentRef[] = [];
  let previousEnd = -1;
  for (let index = 0; index < count; index++) {
    const offset = view.getFloat64(HEADER + index * ENTRY, true);
    const length = view.getFloat64(HEADER + index * ENTRY + 8, true);
    if (!Number.isSafeInteger(offset) || !Number.isSafeInteger(length) || offset < 0 || length < 1)
      fail(`names an invalid extent (${String(offset)}, ${String(length)})`);
    if (offset < previousEnd) fail("is not in increasing offset order");
    if (index === 0 && offset !== page.first)
      fail(`starts at ${String(offset)}, the root says ${String(page.first)}`);
    extents.push({ offset, length });
    previousEnd = offset + length;
  }
  return extents;
}

// Governor accounting: 64-byte object overhead plus array slots/growth. Storage depends only
// on the structural ByteBudget, not the governor implementation.
const EXTENT_MEMORY_BYTES = 80;
const PAGE_MEMORY_BYTES = 256;
const BATCH_PAGES = 16;

function reserve(budget: ByteBudget, bytes: number): void {
  if (!budget.allocate(bytes)) throw new ReadDeniedError(bytes);
}

/** Bounded batches; the caller charges their small reference vectors before iteration. */
function* batches(pages: readonly ExtentListPage[]): Generator<ExtentListPage[]> {
  let batch: ExtentListPage[] = [];
  let bytes = 0;
  for (const page of pages) {
    if (
      batch.length > 0 &&
      (bytes + page.blob.length > MAX_READ_BYTES || batch.length === BATCH_PAGES)
    ) {
      yield batch;
      batch = [];
      bytes = 0;
    }
    batch.push(page);
    bytes += page.blob.length;
  }
  if (batch.length > 0) yield batch;
}

/** Decoded objects stay charged until the consumer releases them, including across awaits. */
async function readPages(
  pages: readonly ExtentListPage[],
  reader: ExtentReader,
  budget: ByteBudget,
  work?: TaskWork,
): Promise<{ pages: ExtentRef[][]; release(): void }> {
  const firstPause = pauseIfDue(work);
  if (firstPause !== undefined) await firstPause;
  const charge = 128 + pages.reduce((sum, page) => sum + 128 + page.count * EXTENT_MEMORY_BYTES, 0);
  reserve(budget, charge);
  let lease: import("./extents.js").ReadLease | null = null;
  try {
    lease = await reader.read(
      pages.map((page) => page.blob),
      { budget },
    );
    const resumed = pauseIfDue(work);
    if (resumed !== undefined) await resumed;
    const decoded: ExtentRef[][] = [];
    for (let index = 0; index < pages.length; index++) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
      const page = pages[index];
      const bytes = lease.bytes[index];
      if (page === undefined || bytes === undefined)
        throw new StoreCorruptionError("An extent list page is missing");
      decoded.push(decodeListPage(bytes, page));
    }
    const held = lease;
    let released = false;
    return {
      pages: decoded,
      release() {
        if (released) return;
        released = true;
        held.release();
        budget.free(charge);
      },
    };
  } catch (error) {
    lease?.release();
    budget.free(charge);
    throw error;
  }
}

/**
 * Everything a root reaches according to its list: each page's blob, then each listed extent (a
 * `ReachableExtents` source for `openExtentSpace`). Checks that the pages are in order and that no
 * page reaches into the next one's range.
 */
export async function* listedExtents(
  list: ExtentList,
  reader: ExtentReader,
  budget: ByteBudget,
  work?: TaskWork,
): AsyncGenerator<ExtentRef | BlobRef> {
  const firstPause = pauseIfDue(work);
  if (firstPause !== undefined) await firstPause;
  for (let index = 1; index < list.length; index++) {
    if (index % 128 === 0) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
    }
    const before = list[index - 1];
    const page = list[index];
    if (before !== undefined && page !== undefined && page.first <= before.first)
      throw new StoreCorruptionError("The extent list's pages are not in offset order");
  }
  reserve(budget, 512); // two bounded page-reference vectors across a generator suspension
  try {
    let pageIndex = 0;
    for (const batch of batches(list)) {
      const decoded = await readPages(batch, reader, budget, work);
      try {
        for (let index = 0; index < batch.length; index++) {
          const page = batch[index];
          const extents = decoded.pages[index];
          const next = list[pageIndex + 1];
          pageIndex += 1;
          if (page === undefined || extents === undefined) continue;
          const last = extents[extents.length - 1];
          if (next !== undefined && last !== undefined && last.offset >= next.first)
            throw new StoreCorruptionError(
              `Extent list page at ${String(page.first)} reaches past the next page's first offset`,
            );
          const pending = pauseIfDue(work);
          if (pending !== undefined) await pending;
          yield page.blob;
          for (const extent of extents) {
            const pending = pauseIfDue(work);
            if (pending !== undefined) await pending;
            yield extent;
            work?.signal?.throwIfAborted();
          }
        }
      } finally {
        decoded.release();
      }
    }
  } finally {
    budget.free(512);
  }
}

function checkOrder(list: ExtentList): void {
  for (let index = 1; index < list.length; index++) {
    const before = list[index - 1];
    const page = list[index];
    if (before !== undefined && page !== undefined && page.first <= before.first)
      throw new StoreCorruptionError("The extent list's pages are not in offset order");
  }
}

/** Every extent `list` names, in offset order (tests, the audit). */
export async function extentsOf(
  list: ExtentList,
  reader: ExtentReader,
  budget: ByteBudget,
): Promise<ExtentRef[]> {
  const out: ExtentRef[] = [];
  for await (const item of listedExtents(list, reader, budget))
    if (!("extent" in item)) out.push(item);
  return out;
}

/** One publication attempt's change to the set of extents its root reaches (`PublishDelta`). */
export interface ExtentListDelta {
  readonly added: readonly ExtentRef[];
  readonly released: readonly ExtentRef[];
  /** The publication's writer, still open: the new pages are written into it. */
  readonly writer: ExtentWriter;
}

/** What `prepare` returns: the new list for the root, and the page extents it no longer uses. */
export interface PreparedList {
  /** Always empty: the keeper writes its pages itself (kept for the sink protocol's shape). */
  readonly pages: readonly Uint8Array[];
  readonly retire: readonly ExtentRef[];
  readonly list: ExtentList;
}

export interface ExtentListKeeperOptions extends TaskWork {
  /** Reads the pages a delta touches (the extent space). */
  readonly reader: ExtentReader;
  /** Admits keeper scratch, read leases and retained prepared/committed list metadata. */
  readonly budget: ByteBudget;
  /** Extents per page at most (tests use small pages); default `LIST_PAGE_EXTENTS`. */
  readonly pageExtents?: number;
}

const byOffset = (a: ExtentRef, b: ExtentRef) => a.offset - b.offset;

/**
 * The storage track's `ExtentDeltaSink` (access/write-path.ts): keeps the committed list and turns
 * each publication's delta into the next one (see the module comment).
 */
export class ExtentListKeeper {
  readonly #reader: ExtentReader;
  readonly #work: TaskWork;
  readonly #budget: ByteBudget;
  readonly #pageExtents: number;
  #committed: ExtentList;
  #pending: {
    readonly attempt: number;
    readonly list: ExtentList;
    readonly charge: number;
    readonly retireCharge: number;
  } | null = null;
  #owned = 0;
  #preparing = false;
  #sequence = 0;

  /** `list`: the durable current root's list (empty for a new store). */
  constructor(list: ExtentList, options: ExtentListKeeperOptions) {
    checkOrder(list);
    this.#committed = list;
    this.#reader = options.reader;
    this.#work = options;
    this.#budget = options.budget;
    this.#pageExtents = options.pageExtents ?? LIST_PAGE_EXTENTS;
  }

  /** The committed list (the durable current root's). */
  get list(): ExtentList {
    return this.#committed;
  }

  async prepare(attempt: number, delta: ExtentListDelta): Promise<PreparedList> {
    if (this.#preparing || this.#pending !== null)
      throw new Error(
        `Extent list: attempt ${String(attempt)} prepared before another attempt settled`,
      );
    this.#preparing = true;
    try {
      const prepared = await this.#apply(delta);
      try {
        const resumed = pauseIfDue(this.#work);
        if (resumed !== undefined) await resumed;
      } catch (error) {
        this.#budget.free(prepared.charge + prepared.retireCharge);
        throw error;
      }
      this.#pending = {
        attempt,
        list: prepared.list,
        charge: prepared.charge,
        retireCharge: prepared.retireCharge,
      };
      return { pages: [], retire: prepared.retire, list: prepared.list };
    } finally {
      this.#preparing = false;
    }
  }

  commit(attempt: number, sequence: number): void {
    const pending = this.#take(attempt);
    if (pending.list !== this.#committed) {
      this.#budget.free(this.#owned);
      this.#owned = pending.charge;
    }
    this.#budget.free(pending.retireCharge);
    this.#committed = pending.list;
    this.#sequence = sequence;
  }

  /** The root sequence of the last committed publication (0 before any). */
  get sequence(): number {
    return this.#sequence;
  }

  abort(attempt: number): void {
    const pending = this.#take(attempt);
    this.#budget.free(pending.charge + pending.retireCharge);
  }

  /** Releases keeper-owned metadata; the constructor's durable list is borrowed from its root. */
  close(): void {
    if (this.#preparing) throw new Error("Extent list: close while preparing");
    if (this.#pending !== null) this.abort(this.#pending.attempt);
    this.#budget.free(this.#owned);
    this.#owned = 0;
    this.#committed = [];
  }

  #take(attempt: number): {
    readonly attempt: number;
    readonly list: ExtentList;
    readonly charge: number;
    readonly retireCharge: number;
  } {
    const pending = this.#pending;
    if (pending?.attempt !== attempt)
      throw new Error(`Extent list: attempt ${String(attempt)} was not the one prepared`);
    this.#pending = null;
    return pending;
  }

  /** One decoded source page and one output page at a time; untouched pages retain identity. */
  async #apply(delta: ExtentListDelta): Promise<{
    list: ExtentList;
    retire: readonly ExtentRef[];
    charge: number;
    retireCharge: number;
  }> {
    const firstPause = pauseIfDue(this.#work);
    if (firstPause !== undefined) await firstPause;
    const pages = this.#committed;
    if (delta.added.length === 0 && delta.released.length === 0)
      return { list: pages, retire: [], charge: 0, retireCharge: 0 };
    // Input copies + sort workspace; two pages' objects/slots and one encoded output; retained
    // result pages plus retirement set/maps. Reserve every component before allocating it.
    const resultCapacity = 2 * pages.length + Math.ceil(delta.added.length / this.#pageExtents) + 1;
    const charge =
      32 * (delta.added.length + delta.released.length) +
      256 * this.#pageExtents +
      1024 +
      512 * resultCapacity +
      128 * pages.length;
    reserve(this.#budget, charge);
    let retained = 0;
    try {
      const added = [...delta.added];
      const released = [...delta.released];
      await sortInSlices(added, byOffset, this.#work);
      await sortInSlices(released, byOffset, this.#work);
      const out: ExtentListPage[] = [];
      let buffer: ExtentRef[] = [];
      let previousEnd = -1;
      let ai = 0;
      let ri = 0;
      const flush = async () => {
        const first = buffer[0];
        if (first === undefined) return;
        const bytes = encodeListPage(buffer);
        const written = await delta.writer.write([bytes]);
        const resumed = pauseIfDue(this.#work);
        if (resumed !== undefined) await resumed;
        const blob = written.blobs[0];
        if (blob === undefined) throw new Error("Extent list: a page was not written");
        out.push({ blob, first: first.offset, count: buffer.length });
        buffer = [];
      };
      const append = (extent: ExtentRef): Promise<void> | undefined => {
        if (previousEnd > extent.offset)
          throw new StoreCorruptionError(
            `Extent list: duplicate or overlapping addition at ${String(extent.offset)}`,
          );
        previousEnd = extent.offset + extent.length;
        buffer.push(extent);
        if (buffer.length === this.#pageExtents) return flush();
        return undefined;
      };
      for (let index = 0; index < Math.max(1, pages.length); index++) {
        const pending = pauseIfDue(this.#work);
        if (pending !== undefined) await pending;
        const page = pages[index];
        const next = pages[index + 1];
        const end = next?.first ?? Infinity;
        const addStart = ai;
        const releaseStart = ri;
        while ((added[ai]?.offset ?? Infinity) < end) {
          if (ai % 128 === 0) {
            const pending = pauseIfDue(this.#work);
            if (pending !== undefined) await pending;
          }
          ai++;
        }
        while ((released[ri]?.offset ?? Infinity) < end) {
          if (ri % 128 === 0) {
            const pending = pauseIfDue(this.#work);
            if (pending !== undefined) await pending;
          }
          ri++;
        }
        const mergeNeighbour =
          page !== undefined &&
          buffer.length > 0 &&
          buffer.length < this.#pageExtents / 2 &&
          buffer.length + page.count <= this.#pageExtents;
        if (page !== undefined && ai === addStart && ri === releaseStart && !mergeNeighbour) {
          await flush();
          out.push(page);
          continue;
        }
        const held =
          page === undefined
            ? null
            : await readPages([page], this.#reader, this.#budget, this.#work);
        try {
          let a = addStart;
          let r = releaseStart;
          let processed = 0;
          for (const extent of held?.pages[0] ?? []) {
            if (processed++ % 128 === 0) {
              const pending = pauseIfDue(this.#work);
              if (pending !== undefined) await pending;
            }
            const removal = released[r];
            if (r < ri && removal !== undefined && removal.offset < extent.offset)
              throw new StoreCorruptionError(
                `Extent list: the publication releases extents the list does not name (first at ${String(removal.offset)})`,
              );
            let keep = true;
            if (r < ri && removal?.offset === extent.offset) {
              if (removal.length !== extent.length)
                throw new StoreCorruptionError(
                  `Extent list: the publication releases ${String(extent.offset)} as ${String(removal.length)} bytes, the list has ${String(extent.length)}`,
                );
              keep = false;
              r++;
            }
            while (a < ai && (added[a]?.offset ?? Infinity) < extent.offset) {
              if (a % 128 === 0) {
                const pending = pauseIfDue(this.#work);
                if (pending !== undefined) await pending;
              }
              const addition = added[a++];
              if (addition !== undefined) {
                const pending = append(addition);
                if (pending !== undefined) await pending;
              }
            }
            if (keep) {
              if (a < ai && added[a]?.offset === extent.offset)
                throw new StoreCorruptionError(
                  `Extent list: the publication adds the extent at ${String(extent.offset)}, which the list already names`,
                );
              const pending = append(extent);
              if (pending !== undefined) await pending;
            }
          }
          if (r !== ri)
            throw new StoreCorruptionError(
              "Extent list: the publication releases extents the list does not name",
            );
          while (a < ai) {
            if (a % 128 === 0) {
              const pending = pauseIfDue(this.#work);
              if (pending !== undefined) await pending;
            }
            const addition = added[a++];
            if (addition !== undefined) {
              const pending = append(addition);
              if (pending !== undefined) await pending;
            }
          }
        } finally {
          held?.release();
        }
        const nextTouched =
          next !== undefined &&
          ((added[ai]?.offset ?? Infinity) < (pages[index + 2]?.first ?? Infinity) ||
            (released[ri]?.offset ?? Infinity) < (pages[index + 2]?.first ?? Infinity));
        if (
          next === undefined ||
          nextTouched ||
          buffer.length >= this.#pageExtents / 2 ||
          buffer.length + next.count > this.#pageExtents
        )
          await flush();
      }
      await flush();
      const retire = await retiredInSlices(pages, out, this.#work);
      const pending = pauseIfDue(this.#work);
      if (pending !== undefined) await pending;
      const listCharge = 128 + out.length * PAGE_MEMORY_BYTES;
      const retireCharge = 64 + retire.length * 16;
      retained = listCharge + retireCharge;
      return { list: out, retire, charge: listCharge, retireCharge };
    } finally {
      this.#budget.free(charge - retained);
    }
  }
}

/** Extents holding pages of `before` that no page of `after` lives in. */
export function retiredPageExtents(before: ExtentList, after: ExtentList): ExtentRef[] {
  const kept = new Set(after.map((page) => page.blob.extent.offset));
  const out = new Map<number, ExtentRef>();
  for (const page of before)
    if (!kept.has(page.blob.extent.offset)) out.set(page.blob.extent.offset, page.blob.extent);
  return [...out.values()];
}

async function retiredInSlices(
  before: ExtentList,
  after: ExtentList,
  work: TaskWork,
): Promise<ExtentRef[]> {
  const kept = new Set<number>();
  const retired = new Map<number, ExtentRef>();
  for (let index = 0; index < after.length; index++) {
    if (index % 128 === 0) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
    }
    const page = after[index];
    if (page !== undefined) kept.add(page.blob.extent.offset);
  }
  for (let index = 0; index < before.length; index++) {
    if (index % 128 === 0) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
    }
    const page = before[index];
    if (page !== undefined && !kept.has(page.blob.extent.offset))
      retired.set(page.blob.extent.offset, page.blob.extent);
  }
  const out: ExtentRef[] = [];
  for (const extent of retired.values()) {
    if (out.length % 128 === 0) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
    }
    out.push(extent);
  }
  return out;
}
