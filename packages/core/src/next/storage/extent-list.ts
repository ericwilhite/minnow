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
import { StoreCorruptionError } from "./contract.js";
import {
  MAX_READ_BYTES,
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

/** Batches of pages whose bytes fit one read. */
function batches(pages: readonly ExtentListPage[]): ExtentListPage[][] {
  const out: ExtentListPage[][] = [];
  let batch: ExtentListPage[] = [];
  let bytes = 0;
  for (const page of pages) {
    if (batch.length > 0 && bytes + page.blob.length > MAX_READ_BYTES) {
      out.push(batch);
      batch = [];
      bytes = 0;
    }
    batch.push(page);
    bytes += page.blob.length;
  }
  if (batch.length > 0) out.push(batch);
  return out;
}

/** Reads and decodes `pages` (any subset of a list), in order. */
async function readPages(
  pages: readonly ExtentListPage[],
  reader: ExtentReader,
  budget: ByteBudget,
): Promise<ExtentRef[][]> {
  const out: ExtentRef[][] = [];
  for (const batch of batches(pages)) {
    const lease = await reader.read(
      batch.map((page) => page.blob),
      { budget },
    );
    try {
      batch.forEach((page, index) => {
        const bytes = lease.bytes[index];
        if (bytes === undefined) throw new StoreCorruptionError("An extent list page is missing");
        out.push(decodeListPage(bytes, page));
      });
    } finally {
      lease.release();
    }
  }
  return out;
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
): AsyncGenerator<ExtentRef | BlobRef> {
  checkOrder(list);
  for (const batch of batches(list)) {
    const decoded = await readPages(batch, reader, budget);
    for (let index = 0; index < batch.length; index++) {
      const page = batch[index];
      const extents = decoded[index];
      if (page === undefined || extents === undefined) continue;
      const next = list[list.indexOf(page) + 1];
      const last = extents[extents.length - 1];
      if (next !== undefined && last !== undefined && last.offset >= next.first)
        throw new StoreCorruptionError(
          `Extent list page at ${String(page.first)} reaches past the next page's first offset`,
        );
      yield page.blob;
      yield* extents;
    }
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

export interface ExtentListKeeperOptions {
  /** Reads the pages a delta touches (the extent space). */
  readonly reader: ExtentReader;
  /** Charged while those pages are read. */
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
  readonly #budget: ByteBudget;
  readonly #pageExtents: number;
  #committed: ExtentList;
  #pending: { readonly attempt: number; readonly list: ExtentList } | null = null;
  #sequence = 0;

  /** `list`: the durable current root's list (empty for a new store). */
  constructor(list: ExtentList, options: ExtentListKeeperOptions) {
    checkOrder(list);
    this.#committed = list;
    this.#reader = options.reader;
    this.#budget = options.budget;
    this.#pageExtents = options.pageExtents ?? LIST_PAGE_EXTENTS;
  }

  /** The committed list (the durable current root's). */
  get list(): ExtentList {
    return this.#committed;
  }

  async prepare(attempt: number, delta: ExtentListDelta): Promise<PreparedList> {
    if (this.#pending !== null)
      throw new Error(
        `Extent list: attempt ${String(attempt)} prepared before ${String(this.#pending.attempt)} settled`,
      );
    const list = await this.#apply(delta);
    this.#pending = { attempt, list };
    return { pages: [], retire: retiredPageExtents(this.#committed, list), list };
  }

  commit(attempt: number, sequence: number): void {
    const pending = this.#take(attempt);
    this.#committed = pending.list;
    this.#sequence = sequence;
  }

  /** The root sequence of the last committed publication (0 before any). */
  get sequence(): number {
    return this.#sequence;
  }

  abort(attempt: number): void {
    this.#take(attempt);
  }

  #take(attempt: number): { readonly attempt: number; readonly list: ExtentList } {
    const pending = this.#pending;
    if (pending?.attempt !== attempt)
      throw new Error(`Extent list: attempt ${String(attempt)} was not the one prepared`);
    this.#pending = null;
    return pending;
  }

  /** The committed list with `delta` applied: touched pages rewritten, the rest shared. */
  async #apply(delta: ExtentListDelta): Promise<ExtentList> {
    const pages = this.#committed;
    const added = [...delta.added].sort(byOffset);
    const released = [...delta.released].sort(byOffset);
    if (added.length === 0 && released.length === 0) return pages;
    // The page each change falls in: the last page whose first offset is at or below it (an
    // addition below every page goes to the first one).
    const pageOf = (offset: number): number => {
      let low = 0;
      let high = pages.length - 1;
      let found = 0;
      while (low <= high) {
        const middle = (low + high) >>> 1;
        if ((pages[middle]?.first ?? Infinity) <= offset) {
          found = middle;
          low = middle + 1;
        } else high = middle - 1;
      }
      return found;
    };
    const touched = new Map<number, { added: ExtentRef[]; released: ExtentRef[] }>();
    const touch = (index: number) => {
      let entry = touched.get(index);
      if (entry === undefined) {
        entry = { added: [], released: [] };
        touched.set(index, entry);
      }
      return entry;
    };
    if (pages.length === 0) {
      if (released.length > 0)
        throw new StoreCorruptionError(
          `Extent list: the publication releases ${String(released.length)} extents an empty list does not name`,
        );
      return this.#writePages(delta.writer, this.#split(added));
    }
    for (const extent of added) touch(pageOf(extent.offset)).added.push(extent);
    for (const extent of released) touch(pageOf(extent.offset)).released.push(extent);
    // A rewritten page that ends up small takes its next neighbour along (merged when they fit).
    const order = [...touched.keys()].sort((a, b) => a - b);
    const loaded = await readPages(
      order.map((index) => pageAt(pages, index)),
      this.#reader,
      this.#budget,
    );
    const runs: Array<{ from: number; to: number; extents: ExtentRef[] }> = [];
    order.forEach((index, at) => {
      const change = touched.get(index);
      const current = loaded[at];
      if (change === undefined || current === undefined) return;
      const releasing = new Map(change.released.map((extent) => [extent.offset, extent.length]));
      const kept: ExtentRef[] = [];
      for (const extent of current) {
        const length = releasing.get(extent.offset);
        if (length === undefined) kept.push(extent);
        else if (length !== extent.length)
          throw new StoreCorruptionError(
            `Extent list: the publication releases ${String(extent.offset)} as ${String(length)} bytes, the list has ${String(extent.length)}`,
          );
        else releasing.delete(extent.offset);
      }
      if (releasing.size > 0)
        throw new StoreCorruptionError(
          `Extent list: the publication releases ${String(releasing.size)} extents the list does not name (first at ${String([...releasing.keys()][0])})`,
        );
      const merged = mergeSorted(kept, change.added);
      runs.push({ from: index, to: index, extents: merged });
    });
    // Merge a small rewritten page with an untouched next neighbour when they fit one page.
    const neighbours: Array<{ run: (typeof runs)[number]; next: number }> = [];
    for (const run of runs) {
      const next = run.to + 1;
      const page = pages[next];
      if (
        page !== undefined &&
        !touched.has(next) &&
        run.extents.length < this.#pageExtents / 2 &&
        run.extents.length + page.count <= this.#pageExtents
      )
        neighbours.push({ run, next });
    }
    if (neighbours.length > 0) {
      const extra = await readPages(
        neighbours.map(({ next }) => pageAt(pages, next)),
        this.#reader,
        this.#budget,
      );
      neighbours.forEach(({ run, next }, at) => {
        run.extents = [...run.extents, ...(extra[at] ?? [])];
        run.to = next;
      });
    }
    const chunks: ExtentRef[][] = [];
    for (const run of runs) chunks.push(...this.#split(run.extents));
    const written = chunks.length === 0 ? [] : await this.#writePages(delta.writer, chunks);
    // The new list: untouched pages by reference, each run replaced by its new pages.
    const out: ExtentListPage[] = [];
    let cursor = 0;
    let at = 0;
    for (const run of runs) {
      for (; cursor < run.from; cursor++) {
        const page = pages[cursor];
        if (page !== undefined) out.push(page);
      }
      const count = this.#split(run.extents).length;
      out.push(...written.slice(at, at + count));
      at += count;
      cursor = run.to + 1;
    }
    for (; cursor < pages.length; cursor++) {
      const page = pages[cursor];
      if (page !== undefined) out.push(page);
    }
    return out;
  }

  /** Splits a sorted run into as few pages as fit, of near-equal sizes. */
  #split(extents: readonly ExtentRef[]): ExtentRef[][] {
    if (extents.length === 0) return [];
    const parts = Math.ceil(extents.length / this.#pageExtents);
    const size = Math.ceil(extents.length / parts);
    const out: ExtentRef[][] = [];
    for (let at = 0; at < extents.length; at += size) out.push(extents.slice(at, at + size));
    return out;
  }

  async #writePages(
    writer: ExtentWriter,
    chunks: readonly ExtentRef[][],
  ): Promise<ExtentListPage[]> {
    const nonEmpty = chunks.filter((chunk) => chunk.length > 0);
    if (nonEmpty.length === 0) return [];
    const written = await writer.write(nonEmpty.map((chunk) => encodeListPage(chunk)));
    return nonEmpty.map((chunk, index) => {
      const blob = written.blobs[index];
      const first = chunk[0];
      if (blob === undefined || first === undefined)
        throw new Error("Extent list: a page was not written");
      return { blob, first: first.offset, count: chunk.length };
    });
  }
}

function pageAt(pages: ExtentList, index: number): ExtentListPage {
  const page = pages[index];
  if (page === undefined) throw new RangeError(`Extent list: no page ${String(index)}`);
  return page;
}

function mergeSorted(a: readonly ExtentRef[], b: readonly ExtentRef[]): ExtentRef[] {
  const out: ExtentRef[] = [];
  let i = 0;
  let j = 0;
  while (i < a.length || j < b.length) {
    const x = a[i];
    const y = b[j];
    if (y === undefined || (x !== undefined && x.offset < y.offset)) {
      if (x !== undefined) out.push(x);
      i += 1;
    } else if (x === undefined || y.offset < x.offset) {
      out.push(y);
      j += 1;
    } else
      throw new StoreCorruptionError(
        `Extent list: the publication adds the extent at ${String(y.offset)}, which the list already names`,
      );
  }
  return out;
}

/** Extents holding pages of `before` that no page of `after` lives in. */
export function retiredPageExtents(before: ExtentList, after: ExtentList): ExtentRef[] {
  const kept = new Set(after.map((page) => page.blob.extent.offset));
  const out = new Map<number, ExtentRef>();
  for (const page of before)
    if (!kept.has(page.blob.extent.offset)) out.set(page.blob.extent.offset, page.blob.extent);
  return [...out.values()];
}
