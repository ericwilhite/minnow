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
import type {
  RetirementCursor,
  RetirementInventory,
  RetirementInput,
  RetirementOptions,
} from "./retirement-contract.js";
import { DURABLE_FORMATS, requireHeader } from "../durable-format.js";
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

const MAGIC = DURABLE_FORMATS.extentList.magic; // "MXL1"
const VERSION = DURABLE_FORMATS.extentList.version;
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
  requireHeader("extentList", bytes, StoreCorruptionError);
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
function* batches(
  pages: readonly ExtentListPage[],
  pageLimit = BATCH_PAGES,
): Generator<ExtentListPage[]> {
  let batch: ExtentListPage[] = [];
  let bytes = 0;
  for (const page of pages) {
    if (
      batch.length > 0 &&
      (bytes + page.blob.length > MAX_READ_BYTES || batch.length === pageLimit)
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
  includePages = true,
  pageLimit = BATCH_PAGES,
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
    for (const batch of batches(list, pageLimit)) {
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
          if (includePages) yield page.blob;
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
  /** Additional immutable releases; caller owns it through every observer and publication. */
  readonly releasedInventory?: RetirementInventory;
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
  /** Reads the pages a delta touches. Unbounded release arrays require the native
   * ExtentSpace.prepareRetirements service; alternate readers support the bounded array path. */
  readonly reader: ExtentReader;
  /** Admits keeper scratch, read leases and retained prepared/committed list metadata. */
  readonly budget: ByteBudget;
  /** Extents per page at most (tests use small pages); default `LIST_PAGE_EXTENTS`. */
  readonly pageExtents?: number;
}

/** Small deltas avoid scratch I/O. Larger arrays use the same disk inventory as streamed input. */
const RESIDENT_RELEASE_LIMIT = 4096;

class ResidentReleases implements RetirementCursor {
  offset = 0;
  length = 0;
  #at = 0;
  constructor(private values: readonly ExtentRef[]) {}
  next(): boolean {
    const value = this.values[this.#at++];
    if (value === undefined) return false;
    this.offset = value.offset;
    this.length = value.length;
    return true;
  }
  async close(): Promise<void> {
    this.values = [];
  }
}

/** No page/frame borrow survives the yielded descriptor. captureAudit releases its workspace
 * while awaiting this source, including when the source cursor belongs to this same space. */
async function* combinedReleases(
  delta: ExtentListDelta,
  budget: ByteBudget,
  work: TaskWork,
): AsyncGenerator<ExtentRef> {
  const cursor = delta.releasedInventory?.cursor({
    budget,
    ...(work.signal === undefined ? {} : { signal: work.signal }),
  });
  try {
    if (cursor !== undefined)
      while (await cursor.next(work)) yield { offset: cursor.offset, length: cursor.length };
    for (const extent of delta.released) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
      yield extent;
    }
  } finally {
    await cursor?.close();
  }
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
    if (
      delta.added.length === 0 &&
      delta.released.length === 0 &&
      (delta.releasedInventory?.count ?? 0) === 0
    )
      return { list: pages, retire: [], charge: 0, retireCharge: 0 };
    // Large sources and refused resident reservations stream from the native inventory.
    let ownedInventory: RetirementInventory | undefined;
    let cursor: RetirementCursor | undefined;
    let charge = 0;
    let retained = 0;
    try {
      let inventory = delta.releasedInventory;
      const resultCapacity =
        2 * pages.length + Math.ceil(delta.added.length / this.#pageExtents) + 1;
      const common =
        32 * delta.added.length +
        256 * this.#pageExtents +
        1024 +
        512 * resultCapacity +
        128 * pages.length;
      const residentRequired = common + 32 * delta.released.length;
      let spool =
        delta.released.length > RESIDENT_RELEASE_LIMIT ||
        (inventory !== undefined && delta.released.length > 0);
      let refusal: ReadDeniedError | undefined;
      // The small array is only an optional optimization. Refuse neither the legal
      // delta nor its cheaper disk route just because this larger reservation fails.
      if (!spool && inventory === undefined && delta.released.length > 0) {
        let admitted: boolean;
        try {
          admitted = this.#budget.allocate(residentRequired);
        } catch (error) {
          if (!(error instanceof ReadDeniedError)) throw error;
          refusal = error;
          admitted = false;
        }
        if (admitted) charge = residentRequired;
        else spool = true;
      }
      if (spool) {
        const service = this.#reader as ExtentReader & {
          prepareRetirements?: (
            input: RetirementInput,
            options: RetirementOptions,
          ) => Promise<RetirementInventory>;
        };
        if (service.prepareRetirements === undefined) {
          if (delta.released.length <= RESIDENT_RELEASE_LIMIT && inventory === undefined)
            throw refusal ?? new ReadDeniedError(residentRequired);
          throw new TypeError(
            "Large extent deltas require the native retirement inventory service",
          );
        }
        ownedInventory = await service.prepareRetirements(
          combinedReleases(delta, this.#budget, this.#work),
          { ...this.#work, budget: this.#budget, duplicates: "refuse" },
        );
        inventory = ownedInventory;
      }
      if (charge === 0) {
        const required = common + (inventory === undefined ? 32 * delta.released.length : 0);
        reserve(this.#budget, required);
        charge = required;
      }
      const copy = async (input: readonly ExtentRef[]) => {
        const result: ExtentRef[] = [];
        for (let start = 0; start < input.length; start += 128) {
          const pending = pauseIfDue(this.#work);
          if (pending !== undefined) await pending;
          const end = Math.min(start + 128, input.length);
          for (let at = start; at < end; at++) {
            const extent = input[at];
            if (extent === undefined) throw new RangeError("Extent delta requires dense inputs");
            result.push(extent);
          }
        }
        const pending = pauseIfDue(this.#work);
        if (pending !== undefined) await pending;
        return result;
      };
      const added = await copy(delta.added);
      await sortInSlices(added, byOffset, this.#work);
      if (inventory === undefined) {
        const released = await copy(delta.released);
        await sortInSlices(released, byOffset, this.#work);
        cursor = new ResidentReleases(released);
      } else
        cursor = inventory.cursor({
          budget: this.#budget,
          ...(this.#work.signal === undefined ? {} : { signal: this.#work.signal }),
        });
      let hasRelease = await cursor.next(this.#work);
      const out: ExtentListPage[] = [];
      let buffer: ExtentRef[] = [];
      let previousEnd = -1;
      let ai = 0;

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
        buffer.push({ offset: extent.offset, length: extent.length });
        if (buffer.length === this.#pageExtents) return flush();
        return undefined;
      };
      for (let index = 0; index < Math.max(1, pages.length); index++) {
        const pending = pauseIfDue(this.#work);
        if (pending !== undefined) await pending;
        const page = pages[index];
        const next = pages[index + 1];
        const end = next?.first ?? Infinity;
        const mergeNeighbour =
          page !== undefined &&
          buffer.length > 0 &&
          buffer.length < this.#pageExtents / 2 &&
          buffer.length + page.count <= this.#pageExtents;
        if (
          page !== undefined &&
          (added[ai]?.offset ?? Infinity) >= end &&
          (!hasRelease || cursor.offset >= end) &&
          !mergeNeighbour
        ) {
          await flush();
          out.push(page);
          continue;
        }
        const held =
          page === undefined
            ? null
            : await readPages([page], this.#reader, this.#budget, this.#work);
        try {
          let processed = 0;
          for (const extent of held?.pages[0] ?? []) {
            if (processed++ % 128 === 0) {
              const pending = pauseIfDue(this.#work);
              if (pending !== undefined) await pending;
            }
            if (hasRelease && cursor.offset < extent.offset)
              throw new StoreCorruptionError(
                `Extent list: the publication releases extents the list does not name (first at ${String(cursor.offset)})`,
              );
            let keep = true;
            if (hasRelease && cursor.offset === extent.offset) {
              if (cursor.length !== extent.length)
                throw new StoreCorruptionError(
                  `Extent list: the publication releases ${String(extent.offset)} as ${String(cursor.length)} bytes, the list has ${String(extent.length)}`,
                );
              keep = false;
              hasRelease = await cursor.next(this.#work);
            }
            while ((added[ai]?.offset ?? Infinity) < extent.offset) {
              if (ai % 128 === 0) {
                const pending = pauseIfDue(this.#work);
                if (pending !== undefined) await pending;
              }
              const addition = added[ai++];
              if (addition !== undefined) {
                const pending = append(addition);
                if (pending !== undefined) await pending;
              }
            }
            if (keep) {
              if (added[ai]?.offset === extent.offset)
                throw new StoreCorruptionError(
                  `Extent list: the publication adds the extent at ${String(extent.offset)}, which the list already names`,
                );
              const pending = append(extent);
              if (pending !== undefined) await pending;
            }
          }
          if (hasRelease && cursor.offset < end)
            throw new StoreCorruptionError(
              "Extent list: the publication releases extents the list does not name",
            );
          while ((added[ai]?.offset ?? Infinity) < end) {
            if (ai % 128 === 0) {
              const pending = pauseIfDue(this.#work);
              if (pending !== undefined) await pending;
            }
            const addition = added[ai++];
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
            (hasRelease ? cursor.offset : Infinity) < (pages[index + 2]?.first ?? Infinity));
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
      await cursor.close();
      cursor = undefined;
      await ownedInventory?.release();
      ownedInventory = undefined;
      retained = listCharge + retireCharge;
      return { list: out, retire, charge: listCharge, retireCharge };
    } finally {
      try {
        await cursor?.close();
      } finally {
        try {
          await ownedInventory?.release();
        } finally {
          this.#budget.free(charge - retained);
        }
      }
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

/** Full ordered reachability, including the list pages' scattered allocations. */
export async function* sortedListedExtents(
  list: ExtentList,
  reader: ExtentReader,
  budget: ByteBudget,
  work?: TaskWork,
): AsyncGenerator<ExtentRef> {
  const initial = pauseIfDue(work);
  if (initial !== undefined) await initial;
  const charge = 32 * list.length + 2048;
  reserve(budget, charge);
  try {
    let refs: ExtentRef[] = [];
    let scratch: ExtentRef[] = [];
    const check = async () => {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
    };
    for (let i = 0; i < list.length; i++) {
      if (i % 128 === 0) await check();
      const page = list[i];
      if (page !== undefined) {
        const ref = page.blob.extent;
        if (
          !Number.isSafeInteger(ref.offset) ||
          ref.offset < 0 ||
          !Number.isSafeInteger(ref.length) ||
          ref.length <= 0
        )
          throw new StoreCorruptionError("Invalid extent-list page allocation");
        refs.push(ref);
      }
    }
    // Seven byte passes order safe-integer offsets in O(page references), not O(E log E).
    const counts = new Uint32Array(256);
    for (let pass = 0; pass < 7; pass++) {
      counts.fill(0);
      const divisor = 2 ** (pass * 8);
      for (let i = 0; i < refs.length; i++) {
        if (i % 128 === 0) await check();
        const ref = refs[i];
        if (ref !== undefined) {
          const key = Math.floor(ref.offset / divisor) % 256;
          counts[key] = (counts[key] ?? 0) + 1;
        }
      }
      let sum = 0;
      for (let key = 0; key < 256; key++) {
        const n = counts[key] ?? 0;
        counts[key] = sum;
        sum += n;
      }
      for (let i = 0; i < refs.length; i++) {
        if (i % 128 === 0) await check();
        const ref = refs[i];
        if (ref !== undefined) {
          const key = Math.floor(ref.offset / divisor) % 256;
          const at = counts[key] ?? 0;
          scratch[at] = ref;
          counts[key] = at + 1;
        }
      }
      [refs, scratch] = [scratch, refs];
    }
    let at = 0;
    for await (const item of listedExtents(list, reader, budget, work, false, 1)) {
      const extent = "extent" in item ? item.extent : item;
      while (at < refs.length) {
        const page = refs[at];
        if (page === undefined || page.offset > extent.offset) break;
        await check();
        yield page;
        at++;
      }
      yield extent;
    }
    while (at < refs.length) {
      await check();
      const page = refs[at++];
      if (page !== undefined) yield page;
    }
  } finally {
    budget.free(charge);
  }
}
