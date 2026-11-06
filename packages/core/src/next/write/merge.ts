/**
 * Index run merges (PROPOSAL.md "Maintenance"; access/write-path.ts "Backpressure").
 *
 * ## Levels
 * Each index's durable runs are a list, newest first (`TableView.runs`). A flush adds one run at
 * level 0 at the front. Levels never decrease from newest to oldest, so the runs of one level are
 * contiguous. A merge takes **every run of one level** and writes one sorted run at the next level,
 * placed where its inputs were: recency order is kept, so a tombstone stays in the same layer as its
 * put or a newer one (access/key-index.ts "Structure"), and resolution is unchanged.
 * - **Sorted runs.** Each run counts except the continuation pieces of a merge output: an output
 *   that outgrows one run's block index is split into pieces in ascending key order (possibly
 *   inside one key's entries), and every piece after the first carries `RunRef.continues`.
 *   Together they are one sorted run.
 * - **Debt.** A level holding `fanIn` sorted runs (8) is debt. Merges run only while some level is
 *   in debt, lowest level first, and stop when none is. So each entry is rewritten once per level it
 *   climbs: write amplification is 1 + ⌈log₈(entries / memtable entries)⌉ (2 at 1M keyed rows, 3 at
 *   5M), and a point read meets at most 7 overlapping runs per level.
 * - **Bottom.** A merge whose inputs reach the oldest run drops the tombstones left (no older put
 *   can exist); a put and its tombstone that meet cancel at any level (runs.ts "Writing").
 *
 * ## A merge
 * - Streams: each input is read a batch of blocks at a time through the extent space (a lease
 *   charged to the merge's grant), entries go through a heap of the inputs' current entries into a
 *   `RunEncoder`, and the encoder's parts are appended to an `ExtentBuilder` 256 KiB at a time. The
 *   output's block index and fences (≤ 1 MiB each) are the only parts held whole; past
 *   `splitBytes` the output is split before the next entry (never between a put and its own
 *   tombstone), so fences stay bounded even when one key has many row ids.
 * - A merge that fails before its publish (a run it cannot encode, a read the budget refuses)
 *   never fails the write path (engine.ts): its writer is aborted, the inputs stay, the failure is
 *   counted, and it is retried once with a quarter of the split bound before the inputs are left
 *   alone until they change.
 * - Charges its working memory to a grant of its own, admitted before it starts.
 * - Yields to the event loop (a task, not a microtask) after `YIELD_INTERVAL_MS` of synchronous work
 *   (ruling R23), and stops at a slice boundary when the write path closes. Its scratch is owned by
 *   the merge (never module-level), so two merges could interleave.
 * - Publishes by root swap under the write path's publish lock (engine.ts), retiring its inputs'
 *   extents; the extent space's two-generation reclaim and the readers' pins keep older views'
 *   runs readable. A crash before the publish leaves orphans the next open frees.
 */
import type { ExtentRef, RunRef } from "../format/manifest.js";
import type { Grant } from "../memory/governor.js";
import type { SpaceReader } from "../access/space.js";
import type { ExtentBuilder, ExtentWriter, ReadLease } from "../storage/extents.js";
import { compareBytes } from "../vector/varlen.js";
import { need } from "./bytes.js";
import { RunEncoder, type RunReader } from "./runs.js";

/** Sorted runs per level before the level is merged. */
export const FAN_IN = 8;
/** Synchronous work between task yields (ruling R23; exec/pipeline.ts `YIELD_INTERVAL_MS`). */
export const YIELD_INTERVAL_MS = 4;
/** An output run is split at the next key boundary once its block index or fences pass this. */
export const SPLIT_BYTES = 896 * 1024;
/** Encoded parts are appended to the output extent this many bytes at a time. */
const DRAIN_BYTES = 256 * 1024;
/** Blocks read per input per lease. */
const BATCH_BLOCKS = 16;
/** Working memory charged per merge before it starts: staging, the encoder's buffers, slack. */
export const MERGE_FIXED_BYTES = 4 * 1024 * 1024;
/** And per input: one batch of blocks (≤ 16 × ~4 KiB). */
export const MERGE_INPUT_BYTES = BATCH_BLOCKS * 4200;

export interface MergePolicy {
  readonly fanIn: number;
  readonly splitBytes: number;
  /** Tests: also split an output run past this many entries (small data reaches splits). */
  readonly splitEntries: number | null;
  /** Tests: yield after this many entries instead of by time (deterministic crash sweeps). */
  readonly sliceEntries: number | null;
}

export const DEFAULT_MERGE_POLICY: MergePolicy = {
  fanIn: FAN_IN,
  splitBytes: SPLIT_BYTES,
  splitEntries: null,
  sliceEntries: null,
};

/** Resolves in a later task, so pending messages and timers run first (exec/pipeline.ts). */
export function yieldTask(): Promise<void> {
  return new Promise<void>((resolve) => {
    if (typeof setImmediate === "function") setImmediate(resolve);
    else {
      const channel = new MessageChannel();
      channel.port1.onmessage = () => {
        channel.port1.close();
        resolve();
      };
      channel.port2.postMessage(null);
    }
  });
}

/**
 * Sorted runs among `runs[from, to)`, all of one level (see "Levels"): every run but the
 * continuation pieces of a merge output (`RunRef.continues`, review B3-p2b-2 B4). Equal endpoints
 * prove nothing: independent merges of one popular key all have minKey = maxKey.
 */
export function sortedRuns(runs: readonly RunRef[], from: number, to: number): number {
  let count = 0;
  for (let k = from; k < to; k++) if (need(runs[k]).continues !== true) count += 1;
  return count;
}

export interface MergePlan {
  /** Inputs: `runs[from, to)`, every run of `level`. */
  readonly from: number;
  readonly to: number;
  readonly level: number;
  /** The inputs reach the oldest run: tombstones are dropped. */
  readonly bottom: boolean;
}

/** The lowest level in debt, or null (runs newest first; levels never decrease). */
export function planMerge(runs: readonly RunRef[], fanIn: number): MergePlan | null {
  let from = 0;
  while (from < runs.length) {
    const level = need(runs[from]).level;
    let to = from + 1;
    while (to < runs.length && need(runs[to]).level === level) to += 1;
    if (sortedRuns(runs, from, to) >= fanIn) return { from, to, level, bottom: to === runs.length };
    from = to;
  }
  return null;
}

/** Level-0 runs of a list (backpressure and stats). */
export function levelZero(runs: readonly RunRef[]): number {
  let count = 0;
  while (count < runs.length && need(runs[count]).level === 0) count += 1;
  return count;
}

/** A run's extent: its entries, fences and Bloom directory share it (runs.ts "Stored form"). */
export function runExtent(run: RunRef): ExtentRef {
  return run.entries.extent;
}

/** One input: its blocks read a batch at a time; its current entry. */
class Cursor {
  readonly #refs: ReturnType<RunReader["blockRefs"]>;
  #next = 0;
  #lease: ReadLease | null = null;
  #blocks: readonly Uint8Array[] = [];
  #block = 0;
  #view: DataView | null = null;
  #count = 0;
  #entry = 0;
  bytes: Uint8Array = new Uint8Array(0);
  keyStart = 0;
  keyEnd = 0;
  rowId = 0;
  tombstone = false;
  done = false;

  constructor(
    reader: RunReader,
    readonly order: number,
    readonly space: SpaceReader,
    readonly budget: Grant,
  ) {
    this.#refs = reader.blockRefs();
  }

  /** Loads the next batch and positions on its first entry; false at the end. */
  async load(): Promise<boolean> {
    this.release();
    if (this.#next >= this.#refs.length) {
      this.done = true;
      return false;
    }
    const refs = this.#refs.slice(this.#next, this.#next + BATCH_BLOCKS);
    this.#next += refs.length;
    this.#lease = await this.space.read(refs, { budget: this.budget });
    this.#blocks = this.#lease.bytes;
    this.#block = -1;
    return this.#nextBlock() || this.load();
  }

  #nextBlock(): boolean {
    for (;;) {
      this.#block += 1;
      const bytes = this.#blocks[this.#block];
      if (bytes === undefined) return false;
      const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      const count = view.getUint16(0, true);
      if (count === 0) continue;
      this.bytes = bytes;
      this.#view = view;
      this.#count = count;
      this.#entry = 0;
      this.#decode();
      return true;
    }
  }

  #decode(): void {
    const bytes = this.bytes;
    const view = need(this.#view);
    let at = view.getUint16(2 + this.#entry * 2, true);
    let length = 0;
    let scale = 1;
    for (;;) {
      const byte = bytes[at++] ?? 0;
      length += (byte & 0x7f) * scale;
      if (byte < 0x80) break;
      scale *= 0x80;
    }
    this.keyStart = at;
    this.keyEnd = at + length;
    this.rowId =
      view.getUint32(at + length, true) + view.getUint16(at + length + 4, true) * 2 ** 32;
    this.tombstone = ((bytes[at + length + 6] ?? 0) & 1) === 1;
  }

  /** Moves to the next entry: true when one is ready, false when the batch is used up. */
  advance(): boolean {
    this.#entry += 1;
    if (this.#entry < this.#count) {
      this.#decode();
      return true;
    }
    return this.#nextBlock();
  }

  release(): void {
    this.#lease?.release();
    this.#lease = null;
    this.#blocks = [];
  }
}

/** Orders cursors by (key, row id, input order): the merge's heap. */
function less(a: Cursor, b: Cursor): boolean {
  const order = compareBytes(a.bytes, a.keyStart, a.keyEnd, b.bytes, b.keyStart, b.keyEnd);
  if (order !== 0) return order < 0;
  if (a.rowId !== b.rowId) return a.rowId < b.rowId;
  return a.order < b.order;
}

class Heap {
  readonly items: Cursor[] = [];

  push(cursor: Cursor): void {
    const items = this.items;
    items.push(cursor);
    let k = items.length - 1;
    while (k > 0) {
      const parent = (k - 1) >>> 1;
      if (!less(need(items[k]), need(items[parent]))) break;
      [items[k], items[parent]] = [need(items[parent]), need(items[k])];
      k = parent;
    }
  }

  /** Restores the order after the top changed. */
  down(): void {
    const items = this.items;
    let k = 0;
    for (;;) {
      const left = 2 * k + 1;
      if (left >= items.length) return;
      const right = left + 1;
      const child =
        right < items.length && less(need(items[right]), need(items[left])) ? right : left;
      if (!less(need(items[child]), need(items[k]))) return;
      [items[k], items[child]] = [need(items[child]), need(items[k])];
      k = child;
    }
  }

  pop(): void {
    const items = this.items;
    const last = need(items.pop());
    if (items.length === 0) return;
    items[0] = last;
    this.down();
  }
}

export class MergeAbortedError extends Error {
  constructor() {
    super("The merge stopped: the write path is closing");
    this.name = "MergeAbortedError";
  }
}

export interface MergeInput {
  readonly space: SpaceReader;
  readonly writer: ExtentWriter;
  /** The merge's own grant: leases and staging (`MERGE_FIXED_BYTES` + inputs). */
  readonly grant: Grant;
  /** Input readers, newest first. */
  readonly readers: readonly RunReader[];
  /** The output level. */
  readonly level: number;
  readonly bottom: boolean;
  readonly policy: MergePolicy;
  /** True when the merge must stop (checked at every slice boundary). */
  readonly stopped: () => boolean;
}

export interface MergeOutput {
  /** The output runs in ascending key order (none when everything cancelled). */
  readonly runs: readonly RunRef[];
  readonly entriesIn: number;
  readonly entriesOut: number;
  readonly cancelled: number;
  readonly bytesWritten: number;
  /** Task yields taken (R23 slices). */
  readonly slices: number;
}

/** Runs one merge (see "A merge"); the caller publishes its runs or aborts the writer. */
export async function mergeRuns(input: MergeInput): Promise<MergeOutput> {
  const { space, writer, grant, policy } = input;
  // The estimate: the inputs' extents (the output is no larger, short of block repacking).
  let estimate = 0;
  for (const reader of input.readers) estimate += runExtent(reader.ref).length;
  const cursors = input.readers.map((reader, k) => new Cursor(reader, k, space, grant));
  const heap = new Heap();
  const runs: RunRef[] = [];
  let builder: ExtentBuilder | null = null;
  let queue: Uint8Array[] = [];
  let queued = 0;
  let written = 0;
  let entriesIn = 0;
  let entriesOut = 0;
  let cancelled = 0;
  let slices = 0;
  const open = (): ExtentBuilder => writer.open(Math.max(64 * 1024, estimate - written + 4096));
  let encoder = new RunEncoder((part) => {
    queue.push(part);
    queued += part.length;
  }, input.bottom);
  const drain = async (): Promise<void> => {
    if (queue.length === 0) return;
    const target = (builder ??= open());
    const need = target.length + queued - target.capacity;
    if (need > 0) target.extend(Math.max(need, 256 * 1024));
    const parts = queue;
    queue = [];
    written += queued;
    queued = 0;
    await target.append(parts);
  };
  const finishRun = async (): Promise<void> => {
    const summary = encoder.finish();
    cancelled += encoder.cancelled;
    await drain();
    const target = builder;
    builder = null;
    if (target === null) return;
    const sealed = await target.seal();
    if (summary === null || sealed === null) return;
    const blobs = sealed.blobs;
    const count = blobs.length;
    entriesOut += summary.entryCount;
    const piece: RunRef = {
      level: input.level,
      entryCount: summary.entryCount,
      entries: need(blobs[count - 3]),
      fences: need(blobs[count - 2]),
      bloom: need(blobs[count - 1]),
      minKey: summary.minKey,
      maxKey: summary.maxKey,
    };
    // Every piece after the first continues this output's sorted run.
    runs.push(runs.length === 0 ? piece : { ...piece, continues: true });
  };
  try {
    for (const cursor of cursors) if (await cursor.load()) heap.push(cursor);
    let sliceStart = performance.now();
    let sinceCheck = 0;
    while (heap.items.length > 0) {
      const top = need(heap.items[0]);
      const full =
        encoder.indexBytes > policy.splitBytes ||
        (policy.splitEntries !== null && encoder.entryCount >= policy.splitEntries);
      if (full && encoder.canEndBefore(top.bytes, top.keyStart, top.keyEnd, top.rowId)) {
        await finishRun();
        encoder = new RunEncoder((part) => {
          queue.push(part);
          queued += part.length;
        }, input.bottom);
      }
      encoder.add(top.bytes, top.keyStart, top.keyEnd, top.rowId, top.tombstone);
      entriesIn += 1;
      if (top.advance()) heap.down();
      else {
        heap.pop();
        if (await top.load()) heap.push(top);
      }
      if (queued >= DRAIN_BYTES) await drain();
      sinceCheck += 1;
      const due =
        policy.sliceEntries === null
          ? sinceCheck >= 256 && performance.now() - sliceStart >= YIELD_INTERVAL_MS
          : sinceCheck >= policy.sliceEntries;
      if (due) {
        await yieldTask();
        slices += 1;
        if (input.stopped()) throw new MergeAbortedError();
        sliceStart = performance.now();
        sinceCheck = 0;
      } else if (sinceCheck >= 256 && policy.sliceEntries === null) sinceCheck = 0;
    }
    await finishRun();
  } finally {
    for (const cursor of cursors) cursor.release();
  }
  return { runs, entriesIn, entriesOut, cancelled, bytesWritten: written, slices };
}
