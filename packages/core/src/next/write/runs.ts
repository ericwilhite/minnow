/**
 * Persisted key-index runs (format/manifest.ts `RunRef`; access/key-index.ts "Structure"):
 * sorted `(key, rowId, put | tombstone)` entries, written once, read through the extent space's
 * verified cache. Runs carry no seq: a run is visible to every view that pins its root (ruling).
 *
 * ## Stored form (one extent per run, parts back to back from its start, every blob ≤ 1 MiB)
 * - **Blocks** of about `BLOCK_BYTES`: `u16 count`, `u16 offsets[count]` (each entry's start in the
 *   block), then entries `varuint keyLength, key, u32 rowId low, u16 rowId high, u8 flags`
 *   (bit 0 = tombstone). Entries are in (key bytes, rowId) order, so a block is binary-searched in
 *   place, never decoded.
 * - **Bloom partitions**: one Bloom filter per `PARTITION_BLOCKS` consecutive blocks over the keys
 *   with an entry in them (about 10 bits per key, 7 probes, double hashing over xxHash32 with two
 *   seeds): `u32 bits, u8 hashes`, then the bit array. Each follows the last block it covers, so a
 *   run streams out in one pass (readers find every part through the index and directory).
 * - **Block index** (`RunRef.entries`): `varuint count`, then per block
 *   `varuint offset, varuint length, u32 checksum` within the run's extent.
 * - **Fences** (`RunRef.fences`): each block's first key: `varuint count`, then
 *   `varuint length, key` per block.
 * - **Bloom directory** (`RunRef.bloom`): `varuint partitionBlocks, varuint count`, then per
 *   partition `varuint offset, varuint length, u32 checksum`.
 * The block index and fences hold about 25 bytes per block, so one run holds at most about 40,000
 * blocks (≈ 8M entries) before they would pass the blob cap (`RangeError` at write).
 *
 * ## Reading
 * A `RunReader` holds the fences, block refs and Bloom partitions resident (≈ 1.4 B per key: the
 * resident heap budget is ≤ 4 B per keyed row). A lookup costs a binary search of the fences, a
 * Bloom probe of the block's partition, and a binary search inside one block (or a few, when one
 * key's entries continue into the next block). Blocks come from the cache synchronously (`peek`)
 * when resident; otherwise the lookup returns a Promise and reads the block (a lease charged to
 * the caller's budget only while the block is searched). `cursor` streams every entry in order
 * (merges, range scans).
 *
 * ## Writing (`RunEncoder`; `RunBuilder` collects one in memory)
 * Encoded keys are at most 2 KiB; longer keys raise `IndexKeyTooLongError` when emitted, in both
 * ordinary and bulk paths. Entries are added in (key, rowId) order. A put and a tombstone of the same row id cancel (the
 * row was inserted and deleted within the merged layers); a run written to the bottom level also
 * drops the tombstones left (no older put can exist below it).
 */
import type { BlobRef, RunRef } from "../format/manifest.js";
import type { SingleEntries } from "./key-hash.js";
import { hashKey } from "./key-hashes.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { ParseBudgetError } from "../format/manifest-codec.js";
import { StoreCorruptionError } from "../storage/contract.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import { compareBytes } from "../vector/varlen.js";
import { ByteReader, ByteWriter, charged, copyRange, need, UNCHARGED } from "./bytes.js";
import type { BufferAllocator } from "./commit-buffers.js";
import { OBJECT_OVERHEAD_BYTES } from "../memory/governor.js";
import type { MaybePromise } from "../access/key-index.js";

export const BLOCK_BYTES = 4096;

/** The longest encoded index key a run block can hold: half a block. */
export const MAX_KEY_BYTES = BLOCK_BYTES / 2;

/** An encoded index key is longer than `MAX_KEY_BYTES`; no run can hold it. */
export class IndexKeyTooLongError extends RangeError {
  readonly code = "MINNOW_INDEX_KEY_TOO_LONG";
  constructor(readonly bytes: number) {
    super("An index key is longer than 2 KiB");
    this.name = "IndexKeyTooLongError";
  }
}
/** Blocks per Bloom partition (≈ 200 KiB of filter for typical keys). */
export const PARTITION_BLOCKS = 256;
const BLOOM_BITS_PER_KEY = 10;
const BLOOM_HASHES = 7;
const MAX_INDEX_BYTES = 1024 * 1024;
const EMPTY_KEY = new Uint8Array(0);

export interface RunEntry {
  readonly rowId: number;
  readonly tombstone: boolean;
}

export interface SortedKey {
  readonly key: Uint8Array;
  readonly entries: readonly RunEntry[];
}

const compare = (a: Uint8Array, b: Uint8Array): number =>
  compareBytes(a, 0, a.length, b, 0, b.length);

/** The parts of one run, ready for `ExtentWriter.write`, and how to assemble its `RunRef`. */
export interface EncodedRun extends RunSummary {
  /** Blocks and Bloom partitions in stored order, then the block index, fences and Bloom directory. */
  readonly parts: readonly Uint8Array[];
  readonly entryCount: number;
  readonly minKey: Uint8Array;
  readonly maxKey: Uint8Array;
}

/** What a finished `RunEncoder` reports (its tail parts were emitted last). */
export interface RunSummary {
  /** Idempotent: the consumer has copied/published the output, or abandoned it. */
  release(): void;
  readonly entryCount: number;
  readonly minKey: Uint8Array;
  readonly maxKey: Uint8Array;
}

/** Buffer, view, ownership record and queue slot, in governor object units. */
export const RUN_BUFFER_OVERHEAD = 3 * OBJECT_OVERHEAD_BYTES;
/** Initial encoder headroom; larger resident metadata retries after its writer unwinds. */
export const RUN_ENCODER_MINIMUM = 256 * 1024;

/** Native capacity is charged before construction; old and new coexist during resize. */
class RunBuffers implements BufferAllocator {
  readonly #live = new Map<ArrayBufferLike, number>();
  constructor(readonly budget: ByteBudget) {}
  create(bytes: number): Uint8Array {
    charged(this.budget, bytes + RUN_BUFFER_OVERHEAD);
    try {
      const out = new Uint8Array(bytes);
      this.#live.set(out.buffer, bytes + RUN_BUFFER_OVERHEAD);
      return out;
    } catch (error) {
      this.budget.free(bytes + RUN_BUFFER_OVERHEAD);
      throw error;
    }
  }
  copy(bytes: Uint8Array, from = 0, to = bytes.length): Uint8Array {
    const out = this.create(to - from);
    copyRange(out, 0, bytes, from, to);
    return out;
  }
  release(buffer: ArrayBufferView): void {
    const bytes = this.#live.get(buffer.buffer);
    if (bytes === undefined) return;
    this.#live.delete(buffer.buffer);
    this.budget.free(bytes);
  }
  /** Move one paid backing without allocating again or returning its credit prematurely. */
  transfer(buffer: Uint8Array, target: RunBuffers): void {
    const bytes = this.#live.get(buffer.buffer);
    if (bytes === undefined || this.budget !== target.budget || target.#live.has(buffer.buffer))
      throw new TypeError("Invalid run-buffer ownership transfer");
    // Install first: if the ownership record cannot be made, scratch still owns the buffer.
    target.#live.set(buffer.buffer, bytes);
    this.#live.delete(buffer.buffer);
  }

  close(): void {
    for (const bytes of this.#live.values()) this.budget.free(bytes);
    this.#live.clear();
  }
}

/**
 * Metadata payload length excludes a prepaid prefix. Finish right-aligns its counts before
 * the payload and transfers that same backing; it never copies a cardinality-sized tail.
 * Keeping payload length/capacity separate preserves the old growth and split boundaries.
 */
class RunTail extends ByteWriter {
  constructor(
    initial: number,
    readonly prefix: number,
    readonly buffers: RunBuffers,
  ) {
    super(initial + prefix, buffers);
    this.bytes = this.bytes.subarray(prefix);
    this.view = new DataView(this.bytes.buffer, this.bytes.byteOffset, this.bytes.byteLength);
  }

  override ensure(extra: number): void {
    if (this.length + extra <= this.bytes.length) return;
    let capacity = Math.max(1, this.bytes.length * 2);
    while (capacity < this.length + extra) capacity *= 2;
    const storage = this.buffers.create(capacity + this.prefix);
    try {
      const grown = storage.subarray(this.prefix);
      grown.set(this.bytes.subarray(0, this.length));
      const view = new DataView(grown.buffer, grown.byteOffset, grown.byteLength);
      this.buffers.release(this.bytes);
      this.bytes = grown;
      this.view = view;
    } catch (error) {
      this.buffers.release(storage);
      throw error;
    }
  }

  complete(target: RunBuffers, first: number, second?: number): Uint8Array {
    const length = varuintBytes(first) + (second === undefined ? 0 : varuintBytes(second));
    if (length > this.prefix) throw new RangeError("Run metadata prefix is too long");
    const out = new Uint8Array(
      this.bytes.buffer,
      this.bytes.byteOffset - length,
      length + this.length,
    );
    const at = putVaruint(out, 0, first);
    if (second !== undefined) putVaruint(out, at, second);
    this.buffers.transfer(out, target);
    this.close(); // scratch drops its view; the output account still pays the full backing
    return out;
  }
}

function varuintBytes(value: number): number {
  let bytes = 1;
  while (value >= 128) {
    bytes++;
    value = Math.floor(value / 128);
  }
  return bytes;
}
function putVaruint(out: Uint8Array, at: number, value: number): number {
  while (value >= 128) {
    out[at++] = (value % 128) | 128;
    value = Math.floor(value / 128);
  }
  out[at++] = value;
  return at;
}

/** Bytes of one entry as the block-fill rule counts them (a varuint length is at most 5). */
const entrySize = (keyLength: number): number => 5 + keyLength + 7;

/**
 * Streams sorted entries into a run's parts (see "Writing"), synchronously: each finished block
 * and Bloom partition is handed to `emit` at once, so a merge keeps only the block being filled,
 * the current partition's key hashes, and the block index and fences (≤ 1 MiB each) in memory.
 * Parts are placed back to back from offset 0 of the run's own extent, in emit order: blocks,
 * each partition's Bloom filter after its last block, then the block index, fences and Bloom
 * directory. Scratch (the pending entry, the last key, the block) belongs to the instance: two
 * encoders never share it (R23). Emitted parts never alias scratch; their charge remains with
 * the consumer until append settles. Finished boundary keys stay charged until publication.
 */
export class RunEncoder {
  readonly #emit: (part: Uint8Array) => void;
  readonly #dropTombstones: boolean;
  /** Offset of the next part within the run's extent. */
  #offset = 0;
  /** Per block: varuint offset, varuint length, u32 checksum (the count is prefixed at finish). */
  readonly #index: RunTail;
  /** Per block: its first key, length-prefixed. */
  readonly #fences: RunTail;
  /** Per partition: varuint offset, varuint length, u32 checksum. */
  readonly #directory: RunTail;
  #blockCount = 0;
  #partitions = 0;
  #blocksInPartition = 0;
  /** Bloom probes (h1, h2) of the current partition's keys, one pair per distinct key per block. */
  #hashes: Uint32Array;
  #hashCount = 0;
  /** The block being filled: entries, and each entry's start. */
  readonly #block: ByteWriter;
  #offsets: Uint16Array;
  // Reuse the ordinary offsets' backing store for buffered source slots, never per-row objects.
  #directEntries: Int32Array | null = null;
  #directData: SingleEntries | null = null;
  #directCount = 0;
  #directBytes = 0;
  #directDisabled = true;
  #count = 0;
  /** The pending entry (held back so a put and its tombstone can cancel). */
  #pendingKey: Uint8Array;
  #pendingLength = 0;
  #pendingStart = 0;
  #pendingRowId = 0;
  #pendingTombstone = false;
  #hasPending = false;
  #pendingHash: number | undefined;
  #pendingBloomHash: number | undefined;
  readonly #computed = { first: 0, second: 0 };
  #hasComputed = false;
  /** The last emitted key, and whether it is in the block being filled. */
  #lastKey: Uint8Array;
  #lastLength = 0;
  #lastStart = 0;
  #lastInBlock = false;
  #entryCount = 0;
  #minKey: Uint8Array | null = null;
  /** Entries a put/tombstone pair cancelled (both counted). */
  cancelled = 0;

  readonly #scratch: RunBuffers;
  readonly #output: RunBuffers;
  #finished = false;

  constructor(
    emit: (part: Uint8Array) => void,
    dropTombstones = false,
    readonly borrowKeys = false,
    budget: ByteBudget = UNCHARGED,
  ) {
    this.#emit = emit;
    this.#dropTombstones = dropTombstones;
    this.#directDisabled = !borrowKeys;
    this.#scratch = new RunBuffers(budget);
    this.#output = new RunBuffers(budget);
    try {
      this.#index = new RunTail(4096, 5, this.#scratch);
      this.#fences = new RunTail(4096, 5, this.#scratch);
      this.#directory = new RunTail(64, 10, this.#scratch);
      this.#hashes = new Uint32Array(this.#scratch.create(2048 * 4).buffer);
      this.#block = new ByteWriter(BLOCK_BYTES + 512, this.#scratch);
      this.#offsets = new Uint16Array(this.#scratch.create(BLOCK_BYTES * 2).buffer);
      this.#pendingKey = borrowKeys ? EMPTY_KEY : this.#scratch.create(64);
      this.#lastKey = borrowKeys ? EMPTY_KEY : this.#scratch.create(64);
      if (borrowKeys) this.#directEntries = new Int32Array(this.#offsets.buffer);
    } catch (error) {
      this.#scratch.close();
      throw error;
    }
  }

  get entryCount(): number {
    return this.#entryCount;
  }

  /** Bytes the block index or the fences would take now (the larger): a run splits past a bound. */
  get indexBytes(): number {
    return Math.max(this.#index.length, this.#fences.length) + 8;
  }

  /**
   * Whether the run may end before the entry `(bytes[start … end), rowId)`: true unless it is the
   * pending entry's put or tombstone (they must meet to cancel). A run may end inside a key's
   * entries (review B3-p2b-1 #3): one popular key with many row ids then spans several runs of one
   * merge, which are probed together (their key ranges share that key).
   */
  canEndBefore(bytes: Uint8Array, start: number, end: number, rowId: number): boolean {
    if (!this.#hasPending) return true;
    return (
      rowId !== this.#pendingRowId ||
      compareBytes(
        bytes,
        start,
        end,
        this.#pendingKey,
        this.#pendingStart,
        this.#pendingStart + this.#pendingLength,
      ) !== 0
    );
  }

  /** Adds one entry, the key `bytes[start … end)` (copied). Entries must come in (key, rowId) order. */
  add(
    bytes: Uint8Array,
    start: number,
    end: number,
    rowId: number,
    tombstone: boolean,
    hash?: number,
    bloomHash?: number,
  ): void {
    this.#disableDirect();
    const length = end - start;
    if (this.#hasPending) {
      if (
        this.#pendingRowId === rowId &&
        this.#pendingTombstone !== tombstone &&
        this.#pendingLength === length &&
        compareBytes(
          bytes,
          start,
          end,
          this.#pendingKey,
          this.#pendingStart,
          this.#pendingStart + length,
        ) === 0
      ) {
        this.#hasPending = false; // a put and its tombstone cancel
        this.cancelled += 2;
        return;
      }
      this.#emitPending();
    }
    if (this.borrowKeys) {
      this.#pendingKey = bytes;
      this.#pendingStart = start;
    } else {
      if (length > this.#pendingKey.length) {
        const grown = this.#scratch.create(Math.max(length, this.#pendingKey.length * 2));
        this.#scratch.release(this.#pendingKey);
        this.#pendingKey = grown;
      }
      copyRange(this.#pendingKey, 0, bytes, start, end);
    }
    this.#pendingLength = length;
    this.#pendingRowId = rowId;
    this.#pendingTombstone = tombstone;
    this.#pendingHash = hash;
    this.#pendingBloomHash = bloomHash;
    this.#hasPending = true;
  }

  /** Strictly distinct next key; input remains immutable and owned until finish/close. */
  addUniqueRange(
    bytes: Uint8Array,
    start: number,
    end: number,
    rowId: number,
    hash: number,
    bloomHash: number,
  ): void {
    this.#disableDirect();
    if (this.#hasPending) this.#emitPending();
    this.#writeEntry(bytes, start, end - start, rowId, false, hash, bloomHash, true);
  }

  /** Immutable, sorted single-chain slots; returns at the first multi-entry chain. */
  addSingleBatch(
    data: SingleEntries,
    from: number,
    to: number,
    bound: number,
    keep: ((rowId: number, tombstone: boolean) => boolean) | null = null,
  ): number {
    if (this.#directDisabled) return this.#singleFallback(data, from, to, bound, keep);
    if (this.#directData !== null && this.#directData !== data) {
      this.#disableDirect();
      return this.#singleFallback(data, from, to, bound, keep);
    }
    this.#directData = data;
    const positions = need(this.#directEntries);
    for (let at = from; at < to; at++) {
      const slot = data.order === null ? at : (data.order[at] ?? 0);
      const head = data.heads[slot] ?? -1;
      if (head < 0) continue;
      if ((data.next[head] ?? -1) >= 0) return at;
      if ((data.seqs[head] ?? 0) > bound) continue;
      const rowId = data.rowIds[head] ?? 0,
        tombstone = data.tombstones[head] === 1;
      if (keep !== null && !keep(rowId, tombstone)) continue;
      if (tombstone) {
        this.#disableDirect();
        this.add(
          data.arena,
          data.starts[slot] ?? 0,
          data.ends[slot] ?? 0,
          rowId,
          true,
          data.hashes[slot] ?? 0,
          data.bloomHashes[slot] ?? 0,
        );
        return this.#singleFallback(data, at + 1, to, bound, keep);
      }
      const length = (data.ends[slot] ?? 0) - (data.starts[slot] ?? 0);
      if (length > MAX_KEY_BYTES) throw new IndexKeyTooLongError(length);
      if (
        this.#directCount > 0 &&
        (this.#directBytes + entrySize(length) + (this.#directCount + 1) * 2 > BLOCK_BYTES ||
          this.#directCount >= 0xfff0)
      )
        this.#flushDirectBlock();
      positions[this.#directCount++] = slot;
      this.#directBytes += (length < 0x80 ? 1 : 2) + length + 7;
    }
    return to;
  }

  #singleFallback(
    data: SingleEntries,
    from: number,
    to: number,
    bound: number,
    keep: ((rowId: number, tombstone: boolean) => boolean) | null,
  ): number {
    for (let at = from; at < to; at++) {
      const slot = data.order === null ? at : (data.order[at] ?? 0);
      const head = data.heads[slot] ?? -1;
      if (head < 0) continue;
      if ((data.next[head] ?? -1) >= 0) return at;
      if ((data.seqs[head] ?? 0) > bound) continue;
      const start = data.starts[slot] ?? 0,
        end = data.ends[slot] ?? 0;
      const rowId = data.rowIds[head] ?? 0,
        hash = data.hashes[slot] ?? 0,
        bloomHash = data.bloomHashes[slot] ?? 0;
      const tombstone = data.tombstones[head] === 1;
      if (keep !== null && !keep(rowId, tombstone)) continue;
      if (tombstone) this.add(data.arena, start, end, rowId, true, hash, bloomHash);
      else this.addUniqueRange(data.arena, start, end, rowId, hash, bloomHash);
    }
    return to;
  }

  #disableDirect(): void {
    if (this.#directDisabled) return;
    this.#directDisabled = true;
    const data = this.#directData,
      count = this.#directCount;
    this.#directCount = 0;
    this.#directBytes = 0;
    if (data !== null)
      for (let at = 0; at < count; at++) {
        // Ordinary offsets overwrite only positions already consumed in this forward walk.
        const slot = need(this.#directEntries)[at] ?? 0,
          head = data.heads[slot] ?? -1;
        const start = data.starts[slot] ?? 0;
        this.#writeEntry(
          data.arena,
          start,
          (data.ends[slot] ?? 0) - start,
          data.rowIds[head] ?? 0,
          false,
          data.hashes[slot] ?? 0,
          data.bloomHashes[slot] ?? 0,
          true,
        );
      }
    this.#directData = null;
  }

  #flushDirectBlock(): void {
    const count = this.#directCount;
    if (count === 0) return;
    const data = need(this.#directData),
      positions = need(this.#directEntries);
    const header = 2 + count * 2;
    const out = this.#output.create(header + this.#directBytes),
      view = new DataView(out.buffer);
    view.setUint16(0, count, true);
    const first = positions[0] ?? 0,
      firstStart = data.starts[first] ?? 0,
      firstEnd = data.ends[first] ?? 0;
    this.#fences.varuint(firstEnd - firstStart);
    this.#fences.rawRange(data.arena, firstStart, firstEnd);
    this.#minKey ??= this.#output.copy(data.arena, firstStart, firstEnd);
    const needed = this.#hashCount + count * 2;
    if (needed > this.#hashes.length) {
      let capacity = this.#hashes.length * 2;
      while (capacity < needed) capacity *= 2;
      const grown = new Uint32Array(this.#scratch.create(capacity * 4).buffer);
      grown.set(this.#hashes);
      this.#scratch.release(this.#hashes);
      this.#hashes = grown;
    }
    let cursor = header;
    for (let at = 0; at < count; at++) {
      const slot = positions[at] ?? 0,
        head = data.heads[slot] ?? -1;
      const start = data.starts[slot] ?? 0,
        end = data.ends[slot] ?? 0,
        length = end - start;
      view.setUint16(2 + at * 2, cursor, true);
      if (length < 0x80) out[cursor++] = length;
      else {
        out[cursor++] = (length & 0x7f) | 0x80;
        out[cursor++] = length >>> 7;
      }
      copyRange(out, cursor, data.arena, start, end);
      cursor += length;
      const rowId = data.rowIds[head] ?? 0;
      view.setUint32(cursor, rowId % 2 ** 32, true);
      view.setUint16(cursor + 4, Math.floor(rowId / 2 ** 32), true);
      out[cursor + 6] = 0;
      cursor += 7;
      this.#hashes[this.#hashCount++] = (data.hashes[slot] ?? 0) >>> 0;
      this.#hashes[this.#hashCount++] = ((data.bloomHashes[slot] ?? 0) | 1) >>> 0;
    }
    const last = positions[count - 1] ?? 0;
    this.#lastKey = data.arena;
    this.#lastStart = data.starts[last] ?? 0;
    this.#lastLength = (data.ends[last] ?? 0) - this.#lastStart;
    this.#computed.first = data.hashes[last] ?? 0;
    this.#computed.second = data.bloomHashes[last] ?? 0;
    this.#hasComputed = true;
    this.#lastInBlock = false;
    this.#index.varuint(this.#offset);
    this.#index.varuint(out.length);
    this.#index.u32(xxHash32(out));
    this.#put(out);
    this.#entryCount += count;
    this.#blockCount++;
    this.#directCount = 0;
    this.#directBytes = 0;
    this.#blocksInPartition++;
    if (this.#blocksInPartition === PARTITION_BLOCKS) this.#closePartition();
  }

  #emitPending(): void {
    this.#hasPending = false;
    this.#writeEntry(
      this.#pendingKey,
      this.#pendingStart,
      this.#pendingLength,
      this.#pendingRowId,
      this.#pendingTombstone,
      this.#pendingHash,
      this.#pendingBloomHash,
      false,
    );
  }

  #writeEntry(
    key: Uint8Array,
    start: number,
    length: number,
    rowId: number,
    tombstone: boolean,
    hash: number | undefined,
    bloomHash: number | undefined,
    distinct: boolean,
  ): void {
    if (tombstone && this.#dropTombstones) return;
    if (length > MAX_KEY_BYTES) throw new IndexKeyTooLongError(length);
    if (
      this.#count > 0 &&
      (this.#block.length + entrySize(length) + (this.#count + 1) * 2 > BLOCK_BYTES ||
        this.#count >= 0xfff0)
    )
      this.#closeBlock();
    const same =
      !distinct &&
      this.#lastLength === length &&
      compareBytes(
        key,
        start,
        start + length,
        this.#lastKey,
        this.#lastStart,
        this.#lastStart + length,
      ) === 0;
    if (this.#count === 0) {
      this.#fences.varuint(length);
      this.#fences.rawRange(key, start, start + length);
    }
    if (!(this.#lastInBlock && same)) {
      if (this.#hashCount + 2 > this.#hashes.length) {
        const grown = new Uint32Array(this.#scratch.create(this.#hashes.byteLength * 2).buffer);
        grown.set(this.#hashes);
        this.#scratch.release(this.#hashes);
        this.#hashes = grown;
      }
      // The delta supplies canonical hashes. Disk merges compute the pair once per distinct
      // emitted key, then reuse it when the same key crosses a block or Bloom partition.
      if (!same || !this.#hasComputed) {
        if (hash !== undefined && bloomHash !== undefined) {
          this.#computed.first = hash;
          this.#computed.second = bloomHash;
        } else hashKey(key, start, start + length, this.#computed);
        this.#hasComputed = true;
      }
      this.#hashes[this.#hashCount++] = this.#computed.first >>> 0;
      this.#hashes[this.#hashCount++] = (this.#computed.second | 1) >>> 0;
    }
    this.#lastInBlock = true;
    if (!same) {
      if (this.borrowKeys) {
        this.#lastKey = key;
        this.#lastStart = start;
      } else {
        if (length > this.#lastKey.length) {
          const grown = this.#scratch.create(Math.max(length, this.#lastKey.length * 2));
          this.#scratch.release(this.#lastKey);
          this.#lastKey = grown;
        }
        copyRange(this.#lastKey, 0, key, start, start + length);
      }
      this.#lastLength = length;
    }
    const block = this.#block;
    this.#offsets[this.#count++] = block.length;
    // One reserve, then the entry's bytes in place: varuint length (a key is ≤ 2 KiB, so one
    // or two bytes), key, u32 row id low, u16 row id high, flags.
    block.ensure(length + 9);
    const bytes = block.bytes;
    let at = block.length;
    if (length < 0x80) bytes[at++] = length;
    else {
      bytes[at++] = (length & 0x7f) | 0x80;
      bytes[at++] = length >>> 7;
    }
    copyRange(bytes, at, key, start, start + length);
    at += length;
    const low = rowId % 2 ** 32;
    const high = Math.floor(rowId / 2 ** 32);
    bytes[at] = low & 0xff;
    bytes[at + 1] = (low >>> 8) & 0xff;
    bytes[at + 2] = (low >>> 16) & 0xff;
    bytes[at + 3] = (low >>> 24) & 0xff;
    bytes[at + 4] = high & 0xff;
    bytes[at + 5] = (high >>> 8) & 0xff;
    bytes[at + 6] = tombstone ? 1 : 0;
    block.length = at + 7;
    this.#entryCount += 1;
    this.#minKey ??= this.#output.copy(key, start, start + length);
  }

  #closeBlock(): void {
    const count = this.#count;
    if (count === 0) return;
    const header = 2 + count * 2;
    const out = this.#output.create(header + this.#block.length);
    const view = new DataView(out.buffer);
    view.setUint16(0, count, true);
    for (let k = 0; k < count; k++)
      view.setUint16(2 + k * 2, header + (this.#offsets[k] ?? 0), true);
    out.set(this.#block.bytes.subarray(0, this.#block.length), header);
    this.#index.varuint(this.#offset);
    this.#index.varuint(out.length);
    this.#index.u32(xxHash32(out));
    this.#put(out);
    this.#blockCount += 1;
    this.#block.reset();
    this.#count = 0;
    this.#lastInBlock = false;
    this.#blocksInPartition += 1;
    if (this.#blocksInPartition === PARTITION_BLOCKS) this.#closePartition();
  }

  #closePartition(): void {
    if (this.#blocksInPartition === 0) return;
    const bloom = encodeBloom(this.#hashes, this.#hashCount, this.#output);
    this.#directory.varuint(this.#offset);
    this.#directory.varuint(bloom.length);
    this.#directory.u32(xxHash32(bloom));
    this.#put(bloom);
    this.#partitions += 1;
    this.#blocksInPartition = 0;
    this.#hashCount = 0;
  }

  #put(part: Uint8Array): void {
    this.#offset += part.length;
    this.#emit(part);
  }

  /**
   * Emits what is left and the tail parts (block index, fences, Bloom directory: the run's last
   * three blobs). Null when the run holds no entry (nothing at all was emitted then).
   */
  finish(): RunSummary | null {
    this.#flushDirectBlock();
    if (this.#hasPending) this.#emitPending();
    this.#closeBlock();
    this.#closePartition();
    const minKey = this.#minKey;
    if (this.#entryCount === 0 || minKey === null) return null;
    const index = this.#index.complete(this.#output, this.#blockCount);
    const fences = this.#fences.complete(this.#output, this.#blockCount);
    const directory = this.#directory.complete(this.#output, PARTITION_BLOCKS, this.#partitions);
    if (index.length > MAX_INDEX_BYTES || fences.length > MAX_INDEX_BYTES)
      throw new RangeError("An index run has more blocks than one run can index");
    this.#put(index);
    this.#put(fences);
    this.#put(directory);
    const maxKey = this.#output.copy(
      this.#lastKey,
      this.#lastStart,
      this.#lastStart + this.#lastLength,
    );
    this.#finished = true;
    const output = this.#output;
    return {
      entryCount: this.#entryCount,
      minKey,
      maxKey,
      release: () => output.close(),
    };
  }
  /** The extent writer has resolved append(); it no longer borrows this part. */
  releasePart(part: Uint8Array): void {
    this.#output.release(part);
  }

  /** Release scratch and borrowed inputs; unfinished output is aborted. Not resumable. */
  close(): void {
    this.#index.close();
    this.#fences.close();
    this.#directory.close();
    this.#block.close();
    this.#scratch.close();
    this.#hashes = new Uint32Array(0);
    this.#offsets = new Uint16Array(0);
    this.#pendingKey = EMPTY_KEY;
    this.#lastKey = EMPTY_KEY;
    this.#minKey = null;
    this.#hasPending = false;
    this.#directData = null;
    this.#directEntries = null;
    this.#directCount = 0;
    this.#directBytes = 0;
    if (!this.#finished) this.#output.close();
  }
}

/** Collects encoded parts until finish or a bounded consumer takes and releases them. */
export class RunBuilder {
  readonly #parts: Uint8Array[] = [];
  readonly #encoder: RunEncoder;
  #queuedBytes = 0;
  #transferred = false;

  constructor(dropTombstones = false, borrowKeys = false, budget: ByteBudget = UNCHARGED) {
    this.#encoder = new RunEncoder(
      (part) => {
        this.#parts.push(part);
        this.#queuedBytes += part.length;
      },
      dropTombstones,
      borrowKeys,
      budget,
    );
  }

  get queuedBytes(): number {
    return this.#queuedBytes;
  }

  /** Bounded streaming consumer; returned buffers stay charged until releasePart after append. */
  takeParts(): Uint8Array[] {
    this.#queuedBytes = 0;
    return this.#parts.splice(0);
  }

  releasePart(part: Uint8Array): void {
    this.#encoder.releasePart(part);
  }

  /** Adds one entry; `key` is copied. Entries must come in (key, rowId) order. */
  add(key: Uint8Array, rowId: number, tombstone: boolean): void {
    this.#encoder.add(key, 0, key.length, rowId, tombstone);
  }

  addSingleBatch(
    data: SingleEntries,
    from: number,
    to: number,
    bound: number,
    keep: ((rowId: number, tombstone: boolean) => boolean) | null = null,
  ): number {
    return this.#encoder.addSingleBatch(data, from, to, bound, keep);
  }

  /** Caller retains immutable input to finish/close; this key occurs only once in the run. */
  addUniqueRange(
    bytes: Uint8Array,
    start: number,
    end: number,
    rowId: number,
    hash: number,
    bloomHash: number,
  ): void {
    this.#encoder.addUniqueRange(bytes, start, end, rowId, hash, bloomHash);
  }

  close(): void {
    this.#encoder.close();
    if (!this.#transferred) {
      this.#parts.length = 0;
      this.#queuedBytes = 0;
    }
  }

  /** `add` of the key `bytes[start … end)`. Optional hashes are raw XXH32 with seeds 0 and KEY_BLOOM_SEED. */
  addRange(
    bytes: Uint8Array,
    start: number,
    end: number,
    rowId: number,
    tombstone: boolean,
    hash?: number,
    bloomHash?: number,
  ): void {
    this.#encoder.add(bytes, start, end, rowId, tombstone, hash, bloomHash);
  }

  get entryCount(): number {
    return this.#encoder.entryCount;
  }

  /** The finished run, or null when nothing remains. */
  finish(): EncodedRun | null {
    try {
      const summary = this.#encoder.finish();
      this.#transferred = summary !== null;
      return summary === null ? null : { parts: this.#parts, ...summary };
    } finally {
      this.#encoder.close();
    }
  }
}

/** Encodes sorted keys (byte order; entries per key in row-id order) as a run, or null. */
export function encodeRun(keys: readonly SortedKey[]): EncodedRun | null {
  const builder = new RunBuilder();
  for (const { key, entries } of keys)
    for (const entry of entries) builder.add(key, entry.rowId, entry.tombstone);
  return builder.finish();
}

/** The `RunRef` of an encoded run once `ExtentWriter.write(run.parts)` returned its blobs. */
export function runRef(run: RunSummary, blobs: readonly BlobRef[], level = 0): RunRef {
  const count = blobs.length;
  return {
    level,
    entryCount: run.entryCount,
    entries: need(blobs[count - 3]),
    fences: need(blobs[count - 2]),
    bloom: need(blobs[count - 1]),
    minKey: run.minKey,
    maxKey: run.maxKey,
  };
}

/** One partition's Bloom filter over `count / 2` keys' probe pairs (exported for its test). */
export function encodeBloom(
  hashes: Uint32Array,
  count: number,
  allocator?: BufferAllocator,
): Uint8Array {
  const keys = count / 2;
  const bits = Math.max(64, Math.ceil((keys * BLOOM_BITS_PER_KEY) / 8) * 8);
  const out = allocator?.create(5 + bits / 8) ?? new Uint8Array(5 + bits / 8);
  new DataView(out.buffer).setUint32(0, bits, true);
  out[4] = BLOOM_HASHES;
  // (h1 + p·h2) mod bits, stepped: (h1 mod bits) + p·(h2 mod bits), wrapping at bits. Exactly
  // the same bits (every term is an integer below 2^35), without a double modulo per probe.
  for (let k = 0; k < count; k += 2) {
    let bit = (hashes[k] ?? 0) % bits;
    const step = (hashes[k + 1] ?? 0) % bits;
    // Seven fixed stored probes, unrolled to remove the inner loop's counter and branch.
    // The size, seeds, arithmetic and probe order are unchanged (a stored-format contract).
    out[5 + (bit >>> 3)] = (out[5 + (bit >>> 3)] ?? 0) | (1 << (bit & 7));
    bit += step;
    if (bit >= bits) bit -= bits;
    out[5 + (bit >>> 3)] = (out[5 + (bit >>> 3)] ?? 0) | (1 << (bit & 7));
    bit += step;
    if (bit >= bits) bit -= bits;
    out[5 + (bit >>> 3)] = (out[5 + (bit >>> 3)] ?? 0) | (1 << (bit & 7));
    bit += step;
    if (bit >= bits) bit -= bits;
    out[5 + (bit >>> 3)] = (out[5 + (bit >>> 3)] ?? 0) | (1 << (bit & 7));
    bit += step;
    if (bit >= bits) bit -= bits;
    out[5 + (bit >>> 3)] = (out[5 + (bit >>> 3)] ?? 0) | (1 << (bit & 7));
    bit += step;
    if (bit >= bits) bit -= bits;
    out[5 + (bit >>> 3)] = (out[5 + (bit >>> 3)] ?? 0) | (1 << (bit & 7));
    bit += step;
    if (bit >= bits) bit -= bits;
    out[5 + (bit >>> 3)] = (out[5 + (bit >>> 3)] ?? 0) | (1 << (bit & 7));
  }
  return out;
}

interface Bloom {
  readonly bits: number;
  readonly hashes: number;
  readonly array: Uint8Array;
}

function parseBloom(bytes: Uint8Array): Bloom {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const bits = view.getUint32(0, true);
  if (bits === 0 || bytes.length !== 5 + bits / 8)
    throw new StoreCorruptionError("A run's Bloom filter is malformed");
  return { bits, hashes: bytes[4] ?? 0, array: bytes.slice(5) };
}

function readRefs(reader: ByteReader, extent: BlobRef["extent"]): BlobRef[] {
  const count = reader.varuint();
  return Array.from({ length: count }, () => ({
    extent,
    offset: reader.varuint(),
    length: reader.varuint(),
    checksum: reader.u32(),
  }));
}

/** Reads `refs` a few at a time (bounded per read: B1 `MAX_READ_BYTES`), copying the bytes. */
async function readAll(
  space: SpaceReader,
  refs: readonly BlobRef[],
  budget: ByteBudget,
): Promise<Uint8Array[]> {
  const out: Uint8Array[] = [];
  for (let from = 0; from < refs.length;) {
    let bytes = 0;
    let to = from;
    while (to < refs.length && (to === from || bytes + need(refs[to]).length <= 2 * 1024 * 1024)) {
      bytes += need(refs[to]).length;
      to += 1;
    }
    const lease = await space.read(refs.slice(from, to), { budget });
    try {
      for (const part of lease.bytes) out.push(part.slice());
    } finally {
      lease.release();
    }
    from = to;
  }
  return out;
}

/** A run's resident parts: fences, block refs and Bloom partitions. */
export class RunReader {
  readonly #firstKeys: Uint8Array[];
  readonly #blocks: BlobRef[];
  readonly #blooms: Bloom[];
  readonly #partitionBlocks: number;
  /** Resident bytes (charged by the owner of the reader). */
  readonly bytes: number;

  private constructor(
    readonly ref: RunRef,
    fences: Uint8Array,
    index: Uint8Array,
    partitionBlocks: number,
    blooms: readonly Uint8Array[],
    charged: number,
  ) {
    const fenceReader = new ByteReader(fences);
    const fenceCount = fenceReader.varuint();
    this.#firstKeys = Array.from({ length: fenceCount }, () => fenceReader.blob().slice());
    fenceReader.end();
    const indexReader = new ByteReader(index);
    this.#blocks = readRefs(indexReader, ref.entries.extent);
    indexReader.end();
    if (this.#blocks.length !== fenceCount)
      throw new StoreCorruptionError("A run's fences and blocks differ");
    this.#partitionBlocks = partitionBlocks;
    this.#blooms = blooms.map(parseBloom);
    if (this.#blooms.length !== Math.ceil(fenceCount / partitionBlocks))
      throw new StoreCorruptionError("A run's Bloom partitions do not cover its blocks");
    this.bytes = charged;
  }

  /**
   * The charge for a run's resident parts, from the raw bytes before anything is built: every
   * Bloom partition, every fence key with its object (48 B), and each block's ref (64 B). It bounds
   * what the constructor keeps (the partitions less their 5-byte headers, the keys less their
   * length prefixes).
   */
  /**
   * Pre-admission bound from an existing run reference, without reading its directories.
   * Every block-index entry uses at least two varints and a u32 checksum (six bytes).
   * Bloom construction uses at most ten bits per input entry, plus thirteen bytes per
   * partition for rounding, the minimum bitset and its header. Stored bytes are unchanged.
   */
  static residentBound(ref: RunRef): number {
    const blocks = Math.min(ref.entryCount, Math.max(0, Math.floor((ref.entries.length - 1) / 6)));
    return (
      ref.fences.length +
      blocks * (48 + 64) +
      Math.ceil((ref.entryCount * BLOOM_BITS_PER_KEY) / 8) +
      13 * Math.ceil(blocks / PARTITION_BLOCKS)
    );
  }

  static residentCharge(fences: Uint8Array, blooms: readonly Uint8Array[]): number {
    const count = new ByteReader(fences).varuint();
    let total = fences.length + count * (48 + 64);
    for (const bloom of blooms) total += bloom.length;
    return total;
  }

  static #build(
    ref: RunRef,
    fences: Uint8Array,
    index: Uint8Array,
    partitionBlocks: number,
    blooms: readonly Uint8Array[],
    charge: ByteBudget,
  ): RunReader {
    const bytes = RunReader.residentCharge(fences, blooms);
    if (!charge.allocate(bytes)) throw new ParseBudgetError(bytes);
    try {
      return new RunReader(ref, fences, index, partitionBlocks, blooms, bytes);
    } catch (error) {
      charge.free(bytes);
      throw error;
    }
  }

  /**
   * Loads a run's resident parts (synchronously when they are all cached). Reads are leased on
   * `budget`; the resident parts are charged to `charge` (`bytes`, `ParseBudgetError` on refusal)
   * before they are built, and the caller frees `bytes` when it drops the reader.
   */
  static open(
    space: SpaceReader,
    ref: RunRef,
    budget: ByteBudget,
    charge: ByteBudget,
  ): MaybePromise<RunReader> {
    const fences = space.peek(ref.fences);
    const index = space.peek(ref.entries);
    const directory = space.peek(ref.bloom);
    if (fences !== null && index !== null && directory !== null) {
      const { partitionBlocks, refs } = RunReader.#directory(ref, directory);
      const blooms = refs.map((blob) => space.peek(blob));
      if (blooms.every((bytes): bytes is Uint8Array => bytes !== null))
        return RunReader.#build(ref, fences, index, partitionBlocks, blooms, charge);
    }
    return (async () => {
      const [fenceBytes, indexBytes, directoryBytes] = await readAll(
        space,
        [ref.fences, ref.entries, ref.bloom],
        budget,
      );
      const { partitionBlocks, refs } = RunReader.#directory(ref, need(directoryBytes));
      const blooms = await readAll(space, refs, budget);
      return RunReader.#build(
        ref,
        need(fenceBytes),
        need(indexBytes),
        partitionBlocks,
        blooms,
        charge,
      );
    })();
  }

  static #directory(ref: RunRef, bytes: Uint8Array): { partitionBlocks: number; refs: BlobRef[] } {
    const reader = new ByteReader(bytes);
    const partitionBlocks = reader.varuint();
    if (partitionBlocks === 0)
      throw new StoreCorruptionError("A run's Bloom directory is malformed");
    const refs = readRefs(reader, ref.bloom.extent);
    reader.end();
    return { partitionBlocks, refs };
  }

  get blockCount(): number {
    return this.#blocks.length;
  }

  readonly #computed = { first: 0, second: 0 };

  #bloomHas(
    key: Uint8Array,
    first: number,
    end: number,
    hash?: number,
    bloomHash?: number,
  ): boolean {
    if (hash === undefined || bloomHash === undefined) {
      hashKey(key, 0, key.length, this.#computed);
      hash = this.#computed.first;
      bloomHash = this.#computed.second;
    }
    const h1 = hash >>> 0,
      h2 = (bloomHash | 1) >>> 0;
    const size = this.#partitionBlocks;
    for (let partition = Math.floor(first / size); partition * size < end; partition++) {
      const bloom = this.#blooms[partition];
      if (bloom === undefined) break;
      let hit = true;
      for (let k = 0; k < bloom.hashes && hit; k++) {
        const bit = (h1 + k * h2) % bloom.bits;
        if (((bloom.array[bit >>> 3] ?? 0) & (1 << (bit & 7))) === 0) hit = false;
      }
      if (hit) return true;
    }
    return false;
  }

  mayContain(key: Uint8Array): boolean {
    if (compare(key, this.ref.minKey) < 0 || compare(key, this.ref.maxKey) > 0) return false;
    const [first, end] = this.#blocksFor(key);
    return this.#bloomHas(key, first, end);
  }

  /** Blocks that may hold `key`'s entries: `[first, end)`. */
  #blocksFor(key: Uint8Array): [number, number] {
    const keys = this.#firstKeys;
    // Last block whose first key is < key (entries equal to key may start at its end).
    let lo = 0;
    let hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (compare(need(keys[mid]), key) < 0) lo = mid + 1;
      else hi = mid;
    }
    const first = Math.max(0, lo - 1);
    let end = lo;
    while (end < keys.length && compare(need(keys[end]), key) === 0) end += 1;
    return [first, Math.max(end, first + 1)];
  }

  /**
   * Entries of `key` in this run, in row-id order (appended to `out`). Synchronous when its
   * blocks are cached.
   */
  lookup(
    space: SpaceReader,
    key: Uint8Array,
    out: RunEntry[],
    budget: ByteBudget,
    hash?: number,
    bloomHash?: number,
  ): MaybePromise<void> {
    if (compare(key, this.ref.minKey) < 0 || compare(key, this.ref.maxKey) > 0) return;
    const [first, end] = this.#blocksFor(key);
    if (!this.#bloomHas(key, first, end, hash, bloomHash)) return;
    let missing = false;
    const blocks: Array<Uint8Array | null> = [];
    for (let block = first; block < end; block++) {
      const bytes = space.peek(need(this.#blocks[block]));
      blocks.push(bytes);
      if (bytes === null) missing = true;
    }
    if (!missing) {
      for (const bytes of blocks) searchBlock(need(bytes), key, out);
      return;
    }
    return space.read(this.#blocks.slice(first, end), { budget }).then((lease) => {
      try {
        for (const bytes of lease.bytes) searchBlock(bytes, key, out);
      } finally {
        lease.release();
      }
    });
  }

  /**
   * Streams every entry in order, from the block that may hold `from` (or the first). `visit`
   * returns true to stop. The key is a view valid during the call.
   */
  async cursor(
    space: SpaceReader,
    budget: ByteBudget,
    visit: (key: Uint8Array, rowId: number, tombstone: boolean) => boolean,
    from: Uint8Array | null = null,
    drain?: () => MaybePromise<void>,
  ): Promise<void> {
    const start = from === null ? 0 : this.#blocksFor(from)[0];
    for (let block = start; block < this.#blocks.length; block++) {
      const ref = need(this.#blocks[block]);
      let bytes = space.peek(ref);
      let release: (() => void) | null = null;
      if (bytes === null) {
        const lease = await space.read([ref], { budget });
        bytes = lease.bytes[0] ?? new Uint8Array(0);
        release = () => {
          lease.release();
        };
      }
      try {
        if (forEachEntry(bytes, visit)) return;
      } finally {
        release?.();
      }
      await drain?.();
    }
  }

  /** Every entry whose key satisfies `accept` (−1 below, 0 in, 1 above the range), in order. */
  async scan(
    space: SpaceReader,
    budget: ByteBudget,
    accept: (key: Uint8Array) => -1 | 0 | 1,
    visit: (key: Uint8Array, entry: RunEntry) => void,
  ): Promise<void> {
    await this.cursor(space, budget, (key, rowId, tombstone) => {
      const position = accept(key);
      if (position === 0) visit(key.slice(), { rowId, tombstone });
      return position > 0;
    });
  }

  /** Every block's bytes, copied, in order (merges read whole runs a block at a time). */
  blockRefs(): readonly BlobRef[] {
    return this.#blocks;
  }
}

/**
 * Entry k's key range in a block, written into `located` (no allocation per probe step). Module
 * scratch, allowed by ruling R23 only because every use is inside one synchronous call
 * (`searchBlock`, `forEachEntry` and its visitor) that never suspends.
 */
const located = { keyStart: 0, keyEnd: 0 };
function locate(block: Uint8Array, view: DataView, k: number): typeof located {
  let at = view.getUint16(2 + k * 2, true);
  let length = 0;
  let scale = 1;
  for (;;) {
    const byte = block[at++] ?? 0;
    length += (byte & 0x7f) * scale;
    if (byte < 0x80) break;
    scale *= 0x80;
  }
  located.keyStart = at;
  located.keyEnd = at + length;
  return located;
}

function rowIdAt(view: DataView, at: number): number {
  return view.getUint32(at, true) + view.getUint16(at + 4, true) * 2 ** 32;
}

/** Appends the entries of `key` in `block` to `out`. */
function searchBlock(block: Uint8Array, key: Uint8Array, out: RunEntry[]): void {
  const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
  const count = view.getUint16(0, true);
  let lo = 0;
  let hi = count;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    const { keyStart, keyEnd } = locate(block, view, mid);
    if (compareBytes(block, keyStart, keyEnd, key, 0, key.length) < 0) lo = mid + 1;
    else hi = mid;
  }
  for (let k = lo; k < count; k++) {
    const { keyStart, keyEnd } = locate(block, view, k);
    if (compareBytes(block, keyStart, keyEnd, key, 0, key.length) !== 0) return;
    out.push({ rowId: rowIdAt(view, keyEnd), tombstone: ((block[keyEnd + 6] ?? 0) & 1) === 1 });
  }
}

/** Decodes every entry of a block, in order, into `out` (merges). */
export function blockEntries(
  block: Uint8Array,
  out: Array<{ key: Uint8Array; rowId: number; tombstone: boolean }>,
): void {
  forEachEntry(block, (key, rowId, tombstone) => {
    out.push({ key: key.slice(), rowId, tombstone });
    return false;
  });
}

/** Visits a block's entries in order until `visit` returns true; returns whether it stopped. */
function forEachEntry(
  block: Uint8Array,
  visit: (key: Uint8Array, rowId: number, tombstone: boolean) => boolean,
): boolean {
  const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
  const count = view.getUint16(0, true);
  for (let k = 0; k < count; k++) {
    const { keyStart, keyEnd } = locate(block, view, k);
    const tombstone = ((block[keyEnd + 6] ?? 0) & 1) === 1;
    if (visit(block.subarray(keyStart, keyEnd), rowIdAt(view, keyEnd), tombstone)) return true;
  }
  return false;
}
