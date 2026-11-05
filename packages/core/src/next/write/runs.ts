/**
 * Persisted key-index runs (format/manifest.ts `RunRef`; access/key-index.ts "Structure"):
 * sorted `(key, rowId, put | tombstone)` entries, written once, read through the extent space's
 * verified cache. Runs carry no seq: a run is visible to every view that pins its root (ruling).
 *
 * ## Stored form (one extent per run, parts back to back, every blob ≤ 1 MiB)
 * - **Blocks** of about `BLOCK_BYTES`: `u16 count`, `u16 offsets[count]` (each entry's start in the
 *   block), then entries `varuint keyLength, key, u32 rowId low, u16 rowId high, u8 flags`
 *   (bit 0 = tombstone). Entries are in (key bytes, rowId) order, so a block is binary-searched in
 *   place, never decoded.
 * - **Bloom partitions**: one Bloom filter per `PARTITION_BLOCKS` consecutive blocks over the keys
 *   with an entry in them (about 10 bits per key, 7 probes, double hashing over xxHash32 with two
 *   seeds): `u32 bits, u8 hashes`, then the bit array.
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
 * ## Writing (`RunBuilder`)
 * Entries are added in (key, rowId) order. A put and a tombstone of the same row id cancel (the
 * row was inserted and deleted within the merged layers); a run written to the bottom level also
 * drops the tombstones left (no older put can exist below it).
 */
import type { BlobRef, RunRef } from "../format/manifest.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { StoreCorruptionError } from "../storage/contract.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import { compareBytes } from "../vector/varlen.js";
import { ByteReader, ByteWriter, need } from "./bytes.js";
import type { MaybePromise } from "../access/key-index.js";

export const BLOCK_BYTES = 4096;
/** Blocks per Bloom partition (≈ 200 KiB of filter for typical keys). */
export const PARTITION_BLOCKS = 256;
const BLOOM_BITS_PER_KEY = 10;
const BLOOM_HASHES = 7;
const BLOOM_SEED = 0x9747b28c;
const MAX_INDEX_BYTES = 1024 * 1024;

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

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let k = 0; k < a.length; k++) if (a[k] !== b[k]) return false;
  return true;
}

/** The parts of one run, ready for `ExtentWriter.write`, and how to assemble its `RunRef`. */
export interface EncodedRun {
  /** Blocks, Bloom partitions, then the block index, fences and Bloom directory (last three). */
  readonly parts: readonly Uint8Array[];
  readonly entryCount: number;
  readonly minKey: Uint8Array;
  readonly maxKey: Uint8Array;
}

/** Streams sorted entries into a run (see "Writing"). */
export class RunBuilder {
  readonly #blocks: Uint8Array[] = [];
  readonly #firstKeys: Uint8Array[] = [];
  /** Bloom probes (h1, h2) of each block's distinct keys. */
  readonly #blockHashes: number[][] = [];
  #hashes: number[] = [];
  readonly #block = new ByteWriter(BLOCK_BYTES + 512);
  #offsets: number[] = [];
  #firstKey: Uint8Array | null = null;
  #lastBlockKey: Uint8Array | null = null;
  #pending: { key: Uint8Array; rowId: number; tombstone: boolean } | null = null;
  #entryCount = 0;
  #minKey: Uint8Array | null = null;
  #maxKey: Uint8Array | null = null;

  constructor(readonly dropTombstones = false) {}

  /** Adds one entry; `key` is copied. Entries must come in (key, rowId) order. */
  add(key: Uint8Array, rowId: number, tombstone: boolean): void {
    const pending = this.#pending;
    if (pending !== null) {
      if (
        pending.rowId === rowId &&
        pending.tombstone !== tombstone &&
        sameBytes(pending.key, key)
      ) {
        this.#pending = null; // a put and its tombstone cancel
        return;
      }
      this.#emit(pending);
    }
    this.#pending = { key: key.slice(), rowId, tombstone };
  }

  #emit(entry: { key: Uint8Array; rowId: number; tombstone: boolean }): void {
    if (entry.tombstone && this.dropTombstones) return;
    const key = entry.key;
    if (key.length > BLOCK_BYTES / 2) throw new RangeError("An index key is longer than 2 KiB");
    const size = 5 + key.length + 7;
    if (
      this.#offsets.length > 0 &&
      (this.#block.length + size + (this.#offsets.length + 1) * 2 > BLOCK_BYTES ||
        this.#offsets.length >= 0xfff0)
    )
      this.#flush();
    if (this.#offsets.length === 0) this.#firstKey = key;
    if (this.#lastBlockKey === null || !sameBytes(this.#lastBlockKey, key)) {
      const [h1, h2] = bloomProbes(key);
      this.#hashes.push(h1, h2);
      this.#lastBlockKey = key;
    }
    this.#offsets.push(this.#block.length);
    this.#block.varuint(key.length);
    this.#block.raw(key);
    this.#block.u32(entry.rowId % 2 ** 32);
    this.#block.u16(Math.floor(entry.rowId / 2 ** 32));
    this.#block.u8(entry.tombstone ? 1 : 0);
    this.#entryCount += 1;
    this.#minKey ??= key;
    this.#maxKey = key;
  }

  #flush(): void {
    const count = this.#offsets.length;
    if (count === 0) return;
    const header = 2 + count * 2;
    const out = new Uint8Array(header + this.#block.length);
    const view = new DataView(out.buffer);
    view.setUint16(0, count, true);
    this.#offsets.forEach((offset, k) => {
      view.setUint16(2 + k * 2, header + offset, true);
    });
    out.set(this.#block.bytes.subarray(0, this.#block.length), header);
    this.#blocks.push(out);
    this.#firstKeys.push(this.#firstKey ?? new Uint8Array(0));
    this.#blockHashes.push(this.#hashes);
    this.#hashes = [];
    this.#lastBlockKey = null;
    this.#block.reset();
    this.#offsets = [];
    this.#firstKey = null;
  }

  get entryCount(): number {
    return this.#entryCount;
  }

  /** The finished run, or null when nothing remains. */
  finish(): EncodedRun | null {
    if (this.#pending !== null) this.#emit(this.#pending);
    this.#pending = null;
    this.#flush();
    if (this.#entryCount === 0 || this.#minKey === null || this.#maxKey === null) return null;
    const blocks = this.#blocks;
    const partitions: Uint8Array[] = [];
    for (let first = 0; first < blocks.length; first += PARTITION_BLOCKS)
      partitions.push(encodeBloom(this.#blockHashes.slice(first, first + PARTITION_BLOCKS).flat()));
    // Parts are placed back to back from the extent's start: offsets are prefix sums.
    let offset = 0;
    const index = new ByteWriter();
    index.varuint(blocks.length);
    for (const bytes of blocks) {
      index.varuint(offset);
      index.varuint(bytes.length);
      index.u32(xxHash32(bytes));
      offset += bytes.length;
    }
    const directory = new ByteWriter();
    directory.varuint(PARTITION_BLOCKS);
    directory.varuint(partitions.length);
    for (const bytes of partitions) {
      directory.varuint(offset);
      directory.varuint(bytes.length);
      directory.u32(xxHash32(bytes));
      offset += bytes.length;
    }
    const fences = new ByteWriter();
    fences.varuint(this.#firstKeys.length);
    for (const key of this.#firstKeys) fences.blob(key);
    if (index.length > MAX_INDEX_BYTES || fences.length > MAX_INDEX_BYTES)
      throw new RangeError("An index run has more blocks than one run can index");
    return {
      parts: [...blocks, ...partitions, index.finish(), fences.finish(), directory.finish()],
      entryCount: this.#entryCount,
      minKey: this.#minKey,
      maxKey: this.#maxKey,
    };
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
export function runRef(run: EncodedRun, blobs: readonly BlobRef[], level = 0): RunRef {
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

function bloomProbes(key: Uint8Array): [number, number] {
  return [xxHash32(key, 0) >>> 0, (xxHash32(key, BLOOM_SEED) | 1) >>> 0];
}

function encodeBloom(hashes: readonly number[]): Uint8Array {
  const keys = hashes.length / 2;
  const bits = Math.max(64, Math.ceil((keys * BLOOM_BITS_PER_KEY) / 8) * 8);
  const out = new Uint8Array(5 + bits / 8);
  new DataView(out.buffer).setUint32(0, bits, true);
  out[4] = BLOOM_HASHES;
  for (let k = 0; k < hashes.length; k += 2) {
    const h1 = hashes[k] ?? 0;
    const h2 = hashes[k + 1] ?? 0;
    for (let p = 0; p < BLOOM_HASHES; p++) {
      const bit = (h1 + p * h2) % bits;
      out[5 + (bit >>> 3)] = (out[5 + (bit >>> 3)] ?? 0) | (1 << (bit & 7));
    }
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
    this.bytes =
      this.#blooms.reduce((sum, bloom) => sum + bloom.array.byteLength, 0) +
      this.#firstKeys.reduce((sum, key) => sum + key.byteLength + 48, 0) +
      fenceCount * 64;
  }

  /** Loads a run's resident parts (synchronously when they are all cached). */
  static open(space: SpaceReader, ref: RunRef, budget: ByteBudget): MaybePromise<RunReader> {
    const fences = space.peek(ref.fences);
    const index = space.peek(ref.entries);
    const directory = space.peek(ref.bloom);
    if (fences !== null && index !== null && directory !== null) {
      const { partitionBlocks, refs } = RunReader.#directory(ref, directory);
      const blooms = refs.map((blob) => space.peek(blob));
      if (blooms.every((bytes): bytes is Uint8Array => bytes !== null))
        return new RunReader(ref, fences, index, partitionBlocks, blooms);
    }
    return (async () => {
      const [fenceBytes, indexBytes, directoryBytes] = await readAll(
        space,
        [ref.fences, ref.entries, ref.bloom],
        budget,
      );
      const { partitionBlocks, refs } = RunReader.#directory(ref, need(directoryBytes));
      const blooms = await readAll(space, refs, budget);
      return new RunReader(ref, need(fenceBytes), need(indexBytes), partitionBlocks, blooms);
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

  #bloomHas(key: Uint8Array, first: number, end: number): boolean {
    const [h1, h2] = bloomProbes(key);
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
  ): MaybePromise<void> {
    if (compare(key, this.ref.minKey) < 0 || compare(key, this.ref.maxKey) > 0) return;
    const [first, end] = this.#blocksFor(key);
    if (!this.#bloomHas(key, first, end)) return;
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

/** Entry k's key range in a block, written into `located` (no allocation per probe step). */
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
