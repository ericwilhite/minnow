/**
 * Persisted key-index runs (format/manifest.ts `RunRef`; access/key-index.ts "Structure"):
 * sorted `(key, rowId, put | tombstone)` entries, written once, read through the extent space's
 * verified cache. Runs carry no seq: a run is visible to every view that pins its root (ruling).
 *
 * ## Stored form (one extent per run, every blob ≤ 1 MiB)
 * - **Blocks** of about `BLOCK_BYTES`: `u16 count`, `u16 offsets[count]` (each entry's start in the
 *   block), then entries `varuint keyLength, key, u32 rowId low, u16 rowId high, u8 flags`
 *   (bit 0 = tombstone). Entries are in (key bytes, rowId) order, so a block is binary-searched in
 *   place, never decoded.
 * - **Fences** (`RunRef.fences`): each block's first key: `varuint count`, then
 *   `varuint length, key` per block.
 * - **Block index** (`RunRef.entries`): `varuint count`, then per block
 *   `varuint offset, varuint length, u32 checksum` within the run's extent.
 * - **Bloom** (`RunRef.bloom`): `u32 bits, u8 hashes`, then the bit array; about 10 bits per key,
 *   7 probes, double hashing over xxHash32 with two seeds.
 *
 * ## Reading
 * A `RunReader` holds the fences, block refs and Bloom filter resident (≈1.4 B per key: the
 * resident heap budget is ≤ 4 B per keyed row). A lookup costs a Bloom probe, a binary search of
 * the fences, and a binary search inside one block (or a few, when one key's entries continue into
 * the next block). Blocks come from the cache synchronously (`peek`) when resident; otherwise the
 * lookup returns a Promise and reads the block (a lease charged to the caller's budget only while
 * the block is searched).
 */
import type { BlobRef, RunRef } from "../format/manifest.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { StoreCorruptionError } from "../storage/contract.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import { compareBytes } from "../vector/varlen.js";
import { ByteReader, ByteWriter } from "./bytes.js";
import type { MaybePromise } from "../access/key-index.js";
import { need } from "./bytes.js";

export const BLOCK_BYTES = 4096;
const BLOOM_BITS_PER_KEY = 10;
const BLOOM_HASHES = 7;
const BLOOM_SEED = 0x9747b28c;

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
export interface EncodedRun {
  /** Blocks, then the block index, fences and Bloom filter (always the last three parts). */
  readonly parts: readonly Uint8Array[];
  readonly entryCount: number;
  readonly minKey: Uint8Array;
  readonly maxKey: Uint8Array;
}

/**
 * Encodes sorted keys (byte order; entries per key in row-id order) as a run. A put and a
 * tombstone of the same row id in the input cancel (the row was inserted and deleted within the
 * layer); a tombstone without its put is kept (its put is in an older layer). Returns null when
 * nothing remains.
 */
export function encodeRun(keys: readonly SortedKey[]): EncodedRun | null {
  const blocks: Uint8Array[] = [];
  const firstKeys: Uint8Array[] = [];
  let entryCount = 0;
  let keyCount = 0;
  const block = new ByteWriter(BLOCK_BYTES + 512);
  let offsets: number[] = [];
  let firstKey: Uint8Array | null = null;
  const bloomKeys: Uint8Array[] = [];
  const flush = () => {
    if (offsets.length === 0) return;
    const header = 2 + offsets.length * 2;
    const out = new Uint8Array(header + block.length);
    const view = new DataView(out.buffer);
    view.setUint16(0, offsets.length, true);
    offsets.forEach((offset, k) => {
      view.setUint16(2 + k * 2, header + offset, true);
    });
    out.set(block.bytes.subarray(0, block.length), header);
    blocks.push(out);
    firstKeys.push(firstKey ?? new Uint8Array(0));
    block.reset();
    offsets = [];
    firstKey = null;
  };
  let minKey: Uint8Array | null = null;
  let maxKey: Uint8Array | null = null;
  for (const { key, entries } of keys) {
    const kept = cancel(entries);
    if (kept.length === 0) continue;
    if (key.length > BLOCK_BYTES / 2) throw new RangeError("An index key is longer than 2 KiB");
    keyCount += 1;
    bloomKeys.push(key);
    minKey ??= key;
    maxKey = key;
    for (const entry of kept) {
      const size = 5 + key.length + 7;
      if (
        offsets.length > 0 &&
        (block.length + size + (offsets.length + 1) * 2 > BLOCK_BYTES || offsets.length >= 0xfff0)
      )
        flush();
      if (offsets.length === 0) firstKey = key;
      offsets.push(block.length);
      block.varuint(key.length);
      block.raw(key);
      block.u32(entry.rowId % 2 ** 32);
      block.u16(Math.floor(entry.rowId / 2 ** 32));
      block.u8(entry.tombstone ? 1 : 0);
      entryCount += 1;
    }
  }
  flush();
  if (entryCount === 0 || minKey === null || maxKey === null) return null;
  const fences = new ByteWriter();
  fences.varuint(firstKeys.length);
  for (const key of firstKeys) fences.blob(key);
  const index = new ByteWriter();
  index.varuint(blocks.length);
  let offset = 0;
  for (const bytes of blocks) {
    index.varuint(offset);
    index.varuint(bytes.length);
    index.u32(xxHash32(bytes));
    offset += bytes.length;
  }
  const bloom = encodeBloom(bloomKeys, keyCount);
  return {
    parts: [...blocks, index.finish(), fences.finish(), bloom],
    entryCount,
    minKey: minKey.slice(),
    maxKey: maxKey.slice(),
  };
}

function cancel(entries: readonly RunEntry[]): readonly RunEntry[] {
  // One entry (the common case) has nothing to cancel.
  if (entries.length === 1) return entries;
  const puts = new Set<number>();
  const tombstones = new Set<number>();
  for (const entry of entries) (entry.tombstone ? tombstones : puts).add(entry.rowId);
  return entries.filter((entry) =>
    entry.tombstone ? !puts.has(entry.rowId) : !tombstones.has(entry.rowId),
  );
}

/** The `RunRef` of an encoded run once `ExtentWriter.write(run.parts)` returned its blobs. */
export function runRef(run: EncodedRun, blobs: readonly BlobRef[], level = 0): RunRef {
  const count = blobs.length;
  const at = (k: number): BlobRef => {
    const blob = blobs[k];
    if (blob === undefined) throw new RangeError("A run's blobs are missing");
    return blob;
  };
  return {
    level,
    entryCount: run.entryCount,
    entries: at(count - 3),
    fences: at(count - 2),
    bloom: at(count - 1),
    minKey: run.minKey,
    maxKey: run.maxKey,
  };
}

function bloomProbes(key: Uint8Array): [number, number] {
  return [xxHash32(key, 0) >>> 0, (xxHash32(key, BLOOM_SEED) | 1) >>> 0];
}

function encodeBloom(keys: readonly Uint8Array[], count: number): Uint8Array {
  const bits = Math.max(64, Math.ceil((count * BLOOM_BITS_PER_KEY) / 8) * 8);
  const out = new Uint8Array(5 + bits / 8);
  new DataView(out.buffer).setUint32(0, bits, true);
  out[4] = BLOOM_HASHES;
  for (const key of keys) {
    const [h1, h2] = bloomProbes(key);
    for (let k = 0; k < BLOOM_HASHES; k++) {
      const bit = (h1 + k * h2) % bits;
      out[5 + (bit >>> 3)] = (out[5 + (bit >>> 3)] ?? 0) | (1 << (bit & 7));
    }
  }
  return out;
}

/** A run's resident parts: fences, block refs and Bloom filter. */
export class RunReader {
  readonly #firstKeys: Uint8Array[];
  readonly #blocks: BlobRef[];
  readonly #bloom: Uint8Array;
  readonly #bits: number;
  readonly #hashes: number;
  /** Resident bytes (charged by the owner of the reader). */
  readonly bytes: number;

  private constructor(
    readonly ref: RunRef,
    fences: Uint8Array,
    index: Uint8Array,
    bloom: Uint8Array,
  ) {
    const fenceReader = new ByteReader(fences);
    const fenceCount = fenceReader.varuint();
    this.#firstKeys = Array.from({ length: fenceCount }, () => fenceReader.blob().slice());
    fenceReader.end();
    const indexReader = new ByteReader(index);
    const blockCount = indexReader.varuint();
    if (blockCount !== fenceCount)
      throw new StoreCorruptionError("A run's fences and blocks differ");
    const extent = ref.entries.extent;
    this.#blocks = Array.from({ length: blockCount }, () => ({
      extent,
      offset: indexReader.varuint(),
      length: indexReader.varuint(),
      checksum: indexReader.u32(),
    }));
    indexReader.end();
    const bloomView = new DataView(bloom.buffer, bloom.byteOffset, bloom.byteLength);
    this.#bits = bloomView.getUint32(0, true);
    this.#hashes = bloom[4] ?? 0;
    if (this.#bits === 0 || bloom.length !== 5 + this.#bits / 8)
      throw new StoreCorruptionError("A run's Bloom filter is malformed");
    this.#bloom = bloom.slice(5);
    this.bytes =
      this.#bloom.byteLength +
      this.#firstKeys.reduce((sum, key) => sum + key.byteLength + 48, 0) +
      blockCount * 64;
  }

  /** Loads a run's resident parts (synchronously when they are cached). */
  static open(space: SpaceReader, ref: RunRef, budget: ByteBudget): MaybePromise<RunReader> {
    const refs = [ref.fences, ref.entries, ref.bloom];
    const cached = refs.map((blob) => space.peek(blob));
    if (cached.every((bytes) => bytes !== null))
      return new RunReader(ref, need(cached[0]), need(cached[1]), need(cached[2]));
    return space.read(refs, { budget }).then((lease) => {
      try {
        const [fences, index, bloom] = lease.bytes;
        if (fences === undefined || index === undefined || bloom === undefined)
          throw new StoreCorruptionError("A run's parts are missing");
        return new RunReader(ref, fences, index, bloom);
      } finally {
        lease.release();
      }
    });
  }

  get blockCount(): number {
    return this.#blocks.length;
  }

  mayContain(key: Uint8Array): boolean {
    if (compare(key, this.ref.minKey) < 0 || compare(key, this.ref.maxKey) > 0) return false;
    const [h1, h2] = bloomProbes(key);
    for (let k = 0; k < this.#hashes; k++) {
      const bit = (h1 + k * h2) % this.#bits;
      if (((this.#bloom[bit >>> 3] ?? 0) & (1 << (bit & 7))) === 0) return false;
    }
    return true;
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
    if (!this.mayContain(key)) return;
    const [first, end] = this.#blocksFor(key);
    const missing: BlobRef[] = [];
    const blocks: Array<Uint8Array | null> = [];
    for (let block = first; block < end; block++) {
      const ref = need(this.#blocks[block]);
      const bytes = space.peek(ref);
      blocks.push(bytes);
      if (bytes === null) missing.push(ref);
    }
    if (missing.length === 0) {
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

  /** Every entry whose key satisfies `accept`, scanning blocks `[from, to)` in order (async). */
  async scan(
    space: SpaceReader,
    budget: ByteBudget,
    accept: (key: Uint8Array) => -1 | 0 | 1,
    visit: (key: Uint8Array, entry: RunEntry) => void,
  ): Promise<void> {
    for (let block = 0; block < this.#blocks.length; block++) {
      const next = this.#firstKeys[block + 1];
      // Skip blocks entirely below the range.
      if (next !== undefined && accept(next) < 0) continue;
      const first = need(this.#firstKeys[block]);
      if (accept(first) > 0) return;
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
        const stopped = forEachEntry(bytes, (key, entry) => {
          const position = accept(key);
          if (position === 0) visit(key.slice(), entry);
          return position > 0;
        });
        if (stopped) return;
      } finally {
        release?.();
      }
    }
  }
}

/** Entry k's key range in a block, written into `out` (no allocation per probe step). */
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

/** Visits a block's entries in order until `visit` returns true; returns whether it stopped. */
function forEachEntry(
  block: Uint8Array,
  visit: (key: Uint8Array, entry: RunEntry) => boolean,
): boolean {
  const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
  const count = view.getUint16(0, true);
  for (let k = 0; k < count; k++) {
    const { keyStart, keyEnd } = locate(block, view, k);
    const stop = visit(block.subarray(keyStart, keyEnd), {
      rowId: rowIdAt(view, keyEnd),
      tombstone: ((block[keyEnd + 6] ?? 0) & 1) === 1,
    });
    if (stop) return true;
  }
  return false;
}
