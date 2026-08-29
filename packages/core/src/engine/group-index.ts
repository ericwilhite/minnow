import { QueryMemoryContext, type QueryMemoryReservation } from "./memory.js";

const INITIAL_ENTRY_CAPACITY = 4;
const INITIAL_BUCKET_CAPACITY = 8;
const INITIAL_KEY_CAPACITY = 32;
const INDEX_FIELDS = 4;
const UINT32_BYTES = Uint32Array.BYTES_PER_ELEMENT;
const INITIAL_SCRATCH_CAPACITY = 64;

// Every probe encodes its key into this shared scratch arena instead of allocating a fresh buffer
// per row. Encoding is synchronous and the bytes are consumed before the next probe begins, so a
// single arena serves every index; the miss path copies out of it before any caller code runs.
let scratch = new Uint8Array(INITIAL_SCRATCH_CAPACITY);
let scratchView = new DataView(scratch.buffer);

// The arena grows to fit the largest key ever encoded, so a single pathological key (a
// multi-megabyte string) would otherwise pin that memory for the life of the process — hostile
// to low-end devices. Each fresh encoding starts by shedding anything beyond the retention cap;
// ordinary keys never reach it, so the check is one branch per operation.
const SCRATCH_RETAIN_BYTES = 1024 * 1024;

function reclaimScratch(): void {
  if (scratch.byteLength <= SCRATCH_RETAIN_BYTES) return;
  scratch = new Uint8Array(SCRATCH_RETAIN_BYTES);
  scratchView = new DataView(scratch.buffer);
}

export type GroupIndexKey = null | boolean | number | string;

/** Byte-addressable, insertion-ordered grouping index with fully reserved typed storage. */
export class ByteGroupIndex<T> {
  readonly #memory: QueryMemoryContext;
  readonly #values: T[] = [];
  #buckets = new Int32Array(0);
  #hashes = new Uint32Array(0);
  #offsets = new Uint32Array(0);
  #lengths = new Uint32Array(0);
  #next = new Int32Array(0);
  #keys = new Uint8Array(0);
  #keyLength = 0;
  #entryReservation: QueryMemoryReservation | undefined;
  #bucketReservation: QueryMemoryReservation | undefined;
  #keyReservation: QueryMemoryReservation | undefined;

  constructor(memory: QueryMemoryContext) {
    this.#memory = memory;
  }

  get size(): number {
    return this.#values.length;
  }

  get(keys: readonly GroupIndexKey[]): T | undefined {
    const length = encodeGroupKey(keys);
    const index = this.#find(length, hashScratch(length));
    return index < 0 ? undefined : this.#values[index];
  }

  getEmpty(): T | undefined {
    return this.get([]);
  }

  getOne(key: GroupIndexKey): T | undefined {
    const length = encodeSingleScalarKey(key);
    const index = this.#find(length, hashScratch(length));
    return index < 0 ? undefined : this.#values[index];
  }

  set(keys: readonly GroupIndexKey[], value: T): void {
    const length = encodeGroupKey(keys);
    const hash = hashScratch(length);
    const existing = this.#find(length, hash);
    if (existing >= 0) {
      this.#values[existing] = value;
      return;
    }
    this.#insert(scratch.slice(0, length), hash, value);
  }

  getOrInsert(keys: readonly GroupIndexKey[], create: () => T): T {
    const length = encodeGroupKey(keys);
    return this.#getOrInsertScratch(length, create);
  }

  getOrInsertOne(key: GroupIndexKey, create: () => T): T {
    const length = encodeSingleScalarKey(key);
    return this.#getOrInsertScratch(length, create);
  }

  /**
   * Resolves the key currently held in the scratch arena. A hit touches no allocation at all; a
   * miss — at most one per distinct group — copies the key out before `create` runs, so caller
   * code can never observe or disturb the shared arena.
   */
  #getOrInsertScratch(length: number, create: () => T): T {
    const hash = hashScratch(length);
    const existing = this.#find(length, hash);
    if (existing >= 0) return this.#values[existing] as T;
    const encoded = scratch.slice(0, length);
    const value = create();
    this.#insert(encoded, hash, value);
    return value;
  }

  #insert(encoded: Uint8Array, hash: number, value: T): void {
    const index = this.#values.length;
    this.#ensureEntryCapacity(index + 1);
    this.#ensureBucketCapacity(index + 1);
    this.#ensureKeyCapacity(encoded.byteLength);
    const offset = this.#keyLength;
    this.#keys.set(encoded, offset);
    this.#keyLength += encoded.byteLength;
    this.#hashes[index] = hash;
    this.#offsets[index] = offset;
    this.#lengths[index] = encoded.byteLength;
    const bucket = hash & (this.#buckets.length - 1);
    this.#next[index] = this.#buckets[bucket] ?? -1;
    this.#buckets[bucket] = index;
    this.#values.push(value);
  }

  setEmpty(value: T): void {
    this.set([], value);
  }

  setOne(key: GroupIndexKey, value: T): void {
    this.set([key], value);
  }

  values(): readonly T[] {
    return this.#values;
  }

  /** Matches the scratch arena's first `length` bytes against stored keys without copying them. */
  #find(length: number, hash: number): number {
    if (this.#buckets.length === 0) return -1;
    let index = this.#buckets[hash & (this.#buckets.length - 1)] ?? -1;
    while (index >= 0) {
      if (
        this.#hashes[index] === hash &&
        this.#lengths[index] === length &&
        equalsScratch(this.#keys, this.#offsets[index] ?? 0, length)
      ) {
        return index;
      }
      index = this.#next[index] ?? -1;
    }
    return -1;
  }

  #ensureEntryCapacity(required: number): void {
    if (required <= this.#hashes.length) return;
    let capacity = Math.max(INITIAL_ENTRY_CAPACITY, this.#hashes.length * 2);
    while (capacity < required) capacity = safeDouble(capacity, "Group index entries");
    const reservation = this.#memory.reserve(
      safeProduct(capacity, INDEX_FIELDS * UINT32_BYTES, "Group index entry storage"),
      "Group index entry storage",
    );
    try {
      const hashes = new Uint32Array(capacity);
      const offsets = new Uint32Array(capacity);
      const lengths = new Uint32Array(capacity);
      const next = new Int32Array(capacity);
      next.fill(-1);
      hashes.set(this.#hashes);
      offsets.set(this.#offsets);
      lengths.set(this.#lengths);
      next.set(this.#next);
      this.#hashes = hashes;
      this.#offsets = offsets;
      this.#lengths = lengths;
      this.#next = next;
      this.#entryReservation?.release();
      this.#entryReservation = reservation;
    } catch (error) {
      reservation.release();
      throw error;
    }
  }

  #ensureBucketCapacity(requiredEntries: number): void {
    let capacity = this.#buckets.length;
    if (capacity === 0) capacity = INITIAL_BUCKET_CAPACITY;
    while (requiredEntries * 4 > capacity * 3) {
      capacity = safeDouble(capacity, "Group index buckets");
    }
    if (capacity === this.#buckets.length) return;
    const reservation = this.#memory.reserve(
      safeProduct(capacity, Int32Array.BYTES_PER_ELEMENT, "Group index bucket storage"),
      "Group index bucket storage",
    );
    try {
      const buckets = new Int32Array(capacity);
      buckets.fill(-1);
      for (let index = 0; index < this.#values.length; index += 1) {
        const bucket = (this.#hashes[index] ?? 0) & (capacity - 1);
        this.#next[index] = buckets[bucket] ?? -1;
        buckets[bucket] = index;
      }
      this.#buckets = buckets;
      this.#bucketReservation?.release();
      this.#bucketReservation = reservation;
    } catch (error) {
      reservation.release();
      throw error;
    }
  }

  #ensureKeyCapacity(additionalBytes: number): void {
    const required = this.#keyLength + additionalBytes;
    if (!Number.isSafeInteger(required)) throw new RangeError("Group key arena is too large");
    if (required > 0xffff_ffff) throw new RangeError("Group key arena exceeds uint32 offsets");
    if (required <= this.#keys.byteLength) return;
    let capacity = Math.max(INITIAL_KEY_CAPACITY, this.#keys.byteLength * 2);
    while (capacity < required) capacity = safeDouble(capacity, "Group key arena");
    const reservation = this.#memory.reserve(capacity, "Group key arena");
    try {
      const keys = new Uint8Array(capacity);
      keys.set(this.#keys.subarray(0, this.#keyLength));
      this.#keys = keys;
      this.#keyReservation?.release();
      this.#keyReservation = reservation;
    } catch (error) {
      reservation.release();
      throw error;
    }
  }
}

/**
 * Writes the canonical tagged encoding of `keys` into the scratch arena and returns its byte
 * length. The byte layout is unchanged from the allocating encoder it replaces — a one-byte tag
 * per key, little-endian float64 numbers, and length-prefixed UTF-8 strings — so stored keys and
 * their hashes stay comparable across both paths.
 */
function encodeGroupKey(keys: readonly GroupIndexKey[]): number {
  reclaimScratch();
  let offset = 0;
  for (const key of keys) offset = writeGroupKey(key, offset);
  return offset;
}

/**
 * Encodes one scalar key into the shared scratch arena and returns its byte length. Exported for
 * the join index, which shares the arena and byte layout: both consume the encoding synchronously
 * before any other key operation can run, so a single arena serves every index. NaN is the one
 * excluded number — group callers canonicalize non-finite to null, join callers skip NaN keys —
 * while ±Infinity encodes as an ordinary float64 so computed join keys can overflow and still match.
 */
export function encodeSingleScalarKey(key: GroupIndexKey): number {
  reclaimScratch();
  return writeGroupKey(key, 0);
}

function writeGroupKey(key: GroupIndexKey, offset: number): number {
  if (key === null) {
    ensureScratchCapacity(offset + 1);
    scratch[offset] = 0;
    return offset + 1;
  }
  if (typeof key === "boolean") {
    ensureScratchCapacity(offset + 1);
    scratch[offset] = key ? 2 : 1;
    return offset + 1;
  }
  if (typeof key === "number") {
    if (Number.isNaN(key)) throw new TypeError("Scalar index keys cannot be NaN");
    ensureScratchCapacity(offset + 1 + Float64Array.BYTES_PER_ELEMENT);
    scratch[offset] = 3;
    scratchView.setFloat64(offset + 1, key === 0 ? 0 : key, true);
    return offset + 1 + Float64Array.BYTES_PER_ELEMENT;
  }
  // A code unit never expands past three UTF-8 bytes. Reserving the one conservative bound keeps
  // this query hot path single-pass and branch-light; oversized scratch is reclaimed before the
  // next key operation.
  const bound = safeSum(
    offset + 5,
    safeProduct(key.length, 3, "Encoded group key"),
    "Encoded group key",
  );
  if (bound > 0xffff_ffff) throw new RangeError("Encoded group key is too large");
  ensureScratchCapacity(bound);
  scratch[offset] = 4;
  const payloadStart = offset + 5;
  const payloadEnd = writeUtf8(key, payloadStart);
  scratchView.setUint32(offset + 1, payloadEnd - payloadStart, true);
  return payloadEnd;
}

/**
 * Encodes `value` as UTF-8 in one pass. Isolated surrogates fail before the encoded bytes can
 * participate in a lookup or insertion.
 */
function writeUtf8(value: string, offset: number): number {
  let write = offset;
  for (let index = 0; index < value.length; index += 1) {
    let code = value.charCodeAt(index);
    if (code < 0x80) {
      scratch[write] = code;
      write += 1;
      continue;
    }
    if (code < 0x800) {
      scratch[write] = 0xc0 | (code >>> 6);
      scratch[write + 1] = 0x80 | (code & 0x3f);
      write += 2;
      continue;
    }
    if (code >= 0xd800 && code <= 0xdbff) {
      const low = index + 1 < value.length ? value.charCodeAt(index + 1) : 0;
      if (low >= 0xdc00 && low <= 0xdfff) {
        code = 0x10000 + ((code - 0xd800) << 10) + (low - 0xdc00);
        index += 1;
        scratch[write] = 0xf0 | (code >>> 18);
        scratch[write + 1] = 0x80 | ((code >>> 12) & 0x3f);
        scratch[write + 2] = 0x80 | ((code >>> 6) & 0x3f);
        scratch[write + 3] = 0x80 | (code & 0x3f);
        write += 4;
        continue;
      }
    }
    if (code >= 0xd800 && code <= 0xdfff) {
      throw new TypeError(
        `String value contains an unpaired surrogate at UTF-16 index ${String(index)}`,
      );
    }
    scratch[write] = 0xe0 | (code >>> 12);
    scratch[write + 1] = 0x80 | ((code >>> 6) & 0x3f);
    scratch[write + 2] = 0x80 | (code & 0x3f);
    write += 3;
  }
  return write;
}

function ensureScratchCapacity(required: number): void {
  if (required <= scratch.byteLength) return;
  let capacity = scratch.byteLength;
  while (capacity < required) capacity = safeDouble(capacity, "Group key scratch arena");
  const grown = new Uint8Array(capacity);
  // Earlier keys of a compound encoding already occupy the arena, so growth carries them over.
  grown.set(scratch);
  scratch = grown;
  scratchView = new DataView(scratch.buffer);
}

/** FNV-1a over the scratch arena's first `length` bytes. Exported for the join index. */
export function hashScratch(length: number): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < length; index += 1) {
    hash = Math.imul(hash ^ (scratch[index] ?? 0), 0x01000193) >>> 0;
  }
  return hash;
}

/** Matches `arena[offset..offset+length)` against the scratch arena. Exported for the join index. */
export function equalsScratch(arena: Uint8Array, offset: number, length: number): boolean {
  for (let index = 0; index < length; index += 1) {
    if (arena[offset + index] !== scratch[index]) return false;
  }
  return true;
}

/** Copies the scratch arena's first `length` bytes into owned storage. Exported for the join index. */
export function copyScratchKey(length: number): Uint8Array {
  return scratch.slice(0, length);
}

export function safeDouble(value: number, label: string): number {
  const doubled = value * 2;
  if (!Number.isSafeInteger(doubled))
    throw new RangeError(`${label} exceeds the safe integer range`);
  return doubled;
}

export function safeProduct(left: number, right: number, label: string): number {
  const product = left * right;
  if (!Number.isSafeInteger(product))
    throw new RangeError(`${label} exceeds the safe integer range`);
  return product;
}

function safeSum(left: number, right: number, label: string): number {
  const sum = left + right;
  if (!Number.isSafeInteger(sum)) throw new RangeError(`${label} exceeds the safe integer range`);
  return sum;
}
