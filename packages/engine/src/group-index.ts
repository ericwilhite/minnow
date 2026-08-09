import { QueryMemoryContext, type QueryMemoryReservation } from "./memory.js";

const INITIAL_ENTRY_CAPACITY = 4;
const INITIAL_BUCKET_CAPACITY = 8;
const INITIAL_KEY_CAPACITY = 32;
const INDEX_FIELDS = 4;
const UINT32_BYTES = Uint32Array.BYTES_PER_ELEMENT;
const groupKeyEncoder = new TextEncoder();

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
    const encoded = encodeGroupKey(keys);
    const index = this.#find(encoded, hashBytes(encoded));
    return index < 0 ? undefined : this.#values[index];
  }

  getEmpty(): T | undefined {
    return this.get([]);
  }

  getOne(key: GroupIndexKey): T | undefined {
    return this.get([key]);
  }

  set(keys: readonly GroupIndexKey[], value: T): void {
    const encoded = encodeGroupKey(keys);
    const hash = hashBytes(encoded);
    const existing = this.#find(encoded, hash);
    if (existing >= 0) {
      this.#values[existing] = value;
      return;
    }
    this.#insert(encoded, hash, value);
  }

  getOrInsert(keys: readonly GroupIndexKey[], create: () => T): T {
    const encoded = encodeGroupKey(keys);
    const hash = hashBytes(encoded);
    const existing = this.#find(encoded, hash);
    if (existing >= 0) return this.#values[existing] as T;
    const value = create();
    this.#insert(encoded, hash, value);
    return value;
  }

  getOrInsertOne(key: GroupIndexKey, create: () => T): T {
    return this.getOrInsert([key], create);
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

  #find(encoded: Uint8Array, hash: number): number {
    if (this.#buckets.length === 0) return -1;
    let index = this.#buckets[hash & (this.#buckets.length - 1)] ?? -1;
    while (index >= 0) {
      if (
        this.#hashes[index] === hash &&
        this.#lengths[index] === encoded.byteLength &&
        equalBytes(this.#keys, this.#offsets[index] ?? 0, encoded)
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

function encodeGroupKey(keys: readonly GroupIndexKey[]): Uint8Array {
  const stringPayloads = new Map<number, Uint8Array>();
  let byteLength = 0;
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    if (key === null || typeof key === "boolean") {
      byteLength = safeSum(byteLength, 1, "Encoded group key");
    } else if (typeof key === "number") {
      if (!Number.isFinite(key)) throw new TypeError("Group index numbers must be finite");
      byteLength = safeSum(byteLength, 1 + Float64Array.BYTES_PER_ELEMENT, "Encoded group key");
    } else {
      const payload = groupKeyEncoder.encode(key);
      if (payload.byteLength > 0xffff_ffff) throw new RangeError("Group index string is too large");
      stringPayloads.set(index, payload);
      byteLength = safeSum(byteLength, 5 + payload.byteLength, "Encoded group key");
    }
  }
  if (byteLength > 0xffff_ffff) throw new RangeError("Encoded group key is too large");

  const encoded = new Uint8Array(byteLength);
  const view = new DataView(encoded.buffer);
  let offset = 0;
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    if (key === null) {
      encoded[offset] = 0;
      offset += 1;
    } else if (typeof key === "boolean") {
      encoded[offset] = key ? 2 : 1;
      offset += 1;
    } else if (typeof key === "number") {
      encoded[offset] = 3;
      view.setFloat64(offset + 1, key === 0 ? 0 : key, true);
      offset += 1 + Float64Array.BYTES_PER_ELEMENT;
    } else {
      const payload = stringPayloads.get(index);
      if (payload === undefined) throw new Error("Encoded group string is missing");
      encoded[offset] = 4;
      view.setUint32(offset + 1, payload.byteLength, true);
      encoded.set(payload, offset + 5);
      offset += 5 + payload.byteLength;
    }
  }
  return encoded;
}

function hashBytes(bytes: Uint8Array): number {
  let hash = 0x811c9dc5;
  for (const byte of bytes) hash = Math.imul(hash ^ byte, 0x01000193) >>> 0;
  return hash;
}

function equalBytes(arena: Uint8Array, offset: number, value: Uint8Array): boolean {
  for (let index = 0; index < value.byteLength; index += 1) {
    if (arena[offset + index] !== value[index]) return false;
  }
  return true;
}

function safeDouble(value: number, label: string): number {
  const doubled = value * 2;
  if (!Number.isSafeInteger(doubled))
    throw new RangeError(`${label} exceeds the safe integer range`);
  return doubled;
}

function safeProduct(left: number, right: number, label: string): number {
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
