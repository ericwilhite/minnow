import { QueryMemoryContext, type QueryMemoryReservation } from "./memory.js";

const INITIAL_ENTRY_CAPACITY = 4;
const INITIAL_BUCKET_CAPACITY = 8;
const INITIAL_KEY_CAPACITY = 32;
const ENTRY_FIELDS = 6;
const UINT32_BYTES = Uint32Array.BYTES_PER_ELEMENT;
const joinKeyEncoder = new TextEncoder();

export type JoinIndexKey = boolean | number | string | Date;

/** Collision-checked scalar-key hash index with typed duplicate row chains. */
export class ByteJoinIndex {
  readonly #memory: QueryMemoryContext;
  readonly #rowNext: Int32Array;
  #buckets = new Int32Array(0);
  #hashes = new Uint32Array(0);
  #offsets = new Uint32Array(0);
  #lengths = new Uint32Array(0);
  #bucketNext = new Int32Array(0);
  #rowHeads = new Int32Array(0);
  #rowTails = new Int32Array(0);
  #keys = new Uint8Array(0);
  #keyLength = 0;
  #entryCount = 0;
  #unique = true;
  #entryReservation: QueryMemoryReservation | undefined;
  #bucketReservation: QueryMemoryReservation | undefined;
  #keyReservation: QueryMemoryReservation | undefined;

  constructor(memory: QueryMemoryContext, rowCapacity: number) {
    if (!Number.isSafeInteger(rowCapacity) || rowCapacity < 0) {
      throw new RangeError("Join index row capacity must be a non-negative safe integer");
    }
    this.#memory = memory;
    memory.reserve(
      safeProduct(rowCapacity, Int32Array.BYTES_PER_ELEMENT, "Join duplicate row chains"),
      "Join duplicate row chains",
    );
    this.#rowNext = new Int32Array(rowCapacity);
    this.#rowNext.fill(-1);
  }

  get unique(): boolean {
    return this.#unique;
  }

  add(value: unknown, row: number): void {
    if (!Number.isSafeInteger(row) || row < 0 || row >= this.#rowNext.length) {
      throw new RangeError("Join index row is outside its declared capacity");
    }
    const encoded = encodeJoinKey(value);
    if (encoded === undefined) return;
    const hash = hashBytes(encoded);
    const existing = this.#find(encoded, hash);
    if (existing >= 0) {
      const tail = this.#rowTails[existing] ?? -1;
      if (tail < 0) throw new Error("Join index row chain is corrupt");
      this.#rowNext[tail] = row;
      this.#rowTails[existing] = row;
      this.#unique = false;
      return;
    }

    const index = this.#entryCount;
    this.#ensureEntryCapacity(index + 1);
    this.#ensureBucketCapacity(index + 1);
    this.#ensureKeyCapacity(encoded.byteLength);
    const offset = this.#keyLength;
    this.#keys.set(encoded, offset);
    this.#keyLength += encoded.byteLength;
    this.#hashes[index] = hash;
    this.#offsets[index] = offset;
    this.#lengths[index] = encoded.byteLength;
    this.#rowHeads[index] = row;
    this.#rowTails[index] = row;
    const bucket = hash & (this.#buckets.length - 1);
    this.#bucketNext[index] = this.#buckets[bucket] ?? -1;
    this.#buckets[bucket] = index;
    this.#entryCount += 1;
  }

  firstRow(value: unknown): number {
    const encoded = encodeJoinKey(value);
    if (encoded === undefined) return -1;
    const index = this.#find(encoded, hashBytes(encoded));
    return index < 0 ? -1 : (this.#rowHeads[index] ?? -1);
  }

  nextRow(row: number): number {
    return row < 0 || row >= this.#rowNext.length ? -1 : (this.#rowNext[row] ?? -1);
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
      index = this.#bucketNext[index] ?? -1;
    }
    return -1;
  }

  #ensureEntryCapacity(required: number): void {
    if (required <= this.#hashes.length) return;
    let capacity = Math.max(INITIAL_ENTRY_CAPACITY, this.#hashes.length * 2);
    while (capacity < required) capacity = safeDouble(capacity, "Join index entries");
    const reservation = this.#memory.reserve(
      safeProduct(capacity, ENTRY_FIELDS * UINT32_BYTES, "Join index entry storage"),
      "Join index entry storage",
    );
    try {
      const hashes = new Uint32Array(capacity);
      const offsets = new Uint32Array(capacity);
      const lengths = new Uint32Array(capacity);
      const bucketNext = new Int32Array(capacity);
      const rowHeads = new Int32Array(capacity);
      const rowTails = new Int32Array(capacity);
      bucketNext.fill(-1);
      rowHeads.fill(-1);
      rowTails.fill(-1);
      hashes.set(this.#hashes);
      offsets.set(this.#offsets);
      lengths.set(this.#lengths);
      bucketNext.set(this.#bucketNext);
      rowHeads.set(this.#rowHeads);
      rowTails.set(this.#rowTails);
      this.#hashes = hashes;
      this.#offsets = offsets;
      this.#lengths = lengths;
      this.#bucketNext = bucketNext;
      this.#rowHeads = rowHeads;
      this.#rowTails = rowTails;
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
      capacity = safeDouble(capacity, "Join index buckets");
    }
    if (capacity === this.#buckets.length) return;
    const reservation = this.#memory.reserve(
      safeProduct(capacity, Int32Array.BYTES_PER_ELEMENT, "Join index bucket storage"),
      "Join index bucket storage",
    );
    try {
      const buckets = new Int32Array(capacity);
      buckets.fill(-1);
      for (let index = 0; index < this.#entryCount; index += 1) {
        const bucket = (this.#hashes[index] ?? 0) & (capacity - 1);
        this.#bucketNext[index] = buckets[bucket] ?? -1;
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
    if (!Number.isSafeInteger(required)) throw new RangeError("Join key arena is too large");
    if (required > 0xffff_ffff) throw new RangeError("Join key arena exceeds uint32 offsets");
    if (required <= this.#keys.byteLength) return;
    let capacity = Math.max(INITIAL_KEY_CAPACITY, this.#keys.byteLength * 2);
    while (capacity < required) capacity = safeDouble(capacity, "Join key arena");
    const reservation = this.#memory.reserve(capacity, "Join key arena");
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

function encodeJoinKey(value: unknown): Uint8Array | undefined {
  const key = value instanceof Date ? value.getTime() : value;
  if (key === null || key === undefined) return undefined;
  if (typeof key === "boolean") return Uint8Array.of(key ? 2 : 1);
  if (typeof key === "number") {
    if (Number.isNaN(key)) return undefined;
    const encoded = new Uint8Array(1 + Float64Array.BYTES_PER_ELEMENT);
    encoded[0] = 3;
    new DataView(encoded.buffer).setFloat64(1, key === 0 ? 0 : key, true);
    return encoded;
  }
  if (typeof key === "string") {
    const payload = joinKeyEncoder.encode(key);
    if (payload.byteLength > 0xffff_ffff) throw new RangeError("Join index string is too large");
    const encoded = new Uint8Array(5 + payload.byteLength);
    encoded[0] = 4;
    new DataView(encoded.buffer).setUint32(1, payload.byteLength, true);
    encoded.set(payload, 5);
    return encoded;
  }
  throw new TypeError("Join keys must be SQL scalar values");
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
  if (!Number.isSafeInteger(doubled)) {
    throw new RangeError(`${label} exceeds the safe integer range`);
  }
  return doubled;
}

function safeProduct(left: number, right: number, label: string): number {
  const product = left * right;
  if (!Number.isSafeInteger(product)) {
    throw new RangeError(`${label} exceeds the safe integer range`);
  }
  return product;
}
