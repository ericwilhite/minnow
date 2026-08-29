import { dateMilliseconds } from "../date-value.js";
import {
  copyScratchKey,
  encodeSingleScalarKey,
  equalsScratch,
  hashScratch,
} from "./group-index.js";
import { QueryMemoryContext, type QueryMemoryReservation } from "./memory.js";

const INITIAL_ENTRY_CAPACITY = 4;
const INITIAL_BUCKET_CAPACITY = 8;
const INITIAL_KEY_CAPACITY = 32;
const ENTRY_FIELDS = 6;
const UINT32_BYTES = Uint32Array.BYTES_PER_ELEMENT;

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
    const length = encodeJoinKey(value);
    if (length < 0) return;
    const hash = hashScratch(length);
    const existing = this.#find(length, hash);
    if (existing >= 0) {
      const tail = this.#rowTails[existing] ?? -1;
      if (tail < 0) throw new Error("Join index row chain is corrupt");
      this.#rowNext[tail] = row;
      this.#rowTails[existing] = row;
      this.#unique = false;
      return;
    }

    const encoded = copyScratchKey(length);
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
    const length = encodeJoinKey(value);
    if (length < 0) return -1;
    const index = this.#find(length, hashScratch(length));
    return index < 0 ? -1 : (this.#rowHeads[index] ?? -1);
  }

  nextRow(row: number): number {
    return row < 0 || row >= this.#rowNext.length ? -1 : (this.#rowNext[row] ?? -1);
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

/**
 * Encodes one join key into the shared scratch arena and returns its byte length, or -1 for keys
 * SQL equality can never match (null, undefined, NaN). Dates coerce to their epoch milliseconds,
 * so a Date and its number representation join as the same key.
 */
function encodeJoinKey(value: unknown): number {
  const key = value instanceof Date ? dateMilliseconds(value) : value;
  if (key === null || key === undefined) return -1;
  if (typeof key === "number" && Number.isNaN(key)) return -1;
  if (typeof key === "boolean" || typeof key === "number" || typeof key === "string") {
    return encodeSingleScalarKey(key);
  }
  throw new TypeError("Join keys must be SQL scalar values");
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
