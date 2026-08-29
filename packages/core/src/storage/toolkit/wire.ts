import { crc32 } from "../../block-format/index.js";
import { MAX_ROW_ID_EXCLUSIVE_END, StorageFormatVersionError } from "../types.js";

/**
 * Payload encodings for a log-structured adapter's control data: checkpoint files, immutable
 * artifact chunks, and the JSON codec write-ahead-log frames carry. Every envelope is
 * checksummed so a torn write — the only artifact a crash can leave — is detectable and
 * indistinguishable from "not written".
 *
 * Record payloads are JSON with one extension: bigints (segment row ids, counters, full-text
 * posting row ids, compaction rewrite plans) encode as `{"$n":"<decimal>"}`. No record shape
 * can produce that object naturally — keys are ids and column names, values are JSON scalars —
 * and the reviver only converts an object whose sole key is `$n`.
 */

export const LOG_FORMAT_VERSION = 5;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

export function encodeRecordJson(value: unknown): Uint8Array {
  // The replacer costs ~3x on big payloads because it runs per node; most frames carry no
  // bigints at all, and stringify announces one by throwing.
  let json: string;
  try {
    json = JSON.stringify(value);
  } catch {
    json = JSON.stringify(value, (_key, entry: unknown) => {
      if (typeof entry !== "bigint") return entry;
      if (entry < 0n || entry > MAX_ROW_ID_EXCLUSIVE_END) {
        throw new RangeError("Record bigint exceeds the unsigned 64-bit persisted range");
      }
      return { $n: entry.toString() };
    });
  }
  if (typeof json !== "string") throw new TypeError("Record value is not JSON-serializable");
  return textEncoder.encode(json);
}

export function decodeRecordJson(bytes: Uint8Array): unknown {
  const json = textDecoder.decode(bytes);
  return JSON.parse(json, (_key, entry: unknown) => {
    if (
      typeof entry === "object" &&
      entry !== null &&
      !Array.isArray(entry) &&
      Object.keys(entry).length === 1 &&
      typeof (entry as { $n?: unknown }).$n === "string"
    ) {
      const decimal = (entry as { $n: string }).$n;
      if (!/^(?:0|[1-9][0-9]{0,19})$/.test(decimal)) {
        throw new TypeError("Record bigint is not a canonical bounded unsigned decimal");
      }
      const value = BigInt(decimal);
      if (value > MAX_ROW_ID_EXCLUSIVE_END) {
        throw new RangeError("Record bigint exceeds the unsigned 64-bit persisted range");
      }
      return value;
    }
    return entry;
  });
}

const ENVELOPE_HEADER_BYTES = 8 + 4 + 4 + 4;

function encodeEnvelope(magic: string, payload: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(ENVELOPE_HEADER_BYTES + payload.byteLength);
  const view = new DataView(bytes.buffer);
  textEncoder.encodeInto(magic, bytes);
  view.setUint32(8, LOG_FORMAT_VERSION, true);
  view.setUint32(12, payload.byteLength, true);
  view.setUint32(16, crc32(payload), true);
  bytes.set(payload, ENVELOPE_HEADER_BYTES);
  return bytes;
}

/**
 * `undefined` means the bytes are torn or foreign — treat as "not written". A recognized magic
 * with an unknown format version throws instead: reading a different layout as the current one
 * would silently corrupt or roll back the database.
 */
function decodeEnvelope(magic: string, bytes: Uint8Array): Uint8Array | undefined {
  if (bytes.byteLength < ENVELOPE_HEADER_BYTES) return undefined;
  for (let index = 0; index < 8; index += 1) {
    if (bytes[index] !== magic.charCodeAt(index)) return undefined;
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const version = view.getUint32(8, true);
  if (version !== LOG_FORMAT_VERSION) {
    throw new StorageFormatVersionError(
      "opfs",
      `envelope/${magic}`,
      version,
      LOG_FORMAT_VERSION,
      version < LOG_FORMAT_VERSION ? "older" : "newer",
    );
  }
  const payloadLength = view.getUint32(12, true);
  if (bytes.byteLength !== ENVELOPE_HEADER_BYTES + payloadLength) return undefined;
  const payload = bytes.subarray(ENVELOPE_HEADER_BYTES, ENVELOPE_HEADER_BYTES + payloadLength);
  if (crc32(payload) !== view.getUint32(16, true)) return undefined;
  return payload;
}

const CHUNK_MAGIC = "MNWCHNK1";
const POSTING_CHUNK_MAGIC = "MNWPOST1";
const SYNC_CHECKPOINT_MAGIC = "MNWCKPS1";

/** Immutable artifact payloads (full-text base chunks) inside extents. */
export function encodeChunk(value: unknown): Uint8Array {
  return encodeEnvelope(CHUNK_MAGIC, encodeRecordJson(value));
}

export function decodeChunk(bytes: Uint8Array): unknown {
  const payload = decodeEnvelope(CHUNK_MAGIC, bytes);
  return payload === undefined ? undefined : decodeRecordJson(payload);
}

/** The structural part of FtsPosting kept here to avoid coupling the low-level codec to storage. */
interface PostingChunkEntry {
  term: string;
  rowIds: bigint[];
  tf: number[];
}

/**
 * Compact postings payload for OPFS. Terms are UTF-8 and the already-sorted row locators are
 * delta-varints, keeping secondary indexes smaller than the table they accelerate.
 * The envelope keeps the same version and checksum guarantees as every other immutable chunk.
 */
export function encodePostingChunk(entries: readonly PostingChunkEntry[]): Uint8Array {
  const writer = new BinaryWriter();
  writer.varuint(BigInt(entries.length));
  for (const entry of entries) {
    if (entry.rowIds.length !== entry.tf.length) {
      throw new TypeError("Posting row IDs and term frequencies must have the same length");
    }
    const term = textEncoder.encode(entry.term);
    if (term.byteLength === 0) throw new TypeError("Posting terms cannot be empty");
    if (entry.rowIds.length === 0) throw new TypeError("Postings cannot have no row IDs");
    writer.varuint(BigInt(term.byteLength));
    writer.bytes(term);
    writer.varuint(BigInt(entry.rowIds.length));
    let previous = 0n;
    for (const rowId of entry.rowIds) {
      if (rowId <= previous || rowId > MAX_ROW_ID_EXCLUSIVE_END - 1n) {
        throw new TypeError("Posting row IDs must be positive, uint64, and strictly sorted");
      }
      writer.varuint(rowId - previous);
      previous = rowId;
    }
    for (const frequency of entry.tf) {
      if (!Number.isSafeInteger(frequency) || frequency <= 0) {
        throw new TypeError("Posting term frequencies must be positive whole numbers");
      }
      writer.varuint(BigInt(frequency));
    }
  }
  return encodeEnvelope(POSTING_CHUNK_MAGIC, writer.finish());
}

/** `undefined` means the bytes are torn or are not a canonical postings envelope. */
export function decodePostingChunk(bytes: Uint8Array): PostingChunkEntry[] | undefined {
  const payload = decodeEnvelope(POSTING_CHUNK_MAGIC, bytes);
  if (payload === undefined) return undefined;
  const reader = new BinaryReader(payload);
  const count = reader.safeInteger("posting count");
  if (count > Math.floor(reader.remaining / 5)) {
    throw new Error("Posting count exceeds the remaining payload");
  }
  const entries = new Array<PostingChunkEntry>(count);
  for (let entryIndex = 0; entryIndex < count; entryIndex += 1) {
    const termLength = reader.safeInteger("posting term length");
    if (termLength < 1 || termLength > reader.remaining) {
      throw new Error("Posting term length exceeds the remaining payload");
    }
    const term = textDecoder.decode(reader.bytes(termLength));
    const rowCount = reader.safeInteger("posting row count");
    if (rowCount < 1 || rowCount > Math.floor(reader.remaining / 2)) {
      throw new Error("Posting row count exceeds the remaining payload");
    }
    const rowIds = new Array<bigint>(rowCount);
    let previous = 0n;
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      previous += reader.varuint();
      if (previous < 1n || previous > MAX_ROW_ID_EXCLUSIVE_END - 1n) {
        throw new RangeError("Posting row ID exceeds the uint64 range");
      }
      if (rowIndex > 0 && previous <= (rowIds[rowIndex - 1] ?? 0n)) {
        throw new TypeError("Posting row IDs are not strictly sorted");
      }
      rowIds[rowIndex] = previous;
    }
    const tf = new Array<number>(rowCount);
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      const frequency = reader.safeInteger("posting term frequency");
      if (frequency <= 0) throw new TypeError("Posting term frequency must be positive");
      tf[rowIndex] = frequency;
    }
    entries[entryIndex] = { term, rowIds, tf };
  }
  if (!reader.done) throw new Error("Posting chunk has trailing bytes");
  return entries;
}

class BinaryWriter {
  #buffer = new Uint8Array(1_024);
  #length = 0;

  bytes(bytes: Uint8Array): void {
    this.#reserve(bytes.byteLength);
    this.#buffer.set(bytes, this.#length);
    this.#length += bytes.byteLength;
  }

  varuint(value: bigint): void {
    if (value < 0n || value >= MAX_ROW_ID_EXCLUSIVE_END) {
      throw new RangeError("A varuint must fit canonical uint64");
    }
    do {
      this.#reserve(1);
      const byte = Number(value & 0x7fn);
      value >>= 7n;
      this.#buffer[this.#length] = value === 0n ? byte : byte | 0x80;
      this.#length += 1;
    } while (value !== 0n);
  }

  finish(): Uint8Array {
    return this.#buffer.slice(0, this.#length);
  }

  #reserve(extra: number): void {
    if (!Number.isSafeInteger(extra) || extra < 0) throw new RangeError("Invalid binary growth");
    const needed = this.#length + extra;
    if (!Number.isSafeInteger(needed)) throw new RangeError("Binary payload is too large");
    if (needed <= this.#buffer.byteLength) return;
    let capacity = this.#buffer.byteLength;
    while (capacity < needed) {
      const doubled = capacity * 2;
      capacity = Number.isSafeInteger(doubled) ? doubled : needed;
    }
    const grown = new Uint8Array(capacity);
    grown.set(this.#buffer);
    this.#buffer = grown;
  }
}

class BinaryReader {
  #offset = 0;

  constructor(private readonly source: Uint8Array) {}

  get done(): boolean {
    return this.#offset === this.source.byteLength;
  }

  get remaining(): number {
    return this.source.byteLength - this.#offset;
  }

  bytes(length: number): Uint8Array {
    if (!Number.isSafeInteger(length) || length < 0 || length > this.remaining)
      throw new Error("Posting chunk is truncated");
    const bytes = this.source.subarray(this.#offset, this.#offset + length);
    this.#offset += length;
    return bytes;
  }

  varuint(): bigint {
    let value = 0n;
    let shift = 0n;
    for (let byteIndex = 0; byteIndex < 10; byteIndex += 1) {
      const byte = this.source[this.#offset];
      if (byte === undefined) throw new Error("Posting chunk is truncated");
      this.#offset += 1;
      if (byteIndex === 9 && (byte & 0xfe) !== 0) {
        throw new Error("Posting chunk varuint exceeds uint64");
      }
      value |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) {
        if (byteIndex > 0 && (byte & 0x7f) === 0) {
          throw new Error("Posting chunk varuint is overlong");
        }
        return value;
      }
      shift += 7n;
    }
    throw new Error("Posting chunk varuint is too wide");
  }

  safeInteger(label: string): number {
    const value = this.varuint();
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new RangeError(`${label} is too large`);
    return Number(value);
  }
}

/**
 * The leader's checkpoint slots are written synchronously (no compression — that needs an
 * await, and the checkpoint must land in the same synchronous run that resets the WAL, or
 * frames appended in between would be lost by the reset).
 */
export function encodeSyncCheckpoint(state: unknown): Uint8Array {
  return encodeEnvelope(SYNC_CHECKPOINT_MAGIC, encodeRecordJson(state));
}

export function decodeSyncCheckpoint(bytes: Uint8Array): unknown {
  const payload = decodeEnvelope(SYNC_CHECKPOINT_MAGIC, bytes);
  return payload === undefined ? undefined : decodeRecordJson(payload);
}
