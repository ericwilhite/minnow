import { crc32, crc32Continue } from "../../block-format/index.js";
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

export const LOG_FORMAT_VERSION = 7;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

export function encodeRecordJson(value: unknown): Uint8Array {
  const json = stringifyRecordJson(value);
  if (json === undefined) throw new TypeError("Record value is not JSON-serializable");
  return textEncoder.encode(json);
}

/** The record JSON for `value`, or undefined where `JSON.stringify` would give no text. */
function stringifyRecordJson(value: unknown): string | undefined {
  // The replacer costs ~3x on big payloads because it runs per node; most frames carry no
  // bigints at all, and stringify announces one by throwing.
  // Typed `string`, but undefined at runtime for an undefined, function, or symbol value.
  try {
    return JSON.stringify(value);
  } catch {
    return JSON.stringify(value, recordBigintReplacer);
  }
}

function recordBigintReplacer(_key: string, entry: unknown): unknown {
  if (typeof entry !== "bigint") return entry;
  if (entry < 0n || entry > MAX_ROW_ID_EXCLUSIVE_END) {
    throw new RangeError("Record bigint exceeds the unsigned 64-bit persisted range");
  }
  return { $n: entry.toString() };
}

/** JSON values one native stringify call may cover in a sliced encoding. */
const SLICED_JSON_NODES = 4_096;
/** Characters a sliced encoding gathers before it encodes them and offers a pause. */
const SLICED_JSON_FLUSH_CHARS = 64 * 1024;

/**
 * The record JSON of `value`, built a bounded piece at a time: containers too large for one
 * slice are walked, everything else is stringified natively, and `pause` is awaited between
 * pieces. The bytes are exactly `encodeRecordJson(value)`'s; `value` must not change while the
 * encoding is in flight. Returns the UTF-8 pieces, their total length, and their CRC-32.
 */
async function encodeRecordJsonSliced(
  value: unknown,
  pause: () => Promise<void>,
): Promise<{ chunks: Uint8Array[]; byteLength: number; checksum: number }> {
  const chunks: Uint8Array[] = [];
  let pending: string[] = [];
  let pendingChars = 0;
  let byteLength = 0;
  let checksum = 0;
  const flush = async (): Promise<void> => {
    if (pending.length > 0) {
      const bytes = textEncoder.encode(pending.join(""));
      pending = [];
      pendingChars = 0;
      chunks.push(bytes);
      byteLength += bytes.byteLength;
      checksum = crc32Continue(checksum, bytes);
    }
    await pause();
  };
  const push = async (text: string): Promise<void> => {
    pending.push(text);
    pendingChars += text.length;
    if (pendingChars >= SLICED_JSON_FLUSH_CHARS) await flush();
  };
  /** Writes `entry`'s JSON; false when `JSON.stringify` would omit it (an object property). */
  const write = async (entry: unknown): Promise<boolean> => {
    if (jsonNodes(entry, SLICED_JSON_NODES) <= SLICED_JSON_NODES) {
      const text = stringifyRecordJson(entry);
      if (text === undefined) return false;
      await push(text);
      return true;
    }
    if (Array.isArray(entry)) {
      await push("[");
      // Runs of small elements share one native call: `[a,b]` minus its brackets is exactly
      // the text `a,b` contributes, holes and unserializable entries included (as `null`).
      let run: unknown[] = [];
      let runNodes = 0;
      let first = true;
      const writeRun = async (): Promise<void> => {
        if (run.length === 0) return;
        const text = stringifyRecordJson(run) ?? "[]";
        await push(`${first ? "" : ","}${text.slice(1, -1)}`);
        first = false;
        run = [];
        runNodes = 0;
      };
      for (const element of entry as unknown[]) {
        const nodes = jsonNodes(element, SLICED_JSON_NODES);
        if (nodes > SLICED_JSON_NODES) {
          await writeRun();
          if (!first) await push(",");
          first = false;
          if (!(await write(element))) await push("null");
          continue;
        }
        if (runNodes + nodes > SLICED_JSON_NODES) await writeRun();
        run.push(element);
        runNodes += nodes;
      }
      await writeRun();
      await push("]");
      return true;
    }
    // A plain object (jsonNodes walks nothing else): keys in `JSON.stringify`'s order, each
    // value written whole or walked, and omitted exactly when stringify omits it.
    await push("{");
    let first = true;
    for (const key of Object.keys(entry as object)) {
      const property: unknown = (entry as Record<string, unknown>)[key];
      const label = `${first ? "" : ","}${JSON.stringify(key)}:`;
      if (jsonNodes(property, SLICED_JSON_NODES) > SLICED_JSON_NODES) {
        await push(label);
        await write(property);
        first = false;
        continue;
      }
      const text = stringifyRecordJson(property);
      if (text === undefined) continue;
      await push(`${label}${text}`);
      first = false;
    }
    await push("}");
    return true;
  };
  if (!(await write(value))) throw new TypeError("Record value is not JSON-serializable");
  await flush();
  return { chunks, byteLength, checksum };
}

/**
 * JSON values in `entry`, counted until `limit` is passed. Only arrays and plain objects are
 * walked: anything else, a Date or a typed array included, stringifies natively as one piece.
 */
function jsonNodes(entry: unknown, limit: number): number {
  if (typeof entry !== "object" || entry === null) return 1;
  if (Array.isArray(entry)) {
    if (entry.length >= limit) return limit + 1;
    let count = 1;
    for (const element of entry as unknown[]) {
      count += jsonNodes(element, limit - count);
      if (count > limit) return limit + 1;
    }
    return count;
  }
  const prototype: unknown = Object.getPrototypeOf(entry);
  if (prototype !== Object.prototype && prototype !== null) return 1;
  if (typeof (entry as { toJSON?: unknown }).toJSON === "function") return 1;
  const keys = Object.keys(entry);
  if (keys.length >= limit) return limit + 1;
  let count = 1;
  for (const key of keys) {
    count += jsonNodes((entry as Record<string, unknown>)[key], limit - count);
    if (count > limit) return limit + 1;
  }
  return count;
}

export function decodeRecordJson(bytes: Uint8Array): unknown {
  // A reviver runs once per parsed value and made a large checkpoint parse fifteen times slower
  // than the parse itself; tagged bigints are rare, so one pass after parsing finds them.
  return reviveRecordBigints(JSON.parse(textDecoder.decode(bytes)) as unknown);
}

/**
 * Replaces every `{"$n":"<decimal>"}` object in a parsed record with its bigint, in place, and
 * returns the value. Only an object whose sole key is `$n` with a string value converts, as
 * the reviver this replaces did; a replaced property is defined rather than assigned, so a key
 * such as `__proto__` stays an own property.
 */
function reviveRecordBigints(value: unknown): unknown {
  if (typeof value !== "object" || value === null) return value;
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      const entry: unknown = value[index];
      if (typeof entry !== "object" || entry === null) continue;
      const revived = reviveRecordBigints(entry);
      if (revived !== entry) value[index] = revived;
    }
    return value;
  }
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record);
  if (keys.length === 1 && keys[0] === "$n" && typeof record.$n === "string") {
    return recordBigint(record.$n);
  }
  for (const key of keys) {
    const entry = record[key];
    if (typeof entry !== "object" || entry === null) continue;
    const revived = reviveRecordBigints(entry);
    if (revived !== entry) {
      Object.defineProperty(record, key, {
        value: revived,
        writable: true,
        enumerable: true,
        configurable: true,
      });
    }
  }
  return value;
}

function recordBigint(decimal: string): bigint {
  if (!/^(?:0|[1-9][0-9]{0,19})$/.test(decimal)) {
    throw new TypeError("Record bigint is not a canonical bounded unsigned decimal");
  }
  const value = BigInt(decimal);
  if (value > MAX_ROW_ID_EXCLUSIVE_END) {
    throw new RangeError("Record bigint exceeds the unsigned 64-bit persisted range");
  }
  return value;
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
 * would silently corrupt or roll back the database. The version is only believed once the
 * length and checksum vouch for the envelope: a slot whose magic landed ahead of the rest of
 * the write (zeros where the header should be) is torn, not a version-0 database, and a torn
 * slot must leave its mirror and the un-reset log to answer instead.
 */
function decodeEnvelope(
  magic: string,
  bytes: Uint8Array,
  retainLayout6 = false,
): Uint8Array | undefined {
  const framed = envelopeFrame(magic, bytes);
  if (framed === undefined) return undefined;
  if (crc32(framed.payload) !== framed.checksum) return undefined;
  return verifiedEnvelopePayload(magic, framed, retainLayout6);
}

/** `decodeEnvelope`, with the checksum taken a megabyte at a time and `pause` between pieces. */
async function decodeEnvelopeSliced(
  magic: string,
  bytes: Uint8Array,
  pause: () => Promise<void>,
): Promise<Uint8Array | undefined> {
  const framed = envelopeFrame(magic, bytes);
  if (framed === undefined) return undefined;
  let checksum = 0;
  for (let start = 0; start < framed.payload.byteLength; start += ENVELOPE_CHECKSUM_SLICE_BYTES) {
    checksum = crc32Continue(
      checksum,
      framed.payload.subarray(start, start + ENVELOPE_CHECKSUM_SLICE_BYTES),
    );
    await pause();
  }
  if (checksum !== framed.checksum) return undefined;
  return verifiedEnvelopePayload(magic, framed, false);
}

/** Payload bytes a sliced envelope decode checksums between pauses. */
const ENVELOPE_CHECKSUM_SLICE_BYTES = 1024 * 1024;

interface EnvelopeFrame {
  version: number;
  checksum: number;
  payload: Uint8Array;
}

/** The header and payload of a well-framed envelope, before its checksum is trusted. */
function envelopeFrame(magic: string, bytes: Uint8Array): EnvelopeFrame | undefined {
  if (bytes.byteLength < ENVELOPE_HEADER_BYTES) return undefined;
  for (let index = 0; index < 8; index += 1) {
    if (bytes[index] !== magic.charCodeAt(index)) return undefined;
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const payloadLength = view.getUint32(12, true);
  if (bytes.byteLength !== ENVELOPE_HEADER_BYTES + payloadLength) return undefined;
  return {
    version: view.getUint32(8, true),
    checksum: view.getUint32(16, true),
    payload: bytes.subarray(ENVELOPE_HEADER_BYTES, ENVELOPE_HEADER_BYTES + payloadLength),
  };
}

/** A checksum-verified frame's payload, once its version is one this build reads. */
function verifiedEnvelopePayload(
  magic: string,
  framed: EnvelopeFrame,
  retainLayout6: boolean,
): Uint8Array {
  const version = framed.version;
  if (version !== LOG_FORMAT_VERSION && !(retainLayout6 && version === 6)) {
    throw new StorageFormatVersionError(
      "opfs",
      `envelope/${magic}`,
      version,
      LOG_FORMAT_VERSION,
      version < LOG_FORMAT_VERSION ? "older" : "newer",
    );
  }
  return framed.payload;
}

const CHUNK_MAGIC = "MNWCHNK1";
const POSTING_CHUNK_MAGIC = "MNWPOST1";
const SYNC_CHECKPOINT_MAGIC = "MNWCKPS1";

/** Immutable artifact payloads (full-text base chunks) inside extents. */
export function encodeChunk(value: unknown): Uint8Array {
  return encodeEnvelope(CHUNK_MAGIC, encodeRecordJson(value));
}

export function decodeChunk(bytes: Uint8Array): unknown {
  const payload = decodeEnvelope(CHUNK_MAGIC, bytes, true);
  return payload === undefined ? undefined : decodeRecordJson(payload);
}

/** The structural part of FtsPosting kept here to avoid coupling the low-level codec to storage. */
export interface PostingChunkEntry {
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
  // Layout 7 changes acknowledgement/control files, not immutable posting encodings. Keep
  // the frozen layout-6 reader after automatic conversion; never rewrite live extent bytes.
  const payload = decodeEnvelope(POSTING_CHUNK_MAGIC, bytes, true);
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

/**
 * `encodeSyncCheckpoint`'s exact bytes, encoded a bounded piece at a time with `pause` awaited
 * between pieces, so checkpointing a large database does not hold the thread for its whole
 * size. `state` must not change until the returned promise settles.
 */
export async function encodeSyncCheckpointSliced(
  state: unknown,
  pause: () => Promise<void>,
): Promise<Uint8Array> {
  const payload = await encodeRecordJsonSliced(state, pause);
  const bytes = new Uint8Array(ENVELOPE_HEADER_BYTES + payload.byteLength);
  const view = new DataView(bytes.buffer);
  textEncoder.encodeInto(SYNC_CHECKPOINT_MAGIC, bytes);
  view.setUint32(8, LOG_FORMAT_VERSION, true);
  view.setUint32(12, payload.byteLength, true);
  view.setUint32(16, payload.checksum, true);
  let offset = ENVELOPE_HEADER_BYTES;
  for (const chunk of payload.chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

export function decodeSyncCheckpoint(bytes: Uint8Array): unknown {
  const payload = decodeEnvelope(SYNC_CHECKPOINT_MAGIC, bytes);
  return payload === undefined ? undefined : decodeRecordJson(payload);
}

/**
 * `decodeSyncCheckpoint`, with `pause` awaited between its steps: the checksum a megabyte at a
 * time, then the text decode, the parse, and the bigint pass each as their own step. The parse
 * itself is one call, about a millisecond and a half per megabyte of checkpoint.
 */
export async function decodeSyncCheckpointSliced(
  bytes: Uint8Array,
  pause: () => Promise<void>,
): Promise<unknown> {
  const payload = await decodeEnvelopeSliced(SYNC_CHECKPOINT_MAGIC, bytes, pause);
  if (payload === undefined) return undefined;
  const json = textDecoder.decode(payload);
  await pause();
  const parsed = JSON.parse(json) as unknown;
  await pause();
  return reviveRecordBigints(parsed);
}
