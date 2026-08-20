import { crc32 } from "../../block-format/index.js";

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

export const LOG_FORMAT_VERSION = 2;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function encodeRecordJson(value: unknown): Uint8Array {
  // The replacer costs ~3x on big payloads because it runs per node; most frames carry no
  // bigints at all, and stringify announces one by throwing.
  let json: string;
  try {
    json = JSON.stringify(value);
  } catch {
    json = JSON.stringify(value, (_key, entry: unknown) =>
      typeof entry === "bigint" ? { $n: entry.toString() } : entry,
    );
  }
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
      return BigInt((entry as { $n: string }).$n);
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
 * with an unknown format version throws instead: that file was written by a newer build and
 * ignoring it would silently roll the database back.
 */
function decodeEnvelope(magic: string, bytes: Uint8Array): Uint8Array | undefined {
  if (bytes.byteLength < ENVELOPE_HEADER_BYTES) return undefined;
  for (let index = 0; index < 8; index += 1) {
    if (bytes[index] !== magic.charCodeAt(index)) return undefined;
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const version = view.getUint32(8, true);
  if (version !== LOG_FORMAT_VERSION) {
    throw new Error(
      `This database was written by a newer format (version ${String(version)}); ` +
        `this build reads version ${String(LOG_FORMAT_VERSION)}`,
    );
  }
  const payloadLength = view.getUint32(12, true);
  if (bytes.byteLength < ENVELOPE_HEADER_BYTES + payloadLength) return undefined;
  const payload = bytes.subarray(ENVELOPE_HEADER_BYTES, ENVELOPE_HEADER_BYTES + payloadLength);
  if (crc32(payload) !== view.getUint32(16, true)) return undefined;
  return payload;
}

const CHUNK_MAGIC = "MNWCHNK1";
const SYNC_CHECKPOINT_MAGIC = "MNWCKPS1";

/** Immutable artifact payloads (full-text base chunks) inside extents. */
export function encodeChunk(value: unknown): Uint8Array {
  return encodeEnvelope(CHUNK_MAGIC, encodeRecordJson(value));
}

export function decodeChunk(bytes: Uint8Array): unknown {
  const payload = decodeEnvelope(CHUNK_MAGIC, bytes);
  return payload === undefined ? undefined : decodeRecordJson(payload);
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
