import { crc32 } from "./checksum.js";
import { decodeColumn, encodeColumn } from "./column.js";
import { getCodec } from "./codecs.js";
import { MAX_PHYSICAL_COLUMN_BYTE_LENGTH, validatePhysicalColumn } from "./physical.js";
import type {
  BlockDescription,
  BlockMetadata,
  ColumnInput,
  Compression,
  DecodedBlock,
  DecodedPhysicalBlock,
  LogicalType,
  PhysicalColumnPayload,
  ValidatedPhysicalColumn,
} from "./types.js";

const MAGIC = Uint8Array.of(0x42, 0x52, 0x44, 0x42);
const VERSION = 0;
const HEADER_LENGTH = 36;
const MAX_BLOCK_LENGTH = MAX_PHYSICAL_COLUMN_BYTE_LENGTH;
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

const typeIds: Record<LogicalType, number> = { boolean: 1, number: 2, string: 3, datetime: 4 };
const typesById = new Map(Object.entries(typeIds).map(([type, id]) => [id, type as LogicalType]));
const codecIds: Record<Compression, number> = { raw: 0, rle: 1, gzip: 2 };
const codecsById = new Map(
  Object.entries(codecIds).map(([codec, id]) => [id, codec as Compression]),
);

export async function encodeBlock(
  input: ColumnInput,
  compression: Compression = "raw",
): Promise<Uint8Array> {
  if (input.values.length > 0xffffffff) throw new RangeError("Too many rows in one block");
  const encoded = encodeColumn(input);
  return encodeValidatedPhysicalBlock(
    {
      type: input.type,
      rowCount: input.values.length,
      nullCount: input.values.filter((value) => value === null).length,
      bytes: encoded.bytes,
      metadata: encoded.metadata,
    },
    compression,
  );
}

/** Encodes physical column bytes after validating their complete version-zero layout. */
export async function encodePhysicalBlock(
  input: PhysicalColumnPayload,
  compression: Compression = "raw",
): Promise<Uint8Array> {
  return encodeValidatedPhysicalBlock(validatePhysicalColumn(input), compression);
}

async function encodeValidatedPhysicalBlock(
  input: ValidatedPhysicalColumn,
  compression: Compression,
): Promise<Uint8Array> {
  const metadata = textEncoder.encode(JSON.stringify(input.metadata));
  const stored = await getCodec(compression).compress(input.bytes);
  assertLength(metadata.byteLength, "metadata");
  assertLength(input.bytes.byteLength, "encoded payload");
  assertLength(stored.byteLength, "stored payload");

  const output = new Uint8Array(HEADER_LENGTH + metadata.byteLength + stored.byteLength);
  output.set(MAGIC);
  const view = new DataView(output.buffer);
  view.setUint16(4, VERSION, true);
  view.setUint16(6, HEADER_LENGTH, true);
  view.setUint8(8, typeIds[input.type]);
  view.setUint8(9, typeIds[input.type]); // physical encoding v0 mirrors the logical type
  view.setUint8(10, codecIds[compression]);
  view.setUint8(11, 0);
  view.setUint32(12, input.rowCount, true);
  view.setUint32(16, input.nullCount, true);
  view.setUint32(20, metadata.byteLength, true);
  view.setUint32(24, input.bytes.byteLength, true);
  view.setUint32(28, stored.byteLength, true);
  view.setUint32(32, crc32(input.bytes), true);
  output.set(metadata, HEADER_LENGTH);
  output.set(stored, HEADER_LENGTH + metadata.byteLength);
  return output;
}

export async function decodeBlock(bytes: Uint8Array): Promise<DecodedBlock> {
  const decoded = await decodePhysicalBlock(bytes);
  return {
    description: decoded.description,
    column: decodeColumn(
      decoded.description.type,
      decoded.column.bytes,
      decoded.description.rowCount,
    ),
  };
}

/**
 * Decompresses and checksum-verifies a block without materializing row values.
 * The returned physical byte array is owned by the result.
 */
export async function decodePhysicalBlock(bytes: Uint8Array): Promise<DecodedPhysicalBlock> {
  const description = inspectBlock(bytes);
  const metadataLength = bytes.byteLength - HEADER_LENGTH - description.storedLength;
  const stored = bytes.subarray(HEADER_LENGTH + metadataLength);
  const encoded = await getCodec(description.compression).decompress(
    stored,
    description.encodedLength,
  );
  if (crc32(encoded) !== description.checksum) throw new Error("Block checksum mismatch");
  const column = validatePhysicalColumn({
    type: description.type,
    rowCount: description.rowCount,
    bytes: encoded,
  });
  if (column.nullCount !== description.nullCount) throw new Error("Block null count mismatch");
  if (!metadataEquals(column.metadata, description.metadata)) {
    throw new Error("Block metadata does not match physical payload");
  }
  return {
    description,
    column,
  };
}

export function inspectBlock(bytes: Uint8Array): BlockDescription {
  if (bytes.byteLength < HEADER_LENGTH) throw new Error("Truncated block header");
  if (!MAGIC.every((byte, index) => bytes[index] === byte)) throw new Error("Invalid block magic");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const formatVersion = view.getUint16(4, true);
  if (formatVersion !== VERSION) {
    throw new Error(`Unsupported block version ${String(formatVersion)}`);
  }
  if (view.getUint16(6, true) !== HEADER_LENGTH) throw new Error("Invalid block header length");
  const type = typesById.get(view.getUint8(8));
  if (type === undefined) throw new Error("Unknown logical type");
  if (view.getUint8(9) !== typeIds[type]) throw new Error("Unsupported physical encoding");
  const compression = codecsById.get(view.getUint8(10));
  if (compression === undefined) throw new Error("Unknown compression codec");
  if (view.getUint8(11) !== 0) throw new Error("Unsupported mandatory block flags");
  const rowCount = view.getUint32(12, true);
  const nullCount = view.getUint32(16, true);
  if (nullCount > rowCount) throw new Error("Null count exceeds row count");
  const metadataLength = view.getUint32(20, true);
  const encodedLength = view.getUint32(24, true);
  const storedLength = view.getUint32(28, true);
  assertLength(metadataLength, "metadata");
  assertLength(encodedLength, "encoded payload");
  assertLength(storedLength, "stored payload");
  if (HEADER_LENGTH + metadataLength + storedLength !== bytes.byteLength) {
    throw new Error("Block length mismatch");
  }
  let parsedMetadata: unknown;
  try {
    parsedMetadata = JSON.parse(
      textDecoder.decode(bytes.subarray(HEADER_LENGTH, HEADER_LENGTH + metadataLength)),
    ) as unknown;
  } catch {
    throw new Error("Invalid block metadata");
  }
  const metadata = parseMetadata(parsedMetadata);
  return {
    formatVersion,
    type,
    compression,
    rowCount,
    nullCount,
    encodedLength,
    storedLength,
    checksum: view.getUint32(32, true),
    metadata,
  };
}

function assertLength(length: number, name: string): void {
  if (!Number.isSafeInteger(length) || length < 0 || length > MAX_BLOCK_LENGTH) {
    throw new RangeError(`Invalid ${name} length`);
  }
}

function parseMetadata(value: unknown): BlockMetadata {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Invalid block metadata shape");
  }
  const zoneMap = Reflect.get(value, "zoneMap") as unknown;
  if (zoneMap !== undefined) {
    if (
      typeof zoneMap !== "object" ||
      zoneMap === null ||
      !Number.isFinite(Reflect.get(zoneMap, "min")) ||
      !Number.isFinite(Reflect.get(zoneMap, "max"))
    ) {
      throw new Error("Invalid zone map");
    }
    const min = Reflect.get(zoneMap, "min") as number;
    const max = Reflect.get(zoneMap, "max") as number;
    if (min > max) throw new Error("Invalid zone map");
    return { zoneMap: { min, max } };
  }
  return {};
}

function metadataEquals(left: BlockMetadata, right: BlockMetadata): boolean {
  if (left.zoneMap === undefined || right.zoneMap === undefined) {
    return left.zoneMap === undefined && right.zoneMap === undefined;
  }
  return left.zoneMap.min === right.zoneMap.min && left.zoneMap.max === right.zoneMap.max;
}
