import { CRC32_BYTE_TABLE, crc32 } from "./checksum.js";
import { encodeColumn } from "./column.js";
import { assertBlockRowCount } from "./physical.js";
import {
  BLOCK_HEADER_LENGTH,
  encodedRawBlockLength,
  verifyStoredBlock,
  writeRawBlock,
} from "./block.js";
import type { BlockDescription, ColumnInput } from "./types.js";

/**
 * A packed block holds one small segment's column blocks in a single stored payload, so a
 * single-row commit to a wide table stores one block instead of one per column. Each member is
 * a complete, frozen v2 block, so a member is read with the ordinary v2 decoders and keeps its
 * own zone map; the container only adds a directory.
 *
 * Version 1 layout (all integers little-endian):
 *
 *    0  magic "BRPK"
 *    4  envelope checksum: crc32 over bytes [8, D), where D = 24 + 12 * memberCount
 *    8  container format version (u16)
 *   10  header length (u16) = 24
 *   12  member count (u16), 1 to 1,024
 *   14  flags (u16), always zero
 *   16  member byte length (u32): the payload after the directory
 *   20  members checksum: crc32 over bytes [D, end)
 *   24  directory, one 12-byte entry per member: byte length (u32), then the member tag as a
 *       CRC-32 (u32) and an FNV-1a-32 (u32) of UTF-8(member id, 0x00, column id)
 *    D  the members: complete v2 blocks, back to back in directory order
 *
 * The tag binds each member to the segment record's (member id, column id) pair, so a record
 * naming a member the directory lacks, or naming it under the wrong column, is refused as
 * corruption. Two hashes because CRC is affine: ids of equal length that collide under one CRC
 * collide under every CRC, and the ids in one container usually have equal lengths.
 *
 * Never change version 1 decoding in place; format-vectors.test.ts freezes its bytes.
 */
export const PACKED_BLOCK_FORMAT_VERSION = 1;
export const PACKED_BLOCK_HEADER_LENGTH = 24;
export const PACKED_BLOCK_ENTRY_LENGTH = 12;
/** One member per column, and a table has at most 1,024 columns. */
export const MAX_PACKED_BLOCK_MEMBERS = 1_024;
const ENVELOPE_CHECKSUM_START = 8;
const MAGIC = Uint8Array.of(0x42, 0x52, 0x50, 0x4b);
const textEncoder = new TextEncoder();

export interface PackedColumnInput {
  readonly memberId: string;
  readonly columnId: string;
  readonly column: ColumnInput;
}

export interface PackedBlockMember {
  readonly memberId: string;
  readonly columnId: string;
  /** Byte offset of the member's v2 block inside the container. */
  readonly offset: number;
  readonly length: number;
  /** The member's v2 header, exactly as `inspectBlock` reads it back. */
  readonly description: BlockDescription;
}

export interface EncodedPackedBlock {
  readonly bytes: Uint8Array;
  readonly members: readonly PackedBlockMember[];
}

/** A packed block's verified directory: where each member sits and the tag it was written for. */
export interface PackedBlockDirectory {
  readonly memberCount: number;
  readonly offsets: Uint32Array;
  readonly lengths: Uint32Array;
  readonly tagCrc: Uint32Array;
  readonly tagFnv: Uint32Array;
}

/** The two halves of a member's tag; see the layout above. */
export interface PackedMemberTag {
  readonly crc: number;
  readonly fnv: number;
}

export function packedMemberTag(memberId: string, columnId: string): PackedMemberTag {
  // Ids are ASCII almost always, and their UTF-8 is their code units: hash those directly, the
  // way every read of a packed member does, without building or encoding the pair first.
  const table = CRC32_BYTE_TABLE;
  let crc = -1;
  let fnv = 0x811c9dc5;
  for (let at = 0; at < memberId.length; at += 1) {
    const code = memberId.charCodeAt(at);
    if (code > 0x7f) return encodedMemberTag(memberId, columnId);
    crc = (table[(crc ^ code) & 0xff] ?? 0) ^ (crc >>> 8);
    fnv = Math.imul(fnv ^ code, 0x01000193);
  }
  crc = (table[crc & 0xff] ?? 0) ^ (crc >>> 8);
  fnv = Math.imul(fnv, 0x01000193);
  for (let at = 0; at < columnId.length; at += 1) {
    const code = columnId.charCodeAt(at);
    if (code > 0x7f) return encodedMemberTag(memberId, columnId);
    crc = (table[(crc ^ code) & 0xff] ?? 0) ^ (crc >>> 8);
    fnv = Math.imul(fnv ^ code, 0x01000193);
  }
  return { crc: ~crc >>> 0, fnv: fnv >>> 0 };
}

function encodedMemberTag(memberId: string, columnId: string): PackedMemberTag {
  const bytes = textEncoder.encode(`${memberId}\u0000${columnId}`);
  let fnv = 0x811c9dc5;
  for (const byte of bytes) fnv = Math.imul(fnv ^ byte, 0x01000193) >>> 0;
  return { crc: crc32(bytes), fnv };
}

/**
 * Both tag hashes after a member id's prefix — the text up to and including the NUL before its
 * column id. A segment's canonical members (`<segment id>\0<column id>`) all share one, so a
 * check over a whole segment hashes the segment id once instead of once per column.
 */
interface PrefixTagState {
  readonly text: string;
  /** The CRC register, not yet inverted. */
  readonly crc: number;
  readonly fnv: number;
}

/** The prefix state of a canonical ASCII member id, or undefined for any other id. */
function prefixTagState(memberId: string, columnId: string): PrefixTagState | undefined {
  const prefixLength = memberId.length - columnId.length;
  if (
    prefixLength < 2 ||
    memberId.charCodeAt(prefixLength - 1) !== 0 ||
    !memberId.endsWith(columnId)
  ) {
    return undefined;
  }
  const table = CRC32_BYTE_TABLE;
  let crc = -1;
  let fnv = 0x811c9dc5;
  for (let at = 0; at < prefixLength; at += 1) {
    const code = memberId.charCodeAt(at);
    if (code > 0x7f) return undefined;
    crc = (table[(crc ^ code) & 0xff] ?? 0) ^ (crc >>> 8);
    fnv = Math.imul(fnv ^ code, 0x01000193);
  }
  return { text: memberId.slice(0, prefixLength), crc, fnv };
}

/**
 * `packedMemberTag(memberId, columnId)` for a member id made of the prefix's text and the
 * column id, continued from the prefix's state: it hashes `<column id>\0<column id>` only.
 * Undefined when the id has another shape or the column id is not ASCII.
 */
function sharedPrefixTag(
  prefix: PrefixTagState | undefined,
  memberId: string,
  columnId: string,
): PackedMemberTag | undefined {
  if (
    prefix === undefined ||
    memberId.length !== prefix.text.length + columnId.length ||
    !memberId.startsWith(prefix.text) ||
    !memberId.endsWith(columnId)
  ) {
    return undefined;
  }
  const table = CRC32_BYTE_TABLE;
  let crc = prefix.crc;
  let fnv = prefix.fnv;
  for (let pass = 0; pass < 2; pass += 1) {
    for (let at = 0; at < columnId.length; at += 1) {
      const code = columnId.charCodeAt(at);
      if (code > 0x7f) return undefined;
      crc = (table[(crc ^ code) & 0xff] ?? 0) ^ (crc >>> 8);
      fnv = Math.imul(fnv ^ code, 0x01000193);
    }
    if (pass === 0) {
      // The NUL between the member id and its column id.
      crc = (table[crc & 0xff] ?? 0) ^ (crc >>> 8);
      fnv = Math.imul(fnv, 0x01000193);
    }
  }
  return { crc: ~crc >>> 0, fnv: fnv >>> 0 };
}

/**
 * Encodes small raw columns as one packed block, or returns undefined when two members' tags
 * collide and the caller should write ordinary blocks instead. Every member is raw: packing is
 * for blocks too small for compression to pay.
 */
export function encodePackedBlock(
  inputs: readonly PackedColumnInput[],
): EncodedPackedBlock | undefined {
  if (inputs.length === 0 || inputs.length > MAX_PACKED_BLOCK_MEMBERS) {
    throw new RangeError("A packed block holds 1 to 1,024 members");
  }
  const tags = inputs.map((input) => packedMemberTag(input.memberId, input.columnId));
  const seen = new Set<string>();
  for (const tag of tags) {
    const key = `${String(tag.crc)}:${String(tag.fnv)}`;
    if (seen.has(key)) return undefined;
    seen.add(key);
  }
  const encoded = inputs.map(({ column }) => {
    assertBlockRowCount(column.values.length);
    const physical = encodeColumn(column);
    return {
      type: column.type,
      rowCount: column.values.length,
      physical,
      metadata: textEncoder.encode(JSON.stringify(physical.metadata)),
    };
  });
  const directoryEnd = PACKED_BLOCK_HEADER_LENGTH + PACKED_BLOCK_ENTRY_LENGTH * inputs.length;
  let byteLength = directoryEnd;
  for (const column of encoded) {
    byteLength += encodedRawBlockLength(
      column.metadata.byteLength,
      column.physical.bytes.byteLength,
    );
  }
  const bytes = new Uint8Array(byteLength);
  const view = new DataView(bytes.buffer);
  const members: PackedBlockMember[] = [];
  let at = directoryEnd;
  for (const [index, column] of encoded.entries()) {
    const input = inputs[index];
    const tag = tags[index];
    if (input === undefined || tag === undefined) throw new Error("Packed block input is missing");
    const description = writeRawBlock(bytes, at, {
      type: column.type,
      rowCount: column.rowCount,
      nullCount: column.physical.nullCount,
      payload: column.physical.bytes,
      metadataBytes: column.metadata,
      metadata: column.physical.metadata,
    });
    const length = BLOCK_HEADER_LENGTH + description.metadataLength + description.storedLength;
    const entry = PACKED_BLOCK_HEADER_LENGTH + PACKED_BLOCK_ENTRY_LENGTH * index;
    view.setUint32(entry, length, true);
    view.setUint32(entry + 4, tag.crc, true);
    view.setUint32(entry + 8, tag.fnv, true);
    members.push({
      memberId: input.memberId,
      columnId: input.columnId,
      offset: at,
      length,
      description,
    });
    at += length;
  }
  bytes.set(MAGIC);
  view.setUint16(8, PACKED_BLOCK_FORMAT_VERSION, true);
  view.setUint16(10, PACKED_BLOCK_HEADER_LENGTH, true);
  view.setUint16(12, inputs.length, true);
  view.setUint16(14, 0, true);
  view.setUint32(16, byteLength - directoryEnd, true);
  view.setUint32(20, crc32(bytes.subarray(directoryEnd)), true);
  view.setUint32(4, crc32(bytes.subarray(ENVELOPE_CHECKSUM_START, directoryEnd)), true);
  return { bytes, members };
}

/** Whether the payload carries the packed-block magic; says nothing about its validity. */
export function isPackedBlock(bytes: Uint8Array): boolean {
  return (
    bytes.byteLength >= 4 &&
    bytes[0] === MAGIC[0] &&
    bytes[1] === MAGIC[1] &&
    bytes[2] === MAGIC[2] &&
    bytes[3] === MAGIC[3]
  );
}

/**
 * Checks the header and directory against their envelope checksum and returns where each
 * member sits. Reads trust the directory from here; each member's own v2 checksums still guard
 * its bytes when it is inspected or decoded.
 */
export function inspectPackedBlock(bytes: Uint8Array): PackedBlockDirectory {
  if (bytes.byteLength < PACKED_BLOCK_HEADER_LENGTH) throw new Error("Truncated packed block");
  if (!isPackedBlock(bytes)) throw new Error("Invalid packed block magic");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const version = view.getUint16(8, true);
  if (version !== PACKED_BLOCK_FORMAT_VERSION) {
    throw new Error(`Unsupported packed block version ${String(version)}`);
  }
  if (view.getUint16(10, true) !== PACKED_BLOCK_HEADER_LENGTH) {
    throw new Error("Invalid packed block header length");
  }
  const memberCount = view.getUint16(12, true);
  if (memberCount === 0 || memberCount > MAX_PACKED_BLOCK_MEMBERS) {
    throw new Error("Invalid packed block member count");
  }
  if (view.getUint16(14, true) !== 0) throw new Error("Unsupported packed block flags");
  const directoryEnd = PACKED_BLOCK_HEADER_LENGTH + PACKED_BLOCK_ENTRY_LENGTH * memberCount;
  if (bytes.byteLength < directoryEnd) throw new Error("Truncated packed block directory");
  if (view.getUint32(16, true) !== bytes.byteLength - directoryEnd) {
    throw new Error("Packed block length mismatch");
  }
  if (crc32(bytes.subarray(ENVELOPE_CHECKSUM_START, directoryEnd)) !== view.getUint32(4, true)) {
    throw new Error("Packed block envelope checksum mismatch");
  }
  const offsets = new Uint32Array(memberCount);
  const lengths = new Uint32Array(memberCount);
  const tagCrc = new Uint32Array(memberCount);
  const tagFnv = new Uint32Array(memberCount);
  let at = directoryEnd;
  for (let index = 0; index < memberCount; index += 1) {
    const entry = PACKED_BLOCK_HEADER_LENGTH + PACKED_BLOCK_ENTRY_LENGTH * index;
    const length = view.getUint32(entry, true);
    if (length < BLOCK_HEADER_LENGTH || length > bytes.byteLength - at) {
      throw new Error("Packed block member length is invalid");
    }
    offsets[index] = at;
    lengths[index] = length;
    tagCrc[index] = view.getUint32(entry + 4, true);
    tagFnv[index] = view.getUint32(entry + 8, true);
    at += length;
  }
  if (at !== bytes.byteLength) throw new Error("Packed block members do not fill the payload");
  return { memberCount, offsets, lengths, tagCrc, tagFnv };
}

/** The index of the member written for this tag, or -1. */
export function findPackedMember(directory: PackedBlockDirectory, tag: PackedMemberTag): number {
  for (let index = 0; index < directory.memberCount; index += 1) {
    if (directory.tagCrc[index] === tag.crc && directory.tagFnv[index] === tag.fnv) return index;
  }
  return -1;
}

/**
 * The first of a packed segment's members that its packed block does not hold for the member's
 * column, or undefined when every one is there. Full integrity checks call this; reads refuse the
 * same mismatch when they cut a member out.
 */
export function missingPackedMember(
  bytes: Uint8Array,
  columnBlockIds: Readonly<Record<string, readonly string[]>>,
): string | undefined {
  const directory = inspectPackedBlock(bytes);
  // A segment written with its block names the members in directory order, so each one is
  // tried at the next position first; only a miss builds the lookup. A wide table names up to
  // 1,024 members, and a linear `findPackedMember` scan for each would make this quadratic.
  // Canonical member ids share their segment-id prefix, hashed once (see `sharedPrefixTag`).
  let position = 0;
  let tags: Set<string> | undefined;
  let prefix: PrefixTagState | undefined;
  for (const [columnId, memberIds] of Object.entries(columnBlockIds)) {
    for (const memberId of memberIds) {
      prefix ??= prefixTagState(memberId, columnId);
      const tag =
        sharedPrefixTag(prefix, memberId, columnId) ?? packedMemberTag(memberId, columnId);
      if (directory.tagCrc[position] === tag.crc && directory.tagFnv[position] === tag.fnv) {
        position += 1;
        continue;
      }
      if (tags === undefined) {
        tags = new Set<string>();
        for (let index = 0; index < directory.memberCount; index += 1) {
          tags.add(`${String(directory.tagCrc[index])}:${String(directory.tagFnv[index])}`);
        }
      }
      if (!tags.has(`${String(tag.crc)}:${String(tag.fnv)}`)) return memberId;
    }
  }
  return undefined;
}

/**
 * Why a packed segment cannot be staged with the block bytes the same call carries, or
 * undefined when it can — or when the call does not carry its packed block, which the journal
 * rules judge instead. Reads refuse the same mismatches, so refusing them here keeps a record
 * whose column could never be read out of the store. Directory-only: the members' bytes are not
 * read, so the check costs a few microseconds however large the block.
 */
export function packedSegmentStagingProblem(
  columnBlockIds: Readonly<Record<string, readonly string[]>>,
  bytes: Uint8Array | undefined,
): string | undefined {
  if (bytes === undefined) return undefined;
  if (!isPackedBlock(bytes)) return "packed block is not a packed block";
  let missing: string | undefined;
  try {
    missing = missingPackedMember(bytes, columnBlockIds);
  } catch (error) {
    return `packed block is invalid: ${error instanceof Error ? error.message : String(error)}`;
  }
  return missing === undefined ? undefined : `packed block lacks member ${missing}`;
}

/**
 * A copy of one member's v2 block. A copy, not a view: caches keep what they are handed, and a
 * view would keep the whole container alive behind every member.
 */
export function packedMemberBytes(
  bytes: Uint8Array,
  directory: PackedBlockDirectory,
  tag: PackedMemberTag,
  memberId: string,
): Uint8Array {
  const index = findPackedMember(directory, tag);
  if (index < 0) throw new Error(`Packed block has no member ${memberId} for its column`);
  const offset = directory.offsets[index] ?? 0;
  return bytes.slice(offset, offset + (directory.lengths[index] ?? 0));
}

/**
 * Verifies every stored byte of a packed block: the directory envelope, the members checksum,
 * tag uniqueness, and each member as a complete v2 block.
 */
export function verifyPackedBlock(bytes: Uint8Array): PackedBlockDirectory {
  const directory = inspectPackedBlock(bytes);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const directoryEnd =
    PACKED_BLOCK_HEADER_LENGTH + PACKED_BLOCK_ENTRY_LENGTH * directory.memberCount;
  if (crc32(bytes.subarray(directoryEnd)) !== view.getUint32(20, true)) {
    throw new Error("Packed block members checksum mismatch");
  }
  const tags = new Set<string>();
  for (let index = 0; index < directory.memberCount; index += 1) {
    const key = `${String(directory.tagCrc[index])}:${String(directory.tagFnv[index])}`;
    if (tags.has(key)) throw new Error("Packed block repeats a member tag");
    tags.add(key);
    const offset = directory.offsets[index] ?? 0;
    verifyStoredBlock(bytes.subarray(offset, offset + (directory.lengths[index] ?? 0)));
  }
  return directory;
}

/**
 * Verifies any stored payload — a v2 block or a packed block — byte for byte. Storage calls
 * this wherever it accepts payload bytes it did not just write itself: recovery, relocation,
 * snapshot import, and full integrity checks.
 */
export function verifyStoredPayload(bytes: Uint8Array): void {
  if (isPackedBlock(bytes)) verifyPackedBlock(bytes);
  else verifyStoredBlock(bytes);
}
