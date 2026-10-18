/**
 * Snapshots carry packed segments unpacked. A packed segment's members are each a complete v2
 * block, so an export writes them as ordinary block frames under their member ids, in place of
 * the packed block, and writes the segment record without `packedBlockId`. Packing never changes
 * the snapshot's format: a reader that could import the snapshot before, core 0.16 included,
 * imports the segment as an ordinary one. These helpers hold the rules every adapter's export
 * follows.
 *
 * Member ids sort directly after their packed block: a member is the packed block id, a NUL,
 * then its column id. So an export that walks manifest blocks in id order can emit a packed
 * block's members where the packed block would have gone and stay in canonical order.
 */
import {
  crc32,
  inspectPackedBlock,
  isPackedBlock,
  packedMemberBytes,
  packedMemberTag,
} from "../../block-format/index.js";
import { StorageCorruptionError, type SegmentRecord } from "../types.js";

/** One block frame a packed block exports. */
export interface PackedSnapshotFrame {
  readonly key: string;
  readonly payload: Uint8Array;
  readonly checksum: number;
}

/** The segment record a snapshot carries: a packed segment's members stand on their own. */
export function unpackedSnapshotSegment(segment: SegmentRecord): SegmentRecord {
  if (segment.packedBlockId === undefined) return segment;
  const { packedBlockId, ...unpacked } = segment;
  void packedBlockId;
  return unpacked;
}

/** A packed segment's members, as (member id, column id) pairs in the key order they export in. */
export function packedSnapshotMembers(
  backend: string,
  segment: SegmentRecord,
): Array<{ readonly memberId: string; readonly columnId: string }> {
  const members: Array<{ memberId: string; columnId: string }> = [];
  for (const [columnId, ids] of Object.entries(segment.columnBlockIds)) {
    const memberId = ids[0];
    if (ids.length !== 1 || memberId === undefined) {
      throw new StorageCorruptionError(
        backend,
        `segments/${segment.id}`,
        "a packed segment names exactly one member per column",
      );
    }
    members.push({ memberId, columnId });
  }
  return members.sort((left, right) =>
    left.memberId < right.memberId ? -1 : left.memberId > right.memberId ? 1 : 0,
  );
}

/**
 * The block frames a packed block exports, in key order: one per member the exported segment
 * names. A member the packed block lacks, or a payload that is not a packed block, is corruption.
 */
export function packedSnapshotFrames(
  backend: string,
  segment: SegmentRecord,
  packed: Uint8Array,
): PackedSnapshotFrame[] {
  const location = `blocks/${segment.packedBlockId ?? segment.id}`;
  if (!isPackedBlock(packed)) {
    throw new StorageCorruptionError(backend, location, "a packed segment's block is not packed");
  }
  let directory;
  try {
    directory = inspectPackedBlock(packed);
  } catch (error) {
    throw new StorageCorruptionError(
      backend,
      location,
      error instanceof Error ? error.message : "packed block is invalid",
    );
  }
  return packedSnapshotMembers(backend, segment).map(({ memberId, columnId }) => {
    let payload: Uint8Array;
    try {
      payload = packedMemberBytes(packed, directory, packedMemberTag(memberId, columnId), memberId);
    } catch (error) {
      throw new StorageCorruptionError(
        backend,
        location,
        error instanceof Error ? error.message : "packed member is missing",
      );
    }
    return { key: memberId, payload, checksum: crc32(payload) };
  });
}

/**
 * How a set of exported packed blocks changes a snapshot's block summary: each packed block's
 * frame is replaced by its members' frames.
 */
export function packedSnapshotBlockSummary(
  summary: { readonly frameCount: number; readonly storedBytes: number },
  packed: ReadonlyArray<{
    readonly packedBytes: number;
    readonly frames: readonly PackedSnapshotFrame[];
  }>,
): { frameCount: number; itemCount: number; storedBytes: number } {
  let frameCount = summary.frameCount;
  let storedBytes = summary.storedBytes;
  for (const block of packed) {
    frameCount += block.frames.length - 1;
    storedBytes -= block.packedBytes;
    for (const frame of block.frames) storedBytes += frame.payload.byteLength;
  }
  return { frameCount, itemCount: frameCount, storedBytes };
}
