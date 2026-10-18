/**
 * Packed segments, engine side. A small level-zero data segment's column blocks are written as
 * one packed block (see block-format/packed.ts); the segment record keeps one *member* id per
 * column, and the engine turns member ids back into ordinary v2 block bytes at its block-read
 * boundary. Everything above that boundary — caches, decoders, plans — sees one id per column
 * block exactly as for an unpacked segment.
 *
 * The boundary needs to know, for a member id, which stored block holds it. `PackedMemberIndex`
 * records it from every segment record the engine reads or writes, so a read costs a map lookup.
 * A read that misses — an id the index never saw, or dropped past its bound — finds the member
 * from the id itself: a member id is its packed block's id, a NUL, then its column id, so each
 * split of the id at a NUL names a candidate packed block. Ids are arbitrary strings and may
 * hold other NULs, so a candidate answers only when its directory holds the member's tag, which
 * binds the whole id and the column.
 */
import {
  findPackedMember,
  inspectPackedBlock,
  packedMemberBytes,
  packedMemberTag,
  type PackedBlockDirectory,
  type PackedMemberTag,
} from "../block-format/index.js";
import type { SegmentRecord } from "../storage/types.js";

/**
 * The largest packed block a write produces. Below it the per-block costs of storage — records,
 * journal entries, requests — dominate a commit; above it a read that needs one column of the
 * segment would fetch more than it needs.
 */
export const PACKED_SEGMENT_MAX_BYTES = 64 * 1024;

/**
 * Members the index holds before it starts over. Collection drops entries as it reclaims
 * segments; this bounds an engine that only ever reads tables another connection collects, or a
 * table with more packed members than this. A read of a dropped entry finds it from its id.
 */
const MAX_INDEXED_MEMBERS = 262_144;

/** Testing seams: force packing for every eligible segment, and make an index miss fatal. */
export const packedSegmentTestHooks: {
  /** Pack every single-range segment of raw columns, whatever its size or width. */
  forcePacking: boolean;
  /** Throw on a member the index did not know instead of finding it from its id. */
  missIsFatal: boolean;
  /** Members found from their ids, for tests of reads the index did not cover. */
  unindexedReads: number;
  /** The index's bound, lowered by tests of a table with more packed members than it holds. */
  maxIndexedMembers: number;
} = {
  forcePacking: false,
  missIsFatal: false,
  unindexedReads: 0,
  maxIndexedMembers: MAX_INDEXED_MEMBERS,
};

/** Where one member lives. Its tag is computed on its first read and kept. */
export interface PackedMemberRef {
  readonly packedBlockId: string;
  readonly columnId: string;
  tag?: PackedMemberTag;
}

/**
 * Member id to packed block, for every packed segment the engine has seen. Entries go when the
 * engine collects their segment, and all at once past MAX_INDEXED_MEMBERS; a read that needs a
 * dropped entry finds it from its id.
 */
export class PackedMemberIndex {
  readonly #members = new Map<string, PackedMemberRef>();
  /** The member ids indexed under each packed block. */
  readonly #segments = new Map<string, string[]>();
  /** Packed blocks indexed from their segment record, which names every member. */
  readonly #complete = new Set<string>();

  /** Member ids indexed; zero means every read takes today's unpacked path. */
  get size(): number {
    return this.#members.size;
  }

  /** Indexes a segment's members when it is packed; ignores every other segment. */
  register(segment: SegmentRecord): void {
    const packedBlockId = segment.packedBlockId;
    if (packedBlockId === undefined) return;
    this.registerPacked(packedBlockId, segment.columnBlockIds);
  }

  registerAll(segments: Iterable<SegmentRecord>): void {
    for (const segment of segments) {
      if (segment.packedBlockId !== undefined) this.register(segment);
    }
  }

  /** Indexes every member a packed segment's columns name. */
  registerPacked(
    packedBlockId: string,
    columnBlockIds: Readonly<Record<string, readonly string[]>>,
  ): void {
    if (this.#complete.has(packedBlockId)) return;
    if (this.#members.size >= packedSegmentTestHooks.maxIndexedMembers) this.clear();
    for (const [columnId, ids] of Object.entries(columnBlockIds)) {
      const memberId = ids[0];
      if (memberId !== undefined) this.#add(memberId, columnId, packedBlockId);
    }
    this.#complete.add(packedBlockId);
  }

  /** Indexes one member a durable plan names, for a job resumed without its source records. */
  registerMember(memberId: string, columnId: string, packedBlockId: string): void {
    if (this.#members.has(memberId)) return;
    if (this.#members.size >= packedSegmentTestHooks.maxIndexedMembers) this.clear();
    this.#add(memberId, columnId, packedBlockId);
  }

  #add(memberId: string, columnId: string, packedBlockId: string): void {
    if (this.#members.has(memberId)) return;
    this.#members.set(memberId, { packedBlockId, columnId });
    let members = this.#segments.get(packedBlockId);
    if (members === undefined) {
      members = [];
      this.#segments.set(packedBlockId, members);
    }
    members.push(memberId);
  }

  lookup(id: string): PackedMemberRef | undefined {
    return this.#members.get(id);
  }

  /** Drops the packed segments with these ids (a packed block's id is its segment's id). */
  forget(segmentIds: Iterable<string>): void {
    for (const id of segmentIds) {
      const members = this.#segments.get(id);
      if (members === undefined) continue;
      for (const member of members) this.#members.delete(member);
      this.#segments.delete(id);
      this.#complete.delete(id);
    }
  }

  clear(): void {
    this.#members.clear();
    this.#segments.clear();
    this.#complete.clear();
  }
}

/** A packed block as the read boundary keeps it: the bytes and their verified directory. */
export interface ResolvedPackedBlock {
  readonly bytes: Uint8Array;
  readonly directory: PackedBlockDirectory;
}

/** Checks a packed block's directory once, so its members can be cut out of it. */
export function resolvePackedBlock(bytes: Uint8Array): ResolvedPackedBlock {
  return { bytes, directory: inspectPackedBlock(bytes) };
}

/** Retained bytes of a resolved packed block, for the artifact cache. */
export function resolvedPackedBlockBytes(block: ResolvedPackedBlock): number {
  return block.bytes.byteLength + block.directory.memberCount * 16 + 160;
}

/**
 * The packed blocks an unindexed member id could live in: the id split at each NUL, the last
 * first. Only a split whose packed block holds the member's tag is the member's.
 */
export function packedMemberCandidates(
  id: string,
): Array<{ readonly packedBlockId: string; readonly columnId: string }> {
  const candidates: Array<{ packedBlockId: string; columnId: string }> = [];
  for (let at = id.lastIndexOf("\u0000"); at > 0; at = id.lastIndexOf("\u0000", at - 1)) {
    candidates.push({ packedBlockId: id.slice(0, at), columnId: id.slice(at + 1) });
  }
  return candidates;
}

/** An unindexed member's standalone v2 block, when this packed block holds it. */
export function unindexedMemberBytes(
  block: ResolvedPackedBlock,
  memberId: string,
  columnId: string,
): Uint8Array | undefined {
  const tag = packedMemberTag(memberId, columnId);
  if (findPackedMember(block.directory, tag) < 0) return undefined;
  return packedMemberBytes(block.bytes, block.directory, tag, memberId);
}

/** One member's standalone v2 block, copied out of its packed block. */
export function memberBlockBytes(
  block: ResolvedPackedBlock,
  ref: PackedMemberRef,
  memberId: string,
): Uint8Array {
  ref.tag ??= packedMemberTag(memberId, ref.columnId);
  return packedMemberBytes(block.bytes, block.directory, ref.tag, memberId);
}
