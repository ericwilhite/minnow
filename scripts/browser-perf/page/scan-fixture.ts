/** Fixture descriptors shared by scan diagnostics; no executor or frame builder. */
import type { ColumnDef } from "../../../packages/core/src/next/access/catalog.js";
import type {
  GroupSegment,
  TableSnapshot,
} from "../../../packages/core/src/next/access/snapshot.js";
import type { ExportGroup } from "./export-stream.js";
import { encodeGroupRoot } from "../../../packages/core/src/next/format/manifest-codec.js";
import type { GroupRef } from "../../../packages/core/src/next/format/manifest.js";
import { xxHash32 } from "../../../packages/core/src/next/hash/xxhash32.js";
import { DATA_ALIGN } from "../../../packages/core/src/next/storage/contract.js";
import {
  MAX_READ_BYTES,
  ReadDeniedError,
  blobAddress,
  type BlobRef,
  type ExtentReader,
  type ReadLease,
} from "../../../packages/core/src/next/storage/extents.js";

/** BENCH-only snapshot descriptors; stands in for catalog/group-root reads. */
export function benchTableOf(
  groups: readonly ExportGroup[],
  roots?: readonly GroupRef[],
): TableSnapshot {
  const first = groups[0];
  if (!first) throw new Error("No scan groups");
  const columns: ColumnDef[] = first.chunks.map((chunk) => ({
    id: chunk.columnId,
    name: `c${String(chunk.columnId)}`,
    type: chunk.type,
    get declared(): never {
      throw new Error("Bench has no SQL catalog descriptors");
    },
    nullable: true,
    defaultSql: null,
    generatedSql: null,
    identitySequence: null,
    missing: null,
  }));
  const segments: GroupSegment[] = groups.map((g, index) => ({
    kind: "group",
    index,
    rowCount: g.rows,
    liveRowCount: g.rows,
    staged: false,
    get group(): GroupRef {
      const ref = roots?.[index];
      if (!ref) throw new Error("Scan-only fixture has no encoded group roots");
      return ref;
    },
    root: () => ({ rowCount: g.rows, rowIds: { kind: "dense" }, columns: g.chunks }),
    chunk: (id: number) => g.chunks.find((c) => c.columnId === id) ?? null,
    deletions: () => null,
    statistics: () => null,
    pages: () => {
      throw new Error("Unfiltered bench does not supply B3 page summaries");
    },
  }));
  return {
    tableId: 1,
    schemaVersion: 1,
    columns,
    segments,
    liveRowCount: groups.reduce((n, g) => n + g.rows, 0),
  };
}

/**
 * Executor admission needs encoded roots, even when catalog discovery is a fixture. Build
 * warm metadata before timing; production MetadataReader still charges parsing, and explicit
 * reads charge leases. Payload reads retain the original storage/cache/pool implementation.
 * The synthetic metadata address space is diagnostic only, never persisted or exported.
 */
export function benchSnapshotOf(space: ExtentReader, groups: readonly ExportGroup[]) {
  const metadata = new Map<string, Uint8Array>();
  const key = (ref: BlobRef) => `${blobAddress(ref)}:${ref.length}:${ref.checksum}`;
  let offset = 2 ** 52,
    firstRowId = 1;
  const roots: GroupRef[] = groups.map((group, groupId) => {
    const bytes = encodeGroupRoot({
      rowCount: group.rows,
      rowIds: { kind: "dense" },
      columns: group.chunks,
    });
    const root: BlobRef = {
      extent: { offset, length: bytes.length },
      offset: 0,
      length: bytes.length,
      checksum: xxHash32(bytes),
    };
    offset += Math.ceil(bytes.length / DATA_ALIGN) * DATA_ALIGN;
    metadata.set(key(root), bytes);
    const first = firstRowId;
    firstRowId += group.rows;
    return {
      groupId: groupId + 1,
      firstRowId: first,
      lastRowId: firstRowId - 1,
      rowCount: group.rows,
      liveRowCount: group.rows,
      root,
      deletions: null,
    };
  });
  const pages: ExtentReader = {
    peek: (ref, options) => metadata.get(key(ref)) ?? space.peek(ref, options),
    parsed: (ref) => space.parsed?.(ref) ?? null,
    attach: (ref, parsed, bytes, of) => space.attach?.(ref, parsed, bytes, of) ?? false,
    async read(refs, options) {
      if (refs.reduce((sum, ref) => sum + ref.length, 0) > MAX_READ_BYTES)
        throw new RangeError("A read asks for more than MAX_READ_BYTES");
      options.signal?.throwIfAborted();
      const stored = refs.filter((ref) => !metadata.has(key(ref))),
        held = refs.reduce((sum, ref) => sum + (metadata.has(key(ref)) ? ref.length : 0), 0);
      if (!options.budget.allocate(held)) throw new ReadDeniedError(held);
      let lease: ReadLease | null = null;
      try {
        lease = stored.length === 0 ? null : await space.read(stored, options);
        let index = 0,
          released = false;
        return {
          bytes: refs.map((ref) => {
            const bytes = metadata.get(key(ref)) ?? lease?.bytes[index++];
            if (!bytes) throw new Error("Fixture storage returned an incomplete lease");
            return bytes;
          }),
          release() {
            if (released) return;
            released = true;
            lease?.release();
            options.budget.free(held);
          },
        };
      } catch (error) {
        lease?.release();
        options.budget.free(held);
        throw error;
      }
    },
  };
  return { table: benchTableOf(groups, roots), pages };
}
