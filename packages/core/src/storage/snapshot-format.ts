import type { SnapshotMetadataItem } from "./types.js";

/**
 * Snapshot formats this build reads. Format 2 is format 1's container and frames; it differs only
 * in admitting the internal key-forwards catalog record (`keyForwards: true`), which a format-1
 * importer would restore as an ordinary table. A snapshot without that record is still written
 * as format 1, byte for byte, so older builds can import it.
 */
export type SnapshotFormatVersion = 1 | 2;

/** The snapshot format one exported metadata item needs. */
export function snapshotMetadataItemFormatVersion(
  item: SnapshotMetadataItem,
): SnapshotFormatVersion {
  return item.kind === "table" && item.record.keyForwards === true ? 2 : 1;
}

/** Validates a stored or decoded snapshot format version. */
export function snapshotFormatVersion(value: unknown): SnapshotFormatVersion {
  if (value !== 1 && value !== 2) throw new TypeError("Unsupported snapshot format version");
  return value;
}
