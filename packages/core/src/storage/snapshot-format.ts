import type { SnapshotMetadataItem } from "./types.js";

/**
 * Snapshot formats this build reads. Formats 2 and 3 are format 1's container and frames; each
 * admits one catalog record an older importer would restore wrongly:
 *
 * - format 2, the internal key-forwards catalog record (`keyForwards: true`), which a format-1
 *   importer would restore as an ordinary table;
 * - format 3, a column declared TIMESTAMP without time zone (`withoutTimeZone: true`), which a
 *   format-2 importer would refuse as corruption or restore as `timestamp with time zone`.
 *
 * A snapshot is written at the lowest format its records need, so one without either record is
 * still format 1, byte for byte, and older builds can import it.
 */
export type SnapshotFormatVersion = 1 | 2 | 3;

/** The snapshot format one exported metadata item needs. */
export function snapshotMetadataItemFormatVersion(
  item: SnapshotMetadataItem,
): SnapshotFormatVersion {
  if (item.kind !== "table") return 1;
  if (item.record.columns.some((column) => column.withoutTimeZone === true)) return 3;
  return item.record.keyForwards === true ? 2 : 1;
}

/** What an item needs a newer format for, as the refusal of an older label names it. */
export function snapshotMetadataItemFormatFeature(item: SnapshotMetadataItem): string {
  return snapshotMetadataItemFormatVersion(item) === 3
    ? "a TIMESTAMP without time zone column"
    : "a key-forwards record";
}

/** The format a snapshot needs once it also holds `item`. */
export function withSnapshotMetadataItem(
  current: SnapshotFormatVersion,
  item: SnapshotMetadataItem,
): SnapshotFormatVersion {
  const needed = snapshotMetadataItemFormatVersion(item);
  return needed > current ? needed : current;
}

/** Validates a stored or decoded snapshot format version. */
export function snapshotFormatVersion(value: unknown): SnapshotFormatVersion {
  if (value !== 1 && value !== 2 && value !== 3) {
    throw new TypeError("Unsupported snapshot format version");
  }
  return value;
}
