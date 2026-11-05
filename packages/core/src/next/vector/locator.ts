/**
 * Row locators: how late materialization names a row it has not fetched yet (PROPOSAL "Late
 * materialization"; review finding 10).
 *
 * ## Representation (frozen)
 * A locator is one f64 slot of a vector whose type is `{ kind: "locator", tableId }`:
 * `segment × 65,536 + position`, exact while segment < 2^37.
 * - `segment` indexes the **snapshot's segment list** for that table: the pinned snapshot lists
 *   the table's groups (in row-id order) followed by its frozen memtable views. It is not a
 *   durable group id; it means nothing outside the snapshot.
 * - `position` is the row's position inside that segment (< 65,536: groups and memtable views
 *   never exceed 65,536 rows).
 * - A null locator (validity bit clear) is a null-extended row of an outer join: every column
 *   fetched through it is null.
 *
 * ## Lifetime
 * - A query pins one snapshot when it starts (root + memtable views) and releases it when it ends,
 *   fails or is cancelled. Locators are valid only while that pin is held, so they never cross
 *   queries, results or connections.
 * - Maintenance never invalidates a pinned snapshot: merges publish new roots and reclaim only
 *   space no pinned root reaches (format/manifest.ts "Reclaim"). Deletions after the pin are
 *   invisible to it.
 * - Spill keeps locators as plain f64 columns; the pin outlives every spill file of its query, so
 *   they stay valid when read back.
 *
 * ## Fetch
 * The IR's `fetch` operator (ir/ir.ts) turns locators into columns through a `RowFetcher`, which
 * sorts the batch's locators by (segment, page), reads each page once, and scatters values back
 * into the batch's row order. One fetch call serves a whole batch; it awaits page reads, so
 * `fetch` is an async point of the pipeline (exec/pipeline.ts).
 */
export const LOCATOR_POSITIONS = 65_536;
export const MAX_LOCATOR_SEGMENTS = 2 ** 37;

export function packLocator(segment: number, position: number): number {
  if (!(position >= 0 && position < LOCATOR_POSITIONS && Number.isInteger(position)))
    throw new RangeError("Locator position out of range");
  if (!(segment >= 0 && segment < MAX_LOCATOR_SEGMENTS && Number.isInteger(segment)))
    throw new RangeError("Locator segment out of range");
  return segment * LOCATOR_POSITIONS + position;
}

export function locatorSegment(locator: number): number {
  return Math.floor(locator / LOCATOR_POSITIONS);
}

export function locatorPosition(locator: number): number {
  return locator % LOCATOR_POSITIONS;
}
