/**
 * Reading stored rows by row id, for the write path's own needs: the old row an UPDATE completes
 * and re-inserts, the index keys a DELETE tombstones, and index backfill. Rows come from a
 * memtable (copied from its chunk views) or from a durable group (decoded through the page
 * format's chunk reader, pages read once per call). Output: one stored-form vector per column, in
 * request order, built in `ColumnChunk`s (so at most `MEMORY_CHUNK_ROWS` rows per call).
 */
import type { MaybePromise } from "../access/key-index.js";
import { ChunkReader } from "../page/reader.js";
import type { FlatVector } from "../vector/vector.js";
import { allocateData } from "../vector/vector.js";
import { ColumnChunk, MEMORY_CHUNK_ROWS } from "./columns.js";
import type { TableSnapshotImpl } from "./view.js";

const ONE = new Uint16Array(1);

/** Rows `rowIds` (≤ MEMORY_CHUNK_ROWS) of `table`, every column in catalog order. */
export function readRows(
  table: TableSnapshotImpl,
  rowIds: readonly number[],
): MaybePromise<FlatVector[]> {
  if (rowIds.length > MEMORY_CHUNK_ROWS) throw new RangeError("readRows reads one chunk of rows");
  const columns = table.columns;
  const out = columns.map((column) => new ColumnChunk(column.type));
  // Group the rows by segment, keeping their output slots.
  const bySegment = new Map<number, Array<{ slot: number; position: number }>>();
  rowIds.forEach((rowId, slot) => {
    const locator = table.locator(rowId);
    const segment = Math.floor(locator / 65_536);
    let list = bySegment.get(segment);
    if (list === undefined) {
      list = [];
      bySegment.set(segment, list);
    }
    list.push({ slot, position: locator % 65_536 });
  });
  // Each output column is filled slot by slot in request order: stage per-slot sources first.
  const sources = new Array<{ vectors: FlatVector[]; index: number } | undefined>(rowIds.length);
  const pending: Array<Promise<void>> = [];
  for (const [segmentIndex, rows] of bySegment) {
    const segment = table.segments[segmentIndex];
    if (segment === undefined) throw new RangeError("A row id names no segment");
    if (segment.kind === "memory") {
      const memtable = (segment as unknown as { memtable: import("./memtable.js").Memtable })
        .memtable;
      if (memtable.published === null) {
        for (const { slot, position } of rows) {
          const chunk = Math.floor(position / MEMORY_CHUNK_ROWS);
          const vectors = columns.map((_, c) => {
            const stored = memtable.chunks[c]?.[chunk];
            if (stored === undefined) throw new RangeError("A memtable row is missing");
            return stored.view();
          });
          sources[slot] = { vectors, index: position % MEMORY_CHUNK_ROWS };
        }
        continue;
      }
      pending.push(readGroupRows(table, memtable.published.groupRoot, rows, sources));
      continue;
    }
    const root = segment.root();
    if (root instanceof Promise)
      pending.push(root.then((resolved) => readGroupRows(table, resolved, rows, sources)));
    else pending.push(readGroupRows(table, root, rows, sources));
  }
  const assemble = (): FlatVector[] => {
    for (let slot = 0; slot < rowIds.length; slot++) {
      const source = sources[slot];
      if (source === undefined) throw new RangeError("A row was not read");
      ONE[0] = source.index;
      out.forEach((chunk, c) => {
        const vector = source.vectors[c];
        if (vector === undefined) throw new RangeError("A row lacks a column");
        chunk.append(vector, ONE, 0, 1);
      });
    }
    return out.map((chunk) => chunk.view());
  };
  return pending.length === 0 ? assemble() : Promise.all(pending).then(assemble);
}

async function readGroupRows(
  table: TableSnapshotImpl,
  root: import("../format/manifest.js").GroupRoot,
  rows: ReadonlyArray<{ slot: number; position: number }>,
  sources: Array<{ vectors: FlatVector[]; index: number } | undefined>,
): Promise<void> {
  const shared = table.snapshot.shared;
  const positions = Int32Array.from(
    [...rows].sort((a, b) => a.position - b.position),
    (row) => row.position,
  );
  const order = [...rows].sort((a, b) => a.position - b.position);
  const vectors: FlatVector[] = [];
  for (const column of table.columns) {
    const stored = root.columns.find((chunk) => chunk.columnId === column.id);
    if (stored === undefined) {
      // The group predates the column: every row reads as NULL (no defaults stored yet).
      const vector: FlatVector = {
        form: "flat",
        type: column.type,
        length: positions.length,
        validity: new Uint32Array(Math.ceil(positions.length / 32)),
        data: allocateData(column.type, positions.length),
      };
      vectors.push(vector);
      continue;
    }
    const reader = await ChunkReader.open(shared.space, stored, { budget: shared.budget });
    try {
      await reader.load(reader.pagesForRows(positions));
      // Out-of-line values decode only once loaded.
      await reader.loadValuesOf(positions);
      const bytes = reader.gatherBytes(positions);
      const target: FlatVector = {
        form: "flat",
        type: column.type,
        length: 0,
        validity:
          stored.statistics.nullCount === 0
            ? null
            : new Uint32Array(Math.ceil(positions.length / 32)),
        data: allocateData(column.type, positions.length, bytes),
      };
      reader.gather(positions, target, 0);
      target.length = positions.length;
      vectors.push(target);
    } finally {
      reader.close();
    }
  }
  order.forEach((row, k) => {
    sources[row.slot] = { vectors, index: k };
  });
}
