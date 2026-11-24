/**
 * What a step of rows charges the write path's row account when it is stored in memtables: the
 * chunks and memtables it opens, the arenas it grows and the key deltas it extends. An insert
 * (write/transaction.ts `TableWriterImpl`) and log replay (write/engine.ts `#replayRows`) admit
 * this before they append, so a memtable append never meets a refused charge it could have
 * waited for. A step is at most `MEMORY_CHUNK_ROWS` rows.
 */
import type { TableDef } from "../access/catalog.js";
import { layoutOf } from "../types.js";
import { BATCH_TARGET_BYTES, type FlatVector, type Vector } from "../vector/vector.js";
import { MEMORY_CHUNK_ROWS, newChunkBytes, sourceSlot } from "./columns.js";
import { freshDeltaAppendBytes, KEY_DELTA_INITIAL_BYTES } from "./key-hash.js";
import type { Memtable } from "./memtable.js";
import { need } from "./bytes.js";

/** Per-row undo an insert of a table with full-text columns keeps (write/transaction.ts). */
export const FTS_ROW_UNDO_BYTES = 32;

/** A bounded slice of physical rows or of a selected-row list. No per-row iterator/result. */
export interface RowPlan {
  readonly rows: Uint16Array | null;
  readonly from: number;
  readonly count: number;
}

/** The table a step is stored into, as the step sees it when it is sized. */
export interface StepTable {
  /** The stored columns (`input[c]` is column `c`) and the table's indexes. */
  readonly def: TableDef;
  /** Whether an index is invalid (its keys are not kept). */
  invalid(indexId: number): boolean;
  /** The table's newest memtable, if any (`liveMemtable` decides whether it takes the rows). */
  live(): Memtable | undefined;
  /** The memtable row limit a new memtable takes, when configured. */
  readonly memtableRows: number | undefined;
  /** The input vector of a key column. */
  key(input: readonly Vector[], columnId: number): Vector;
}

export function inputBytes(vector: Vector, plan: RowPlan): number {
  if (vector.form === "flat" && vector.data.layout === "varlen" && plan.rows === null)
    return (
      (vector.data.offsets[plan.from + plan.count] ?? 0) - (vector.data.offsets[plan.from] ?? 0)
    );
  if (
    vector.type.kind !== "text" &&
    vector.type.kind !== "bytes" &&
    vector.type.kind !== "enum" &&
    vector.type.kind !== "json" &&
    vector.type.kind !== "array"
  )
    return 0;
  const slot: { flat: FlatVector | null; index: number } = { flat: null, index: -1 };
  let bytes = 0;
  for (let i = plan.from; i < plan.from + plan.count; i++) {
    sourceSlot(vector, plan.rows === null ? i : (plan.rows[i] ?? 0), slot);
    if (slot.index < 0 || slot.flat.data.layout !== "varlen") continue;
    bytes +=
      (slot.flat.data.offsets[slot.index + 1] ?? 0) - (slot.flat.data.offsets[slot.index] ?? 0);
  }
  return bytes;
}

/**
 * What the step's rows open before they are stored: each new chunk's fixed arrays for every
 * column (`newChunkBytes`), and for a new memtable its key deltas and summary arrays. A step
 * takes at most `MEMORY_CHUNK_ROWS` rows; the live memtable (when it can take rows, as
 * `liveMemtable` decides) opens the chunks its rows reach past its last, and a new memtable
 * takes what it cannot.
 */
export function opening(t: StepTable, input: readonly Vector[], plan: RowPlan): number {
  const columns = t.def.columns;
  let chunkSet = 0,
    varlen = 0;
  for (const [c, column] of columns.entries()) {
    chunkSet += newChunkBytes(column.type);
    if (layoutOf(column.type) !== "varlen") continue;
    varlen++;
    // The chunk's 256-byte arena may double several times within the step: its final size
    // and the replaced half beside it (as in `#growth`), beyond the `2 × input` the estimate
    // already counts for these bytes.
    const data = inputBytes(need(input[c]), plan);
    if (data > 256) {
      let size = 512;
      while (size < data) size *= 2;
      chunkSet += Math.max(0, size + size / 2 - 256 - 2 * data);
    }
  }
  // A new memtable's key deltas start empty: their growth for the rows it may take.
  let freshDeltas = 0;
  for (const index of t.def.indexes)
    if (!t.invalid(index.id))
      freshDeltas += freshDeltaAppendBytes(
        plan.count,
        plan.count,
        keyBytes(t, input, plan, index.columnIds),
      );
  const memtable = (limit: number) =>
    t.def.indexes.length * KEY_DELTA_INITIAL_BYTES +
    freshDeltas +
    columns.length * 4 +
    2 * 8 * varlen * Math.ceil(limit / MEMORY_CHUNK_ROWS);
  const live = t.live();
  let bytes = 0,
    rest = plan.count;
  if (
    live?.state === "live" &&
    live.schemaVersion === t.def.schemaVersion &&
    !live.full &&
    rest > 0
  ) {
    const take = Math.min(rest, live.capacity);
    const first = Math.floor(live.rowCount / MEMORY_CHUNK_ROWS),
      last = Math.floor((live.rowCount + take - 1) / MEMORY_CHUNK_ROWS),
      existing = live.chunks[0]?.length ?? 0;
    bytes += chunkSet * Math.max(0, last - Math.max(first, existing) + 1);
    // A memtable charges its summaries with its first rows.
    if (live.rowCount === 0)
      bytes += memtable(live.limit) - t.def.indexes.length * KEY_DELTA_INITIAL_BYTES - freshDeltas;
    rest -= take;
  }
  if (rest > 0)
    bytes +=
      memtable(t.memtableRows ?? live?.limit ?? MEMORY_CHUNK_ROWS) +
      chunkSet * Math.ceil(rest / MEMORY_CHUNK_ROWS);
  return bytes;
}

/**
 * What an insert step charges beyond its per-row metadata (`#rowMetadata`), recomputed after
 * relief: a freeze or stage while the step waits changes the memtable its rows reach.
 * - **Rows into chunks the live memtable already has** (`#intoLiveChunks`): exactly what they
 *   allocate there, the arenas' growth with the resize overlap (`#growth`) and each chunk's
 *   lazy arrays (`ColumnChunk.lazyBytes`).
 * - **Otherwise:** what the step opens (`#opening`), plus the larger of `2 × input` (a fresh
 *   arena: the next power of two at most doubles the value) and the live arenas' growth.
 *   Before, both were charged (their sum), 2.0-3.0× what the step allocates.
 * Both add the key deltas' growth (`#deltaGrowth`). Native capacity growth is paid before the
 * append; a frozen or staged memtable's arenas count nothing here, so the next small batch
 * never waits for a stale multi-MiB growth.
 */
export function stepCharge(t: StepTable, columns: readonly Vector[], plan: RowPlan): number {
  const live = t.live();
  if (live !== undefined && intoLiveChunks(t, live, plan)) {
    let lazy = 0;
    for (const list of live.chunks) lazy += list.at(-1)?.lazyBytes ?? 0;
    return growth(t, columns, plan) + lazy + deltaGrowth(t, columns, plan);
  }
  let input = 0;
  for (const vector of columns) input += inputBytes(vector, plan);
  return (
    opening(t, columns, plan) +
    Math.max(2 * input, growth(t, columns, plan)) +
    deltaGrowth(t, columns, plan)
  );
}

/** Whether `liveMemtable` keeps `live` for the step and every row lands in its last chunk,
 * so the step opens no chunk, memtable or summary. */
export function intoLiveChunks(t: StepTable, live: Memtable, plan: RowPlan): boolean {
  return (
    live.state === "live" &&
    live.schemaVersion === t.def.schemaVersion &&
    !live.full &&
    live.rowCount > 0 &&
    live.capacity >= plan.count &&
    Math.floor((live.rowCount + plan.count - 1) / MEMORY_CHUNK_ROWS) < (live.chunks[0]?.length ?? 0)
  );
}

/** Per-row metadata an insert step charges: row ids, index entries and FTS undo. */
export function rowMetadata(t: StepTable, columns: readonly Vector[], plan: RowPlan): number {
  return plan.count * (16 * columns.length + 96 * t.def.indexes.length + FTS_ROW_UNDO_BYTES);
}

/** Each index's key bytes for the step's rows (fixed per-key room plus twice the input). */
export function keyBytes(
  t: StepTable,
  columns: readonly Vector[],
  plan: RowPlan,
  columnIds: readonly number[],
): number {
  let wanted = 32 * plan.count * columnIds.length;
  for (const id of columnIds) wanted += 2 * inputBytes(t.key(columns, id), plan);
  return wanted;
}

/** The live memtable's key deltas' growth for the step (`KeyDelta.appendBytes`); a new
 * memtable's deltas are in `#opening`. */
export function deltaGrowth(t: StepTable, columns: readonly Vector[], plan: RowPlan): number {
  const live = t.live();
  if (live?.state !== "live") return 0;
  let bytes = 0;
  for (const index of t.def.indexes) {
    if (t.invalid(index.id)) continue;
    bytes +=
      live.deltas
        .get(index.id)
        ?.appendBytes(plan.count, plan.count, keyBytes(t, columns, plan, index.columnIds)) ?? 0;
  }
  return bytes;
}

/**
 * The live memtable's growth for this step: each varlen arena the input outgrows, at its next
 * power of two, with the resize overlap. A resize charges the new arena before it frees the
 * old one. An arena the step finds is already charged, so one resize adds the new size; a
 * batch that resizes the same arena again holds the previous new arena (charged in this step)
 * beside the next one, at most half of it: `max(0, size / 2 − current)` more. Plus each
 * index's key room.
 */
export function growth(t: StepTable, columns: readonly Vector[], plan: RowPlan): number {
  const live = t.live();
  if (live?.state !== "live") return 0;
  let bytes = 0;
  for (let c = 0; c < columns.length; c++) {
    const chunk = live.chunks[c]?.at(-1),
      arena = chunk?.arena;
    if (chunk === undefined || arena == null) continue;
    const wanted = (chunk.offsets?.[chunk.length] ?? 0) + inputBytes(need(columns[c]), plan);
    if (wanted <= arena.length) continue;
    let size = arena.length * 2;
    while (size < wanted) size *= 2;
    // One row resizes an arena once (`ColumnChunk.#putBytes` jumps to the size it needs).
    bytes += size + (plan.count > 1 ? Math.max(0, size / 2 - arena.length) : 0);
  }
  return bytes;
}

/** Only key-bearing input needs the new byte-sliced ingress. Large unindexed payloads keep
 * their existing admission/publication boundaries. Escaping may double raw key bytes; fixed
 * components and tuple headers fit within the per-column upper bound used here. */
export function wideKeys(t: StepTable, columns: readonly Vector[], plan: RowPlan): boolean {
  let bytes = 0;
  for (const index of t.def.indexes) {
    if (t.invalid(index.id)) continue;
    bytes += plan.count * 17 * index.columnIds.length;
    for (const id of index.columnIds) bytes += 2 * inputBytes(t.key(columns, id), plan);
    if (bytes > BATCH_TARGET_BYTES) return true;
  }
  return false;
}

/** The step's size for slicing wide key input (`BATCH_TARGET_BYTES`): what it opens, its
 * per-row metadata and twice its input (a contiguous varlen range uses its two end offsets).
 * What it is charged is `#rowMetadata` + `#stepCharge`. */
export function estimate(t: StepTable, columns: readonly Vector[], plan: RowPlan): number {
  let bytes = opening(t, columns, plan) + rowMetadata(t, columns, plan);
  for (const vector of columns) bytes += 2 * inputBytes(vector, plan);
  return bytes;
}
