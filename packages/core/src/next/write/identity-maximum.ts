/** Sequence adoption scans one integer column, never unrelated payloads or whole decoded rows. */
import type { TableSnapshot, Snapshot } from "../access/snapshot.js";
import type { ColumnDef } from "../access/catalog.js";
import type { ByteBudget } from "../access/space.js";
import type { ColumnChunk } from "../format/manifest.js";
import { ChunkReader, PageBudgetError } from "../page/reader.js";
import { int64At } from "../vector/int64.js";
import type { FlatVector, Vector } from "../vector/vector.js";
import { isNullAt, sourceSlot, MEMORY_CHUNK_ROWS } from "./columns.js";
import { missingVector } from "./missing-value.js";
import { taskClock, yieldTask } from "../task.js";
export async function identityMaximum(
  snapshot: Snapshot,
  table: TableSnapshot,
  column: ColumnDef,
  budget: ByteBudget,
  check: () => void,
): Promise<bigint | null> {
  if (
    column.type.kind !== "int32" &&
    column.type.kind !== "int64" &&
    column.type.kind !== "float64"
  )
    throw new TypeError("Identity requires an integer");
  if (!budget.allocate(128)) throw new PageBudgetError(128);
  try {
    const output: FlatVector = {
      form: "flat",
      type: column.type,
      length: 1,
      validity: new Uint32Array(1),
      data:
        column.type.kind === "int32"
          ? { layout: "i32", values: new Int32Array(1) }
          : column.type.kind === "float64"
            ? { layout: "f64", values: new Float64Array(1) }
            : { layout: "int64", values: { repr: "int64", values: new BigInt64Array(1) } },
    };
    let maximum: bigint | null = null;
    const add = (vector: Vector, row: number) => {
      if (isNullAt(vector, row)) return;
      const slot = { flat: null as FlatVector | null, index: -1 };
      sourceSlot(vector, row, slot);
      const data = slot.flat.data;
      const value =
        data.layout === "i32" || data.layout === "f64"
          ? Number.isSafeInteger(data.values[slot.index] ?? 0)
            ? BigInt(data.values[slot.index] ?? 0)
            : null
          : data.layout === "int64"
            ? int64At(data.values, slot.index)
            : null;
      if (value === null) throw new TypeError("Identity column has another layout");
      if (maximum === null || value > maximum) maximum = value;
    };
    for (const segment of table.segments) {
      const deleted = await segment.deletions();
      check();
      const stored = async (chunk: ColumnChunk, from: number, count: number) => {
        const reader = await ChunkReader.open(snapshot.pages, chunk, { budget });
        try {
          const [first, end] = reader.pagesInRange(from, from + count);
          for (let page = first; page < end; page++) {
            check();
            await reader.load([page]);
            check();
            const entry = reader.directory.pages[page];
            if (!entry) throw new RangeError("Missing identity page");
            const start = Math.max(from, entry.firstRow),
              stop = Math.min(from + count, entry.firstRow + entry.rowCount);
            for (let row = start; row < stop; row++) {
              if (!deleted?.isDeleted(row)) {
                reader.decodeRange(row, row + 1, output, 0);
                add(output, 0);
              }
              if ((row & 255) === 255 && taskClock.due()) {
                await yieldTask();
                check();
              }
            }
            reader.release([page]);
            if (taskClock.due()) await yieldTask();
          }
        } finally {
          reader.close();
        }
      };
      if (segment.kind === "group") {
        await segment.root();
        check();
        const chunk = segment.chunk(column.id);
        if (chunk) await stored(chunk, 0, segment.rowCount);
        else if (segment.liveRowCount > 0) add(missingVector(column, 1), 0);
      } else {
        if (!segment.portableChunk) throw new TypeError("Identity adoption requires native chunks");
        for (let first = 0; first < segment.rowCount; first += MEMORY_CHUNK_ROWS) {
          check();
          const got = await segment.portableChunk(column.id, first / MEMORY_CHUNK_ROWS);
          check();
          const count = Math.min(MEMORY_CHUNK_ROWS, segment.rowCount - first);
          if (got.kind === "stored") await stored(got.chunk, first - got.firstRow, count);
          else
            for (let row = 0; row < count; row++) {
              if (!deleted?.isDeleted(first + row)) add(got.vector, row);
              if ((row & 255) === 255 && taskClock.due()) {
                await yieldTask();
                check();
              }
            }
          if (taskClock.due()) await yieldTask();
        }
      }
    }
    check();
    return maximum;
  } finally {
    budget.free(128);
  }
}
