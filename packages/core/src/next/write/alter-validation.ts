/** Tightening checks only live validity bits; large values are never decoded for NOT NULL. */
import type { TableDef } from "../access/catalog.js";
import { NotNullViolationError } from "../access/modify.js";
import type { ColumnChunk } from "../format/manifest.js";
import { ChunkReader, PageBudgetError } from "../page/reader.js";
import { taskClock, yieldTask } from "../task.js";
import { isNullAt, MEMORY_CHUNK_ROWS } from "./columns.js";
import { missingScalar } from "./missing-value.js";
import type { TableSnapshotImpl } from "./view.js";

export async function validateAlterNullability(
  table: TableSnapshotImpl,
  next: TableDef,
  check: () => void,
): Promise<void> {
  const tightened = next.columns.filter(
    (column) =>
      !column.nullable && table.columns.find((prior) => prior.id === column.id)?.nullable !== false,
  );
  if (!tightened.length) return;
  const budget = table.budget;
  const bytes = 65536;
  if (!budget.allocate(bytes)) throw new PageBudgetError(bytes);
  try {
    const mask = new Uint8Array(bytes);
    for (const column of tightened)
      for (const segment of table.segments) {
        check();
        if (segment.liveRowCount === 0) continue;
        const deletions = await segment.deletions();
        const fail = (row: number) => {
          throw new NotNullViolationError(table.tableId, column.id, row);
        };
        const stored = async (chunk: ColumnChunk, first: number, length: number) => {
          if (chunk.statistics.nullCount === 0) return;
          const reader = await ChunkReader.open(table.snapshot.pages, chunk, { budget });
          try {
            const nulls = reader.compilePredicate({ op: "isNull" });
            if (!nulls) throw new TypeError("Missing NULL predicate");
            const [start, end] = reader.pagesInRange(first, first + length);
            for (let page = start; page < end; page++) {
              check();
              await reader.load([page]);
              check();
              const entry = reader.directory.pages[page];
              if (!entry) throw new RangeError("Missing ALTER validation page");
              const from = Math.max(first, entry.firstRow),
                to = Math.min(first + length, entry.firstRow + entry.rowCount);
              reader.selectMask(nulls, from, to, mask);
              for (let row = from; row < to; row++)
                if (mask[row - from] === 1 && !deletions?.isDeleted(row)) fail(row);
              reader.release([page]);
              if (taskClock.due()) await yieldTask();
            }
          } finally {
            reader.close();
          }
        };
        if (!table.columns.some((prior) => prior.id === column.id)) {
          if (isNullAt(missingScalar(column), 0)) fail(-1);
        } else if (segment.kind === "group") {
          await segment.root();
          check();
          const chunk = segment.chunk(column.id);
          if (chunk) await stored(chunk, 0, segment.rowCount);
          else if (isNullAt(missingScalar(column), 0)) fail(-1);
        } else {
          if (!segment.portableChunk) throw new TypeError("ALTER requires native memory chunks");
          for (let first = 0; first < segment.rowCount; first += MEMORY_CHUNK_ROWS) {
            const got = await segment.portableChunk(column.id, first / MEMORY_CHUNK_ROWS);
            check();
            const length = Math.min(MEMORY_CHUNK_ROWS, segment.rowCount - first);
            if (got.kind === "stored") await stored(got.chunk, first - got.firstRow, length);
            else
              for (let row = 0; row < length; row++)
                if (!deletions?.isDeleted(first + row) && isNullAt(got.vector, row))
                  fail(first + row);
            if (taskClock.due()) await yieldTask();
          }
        }
      }
    check();
  } finally {
    budget.free(bytes);
  }
}
