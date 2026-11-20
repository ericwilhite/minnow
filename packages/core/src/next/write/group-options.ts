/** Actual immutable memtable dimensions, shared by preflight and group construction. */
import type { GroupWriterOptions, WriterColumn } from "../page/writer.js";
import type { Memtable } from "./memtable.js";

export function* nativeGroupOptions(
  memtable: Memtable,
): Generator<void, Omit<GroupWriterOptions, "writer" | "charge">> {
  const columns: WriterColumn[] = [];
  for (let i = 0; i < memtable.columns.length; i++) {
    const column = memtable.columns[i];
    if (!column) throw new Error("Missing native group column");
    let bytes = 0,
      maximum = 0,
      dictionary = 0,
      largeValues = 0,
      varlen = false;
    for (const chunk of memtable.chunks[i] ?? []) {
      if (chunk.offsets !== null) {
        varlen = true;
        bytes += chunk.pageValueBytes;
        maximum = Math.max(maximum, chunk.maxPageValueBytes);
        dictionary += chunk.dictionaryValueBytes;
        largeValues += chunk.largeValueCount;
      }
      yield;
    }
    columns.push({
      columnId: column.id,
      type: column.type,
      ...(varlen
        ? {
            valueBytes: bytes,
            maxValueBytes: maximum,
            dictionaryValueBytes: dictionary,
            largeValues,
          }
        : {}),
    });
    yield;
  }
  return { columns, maxRows: memtable.rowCount, estimatedRows: memtable.rowCount };
}
