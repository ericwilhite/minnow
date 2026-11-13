/** Exact, selective FK fallback for an invalid nonunique supporting index. No complete row
 * copies or covering columns: only the FK leading fields, one row at a time, remain resident.
 * Native output/parse credit is returned before any outer metadata-pressure retry can wait. */
import { MinimumBudget } from "../memory/working-set.js";
import type { Held } from "../format/reachable.js";
import { ChunkReader } from "../page/reader.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import { allocateData, dataBytes, type FlatVector, type VectorData } from "../vector/vector.js";
import { compareBytes } from "../vector/varlen.js";
import type { PhysicalType } from "../types.js";
import { groupRootOf, type Memtable } from "./memtable.js";
import { MEMORY_CHUNK_ROWS, isNullAt } from "./columns.js";
import { charged, need } from "./bytes.js";
import type { TableSnapshotImpl } from "./view.js";

function output(type: PhysicalType, bytes: number): VectorData {
  switch (type.kind) {
    case "int64":
    case "time":
    case "timestamp":
      return { layout: "int64", values: { repr: "int64", values: new BigInt64Array(1) } };
    case "decimal":
      return {
        layout: "decimal",
        scale: null,
        scales: new Uint16Array(1),
        specials: new Uint8Array(1),
        coefficients: { repr: "int128", limbs: new Uint32Array(4) },
      };
    case "interval":
      return {
        layout: "interval",
        months: new Int32Array(1),
        days: new Int32Array(1),
        micros: { repr: "int64", values: new BigInt64Array(1) },
      };
    default:
      return allocateData(type, 1, bytes);
  }
}
export async function scanIndexHolders(
  table: TableSnapshotImpl,
  columnIds: readonly number[],
  key: Uint8Array,
): Promise<Held<readonly number[]>> {
  const snapshot = table.snapshot,
    budget = new MinimumBudget(snapshot.shared.budget, Number.MAX_SAFE_INTEGER);
  charged(budget, 1024);
  let retained = 1024,
    done = false,
    keys: KeyBatch | undefined;
  const release = () => {
    const n = retained;
    retained = 0;
    if (n > 0) budget.free(n);
  };
  try {
    const rows: number[] = [],
      positions = new Int32Array(1);
    keys = new KeyBatch(budget);
    const workspace = keys;
    let at = 0;
    // Equality tuple fields concatenate prefix-free encodings, so fields can be compared
    // individually. No borrowed field survives the next read or an encoder reset.
    const matches = (vector: FlatVector, row: number): boolean => {
      snapshot.check();
      if (isNullAt(vector, row)) return false;
      positions[0] = row;
      const selected = {
        form: "dictionary" as const,
        type: vector.type,
        length: 1,
        validity: null,
        dictionary: vector,
        codes: positions,
        dictionaryId: 0,
      };
      const result = encodeKeyBatch(
        "equality",
        [{ vector: selected, direction: "asc", nulls: "first" }],
        { length: 1, selection: null },
        workspace,
      );
      if (!result.done) throw new RangeError("One FK field did not fit");
      const candidate = workspace.key(0);
      if (
        candidate.length > key.length - at ||
        compareBytes(candidate, 0, candidate.length, key, at, at + candidate.length) !== 0
      )
        return false;
      at += candidate.length;
      return true;
    };
    for (const segment of table.segments) {
      const deletions = await segment.deletions();
      for (let position = 0; position < segment.rowCount; position++) {
        snapshot.check();
        if (deletions?.isDeleted(position)) continue;
        at = 0;
        let possible = true;
        for (const columnId of columnIds) {
          const column = need(table.columns.find((c) => c.id === columnId));
          if (segment.kind === "memory") {
            const memtable = (segment as unknown as { memtable: Memtable }).memtable;
            if (memtable.published === null) {
              const chunk = Math.floor(position / MEMORY_CHUNK_ROWS);
              memtable.borrow(chunk, -1);
              try {
                possible = matches(
                  need(memtable.chunks[table.columnIndex(columnId)]?.[chunk]).view(),
                  position % MEMORY_CHUNK_ROWS,
                );
              } finally {
                memtable.unborrow(chunk);
              }
              if (!possible) break;
              continue;
            }
          }
          const root =
            segment.kind === "group"
              ? await segment.root()
              : await groupRootOf(
                  need((segment as unknown as { memtable: Memtable }).memtable.published).groupRoot,
                );
          const stored = root.columns.find((c) => c.columnId === columnId);
          if (stored === undefined) {
            possible = false;
            break;
          }
          const reader = await ChunkReader.open(snapshot.shared.space, stored, {
            budget,
            once: true,
          });
          let native = 0;
          try {
            positions[0] = position;
            await reader.load(reader.pagesForRows(positions));
            const bytes = reader.gatherBytes(positions);
            if (bytes > key.length - at) {
              possible = false;
              break;
            }
            await reader.loadValuesOf(positions);
            const wanted = dataBytes(column.type, 1, bytes) + 1024;
            charged(budget, wanted);
            native = wanted;
            const vector: FlatVector = {
              form: "flat",
              type: column.type,
              length: 1,
              validity: new Uint32Array(1),
              data: output(column.type, bytes),
            };
            reader.gather(positions, vector, 0);
            possible = matches(vector, 0);
          } finally {
            reader.close();
            if (native > 0) budget.free(native);
          }
          if (!possible) break;
        }
        if (!possible || at !== key.length) continue;
        charged(budget, 64);
        retained += 64;
        rows.push(table.rowId(segment.index * 65536 + position));
      }
    }
    rows.sort((a, b) => a - b);
    done = true;
    return { value: rows, release };
  } finally {
    keys?.close();
    if (!done) {
      if (budget.allocated > 0) budget.free(budget.allocated);
      retained = 0;
    }
  }
}
