/** Accepted UPDATE images only. A bounded optional accelerator: capture refusal requests reset,
 * never fails a legal write. These bytes are owner-session state, never WAL or stored format.
 */
import { stringBytes } from "../memory/governor.js";
import type { RekeyEffect } from "../access/changes.js";
import type { TableDef } from "../access/catalog.js";
import type { Vector } from "../vector/vector.js";
import type { ByteBudget } from "../storage/extents.js";
import { encodeKeyBatch, type KeyBatch } from "../keys/key-encoding.js";
import { ByteWriter } from "./bytes.js";
import { encodeColumn, sourceSlot } from "./columns.js";

const KEY_BYTES = 2048;
export const LIVE_REKEY_TRANSACTION_BYTES = 128 * 1024;
export function captureRekey(
  table: TableDef,
  old: readonly Vector[],
  next: readonly Vector[],
  row: number,
  fromRowId: number,
  toRowId: number,
  keys: KeyBatch,
  budget: ByteBudget,
  kind: "update" | "delete",
): RekeyEffect | "reset" | null {
  const index = table.indexes.find((candidate) => candidate.id === table.primaryKey);
  if (index === undefined) return null;
  let bytes = 8192 + stringBytes(table.name);
  for (const id of index.columnIds) {
    const column = table.columns.find((candidate) => candidate.id === id);
    if (column === undefined) return "reset";
    bytes += 2 * KEY_BYTES + 1024 + stringBytes(column.name);
  }
  let charged = false;
  try {
    if (!budget.allocate(bytes)) return "reset";
    charged = true;
    const positions = index.columnIds.map((id) =>
      table.columns.findIndex((column) => column.id === id),
    );
    const selection = { rows: Uint16Array.of(row), count: 1 };
    const encode = (vectors: readonly Vector[]) => {
      const result = encodeKeyBatch(
        "equality",
        positions.map((position) => ({
          vector: required(vectors[position]),
          direction: "asc" as const,
          nulls: "first" as const,
        })),
        { length: vectors[0]?.length ?? 0, selection },
        keys,
      );
      if (result.encoded !== 1 || keys.key(0).length > KEY_BYTES)
        throw new RangeError("Wide live key");
      return keys.key(0).slice();
    };
    const fromKey = encode(old),
      toKey = encode(next);
    const keyChanged = !(
      fromKey.length === toKey.length && fromKey.every((byte, i) => byte === toKey[i])
    );
    const slot = { flat: null, index: -1 } as Parameters<typeof sourceSlot>[2];
    const image = (vectors: readonly Vector[]) =>
      positions.map((position) => {
        const vector = vectors[position];
        if (vector === undefined) throw new Error("Missing live key column");
        sourceSlot(vector, row, slot);
        if (slot.index < 0) throw new Error("Null live key");
        const writer = new ByteWriter(KEY_BYTES, undefined, KEY_BYTES);
        encodeColumn(slot.flat, slot.index, 1, writer);
        return writer.bytes.subarray(0, writer.length);
      });
    const from = image(old),
      to = image(next);
    return {
      lsn: 0,
      fromRowId,
      toRowId,
      keyChanged,
      kind,
      tableId: table.id,
      table: table.name,
      bytes,
      columns: positions.map((position) => {
        const column = table.columns[position];
        if (column === undefined) throw new Error("Missing key definition");
        return {
          name: column.name,
          type: column.type.kind === "enum" ? { kind: "text" as const } : column.type,
        };
      }),
      from,
      to,
    };
  } catch {
    if (charged) budget.free(bytes);
    return "reset";
  }
}

function required<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("Missing key vector");
  return value;
}
