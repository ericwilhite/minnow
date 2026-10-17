/**
 * Delta merges: a run of level-zero updates and deletes merged among themselves, leaving every
 * base partition as it is (`DeltaMergeCompactionRewritePlan` in storage/types.ts).
 *
 * Everything here is synchronous and storage-free. The engine reads and decodes the sources;
 * this module resolves each key's net change over the run, cuts the result into blocks, and
 * checksums exactly the physical bytes those blocks hold, so a merge resumed on another
 * connection writes the same blocks or none.
 *
 * The net change of one key over the run, replayed in source order:
 * - a delete leaves the key deleted, whatever came before it;
 * - an update after a delete changes nothing (a reader ignores an update to a missing key, and
 *   a valid history has none: nothing in the run can insert the key again);
 * - an update otherwise sets the columns it carries, a later value replacing an earlier one;
 * - an update carrying only the key (its columns dropped since) changes nothing.
 * The merge publishes the deleted keys as one delete, and the updated keys grouped by the set
 * of columns they change, each group one update. Every key appears once, so the outputs commute
 * with each other and replay at the end of the run exactly as the run did.
 */
import {
  crc32Continue,
  encodeColumn,
  type ColumnInput,
  type LogicalType,
} from "../block-format/index.js";
import { dateMilliseconds } from "../date-value.js";
import type { BatchValue } from "./batch.js";
import { planAlignedWriteBlockRanges } from "./write-block-planner.js";

export interface DeltaMergeColumn {
  readonly columnId: string;
  readonly type: LogicalType;
}

/** One source segment, decoded. */
export interface DeltaMergeSourceRows {
  readonly kind: "update" | "delete";
  readonly rowCount: number;
  /** Every column the source holds, the key column among them, by column ID. */
  readonly columns: ReadonlyMap<string, readonly BatchValue[]>;
}

/** One segment the merge publishes, rows in ascending key order. */
export interface DeltaMergeOutputRows {
  readonly kind: "delete" | "update";
  /** The key column first, then the changed columns in table order. */
  readonly columns: readonly DeltaMergeColumn[];
  /** Per column, aligned with `columns`. */
  readonly values: ReadonlyArray<readonly BatchValue[]>;
  readonly rowCount: number;
}

/** One output block's physical payload, in canonical order: output, then column, then part. */
export interface DeltaMergeOutputBlock {
  readonly outputIndex: number;
  readonly columnIndex: number;
  readonly part: number;
  readonly type: LogicalType;
  readonly rowCount: number;
  readonly bytes: Uint8Array;
}

type MergeKey = string | number | boolean;

interface KeyState {
  readonly key: Exclude<BatchValue, null>;
  deleted: boolean;
  /** Per table column: the newest value the run set, or undefined where it set none. */
  values: Array<BatchValue | undefined> | undefined;
}

function mergeKeyOf(type: LogicalType, value: BatchValue | undefined): MergeKey {
  if (value === null || value === undefined) throw new TypeError("A delta key cannot be null");
  switch (type) {
    case "boolean":
      if (typeof value !== "boolean") throw new TypeError("Invalid boolean delta key");
      return value;
    case "number":
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw new TypeError("Invalid number delta key");
      }
      return value;
    case "string":
      if (typeof value !== "string") throw new TypeError("Invalid string delta key");
      return value;
    case "datetime":
      if (!(value instanceof Date) || !Number.isFinite(dateMilliseconds(value))) {
        throw new TypeError("Invalid datetime delta key");
      }
      return dateMilliseconds(value);
  }
}

function presentKey(value: BatchValue | undefined): Exclude<BatchValue, null> {
  if (value === null || value === undefined) throw new TypeError("A delta key cannot be null");
  return value;
}

function compareMergeKeys(left: MergeKey, right: MergeKey): number {
  if (typeof left === "boolean") return Number(left) - Number(right);
  if (typeof left === "number") return left - (right as number);
  return left < right ? -1 : left > right ? 1 : 0;
}

/**
 * Resolves a run's net change per key. `columns` is the table's columns in table order;
 * `sources` are the run's segments in canonical order.
 */
export function resolveDeltaMerge(
  columns: readonly DeltaMergeColumn[],
  keyColumnId: string,
  sources: readonly DeltaMergeSourceRows[],
): DeltaMergeOutputRows[] {
  const keyIndex = columns.findIndex((column) => column.columnId === keyColumnId);
  const keyColumn = columns[keyIndex];
  if (keyColumn === undefined) throw new TypeError("A delta merge needs its key column");
  const columnIndexById = new Map(columns.map((column, index) => [column.columnId, index]));
  const states = new Map<MergeKey, KeyState>();
  for (const source of sources) {
    const keys = source.columns.get(keyColumnId);
    if (keys?.length !== source.rowCount) {
      throw new TypeError("A delta merge source must hold one key per row");
    }
    if (source.kind === "delete") {
      if (source.columns.size !== 1) {
        throw new TypeError("A delta merge delete source holds only its key");
      }
      for (const key of keys) {
        const token = mergeKeyOf(keyColumn.type, key);
        const state = states.get(token);
        if (state === undefined) {
          states.set(token, {
            key: presentKey(key),
            deleted: true,
            values: undefined,
          });
        } else {
          state.deleted = true;
          state.values = undefined;
        }
      }
      continue;
    }
    const changed: Array<{ index: number; values: readonly BatchValue[] }> = [];
    for (const [columnId, values] of source.columns) {
      if (columnId === keyColumnId) continue;
      const index = columnIndexById.get(columnId);
      if (index === undefined) throw new TypeError(`Delta merge column is unknown: ${columnId}`);
      if (values.length !== source.rowCount) {
        throw new TypeError("A delta merge source column must hold one value per row");
      }
      changed.push({ index, values });
    }
    // An update left with only its key (its columns dropped since) changes nothing.
    if (changed.length === 0) continue;
    for (let row = 0; row < source.rowCount; row += 1) {
      const key = keys[row];
      const token = mergeKeyOf(keyColumn.type, key);
      let state = states.get(token);
      if (state?.deleted === true) continue;
      if (state === undefined) {
        state = { key: presentKey(key), deleted: false, values: undefined };
        states.set(token, state);
      }
      const values = (state.values ??= new Array<BatchValue | undefined>(columns.length));
      for (const column of changed) values[column.index] = column.values[row];
    }
  }

  const deleted: Array<[MergeKey, KeyState]> = [];
  const updates = new Map<string, { indexes: number[]; rows: Array<[MergeKey, KeyState]> }>();
  for (const entry of states) {
    const state = entry[1];
    if (state.deleted) {
      deleted.push(entry);
      continue;
    }
    const indexes: number[] = [];
    for (let index = 0; index < columns.length; index += 1) {
      if (index !== keyIndex && state.values?.[index] !== undefined) indexes.push(index);
    }
    if (indexes.length === 0) continue;
    const signature = indexes.join(",");
    let group = updates.get(signature);
    if (group === undefined) {
      group = { indexes, rows: [] };
      updates.set(signature, group);
    }
    group.rows.push(entry);
  }

  const byKey = (left: [MergeKey, KeyState], right: [MergeKey, KeyState]): number =>
    compareMergeKeys(left[0], right[0]);
  const outputs: DeltaMergeOutputRows[] = [];
  if (deleted.length > 0) {
    deleted.sort(byKey);
    outputs.push({
      kind: "delete",
      columns: [keyColumn],
      values: [deleted.map(([, state]) => state.key)],
      rowCount: deleted.length,
    });
  }
  const groups = [...updates.values()].sort((left, right) => {
    for (let index = 0; index < Math.min(left.indexes.length, right.indexes.length); index += 1) {
      const difference = (left.indexes[index] ?? 0) - (right.indexes[index] ?? 0);
      if (difference !== 0) return difference;
    }
    return left.indexes.length - right.indexes.length;
  });
  for (const group of groups) {
    group.rows.sort(byKey);
    const outputColumns = [keyColumn, ...group.indexes.map((index) => columns[index])];
    if (outputColumns.some((column) => column === undefined)) {
      throw new Error("Delta merge output column is missing");
    }
    outputs.push({
      kind: "update",
      columns: outputColumns as DeltaMergeColumn[],
      values: [
        group.rows.map(([, state]) => state.key),
        ...group.indexes.map((index) =>
          group.rows.map(([, state]) => {
            const value = state.values?.[index];
            if (value === undefined) throw new Error("Delta merge update value is missing");
            return value;
          }),
        ),
      ],
      rowCount: group.rows.length,
    });
  }
  return outputs;
}

/** Rows per block of one output, cut the way the write path cuts a segment it writes. */
export function deltaMergeBlockRows(
  output: DeltaMergeOutputRows,
  maximumRows: number,
  targetBytes: number,
): number[] {
  return planAlignedWriteBlockRanges(
    output.columns.map((column, index) => ({
      type: column.type,
      values: output.values[index] ?? [],
    })),
    output.rowCount,
    maximumRows,
    targetBytes,
  ).map(({ start, end }) => end - start);
}

const textEncoder = new TextEncoder();

/**
 * Encodes every output block's physical payload, cut as `blockRows` says, and the CRC-32 over
 * the outputs' kinds, columns, block cuts, and those payloads. The checksum is what a plan
 * records and what a resumed merge must reproduce before it writes a block.
 */
export function encodeDeltaMergeOutputs(
  outputs: readonly DeltaMergeOutputRows[],
  blockRows: ReadonlyArray<readonly number[]>,
): { blocks: DeltaMergeOutputBlock[]; checksum: number } {
  if (blockRows.length !== outputs.length) {
    throw new TypeError("Delta merge block cuts must cover every output");
  }
  const blocks: DeltaMergeOutputBlock[] = [];
  let checksum = 0;
  for (const [outputIndex, output] of outputs.entries()) {
    const rows = blockRows[outputIndex] ?? [];
    if (rows.reduce((total, count) => total + count, 0) !== output.rowCount) {
      throw new TypeError("Delta merge block cuts must hold exactly the output's rows");
    }
    checksum = crc32Continue(
      checksum,
      textEncoder.encode(
        `${output.kind}|${output.columns.map((column) => `${column.columnId}:${column.type}`).join(",")}|${rows.join(",")}\n`,
      ),
    );
    for (const [columnIndex, column] of output.columns.entries()) {
      const values = output.values[columnIndex];
      if (values?.length !== output.rowCount) {
        throw new TypeError("Delta merge output column must hold one value per row");
      }
      let start = 0;
      for (const [part, count] of rows.entries()) {
        const encoded = encodeColumn(columnInput(column.type, values.slice(start, start + count)));
        start += count;
        checksum = crc32Continue(
          checksum,
          textEncoder.encode(
            `${String(count)},${String(encoded.nullCount)},${String(encoded.bytes.byteLength)};`,
          ),
        );
        checksum = crc32Continue(checksum, encoded.bytes);
        blocks.push({
          outputIndex,
          columnIndex,
          part,
          type: column.type,
          rowCount: count,
          bytes: encoded.bytes,
        });
      }
    }
  }
  return { blocks, checksum };
}

function columnInput(type: LogicalType, values: readonly BatchValue[]): ColumnInput {
  switch (type) {
    case "boolean":
      return { type, values: values as ReadonlyArray<boolean | null> };
    case "number":
      return { type, values: values as ReadonlyArray<number | null> };
    case "string":
      return { type, values: values as ReadonlyArray<string | null> };
    case "datetime":
      return { type, values: values as ReadonlyArray<Date | null> };
  }
}
