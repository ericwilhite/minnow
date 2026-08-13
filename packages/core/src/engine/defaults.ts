/**
 * Write-time default filling. For a default-bearing column (always non-nullable), a null or
 * absent slot means "generate" and an explicit value passes through untouched. Pure defaults
 * (uuid, nanoid, now, literals) are filled here before validation; auto-increment slots stay
 * null until the write path reserves a range from storage, keeping generation atomic across
 * concurrent writers.
 */

import type { RowIdRange, TableColumnRecord, TableRecord } from "../storage/types.js";
import type { BatchValue, ColumnarBatch } from "./batch.js";

// The standard nanoid alphabet: 64 characters, so one random byte maps to one character with a
// bitmask and no modulo bias.
const NANOID_ALPHABET = "useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict";

export function nanoid(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(21));
  let id = "";
  // `charAt` over indexing: the mask keeps the index in range, and it types as `string`.
  for (const byte of bytes) id += NANOID_ALPHABET.charAt(byte & 63);
  return id;
}

/** The deferred part of a fill: slots that need storage-reserved auto-increment values. */
export interface AutoIncrementFill {
  readonly column: TableColumnRecord;
  /** Row indexes whose value must come from the reserved range, in row order. */
  readonly missingIndexes: readonly number[];
  /** One past the largest explicit value in the batch, so generated keys never collide with it. */
  readonly atLeast: bigint;
}

export interface FilledBatch {
  readonly batch: ColumnarBatch;
  /** Full written vectors, input row order, for columns where at least one slot was generated. */
  readonly generated: Map<string, BatchValue[]>;
  /** Present whenever the table has an auto-increment column, even if every value is explicit —
   * the counter still bumps past the explicit maximum in the same atomic step. */
  readonly autoIncrement?: AutoIncrementFill;
}

export function fillColumnDefaults(
  table: TableRecord,
  input: ColumnarBatch,
  now: () => Date,
  knownRowCount?: number,
): FilledBatch {
  const defaultColumns = table.columns.filter((column) => column.defaultValue !== undefined);
  if (defaultColumns.length === 0) return { batch: input, generated: new Map() };
  const rowCount =
    knownRowCount ?? Object.values(input.columns).find((values) => values.length > 0)?.length ?? 0;
  if (rowCount === 0) return { batch: input, generated: new Map() };
  const columns: Record<string, readonly BatchValue[]> = { ...input.columns };
  const generated = new Map<string, BatchValue[]>();
  let autoIncrement: AutoIncrementFill | undefined;
  let nowValue: Date | undefined;
  for (const column of defaultColumns) {
    const defaultValue = column.defaultValue;
    if (defaultValue === undefined) continue;
    const provided = input.columns[column.name];
    if (defaultValue.kind === "autoincrement") {
      const missingIndexes: number[] = [];
      let maxExplicit = 0n;
      for (let index = 0; index < rowCount; index += 1) {
        const value = provided?.[index] ?? null;
        if (value === null) {
          missingIndexes.push(index);
          continue;
        }
        if (typeof value !== "number" || !Number.isSafeInteger(value)) {
          throw new TypeError(
            `${column.name}[${String(index)}] must be a whole number in the safe integer range`,
          );
        }
        if (BigInt(value) > maxExplicit) maxExplicit = BigInt(value);
      }
      if (missingIndexes.length > 0) {
        columns[column.name] =
          provided === undefined ? new Array<BatchValue>(rowCount).fill(null) : [...provided];
      }
      autoIncrement = { column, missingIndexes, atLeast: maxExplicit + 1n };
      continue;
    }
    const values =
      provided === undefined ? new Array<BatchValue>(rowCount).fill(null) : [...provided];
    let filled = false;
    for (let index = 0; index < rowCount; index += 1) {
      if ((values[index] ?? null) !== null) continue;
      filled = true;
      switch (defaultValue.kind) {
        case "uuid":
          values[index] = crypto.randomUUID();
          break;
        case "nanoid":
          values[index] = nanoid();
          break;
        case "now":
          values[index] = nowValue ??= now();
          break;
        case "literal":
          values[index] = defaultValue.value;
          break;
      }
    }
    if (filled) {
      columns[column.name] = values;
      generated.set(column.name, values);
    }
  }
  return {
    batch: { columns },
    generated,
    ...(autoIncrement === undefined ? {} : { autoIncrement }),
  };
}

/**
 * Writes the reserved range into the batch's null slots. The vector is the fresh array
 * `fillColumnDefaults` created for this fill, never a caller-owned one.
 */
export function patchAutoIncrementValues(
  input: ColumnarBatch,
  fill: AutoIncrementFill,
  range: RowIdRange,
): void {
  const vector = input.columns[fill.column.name] as BatchValue[];
  fill.missingIndexes.forEach((rowIndex, offset) => {
    const value = range.start + BigInt(offset);
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new RangeError(`Auto-increment exhausted the safe integer range: ${fill.column.name}`);
    }
    vector[rowIndex] = Number(value);
  });
}
