/**
 * Write-time default filling. Only an omitted property or SQL `DEFAULT` means "generate";
 * explicit NULL passes through untouched. Literal and SQL-expression defaults are filled before
 * validation. Auto-increment slots stay null until the write path atomically reserves a range.
 */

import type { RowIdRange, TableColumnRecord, TableRecord } from "../storage/types.js";
import type { BatchValue, ColumnarBatch } from "./batch.js";

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

export async function fillColumnDefaults(
  table: TableRecord,
  input: ColumnarBatch,
  evaluateExpression: (sql: string, rowIndex: number) => Promise<BatchValue>,
  knownRowCount?: number,
): Promise<FilledBatch> {
  const rowCount =
    knownRowCount ?? Object.values(input.columns).find((values) => values.length > 0)?.length ?? 0;
  if (!Number.isSafeInteger(rowCount) || rowCount < 0) {
    throw new TypeError(`Batch row count must be a non-negative safe integer: ${String(rowCount)}`);
  }
  for (const [name, values] of Object.entries(input.columns)) {
    if (values.length !== rowCount) {
      throw new TypeError(
        `Column ${name} has ${String(values.length)} rows; expected ${String(rowCount)}`,
      );
    }
  }
  if (rowCount === 0) return { batch: input, generated: new Map() };
  const columns: Record<string, readonly BatchValue[]> = { ...input.columns };
  const generated = new Map<string, BatchValue[]>();
  let autoIncrement: AutoIncrementFill | undefined;
  for (const [name, mask] of Object.entries(input.omitted ?? {})) {
    if (!table.columns.some((column) => column.name === name)) {
      throw new TypeError(`Unknown column in omission mask: ${name}`);
    }
    if (mask.length !== rowCount) {
      throw new TypeError(
        `Omission mask ${name} has ${String(mask.length)} rows; expected ${String(rowCount)}`,
      );
    }
    if (mask.some((value) => typeof value !== "boolean")) {
      throw new TypeError(`Omission mask ${name} must contain booleans`);
    }
  }
  for (const column of table.columns) {
    const defaultValue = column.defaultValue;
    const provided = input.columns[column.name];
    const omitted = input.omitted?.[column.name];
    const isOmitted = (index: number): boolean =>
      provided === undefined || omitted?.[index] === true;
    if (defaultValue === undefined) {
      if (provided === undefined || omitted?.some(Boolean) === true) {
        const values =
          provided === undefined ? new Array<BatchValue>(rowCount).fill(null) : [...provided];
        for (let index = 0; index < rowCount; index += 1) {
          if (isOmitted(index)) values[index] = null;
        }
        columns[column.name] = values;
      }
      continue;
    }
    if (defaultValue.kind === "autoincrement") {
      const missingIndexes: number[] = [];
      let maxExplicit = 0n;
      for (let index = 0; index < rowCount; index += 1) {
        const value = provided?.[index] ?? null;
        if (isOmitted(index)) {
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
      if (!isOmitted(index)) continue;
      filled = true;
      switch (defaultValue.kind) {
        case "literal":
          values[index] =
            defaultValue.value instanceof Date
              ? new Date(defaultValue.value.getTime())
              : defaultValue.value;
          break;
        case "expression":
          values[index] = await evaluateExpression(defaultValue.sql, index);
          break;
      }
    }
    if (filled) {
      columns[column.name] = values;
      generated.set(column.name, values);
    }
  }
  return {
    batch: { columns, rowCount },
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
