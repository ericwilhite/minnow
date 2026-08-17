import { compareSqlValues } from "./sql-semantics.js";

/**
 * Order-by keys prepared for comparison.
 *
 * Sorting is comparison-bound: a merge over n rows performs O(n log n) of them, so what one
 * comparison costs decides what the sort costs. Reading a value out of a row by name and then
 * dispatching on its type at every step is most of that cost, and both of it is avoidable —
 * the values are known before the sort starts, and a term's type does not vary row to row.
 *
 * Each term is extracted once into its own column. A term whose values are all numbers — which
 * includes every datetime, since extraction unboxes them to epoch milliseconds — is stored in a
 * Float64Array beside a null mask, and comparing it is two typed reads and a subtraction. Any
 * other term keeps its values and the generic comparison. The specialized form costs the same
 * eight bytes per key a reference did, plus one byte of mask, so this buys speed with locality
 * rather than with memory.
 */
export interface SortKeyColumn {
  /**
   * Ascending comparison with NULL smallest — the engine's default placement, which matches
   * SQLite. Direction and an explicit NULLS FIRST/LAST stay with the caller: a placement is
   * absolute, while direction negates the value comparison.
   *
   * Specialized when the column is built, so the comparison a sort runs millions of times is a
   * closure over one representation rather than a branch on which representation it holds.
   */
  readonly compare: (left: number, right: number) => number;
  readonly isNull: (index: number) => boolean;
}

/** Unwraps a datetime to the number the comparisons use; every other value passes through. */
export function comparableSortValue(value: unknown): unknown {
  return value instanceof Date ? value.getTime() : value;
}

/**
 * Extracts one term's values into a comparison-ready column. `valueAt` is called exactly once
 * per row, so a caller that reads through a row object or a vector pays that cost once rather
 * than once per comparison.
 */
export function buildSortKeyColumn(
  count: number,
  valueAt: (index: number) => unknown,
): SortKeyColumn {
  const values = new Array<unknown>(count);
  let numeric = true;
  for (let index = 0; index < count; index += 1) {
    const value = comparableSortValue(valueAt(index));
    values[index] = value;
    if (numeric && value !== null && value !== undefined && typeof value !== "number") {
      numeric = false;
    }
  }
  if (!numeric) {
    return {
      compare: (left, right) => compareSqlValues(values[left], values[right]),
      isNull: (index) => {
        const value = values[index];
        return value === null || value === undefined;
      },
    };
  }
  const numbers = new Float64Array(count);
  const nulls = new Uint8Array(count);
  for (let index = 0; index < count; index += 1) {
    const value = values[index];
    if (value === null || value === undefined) nulls[index] = 1;
    else numbers[index] = value as number;
  }
  return {
    compare: (left, right) => {
      const leftNull = nulls[left] === 1;
      const rightNull = nulls[right] === 1;
      if (leftNull || rightNull) return leftNull && rightNull ? 0 : leftNull ? -1 : 1;
      const leftValue = numbers[left] ?? 0;
      const rightValue = numbers[right] ?? 0;
      if (leftValue === rightValue) return 0;
      // NaN sorts last, as compareSqlValues has it. The typed path has to say so outright,
      // because a NaN comparison is false whichever way round it is asked.
      if (Number.isNaN(leftValue)) return Number.isNaN(rightValue) ? 0 : 1;
      if (Number.isNaN(rightValue)) return -1;
      return leftValue < rightValue ? -1 : 1;
    },
    isNull: (index) => nulls[index] === 1,
  };
}
