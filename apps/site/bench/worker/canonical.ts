/**
 * Result comparison machinery for the reference suite: engine rows and JavaScript rows
 * are put into one comparable shape — a tuple per row in select-list order, sorted so
 * row order never matters. Ordering is safe to drop because the only queries whose
 * result set depends on ORDER BY are the limited ones, and each of those orders on a
 * unique tie-break.
 */

export function canonicalTuples(
  resultRows: readonly unknown[],
  project: (row: Record<string, unknown>) => unknown[],
): unknown[][] {
  // The sort key is built once per row rather than inside the comparator: the write suite
  // canonicalizes result sets of 100,000 rows, where re-deriving it per comparison would
  // cost millions of serializations.
  return resultRows
    .map((row) => {
      const tuple = project(row as Record<string, unknown>).map(canonicalValue);
      return { tuple, key: tupleSortKey(tuple) };
    })
    .sort((left, right) => (left.key < right.key ? -1 : left.key > right.key ? 1 : 0))
    .map((entry) => entry.tuple);
}

function canonicalValue(value: unknown): unknown {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "bigint") return Number(value);
  return value === undefined ? null : value;
}

// Rounded so two implementations that differ in the last bits of a sum still sort together.
function tupleSortKey(tuple: readonly unknown[]): string {
  return JSON.stringify(
    tuple.map((value) =>
      typeof value === "number" && Number.isFinite(value) ? value.toPrecision(12) : value,
    ),
  );
}

/**
 * Aggregates accumulated in a different row order differ in the last bits, so numbers
 * compare within a relative tolerance rather than exactly.
 */
export function tuplesMatch(left: readonly unknown[][], right: readonly unknown[][]): boolean {
  if (left.length !== right.length) return false;
  return left.every((tuple, index) => {
    const other = right[index];
    if (tuple.length !== other?.length) return false;
    return tuple.every((value, position) => valuesMatch(value, other[position]));
  });
}

function valuesMatch(left: unknown, right: unknown): boolean {
  if (typeof left === "number" && typeof right === "number") {
    if (Number.isNaN(left) && Number.isNaN(right)) return true;
    return Math.abs(left - right) <= Math.max(Math.abs(left), Math.abs(right), 1) * 1e-9;
  }
  return left === right;
}

export function referenceChecksum(value: unknown): number {
  const text = JSON.stringify(value, (_key, item: unknown) => {
    if (item instanceof Date) return item.toISOString();
    // Equivalent aggregates can differ by sub-cent floating-point noise when
    // their independent oracles visit rows in a different order.
    if (typeof item === "number" && Number.isFinite(item)) {
      return Math.round(item * 1_000_000_000) / 1_000_000_000;
    }
    return item;
  });
  let hash = 2_166_136_261;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16_777_619);
  }
  return hash >>> 0;
}
