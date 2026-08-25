import { dateMilliseconds } from "../date-value.js";
import { compareSqlStrings, compareSqlValues } from "./sql-semantics.js";

/**
 * Order-by keys prepared for sorting, and the sort that runs over them.
 *
 * Sorting is comparison-bound: a merge over n rows performs O(n log n) of them, so what one
 * comparison costs decides what the sort costs. Reading a value out of a row by name and then
 * dispatching on its type at every step is most of that cost, and all of it is avoidable —
 * the values are known before the sort starts, and a term's type does not vary row to row.
 *
 * Each term is extracted once into its own column. A term whose values are all numbers —
 * which includes every datetime, since extraction unboxes them to epoch milliseconds — or all
 * booleans is stored in a Float64Array beside a null mask. A string term is stored the same
 * way, as the rank of each string among the sorted distinct values: the dictionary a string
 * vector already carries is exactly that set, and comparing two ranks orders the two strings.
 * Only a term that mixes types keeps its values and the generic comparison.
 *
 * With the keys unboxed, the sort itself stops comparing. `sortKeyIndexes` radix-sorts the
 * leading key's float bits, recognizes input that already arrives in order, and only falls back
 * to a comparison merge for the terms it cannot encode. Ties on one term are broken by the
 * next, so what a multi-term sort pays for its later terms is proportional to how many rows tie
 * on the earlier ones.
 */
export interface SortKeyColumn {
  /**
   * Ascending comparison with NULL smallest. This is the column's raw comparison, not SQL's
   * default placement: the sorter applies PostgreSQL's NULLS LAST for ASC and NULLS FIRST for
   * DESC. Direction and an explicit NULLS FIRST/LAST stay with the caller because a placement is
   * absolute, while direction negates only the non-null value comparison.
   *
   * Specialized when the column is built, so a comparison that runs millions of times is a
   * closure over one representation rather than a branch on which representation it holds.
   */
  readonly compare: (left: number, right: number) => number;
  readonly isNull: (index: number) => boolean;
  /**
   * The unboxed keys, when the column has them: numbers, epoch milliseconds, 0/1 for booleans,
   * or string ranks. Both arrays are present or neither is. Equal numbers mean equal keys and
   * their order is the order `compare` reports, so a sort can work on these alone.
   */
  readonly numbers: Float64Array | undefined;
  readonly nulls: Uint8Array | undefined;
}

/** One ORDER BY term as the sort sees it: its keys, its direction, and its NULL placement. */
export interface SortKeyTerm {
  readonly column: SortKeyColumn;
  readonly descending: boolean;
  readonly nulls: "first" | "last" | undefined;
}

/** Unwraps a datetime to the number the comparisons use; every other value passes through. */
export function comparableSortValue(value: unknown): unknown {
  return value instanceof Date ? dateMilliseconds(value) : value;
}

/**
 * Extracts one term's values into a comparison-ready column. `valueAt` is called once per row,
 * so a caller that reads through a row object or a vector pays that cost once rather than once
 * per comparison. A term that mixes types cannot be unboxed; it is read a second time into the
 * generic column, whose comparison reports the type error the way it always has.
 *
 * Strings are always ranked, whatever their cardinality. Measured at 200k rows, ranking wins by
 * ten times when six strings repeat and still by three times when nearly every string is
 * distinct: the distinct set sorts with the engine's own string sort, the rows then sort as
 * numbers, and neither pays the per-comparison dispatch the generic path does.
 */
export function buildSortKeyColumn(
  count: number,
  valueAt: (index: number) => unknown,
): SortKeyColumn {
  const numbers = new Float64Array(count);
  const nulls = new Uint8Array(count);
  let kind: "number" | "boolean" | "string" | undefined;
  let ranks: Map<string, number> | undefined;
  for (let index = 0; index < count; index += 1) {
    const value = comparableSortValue(valueAt(index));
    if (value === null || value === undefined) {
      nulls[index] = 1;
      continue;
    }
    if (typeof value === "number" && kind !== "boolean" && kind !== "string") {
      kind = "number";
      numbers[index] = value;
    } else if (typeof value === "boolean" && kind !== "number" && kind !== "string") {
      kind = "boolean";
      numbers[index] = value ? 1 : 0;
    } else if (
      typeof value === "string" &&
      // NUL-prefixed strings are the engine's lossless internal SQL-domain values
      // (NUMERIC, ENUM, COLLATE, and friends). Their physical encoding is deliberately
      // stable, not lexicographically ordered, so ranking the encoded strings would make
      // ORDER BY disagree with the SQL comparator. Keep those terms on the generic path.
      value.charCodeAt(0) !== 0 &&
      kind !== "number" &&
      kind !== "boolean"
    ) {
      kind = "string";
      ranks ??= new Map();
      let rank = ranks.get(value);
      if (rank === undefined) {
        rank = ranks.size;
        ranks.set(value, rank);
      }
      numbers[index] = rank;
    } else {
      return buildGenericSortKeyColumn(count, valueAt);
    }
  }
  if (ranks !== undefined) {
    // Ranks were handed out in first-seen order; renumber them in sorted order so the numeric
    // comparison of two ranks is the string comparison of the two strings.
    const distinct = [...ranks.keys()].sort(compareSqlStrings);
    const sortedRank = new Float64Array(distinct.length);
    for (let position = 0; position < distinct.length; position += 1) {
      sortedRank[ranks.get(distinct[position] ?? "") ?? 0] = position;
    }
    for (let index = 0; index < count; index += 1) {
      if (nulls[index] === 0) numbers[index] = sortedRank[numbers[index] ?? 0] ?? 0;
    }
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
    numbers,
    nulls,
  };
}

function buildGenericSortKeyColumn(
  count: number,
  valueAt: (index: number) => unknown,
): SortKeyColumn {
  const values = new Array<unknown>(count);
  for (let index = 0; index < count; index += 1) {
    values[index] = comparableSortValue(valueAt(index));
  }
  return {
    compare: (left, right) => compareSqlValues(values[left], values[right]),
    isNull: (index) => {
      const value = values[index];
      return value === null || value === undefined;
    },
    numbers: undefined,
    nulls: undefined,
  };
}

/**
 * A stable sort of `count` rows by the given terms, returned as the permutation of row indexes
 * in sorted order. Equal rows keep their input order, which is what makes ORDER BY
 * deterministic and what lets a bounded top-N decide ties by arrival.
 *
 * Each term is sorted in turn: the first over every row, then each later term only within the
 * runs of rows the earlier terms left tied. A term with unboxed keys is encoded once into
 * 64-bit integers whose unsigned order is the term's order — direction and NULL placement
 * folded in — and large ranges are radix-sorted on those bits while small ones merge. A term
 * without unboxed keys merges under the generic comparison. Both kernels check for input that
 * is already in order before doing anything else, since a scan often arrives sorted.
 */
export function sortKeyIndexes(count: number, terms: readonly SortKeyTerm[]): Uint32Array {
  if (count > 0xffffffff) throw new RangeError("Too many rows to order");
  const indexes = new Uint32Array(count);
  for (let index = 0; index < count; index += 1) indexes[index] = index;
  if (count < 2 || terms.length === 0) return indexes;
  const sorter = new KeySorter(count, terms);
  sorter.sortRange(indexes, 0, count, 0);
  return indexes;
}

/** Ranges at least this long radix-sort; shorter ones merge, where the constant factor wins. */
const RADIX_MIN_ROWS = 2048;
const RADIX_DIGIT_BITS = 11;
const RADIX_BUCKETS = 1 << RADIX_DIGIT_BITS;
/** Ranges at most this long insertion-sort, which is what a merge bottoms out in anyway. */
const INSERTION_MAX_ROWS = 16;
const UINT32_MAX = 0xffffffff;
const LITTLE_ENDIAN = new Uint8Array(new Uint16Array([1]).buffer)[0] === 1;
const HI = LITTLE_ENDIAN ? 1 : 0;
const LO = 1 - HI;

type Comparator = (left: number, right: number) => number;

class KeySorter {
  readonly #count: number;
  readonly #terms: readonly SortKeyTerm[];
  readonly #scratch: Uint32Array;
  /** Per term, lazily: the encoded 64-bit keys as (lo, hi) word pairs, when the term has them. */
  readonly #encoded: Array<Uint32Array | undefined>;
  readonly #comparators: Array<Comparator | undefined>;
  #counts: Uint32Array | undefined;

  constructor(count: number, terms: readonly SortKeyTerm[]) {
    this.#count = count;
    this.#terms = terms;
    this.#scratch = new Uint32Array(count);
    this.#encoded = new Array<Uint32Array | undefined>(terms.length);
    this.#comparators = new Array<Comparator | undefined>(terms.length);
  }

  /** Sorts `indexes[start, end)` by term `term` and every term after it. */
  sortRange(indexes: Uint32Array, start: number, end: number, term: number): void {
    const spec = this.#terms[term];
    if (spec === undefined || end - start < 2) return;
    const encoded = this.#encodedKeys(term);
    if (encoded === undefined) {
      const compare = this.#comparator(term);
      this.#mergeSort(indexes, start, end, compare);
      if (term + 1 < this.#terms.length) {
        let runStart = start;
        for (let index = start + 1; index <= end; index += 1) {
          if (index < end && compare(indexes[index - 1] ?? 0, indexes[index] ?? 0) === 0) continue;
          if (index - runStart > 1) this.sortRange(indexes, runStart, index, term + 1);
          runStart = index;
        }
      }
      return;
    }
    this.#sortEncoded(indexes, start, end, encoded, term);
    if (term + 1 < this.#terms.length) {
      let runStart = start;
      let previous = indexes[start] ?? 0;
      for (let index = start + 1; index <= end; index += 1) {
        const current = indexes[index] ?? 0;
        if (
          index < end &&
          encoded[current * 2 + HI] === encoded[previous * 2 + HI] &&
          encoded[current * 2 + LO] === encoded[previous * 2 + LO]
        ) {
          previous = current;
          continue;
        }
        if (index - runStart > 1) this.sortRange(indexes, runStart, index, term + 1);
        runStart = index;
        previous = current;
      }
    }
  }

  #sortEncoded(
    indexes: Uint32Array,
    start: number,
    end: number,
    encoded: Uint32Array,
    term: number,
  ): void {
    const length = end - start;
    if (length < RADIX_MIN_ROWS) {
      this.#mergeSort(indexes, start, end, this.#comparator(term));
      return;
    }
    // One pass reads everything the radix needs to know: whether the range is already in
    // order, in which case there is nothing to do, and which bits vary across it, so the passes
    // over constant digits — most of them, for integers and short decimals — are skipped.
    let orLo = 0;
    let andLo = UINT32_MAX;
    let orHi = 0;
    let andHi = UINT32_MAX;
    let ordered = true;
    let previousLo = 0;
    let previousHi = 0;
    for (let index = start; index < end; index += 1) {
      const row = (indexes[index] ?? 0) * 2;
      const lo = encoded[row + LO] ?? 0;
      const hi = encoded[row + HI] ?? 0;
      orLo |= lo;
      andLo &= lo;
      orHi |= hi;
      andHi &= hi;
      if (ordered && index > start && (hi < previousHi || (hi === previousHi && lo < previousLo))) {
        ordered = false;
      }
      previousLo = lo;
      previousHi = hi;
    }
    if (ordered) return;
    const varying = [(orLo ^ andLo) >>> 0, (orHi ^ andHi) >>> 0];
    const counts = (this.#counts ??= new Uint32Array(RADIX_BUCKETS));
    let source = indexes;
    let target = this.#scratch;
    for (let pass = 0; pass < 6; pass += 1) {
      const word = pass < 3 ? LO : HI;
      const shift = (pass % 3) * RADIX_DIGIT_BITS;
      const mask = pass % 3 === 2 ? (1 << (32 - 2 * RADIX_DIGIT_BITS)) - 1 : RADIX_BUCKETS - 1;
      if (((varying[pass < 3 ? 0 : 1] ?? 0) & (mask << shift)) === 0) continue;
      counts.fill(0);
      for (let index = start; index < end; index += 1) {
        const digit = ((encoded[(source[index] ?? 0) * 2 + word] ?? 0) >>> shift) & mask;
        counts[digit] = (counts[digit] ?? 0) + 1;
      }
      let offset = start;
      for (let digit = 0; digit <= mask; digit += 1) {
        const bucket = counts[digit] ?? 0;
        counts[digit] = offset;
        offset += bucket;
      }
      for (let index = start; index < end; index += 1) {
        const row = source[index] ?? 0;
        const digit = ((encoded[row * 2 + word] ?? 0) >>> shift) & mask;
        const slot = counts[digit] ?? 0;
        target[slot] = row;
        counts[digit] = slot + 1;
      }
      const held = source;
      source = target;
      target = held;
    }
    if (source !== indexes) indexes.set(source.subarray(start, end), start);
  }

  /**
   * Stable bottom-up merge over `indexes[start, end)`. Blocks begin as insertion-sorted runs,
   * and a merge whose halves are already in order — the whole input, when it arrives sorted —
   * is a copy rather than a merge.
   */
  #mergeSort(indexes: Uint32Array, start: number, end: number, compare: Comparator): void {
    const length = end - start;
    for (let blockStart = start; blockStart < end; blockStart += INSERTION_MAX_ROWS) {
      const blockEnd = Math.min(blockStart + INSERTION_MAX_ROWS, end);
      for (let index = blockStart + 1; index < blockEnd; index += 1) {
        const row = indexes[index] ?? 0;
        let position = index;
        while (position > blockStart && compare(indexes[position - 1] ?? 0, row) > 0) {
          indexes[position] = indexes[position - 1] ?? 0;
          position -= 1;
        }
        indexes[position] = row;
      }
    }
    if (length <= INSERTION_MAX_ROWS) return;
    const scratch = this.#scratch;
    let source = indexes;
    let target = scratch;
    for (let width = INSERTION_MAX_ROWS; width < length; width *= 2) {
      for (let left = start; left < end; left += width * 2) {
        const middle = Math.min(left + width, end);
        const right = Math.min(left + width * 2, end);
        if (middle >= right || compare(source[middle - 1] ?? 0, source[middle] ?? 0) <= 0) {
          target.set(source.subarray(left, right), left);
          continue;
        }
        let leftIndex = left;
        let rightIndex = middle;
        for (let output = left; output < right; output += 1) {
          if (
            rightIndex >= right ||
            (leftIndex < middle && compare(source[leftIndex] ?? 0, source[rightIndex] ?? 0) <= 0)
          ) {
            target[output] = source[leftIndex] ?? 0;
            leftIndex += 1;
          } else {
            target[output] = source[rightIndex] ?? 0;
            rightIndex += 1;
          }
        }
      }
      const held = source;
      source = target;
      target = held;
    }
    if (source !== indexes) indexes.set(source.subarray(start, end), start);
  }

  /**
   * The term's keys as 64-bit unsigned integers whose order is the term's order, or undefined
   * for a term without unboxed keys. A float's bits already order like the float once the sign
   * is folded (flip every bit of a negative, the sign bit of a positive); NaN is pinned just
   * past +Infinity and signed zero collapses to +0, which is where compareSqlValues puts them.
   * DESC inverts the non-null keys, and NULL takes the end its placement names. When omitted,
   * PostgreSQL's default supplies NULLS LAST for ASC and NULLS FIRST for DESC.
   */
  #encodedKeys(term: number): Uint32Array | undefined {
    const cached = this.#encoded[term];
    if (cached !== undefined) return cached;
    const spec = this.#terms[term];
    const numbers = spec?.column.numbers;
    const nulls = spec?.column.nulls;
    if (spec === undefined || numbers === undefined || nulls === undefined) return undefined;
    const count = this.#count;
    const buffer = new ArrayBuffer(count * 8);
    const floats = new Float64Array(buffer);
    const words = new Uint32Array(buffer);
    const nullsFirst = (spec.nulls ?? (spec.descending ? "first" : "last")) === "first";
    const flip = spec.descending ? UINT32_MAX : 0;
    // Keep the sentinel's low word aligned with the direction's transformed values. A sentinel
    // that differs in an otherwise constant low digit makes radix sorting run a whole extra pass.
    // The non-null domains are bounded away from these keys: ascending runs from encoded
    // -Infinity through pinned NaN (fff80000:00000000), and descending is its bitwise inverse.
    const nullHi = spec.descending
      ? nullsFirst
        ? 0x0007fffe
        : UINT32_MAX
      : nullsFirst
        ? 0
        : 0xfff80001;
    const nullLo = spec.descending ? UINT32_MAX : 0;
    for (let index = 0; index < count; index += 1) {
      const lo = index * 2 + LO;
      const hi = index * 2 + HI;
      if (nulls[index] === 1) {
        words[lo] = nullLo;
        words[hi] = nullHi;
        continue;
      }
      const value = numbers[index] ?? 0;
      if (value !== value) {
        words[lo] = flip;
        words[hi] = (0xfff80000 ^ flip) >>> 0;
      } else {
        // Adding +0 turns -0 into +0 and leaves every other value alone.
        floats[index] = value + 0;
        const rawHi = words[hi] ?? 0;
        if ((rawHi & 0x80000000) !== 0) {
          words[lo] = (~(words[lo] ?? 0) ^ flip) >>> 0;
          words[hi] = (~rawHi ^ flip) >>> 0;
        } else {
          words[lo] = ((words[lo] ?? 0) ^ flip) >>> 0;
          words[hi] = ((rawHi | 0x80000000) ^ flip) >>> 0;
        }
      }
    }
    this.#encoded[term] = words;
    return words;
  }

  /** The term's order as a comparison, for the ranges too small to radix and the generic terms. */
  #comparator(term: number): Comparator {
    const cached = this.#comparators[term];
    if (cached !== undefined) return cached;
    const spec = this.#terms[term];
    if (spec === undefined) throw new Error("Order term is missing");
    const encoded = this.#encodedKeys(term);
    let compare: Comparator;
    if (encoded !== undefined) {
      compare = (left, right) => {
        const leftHi = encoded[left * 2 + HI] ?? 0;
        const rightHi = encoded[right * 2 + HI] ?? 0;
        if (leftHi !== rightHi) return leftHi < rightHi ? -1 : 1;
        const leftLo = encoded[left * 2 + LO] ?? 0;
        const rightLo = encoded[right * 2 + LO] ?? 0;
        return leftLo === rightLo ? 0 : leftLo < rightLo ? -1 : 1;
      };
    } else {
      const { column, descending } = spec;
      const nulls = spec.nulls ?? (spec.descending ? "first" : "last");
      compare = (left, right) => {
        // NULL placement is absolute: direction must not negate it.
        const leftNull = column.isNull(left);
        const rightNull = column.isNull(right);
        if (leftNull || rightNull) {
          return leftNull && rightNull ? 0 : (leftNull ? -1 : 1) * (nulls === "first" ? 1 : -1);
        }
        const comparison = column.compare(left, right);
        return descending ? -comparison : comparison;
      };
    }
    this.#comparators[term] = compare;
    return compare;
  }
}
