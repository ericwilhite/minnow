/**
 * Index term encoding: the strings a secondary index or a full-text column names its rows under,
 * the postings those terms carry, and the row-id locators a posting resolves back to a row with.
 *
 * Every term here is a persisted format. A decoder must stay the exact inverse of its encoder, and
 * a `tuple-v1` term — written before NULL components were indexed — must keep decoding as it
 * always did. The key-token helpers sit here too: a unique key is the locator a scalar posting
 * names its row by, so the two encodings are read together.
 */
import { dateMilliseconds } from "../date-value.js";
import { maybeYieldToEventLoop } from "../work-slicer.js";
import type { BatchValue, ColumnarBatch } from "./batch.js";
import { renderDocumentValue, tokenize as ftsTokenize } from "./fts.js";
import { protectedSqlTextValue } from "./sql-domains.js";
import {
  MAX_FTS_POSTINGS_PER_CHUNK,
  MAX_FTS_POSTING_ROW_IDS_PER_CHUNK,
  MAX_FTS_POSTING_TERM_CHARACTERS,
  MAX_INDEXED_STRING_CHARACTERS,
  secondaryIndexColumnIds,
  secondaryIndexDirections,
  secondaryUniqueKeyNamespace,
  type FtsColumnDelta,
  type FtsPosting,
  type RowIdSpan,
  type SecondaryIndexDirection,
  type SecondaryIndexRecord,
  type SegmentKind,
  type SegmentRecord,
  type SimpleDataType,
  type TableColumnRecord,
  type TableRecord,
} from "../storage/types.js";
import type { DatabaseTransaction } from "../transactions/index.js";
import type { UpdateBatchInput } from "./database.js";

/** A whole-number total that refuses to leave the range a stored count can round-trip. */
function safeWholeNumberSum(values: readonly number[], name: string): number {
  let total = 0;
  for (const value of values) {
    if (!Number.isSafeInteger(value) || value < 0 || total > Number.MAX_SAFE_INTEGER - value) {
      throw new RangeError(`${name} exceeds the safe integer range`);
    }
    total += value;
  }
  return total;
}

function assertIndexedStringLength(value: string): void {
  if (value.length > MAX_INDEXED_STRING_CHARACTERS) {
    throw new RangeError(
      `Indexed strings cannot exceed ${String(MAX_INDEXED_STRING_CHARACTERS)} characters`,
    );
  }
}

export function keyToken(type: SimpleDataType, value: BatchValue): string {
  if (value === null) throw new TypeError("Unique key cannot be null");
  switch (type) {
    case "boolean":
      if (typeof value !== "boolean") throw new TypeError("Invalid boolean unique key");
      return value ? "boolean:true" : "boolean:false";
    case "number":
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw new TypeError("Invalid number unique key");
      }
      return `number:${String(value)}`;
    case "string":
      if (typeof value !== "string") throw new TypeError("Invalid string unique key");
      assertIndexedStringLength(value);
      return `string:${value}`;
    case "datetime":
      if (!(value instanceof Date) || !Number.isFinite(dateMilliseconds(value))) {
        throw new TypeError("Invalid datetime unique key");
      }
      return `datetime:${String(dateMilliseconds(value))}`;
  }
}

export function getUniqueKeyColumn(table: TableRecord): TableColumnRecord | undefined {
  if (table.uniqueKeyColumnId === undefined) return undefined;
  return table.columns.find((column) => column.id === table.uniqueKeyColumnId);
}

export function rowIdSpanEnvelope(spans: readonly RowIdSpan[]): {
  start: bigint;
  endExclusive: bigint;
} {
  if (spans.length === 0) return { start: 0n, endExclusive: 0n };
  let start = spans[0]?.rowIdStart ?? 0n;
  let endExclusive = start + BigInt(spans[0]?.rowCount ?? 0);
  for (const span of spans.slice(1)) {
    if (span.rowIdStart < start) start = span.rowIdStart;
    const spanEnd = span.rowIdStart + BigInt(span.rowCount);
    if (spanEnd > endExclusive) endExclusive = spanEnd;
  }
  return { start, endExclusive };
}

export function mergeSourceRowIdSpans(segment: SegmentRecord, kind: SegmentKind): RowIdSpan[] {
  if (kind === "update" || kind === "delete") {
    if (
      segment.rowIdStart !== 0n ||
      segment.rowIdEndExclusive !== 0n ||
      segment.rowIdSpans.length !== 0
    ) {
      throw new Error(`Mutation marker unexpectedly owns row IDs: ${segment.id}`);
    }
    return [];
  }
  const spans =
    segment.rowIdSpans.length === 0
      ? [{ rowStart: 0, rowCount: segment.rowCount, rowIdStart: segment.rowIdStart }]
      : segment.rowIdSpans.map((span) => ({ ...span }));
  const envelope = rowIdSpanEnvelope(spans);
  let rowStart = 0;
  for (const [index, span] of spans.entries()) {
    if (span.rowStart !== rowStart || span.rowCount <= 0) {
      throw new Error(`Segment row ID spans are not contiguous: ${segment.id}`);
    }
    const previous = spans[index - 1];
    if (
      previous !== undefined &&
      previous.rowIdStart + BigInt(previous.rowCount) === span.rowIdStart
    ) {
      throw new Error(`Segment row ID spans are not coalesced: ${segment.id}`);
    }
    rowStart = safeWholeNumberSum([rowStart, span.rowCount], "Segment row ID span rows");
  }
  if (
    rowStart !== segment.rowCount ||
    envelope.start !== segment.rowIdStart ||
    envelope.endExclusive !== segment.rowIdEndExclusive
  ) {
    throw new Error(`Segment row ID spans differ from their envelope: ${segment.id}`);
  }
  const intervals = spans
    .map((span) => ({
      start: span.rowIdStart,
      end: span.rowIdStart + BigInt(span.rowCount),
    }))
    .sort((left, right) => (left.start < right.start ? -1 : left.start > right.start ? 1 : 0));
  for (let index = 1; index < intervals.length; index += 1) {
    const previous = intervals[index - 1];
    const current = intervals[index];
    if (previous !== undefined && current !== undefined && current.start < previous.end) {
      throw new Error(`Segment row IDs overlap: ${segment.id}`);
    }
  }
  return spans;
}

/**
 * Tokenizes one cell into the term accumulator, tracking per-document term frequency, and
 * returns the cell's token count so producers can total the column's tokens for BM25 stats.
 */
export function addFtsDocument(
  byTerm: Map<string, { rowIds: bigint[]; tf: number[] }>,
  value: BatchValue,
  rowId: bigint,
): number {
  const rendered = renderDocumentValue(value);
  if (rendered === undefined) return 0;
  const tokens = ftsTokenize(rendered);
  const counts = new Map<string, number>();
  for (const token of tokens) counts.set(token, (counts.get(token) ?? 0) + 1);
  for (const [term, tf] of counts) {
    const posting = byTerm.get(term) ?? { rowIds: [], tf: [] };
    posting.rowIds.push(rowId);
    posting.tf.push(tf);
    byTerm.set(term, posting);
  }
  return tokens.length;
}

export function sortedFtsPostings(
  byTerm: Map<string, { rowIds: bigint[]; tf: number[] }>,
): FtsPosting[] {
  return [...byTerm.entries()]
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
    .map(([term, posting]) => ({ term, rowIds: posting.rowIds, tf: posting.tf }));
}

export function postingFrequencyTotal(postings: readonly FtsPosting[]): number {
  let total = 0;
  for (const posting of postings) {
    for (const frequency of posting.tf) {
      total = safeWholeNumberSum([total, frequency], "Posting term-frequency total");
    }
  }
  return total;
}

const secondaryNumberBits = new DataView(new ArrayBuffer(8));

/** Lexicographic scalar encoding whose byte order is the engine's SQL order for one type. */
export function secondaryIndexTerm(type: SimpleDataType, value: BatchValue): string {
  if (value === null) throw new TypeError("A NULL has no secondary-index comparison term");
  if (type === "string") {
    if (typeof value !== "string") throw new TypeError("Invalid string index value");
    assertIndexedStringLength(value);
    return value;
  }
  if (type === "boolean") {
    if (typeof value !== "boolean") throw new TypeError("Invalid boolean index value");
    return value ? "1" : "0";
  }
  const numeric = type === "datetime" && value instanceof Date ? dateMilliseconds(value) : value;
  if (typeof numeric !== "number" || !Number.isFinite(numeric)) {
    throw new TypeError(`Invalid ${type} index value`);
  }
  secondaryNumberBits.setFloat64(0, numeric === 0 ? 0 : numeric, false);
  const bits = secondaryNumberBits.getBigUint64(0, false);
  const sortable =
    (bits & 0x8000_0000_0000_0000n) === 0n
      ? bits ^ 0x8000_0000_0000_0000n
      : ~bits & 0xffff_ffff_ffff_ffffn;
  return sortable.toString(16).padStart(16, "0");
}

/**
 * The NULL component marker of a `tuple-v2` term, per key direction.
 *
 * Every non-null component is hexadecimal, so one non-hexadecimal character is both unambiguous
 * to the decoder and strictly outside the byte span of every real value at that position. An
 * ascending component puts NULL above every value — the engine's own ASC ordering, NULLs last —
 * and a descending one mirrors it, which is what reversing a component means. Neither can ever
 * be produced by an equality or range bound, so no such lookup matches a NULL component.
 */
export const ASCENDING_NULL_COMPONENT = "g";
const DESCENDING_NULL_COMPONENT = "/";

/** The lowest byte any non-null component can start with, either direction: hexadecimal zero. */
export const LOWEST_SECONDARY_COMPONENT = "0";

function secondaryNullComponent(direction: SecondaryIndexDirection): string {
  return direction === "desc" ? DESCENDING_NULL_COMPONENT : ASCENDING_NULL_COMPONENT;
}

/** Whether this index's terms name rows that hold a NULL in a trailing indexed column. */
export function secondaryIndexTermsCoverNulls(index: SecondaryIndexRecord): boolean {
  return index.termEncoding === "tuple-v2";
}

/** Prefix-free hexadecimal scalar component, byte-identical in tuple-v1 and tuple-v2 keys. */
export function secondaryTupleComponent(type: SimpleDataType, value: BatchValue): string {
  if (value === null) throw new TypeError("A NULL has no secondary-index comparison term");
  if (type === "string") {
    if (typeof value !== "string") throw new TypeError("Invalid string index value");
    assertIndexedStringLength(value);
    if (value.length * 5 + 5 > MAX_FTS_POSTING_TERM_CHARACTERS) {
      throw new RangeError("Composite index term exceeds the persisted term limit");
    }
    let encoded = "";
    for (let index = 0; index < value.length; index += 1) {
      encoded += (value.charCodeAt(index) + 1).toString(16).padStart(5, "0");
    }
    return `${encoded}00000`;
  }
  if (type === "boolean") {
    if (typeof value !== "boolean") throw new TypeError("Invalid boolean index value");
    return value ? "1" : "0";
  }
  return secondaryIndexTerm(type, value);
}

const reversedHex = new Map(
  Array.from("0123456789abcdef", (character, index) => [
    character,
    "fedcba9876543210"[index] ?? "",
  ]),
);

/** Reverses the order of an internal hexadecimal term one UTF-16 code unit at a time. */
function reverseSecondaryHex(input: string): string {
  let output = "";
  for (let index = 0; index < input.length; index += 1) {
    const character = input.charAt(index);
    const reversed = reversedHex.get(character);
    if (reversed === undefined) throw new Error("Secondary index has a non-hexadecimal term");
    output += reversed;
  }
  return output;
}

/**
 * The posting term one indexed row is named under, or `undefined` when the row has no term.
 *
 * A leading NULL is deliberately left unindexed: no equality or range predicate can match it, and
 * a lookup only ever prefix-seeks from a non-null leading component, so the posting could never
 * be read. A NULL in a later component is indexed under the marker, which keeps the row inside
 * the prefix range of its non-null leading components — the whole point of `tuple-v2`.
 */
function secondaryTupleIndexTerm(
  index: SecondaryIndexRecord,
  columns: readonly TableColumnRecord[],
  values: readonly BatchValue[],
): string | undefined {
  if (columns.length !== values.length) throw new TypeError("Index key has the wrong arity");
  if (values[0] === null) return undefined;
  // tuple-v1 has no marker for a NULL component, so such a row stays out of the postings and the
  // planner refuses to prefix-prune through the index's nullable trailing columns.
  if (!secondaryIndexTermsCoverNulls(index) && values.some((value) => value === null)) {
    return undefined;
  }
  const directions = secondaryIndexDirections(index);
  let term = "";
  for (const [position, column] of columns.entries()) {
    const direction = directions[position] ?? "asc";
    const value = values[position] ?? null;
    const encoded =
      value === null
        ? secondaryNullComponent(direction)
        : secondaryTupleComponent(column.type, value);
    const component =
      value !== null && direction === "desc" ? reverseSecondaryHex(encoded) : encoded;
    if (term.length + component.length > MAX_FTS_POSTING_TERM_CHARACTERS) {
      throw new RangeError("Composite index term exceeds the persisted term limit");
    }
    term += component;
  }
  return term;
}

export function secondaryIndexComponentTerm(
  index: SecondaryIndexRecord,
  column: TableColumnRecord,
  position: number,
  value: BatchValue,
): string {
  const component = secondaryTupleComponent(column.type, value);
  if (secondaryIndexDirections(index)[position] !== "desc") return component;
  return reverseSecondaryHex(component);
}

export function decodeSecondaryTupleTerm(
  index: SecondaryIndexRecord,
  columns: readonly TableColumnRecord[],
  term: string,
): BatchValue[] {
  const directions = secondaryIndexDirections(index);
  let offset = 0;
  const values = columns.map((column, position) => {
    const direction = directions[position] ?? "asc";
    const descending = direction === "desc";
    const restore = (encoded: string): string =>
      descending ? reverseSecondaryHex(encoded) : encoded;
    // The marker is the one non-hexadecimal character a component can start with, so a tuple-v1
    // term — which never holds one — decodes exactly as before.
    if (term.startsWith(secondaryNullComponent(direction), offset)) {
      offset += 1;
      return null;
    }
    if (column.type === "string") {
      let value = "";
      for (;;) {
        const group = restore(term.slice(offset, offset + 5));
        if (group.length !== 5) throw new Error(`Secondary index ${index.name} has a bad term`);
        offset += 5;
        if (group === "00000") return value;
        const code = Number.parseInt(group, 16) - 1;
        if (!Number.isInteger(code) || code < 0 || code > 0xffff) {
          throw new Error(`Secondary index ${index.name} has a bad string term`);
        }
        value += String.fromCharCode(code);
      }
    }
    if (column.type === "boolean") {
      const encoded = restore(term.slice(offset, offset + 1));
      offset += 1;
      if (encoded !== "0" && encoded !== "1") {
        throw new Error(`Secondary index ${index.name} has a bad boolean term`);
      }
      return encoded === "1";
    }
    const encoded = restore(term.slice(offset, offset + 16));
    if (encoded.length !== 16) throw new Error(`Secondary index ${index.name} has a bad term`);
    offset += 16;
    const sortable = BigInt(`0x${encoded}`);
    const bits =
      (sortable & 0x8000_0000_0000_0000n) === 0n
        ? ~sortable & 0xffff_ffff_ffff_ffffn
        : sortable ^ 0x8000_0000_0000_0000n;
    secondaryNumberBits.setBigUint64(0, bits, false);
    const value = secondaryNumberBits.getFloat64(0, false);
    return column.type === "datetime" ? new Date(value) : value;
  });
  if (offset !== term.length) throw new Error(`Secondary index ${index.name} has a bad term`);
  return values;
}

/** Stable 32-bit key locator. Collisions retain extra candidates and are rechecked by SQL. */
export function secondaryKeyLocator(type: SimpleDataType, value: BatchValue): bigint {
  const token = keyToken(type, value);
  let hash = 0x811c9dc5;
  for (let index = 0; index < token.length; index += 1) {
    const code = token.charCodeAt(index);
    hash = Math.imul(hash ^ (code & 0xff), 0x01000193) >>> 0;
    hash = Math.imul(hash ^ (code >>> 8), 0x01000193) >>> 0;
  }
  return BigInt(hash >>> 0);
}

export function addSecondaryPosting(
  byTerm: Map<string, { rowIds: bigint[]; tf: number[] }>,
  index: SecondaryIndexRecord,
  columns: readonly TableColumnRecord[],
  values: readonly BatchValue[],
  locator: bigint,
  uniqueTerms?: Set<string>,
): void {
  const first = columns[0];
  if (first === undefined) return;
  const term = secondaryTupleIndexTerm(index, columns, values);
  if (term === undefined) return;
  // PostgreSQL UNIQUE ignores a row with a NULL in any indexed column: such rows now carry a
  // posting so a prefix lookup can find them, but they never join the uniqueness set.
  if (!values.some((value) => value === null)) {
    if (uniqueTerms?.has(term) === true) {
      throw new TypeError(`UNIQUE index ${index.name} has a duplicate key`);
    }
    uniqueTerms?.add(term);
  }
  const posting = byTerm.get(term) ?? { rowIds: [], tf: [] };
  posting.rowIds.push(locator);
  posting.tf.push(1);
  byTerm.set(term, posting);
}

/** Hidden row-ID lookup over append/base segment order without retaining one bigint per row. */
export function appendRowIdLocator(
  segments: readonly SegmentRecord[],
  expectedRows: number,
): (row: number) => bigint {
  const spans = appendRowIdSpans(segments, expectedRows);
  let spanIndex = 0;
  return (row) => {
    while (
      spanIndex < spans.length &&
      row >= (spans[spanIndex]?.rowStart ?? 0) + (spans[spanIndex]?.rowCount ?? 0)
    ) {
      spanIndex += 1;
    }
    const span = spans[spanIndex];
    if (span === undefined || row < span.rowStart) {
      throw new Error(`Secondary-index row ID is missing: ${String(row)}`);
    }
    return span.rowIdStart + BigInt(row - span.rowStart);
  };
}

/** Visible append/base row-ID spans with their logical output offsets attached. */
function appendRowIdSpans(segments: readonly SegmentRecord[], expectedRows: number): RowIdSpan[] {
  const spans: RowIdSpan[] = [];
  let outputStart = 0;
  for (const segment of segments) {
    const kind = segment.kind;
    if (kind !== "insert" && kind !== "base") continue;
    for (const span of mergeSourceRowIdSpans(segment, kind)) {
      spans.push({ ...span, rowStart: outputStart + span.rowStart });
    }
    outputStart += segment.rowCount;
  }
  const rows = spans.reduce((total, span) => total + span.rowCount, 0);
  if (rows !== expectedRows) throw new Error("Secondary-index row IDs differ from table rows");
  return spans;
}

/** Inverse append locator without allocating a bigint-keyed Map entry for every visible row. */
export function appendRowForLocator(
  segments: readonly SegmentRecord[],
  expectedRows: number,
): (locator: bigint) => number | undefined {
  const spans = appendRowIdSpans(segments, expectedRows).sort((left, right) =>
    left.rowIdStart < right.rowIdStart ? -1 : left.rowIdStart > right.rowIdStart ? 1 : 0,
  );
  return (locator) => {
    let low = 0;
    let high = spans.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if ((spans[middle]?.rowIdStart ?? 0n) <= locator) low = middle + 1;
      else high = middle;
    }
    const span = spans[low - 1];
    if (span === undefined) return undefined;
    const offset = locator - span.rowIdStart;
    if (offset < 0n || offset >= BigInt(span.rowCount)) return undefined;
    return span.rowStart + Number(offset);
  };
}

/** Term-range partitioning bounded by both record count and aggregate posting cardinality. */
export function chunkFtsPostings(postings: FtsPosting[], size = 128): FtsPosting[][] {
  const chunks: FtsPosting[][] = [];
  let chunk: FtsPosting[] = [];
  let rowIds = 0;
  const flush = (): void => {
    if (chunk.length === 0) return;
    chunks.push(chunk);
    chunk = [];
    rowIds = 0;
  };
  for (const posting of postings) {
    if (posting.rowIds.length > MAX_FTS_POSTING_ROW_IDS_PER_CHUNK) {
      throw new RangeError("One posting exceeds the persisted row-id chunk limit");
    }
    if (
      chunk.length >= Math.min(size, MAX_FTS_POSTINGS_PER_CHUNK) ||
      rowIds + posting.rowIds.length > MAX_FTS_POSTING_ROW_IDS_PER_CHUNK
    ) {
      flush();
    }
    chunk.push(posting);
    rowIds += posting.rowIds.length;
  }
  flush();
  return chunks;
}

/**
 * Tokenizes an insert batch's values for every active full-text column into commit deltas,
 * offering the event loop a turn every few thousand rows and sorting terms a run at a time.
 */
export async function buildFtsColumnDeltas(
  table: TableRecord,
  input: ColumnarBatch,
  rowIdStart: bigint,
): Promise<FtsColumnDelta[]> {
  const active = Object.entries(table.ftsColumns ?? {}).filter(
    ([, record]) => record.state !== "invalid",
  );
  if (active.length === 0) return [];
  const columnsById = new Map(table.columns.map((column) => [column.id, column] as const));
  const deltas: FtsColumnDelta[] = [];
  for (const [columnId] of active) {
    const column = columnsById.get(columnId);
    if (column === undefined) continue;
    const byTerm = new Map<string, { rowIds: bigint[]; tf: number[] }>();
    let totalTokens = 0;
    const values = input.columns[column.name] ?? [];
    for (let index = 0; index < values.length; index += 1) {
      if ((index & DELTA_ROW_SLICE_MASK) === DELTA_ROW_SLICE_MASK) await maybeYieldToEventLoop();
      const value = values[index] ?? null;
      const documentValue =
        column.type === "string" && column.sqlDomain === undefined && typeof value === "string"
          ? protectedSqlTextValue(value)
          : value;
      totalTokens += addFtsDocument(byTerm, documentValue, rowIdStart + BigInt(index));
    }
    const postings: FtsPosting[] = [];
    for (const term of await sortedTermsSliced(byTerm.keys())) {
      const posting = byTerm.get(term);
      if (posting !== undefined) postings.push({ term, rowIds: posting.rowIds, tf: posting.tf });
    }
    deltas.push({ columnId, postings, totalTokens });
  }
  return deltas;
}

/** Scalar postings for an inserted/replaced full row, including empty coverage entries. */
export async function buildSecondaryInsertDeltas(
  table: TableRecord,
  input: ColumnarBatch,
  rowIdStart: bigint,
): Promise<FtsColumnDelta[]> {
  const active = Object.values(table.secondaryIndexes ?? {}).filter(
    (index) => index.state !== "invalid",
  );
  if (active.length === 0) return [];
  const columnsById = new Map(table.columns.map((column) => [column.id, column] as const));
  const keyColumn = getUniqueKeyColumn(table);
  const keys = keyColumn === undefined ? undefined : (input.columns[keyColumn.name] ?? []);
  const rowCount = input.rowCount ?? Object.values(input.columns)[0]?.length ?? 0;
  const deltas: FtsColumnDelta[] = [];
  for (const index of active) {
    const columns = secondaryIndexColumnIds(index).map((columnId) => columnsById.get(columnId));
    if (columns.some((column) => column === undefined)) continue;
    const indexedColumns = columns as TableColumnRecord[];
    const byTerm = new Map<string, { rowIds: bigint[]; tf: number[] }>();
    try {
      for (let row = 0; row < rowCount; row += 1) {
        if ((row & DELTA_ROW_SLICE_MASK) === DELTA_ROW_SLICE_MASK) await maybeYieldToEventLoop();
        const values = indexedColumns.map((column) => input.columns[column.name]?.[row] ?? null);
        const locator =
          keyColumn === undefined
            ? rowIdStart + BigInt(row)
            : secondaryKeyLocator(keyColumn.type, keys?.[row] ?? null);
        addSecondaryPosting(byTerm, index, indexedColumns, values, locator);
      }
    } catch (error) {
      if (error instanceof RangeError && index.unique !== true) continue;
      throw error;
    }
    const postings = await sortedSecondaryPostingsSliced(byTerm);
    deltas.push({
      columnId: index.storageColumnId,
      postings,
      totalTokens: postingFrequencyTotal(postings),
    });
  }
  return deltas;
}

/** Scalar postings for changed indexed values; unchanged indexes still carry stale-writer coverage. */
export async function buildSecondaryUpdateDeltas(
  table: TableRecord,
  input: UpdateBatchInput,
  preImages: ReadonlyArray<Record<string, BatchValue> | undefined> = [],
): Promise<FtsColumnDelta[]> {
  const active = Object.values(table.secondaryIndexes ?? {}).filter(
    (index) => index.state !== "invalid",
  );
  if (active.length === 0) return [];
  const keyColumn = getUniqueKeyColumn(table);
  if (keyColumn === undefined) return [];
  const columnsById = new Map(table.columns.map((column) => [column.id, column] as const));
  const deltas: FtsColumnDelta[] = [];
  for (const index of active) {
    const columns = secondaryIndexColumnIds(index).map((columnId) => columnsById.get(columnId));
    if (columns.some((column) => column === undefined)) continue;
    const indexedColumns = columns as TableColumnRecord[];
    const byTerm = new Map<string, { rowIds: bigint[]; tf: number[] }>();
    const affected = indexedColumns.some((column) => input.changes[column.name] !== undefined);
    if (affected) {
      try {
        for (let row = 0; row < input.keys.length; row += 1) {
          if ((row & DELTA_ROW_SLICE_MASK) === DELTA_ROW_SLICE_MASK) await maybeYieldToEventLoop();
          const values = indexedColumns.map((column) =>
            input.changes[column.name] === undefined
              ? (preImages[row]?.[column.name] ?? null)
              : (input.changes[column.name]?.[row] ?? null),
          );
          addSecondaryPosting(
            byTerm,
            index,
            indexedColumns,
            values,
            secondaryKeyLocator(keyColumn.type, input.keys[row] ?? null),
          );
        }
      } catch (error) {
        if (error instanceof RangeError && index.unique !== true) continue;
        throw error;
      }
    }
    const postings = await sortedSecondaryPostingsSliced(byTerm);
    deltas.push({
      columnId: index.storageColumnId,
      postings,
      totalTokens: postingFrequencyTotal(postings),
    });
  }
  return deltas;
}

/** Rows a delta builder handles between offers of an event-loop turn, less one: a mask. */
const DELTA_ROW_SLICE_MASK = 4_095;

/** Terms one sorted run holds before a sliced sort merges runs: a few milliseconds each. */
export const SORTED_RUN_TERMS = 16_384;

/** Terms per batch `mergeSortedRuns` hands back between turns. */
const MERGE_BATCH_TERMS = 4_096;

/**
 * `terms` in code-unit order, sorted in runs of `SORTED_RUN_TERMS` and merged, with the event
 * loop offered a turn between runs and merged batches, so no single call sorts them all.
 */
export async function sortedTermsSliced(terms: Iterable<string>): Promise<string[]> {
  const all = [...terms];
  if (all.length <= SORTED_RUN_TERMS) return all.sort();
  const runs: string[][] = [];
  for (let start = 0; start < all.length; start += SORTED_RUN_TERMS) {
    await maybeYieldToEventLoop();
    runs.push(all.slice(start, start + SORTED_RUN_TERMS).sort());
  }
  const sorted: string[] = [];
  for await (const batch of mergeSortedRuns(runs)) {
    for (const term of batch) sorted.push(term);
    await maybeYieldToEventLoop();
  }
  return sorted;
}

/**
 * A batch's secondary postings in term order, each with its row locators ascending and without
 * repeats: the terms sort by `sortedTermsSliced`, and each posting's locators by
 * `sortedUniqueRowIdsSliced`, with turns offered between batches.
 */
export async function sortedSecondaryPostingsSliced(
  byTerm: ReadonlyMap<string, { rowIds: bigint[]; tf: number[] }>,
): Promise<FtsPosting[]> {
  const postings: FtsPosting[] = [];
  let sinceYield = 0;
  for (const term of await sortedTermsSliced(byTerm.keys())) {
    const rowIds = await sortedUniqueRowIdsSliced(byTerm.get(term)?.rowIds ?? []);
    postings.push({ term, rowIds, tf: new Array<number>(rowIds.length).fill(1) });
    sinceYield += rowIds.length + 1;
    if (sinceYield >= MERGE_BATCH_TERMS) {
      sinceYield = 0;
      await maybeYieldToEventLoop();
    }
  }
  return postings;
}

const compareRowIds = (left: bigint, right: bigint): number =>
  left < right ? -1 : left > right ? 1 : 0;

/** Row locators one native sort takes, or one merge step copies, between turns. */
const ROW_ID_SORT_RUN = 16_384;

/**
 * One term's row locators, ascending and without repeats. A term shared by most of a large
 * batch — a low-cardinality index — has as many locators as the batch has rows: they are sorted
 * in runs and merged pairwise, with the event loop offered a turn between steps, rather than in
 * one synchronous sort.
 */
export async function sortedUniqueRowIdsSliced(values: readonly bigint[]): Promise<bigint[]> {
  if (values.length <= ROW_ID_SORT_RUN) return [...new Set(values)].sort(compareRowIds);
  let runs: bigint[][] = [];
  for (let start = 0; start < values.length; start += ROW_ID_SORT_RUN) {
    runs.push(values.slice(start, start + ROW_ID_SORT_RUN).sort(compareRowIds));
    await maybeYieldToEventLoop();
  }
  while (runs.length > 1) {
    const merged: bigint[][] = [];
    for (let index = 0; index < runs.length; index += 2) {
      const left = runs[index] ?? [];
      const right = runs[index + 1];
      merged.push(right === undefined ? left : await mergeRowIdRuns(left, right));
    }
    runs = merged;
  }
  // A run sorted whole may still hold repeats; merging drops repeats across runs.
  const only = runs[0] ?? [];
  const unique: bigint[] = [];
  for (let index = 0; index < only.length; index += 1) {
    const value = only[index] as bigint;
    if (unique.length === 0 || unique[unique.length - 1] !== value) unique.push(value);
    if (index % ROW_ID_SORT_RUN === ROW_ID_SORT_RUN - 1) await maybeYieldToEventLoop();
  }
  return unique;
}

async function mergeRowIdRuns(
  left: readonly bigint[],
  right: readonly bigint[],
): Promise<bigint[]> {
  const merged: bigint[] = [];
  let leftIndex = 0;
  let rightIndex = 0;
  while (leftIndex < left.length || rightIndex < right.length) {
    const leftValue = left[leftIndex];
    const rightValue = right[rightIndex];
    if (rightValue === undefined || (leftValue !== undefined && leftValue <= rightValue)) {
      merged.push(leftValue as bigint);
      leftIndex += 1;
    } else {
      merged.push(rightValue);
      rightIndex += 1;
    }
    if (merged.length % ROW_ID_SORT_RUN === 0) await maybeYieldToEventLoop();
  }
  return merged;
}

/**
 * The terms of sorted `runs` in one code-unit order, a batch of at most `MERGE_BATCH_TERMS` at a
 * time, merged through a binary min-heap of the runs' next terms.
 */
export async function* mergeSortedRuns(runs: readonly string[][]): AsyncGenerator<string[]> {
  if (runs.length === 1) {
    const run = runs[0] ?? [];
    for (let start = 0; start < run.length; start += MERGE_BATCH_TERMS) {
      yield run.slice(start, start + MERGE_BATCH_TERMS);
    }
    return;
  }
  const positions = new Int32Array(runs.length);
  const head = (run: number): string => runs[run]?.[positions[run] ?? 0] ?? "";
  const heap = runs.flatMap((run, index) => (run.length > 0 ? [index] : []));
  const siftDown = (from: number): void => {
    let at = from;
    for (;;) {
      const left = at * 2 + 1;
      const right = left + 1;
      let least = at;
      if (left < heap.length && head(heap[left] ?? 0) < head(heap[least] ?? 0)) least = left;
      if (right < heap.length && head(heap[right] ?? 0) < head(heap[least] ?? 0)) least = right;
      if (least === at) return;
      const swap = heap[at] ?? 0;
      heap[at] = heap[least] ?? 0;
      heap[least] = swap;
      at = least;
    }
  };
  for (let index = (heap.length >> 1) - 1; index >= 0; index -= 1) siftDown(index);
  let batch: string[] = [];
  while (heap.length > 0) {
    const run = heap[0] ?? 0;
    batch.push(head(run));
    positions[run] = (positions[run] ?? 0) + 1;
    if ((positions[run] ?? 0) >= (runs[run]?.length ?? 0)) {
      const last = heap.pop() ?? 0;
      if (heap.length > 0) heap[0] = last;
    }
    siftDown(0);
    if (batch.length === MERGE_BATCH_TERMS) {
      yield batch;
      batch = [];
    }
  }
  if (batch.length > 0) yield batch;
}

export function secondaryIndexUpdateNeedsPreImages(
  table: TableRecord,
  input: UpdateBatchInput,
): boolean {
  const changed = new Set(Object.keys(input.changes));
  const columnsById = new Map(table.columns.map((column) => [column.id, column.name] as const));
  return Object.values(table.secondaryIndexes ?? {}).some((index) => {
    if (index.state === "invalid") return false;
    const names = secondaryIndexColumnIds(index).map((columnId) => columnsById.get(columnId));
    return (
      names.some((name) => name !== undefined && changed.has(name)) &&
      names.some((name) => name === undefined || !changed.has(name))
    );
  });
}

/** Empty postings still prove that a delete writer observed every active scalar index. */
export function buildSecondaryDeleteCoverage(table: TableRecord): FtsColumnDelta[] {
  return Object.values(table.secondaryIndexes ?? {}).flatMap((index) =>
    index.state === "invalid"
      ? []
      : [
          {
            columnId: index.storageColumnId,
            postings: [],
            totalTokens: 0,
          },
        ],
  );
}

export function readyUniqueSecondaryIndexes(
  table: TableRecord,
): Array<{ indexId: string; index: SecondaryIndexRecord; columns: TableColumnRecord[] }> {
  const columnsById = new Map(table.columns.map((column) => [column.id, column] as const));
  return Object.entries(table.secondaryIndexes ?? {}).flatMap(([indexId, index]) => {
    if (index.unique !== true || index.uniqueEnforced !== true) return [];
    const columns = secondaryIndexColumnIds(index).map((columnId) => columnsById.get(columnId));
    return columns.some((column) => column === undefined)
      ? []
      : [{ indexId, index, columns: columns as TableColumnRecord[] }];
  });
}

export function secondaryUniqueTerm(
  index: SecondaryIndexRecord,
  columns: readonly TableColumnRecord[],
  values: readonly BatchValue[],
): string | undefined {
  // PostgreSQL UNIQUE semantics: a row with a NULL in any indexed column does not participate in
  // uniqueness, so any number of them may coexist. The postings name those rows under a tuple-v2
  // marker for pruning, but the membership set never holds their terms.
  if (values.some((value) => value === null)) return undefined;
  return secondaryTupleIndexTerm(index, columns, values);
}

export function assertNoDuplicateUniqueTerms(
  index: SecondaryIndexRecord,
  terms: readonly string[],
): void {
  const seen = new Set<string>();
  for (const term of terms) {
    if (seen.has(term)) throw new TypeError(`UNIQUE index ${index.name} has a duplicate key`);
    seen.add(term);
  }
}

/**
 * Refuses a batch that repeats a ready UNIQUE index term within itself, before the statement
 * registers anything: found later, after the table key is registered, the same duplicate
 * would poison the whole scope instead of failing the one statement.
 */
export function assertBatchSecondaryTermsDistinct(table: TableRecord, input: ColumnarBatch): void {
  const rowCount = input.rowCount ?? Object.values(input.columns)[0]?.length ?? 0;
  for (const { index, columns } of readyUniqueSecondaryIndexes(table)) {
    const terms: string[] = [];
    for (let row = 0; row < rowCount; row += 1) {
      const term = secondaryUniqueTerm(
        index,
        columns,
        columns.map((column) => input.columns[column.name]?.[row] ?? null),
      );
      if (term !== undefined) terms.push(term);
    }
    assertNoDuplicateUniqueTerms(index, terms);
  }
}

export function stageSecondaryUniqueInsertChanges(
  transaction: DatabaseTransaction,
  table: TableRecord,
  input: ColumnarBatch,
  oldImages?: ReadonlyArray<Record<string, BatchValue> | undefined>,
): void {
  const rowCount = input.rowCount ?? Object.values(input.columns)[0]?.length ?? 0;
  for (const { indexId, index, columns } of readyUniqueSecondaryIndexes(table)) {
    const namespaceId = secondaryUniqueKeyNamespace(table.id, indexId);
    const removed = (oldImages ?? []).flatMap((old) => {
      if (old === undefined) return [];
      const term = secondaryUniqueTerm(
        index,
        columns,
        columns.map((column) => old[column.name] ?? null),
      );
      return term === undefined ? [] : [term];
    });
    if (removed.length > 0) {
      transaction.setUniqueKeyChanges({
        tableId: namespaceId,
        keyTokens: removed,
        requireAbsent: false,
        remove: true,
      });
    }
    const added: string[] = [];
    for (let row = 0; row < rowCount; row += 1) {
      const term = secondaryUniqueTerm(
        index,
        columns,
        columns.map((column) => input.columns[column.name]?.[row] ?? null),
      );
      if (term !== undefined) added.push(term);
    }
    assertNoDuplicateUniqueTerms(index, added);
    // The empty entry is deliberate coverage: a stale writer that never saw this enforced unique
    // index supplies no namespace entry, which the atomic store rejects instead of accepting an
    // unenforced commit.
    transaction.setUniqueKeyChanges({
      tableId: namespaceId,
      keyTokens: added,
      requireAbsent: true,
    });
  }
}

export function stageSecondaryUniqueMutationChanges(
  transaction: DatabaseTransaction,
  table: TableRecord,
  input: UpdateBatchInput | undefined,
  oldImages: ReadonlyArray<Record<string, BatchValue> | undefined>,
): void {
  for (const { indexId, index, columns } of readyUniqueSecondaryIndexes(table)) {
    const namespaceId = secondaryUniqueKeyNamespace(table.id, indexId);
    const removed = oldImages.flatMap((old) => {
      if (old === undefined) return [];
      const term = secondaryUniqueTerm(
        index,
        columns,
        columns.map((column) => old[column.name] ?? null),
      );
      return term === undefined ? [] : [term];
    });
    transaction.setUniqueKeyChanges({
      tableId: namespaceId,
      keyTokens: removed,
      requireAbsent: false,
      remove: true,
    });
    if (input === undefined) continue;
    const added = oldImages.flatMap((old, row) => {
      if (old === undefined) return [];
      const term = secondaryUniqueTerm(
        index,
        columns,
        // A column the statement assigns takes the assigned value even when that value is
        // NULL: falling back to the old value there would re-register the term the row is
        // giving up, and the phantom would refuse every later row that wants it.
        columns.map((column) => {
          const assigned = input.changes[column.name];
          return assigned === undefined ? (old[column.name] ?? null) : (assigned[row] ?? null);
        }),
      );
      return term === undefined ? [] : [term];
    });
    assertNoDuplicateUniqueTerms(index, added);
    transaction.setUniqueKeyChanges({
      tableId: namespaceId,
      keyTokens: added,
      requireAbsent: true,
    });
  }
}
