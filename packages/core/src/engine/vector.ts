import { dateMilliseconds } from "../date-value.js";
import {
  MAX_TEMP_RUN_BATCH_BYTES,
  MAX_TEMP_RUN_PAGE_BYTES,
  MAX_TEMP_RUN_PAGES_PER_BATCH,
} from "../storage/types.js";
import type { DatabaseRow } from "./database.js";
import type {
  AggregateName,
  BinaryOperator,
  ComparisonOperator,
  CompiledQuery,
  JoinPlan,
  Predicate,
  PredicateOperator,
  Expression,
  QueryResult,
  QueryRow,
  QueryValue,
  ScalarFunctionName,
  TableSource,
} from "./query.js";
import {
  cachedListMembership,
  childExpressions,
  distinctFromComparison,
  nullOrder,
  isScalarFunctionName,
  likeMatches,
  orderOutputName,
  parseQuantified,
  quantifiedComparison,
  scalarFunctionValue,
} from "./query.js";
import { jsonValueOf } from "./sql-json.js";
import {
  bm25DocumentScore,
  cachedQueryTerms,
  FtsStatsAccumulator,
  fullTermsMask,
  renderDocumentValue,
  termFrequencies,
  termsMask,
  tokenize,
  type FtsQueryTerm,
  type FtsStats,
} from "./fts.js";
import { ByteGroupIndex, type GroupIndexKey } from "./group-index.js";
import { ByteJoinIndex } from "./join-index.js";
import {
  QueryMemoryBudgetError,
  QueryMemoryContext,
  type QueryMemoryReservation,
  type QueryMemoryUsage,
} from "./memory.js";
import {
  compareSqlStrings,
  compileSimilarPattern,
  defineSqlResultProperty,
} from "./sql-semantics.js";
import {
  exactNumericBinary,
  exactNumericCompare,
  collatedDomainCompare,
  enumDomainCompare,
  externalSqlDomainValue,
  isExactNumeric,
  protectedSqlTextValue,
} from "./sql-domains.js";
import { buildSortKeyColumn, sortKeyIndexes } from "./sort-keys.js";

const DEFAULT_BATCH_ROWS = 2_048;
/** Above this, locating each IN member separately costs more than scanning between them. */
const MAX_SPLIT_LIST_MEMBERS = 64;
/**
 * Upper bound on the direct-address slot space for compound dictionary-code grouping: 65,536
 * slots reserve 512 KiB of references, small against the default budget while covering typical
 * categorical column combinations.
 */
const MULTI_CODE_GROUP_SLOT_CAP = 65_536;
const HASH_SPILL_SCAN_CHUNK_ROWS = 512;
const NULL_STRING_CODE = 0xffffffff;
const QUERY_REFERENCE_BYTES = 8;
const QUERY_VALUE_TAG_BYTES = 1;
const AGGREGATE_ACCUMULATOR_BYTES = 24;
/** Modeled sparse-map bookkeeping for one packed compound dictionary-code group. */
const PACKED_GROUP_ENTRY_BYTES = 24;
const SPILL_PAGE_MAGIC = 0x5350494c;
const SPILL_PAGE_HEADER_BYTES = 8;
const vectorTextEncoder = new TextEncoder();

export type VectorType = "boolean" | "number" | "string" | "datetime";

/**
 * A resident slice of a logically longer streamed column. Window arrays index rows relative to
 * `start`; `length` on the vector remains the logical table length.
 */
export interface VectorWindow {
  readonly start: number;
  readonly length: number;
}

interface VectorBase {
  readonly validity: Uint8Array;
  readonly length: number;
  readonly window?: VectorWindow;
}

export interface BooleanVector extends VectorBase {
  readonly kind: "boolean";
  readonly values: Uint8Array;
}

export interface NumberVector extends VectorBase {
  readonly kind: "number";
  readonly values: Float64Array;
}

export interface DateTimeVector extends VectorBase {
  readonly kind: "datetime";
  readonly values: Float64Array;
}

export interface StringVector extends VectorBase {
  readonly kind: "string";
  readonly codes: Uint32Array;
  readonly dictionary: readonly string[];
}

export type ColumnVector = BooleanVector | NumberVector | DateTimeVector | StringVector;

export interface ColumnarColumnInput {
  readonly type: VectorType;
  readonly values: readonly QueryValue[];
}

export interface ColumnarTable {
  readonly name: string;
  readonly rowCount: number;
  readonly columns: ReadonlyMap<string, ColumnVector>;
  readonly uniqueKey?: string;
  /** Optional physical-row permutation supplied by an exact index-ordered materialization. */
  readonly scanOrder?: Uint32Array;
  /** True only when that permutation satisfies this prepared plan's complete ORDER BY. */
  readonly orderedForPlan?: true;
}

export interface PreparedVectorQuery {
  readonly memoryUsage: QueryMemoryUsage;
  execute(): QueryResult;
  executeAsync(options?: AsyncQueryExecutionOptions): Promise<QueryResult>;
  executeBatches(
    options: QueryBatchExecutionOptions,
    consume: (batch: QueryResult) => void | Promise<void>,
  ): Promise<readonly string[]>;
  close(): void;
}

export interface QuerySpillStore {
  putPage(ownerId: string, runId: string, pageIndex: number, bytes: Uint8Array): Promise<void>;
  /**
   * Optional: several pages in one storage round trip. The executor batches only pages it has
   * already materialized (chunked, so the batch never grows the spill's own memory footprint)
   * and falls back to per-page writes when this is absent.
   */
  putPages?(
    pages: ReadonlyArray<{ ownerId: string; runId: string; pageIndex: number; bytes: Uint8Array }>,
  ): Promise<void>;
  getPage(ownerId: string, runId: string, pageIndex: number): Promise<Uint8Array | undefined>;
  removeRun(ownerId: string, runId: string): Promise<void>;
  removeOwner(ownerId: string): Promise<void>;
}

/** Pages per batched spill write: bounds the transient encoded copies a batch holds. */
const SPILL_WRITE_BATCH_PAGES = Math.min(8, MAX_TEMP_RUN_PAGES_PER_BATCH);

function boundedSpillPageRows(value: number | undefined): number {
  const rows = value ?? DEFAULT_BATCH_ROWS;
  if (!Number.isSafeInteger(rows) || rows <= 0) {
    throw new RangeError("Query spill page rows must be a positive whole number");
  }
  // Page rows are a tuning target, not a request to buffer an unbounded number of rows. Larger
  // pages do not improve the block-store round trip because byte-bounded batches provide that
  // amortization independently.
  return Math.min(rows, DEFAULT_BATCH_ROWS);
}

/** Writes already-materialized pages through the batch method when the store offers one. */
async function writeSpillPages(
  store: QuerySpillStore,
  pages: Array<{ ownerId: string; runId: string; pageIndex: number; bytes: Uint8Array }>,
): Promise<void> {
  const batched = store.putPages?.bind(store);
  if (batched === undefined) {
    for (const page of pages) {
      await store.putPage(page.ownerId, page.runId, page.pageIndex, page.bytes);
    }
    return;
  }
  let batch: typeof pages = [];
  let batchBytes = 0;
  for (const page of pages) {
    if (page.bytes.byteLength > MAX_TEMP_RUN_PAGE_BYTES) {
      throw new RangeError("Query spill page exceeds the storage limit");
    }
    if (
      batch.length > 0 &&
      (batch.length === SPILL_WRITE_BATCH_PAGES ||
        batchBytes + page.bytes.byteLength > MAX_TEMP_RUN_BATCH_BYTES)
    ) {
      await batched(batch);
      batch = [];
      batchBytes = 0;
    }
    batch.push(page);
    batchBytes += page.bytes.byteLength;
  }
  if (batch.length > 0) await batched(batch);
}

async function writeSpillRowPages(
  store: QuerySpillStore,
  ownerId: string,
  runId: string,
  startPageIndex: number,
  columns: readonly string[],
  rows: readonly QueryRow[],
  pageRows: number,
): Promise<number> {
  let pending: Array<{
    ownerId: string;
    runId: string;
    pageIndex: number;
    bytes: Uint8Array;
  }> = [];
  let pendingBytes = 0;
  let pageCount = 0;
  for (const bytes of encodeSpillRowPages(columns, rows, pageRows)) {
    if (
      pending.length > 0 &&
      (pending.length === SPILL_WRITE_BATCH_PAGES ||
        pendingBytes + bytes.byteLength > MAX_TEMP_RUN_BATCH_BYTES)
    ) {
      await writeSpillPages(store, pending);
      pending = [];
      pendingBytes = 0;
    }
    pending.push({ ownerId, runId, pageIndex: startPageIndex + pageCount, bytes });
    pendingBytes += bytes.byteLength;
    pageCount += 1;
  }
  await writeSpillPages(store, pending);
  return pageCount;
}

export interface AsyncQueryExecutionOptions {
  readonly spillStore?: QuerySpillStore;
  readonly spillPageRows?: number;
  /**
   * Makes the scan-source window [start, start + length) resident before each batch. Supplied by
   * a streaming preparation whose scan-source vectors hold only a sliding window; the executor
   * awaits it at the top of every scan batch in every asynchronous execution path. A numeric
   * return is the exclusive end of the resident window: the executor clamps the batch to it, so
   * batches align to storage-block boundaries and a loader can install whole decoded blocks by
   * reference instead of stitching copies.
   */
  readonly loadScanWindow?: (start: number, length: number) => number | Promise<number>;
}

export interface QueryBatchExecutionOptions extends AsyncQueryExecutionOptions {
  /** Maximum result rows handed to the consumer at once. */
  readonly batchRows: number;
  /** Stops before the next scan batch and is checked after every awaited storage read. */
  readonly signal?: AbortSignal;
}

export interface PrepareVectorQueryOptions {
  readonly memoryContext?: QueryMemoryContext;
  /**
   * Exact BM25 corpus statistics served by the persisted full-text index, keyed by each
   * scoring node's compiled signature (`JSON.stringify` of the plan node). A bound node with
   * served statistics skips its whole-scan corpus pass, which is also what makes streamed
   * scans and index pruning legal for scoring plans.
   */
  readonly ftsStats?: ReadonlyMap<string, FtsStats>;
}

type BoundExpression =
  | { kind: "literal"; value: QueryValue; signature: string }
  | {
      kind: "column";
      source: number;
      column: string;
      vector: ColumnVector;
      signature: string;
    }
  | { kind: "wildcard"; signature: string }
  | {
      kind: "binary";
      operator: "+" | "-" | "*" | "/";
      left: BoundExpression;
      right: BoundExpression;
      signature: string;
    }
  | {
      kind: "call";
      name: AggregateName | ScalarFunctionName;
      arguments: BoundExpression[];
      aggregateIndex?: number;
      signature: string;
    }
  | { kind: "list"; items: BoundExpression[]; signature: string }
  | {
      kind: "condition";
      operator: PredicateOperator;
      left: BoundExpression;
      right: BoundExpression;
      escape?: string;
      signature: string;
    }
  | {
      kind: "logical";
      operator: "and" | "or";
      left: BoundExpression;
      right: BoundExpression;
      signature: string;
    }
  | { kind: "not"; operand: BoundExpression; signature: string }
  | {
      kind: "case";
      branches: Array<{ when: BoundExpression; then: BoundExpression }>;
      otherwise?: BoundExpression;
      signature: string;
    }
  | BoundFtsExpression;

interface BoundFtsExpression {
  kind: "fts";
  op: "match" | "bm25";
  columns: Array<{
    kind: "column";
    source: number;
    column: string;
    vector: ColumnVector;
    signature: string;
  }>;
  terms: FtsQueryTerm[];
  /** Per column, the per-dictionary term table; null until the column's first string batch. */
  caches: Array<FtsDictionaryCache | null>;
  /** BM25 corpus statistics: index-served at bind, else one whole-scan pass on first use. */
  stats?: FtsStats;
  /** Reusable per-row document scratch — evaluation runs once per scanned row. */
  scratchRow?: FtsRowScratch;
  memory?: QueryMemoryContext;
  signature: string;
}

/** The per-row document accumulation target, reused across rows to avoid hot-path garbage. */
interface FtsRowScratch {
  present: boolean;
  mask: number;
  length: number;
  frequencies: number[];
}

interface FtsDictionaryCache {
  dictionary: readonly string[] | undefined;
  /** Per dictionary code: bitmask of query terms present in that entry's tokens. */
  termMask: Uint32Array;
  /** BM25 only — per dictionary code: token count and per-term frequencies (flattened). */
  tokenCount?: Uint32Array;
  termTf?: Uint32Array;
  /** Sticky: once a scored read upgrades the cache, later window rebuilds keep scoring. */
  scoring?: boolean;
  reservation?: QueryMemoryReservation;
}

interface BoundPredicate {
  readonly left: BoundExpression;
  readonly operator: PredicateOperator;
  readonly right: BoundExpression;
  readonly escape?: string;
  /** Dictionary-code rewrite for string equality against a literal; codes compare per row. */
  readonly dictionaryEquality?: DictionaryEquality;
  /** Dictionary-level LIKE against a literal pattern: one match pass per dictionary. */
  readonly dictionaryLike?: DictionaryLike;
  /** Unboxed comparison of a bare numeric/datetime/boolean column against a literal. */
  readonly primitive?: PrimitiveComparison;
  /** Unboxed membership of a bare numeric/datetime column in a literal list. */
  readonly primitiveIn?: PrimitiveInList;
  /** OR of AND-groups, each group's predicates compiled like any other. */
  readonly disjunction?: BoundDisjunction;
}

/**
 * A disjunction in disjunctive normal form: every branch is a conjunction of predicates that
 * compile their own kernels. Only present when every leaf is a plain condition -- anything else
 * keeps the generic row-at-a-time evaluator for the whole predicate.
 */
interface BoundDisjunction {
  readonly branches: ReadonlyArray<readonly BoundPredicate[]>;
}

interface PrimitiveComparison {
  readonly source: number;
  readonly vector: NumberVector | DateTimeVector | BooleanVector;
  readonly operator: ComparisonOperator;
  /** The literal as a float: numbers as-is, datetimes as epoch millis, booleans as 0/1. */
  readonly value: number;
}

interface PrimitiveInList {
  readonly source: number;
  readonly vector: NumberVector | DateTimeVector;
  readonly members: ReadonlySet<number>;
  /** Smallest and largest member: the range an ascending column can restrict the scan to. */
  readonly minimum: number;
  readonly maximum: number;
  /** True for NOT IN, which keeps exactly the non-null rows the list does not contain. */
  readonly negated: boolean;
}

interface DictionaryEquality {
  readonly source: number;
  readonly vector: StringVector;
  readonly value: string;
  readonly negated: boolean;
  readonly cache: { dictionary: readonly string[] | undefined; code: number };
}

interface DictionaryLike {
  readonly source: number;
  readonly vector: StringVector;
  readonly pattern: string;
  readonly caseInsensitive: boolean;
  readonly escape?: string;
  readonly negated: boolean;
  readonly cache: { dictionary: readonly string[] | undefined; matches: Uint8Array };
}

interface BoundJoin {
  readonly kind: "inner" | "left" | "semi" | "anti";
  readonly buildSource: number;
  readonly probe: BoundExpression;
  readonly build: BoundExpression;
  readonly lookup: JoinLookup;
  /** Nested-loop fallback for non-equi or multi-key ON conditions; lookup is unused when set. */
  readonly loop?: { condition: BoundExpression; rowCount: number };
  /**
   * Per-dictionary-code build rows when the probe is a bare dictionary string column: each
   * distinct probe value hits the hash lookup once, and every other row is an array read.
   */
  codeLookup?: { dictionary: readonly string[]; rows: Int32Array };
}

interface BoundSelectItem {
  readonly expression: BoundExpression;
  readonly alias: string;
}

interface AggregateSpec {
  readonly name: AggregateName;
  readonly argument: BoundExpression;
  /** Per-row separator for STRING_AGG. */
  readonly delimiter?: BoundExpression;
  readonly orderBy?: ReadonlyArray<{
    expression: BoundExpression;
    direction: "asc" | "desc";
    nulls?: "first" | "last";
  }>;
  /** `COUNT(DISTINCT x)`: the accumulator sees each distinct value once per group. */
  readonly distinct?: boolean;
  /** Set for MIN/MAX/COUNT over a bare datetime column: aggregate on raw epoch milliseconds. */
  readonly rawDatetime?: { source: number; vector: DateTimeVector };
  /** Set for any aggregate over a bare number column: read the typed vector directly. */
  readonly rawNumber?: { source: number; vector: NumberVector };
}

interface GroupState {
  readonly groupValues: QueryValue[];
  readonly counts: Float64Array;
  readonly sums: Float64Array;
  readonly values: Array<QueryValue | undefined>;
  /** Present only when this plan has a variable-width list aggregate; arrays stay lazy. */
  readonly lists?: Array<QueryValue[] | undefined>;
  readonly valueReservations: Array<QueryMemoryReservation | undefined>;
  readonly valueReservationBytes: Float64Array;
  /**
   * The values a DISTINCT aggregate has already folded in, one set per aggregate slot and only
   * for the slots that are DISTINCT. This is what makes several of them per select possible: each
   * carries its own set, so they no longer compete for the single deduplicating GROUP BY that
   * `COUNT(DISTINCT x)` used to be rewritten into.
   */
  readonly distincts: Array<Set<unknown> | undefined> | undefined;
}

interface BoundPlan {
  readonly sourceTables: readonly ColumnarTable[];
  readonly sourceAliases: readonly string[];
  readonly scanSource: number;
  readonly joins: readonly BoundJoin[];
  readonly predicates: readonly BoundPredicate[];
  readonly having: readonly BoundPredicate[];
  readonly groupBy: readonly BoundExpression[];
  readonly groupIndexBySignature: ReadonlyMap<string, number>;
  readonly aggregates: readonly AggregateSpec[];
  readonly hasListAggregate: boolean;
  readonly select: readonly BoundSelectItem[];
  readonly orderBy: ReadonlyArray<{
    outputName: string;
    direction: "asc" | "desc";
    nulls?: "first" | "last";
  }>;
  readonly grouped: boolean;
  readonly sourceOrdered: boolean;
  readonly codeGrouping?: { source: number; vector: StringVector };
  readonly wildcard: boolean;
  readonly limit?: number;
  readonly offset?: number;
}

interface BatchRows {
  readonly length: number;
  readonly rowsBySource: readonly Int32Array[];
  readonly memory?: QueryMemoryContext;
}

interface JoinLookup {
  readonly unique: boolean;
  firstRow(key: unknown): number;
  nextRow(row: number): number;
}

export function createColumnarTable(
  name: string,
  columns: ReadonlyMap<string, ColumnarColumnInput>,
  uniqueKey?: string,
): ColumnarTable {
  const first = columns.values().next().value;
  const rowCount = first?.values.length ?? 0;
  const vectors = new Map<string, ColumnVector>();
  for (const [columnName, column] of columns) {
    if (column.values.length !== rowCount) {
      throw new Error(`Column row count mismatch: ${name}.${columnName}`);
    }
    vectors.set(columnName, createVector(column));
  }
  if (uniqueKey !== undefined && !vectors.has(uniqueKey)) {
    throw new Error(`Unique-key vector is missing: ${name}.${uniqueKey}`);
  }
  return {
    name,
    rowCount,
    columns: vectors,
    ...(uniqueKey === undefined ? {} : { uniqueKey }),
  };
}

export function columnarTableFromRows(
  name: string,
  rows: readonly DatabaseRow[],
  projectedColumnNames?: readonly string[],
  protectText = true,
): ColumnarTable {
  const columnNameSet = new Set(projectedColumnNames);
  if (projectedColumnNames === undefined) {
    for (const row of rows) {
      for (const columnName of Object.keys(row)) columnNameSet.add(columnName);
    }
  }
  const columnNames = [...columnNameSet];
  if (columnNames.length === 0) return { name, rowCount: rows.length, columns: new Map() };
  const columns = new Map<string, ColumnarColumnInput>();
  for (const columnName of columnNames) {
    const values = rows.map((row) => {
      const value = row[columnName] ?? null;
      return protectText && typeof value === "string" ? protectedSqlTextValue(value) : value;
    });
    columns.set(columnName, { type: inferVectorType(values), values });
  }
  return createColumnarTable(name, columns);
}

export function prepareVectorQuery(
  plan: CompiledQuery,
  inputTables: ReadonlyMap<string, ColumnarTable>,
  options: PrepareVectorQueryOptions = {},
): PreparedVectorQuery {
  const rootMemory = options.memoryContext ?? new QueryMemoryContext();
  const ownsRootMemory = options.memoryContext === undefined;
  const retainedMemory = rootMemory.createChild();
  try {
    for (const table of new Set(inputTables.values())) {
      retainedMemory.reserve(columnarTablePayloadBytes(table), `Columnar table ${table.name}`);
    }
    const bound = bindPlan(plan, inputTables, retainedMemory, options.ftsStats);
    let closed = false;
    return {
      get memoryUsage() {
        return rootMemory.usage;
      },
      execute() {
        if (closed) throw new Error("Prepared vector query is closed");
        const executionMemory = retainedMemory.createChild();
        try {
          return executeBoundPlan(bound, executionMemory);
        } finally {
          executionMemory.close();
        }
      },
      async executeAsync(executionOptions = {}) {
        if (closed) throw new Error("Prepared vector query is closed");
        const canSpillSort = bound.orderBy.length > 0 && !bound.grouped && !bound.sourceOrdered;
        // An unordered grouped plan spills too: the empty ordering makes the pairwise merge a
        // stable concatenation, and partition-wise accumulation bounds peak group state.
        const canSpillHash = bound.grouped && bound.groupBy.length > 0;
        if (executionOptions.spillStore === undefined || (!canSpillSort && !canSpillHash)) {
          if (executionOptions.loadScanWindow === undefined) return this.execute();
          const executionMemory = retainedMemory.createChild();
          try {
            return await executeBoundPlanAsync(bound, executionMemory, executionOptions);
          } finally {
            executionMemory.close();
          }
        }
        const executionMemory = retainedMemory.createChild();
        try {
          return canSpillHash
            ? await executeBoundPlanWithHashSpill(bound, executionMemory, executionOptions)
            : await executeBoundPlanWithSortSpill(bound, executionMemory, executionOptions);
        } finally {
          executionMemory.close();
        }
      },
      async executeBatches(executionOptions, consume) {
        if (closed) throw new Error("Prepared vector query is closed");
        if (bound.grouped || bound.orderBy.length > 0 || bound.joins.length > 0) {
          throw new TypeError(
            "Native query batching requires an ungrouped, unordered, single-source plan",
          );
        }
        if (!Number.isSafeInteger(executionOptions.batchRows) || executionOptions.batchRows <= 0) {
          throw new RangeError("Query batch rows must be a positive whole number");
        }
        const executionMemory = retainedMemory.createChild();
        try {
          return await executeBoundPlanBatches(bound, executionMemory, executionOptions, consume);
        } finally {
          executionMemory.close();
        }
      },
      close() {
        if (closed) return;
        closed = true;
        retainedMemory.close();
        if (ownsRootMemory) rootMemory.close();
      },
    };
  } catch (error) {
    retainedMemory.close();
    if (ownsRootMemory) rootMemory.close();
    throw error;
  }
}

function createVector(input: ColumnarColumnInput): ColumnVector {
  validateVectorType(input.type);
  const length = input.values.length;
  const validity = new Uint8Array(Math.ceil(length / 8));
  if (input.type === "string") {
    const dictionary: string[] = [];
    const dictionaryIndex = new Map<string, number>();
    const codes = new Uint32Array(length);
    codes.fill(NULL_STRING_CODE);
    for (let index = 0; index < length; index += 1) {
      const value = input.values[index];
      if (value === null) continue;
      if (typeof value !== "string") throw vectorTypeError(input.type, value);
      setValid(validity, index);
      let code = dictionaryIndex.get(value);
      if (code === undefined) {
        code = dictionary.length;
        dictionary.push(value);
        dictionaryIndex.set(value, code);
      }
      codes[index] = code;
    }
    return { kind: "string", length, validity, codes, dictionary };
  }
  if (input.type === "boolean") {
    const values = new Uint8Array(length);
    for (let index = 0; index < length; index += 1) {
      const value = input.values[index];
      if (value === null) continue;
      if (typeof value !== "boolean") throw vectorTypeError(input.type, value);
      setValid(validity, index);
      values[index] = value ? 1 : 0;
    }
    return { kind: "boolean", length, validity, values };
  }
  const values = new Float64Array(length);
  for (let index = 0; index < length; index += 1) {
    const value = input.values[index];
    if (value === null) continue;
    const numericValue =
      input.type === "datetime"
        ? value instanceof Date
          ? dateMilliseconds(value)
          : Number.NaN
        : typeof value === "number"
          ? value
          : Number.NaN;
    if (!Number.isFinite(numericValue)) throw vectorTypeError(input.type, value);
    setValid(validity, index);
    values[index] = numericValue;
  }
  return input.type === "datetime"
    ? { kind: "datetime", length, validity, values }
    : { kind: "number", length, validity, values };
}

function validateVectorType(type: unknown): void {
  if (type !== "boolean" && type !== "number" && type !== "string" && type !== "datetime") {
    throw new TypeError(`Unsupported vector type: ${String(type)}`);
  }
}

function vectorTypeError(type: VectorType, value: unknown): TypeError {
  return new TypeError(`Invalid ${type} vector value: ${String(value)}`);
}

function inferVectorType(values: readonly QueryValue[]): VectorType {
  const value = values.find((candidate) => candidate !== null);
  if (typeof value === "boolean") return "boolean";
  if (typeof value === "number") return "number";
  if (value instanceof Date) return "datetime";
  return "string";
}

function setValid(bitmap: Uint8Array, index: number): void {
  const byte = index >>> 3;
  bitmap[byte] = (bitmap[byte] ?? 0) | (1 << (index & 7));
}

function isValid(bitmap: Uint8Array, index: number): boolean {
  return ((bitmap[index >>> 3] ?? 0) & (1 << (index & 7))) !== 0;
}

export function vectorValue(vector: ColumnVector, rowIndex: number): QueryValue {
  if (rowIndex < 0 || rowIndex >= vector.length) return null;
  const window = vector.window;
  let slot = rowIndex;
  if (window !== undefined) {
    slot = rowIndex - window.start;
    if (slot < 0 || slot >= window.length) {
      throw new RangeError(
        `Streamed vector row ${String(rowIndex)} is outside the resident window ${String(window.start)}..${String(window.start + window.length)}`,
      );
    }
  }
  if (!isValid(vector.validity, slot)) return null;
  if (vector.kind === "boolean") return vector.values[slot] === 1;
  if (vector.kind === "number") return vector.values[slot] ?? 0;
  if (vector.kind === "datetime") return new Date(vector.values[slot] ?? 0);
  const code = vector.codes[slot] ?? NULL_STRING_CODE;
  return code === NULL_STRING_CODE ? null : (vector.dictionary[code] ?? null);
}

function columnarTablePayloadBytes(table: ColumnarTable): number {
  let total = table.scanOrder?.byteLength ?? 0;
  for (const vector of table.columns.values()) {
    total = safeMemorySum(total, vector.validity.byteLength, "Column vector payload");
    if (vector.kind === "string") {
      total = safeMemorySum(total, vector.codes.byteLength, "Column vector payload");
      // One byte per UTF-16 code unit, matching queryValuePayloadBytes: exact for Latin-1
      // and O(1) per entry instead of a throwaway UTF-8 encode of the whole dictionary.
      for (const value of vector.dictionary) {
        total = safeMemorySum(total, value.length, "String dictionary payload");
      }
    } else {
      total = safeMemorySum(total, vector.values.byteLength, "Column vector payload");
    }
  }
  return total;
}

/** One conjunction of a disjunction: plain conditions only, or undefined if any leaf is not. */
function conjunctionLeaves(
  expression: Expression,
  output: Array<{
    left: Expression;
    operator: PredicateOperator;
    right: Expression;
    escape?: string;
  }>,
): boolean {
  if (expression.kind === "logical" && expression.operator === "and") {
    return (
      conjunctionLeaves(expression.left, output) && conjunctionLeaves(expression.right, output)
    );
  }
  if (expression.kind !== "condition") return false;
  // A quantified comparison or a subquery leaf carries evaluation rules of its own; leaving it
  // to the generic evaluator is always correct, just slower.
  if (expression.left.kind === "subquery" || expression.right.kind === "subquery") return false;
  output.push({
    left: expression.left,
    operator: expression.operator,
    right: expression.right,
    ...(expression.escape === undefined ? {} : { escape: expression.escape }),
  });
  return true;
}

/**
 * Reads a predicate as an OR of AND-groups. Only a bare boolean expression -- one the parser
 * wrapped in IS TRUE -- can be a disjunction, and every leaf must be a plain condition, so the
 * branches evaluate under the same three-valued rules the whole expression did: a branch is
 * taken only where it is true, and false and unknown are both simply not taken.
 */
function disjunctiveNormalForm(predicate: {
  left: Expression;
  operator: PredicateOperator;
  right: Expression;
}):
  | Array<
      Array<{ left: Expression; operator: PredicateOperator; right: Expression; escape?: string }>
    >
  | undefined {
  if (predicate.operator !== "IS TRUE") return undefined;
  if (predicate.left.kind !== "logical" || predicate.left.operator !== "or") return undefined;
  const branches: Array<
    Array<{ left: Expression; operator: PredicateOperator; right: Expression; escape?: string }>
  > = [];
  const visit = (node: Expression): boolean => {
    if (node.kind === "logical" && node.operator === "or") {
      return visit(node.left) && visit(node.right);
    }
    const group: Array<{
      left: Expression;
      operator: PredicateOperator;
      right: Expression;
      escape?: string;
    }> = [];
    if (!conjunctionLeaves(node, group)) return false;
    branches.push(group);
    return true;
  };
  return visit(predicate.left) && branches.length > 1 ? branches : undefined;
}

/**
 * Gives a pure comma/CROSS-join group a connected physical order once schemas can resolve bare
 * column names. SQLLogicTest deliberately permutes FROM lists; preserving that written order can
 * otherwise materialize several disconnected Cartesian products before their WHERE equalities
 * become applicable. The streamed base stays fixed; explicit/outer joins and wildcard projection
 * keep their complete written order.
 *
 * One equality that connects each new source becomes the inner hash-join key. The remaining
 * conditions stay ordinary predicates, so null and residual-filter semantics do not change.
 */
function orderCartesianJoins(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, ColumnarTable>,
): CompiledQuery {
  if (
    plan.joins.length < 2 ||
    plan.select.some((item) => item.expression.kind === "wildcard") ||
    plan.joins.some((join) => !isCartesianJoin(join))
  ) {
    return plan;
  }
  const entries: Array<{ source: TableSource; originalIndex: number }> = [
    { source: plan.base, originalIndex: 0 },
    ...plan.joins.map((join, index) => ({
      source: tableSourceOf(join),
      originalIndex: index + 1,
    })),
  ];
  const sourceTables = entries.map(({ source }) => tables.get(source.table));
  if (sourceTables.some((table) => table === undefined)) return plan;
  const predicateSources = plan.predicates.map((predicate) => ({
    predicate,
    sources: rawPredicateSources(predicate, entries, sourceTables),
  }));
  // The preparation layer may stream the written base table through a resident window. Keep
  // that source fixed and order only the fully materialized join sides around it.
  const remaining = new Set(entries.slice(1).map(({ originalIndex }) => originalIndex));
  const ordered: number[] = [0];
  while (remaining.size > 0) {
    let best: number | undefined;
    let bestScore = Number.NEGATIVE_INFINITY;
    for (const candidate of remaining) {
      const score = cartesianSourceScore(candidate, ordered, predicateSources);
      if (score > bestScore || (score === bestScore && (best === undefined || candidate < best))) {
        best = candidate;
        bestScore = score;
      }
    }
    if (best === undefined) return plan;
    ordered.push(best);
    remaining.delete(best);
  }
  const predicates = [...plan.predicates];
  const available = new Set<number>([ordered[0] ?? 0]);
  const joins: JoinPlan[] = [];
  for (const sourceIndex of ordered.slice(1)) {
    const source = entries[sourceIndex]?.source;
    if (source === undefined) return plan;
    const keyIndex = predicates.findIndex((predicate) =>
      predicateJoinsSource(predicate, sourceIndex, available, entries, sourceTables),
    );
    const key = keyIndex < 0 ? undefined : predicates.splice(keyIndex, 1)[0];
    joins.push(key === undefined ? cartesianJoin(source) : keyedInnerJoin(source, key));
    available.add(sourceIndex);
  }
  return { ...plan, base: plan.base, joins, predicates };
}

function isCartesianJoin(join: JoinPlan): boolean {
  const condition = join.on;
  return (
    join.kind === "inner" &&
    condition?.kind === "condition" &&
    condition.operator === "=" &&
    condition.left.kind === "literal" &&
    condition.left.value === 1 &&
    condition.right.kind === "literal" &&
    condition.right.value === 1
  );
}

function tableSourceOf(join: JoinPlan): TableSource {
  const { kind, left, right, on, full, natural, ...source } = join;
  void kind;
  void left;
  void right;
  void on;
  void full;
  void natural;
  return source;
}

function cartesianJoin(source: TableSource): JoinPlan {
  return {
    ...source,
    kind: "inner",
    left: { kind: "literal", value: null },
    right: { kind: "literal", value: null },
    on: {
      kind: "condition",
      operator: "=",
      left: { kind: "literal", value: 1 },
      right: { kind: "literal", value: 1 },
    },
  };
}

function keyedInnerJoin(source: TableSource, predicate: Predicate): JoinPlan {
  return {
    ...source,
    kind: "inner",
    left: predicate.left,
    right: predicate.right,
  };
}

function cartesianSourceScore(
  candidate: number,
  ordered: readonly number[],
  predicates: ReadonlyArray<{ predicate: Predicate; sources: Set<number> | undefined }>,
): number {
  const available = new Set(ordered);
  let local = 0;
  let connected = 0;
  let equality = 0;
  for (const entry of predicates) {
    const sources = entry.sources;
    if (sources?.has(candidate) !== true) continue;
    if (sources.size === 1) {
      local++;
      continue;
    }
    if ([...sources].every((source) => source === candidate || available.has(source))) {
      connected++;
      if (entry.predicate.operator === "=") equality++;
    }
  }
  // A usable equality dominates a general connection, which dominates a local filter; original
  // order breaks ties. The empty-order branch keeps the helper correct if base selection is ever
  // made schema-aware too, though the streaming executor currently fixes that source.
  return ordered.length === 0 ? local : equality * 100 + connected * 10 + local;
}

function rawPredicateSources(
  predicate: Predicate,
  entries: ReadonlyArray<{ source: TableSource }>,
  tables: ReadonlyArray<ColumnarTable | undefined>,
): Set<number> | undefined {
  const sources = new Set<number>();
  return rawExpressionSources(predicate.left, entries, tables, sources) &&
    rawExpressionSources(predicate.right, entries, tables, sources)
    ? sources
    : undefined;
}

function rawExpressionSources(
  expression: Expression,
  entries: ReadonlyArray<{ source: TableSource }>,
  tables: ReadonlyArray<ColumnarTable | undefined>,
  into: Set<number>,
): boolean {
  if (expression.kind === "subquery" || expression.kind === "exists") return false;
  if (expression.kind === "column") {
    const parts = expression.reference.split(".");
    let matches: number[];
    if (parts.length === 2) {
      const match = entries.findIndex(({ source }) => source.alias === parts[0]);
      matches = match < 0 || tables[match]?.columns.has(parts[1] ?? "") !== true ? [] : [match];
    } else {
      const column = parts[0] ?? "";
      matches = tables.flatMap((table, index) =>
        table?.columns.has(column) === true ? [index] : [],
      );
    }
    if (matches.length !== 1) return false;
    into.add(matches[0] ?? -1);
    return true;
  }
  return childExpressions(expression).every((child) =>
    rawExpressionSources(child, entries, tables, into),
  );
}

function predicateJoinsSource(
  predicate: Predicate,
  candidate: number,
  available: ReadonlySet<number>,
  entries: ReadonlyArray<{ source: TableSource }>,
  tables: ReadonlyArray<ColumnarTable | undefined>,
): boolean {
  if (predicate.operator !== "=") return false;
  const left = new Set<number>();
  const right = new Set<number>();
  if (
    !rawExpressionSources(predicate.left, entries, tables, left) ||
    !rawExpressionSources(predicate.right, entries, tables, right)
  ) {
    return false;
  }
  const candidateOnly = (sources: ReadonlySet<number>): boolean =>
    sources.size === 1 && sources.has(candidate);
  const availableOnly = (sources: ReadonlySet<number>): boolean =>
    sources.size > 0 && [...sources].every((source) => available.has(source));
  return (
    (candidateOnly(left) && availableOnly(right)) || (candidateOnly(right) && availableOnly(left))
  );
}

function bindPlan(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, ColumnarTable>,
  memory: QueryMemoryContext,
  ftsStats?: ReadonlyMap<string, FtsStats>,
): BoundPlan {
  plan = orderCartesianJoins(plan, tables);
  const sources = [plan.base, ...plan.joins];
  const sourceTables = sources.map((source) => {
    const table = tables.get(source.table);
    if (table === undefined) throw new TypeError(`Unknown table: ${source.table}`);
    return table;
  });
  const sourceAliases = sources.map((source) => source.alias);
  if (new Set(sourceAliases).size !== sourceAliases.length)
    throw new TypeError("Table aliases must be unique");
  const aggregateSpecs: AggregateSpec[] = [];
  const aggregateIndexes = new Map<string, number>();
  const ftsBySignature = new Map<string, BoundFtsExpression>();
  const bind = (expression: Expression): BoundExpression =>
    bindExpression(
      expression,
      sources.map((source, index) => ({ alias: source.alias, table: sourceTables[index] })),
      aggregateSpecs,
      aggregateIndexes,
      memory,
      ftsBySignature,
      ftsStats,
    );
  const select = plan.select.map((item) => ({
    expression: bind(item.expression),
    alias: item.alias,
  }));
  const groupBy = plan.groupBy.map(bind);
  const groupIndexBySignature = new Map(
    groupBy.map((expression, index) => [expression.signature, index]),
  );
  const bindPredicate = (predicate: {
    left: Expression;
    operator: PredicateOperator;
    right: Expression;
    escape?: string;
  }): BoundPredicate => {
    const bound: BoundPredicate = {
      left: bind(predicate.left),
      operator: predicate.operator,
      right: bind(predicate.right),
      ...(predicate.escape === undefined ? {} : { escape: predicate.escape }),
    };
    const dictionaryEquality = detectDictionaryEquality(bound);
    if (dictionaryEquality !== undefined) return { ...bound, dictionaryEquality };
    const dictionaryLike = detectDictionaryLike(bound);
    if (dictionaryLike !== undefined) return { ...bound, dictionaryLike };
    const primitive = detectPrimitiveComparison(bound);
    if (primitive !== undefined) return { ...bound, primitive };
    const primitiveIn = detectPrimitiveInList(bound);
    return primitiveIn === undefined ? bound : { ...bound, primitiveIn };
  };
  const predicates = plan.predicates.map((predicate) => {
    const bound = bindPredicate(predicate);
    if (bound.primitive !== undefined || bound.primitiveIn !== undefined) return bound;
    if (bound.dictionaryEquality !== undefined || bound.dictionaryLike !== undefined) return bound;
    const branches = disjunctiveNormalForm(predicate);
    if (branches === undefined) return bound;
    return {
      ...bound,
      disjunction: { branches: branches.map((group) => group.map(bindPredicate)) },
    };
  });
  const having = plan.having.map((predicate) => ({
    left: bind(predicate.left),
    operator: predicate.operator,
    right: bind(predicate.right),
  }));
  const standardJoins = plan.joins.map((join, joinIndex) => {
    const source = joinIndex + 1;
    if (join.on !== undefined) {
      const condition = bind(join.on);
      if ([...expressionSources(condition)].some((used) => used > source)) {
        throw new TypeError(`JOIN condition for ${join.alias} references a later table`);
      }
      const table = required(sourceTables[source], `JOIN table is missing: ${join.table}`);
      const placeholder: BoundExpression = { kind: "literal", value: null, signature: "" };
      return {
        kind: join.kind,
        buildSource: source,
        probe: placeholder,
        build: placeholder,
        lookup: { unique: false, firstRow: () => -1, nextRow: () => -1 },
        loop: { condition, rowCount: table.rowCount },
      };
    }
    const left = bind(join.left);
    const right = bind(join.right);
    const rightUsesBuild = expressionSources(right).has(source);
    const leftUsesBuild = expressionSources(left).has(source);
    if (leftUsesBuild === rightUsesBuild) {
      throw new TypeError(`JOIN condition must reference the new alias ${join.alias} on one side`);
    }
    const build = rightUsesBuild ? right : left;
    const probe = rightUsesBuild ? left : right;
    const buildSources = expressionSources(build);
    if (buildSources.size !== 1 || !buildSources.has(source)) {
      throw new TypeError(`JOIN build expression for ${join.alias} must reference only that table`);
    }
    if ([...expressionSources(probe)].some((probeSource) => probeSource >= source)) {
      throw new TypeError(`JOIN probe expression for ${join.alias} references a later table`);
    }
    return createBoundJoin(
      join.kind,
      source,
      probe,
      build,
      required(sourceTables[source], `JOIN table is missing: ${join.table}`),
      memory,
    );
  });

  // A wildcard select projects the materialized columns of each source, so ORDER BY resolves
  // against those same names.
  const orderSources = sources.map((source, index) => ({
    alias: source.alias,
    columns: [...(sourceTables[index]?.columns.keys() ?? [])],
  }));
  const orderBy = plan.orderBy.map(({ expression, direction, nulls }) => ({
    outputName: orderOutputName(expression, plan.select, orderSources),
    direction,
    ...(nulls === undefined ? {} : { nulls }),
  }));
  const grouped = groupBy.length > 0 || aggregateSpecs.length > 0;
  const sourceOrdered =
    sourceTables[0]?.orderedForPlan === true && standardJoins.length === 0 && !grouped;
  // A single bare string-column GROUP BY can group on dictionary codes: identical values share a
  // code within one vector, so a code-indexed slot table replaces per-row hashing. Streamed
  // vectors swap dictionaries per window; the accumulator remaps its slot table by value on
  // each swap, so windowed vectors qualify too.
  const groupColumn = groupBy.length === 1 ? groupBy[0] : undefined;
  const codeGrouping =
    grouped && groupColumn?.kind === "column" && groupColumn.vector.kind === "string"
      ? { source: groupColumn.source, vector: groupColumn.vector }
      : undefined;
  return {
    sourceTables,
    sourceAliases,
    scanSource: 0,
    joins: standardJoins,
    predicates,
    having,
    groupBy,
    groupIndexBySignature,
    aggregates: aggregateSpecs,
    hasListAggregate: aggregateSpecs.some(
      (aggregate) => aggregate.name === "JSON_ARRAYAGG" || aggregate.name === "STRING_AGG",
    ),
    select,
    orderBy,
    grouped,
    sourceOrdered,
    ...(codeGrouping === undefined ? {} : { codeGrouping }),
    wildcard: plan.select[0]?.expression.kind === "wildcard",
    ...(plan.limit === undefined ? {} : { limit: plan.limit }),
    ...(plan.offset === undefined ? {} : { offset: plan.offset }),
  };
}

function bindExpression(
  expression: Expression,
  sources: ReadonlyArray<{ alias: string; table: ColumnarTable | undefined }>,
  aggregateSpecs: AggregateSpec[],
  aggregateIndexes: Map<string, number>,
  memory?: QueryMemoryContext,
  ftsBySignature?: Map<string, BoundFtsExpression>,
  ftsStats?: ReadonlyMap<string, FtsStats>,
): BoundExpression {
  const signature = JSON.stringify(expression);
  if (expression.kind === "subquery") {
    throw new TypeError("Subqueries are only supported in WHERE, HAVING, SELECT, and IN");
  }
  if (expression.kind === "parameter") {
    throw new TypeError(
      `Placeholder $${String(expression.index + 1)} is unbound; pass parameters when executing`,
    );
  }
  if (expression.kind === "fts") {
    // The canonical search shape evaluates the same node in WHERE, SELECT, and ORDER BY;
    // sharing one bound node by signature means one set of dictionary tables, one corpus
    // pass, and one budget reservation instead of three.
    const shared = ftsBySignature?.get(signature);
    if (shared !== undefined) return shared;
    if (expression.op === "bm25" && sources.length > 1) {
      throw new TypeError("BM25 requires a single-table query");
    }
    // Every engine entry expands "*" against its catalog before plans reach the executors.
    if (expression.columns === "*") {
      throw new TypeError("Full-text search columns must be expanded before binding");
    }
    const columns = expression.columns.map((columnExpression) => {
      if (columnExpression.kind !== "column") {
        throw new TypeError("Full-text search takes column references");
      }
      const bound = bindExpression(
        columnExpression,
        sources,
        aggregateSpecs,
        aggregateIndexes,
        memory,
        ftsBySignature,
        ftsStats,
      );
      if (bound.kind !== "column") {
        throw new TypeError("Full-text search takes column references");
      }
      if (bound.vector.kind === "boolean") {
        throw new TypeError(
          `Full-text search cannot search a boolean column: ${columnExpression.reference}`,
        );
      }
      return bound;
    });
    const servedStats = ftsStats?.get(signature);
    // `.search()` evaluates a MATCH and a BM25 node over the same columns and query; the
    // per-dictionary term tables depend only on columns + query, so the twin ops share one
    // cache array (scoring fields upgrade lazily on the first scored use).
    const sibling = ftsBySignature?.get(
      JSON.stringify({ ...expression, op: expression.op === "match" ? "bm25" : "match" }),
    );
    const bound: BoundFtsExpression = {
      kind: "fts",
      op: expression.op,
      columns,
      terms: cachedQueryTerms(expression.query),
      caches: sibling?.caches ?? columns.map(() => null),
      ...(servedStats === undefined ? {} : { stats: servedStats }),
      ...(memory === undefined ? {} : { memory }),
      signature,
    };
    ftsBySignature?.set(signature, bound);
    return bound;
  }
  if (expression.kind === "window") {
    throw new TypeError("Window functions are only allowed in the select list");
  }
  if (expression.kind === "list") {
    return {
      kind: "list",
      items: expression.items.map((item) =>
        bindExpression(
          item,
          sources,
          aggregateSpecs,
          aggregateIndexes,
          memory,
          ftsBySignature,
          ftsStats,
        ),
      ),
      signature,
    };
  }
  if (expression.kind === "literal" || expression.kind === "wildcard") {
    return expression.kind === "literal" &&
      typeof expression.value === "string" &&
      expression.internalSqlValue !== true
      ? { ...expression, value: protectedSqlTextValue(expression.value), signature }
      : { ...expression, signature };
  }
  if (expression.kind === "column") {
    const parts = expression.reference.split(".");
    let matches: Array<{ source: number; column: string }>;
    if (parts.length === 2) {
      const source = sources.findIndex(({ alias }) => alias === parts[0]);
      if (source < 0) throw new TypeError(`Unknown table alias: ${parts[0] ?? ""}`);
      matches = [{ source, column: parts[1] ?? "" }];
    } else {
      const column = parts[0] ?? "";
      matches = sources.flatMap(({ table }, source) =>
        table?.columns.has(column) === true ? [{ source, column }] : [],
      );
    }
    const match = matches.length === 1 ? matches[0] : undefined;
    if (match === undefined || sources[match.source]?.table?.columns.has(match.column) !== true) {
      throw new TypeError(`Ambiguous or missing column: ${expression.reference}`);
    }
    return {
      kind: "column",
      source: match.source,
      column: match.column,
      vector: required(
        sources[match.source]?.table?.columns.get(match.column),
        `Bound vector is missing: ${expression.reference}`,
      ),
      signature,
    };
  }
  if (expression.kind === "exists") {
    throw new TypeError("EXISTS subqueries must be resolved before execution");
  }
  if (expression.kind === "binary" || expression.kind === "condition") {
    return {
      kind: expression.kind,
      operator: expression.operator,
      left: bindExpression(
        expression.left,
        sources,
        aggregateSpecs,
        aggregateIndexes,
        memory,
        ftsBySignature,
        ftsStats,
      ),
      right: bindExpression(
        expression.right,
        sources,
        aggregateSpecs,
        aggregateIndexes,
        memory,
        ftsBySignature,
        ftsStats,
      ),
      ...(expression.kind === "condition" && expression.escape !== undefined
        ? { escape: expression.escape }
        : {}),
      signature,
    } as BoundExpression;
  }
  if (expression.kind === "logical") {
    return {
      kind: "logical",
      operator: expression.operator,
      left: bindExpression(
        expression.left,
        sources,
        aggregateSpecs,
        aggregateIndexes,
        memory,
        ftsBySignature,
        ftsStats,
      ),
      right: bindExpression(
        expression.right,
        sources,
        aggregateSpecs,
        aggregateIndexes,
        memory,
        ftsBySignature,
        ftsStats,
      ),
      signature,
    };
  }
  if (expression.kind === "not") {
    return {
      kind: "not",
      operand: bindExpression(
        expression.operand,
        sources,
        aggregateSpecs,
        aggregateIndexes,
        memory,
        ftsBySignature,
        ftsStats,
      ),
      signature,
    };
  }
  if (expression.kind === "case") {
    const otherwise =
      expression.otherwise === undefined
        ? undefined
        : bindExpression(
            expression.otherwise,
            sources,
            aggregateSpecs,
            aggregateIndexes,
            memory,
            ftsBySignature,
            ftsStats,
          );
    return {
      kind: "case",
      branches: expression.branches.map((branch) => ({
        when: bindExpression(
          branch.when,
          sources,
          aggregateSpecs,
          aggregateIndexes,
          memory,
          ftsBySignature,
          ftsStats,
        ),
        then: bindExpression(
          branch.then,
          sources,
          aggregateSpecs,
          aggregateIndexes,
          memory,
          ftsBySignature,
          ftsStats,
        ),
      })),
      ...(otherwise === undefined ? {} : { otherwise }),
      signature,
    };
  }
  const arguments_ = expression.arguments.map((argument) =>
    bindExpression(
      argument,
      sources,
      aggregateSpecs,
      aggregateIndexes,
      memory,
      ftsBySignature,
      ftsStats,
    ),
  );
  if (isScalarFunctionName(expression.name)) {
    return { kind: "call", name: expression.name, arguments: arguments_, signature };
  }
  let aggregateIndex = aggregateIndexes.get(signature);
  if (aggregateIndex === undefined) {
    aggregateIndex = aggregateSpecs.length;
    aggregateIndexes.set(signature, aggregateIndex);
    const argument = required(arguments_[0], `${expression.name} argument is missing`);
    const rawDatetime =
      (expression.name === "MIN" || expression.name === "MAX" || expression.name === "COUNT") &&
      argument.kind === "column" &&
      argument.vector.kind === "datetime"
        ? { source: argument.source, vector: argument.vector }
        : undefined;
    const rawNumber =
      expression.name !== "JSON_ARRAYAGG" &&
      expression.name !== "STRING_AGG" &&
      argument.kind === "column" &&
      argument.vector.kind === "number"
        ? { source: argument.source, vector: argument.vector }
        : undefined;
    aggregateSpecs.push({
      name: expression.name,
      argument,
      ...(expression.name === "STRING_AGG"
        ? { delimiter: required(arguments_[1], "STRING_AGG delimiter is missing") }
        : {}),
      ...(expression.aggregateOrderBy === undefined
        ? {}
        : {
            orderBy: expression.aggregateOrderBy.map((order) => ({
              ...order,
              expression: bindExpression(
                order.expression,
                sources,
                aggregateSpecs,
                aggregateIndexes,
                memory,
                ftsBySignature,
                ftsStats,
              ),
            })),
          }),
      // The signature this slot was keyed by is the JSON of the compiled call, which carries the
      // flag — so COUNT(x) and COUNT(DISTINCT x) in one select land in separate slots.
      ...(expression.distinct === true ? { distinct: true } : {}),
      ...(rawDatetime === undefined ? {} : { rawDatetime }),
      ...(rawNumber === undefined ? {} : { rawNumber }),
    });
  }
  return {
    kind: "call",
    name: expression.name,
    arguments: arguments_,
    aggregateIndex,
    signature,
  };
}

function boundChildren(expression: BoundExpression): BoundExpression[] {
  if (expression.kind === "binary" || expression.kind === "condition") {
    return [expression.left, expression.right];
  }
  if (expression.kind === "logical") return [expression.left, expression.right];
  if (expression.kind === "not") return [expression.operand];
  if (expression.kind === "call") return [...expression.arguments];
  if (expression.kind === "list") return [...expression.items];
  if (expression.kind === "case") {
    return [
      ...expression.branches.flatMap((branch) => [branch.when, branch.then]),
      ...(expression.otherwise === undefined ? [] : [expression.otherwise]),
    ];
  }
  if (expression.kind === "fts") return [...expression.columns];
  return [];
}

/**
 * Rebuilds one string column's per-dictionary term table when the resident dictionary changes
 * (streamed vectors swap dictionaries per window). Each dictionary entry tokenizes exactly once;
 * rows then combine per-code masks. The table is real retained memory — one Uint32 per
 * dictionary entry — so it reserves against the query budget, releasing the previous window's
 * reservation on swap.
 */
function ensureFtsDictionaryCache(
  expression: BoundFtsExpression,
  columnIndex: number,
  vector: StringVector,
  scoring: boolean,
): FtsDictionaryCache {
  let cache = expression.caches[columnIndex];
  if (cache === null || cache === undefined) {
    cache = { dictionary: undefined, termMask: new Uint32Array(0) };
    expression.caches[columnIndex] = cache;
  }
  // A cache built for matching upgrades in place when scoring first needs the same
  // dictionary's token counts and frequencies (twin MATCH/BM25 nodes share cache arrays).
  // Scoring demand is sticky: once a scored read upgrades, later window rebuilds tokenize
  // once and build both tables instead of a mask-only pass plus an immediate upgrade.
  if (cache.dictionary !== vector.dictionary || (scoring && cache.termTf === undefined)) {
    const withScores = scoring || cache.scoring === true;
    const termCount = expression.terms.length;
    // Match tables cost one Uint32 per entry; scoring adds a token count and per-term
    // frequencies, all part of the modeled query memory.
    const bytesPerEntry = 4 * (withScores ? termCount + 2 : 1);
    cache.reservation?.release();
    delete cache.reservation;
    const reservation = expression.memory?.reserve(
      vector.dictionary.length * bytesPerEntry,
      "Full-text dictionary match table",
    );
    if (reservation !== undefined) cache.reservation = reservation;
    const masks = new Uint32Array(vector.dictionary.length);
    const tokenCount = withScores ? new Uint32Array(vector.dictionary.length) : undefined;
    const termTf = withScores ? new Uint32Array(vector.dictionary.length * termCount) : undefined;
    for (let code = 0; code < vector.dictionary.length; code += 1) {
      const rendered = externalSqlDomainValue(vector.dictionary[code] ?? "");
      const tokens = tokenize(typeof rendered === "string" ? rendered : "");
      masks[code] = termsMask(tokens, expression.terms);
      if (tokenCount !== undefined) tokenCount[code] = tokens.length;
      if (termTf !== undefined) {
        const frequencies = termFrequencies(tokens, expression.terms);
        for (let index = 0; index < termCount; index += 1) {
          termTf[code * termCount + index] = frequencies[index] ?? 0;
        }
      }
    }
    cache.dictionary = vector.dictionary;
    cache.termMask = masks;
    // A mask-only rebuild must not leave a previous dictionary's scoring tables behind: the
    // next scored read checks `termTf === undefined` to decide whether to upgrade.
    if (tokenCount !== undefined) cache.tokenCount = tokenCount;
    else delete cache.tokenCount;
    if (termTf !== undefined) cache.termTf = termTf;
    else delete cache.termTf;
    cache.scoring = withScores;
  }
  return cache;
}

/**
 * Resolves one column's row index for the three evaluation shapes without a per-row closure:
 * batch evaluation carries per-source row arrays, join-loop evaluation one row index per
 * source, and the statistics pass addresses vectors by absolute row.
 */
function ftsRowIndex(
  column: { source: number },
  batch: BatchRows | null,
  rowsBySource: Int32Array | null,
  row: number,
): number {
  if (batch !== null) return batch.rowsBySource[column.source]?.[row] ?? -1;
  if (rowsBySource !== null) return rowsBySource[column.source] ?? -1;
  return row;
}

/**
 * One row's document accumulated over the bound columns — the single owner of the per-column
 * rule (dictionary tables for strings, render-and-tokenize for numbers and datetimes) shared
 * by matching, scoring, and the statistics pass, so the three can never drift. Writes into the
 * node's reusable scratch (zero allocations per row for string columns) and returns it.
 * `wantScores` additionally gathers token length and per-term frequencies, and only bm25
 * nodes carry the dictionary tables for those; match-only accumulation may stop early once
 * every term is covered.
 */
function accumulateFtsRow(
  expression: BoundFtsExpression,
  batch: BatchRows | null,
  rowsBySource: Int32Array | null,
  row: number,
  wantScores: boolean,
): FtsRowScratch {
  const terms = expression.terms;
  const into = (expression.scratchRow ??= {
    present: false,
    mask: 0,
    length: 0,
    frequencies: new Array<number>(terms.length).fill(0),
  });
  const fullMask = fullTermsMask(terms.length);
  into.present = false;
  into.mask = 0;
  into.length = 0;
  if (wantScores) into.frequencies.fill(0);
  for (let index = 0; index < expression.columns.length; index += 1) {
    const column = expression.columns[index];
    if (column === undefined) continue;
    const rowIndex = ftsRowIndex(column, batch, rowsBySource, row);
    if (column.vector.kind === "string") {
      const code = stringCodeAt(column.vector, rowIndex);
      if (code === undefined) continue;
      into.present = true;
      if (terms.length === 0 && !wantScores) continue;
      const cache = ensureFtsDictionaryCache(expression, index, column.vector, wantScores);
      into.mask |= cache.termMask[code] ?? 0;
      if (wantScores) {
        into.length += cache.tokenCount?.[code] ?? 0;
        const termTf = cache.termTf;
        if (termTf !== undefined) {
          for (let term = 0; term < terms.length; term += 1) {
            into.frequencies[term] =
              (into.frequencies[term] ?? 0) + (termTf[code * terms.length + term] ?? 0);
          }
        }
      }
    } else {
      const rendered = renderDocumentValue(vectorValue(column.vector, rowIndex));
      if (rendered === undefined) continue;
      into.present = true;
      if (terms.length === 0 && !wantScores) continue;
      const tokens = tokenize(rendered);
      if (wantScores) {
        into.length += tokens.length;
        const partial = termFrequencies(tokens, terms);
        for (let term = 0; term < terms.length; term += 1) {
          const tf = partial[term] ?? 0;
          into.frequencies[term] = (into.frequencies[term] ?? 0) + tf;
          if (tf > 0) into.mask |= 1 << term;
        }
      } else {
        into.mask |= termsMask(tokens, terms);
      }
    }
    if (!wantScores && terms.length > 0 && (into.mask & fullMask) === fullMask) return into;
  }
  return into;
}

/**
 * One whole-scan pass computing BM25 corpus statistics for a bound scoring node, feeding the
 * shared accumulator so its definition (every row is a document; all-null rows have length 0)
 * stays identical across producers. Requires a fully materialized scan; streamed scoring plans
 * carry index-served statistics instead.
 */
function computeBoundFtsStats(expression: BoundFtsExpression): FtsStats {
  for (const column of expression.columns) {
    if (column.vector.window !== undefined) {
      throw new TypeError("BM25 requires a materialized scan");
    }
  }
  const rowCount = expression.columns[0]?.vector.length ?? 0;
  const accumulator = new FtsStatsAccumulator(expression.terms);
  for (let row = 0; row < rowCount; row += 1) {
    const document = accumulateFtsRow(expression, null, null, row, true);
    accumulator.addDocumentCounts(document.mask, document.length);
  }
  const stats = accumulator.stats;
  expression.stats = stats;
  return stats;
}

/** Document-level BM25 score for one row; null when every column is null. */
function ftsBm25BatchValue(
  expression: BoundFtsExpression,
  batch: BatchRows | null,
  rowsBySource: Int32Array | null,
  row: number,
): number | null {
  const stats = expression.stats ?? computeBoundFtsStats(expression);
  const document = accumulateFtsRow(expression, batch, rowsBySource, row, true);
  if (!document.present) return null;
  return bm25DocumentScore(document.frequencies, document.length, stats);
}

/** Document-level MATCH over the bound columns of one row; null when every column is null. */
function ftsBatchTruth(
  expression: BoundFtsExpression,
  batch: BatchRows | null,
  rowsBySource: Int32Array | null,
  row: number,
): boolean | null {
  const document = accumulateFtsRow(expression, batch, rowsBySource, row, false);
  if (!document.present) return null;
  const fullMask = fullTermsMask(expression.terms.length);
  return expression.terms.length > 0 && (document.mask & fullMask) === fullMask;
}

function expressionSources(expression: BoundExpression): Set<number> {
  if (expression.kind === "column") return new Set([expression.source]);
  return new Set(boundChildren(expression).flatMap((child) => [...expressionSources(child)]));
}

function createBoundJoin(
  kind: BoundJoin["kind"],
  buildSource: number,
  probe: BoundExpression,
  build: BoundExpression,
  table: ColumnarTable,
  memory: QueryMemoryContext,
): BoundJoin {
  return {
    kind,
    buildSource,
    probe,
    build,
    lookup: createJoinLookup(table, build, buildSource, memory),
  };
}

function createJoinLookup(
  table: ColumnarTable,
  expression: BoundExpression,
  source: number,
  memory: QueryMemoryContext,
): JoinLookup {
  if (
    expression.kind === "column" &&
    expression.source === source &&
    expression.column === table.uniqueKey
  ) {
    const vector = table.columns.get(expression.column);
    const direct = vector === undefined ? undefined : createDirectLookup(vector, memory);
    if (direct !== undefined) return direct;
  }
  const index = new ByteJoinIndex(memory, table.rowCount);
  const buildScratch = memory.reserve(
    safeMemoryProduct(source + 1, Int32Array.BYTES_PER_ELEMENT, "Hash join bind scratch"),
    `Hash join ${table.name} bind scratch`,
  );
  try {
    const rowBySource = new Int32Array(source + 1);
    rowBySource.fill(-1);
    for (let rowIndex = 0; rowIndex < table.rowCount; rowIndex += 1) {
      rowBySource[source] = rowIndex;
      index.add(evaluateExpression(expression, rowBySource), rowIndex);
    }
  } finally {
    buildScratch.release();
  }
  return {
    get unique() {
      return index.unique;
    },
    firstRow: (key) => index.firstRow(key),
    nextRow: (row) => index.nextRow(row),
  };
}

function createDirectLookup(
  vector: ColumnVector,
  memory: QueryMemoryContext,
): JoinLookup | undefined {
  if (vector.kind !== "number") return undefined;
  let minimum = Number.POSITIVE_INFINITY;
  let maximum = Number.NEGATIVE_INFINITY;
  for (let index = 0; index < vector.length; index += 1) {
    if (!isValid(vector.validity, index)) continue;
    const value = vector.values[index] ?? 0;
    if (!Number.isSafeInteger(value)) return undefined;
    minimum = Math.min(minimum, value);
    maximum = Math.max(maximum, value);
  }
  if (!Number.isFinite(minimum)) {
    return { unique: true, firstRow: () => -1, nextRow: () => -1 };
  }
  const range = maximum - minimum + 1;
  if (range > Math.max(1_024, vector.length * 4) || range > 10_000_000) return undefined;
  const reservation = memory.reserve(
    safeMemoryProduct(range, Int32Array.BYTES_PER_ELEMENT, "Direct join lookup"),
    "Direct join lookup",
  );
  const rowByKey = new Int32Array(range);
  rowByKey.fill(-1);
  for (let index = 0; index < vector.length; index += 1) {
    if (!isValid(vector.validity, index)) continue;
    const slot = (vector.values[index] ?? 0) - minimum;
    if ((rowByKey[slot] ?? -1) !== -1) {
      reservation.release();
      return undefined;
    }
    rowByKey[slot] = index;
  }
  return {
    unique: true,
    firstRow(key) {
      if (typeof key !== "number" || !Number.isSafeInteger(key)) return -1;
      const slot = key - minimum;
      return slot < 0 || slot >= rowByKey.length ? -1 : (rowByKey[slot] ?? -1);
    },
    nextRow: () => -1,
  };
}

function runScanBatch(
  plan: BoundPlan,
  start: number,
  length: number,
  groups: GroupAccumulator,
  output: ResultSink,
  memory: QueryMemoryContext,
): boolean {
  const batchMemory = memory.createChild();
  try {
    batchMemory.reserve(
      safeMemoryProduct(
        safeMemoryProduct(plan.sourceTables.length, length, "Scan batch row-index count"),
        Int32Array.BYTES_PER_ELEMENT,
        "Scan batch row indexes",
      ),
      "Scan batch row indexes",
    );
    const sourceRows = plan.sourceTables.map(() => new Int32Array(length).fill(-1));
    const scan = sourceRows[plan.scanSource];
    if (scan === undefined) return false;
    const order = plan.sourceTables[plan.scanSource]?.scanOrder;
    for (let index = 0; index < length; index += 1) {
      scan[index] = order?.[start + index] ?? start + index;
    }
    const batch: BatchRows = { length, rowsBySource: sourceRows, memory: batchMemory };
    return consumeJoinedBatches(plan, batch, 0, groups, output, memory);
  } finally {
    batchMemory.close();
  }
}

function executeBoundPlan(plan: BoundPlan, memory: QueryMemoryContext): QueryResult {
  const metadataCount = executeMetadataCount(plan, memory);
  if (metadataCount !== undefined) return metadataCount;
  const groups = new GroupAccumulator(plan, memory);
  const output = new ResultSink(plan, memory, true);
  const scanRows = plan.sourceTables[plan.scanSource]?.rowCount ?? 0;
  const narrowed = plan.sourceOrdered ? undefined : ascendingScanRange(plan, 0, scanRows);
  const ranges = narrowed?.ranges ?? [
    { begin: narrowed?.begin ?? 0, end: narrowed?.end ?? scanRows },
  ];
  scan: for (const range of ranges) {
    for (let start = range.begin; start < range.end; start += DEFAULT_BATCH_ROWS) {
      const length = Math.min(DEFAULT_BATCH_ROWS, range.end - start);
      if (runScanBatch(plan, start, length, groups, output, memory)) break scan;
    }
  }
  const rows = plan.grouped ? finishGroups(plan, groups.values(), memory) : output.finish();
  return finishResult(plan, rows, memory);
}

async function executeBoundPlanAsync(
  plan: BoundPlan,
  memory: QueryMemoryContext,
  options: AsyncQueryExecutionOptions,
): Promise<QueryResult> {
  const metadataCount = executeMetadataCount(plan, memory);
  if (metadataCount !== undefined) return metadataCount;
  const groups = new GroupAccumulator(plan, memory);
  const output = new ResultSink(plan, memory, options.loadScanWindow === undefined);
  const scanRows = plan.sourceTables[plan.scanSource]?.rowCount ?? 0;
  for (let start = 0; start < scanRows;) {
    let length = Math.min(DEFAULT_BATCH_ROWS, scanRows - start);
    // The loader answers synchronously when the batch is already resident — the common case,
    // every batch but the first per block — so the scan loop only pays await on real slides.
    const loaded = options.loadScanWindow?.(start, length);
    const residentEnd = typeof loaded === "number" || loaded === undefined ? loaded : await loaded;
    if (typeof residentEnd === "number" && residentEnd > start) {
      length = Math.min(length, residentEnd - start);
    }
    // Narrowing runs against the resident window, so a streamed scan skips the batches this
    // window cannot answer instead of stepping through them. `windowEnd` is where the loader
    // stopped, which is the block boundary — exactly the span the ordering check covers.
    const windowEnd =
      typeof residentEnd === "number" && residentEnd > start ? residentEnd : start + length;
    const narrowed = plan.sourceOrdered ? undefined : ascendingScanRange(plan, start, windowEnd);
    if (narrowed === undefined) {
      if (runScanBatch(plan, start, length, groups, output, memory)) break;
      start += length;
      continue;
    }
    // The window is consumed to its end before the loader is asked for anything else: loaders
    // are forward-only, so re-entering with a start part-way into a resident window would ask
    // one to serve ground it has already passed.
    let stopped = false;
    const ranges = narrowed.ranges ?? [{ begin: narrowed.begin, end: narrowed.end }];
    for (const range of ranges) {
      for (let row = range.begin; row < range.end; row += DEFAULT_BATCH_ROWS) {
        const rows = Math.min(DEFAULT_BATCH_ROWS, range.end - row);
        if (runScanBatch(plan, row, rows, groups, output, memory)) {
          stopped = true;
          break;
        }
      }
      if (stopped) break;
    }
    if (stopped) break;
    start = windowEnd;
  }
  const rows = plan.grouped ? finishGroups(plan, groups.values(), memory) : output.finish();
  return finishResult(plan, rows, memory);
}

/**
 * Pull-driven scan execution for cursor-safe plans. A result page owns a child memory context
 * that closes after the consumer settles, so both modeled memory and live row objects are
 * bounded by one page. OFFSET/LIMIT are applied once across the whole scan, never per page.
 */
async function executeBoundPlanBatches(
  plan: BoundPlan,
  memory: QueryMemoryContext,
  options: QueryBatchExecutionOptions,
  consume: (batch: QueryResult) => void | Promise<void>,
): Promise<readonly string[]> {
  const columns = plan.wildcard ? wildcardColumnNames(plan) : plan.select.map((item) => item.alias);
  const { limit, offset, ...unbounded } = plan;
  const scanPlan: BoundPlan = unbounded;
  let skipped = 0;
  let emitted = 0;
  const scanRows = scanPlan.sourceTables[scanPlan.scanSource]?.rowCount ?? 0;
  const step = Math.min(DEFAULT_BATCH_ROWS, options.batchRows);
  for (let start = 0; start < scanRows && (limit === undefined || emitted < limit);) {
    options.signal?.throwIfAborted();
    let length = Math.min(step, scanRows - start);
    const loaded = options.loadScanWindow?.(start, length);
    const residentEnd = typeof loaded === "number" || loaded === undefined ? loaded : await loaded;
    options.signal?.throwIfAborted();
    if (typeof residentEnd === "number" && residentEnd > start) {
      length = Math.min(length, residentEnd - start);
    }
    const pageMemory = memory.createChild();
    try {
      const groups = new GroupAccumulator(scanPlan, pageMemory);
      const output = new ResultSink(scanPlan, pageMemory, options.loadScanWindow === undefined);
      runScanBatch(scanPlan, start, length, groups, output, pageMemory);
      let rows = output.finish();
      const remainingOffset = Math.max(0, (offset ?? 0) - skipped);
      if (remainingOffset > 0) {
        const remove = Math.min(remainingOffset, rows.length);
        rows = rows.slice(remove);
        skipped += remove;
      }
      if (limit !== undefined && rows.length > limit - emitted) {
        rows = rows.slice(0, limit - emitted);
      }
      if (rows.length > 0) {
        emitted += rows.length;
        await consume({ columns: [...columns], rows });
      }
    } finally {
      pageMemory.close();
    }
    start += length;
  }
  return columns;
}

interface SpillRun {
  readonly id: string;
  readonly pageCount: number;
}

function createSpillOwnerId(): string {
  return `query-${globalThis.crypto.randomUUID()}`;
}

async function executeBoundPlanWithSortSpill(
  plan: BoundPlan,
  memory: QueryMemoryContext,
  options: AsyncQueryExecutionOptions,
): Promise<QueryResult> {
  const store = required(options.spillStore, "Query spill store is missing");
  const pageRows = boundedSpillPageRows(options.spillPageRows);
  const columns = plan.wildcard ? wildcardColumnNames(plan) : plan.select.map((item) => item.alias);
  const ownerId = createSpillOwnerId();
  const runs: SpillRun[] = [];
  let runSequence = 0;
  try {
    const scanRows = plan.sourceTables[plan.scanSource]?.rowCount ?? 0;
    const scanBatchRows = Math.min(DEFAULT_BATCH_ROWS, pageRows);
    for (let start = 0; start < scanRows;) {
      let length = Math.min(scanBatchRows, scanRows - start);
      const loadedSort = options.loadScanWindow?.(start, length);
      const residentEnd =
        typeof loadedSort === "number" || loadedSort === undefined ? loadedSort : await loadedSort;
      if (typeof residentEnd === "number" && residentEnd > start) {
        length = Math.min(length, residentEnd - start);
      }
      const batchMemory = memory.createChild();
      try {
        batchMemory.reserve(
          safeMemoryProduct(
            safeMemoryProduct(plan.sourceTables.length, length, "Scan batch row-index count"),
            Int32Array.BYTES_PER_ELEMENT,
            "Scan batch row indexes",
          ),
          "Scan batch row indexes",
        );
        const sourceRows = plan.sourceTables.map(() => new Int32Array(length).fill(-1));
        const scan = sourceRows[plan.scanSource];
        if (scan === undefined) {
          start += length;
          continue;
        }
        for (let index = 0; index < length; index += 1) scan[index] = start + index;
        await spillJoinedBatches(
          plan,
          { length, rowsBySource: sourceRows, memory: batchMemory },
          0,
          memory,
          async (batch) => {
            const outputMemory = memory.createChild();
            try {
              const rows: QueryRow[] = [];
              projectFilteredBatch(plan, batch, rows, outputMemory);
              if (rows.length === 0) return;
              const ordering = outputMemory.reserve(
                safeMemoryProduct(
                  rows.length,
                  Uint32Array.BYTES_PER_ELEMENT * 2 + Uint8Array.BYTES_PER_ELEMENT,
                  "Spill ordering typed scratch",
                ),
                "Spill ordering typed scratch",
              );
              try {
                stableSortRows(rows, plan.orderBy);
              } finally {
                ordering.release();
              }
              const runId = `run-${String(runSequence++)}`;
              const pageCount = await writeSpillRowPages(
                store,
                ownerId,
                runId,
                0,
                columns,
                rows,
                pageRows,
              );
              runs.push({ id: runId, pageCount });
            } finally {
              outputMemory.close();
            }
          },
        );
      } finally {
        batchMemory.close();
      }
      start += length;
    }

    if (runs.length === 0) return { columns, rows: [] };
    let active = runs;
    while (active.length > 1) {
      const merged: SpillRun[] = [];
      for (let index = 0; index < active.length; index += 2) {
        const left = required(active[index], "Left spill run is missing");
        const right = active[index + 1];
        if (right === undefined) {
          merged.push(left);
          continue;
        }
        const outputId = `merge-${String(runSequence++)}`;
        merged.push(
          await mergeSpillRuns(
            store,
            ownerId,
            left,
            right,
            outputId,
            columns,
            plan.orderBy,
            pageRows,
            memory,
          ),
        );
        await store.removeRun(ownerId, left.id);
        await store.removeRun(ownerId, right.id);
      }
      active = merged;
    }
    const finalRun = required(active[0], "Final spill run is missing");
    const rows: QueryRow[] = [];
    const offset = plan.offset ?? 0;
    const limit = plan.limit === undefined ? Number.MAX_SAFE_INTEGER : plan.limit + offset;
    for (let pageIndex = 0; pageIndex < finalRun.pageCount && rows.length < limit; pageIndex += 1) {
      const bytes = await store.getPage(ownerId, finalRun.id, pageIndex);
      if (bytes === undefined) throw new Error("Query spill page is missing");
      for (const row of decodeSpillRows(columns, bytes)) {
        if (rows.length === limit) break;
        rows.push(row);
      }
    }
    if (offset > 0) rows.splice(0, Math.min(offset, rows.length));
    return { columns, rows };
  } finally {
    await store.removeOwner(ownerId);
  }
}

async function executeBoundPlanWithHashSpill(
  plan: BoundPlan,
  memory: QueryMemoryContext,
  options: AsyncQueryExecutionOptions,
): Promise<QueryResult> {
  const store = required(options.spillStore, "Query spill store is missing");
  const pageRows = boundedSpillPageRows(options.spillPageRows);
  const partitionCount = 64;
  const columns = plan.select.map((item) => item.alias);
  const groupColumnNames = plan.groupBy.map((_, index) => `g${String(index)}`);
  const aggregateColumnNames = plan.aggregates.map((_, index) => `a${String(index)}`);
  const spillColumns = [...groupColumnNames, ...aggregateColumnNames];
  const ownerId = createSpillOwnerId();
  const partitionPages = new Uint32Array(partitionCount);
  let runSequence = 0;
  try {
    const scanRows = plan.sourceTables[plan.scanSource]?.rowCount ?? 0;
    // A fixed chunk bounds buffered evaluated values per flush independently of the configured
    // page size while staying coarse enough to amortize partition-page write transactions.
    const scanChunkRows = Math.min(DEFAULT_BATCH_ROWS, HASH_SPILL_SCAN_CHUNK_ROWS);
    for (let start = 0; start < scanRows;) {
      let length = Math.min(scanChunkRows, scanRows - start);
      const loadedHash = options.loadScanWindow?.(start, length);
      const residentEnd =
        typeof loadedHash === "number" || loadedHash === undefined ? loadedHash : await loadedHash;
      if (typeof residentEnd === "number" && residentEnd > start) {
        length = Math.min(length, residentEnd - start);
      }
      const batchMemory = memory.createChild();
      try {
        batchMemory.reserve(
          safeMemoryProduct(
            safeMemoryProduct(plan.sourceTables.length, length, "Scan batch row-index count"),
            Int32Array.BYTES_PER_ELEMENT,
            "Scan batch row indexes",
          ),
          "Scan batch row indexes",
        );
        const scanRowsBySource = plan.sourceTables.map(() => new Int32Array(length).fill(-1));
        const scan = scanRowsBySource[plan.scanSource];
        if (scan === undefined) {
          start += length;
          continue;
        }
        for (let index = 0; index < length; index += 1) scan[index] = start + index;
        const partitionBuffers = new Map<number, QueryRow[]>();
        await spillJoinedBatches(
          plan,
          { length, rowsBySource: scanRowsBySource, memory: batchMemory },
          0,
          memory,
          // Each surviving row spills its evaluated group keys and aggregate arguments, so the
          // partition phase never re-reads source vectors and the scan source may be windowed.
          async (batch) => {
            for (let row = 0; row < batch.length; row += 1) {
              if (
                !plan.predicates.every((predicate) =>
                  evaluateBatchPredicate(plan, predicate, batch, row),
                )
              ) {
                continue;
              }
              const groupValues = plan.groupBy.map((expression) =>
                asQueryValue(evaluateBatchExpression(plan, expression, batch, row)),
              );
              const spillRow: QueryRow = {};
              for (let index = 0; index < groupValues.length; index += 1) {
                spillRow[`g${String(index)}`] = groupValues[index] ?? null;
              }
              for (let index = 0; index < plan.aggregates.length; index += 1) {
                const spec = required(plan.aggregates[index], "Aggregate specification is missing");
                const raw =
                  spec.argument.kind === "wildcard"
                    ? 1
                    : evaluateBatchExpression(plan, spec.argument, batch, row);
                if (spec.name === "STRING_AGG" && raw !== null && raw !== undefined) {
                  const delimiter = required(spec.delimiter, "STRING_AGG delimiter is missing");
                  spillRow[`a${String(index)}`] = JSON.stringify([
                    raw,
                    evaluateBatchExpression(plan, delimiter, batch, row),
                    (spec.orderBy ?? []).map((order) =>
                      encodeSpillValue(
                        asQueryValue(evaluateBatchExpression(plan, order.expression, batch, row)),
                      ),
                    ),
                  ]);
                } else {
                  spillRow[`a${String(index)}`] =
                    raw === null || raw === undefined ? null : asQueryValue(raw);
                }
              }
              batchMemory.tally(queryRowPayloadBytes(spillRow), "Hash spill value row");
              const partition = hashQueryValues(groupValues) & (partitionCount - 1);
              const rows = partitionBuffers.get(partition) ?? [];
              rows.push(spillRow);
              partitionBuffers.set(partition, rows);
            }
          },
        );
        const flush: Array<{
          ownerId: string;
          runId: string;
          pageIndex: number;
          bytes: Uint8Array;
        }> = [];
        let flushBytes = 0;
        for (const [partition, rows] of partitionBuffers) {
          for (const bytes of encodeSpillRowPages(spillColumns, rows, pageRows)) {
            if (
              flush.length > 0 &&
              (flush.length === SPILL_WRITE_BATCH_PAGES ||
                flushBytes + bytes.byteLength > MAX_TEMP_RUN_BATCH_BYTES)
            ) {
              await writeSpillPages(store, flush);
              flush.length = 0;
              flushBytes = 0;
            }
            const pageIndex = partitionPages[partition] ?? 0;
            flush.push({
              ownerId,
              runId: `partition-${String(partition)}`,
              pageIndex,
              bytes,
            });
            flushBytes += bytes.byteLength;
            partitionPages[partition] = pageIndex + 1;
          }
        }
        await writeSpillPages(store, flush);
      } finally {
        batchMemory.close();
      }
      start += length;
    }

    const runs: SpillRun[] = [];
    for (let partition = 0; partition < partitionCount; partition += 1) {
      const sourcePageCount = partitionPages[partition] ?? 0;
      if (sourcePageCount === 0) continue;
      const partitionMemory = memory.createChild();
      try {
        const groups = new ByteGroupIndex<GroupState>(partitionMemory);
        for (let pageIndex = 0; pageIndex < sourcePageCount; pageIndex += 1) {
          const bytes = await store.getPage(ownerId, `partition-${String(partition)}`, pageIndex);
          if (bytes === undefined) throw new Error("Query hash spill page is missing");
          const pageMemory = partitionMemory.createChild();
          try {
            pageMemory.reserve(spillRowsModeledBytes(bytes), "Hash spill value rows");
            for (const spillRow of decodeSpillRows(spillColumns, bytes)) {
              const groupValues = groupColumnNames.map((name) => spillRow[name] ?? null);
              const state =
                groupValues.length === 1
                  ? groups.getOrInsertOne(groupKey(groupValues[0] ?? null), () =>
                      createGroupState([groupValues[0] ?? null], plan, partitionMemory),
                    )
                  : groups.getOrInsert(groupValues.map(groupKey), () =>
                      createGroupState(groupValues, plan, partitionMemory),
                    );
              updateAggregatesFromValues(
                plan,
                state,
                aggregateColumnNames.map((name) => spillRow[name] ?? null),
                partitionMemory,
              );
            }
          } finally {
            pageMemory.close();
          }
        }
        const rows = finishGroups(plan, groups.values(), partitionMemory);
        if (plan.orderBy.length > 0) {
          const ordering = partitionMemory.reserve(
            safeMemoryProduct(
              rows.length,
              Uint32Array.BYTES_PER_ELEMENT * 2 + Uint8Array.BYTES_PER_ELEMENT,
              "Hash spill ordering typed scratch",
            ),
            "Hash spill ordering typed scratch",
          );
          try {
            stableSortRows(rows, plan.orderBy);
          } finally {
            ordering.release();
          }
        }
        const runId = `group-${String(runSequence++)}`;
        const pageCount = await writeSpillRowPages(
          store,
          ownerId,
          runId,
          0,
          columns,
          rows,
          pageRows,
        );
        runs.push({ id: runId, pageCount });
      } finally {
        partitionMemory.close();
        await store.removeRun(ownerId, `partition-${String(partition)}`);
      }
    }

    if (runs.length === 0) return { columns, rows: [] };
    const finalRun = await mergeAllSpillRuns(
      store,
      ownerId,
      runs,
      columns,
      plan.orderBy,
      pageRows,
      () => `merge-${String(runSequence++)}`,
      memory,
    );
    const spillOffset = plan.offset ?? 0;
    const result = await readFinalSpillRun(
      store,
      ownerId,
      finalRun,
      columns,
      plan.limit === undefined ? undefined : plan.limit + spillOffset,
    );
    if (spillOffset > 0) result.rows.splice(0, Math.min(spillOffset, result.rows.length));
    return result;
  } finally {
    await store.removeOwner(ownerId);
  }
}

async function mergeAllSpillRuns(
  store: QuerySpillStore,
  ownerId: string,
  runs: readonly SpillRun[],
  columns: readonly string[],
  orderBy: BoundPlan["orderBy"],
  pageRows: number,
  nextRunId: () => string,
  memory: QueryMemoryContext,
): Promise<SpillRun> {
  let active = [...runs];
  while (active.length > 1) {
    const merged: SpillRun[] = [];
    for (let index = 0; index < active.length; index += 2) {
      const left = required(active[index], "Left spill run is missing");
      const right = active[index + 1];
      if (right === undefined) {
        merged.push(left);
        continue;
      }
      const outputId = nextRunId();
      merged.push(
        await mergeSpillRuns(
          store,
          ownerId,
          left,
          right,
          outputId,
          columns,
          orderBy,
          pageRows,
          memory,
        ),
      );
      await store.removeRun(ownerId, left.id);
      await store.removeRun(ownerId, right.id);
    }
    active = merged;
  }
  return required(active[0], "Final spill run is missing");
}

async function readFinalSpillRun(
  store: QuerySpillStore,
  ownerId: string,
  run: SpillRun,
  columns: readonly string[],
  requestedLimit?: number,
): Promise<QueryResult> {
  const rows: QueryRow[] = [];
  const limit = requestedLimit ?? Number.MAX_SAFE_INTEGER;
  for (let pageIndex = 0; pageIndex < run.pageCount && rows.length < limit; pageIndex += 1) {
    const bytes = await store.getPage(ownerId, run.id, pageIndex);
    if (bytes === undefined) {
      throw new Error(`Query spill page is missing: ${run.id}/${String(pageIndex)}`);
    }
    for (const row of decodeSpillRows(columns, bytes)) {
      if (rows.length === limit) break;
      rows.push(row);
    }
  }
  return { columns: [...columns], rows };
}

const hashScratch = new DataView(new ArrayBuffer(8));

/**
 * Allocation-free FNV-1a over tagged canonical group-key bytes. Only per-execution partition
 * routing depends on this hash, so it never needs cross-version stability; -0 keeps its sign bit
 * and hashes apart from 0, matching the group index's key distinction.
 */
function hashQueryValues(values: readonly QueryValue[]): number {
  let hash = 0x811c9dc5;
  for (const rawValue of values) {
    const value = groupKey(rawValue);
    if (value === null) {
      hash = Math.imul(hash ^ 0x01, 0x01000193) >>> 0;
    } else if (typeof value === "boolean") {
      hash = Math.imul(hash ^ (value ? 0x03 : 0x02), 0x01000193) >>> 0;
    } else if (typeof value === "number") {
      hash = Math.imul(hash ^ 0x04, 0x01000193) >>> 0;
      hashScratch.setFloat64(0, value, true);
      for (let byte = 0; byte < 8; byte += 1) {
        hash = Math.imul(hash ^ hashScratch.getUint8(byte), 0x01000193) >>> 0;
      }
    } else {
      hash = Math.imul(hash ^ 0x05, 0x01000193) >>> 0;
      for (let index = 0; index < value.length; index += 1) {
        const code = value.charCodeAt(index);
        hash = Math.imul(hash ^ (code & 0xff), 0x01000193) >>> 0;
        hash = Math.imul(hash ^ (code >>> 8), 0x01000193) >>> 0;
      }
    }
    hash = Math.imul(hash ^ 0xff, 0x01000193) >>> 0;
  }
  return hash;
}

async function spillJoinedBatches(
  plan: BoundPlan,
  batch: BatchRows,
  joinIndex: number,
  memory: QueryMemoryContext,
  consume: (batch: BatchRows) => Promise<void>,
): Promise<void> {
  const join = plan.joins[joinIndex];
  if (join === undefined) {
    await consume(batch);
    return;
  }
  for (const joined of joinBatches(plan, batch, join, memory)) {
    try {
      await spillJoinedBatches(plan, joined, joinIndex + 1, memory, consume);
    } finally {
      joined.memory?.close();
    }
  }
}

function projectFilteredBatch(
  plan: BoundPlan,
  batch: BatchRows,
  output: QueryRow[],
  memory: QueryMemoryContext,
): void {
  for (let row = 0; row < batch.length; row += 1) {
    if (!passesPredicates(plan, batch, row)) continue;
    const resultRow = projectBatchRow(plan, batch, row);
    memory.tally(queryRowPayloadBytes(resultRow), "Spill result row");
    output.push(resultRow);
  }
}

/**
 * Applies a plan's WHERE predicates to one row. This runs once per scanned row, so it iterates
 * directly rather than through `every`, which allocates a capturing closure per call.
 */
function passesPredicates(plan: BoundPlan, batch: BatchRows, row: number): boolean {
  for (const predicate of plan.predicates) {
    if (!evaluateBatchPredicate(plan, predicate, batch, row)) return false;
  }
  return true;
}

async function mergeSpillRuns(
  store: QuerySpillStore,
  ownerId: string,
  left: SpillRun,
  right: SpillRun,
  outputId: string,
  columns: readonly string[],
  orderBy: BoundPlan["orderBy"],
  pageRows: number,
  memory: QueryMemoryContext,
): Promise<SpillRun> {
  const mergeMemory = memory.createChild();
  const leftReader = createSpillRunReader(store, ownerId, left, columns, mergeMemory);
  const rightReader = createSpillRunReader(store, ownerId, right, columns, mergeMemory);
  let outputPage: QueryRow[] = [];
  let outputMemory = mergeMemory.createChild();
  let pageIndex = 0;
  const flush = async () => {
    if (outputPage.length === 0) return;
    pageIndex += await writeSpillRowPages(
      store,
      ownerId,
      outputId,
      pageIndex,
      columns,
      outputPage,
      pageRows,
    );
    outputPage = [];
    outputMemory.close();
    outputMemory = mergeMemory.createChild();
  };
  try {
    let leftRow = await leftReader.next();
    let rightRow = await rightReader.next();
    while (leftRow !== undefined || rightRow !== undefined) {
      if (
        rightRow === undefined ||
        (leftRow !== undefined && compareOrderedRows(leftRow, rightRow, orderBy) <= 0)
      ) {
        const row = required(leftRow, "Left spill row is missing");
        outputMemory.tally(queryRowPayloadBytes(row), "Spill merge output row");
        outputPage.push(row);
        leftRow = await leftReader.next();
      } else {
        outputMemory.tally(queryRowPayloadBytes(rightRow), "Spill merge output row");
        outputPage.push(rightRow);
        rightRow = await rightReader.next();
      }
      if (outputPage.length === pageRows) await flush();
    }
    await flush();
    return { id: outputId, pageCount: pageIndex };
  } finally {
    outputMemory.close();
    leftReader.close();
    rightReader.close();
    mergeMemory.close();
  }
}

function createSpillRunReader(
  store: QuerySpillStore,
  ownerId: string,
  run: SpillRun,
  columns: readonly string[],
  memory: QueryMemoryContext,
): { next(): Promise<QueryRow | undefined>; close(): void } {
  let pageIndex = 0;
  let rows: QueryRow[] = [];
  let rowIndex = 0;
  let pageReservation: QueryMemoryReservation | undefined;
  return {
    async next() {
      while (rowIndex >= rows.length) {
        if (pageIndex >= run.pageCount) return undefined;
        const bytes = await store.getPage(ownerId, run.id, pageIndex);
        if (bytes === undefined) throw new Error("Query spill page is missing");
        pageReservation?.release();
        pageReservation = memory.reserve(spillRowsModeledBytes(bytes), "Spill input page");
        rows = decodeSpillRows(columns, bytes);
        rowIndex = 0;
        pageIndex += 1;
      }
      const row = rows[rowIndex];
      rowIndex += 1;
      return row;
    },
    close() {
      pageReservation?.release();
      pageReservation = undefined;
      rows = [];
    },
  };
}

function compareOrderedRows(
  left: QueryRow,
  right: QueryRow,
  orderBy: BoundPlan["orderBy"],
): number {
  for (const order of orderBy) {
    const placed = nullOrder(
      left[order.outputName],
      right[order.outputName],
      order.nulls,
      order.direction,
    );
    if (placed !== undefined && placed !== 0) return placed;
    const comparison = compareValues(left[order.outputName], right[order.outputName]);
    if (comparison !== 0) return order.direction === "desc" ? -comparison : comparison;
  }
  return 0;
}

interface EncodedSpillRow {
  readonly bytes: Uint8Array;
  readonly modeledBytes: number;
}

function encodeSpillRow(columns: readonly string[], row: QueryRow): EncodedSpillRow {
  return {
    bytes: vectorTextEncoder.encode(
      JSON.stringify(columns.map((column) => encodeSpillValue(row[column] ?? null))),
    ),
    modeledBytes: queryRowPayloadBytes(row),
  };
}

function buildSpillPage(
  rows: readonly EncodedSpillRow[],
  byteLength: number,
  modeledBytes: number,
) {
  const bytes = new Uint8Array(byteLength);
  const header = new DataView(bytes.buffer);
  header.setUint32(0, SPILL_PAGE_MAGIC, true);
  header.setUint32(4, modeledBytes, true);
  let offset = SPILL_PAGE_HEADER_BYTES;
  bytes[offset++] = 0x5b;
  for (let index = 0; index < rows.length; index += 1) {
    if (index > 0) bytes[offset++] = 0x2c;
    const row = required(rows[index], "Encoded spill row is missing");
    bytes.set(row.bytes, offset);
    offset += row.bytes.byteLength;
  }
  bytes[offset] = 0x5d;
  return bytes;
}

/** Encodes pages incrementally and splits on both the requested row target and the hard byte cap. */
function* encodeSpillRowPages(
  columns: readonly string[],
  rows: readonly QueryRow[],
  pageRows: number,
): Generator<Uint8Array> {
  let pending: EncodedSpillRow[] = [];
  let byteLength = SPILL_PAGE_HEADER_BYTES + 2;
  let modeledBytes = 0;
  const flush = (): Uint8Array => {
    const page = buildSpillPage(pending, byteLength, modeledBytes);
    pending = [];
    byteLength = SPILL_PAGE_HEADER_BYTES + 2;
    modeledBytes = 0;
    return page;
  };
  for (const row of rows) {
    const encoded = encodeSpillRow(columns, row);
    const separatorBytes = pending.length === 0 ? 0 : 1;
    const nextByteLength = byteLength + separatorBytes + encoded.bytes.byteLength;
    const nextModeledBytes = modeledBytes + encoded.modeledBytes;
    if (
      pending.length > 0 &&
      (pending.length === pageRows ||
        nextByteLength > MAX_TEMP_RUN_PAGE_BYTES ||
        nextModeledBytes > 0xffffffff)
    ) {
      yield flush();
    }
    const addedSeparatorBytes = pending.length === 0 ? 0 : 1;
    byteLength += addedSeparatorBytes + encoded.bytes.byteLength;
    modeledBytes += encoded.modeledBytes;
    if (byteLength > MAX_TEMP_RUN_PAGE_BYTES || modeledBytes > 0xffffffff) {
      throw new RangeError("One query result row exceeds the spill page storage limit");
    }
    pending.push(encoded);
  }
  if (pending.length > 0) yield flush();
}

function decodeSpillRows(columns: readonly string[], bytes: Uint8Array): QueryRow[] {
  spillRowsModeledBytes(bytes);
  const value: unknown = JSON.parse(
    new TextDecoder().decode(bytes.subarray(SPILL_PAGE_HEADER_BYTES)),
  );
  if (!Array.isArray(value)) throw new Error("Query spill page is invalid");
  return value.map((encodedRow) => {
    if (!Array.isArray(encodedRow) || encodedRow.length !== columns.length) {
      throw new Error("Query spill row is invalid");
    }
    return Object.fromEntries(
      columns.map((column, index) => [column, decodeSpillValue(encodedRow[index])]),
    );
  });
}

function spillRowsModeledBytes(bytes: Uint8Array): number {
  if (bytes.byteLength < SPILL_PAGE_HEADER_BYTES) throw new Error("Query spill page is invalid");
  const header = new DataView(bytes.buffer, bytes.byteOffset, SPILL_PAGE_HEADER_BYTES);
  if (header.getUint32(0, true) !== SPILL_PAGE_MAGIC) {
    throw new Error("Query spill page header is invalid");
  }
  return header.getUint32(4, true);
}

function encodeSpillValue(value: QueryValue): readonly unknown[] {
  if (value === null) return [0];
  if (typeof value === "boolean") return [1, value];
  if (typeof value === "number") return [2, Object.is(value, -0) ? "-0" : String(value)];
  if (typeof value === "string") return [3, value];
  return [4, dateMilliseconds(value)];
}

function decodeSpillValue(value: unknown): QueryValue {
  if (!Array.isArray(value)) throw new Error("Query spill value is invalid");
  const tag: unknown = value[0];
  if (tag === 0) return null;
  if (tag === 1 && typeof value[1] === "boolean") return value[1];
  if (tag === 2 && typeof value[1] === "string") return Number(value[1]);
  if (tag === 3 && typeof value[1] === "string") return value[1];
  if (tag === 4 && typeof value[1] === "number") return new Date(value[1]);
  throw new Error("Query spill value is invalid");
}

function executeMetadataCount(
  plan: BoundPlan,
  memory: QueryMemoryContext,
): QueryResult | undefined {
  if (
    !plan.grouped ||
    plan.groupBy.length > 0 ||
    plan.joins.length > 0 ||
    plan.predicates.length > 0 ||
    plan.having.length > 0 ||
    plan.aggregates.length === 0 ||
    plan.aggregates.some(
      (aggregate) => aggregate.name !== "COUNT" || aggregate.argument.kind !== "wildcard",
    )
  ) {
    return undefined;
  }
  const state = createGroupState([], plan, memory);
  const rowCount = plan.sourceTables[plan.scanSource]?.rowCount ?? 0;
  state.counts.fill(rowCount);
  return finishResult(plan, finishGroups(plan, [state], memory), memory);
}

function finishResult(
  plan: BoundPlan,
  inputRows: QueryRow[],
  memory: QueryMemoryContext,
): QueryResult {
  const rows = inputRows;
  if (plan.orderBy.length > 0 && !plan.sourceOrdered) {
    const ordering = memory.reserve(
      safeMemoryProduct(
        rows.length,
        Uint32Array.BYTES_PER_ELEMENT * 2 +
          Uint8Array.BYTES_PER_ELEMENT +
          // Per extracted sort key: one slot, plus the null mask a numeric column adds beside
          // its Float64Array. Modeled for every term, since which columns specialize is only
          // known once the values are read.
          plan.orderBy.length * (QUERY_REFERENCE_BYTES + Uint8Array.BYTES_PER_ELEMENT),
        "Ordering typed scratch",
      ),
      "Ordering typed scratch",
    );
    try {
      stableSortRows(rows, plan.orderBy);
    } finally {
      ordering.release();
    }
  }
  const start = plan.offset ?? 0;
  if (start > 0) rows.splice(0, Math.min(start, rows.length));
  if (plan.limit !== undefined) {
    rows.length = Math.min(plan.limit, rows.length);
  }
  const columns = plan.wildcard ? wildcardColumnNames(plan) : plan.select.map((item) => item.alias);
  return { columns, rows };
}

/** Source indexes a bound expression reads, for pre-join predicate placement. */
function prefilterSources(expression: BoundExpression, into: Set<number>): boolean {
  if (expression.kind === "column") {
    into.add(expression.source);
    return true;
  }
  // FTS and aggregate-bearing expressions stay at the final stage.
  if (expression.kind === "fts") return false;
  if (expression.kind === "call" && expression.aggregateIndex !== undefined) return false;
  for (const child of boundChildren(expression)) {
    if (!prefilterSources(child, into)) return false;
  }
  return true;
}

interface PrefilteredBatch {
  /** Surviving original row indexes; only the first `survivors` entries are meaningful. */
  readonly selection: Uint32Array;
  readonly survivors: number;
  /** True when every plan predicate was applied, so downstream re-checks can be skipped. */
  readonly complete: boolean;
}

/**
 * Remembers which value buffers hold an ascending, null-free run. The key is the buffer
 * itself, not the vector: a streamed window that covers one block aliases that block's
 * decoded array, and decoded blocks live in the buffer pool, so a repeated keyed query pays
 * the ordering check once per block rather than once per scan. A window stitched from several
 * blocks gets a freshly allocated buffer and so a fresh entry, which is correct because that
 * buffer describes exactly one window.
 */
const ascendingValueBuffers = new WeakMap<object, boolean>();

/**
 * True when every slot of the vector's resident window carries a value and those values never
 * decrease. Non-ascending columns bail at the first violation, so the check costs a couple of
 * iterations for the columns it cannot help.
 */
function windowIsAscending(vector: NumberVector | DateTimeVector): boolean {
  const values = vector.values;
  const cached = ascendingValueBuffers.get(values);
  if (cached !== undefined) return cached;
  const length = vector.window?.length ?? Math.min(vector.length, values.length);
  const validity = vector.validity;
  let ascending = true;
  let previous = Number.NEGATIVE_INFINITY;
  for (let slot = 0; slot < length; slot += 1) {
    // A null has no position in the ordering, and NaN compares false against everything, so
    // either one puts the window outside what a binary search can answer.
    if (((validity[slot >>> 3] ?? 0) & (1 << (slot & 7))) === 0) {
      ascending = false;
      break;
    }
    const value = values[slot] ?? 0;
    if (!(value >= previous)) {
      ascending = false;
      break;
    }
    previous = value;
  }
  ascendingValueBuffers.set(values, ascending);
  return ascending;
}

/** First slot in [begin, end) whose value is at least `target`, over an ascending run. */
function lowerBoundSlot(values: Float64Array, begin: number, end: number, target: number): number {
  let low = begin;
  let high = end;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((values[middle] ?? 0) < target) low = middle + 1;
    else high = middle;
  }
  return low;
}

/** First slot in [begin, end) whose value is greater than `target`, over an ascending run. */
function upperBoundSlot(values: Float64Array, begin: number, end: number, target: number): number {
  let low = begin;
  let high = end;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((values[middle] ?? 0) <= target) low = middle + 1;
    else high = middle;
  }
  return low;
}

/**
 * Narrows a scan over [begin, end) to the rows a comparison against an ascending column can
 * still satisfy — the shape a generated key has, and the one a keyed lookup needs to stop
 * costing a block scan. Returns undefined when no predicate qualifies.
 *
 * This only removes rows that provably fail a predicate the scan was going to apply anyway,
 * so every predicate loop downstream runs unchanged: narrowing subtracts work, never a check.
 */
function ascendingScanRange(
  plan: BoundPlan,
  begin: number,
  end: number,
): { begin: number; end: number; ranges?: Array<{ begin: number; end: number }> } | undefined {
  if (end - begin < DEFAULT_BATCH_ROWS) return undefined;
  let low = begin;
  let high = end;
  let narrowed = false;
  for (const predicate of plan.predicates) {
    const primitive = predicate.primitive ?? predicate.primitiveIn;
    if (primitive?.source !== plan.scanSource) continue;
    const vector = primitive.vector;
    if (vector.kind !== "number" && vector.kind !== "datetime") continue;
    const windowStart = vector.window?.start ?? 0;
    const slotBegin = low - windowStart;
    const slotEnd = high - windowStart;
    // The range has to sit inside the resident window for the search to read real values.
    if (slotBegin < 0 || slotEnd > vector.values.length) continue;
    if (!windowIsAscending(vector)) continue;
    const values = vector.values;
    if (!("operator" in primitive)) {
      // NOT IN is satisfied by exactly the rows outside the member span, so the span narrows
      // nothing -- restricting to it would drop every row the predicate keeps.
      if (primitive.negated) continue;
      // A literal list restricts the scan to the span between its smallest and largest member;
      // the membership test still runs, and decides the rows inside that span.
      low = lowerBoundSlot(values, slotBegin, slotEnd, primitive.minimum) + windowStart;
      high = upperBoundSlot(values, low - windowStart, slotEnd, primitive.maximum) + windowStart;
      narrowed = true;
      if (low >= high) return { begin: low, end: low };
      continue;
    }
    const target = primitive.value;
    switch (primitive.operator) {
      case "=": {
        const first = lowerBoundSlot(values, slotBegin, slotEnd, target);
        high = upperBoundSlot(values, first, slotEnd, target) + windowStart;
        low = first + windowStart;
        break;
      }
      case ">":
        low = upperBoundSlot(values, slotBegin, slotEnd, target) + windowStart;
        break;
      case ">=":
        low = lowerBoundSlot(values, slotBegin, slotEnd, target) + windowStart;
        break;
      case "<":
        high = lowerBoundSlot(values, slotBegin, slotEnd, target) + windowStart;
        break;
      case "<=":
        high = upperBoundSlot(values, slotBegin, slotEnd, target) + windowStart;
        break;
      // `!=` keeps rows on both sides of the target, which is not a range.
      default:
        continue;
    }
    narrowed = true;
    if (low >= high) return { begin: low, end: low };
  }
  if (low >= high) return narrowed ? { begin: low, end: low } : undefined;
  const split = splitByListMembers(plan, low, high);
  if (split !== undefined) return { begin: low, end: high, ranges: split };
  return narrowed ? { begin: low, end: high } : undefined;
}

/**
 * The span between a list's smallest and largest member is only useful when the members sit
 * close together; for keys spread across the table it is the whole table. On an ascending
 * column each member can instead be located on its own, turning `key IN (5 scattered values)`
 * into five binary searches over five tiny ranges rather than one scan of everything between
 * them. Returns undefined when no list qualifies, or when the split would not pay for itself.
 */
function splitByListMembers(
  plan: BoundPlan,
  low: number,
  high: number,
): Array<{ begin: number; end: number }> | undefined {
  for (const predicate of plan.predicates) {
    const list = predicate.primitiveIn;
    if (list === undefined || list.negated || list.source !== plan.scanSource) continue;
    // Each member costs a binary search and yields a batch of its own, so the split only pays
    // while the member count stays far below the rows it is skipping.
    if (list.members.size > MAX_SPLIT_LIST_MEMBERS) continue;
    const vector = list.vector;
    const windowStart = vector.window?.start ?? 0;
    const slotBegin = low - windowStart;
    const slotEnd = high - windowStart;
    if (slotBegin < 0 || slotEnd > vector.values.length) continue;
    if (!windowIsAscending(vector)) continue;
    const values = vector.values;
    const ranges: Array<{ begin: number; end: number }> = [];
    // Ascending members keep the ranges ascending, which the forward-only streamed scan needs.
    for (const member of [...list.members].sort((left, right) => left - right)) {
      const first = lowerBoundSlot(values, slotBegin, slotEnd, member);
      const last = upperBoundSlot(values, first, slotEnd, member);
      if (first >= last) continue;
      const previous = ranges[ranges.length - 1];
      // Adjacent members land in adjacent runs; merging them keeps the batch count down.
      if (previous !== undefined && previous.end >= first + windowStart) {
        previous.end = last + windowStart;
        continue;
      }
      ranges.push({ begin: first + windowStart, end: last + windowStart });
    }
    let covered = 0;
    for (const range of ranges) covered += range.end - range.begin;
    // A split that still visits most of the span saves nothing and costs extra batches.
    if (covered * 2 > high - low) continue;
    return ranges;
  }
  return undefined;
}

/** Compacts the selection in place to rows where the primitive comparison holds. */
function filterPrimitive(
  primitive: PrimitiveComparison,
  batch: BatchRows,
  selection: Uint32Array,
  survivors: number,
): number {
  const vector = primitive.vector;
  const values = vector.values;
  const validity = vector.validity;
  const windowStart = vector.window?.start ?? 0;
  const slots = values.length;
  const vectorLength = vector.length;
  const rows = batch.rowsBySource[primitive.source];
  const target = primitive.value;
  // Any comparison operator is three independent outcomes: below, equal, above the target.
  const operator = primitive.operator;
  const passBelow = operator === "<" || operator === "<=" || operator === "!=" || operator === "<>";
  const passEqual = operator === "=" || operator === "<=" || operator === ">=";
  const passAbove = operator === ">" || operator === ">=" || operator === "!=" || operator === "<>";
  let kept = 0;
  for (let index = 0; index < survivors; index += 1) {
    const row = selection[index] ?? 0;
    const sourceRow = rows?.[row] ?? -1;
    if (sourceRow < 0 || sourceRow >= vectorLength) continue;
    const slot = sourceRow - windowStart;
    if (slot < 0 || slot >= slots) {
      throw new RangeError("Streamed vector row is outside the resident window");
    }
    if (((validity[slot >>> 3] ?? 0) & (1 << (slot & 7))) === 0) continue;
    const value = values[slot] ?? 0;
    if (!(value < target ? passBelow : value > target ? passAbove : passEqual)) continue;
    selection[kept] = row;
    kept += 1;
  }
  return kept;
}

/** Compacts the selection in place to rows whose value is a member of the literal list. */
function filterPrimitiveInList(
  primitive: PrimitiveInList,
  batch: BatchRows,
  selection: Uint32Array,
  survivors: number,
): number {
  const vector = primitive.vector;
  const values = vector.values;
  const validity = vector.validity;
  const windowStart = vector.window?.start ?? 0;
  const slots = values.length;
  const vectorLength = vector.length;
  const rows = batch.rowsBySource[primitive.source];
  const members = primitive.members;
  const negated = primitive.negated;
  let kept = 0;
  for (let index = 0; index < survivors; index += 1) {
    const row = selection[index] ?? 0;
    const sourceRow = rows?.[row] ?? -1;
    if (sourceRow < 0 || sourceRow >= vectorLength) continue;
    const slot = sourceRow - windowStart;
    if (slot < 0 || slot >= slots) {
      throw new RangeError("Streamed vector row is outside the resident window");
    }
    if (((validity[slot >>> 3] ?? 0) & (1 << (slot & 7))) === 0) continue;
    if (members.has(values[slot] ?? 0) === negated) continue;
    selection[kept] = row;
    kept += 1;
  }
  return kept;
}

/** Compacts the selection to rows where the dictionary-equality comparison holds. */
function filterDictionaryEquality(
  fast: DictionaryEquality,
  batch: BatchRows,
  selection: Uint32Array,
  survivors: number,
): number {
  const vector = fast.vector;
  if (fast.cache.dictionary !== vector.dictionary) {
    fast.cache.dictionary = vector.dictionary;
    fast.cache.code = vector.dictionary.indexOf(fast.value);
  }
  const target = fast.cache.code;
  const negated = fast.negated;
  const rows = batch.rowsBySource[fast.source];
  const codes = vector.codes;
  const validity = vector.validity;
  const windowStart = vector.window?.start ?? 0;
  const slots = codes.length;
  const vectorLength = vector.length;
  let kept = 0;
  for (let index = 0; index < survivors; index += 1) {
    const row = selection[index] ?? 0;
    const sourceRow = rows?.[row] ?? -1;
    if (sourceRow < 0 || sourceRow >= vectorLength) continue;
    const slot = sourceRow - windowStart;
    if (slot < 0 || slot >= slots) {
      throw new RangeError("Streamed vector row is outside the resident window");
    }
    if (((validity[slot >>> 3] ?? 0) & (1 << (slot & 7))) === 0) continue;
    const code = codes[slot] ?? NULL_STRING_CODE;
    if (code === NULL_STRING_CODE) continue;
    const matches = target >= 0 && code === target;
    if (negated ? matches : !matches) continue;
    selection[kept] = row;
    kept += 1;
  }
  return kept;
}

/** Compacts the selection to rows where the dictionary LIKE comparison holds. */
function filterDictionaryLike(
  fast: DictionaryLike,
  batch: BatchRows,
  selection: Uint32Array,
  survivors: number,
): number {
  const vector = fast.vector;
  if (fast.cache.dictionary !== vector.dictionary) {
    fast.cache.dictionary = vector.dictionary;
    fast.cache.matches = dictionaryLikeMatches(
      vector.dictionary,
      fast.pattern,
      fast.caseInsensitive,
      fast.escape,
    );
  }
  const matches = fast.cache.matches;
  const negated = fast.negated;
  const rows = batch.rowsBySource[fast.source];
  const codes = vector.codes;
  const validity = vector.validity;
  const windowStart = vector.window?.start ?? 0;
  const slots = codes.length;
  const vectorLength = vector.length;
  let kept = 0;
  for (let index = 0; index < survivors; index += 1) {
    const row = selection[index] ?? 0;
    const sourceRow = rows?.[row] ?? -1;
    if (sourceRow < 0 || sourceRow >= vectorLength) continue;
    const slot = sourceRow - windowStart;
    if (slot < 0 || slot >= slots) {
      throw new RangeError("Streamed vector row is outside the resident window");
    }
    if (((validity[slot >>> 3] ?? 0) & (1 << (slot & 7))) === 0) continue;
    const code = codes[slot] ?? NULL_STRING_CODE;
    if (code === NULL_STRING_CODE) continue;
    const matched = matches[code] === 1;
    if (negated ? matched : !matched) continue;
    selection[kept] = row;
    kept += 1;
  }
  return kept;
}

// The per-batch selection scratch: batches are bounded by DEFAULT_BATCH_ROWS, spills may use
// larger pages, so the scratch grows to the largest batch seen and is trivially small.
let selectionScratch = new Uint32Array(DEFAULT_BATCH_ROWS);

/**
 * Compacts the selection with whatever unboxed kernel a predicate compiled to, or returns
 * undefined when it has none and the caller should fall back to the per-row evaluator.
 */
function applyPredicateKernel(
  plan: BoundPlan,
  predicate: BoundPredicate,
  batch: BatchRows,
  selection: Uint32Array,
  survivors: number,
): number | undefined {
  if (predicate.primitive !== undefined) {
    return filterPrimitive(predicate.primitive, batch, selection, survivors);
  }
  if (predicate.primitiveIn !== undefined) {
    return filterPrimitiveInList(predicate.primitiveIn, batch, selection, survivors);
  }
  if (predicate.dictionaryEquality !== undefined) {
    return filterDictionaryEquality(predicate.dictionaryEquality, batch, selection, survivors);
  }
  if (predicate.dictionaryLike !== undefined) {
    return filterDictionaryLike(predicate.dictionaryLike, batch, selection, survivors);
  }
  if (predicate.disjunction !== undefined) {
    return filterDisjunction(plan, predicate.disjunction, batch, selection, survivors);
  }
  return undefined;
}

// Scratch for the disjunction kernel. Branch predicates are always plain conditions, so a
// branch never carries a disjunction of its own and these buffers are never reentered.
let disjunctionCandidates = new Uint32Array(DEFAULT_BATCH_ROWS);
let disjunctionWork = new Uint32Array(DEFAULT_BATCH_ROWS);
let disjunctionMask = new Uint8Array(DEFAULT_BATCH_ROWS);

/**
 * The union kernel: each branch narrows its own copy of the incoming rows, and a row survives
 * the disjunction if any branch kept it. Marking hits in a byte mask keeps the result in the
 * original ascending order and costs one pass per branch plus one to compact, instead of
 * walking the whole boolean tree once per row.
 */
function filterDisjunction(
  plan: BoundPlan,
  disjunction: BoundDisjunction,
  batch: BatchRows,
  selection: Uint32Array,
  survivors: number,
): number {
  if (disjunctionCandidates.length < survivors) {
    disjunctionCandidates = new Uint32Array(survivors);
    disjunctionWork = new Uint32Array(survivors);
  }
  if (disjunctionMask.length < batch.length) disjunctionMask = new Uint8Array(batch.length);
  const candidates = disjunctionCandidates;
  const work = disjunctionWork;
  const mask = disjunctionMask;
  for (let index = 0; index < survivors; index += 1) candidates[index] = selection[index] ?? 0;
  for (const branch of disjunction.branches) {
    for (let index = 0; index < survivors; index += 1) work[index] = candidates[index] ?? 0;
    let kept = survivors;
    for (const predicate of branch) {
      if (kept === 0) break;
      const compacted = applyPredicateKernel(plan, predicate, batch, work, kept);
      if (compacted !== undefined) {
        kept = compacted;
        continue;
      }
      let generic = 0;
      for (let index = 0; index < kept; index += 1) {
        const row = work[index] ?? 0;
        if (!evaluateBatchPredicate(plan, predicate, batch, row)) continue;
        work[generic] = row;
        generic += 1;
      }
      kept = generic;
    }
    for (let index = 0; index < kept; index += 1) mask[work[index] ?? 0] = 1;
  }
  let kept = 0;
  for (let index = 0; index < survivors; index += 1) {
    const row = candidates[index] ?? 0;
    if (mask[row] !== 1) continue;
    mask[row] = 0;
    selection[kept] = row;
    kept += 1;
  }
  return kept;
}

/**
 * The no-join predicate kernel: primitive comparisons and dictionary equality/LIKE compact a
 * shared selection in tight unboxed loops, and only rows surviving those reach the generic
 * per-row evaluator for whatever predicates remain. Batches with no predicates skip it.
 */
function filterScanBatch(
  plan: BoundPlan,
  batch: BatchRows,
): { selection: Uint32Array; survivors: number } {
  if (selectionScratch.length < batch.length) selectionScratch = new Uint32Array(batch.length);
  const selection = selectionScratch;
  for (let row = 0; row < batch.length; row += 1) selection[row] = row;
  let survivors = batch.length;
  let generic: BoundPredicate[] | undefined;
  for (const predicate of plan.predicates) {
    if (survivors === 0) return { selection, survivors };
    const compacted = applyPredicateKernel(plan, predicate, batch, selection, survivors);
    if (compacted === undefined) (generic ??= []).push(predicate);
    else survivors = compacted;
  }
  if (generic !== undefined) {
    for (const predicate of generic) {
      if (survivors === 0) break;
      let kept = 0;
      for (let index = 0; index < survivors; index += 1) {
        const row = selection[index] ?? 0;
        if (!evaluateBatchPredicate(plan, predicate, batch, row)) continue;
        selection[kept] = row;
        kept += 1;
      }
      survivors = kept;
    }
  }
  return { selection, survivors };
}

/**
 * Predicates whose sources are all materialized before the given join filter the batch first,
 * so the join and everything downstream never see rows the WHERE clause was going to discard.
 * Filters are idempotent, so applying one early is pure savings; when every predicate applies
 * here the result is marked complete and the final pass skips re-checking.
 */
function prefilterBatch(
  plan: BoundPlan,
  batch: BatchRows,
  join: BoundJoin,
): PrefilteredBatch | undefined {
  let applicable: BoundPredicate[] | undefined;
  for (const predicate of plan.predicates) {
    const sources = new Set<number>();
    if (!prefilterSources(predicate.left, sources) || !prefilterSources(predicate.right, sources)) {
      continue;
    }
    let available = true;
    for (let later = plan.joins.indexOf(join); later < plan.joins.length; later += 1) {
      if (sources.has(plan.joins[later]?.buildSource ?? -1)) {
        available = false;
        break;
      }
    }
    if (available) (applicable ??= []).push(predicate);
  }
  if (applicable === undefined) return undefined;
  const complete = applicable.length === plan.predicates.length;
  const selection = new Uint32Array(batch.length);
  for (let row = 0; row < batch.length; row += 1) selection[row] = row;
  let survivors = batch.length;
  for (const predicate of applicable) {
    if (survivors === 0) break;
    const primitive = predicate.primitive;
    if (primitive !== undefined) {
      survivors = filterPrimitive(primitive, batch, selection, survivors);
      continue;
    }
    let kept = 0;
    for (let index = 0; index < survivors; index += 1) {
      const row = selection[index] ?? 0;
      if (!evaluateBatchPredicate(plan, predicate, batch, row)) continue;
      selection[kept] = row;
      kept += 1;
    }
    survivors = kept;
  }
  return { selection, survivors, complete };
}

/** Copies the surviving rows into a compact batch, for joins that cannot take a selection. */
function materializeSelection(
  batch: BatchRows,
  selection: Uint32Array,
  survivors: number,
  memory: QueryMemoryContext,
): BatchRows {
  const batchMemory = memory.createChild();
  try {
    batchMemory.reserve(
      safeMemoryProduct(
        safeMemoryProduct(batch.rowsBySource.length, survivors, "Prefiltered row-index count"),
        Int32Array.BYTES_PER_ELEMENT,
        "Prefiltered row indexes",
      ),
      "Prefiltered row indexes",
    );
    const rowsBySource = batch.rowsBySource.map((inputRows) => {
      const outputRows = new Int32Array(survivors);
      for (let output = 0; output < survivors; output += 1) {
        outputRows[output] = inputRows[selection[output] ?? 0] ?? -1;
      }
      return outputRows;
    });
    return { length: survivors, rowsBySource, memory: batchMemory };
  } catch (error) {
    batchMemory.close();
    throw error;
  }
}

function consumeJoinedBatches(
  plan: BoundPlan,
  batch: BatchRows,
  joinIndex: number,
  groups: GroupAccumulator,
  output: ResultSink,
  memory: QueryMemoryContext,
  prefiltered = false,
): boolean {
  const join = plan.joins[joinIndex];
  if (join === undefined) {
    consumeBatch(plan, batch, groups, output, memory, prefiltered);
    return reachedEarlyLimit(plan, output.size);
  }
  let working = batch;
  let complete = prefiltered;
  let owned: QueryMemoryContext | undefined;
  if (!prefiltered) {
    const filtered = prefilterBatch(plan, batch, join);
    if (filtered !== undefined) {
      complete = filtered.complete;
      if (filtered.survivors === 0) return false;
      if (filtered.survivors < batch.length) {
        // Compacting up front measured faster than threading the selection into the join:
        // the probe loop stays dense and the all-match shortcut skips the second copy.
        working = materializeSelection(batch, filtered.selection, filtered.survivors, memory);
        owned = working.memory;
      }
    }
  }
  try {
    for (const joined of joinBatches(plan, working, join, memory)) {
      try {
        if (consumeJoinedBatches(plan, joined, joinIndex + 1, groups, output, memory, complete)) {
          return true;
        }
      } finally {
        joined.memory?.close();
      }
    }
    return false;
  } finally {
    owned?.close();
  }
}

function reachedEarlyLimit(plan: BoundPlan, outputRows: number): boolean {
  // Early termination must still produce the rows the trailing OFFSET will discard.
  return (
    !plan.grouped &&
    (plan.orderBy.length === 0 || plan.sourceOrdered) &&
    plan.limit !== undefined &&
    outputRows >= plan.limit + (plan.offset ?? 0)
  );
}

function* joinBatches(
  plan: BoundPlan,
  input: BatchRows,
  join: BoundJoin,
  memory: QueryMemoryContext,
): Generator<BatchRows> {
  if (join.loop !== undefined) {
    yield* loopJoinBatches(plan, input, join, join.loop, memory);
    return;
  }
  if (join.lookup.unique) {
    yield joinUniqueBatch(plan, input, join, memory);
    return;
  }
  let outputMemory: QueryMemoryContext | undefined;
  try {
    let outputRows: Int32Array[] | undefined;
    let outputLength = 0;
    const ensureOutput = (): Int32Array[] => {
      if (outputRows !== undefined) return outputRows;
      outputMemory = memory.createChild();
      outputMemory.reserve(
        safeMemoryProduct(
          safeMemoryProduct(
            plan.sourceTables.length,
            DEFAULT_BATCH_ROWS,
            "Join fan-out row-index count",
          ),
          Int32Array.BYTES_PER_ELEMENT,
          "Join fan-out row indexes",
        ),
        "Join fan-out row indexes",
      );
      outputRows = plan.sourceTables.map(() => new Int32Array(DEFAULT_BATCH_ROWS));
      return outputRows;
    };
    const emit = function* (): Generator<BatchRows> {
      if (outputRows === undefined || outputMemory === undefined || outputLength === 0) return;
      const batch = {
        length: outputLength,
        rowsBySource: outputRows,
        memory: outputMemory,
      };
      outputRows = undefined;
      outputMemory = undefined;
      outputLength = 0;
      yield batch;
    };
    for (let row = 0; row < input.length; row += 1) {
      const probeKey = evaluateBatchExpression(plan, join.probe, input, row);
      let buildRow = probeKey === null ? -1 : join.lookup.firstRow(probeKey);
      if (buildRow < 0) {
        if (join.kind === "left" || join.kind === "anti") {
          appendJoinedRow(ensureOutput(), outputLength, input, row, join.buildSource, -1);
          outputLength += 1;
        }
      } else if (join.kind === "semi") {
        appendJoinedRow(ensureOutput(), outputLength, input, row, join.buildSource, buildRow);
        outputLength += 1;
      } else if (join.kind === "anti") {
        // A match excludes the probe row.
      } else {
        while (buildRow >= 0) {
          appendJoinedRow(ensureOutput(), outputLength, input, row, join.buildSource, buildRow);
          outputLength += 1;
          if (outputLength === DEFAULT_BATCH_ROWS) yield* emit();
          buildRow = join.lookup.nextRow(buildRow);
        }
      }
      if (outputLength === DEFAULT_BATCH_ROWS) yield* emit();
    }
    yield* emit();
  } finally {
    outputMemory?.close();
  }
}

/**
 * Nested-loop join for general ON conditions: every probe row scans the whole build table and
 * keeps the pairs whose condition evaluates true under three-valued logic. Output batches reuse
 * the hash-join fan-out format, so downstream consumption is identical; cost is probe x build.
 */
function* loopJoinBatches(
  plan: BoundPlan,
  input: BatchRows,
  join: BoundJoin,
  loop: { condition: BoundExpression; rowCount: number },
  memory: QueryMemoryContext,
): Generator<BatchRows> {
  let outputMemory: QueryMemoryContext | undefined;
  const scratchReservation = memory.reserve(
    safeMemoryProduct(
      plan.sourceTables.length,
      Int32Array.BYTES_PER_ELEMENT,
      "Loop join scratch row indexes",
    ),
    "Loop join scratch row indexes",
  );
  try {
    const scratch = new Int32Array(plan.sourceTables.length);
    scratch.fill(-1);
    let outputRows: Int32Array[] | undefined;
    let outputLength = 0;
    const ensureOutput = (): Int32Array[] => {
      if (outputRows !== undefined) return outputRows;
      outputMemory = memory.createChild();
      outputMemory.reserve(
        safeMemoryProduct(
          safeMemoryProduct(
            plan.sourceTables.length,
            DEFAULT_BATCH_ROWS,
            "Join fan-out row-index count",
          ),
          Int32Array.BYTES_PER_ELEMENT,
          "Join fan-out row indexes",
        ),
        "Join fan-out row indexes",
      );
      outputRows = plan.sourceTables.map(() => new Int32Array(DEFAULT_BATCH_ROWS));
      return outputRows;
    };
    const emit = function* (): Generator<BatchRows> {
      if (outputRows === undefined || outputMemory === undefined || outputLength === 0) return;
      const batch = {
        length: outputLength,
        rowsBySource: outputRows,
        memory: outputMemory,
      };
      outputRows = undefined;
      outputMemory = undefined;
      outputLength = 0;
      yield batch;
    };
    for (let row = 0; row < input.length; row += 1) {
      for (let source = 0; source < join.buildSource; source += 1) {
        scratch[source] = input.rowsBySource[source]?.[row] ?? -1;
      }
      let matched = false;
      for (let buildRow = 0; buildRow < loop.rowCount; buildRow += 1) {
        scratch[join.buildSource] = buildRow;
        const holds =
          booleanTruth(loop.condition, (nested) => evaluateExpression(nested, scratch)) === true;
        if (!holds) continue;
        matched = true;
        if (join.kind !== "anti") {
          appendJoinedRow(ensureOutput(), outputLength, input, row, join.buildSource, buildRow);
          outputLength += 1;
          if (outputLength === DEFAULT_BATCH_ROWS) yield* emit();
        }
        if (join.kind === "semi" || join.kind === "anti") break;
      }
      if (!matched && (join.kind === "left" || join.kind === "anti")) {
        appendJoinedRow(ensureOutput(), outputLength, input, row, join.buildSource, -1);
        outputLength += 1;
        if (outputLength === DEFAULT_BATCH_ROWS) yield* emit();
      }
    }
    yield* emit();
  } finally {
    scratchReservation.release();
    outputMemory?.close();
  }
}

function joinUniqueBatch(
  plan: BoundPlan,
  input: BatchRows,
  join: BoundJoin,
  memory: QueryMemoryContext,
): BatchRows {
  const batchMemory = memory.createChild();
  try {
    const selectedReservation = batchMemory.reserve(
      safeMemoryProduct(input.length, Uint32Array.BYTES_PER_ELEMENT, "Join selection vector"),
      "Join selection vector",
    );
    const buildReservation = batchMemory.reserve(
      safeMemoryProduct(input.length, Int32Array.BYTES_PER_ELEMENT, "Join build rows"),
      "Join build rows",
    );
    const selectedRows = new Uint32Array(input.length);
    const buildRows = new Int32Array(input.length);
    let outputLength = 0;
    const probe = join.probe;
    // Windowed probes qualify: stringCodeAt is window-aware and the code cache re-resolves
    // whenever the resident window's dictionary object changes.
    const dictProbe =
      probe.kind === "column" && probe.vector.kind === "string"
        ? { source: probe.source, vector: probe.vector }
        : undefined;
    if (dictProbe !== undefined) {
      const vector = dictProbe.vector;
      if (join.codeLookup?.dictionary !== vector.dictionary) {
        // -2 marks "not resolved yet"; -1 is a genuine miss.
        join.codeLookup = {
          dictionary: vector.dictionary,
          rows: new Int32Array(vector.dictionary.length).fill(-2),
        };
      }
      const cache = join.codeLookup.rows;
      const probeRows = input.rowsBySource[dictProbe.source];
      for (let row = 0; row < input.length; row += 1) {
        const sourceRow = probeRows?.[row] ?? -1;
        const code = stringCodeAt(vector, sourceRow);
        let buildRow = -1;
        if (code !== undefined) {
          buildRow = cache[code] ?? -2;
          if (buildRow === -2) {
            buildRow = join.lookup.firstRow(vector.dictionary[code] ?? "");
            cache[code] = buildRow;
          }
        }
        if (
          (buildRow < 0 && (join.kind === "inner" || join.kind === "semi")) ||
          (buildRow >= 0 && join.kind === "anti")
        ) {
          continue;
        }
        selectedRows[outputLength] = row;
        buildRows[outputLength] = join.kind === "anti" ? -1 : buildRow;
        outputLength += 1;
      }
    } else {
      for (let row = 0; row < input.length; row += 1) {
        const probeKey = evaluateBatchExpression(plan, join.probe, input, row);
        const buildRow = probeKey === null ? -1 : join.lookup.firstRow(probeKey);
        if (
          (buildRow < 0 && (join.kind === "inner" || join.kind === "semi")) ||
          (buildRow >= 0 && join.kind === "anti")
        ) {
          continue;
        }
        selectedRows[outputLength] = row;
        buildRows[outputLength] = join.kind === "anti" ? -1 : buildRow;
        outputLength += 1;
      }
    }
    if (outputLength === input.length) {
      selectedReservation.release();
      const rowsBySource = [...input.rowsBySource];
      rowsBySource[join.buildSource] = buildRows;
      return { length: outputLength, rowsBySource, memory: batchMemory };
    }
    batchMemory.reserve(
      safeMemoryProduct(
        safeMemoryProduct(plan.sourceTables.length, outputLength, "Filtered join row-index count"),
        Int32Array.BYTES_PER_ELEMENT,
        "Filtered join row indexes",
      ),
      "Filtered join row indexes",
    );
    const rowsBySource = input.rowsBySource.map((inputRows, source) => {
      const outputRows = new Int32Array(outputLength);
      for (let output = 0; output < outputLength; output += 1) {
        outputRows[output] =
          source === join.buildSource
            ? (buildRows[output] ?? -1)
            : (inputRows[selectedRows[output] ?? 0] ?? -1);
      }
      return outputRows;
    });
    selectedReservation.release();
    buildReservation.release();
    return { length: outputLength, rowsBySource, memory: batchMemory };
  } catch (error) {
    batchMemory.close();
    throw error;
  }
}

function appendJoinedRow(
  output: Int32Array[],
  outputRow: number,
  input: BatchRows,
  row: number,
  buildSource: number,
  buildRow: number,
): void {
  for (let source = 0; source < output.length; source += 1) {
    const rows = output[source];
    if (rows !== undefined) {
      rows[outputRow] =
        source === buildSource ? buildRow : (input.rowsBySource[source]?.[row] ?? -1);
    }
  }
}

interface RetainedRow {
  keys: QueryValue[];
  seq: number;
  /** The scan row to project at finish, when projection is deferred; -1 otherwise. */
  sourceRow: number;
  /** The projected row, kept when the scan cannot be re-read at finish. */
  row: QueryRow | undefined;
  bytes: number;
}

const RETAINED_ROW_BYTES = 3 * QUERY_REFERENCE_BYTES;

/**
 * A LIMIT that keeps at least this share of the scanned rows is not a bound worth enforcing:
 * what the bounded sink saves in memory it spends in time, and past the share it is cheaper to
 * keep every row and sort once. Measured at 200k rows, `ORDER BY region, id` and `ORDER BY
 * amount`, against an unbounded scan-and-sort that costs the same (30 ms and 21 ms) whatever the
 * LIMIT: at 5% the bound wins (22 ms and 17 ms), at 10% it loses (39 ms and 31 ms), and at 50%
 * it loses by three times (97 ms and 72 ms). The crossover sits between those two shares, so
 * the bound stops at the upper one.
 */
const BOUNDED_SINK_MAX_SHARE = 0.1;

/**
 * Accumulates ungrouped result rows. With ORDER BY and LIMIT the sink keeps only the best
 * `limit + offset` rows instead of materializing every scanned row for one full sort, so memory
 * stays bounded by the limit rather than the table. Retention matches a stable sort exactly:
 * ties resolve by arrival order, so the retained set is precisely the slice a full stable sort
 * would have produced.
 *
 * Candidates are tracked as (keys, arrival) entries in a selection buffer that is compacted by
 * quickselect once it reaches twice the capacity, so a retained row costs a few comparisons
 * amortized, and a row that cannot beat the current cut line costs nothing past its keys.
 * Whether the entry also carries the projected row depends on the scan: when the scan
 * vectors outlive the scan and the order keys resolve to select expressions, projection waits
 * for the survivors; a streamed scan, a join, or a wildcard select projects the candidate as it
 * arrives, since there is nothing to read it back from later.
 *
 * The bound is dropped for a page that would keep most of the scan anyway: a single-source
 * plan whose `limit + offset` reaches BOUNDED_SINK_MAX_SHARE of the scan rows keeps every row
 * and leaves the ordering to one final sort. A join's output size is not known from its scan, so
 * a joined plan stays bounded.
 */
class ResultSink {
  readonly #plan: BoundPlan;
  readonly #memory: QueryMemoryContext;
  readonly #capacity: number | undefined;
  readonly #keyExpressions: readonly BoundExpression[] | undefined;
  readonly #deferred: boolean;
  readonly #rows: QueryRow[] = [];
  readonly #selection: RetainedRow[] = [];
  readonly #keyScratch: QueryValue[] = [];
  #threshold: RetainedRow | undefined;
  /** The threshold's first order key as an unboxed float, when the fast reject applies. */
  #thresholdFirst: number | null | undefined;
  readonly #fastFirstKey:
    { vector: NumberVector | DateTimeVector; source: number; desc: boolean } | undefined;
  #selectionBytes = 0;
  #selectionReservedBytes = 0;
  #seq = 0;

  constructor(plan: BoundPlan, memory: QueryMemoryContext, stableScan: boolean) {
    this.#plan = plan;
    this.#memory = memory;
    const capacity = (plan.limit ?? 0) + (plan.offset ?? 0);
    const scanRows = plan.sourceTables[plan.scanSource]?.rowCount ?? 0;
    const bounded =
      !plan.grouped &&
      !plan.sourceOrdered &&
      plan.orderBy.length > 0 &&
      plan.limit !== undefined &&
      (plan.joins.length > 0 || capacity < scanRows * BOUNDED_SINK_MAX_SHARE);
    this.#capacity = bounded ? capacity : undefined;
    let keyExpressions: BoundExpression[] | undefined;
    if (bounded && !plan.wildcard) {
      keyExpressions = [];
      for (const order of plan.orderBy) {
        const item = plan.select.find((selected) => selected.alias === order.outputName);
        if (item === undefined) {
          keyExpressions = undefined;
          break;
        }
        keyExpressions.push(item.expression);
      }
    }
    this.#keyExpressions = keyExpressions;
    this.#deferred =
      bounded && keyExpressions !== undefined && plan.joins.length === 0 && stableScan;
    // A bare numeric/datetime first order key rejects most rows with one unboxed comparison
    // against the current cut line, before any generic key evaluation or allocation. NULLs and
    // explicit NULLS placement fall through to the full comparison, so semantics are untouched.
    // Windowed scan vectors qualify too: the batch loop re-reads the resident window per batch.
    const firstKey = keyExpressions?.[0];
    const firstOrder = plan.orderBy[0];
    this.#fastFirstKey =
      firstKey !== undefined &&
      firstOrder !== undefined &&
      firstOrder.nulls === undefined &&
      firstKey.kind === "column" &&
      (firstKey.vector.kind === "number" || firstKey.vector.kind === "datetime")
        ? {
            vector: firstKey.vector,
            source: firstKey.source,
            desc: firstOrder.direction === "desc",
          }
        : undefined;
  }

  /** Rows accepted so far; only meaningful for the unbounded early-limit check. */
  get size(): number {
    return this.#capacity === undefined ? this.#rows.length : 0;
  }

  /**
   * The bounded-order batch loop: with an unboxed first key and an established cut line, a
   * whole batch scans in one pass and strictly-worse rows die on a single float comparison.
   * Returns false when the sink shape needs the per-row path.
   */
  tryAddBatch(batch: BatchRows): boolean {
    const fast = this.#fastFirstKey;
    if (this.#capacity === undefined || fast === undefined) return false;
    const rows = batch.rowsBySource[fast.source];
    const vector = fast.vector;
    // Window and arrays re-read per batch: a streamed scan replaces them on every window slide.
    const values = vector.values;
    const validity = vector.validity;
    const window = vector.window;
    const windowStart = window?.start ?? 0;
    const windowLength = window?.length ?? vector.length;
    const desc = fast.desc;
    for (let row = 0; row < batch.length; row += 1) {
      const threshold = this.#thresholdFirst;
      if (threshold === undefined || threshold === null) {
        this.#addCandidate(batch, row);
        continue;
      }
      const sourceRow = rows?.[row] ?? -1;
      const slot = sourceRow - windowStart;
      if (sourceRow >= 0 && sourceRow < vector.length && slot >= 0 && slot < windowLength) {
        if (isValid(validity, slot)) {
          const value = values[slot] ?? 0;
          if ((desc ? threshold - value : value - threshold) > 0) {
            this.#seq += 1;
            continue;
          }
        }
      }
      this.#addCandidate(batch, row);
    }
    return true;
  }

  add(batch: BatchRows, row: number): void {
    if (this.#capacity === undefined) {
      const resultRow = projectBatchRow(this.#plan, batch, row);
      this.#memory.tally(queryRowPayloadBytes(resultRow), "Accumulated result row");
      this.#rows.push(resultRow);
      return;
    }
    if (this.#capacity === 0) return;
    this.#addCandidate(batch, row);
  }

  /** Returns accepted rows in arrival order, ready for the shared stable sort and trim. */
  finish(): QueryRow[] {
    if (this.#capacity === undefined) return this.#rows;
    this.#compactSelection();
    this.#selection.sort((left, right) => left.seq - right.seq);
    if (!this.#deferred) {
      return this.#selection.map((entry) => required(entry.row, "Retained row is missing"));
    }
    const scanIndex = new Int32Array(1);
    const rowsBySource = [scanIndex];
    const batch: BatchRows = { length: 1, rowsBySource };
    const rows: QueryRow[] = [];
    for (const entry of this.#selection) {
      scanIndex[0] = entry.sourceRow;
      const resultRow = projectBatchRow(this.#plan, batch, 0);
      this.#memory.tally(queryRowPayloadBytes(resultRow), "Accumulated result row");
      rows.push(resultRow);
    }
    return rows;
  }

  #addCandidate(batch: BatchRows, row: number): void {
    const fast = this.#fastFirstKey;
    if (fast !== undefined && this.#thresholdFirst !== undefined && this.#thresholdFirst !== null) {
      const value = rawFloat64Value(fast.vector, batch.rowsBySource[fast.source]?.[row] ?? -1);
      if (value !== null) {
        const comparison = fast.desc ? this.#thresholdFirst - value : value - this.#thresholdFirst;
        // Strictly worse on the first key is strictly worse overall; the row cannot survive.
        if (comparison > 0) {
          this.#seq += 1;
          return;
        }
      }
    }
    let projected: QueryRow | undefined;
    if (this.#keyExpressions === undefined) {
      // The order keys are not select expressions, so the projected row is where they live.
      projected = projectBatchRow(this.#plan, batch, row);
      for (let index = 0; index < this.#plan.orderBy.length; index += 1) {
        const order = required(this.#plan.orderBy[index], "Order term is missing");
        this.#keyScratch[index] = projected[order.outputName] ?? null;
      }
    } else {
      this.#evaluateKeys(batch, row);
    }
    const seq = this.#seq;
    this.#seq += 1;
    // The candidate arrived after every retained row, so an order-key tie keeps the retained
    // row — only a strictly better key survives the current cut line.
    if (
      this.#threshold !== undefined &&
      this.#compareKeys(this.#keyScratch, this.#threshold.keys) >= 0
    ) {
      return;
    }
    if (!this.#deferred && projected === undefined) {
      projected = projectBatchRow(this.#plan, batch, row);
    }
    let bytes = RETAINED_ROW_BYTES;
    for (let index = 0; index < this.#plan.orderBy.length; index += 1) {
      bytes = safeMemorySum(
        bytes,
        queryValuePayloadBytes(this.#keyScratch[index] ?? null),
        "Top-N selection entry",
      );
    }
    if (projected !== undefined) {
      bytes = safeMemorySum(bytes, queryRowPayloadBytes(projected), "Top-N selection entry");
    }
    this.#selection.push({
      keys: this.#keyScratch.slice(0, this.#plan.orderBy.length),
      seq,
      sourceRow: this.#deferred ? (batch.rowsBySource[this.#plan.scanSource]?.[row] ?? -1) : -1,
      row: projected,
      bytes,
    });
    this.#reserveSelectionBytes(bytes);
    if (this.#selection.length >= (this.#capacity ?? 0) * 2) this.#compactSelection();
  }

  /**
   * Reserves selection-buffer growth at its high-water mark: the buffer is bounded by twice the
   * limit, so reservations grow monotonically instead of churning a release per evicted entry.
   */
  #reserveSelectionBytes(bytes: number): void {
    this.#selectionBytes += bytes;
    if (this.#selectionBytes <= this.#selectionReservedBytes) return;
    this.#memory.reserve(this.#selectionBytes - this.#selectionReservedBytes, "Top-N selection");
    this.#selectionReservedBytes = this.#selectionBytes;
  }

  /**
   * Trims the selection to the best `capacity` entries and advances the cut line.
   *
   * Partition, not sort: which rows survive is all that matters here, because `finish` hands
   * them back in arrival order for the query's own ORDER BY to sort. Quickselect puts the
   * capacity-th best entry at its final index with everything better ahead of it, in linear
   * comparisons rather than n log n. The retained set is identical to what sorting and
   * truncating produced — the comparator is the same, ties included — and so is the memory,
   * which is the point: a deep page keeps its bound and stops paying a full sort for it.
   */
  #compactSelection(): void {
    const capacity = this.#capacity ?? 0;
    if (this.#selection.length <= capacity) return;
    this.#selectBest(capacity);
    this.#selection.length = capacity;
    let retainedBytes = 0;
    for (const entry of this.#selection) retainedBytes += entry.bytes;
    this.#selectionBytes = retainedBytes;
    // Quickselect leaves the capacity-th best entry at the last retained index, which is the
    // worst of what survived and therefore the new cut line.
    this.#threshold = this.#selection[capacity - 1];
    const first = this.#threshold?.keys[0];
    this.#thresholdFirst =
      first === undefined
        ? undefined
        : first === null
          ? null
          : typeof first === "number"
            ? first
            : first instanceof Date
              ? first.getTime()
              : undefined;
  }

  /**
   * Quickselect over the selection buffer: after it returns, the first `count` entries are the
   * `count` best in some order and index `count - 1` holds the worst of them, which is the cut
   * line. Median-of-three pivots keep the already-sorted input — the common case, since scans
   * arrive in key order often enough — off the quadratic path.
   */
  #selectBest(count: number): void {
    const entries = this.#selection;
    const worse = (left: RetainedRow, right: RetainedRow): boolean => {
      const comparison = this.#compareKeys(left.keys, right.keys);
      return comparison !== 0 ? comparison > 0 : left.seq > right.seq;
    };
    const swap = (left: number, right: number): void => {
      const held = required(entries[left], "Selection entry is missing");
      entries[left] = required(entries[right], "Selection entry is missing");
      entries[right] = held;
    };
    let low = 0;
    let high = entries.length - 1;
    const target = count - 1;
    while (low < high) {
      // Median of three, moved to the front as the pivot.
      const middle = (low + high) >> 1;
      if (
        worse(
          required(entries[low], "Selection entry is missing"),
          required(entries[middle], "Selection entry is missing"),
        )
      )
        swap(low, middle);
      if (
        worse(
          required(entries[low], "Selection entry is missing"),
          required(entries[high], "Selection entry is missing"),
        )
      )
        swap(low, high);
      if (
        worse(
          required(entries[middle], "Selection entry is missing"),
          required(entries[high], "Selection entry is missing"),
        )
      )
        swap(middle, high);
      swap(low, middle);
      const pivot = required(entries[low], "Selection entry is missing");
      let left = low;
      let right = high + 1;
      for (;;) {
        do left += 1;
        while (left <= high && worse(pivot, required(entries[left], "Selection entry is missing")));
        do right -= 1;
        while (worse(required(entries[right], "Selection entry is missing"), pivot));
        if (left >= right) break;
        swap(left, right);
      }
      swap(low, right);
      if (right === target) return;
      if (right > target) high = right - 1;
      else low = right + 1;
    }
  }

  #evaluateKeys(batch: BatchRows, row: number): void {
    const expressions = required(this.#keyExpressions, "Order keys are missing");
    for (let index = 0; index < expressions.length; index += 1) {
      const expression = required(expressions[index], "Order key is missing");
      this.#keyScratch[index] = asQueryValue(
        evaluateBatchExpression(this.#plan, expression, batch, row),
      );
    }
  }

  #compareKeys(left: readonly QueryValue[], right: readonly QueryValue[]): number {
    const orderBy = this.#plan.orderBy;
    for (let index = 0; index < orderBy.length; index += 1) {
      const order = required(orderBy[index], "Order term is missing");
      const placed = nullOrder(left[index], right[index], order.nulls, order.direction);
      if (placed !== undefined && placed !== 0) return placed;
      const comparison = compareValues(left[index], right[index]);
      if (comparison !== 0) return order.direction === "desc" ? -comparison : comparison;
    }
    return 0;
  }
}

/**
 * Insertion-ordered group-state store. A single bare string-column GROUP BY resolves each row
 * through a slot table indexed by the column's dictionary code — no key bytes, no hashing —
 * while every other shape goes through the byte-keyed group index. Both modes surface states in
 * first-seen order, matching the row oracle's grouping order.
 */
class GroupAccumulator {
  readonly #plan: BoundPlan;
  readonly #memory: QueryMemoryContext;
  readonly #index: ByteGroupIndex<GroupState>;
  #codeStates: Array<GroupState | undefined> | undefined;
  /** The dictionary #codeStates is laid out for; a streamed window slide swaps it. */
  #codeDictionary: readonly string[] | undefined;
  /** Value-keyed group states so a new window's dictionary remaps to existing groups. */
  readonly #codeStateByValue = new Map<string, GroupState>();
  #nullCodeState: GroupState | undefined;
  #codeSlotsReserved = 0;
  #ordered: GroupState[] | undefined;
  readonly #codeColumns: ReadonlyArray<{ source: number; vector: StringVector } | undefined>;
  #multiCodeColumns: ReadonlyArray<{ source: number; vector: StringVector }> | undefined;
  #multiCodeStates: Map<number, GroupState> | undefined;
  readonly #multiCodeScratch: number[] = [];
  readonly #keyScratch: GroupIndexKey[] = [];
  // The miss factories live on the accumulator and read the pending row through these fields, so
  // the per-row lookup never allocates a capturing closure; execution is synchronous, so the
  // pending row cannot change while a factory runs.
  #pendingBatch: BatchRows | undefined;
  #pendingRow = 0;
  #pendingSingleValue: QueryValue = null;
  readonly #createPendingSingle = (): GroupState =>
    createGroupState([this.#pendingSingleValue], this.#plan, this.#memory);
  readonly #createPendingCompound = (): GroupState =>
    createGroupState(
      this.#plan.groupBy.map((expression) =>
        asQueryValue(
          evaluateBatchExpression(
            this.#plan,
            expression,
            required(this.#pendingBatch, "Pending group batch is missing"),
            this.#pendingRow,
          ),
        ),
      ),
      this.#plan,
      this.#memory,
    );

  readonly #fastAggregatesCache: Array<{ kind: "star" | "column"; sums: boolean }> | undefined;

  constructor(plan: BoundPlan, memory: QueryMemoryContext) {
    this.#plan = plan;
    this.#memory = memory;
    this.#index = new ByteGroupIndex<GroupState>(memory);
    this.#fastAggregatesCache = plan.grouped ? this.#fastAggregates() : undefined;
    if (plan.grouped && plan.groupBy.length === 0) {
      this.#index.setEmpty(createGroupState([], plan, memory));
    }
    if (plan.codeGrouping !== undefined) {
      const slots = plan.codeGrouping.vector.dictionary.length + 1;
      memory.reserve(
        safeMemoryProduct(slots, QUERY_REFERENCE_BYTES, "Group code slots"),
        "Group code slots",
      );
      this.#codeStates = new Array<GroupState | undefined>(slots).fill(undefined);
      this.#codeDictionary = plan.codeGrouping.vector.dictionary;
      this.#codeSlotsReserved = slots;
      this.#ordered = [];
    }
    // Compound keys substitute the dictionary code for each bare unwindowed string column: codes
    // are stable and value-unique within one execution, so the key encodes a fixed-width number
    // instead of re-encoding the string's UTF-8 on every row. Types are stable per position, so
    // a code can never collide with a genuine number from the same expression.
    this.#codeColumns =
      plan.groupBy.length > 1
        ? plan.groupBy.map((expression) =>
            expression.kind === "column" &&
            expression.vector.kind === "string" &&
            expression.vector.window === undefined
              ? { source: expression.source, vector: expression.vector }
              : undefined,
          )
        : [];
    // When every compound key column is dictionary-coded and the combined code space is small,
    // group lookup packs codes into one exact integer. Small domains use a direct array; large,
    // sparse domains use a numeric Map instead of byte-encoding and hashing each compound key.
    // Each column contributes (dictionary size + 1) slots, the extra one for NULL.
    if (this.#codeColumns.length > 1 && this.#codeColumns.every((column) => column !== undefined)) {
      const columns = this.#codeColumns as ReadonlyArray<{ source: number; vector: StringVector }>;
      let slots = 1;
      for (const column of columns) slots *= column.vector.dictionary.length + 1;
      if (Number.isSafeInteger(slots) && slots <= MULTI_CODE_GROUP_SLOT_CAP) {
        try {
          memory.reserve(
            safeMemoryProduct(slots, QUERY_REFERENCE_BYTES, "Group code slots"),
            "Group code slots",
          );
        } catch (error) {
          if (!(error instanceof QueryMemoryBudgetError)) throw error;
          return;
        }
        this.#multiCodeColumns = columns;
        this.#codeStates = new Array<GroupState | undefined>(slots).fill(undefined);
        this.#ordered = [];
      } else if (Number.isSafeInteger(slots)) {
        this.#multiCodeColumns = columns;
        this.#multiCodeStates = new Map<number, GroupState>();
        this.#ordered = [];
      }
    }
  }

  /**
   * Fast aggregate specs for the batch kernel: every aggregate is COUNT(*) or a bare numeric
   * column under COUNT/SUM/AVG. MIN/MAX keep the generic path for its comparison and memory
   * accounting semantics. Undefined when any aggregate needs the generic path.
   */
  #fastAggregates(): Array<{ kind: "star" | "column"; sums: boolean }> | undefined {
    const specs: Array<{ kind: "star" | "column"; sums: boolean }> = [];
    for (const spec of this.#plan.aggregates) {
      // The kernel counts every row it is given; deduplication needs the per-row path.
      if (spec.distinct === true) return undefined;
      if (spec.argument.kind === "wildcard" && spec.name === "COUNT") {
        specs.push({ kind: "star", sums: false });
        continue;
      }
      if (
        spec.rawNumber !== undefined &&
        (spec.name === "COUNT" || spec.name === "SUM" || spec.name === "AVG")
      ) {
        specs.push({ kind: "column", sums: spec.name !== "COUNT" });
        continue;
      }
      return undefined;
    }
    return specs;
  }

  /**
   * The batch kernel for dictionary-coded single-column grouping (or the global group) over
   * COUNT/SUM/AVG aggregates: one pass with unboxed reads and no per-row dispatch. Returns false
   * when the plan shape needs the generic per-row path.
   */
  consumeFast(batch: BatchRows): boolean {
    const plan = this.#plan;
    const codeGrouping = plan.codeGrouping;
    const globalGroup = plan.groupBy.length === 0;
    if (!globalGroup && (codeGrouping === undefined || this.#codeStates === undefined)) {
      return false;
    }
    const specs = this.#fastAggregatesCache;
    if (specs === undefined) return false;
    // The purest shape — global COUNT(*) with no predicates — needs no row loop at all.
    if (globalGroup && specs.every((spec) => spec.kind === "star")) {
      const state = required(this.#index.getEmpty(), "Grouped query state is missing");
      for (let index = 0; index < specs.length; index += 1) {
        state.counts[index] = (state.counts[index] ?? 0) + batch.length;
      }
      return true;
    }
    const groupRows =
      codeGrouping === undefined ? undefined : batch.rowsBySource[codeGrouping.source];
    const groupVector = codeGrouping?.vector;
    if (groupVector !== undefined) this.#ensureCodeStates(groupVector.dictionary);
    const states = this.#codeStates;
    const nullCode = groupVector?.dictionary.length ?? 0;
    const globalState = globalGroup
      ? required(this.#index.getEmpty(), "Grouped query state is missing")
      : undefined;
    // Hoisted per-column reads: the row loop touches only local typed arrays and numbers.
    const columns: Array<{
      readonly index: number;
      readonly sums: boolean;
      readonly rows: Int32Array | undefined;
      readonly values: Float64Array;
      readonly validity: Uint8Array;
      readonly windowStart: number;
      readonly length: number;
      readonly slots: number;
    }> = [];
    let stars = 0;
    for (let index = 0; index < specs.length; index += 1) {
      const spec = specs[index];
      if (spec === undefined || spec.kind === "star") {
        stars += 1;
        continue;
      }
      const raw = plan.aggregates[index]?.rawNumber;
      if (raw === undefined) return false;
      columns.push({
        index,
        sums: spec.sums,
        rows: batch.rowsBySource[raw.source],
        values: raw.vector.values,
        validity: raw.vector.validity,
        windowStart: raw.vector.window?.start ?? 0,
        length: raw.vector.length,
        slots: raw.vector.values.length,
      });
    }
    const grouping =
      groupVector === undefined
        ? undefined
        : {
            codes: groupVector.codes,
            validity: groupVector.validity,
            length: groupVector.length,
            dictionary: groupVector.dictionary,
            windowStart: groupVector.window?.start ?? 0,
            slots: groupVector.codes.length,
          };
    for (let row = 0; row < batch.length; row += 1) {
      let state = globalState;
      if (state === undefined && grouping !== undefined && states !== undefined) {
        const sourceRow = groupRows?.[row] ?? -1;
        let code = nullCode;
        if (sourceRow >= 0 && sourceRow < grouping.length) {
          const slot = sourceRow - grouping.windowStart;
          if (slot < 0 || slot >= grouping.slots) {
            throw new RangeError("Streamed vector row is outside the resident window");
          }
          if (isValid(grouping.validity, slot)) {
            const rawCode = grouping.codes[slot] ?? NULL_STRING_CODE;
            if (rawCode !== NULL_STRING_CODE) code = rawCode;
          }
        }
        state = states[code];
        if (state === undefined) {
          const value = code === nullCode ? null : (grouping.dictionary[code] ?? null);
          state = createGroupState([value], plan, this.#memory);
          states[code] = state;
          this.#registerCodeState(value, state);
          this.#ordered?.push(state);
        }
      }
      if (state === undefined) return false;
      const counts = state.counts;
      if (stars > 0) {
        for (let index = 0; index < specs.length; index += 1) {
          if (specs[index]?.kind === "star") counts[index] = (counts[index] ?? 0) + 1;
        }
      }
      for (const column of columns) {
        const sourceRow = column.rows?.[row] ?? -1;
        if (sourceRow < 0 || sourceRow >= column.length) continue;
        const slot = sourceRow - column.windowStart;
        if (slot < 0 || slot >= column.slots) {
          throw new RangeError("Streamed vector row is outside the resident window");
        }
        if (!isValid(column.validity, slot)) continue;
        counts[column.index] = (counts[column.index] ?? 0) + 1;
        if (column.sums) {
          state.sums[column.index] = (state.sums[column.index] ?? 0) + (column.values[slot] ?? 0);
        }
      }
    }
    return true;
  }

  get fastAggregatesCache(): Array<{ kind: "star" | "column"; sums: boolean }> | undefined {
    return this.#fastAggregatesCache;
  }

  /**
   * Re-lays the code-slot table for a new window dictionary. Existing groups carry over by
   * value, so a group's state is shared across every window that mentions its value; the cost
   * is one map lookup per distinct value per window, never per row. Only used for the
   * single-column code grouping — compound code keys stay unwindowed.
   */
  #ensureCodeStates(dictionary: readonly string[]): void {
    if (this.#codeDictionary === dictionary || this.#codeStates === undefined) return;
    const slots = dictionary.length + 1;
    if (slots > this.#codeSlotsReserved) {
      this.#memory.reserve(
        safeMemoryProduct(
          slots - this.#codeSlotsReserved,
          QUERY_REFERENCE_BYTES,
          "Group code slots",
        ),
        "Group code slots",
      );
      this.#codeSlotsReserved = slots;
    }
    const next = new Array<GroupState | undefined>(slots).fill(undefined);
    for (let code = 0; code < dictionary.length; code += 1) {
      const state = this.#codeStateByValue.get(dictionary[code] ?? "");
      if (state !== undefined) next[code] = state;
    }
    next[dictionary.length] = this.#nullCodeState;
    this.#codeStates = next;
    this.#codeDictionary = dictionary;
  }

  /** Registers a freshly created code-grouped state so later windows can find it by value. */
  #registerCodeState(value: string | null, state: GroupState): void {
    if (value === null) this.#nullCodeState = state;
    else this.#codeStateByValue.set(value, state);
  }

  /** Resolves the group state for one row, creating it on first touch. */
  stateFor(batch: BatchRows, row: number): GroupState {
    const plan = this.#plan;
    const codeGrouping = plan.codeGrouping;
    if (codeGrouping !== undefined && this.#codeStates !== undefined) {
      const sourceRow = batch.rowsBySource[codeGrouping.source]?.[row] ?? -1;
      const vector = codeGrouping.vector;
      this.#ensureCodeStates(vector.dictionary);
      const states = required(this.#codeStates, "Group code slots are missing");
      let code = vector.dictionary.length;
      if (sourceRow >= 0 && sourceRow < vector.length) {
        const windowStart = vector.window?.start ?? 0;
        const slot = sourceRow - windowStart;
        if (slot < 0 || slot >= vector.codes.length) {
          throw new RangeError("Streamed vector row is outside the resident window");
        }
        if (isValid(vector.validity, slot)) {
          const rawCode = vector.codes[slot] ?? NULL_STRING_CODE;
          if (rawCode !== NULL_STRING_CODE) code = rawCode;
        }
      }
      let state = states[code];
      if (state === undefined) {
        const value = code === vector.dictionary.length ? null : (vector.dictionary[code] ?? null);
        state = createGroupState([value], plan, this.#memory);
        states[code] = state;
        this.#registerCodeState(value, state);
        this.#ordered?.push(state);
      }
      return state;
    }
    if (plan.groupBy.length === 0) {
      return required(this.#index.getEmpty(), "Grouped query state is missing");
    }
    const multiCode = this.#multiCodeColumns;
    if (
      multiCode !== undefined &&
      (this.#codeStates !== undefined || this.#multiCodeStates !== undefined)
    ) {
      let slot = 0;
      for (let index = 0; index < multiCode.length; index += 1) {
        const column = required(multiCode[index], "Group code column is missing");
        const vector = column.vector;
        const sourceRow = batch.rowsBySource[column.source]?.[row] ?? -1;
        let code = vector.dictionary.length;
        if (sourceRow >= 0 && sourceRow < vector.length && isValid(vector.validity, sourceRow)) {
          const rawCode = vector.codes[sourceRow] ?? NULL_STRING_CODE;
          if (rawCode !== NULL_STRING_CODE) code = rawCode;
        }
        this.#multiCodeScratch[index] = code;
        slot = slot * (vector.dictionary.length + 1) + code;
      }
      let state = this.#codeStates?.[slot] ?? this.#multiCodeStates?.get(slot);
      if (state === undefined) {
        const groupValues: QueryValue[] = [];
        for (let index = 0; index < multiCode.length; index += 1) {
          const vector = required(multiCode[index], "Group code column is missing").vector;
          const code = this.#multiCodeScratch[index] ?? vector.dictionary.length;
          groupValues.push(
            code === vector.dictionary.length ? null : (vector.dictionary[code] ?? null),
          );
        }
        state = createGroupState(groupValues, plan, this.#memory);
        if (this.#codeStates !== undefined) this.#codeStates[slot] = state;
        else {
          this.#memory.tally(PACKED_GROUP_ENTRY_BYTES, "Packed group index entry");
          this.#multiCodeStates?.set(slot, state);
        }
        this.#ordered?.push(state);
      }
      return state;
    }
    if (plan.groupBy.length === 1) {
      const groupValue = asQueryValue(
        evaluateBatchExpression(
          plan,
          required(plan.groupBy[0], "Group expression is missing"),
          batch,
          row,
        ),
      );
      this.#pendingSingleValue = groupValue;
      return this.#index.getOrInsertOne(groupKey(groupValue), this.#createPendingSingle);
    }
    // The scratch array carries this row's keys without a per-row allocation; the create callback
    // re-evaluates the group expressions, which runs once per distinct group.
    for (let index = 0; index < plan.groupBy.length; index += 1) {
      const codeColumn = this.#codeColumns[index];
      if (codeColumn !== undefined) {
        const sourceRow = batch.rowsBySource[codeColumn.source]?.[row] ?? -1;
        let key: GroupIndexKey = null;
        if (
          sourceRow >= 0 &&
          sourceRow < codeColumn.vector.length &&
          isValid(codeColumn.vector.validity, sourceRow)
        ) {
          const rawCode = codeColumn.vector.codes[sourceRow] ?? NULL_STRING_CODE;
          if (rawCode !== NULL_STRING_CODE) key = rawCode;
        }
        this.#keyScratch[index] = key;
        continue;
      }
      const expression = required(plan.groupBy[index], "Group expression is missing");
      this.#keyScratch[index] = groupKey(
        asQueryValue(evaluateBatchExpression(plan, expression, batch, row)),
      );
    }
    this.#keyScratch.length = plan.groupBy.length;
    this.#pendingBatch = batch;
    this.#pendingRow = row;
    return this.#index.getOrInsert(this.#keyScratch, this.#createPendingCompound);
  }

  values(): readonly GroupState[] {
    return this.#ordered ?? this.#index.values();
  }
}

function consumeBatch(
  plan: BoundPlan,
  batch: BatchRows,
  groups: GroupAccumulator,
  output: ResultSink,
  memory: QueryMemoryContext,
  prefiltered = false,
): void {
  const checkPredicates = plan.predicates.length > 0 && !prefiltered;
  if (checkPredicates) {
    // Selection-first: unboxed predicate loops compact the batch before any per-row work.
    const { selection, survivors } = filterScanBatch(plan, batch);
    if (survivors === 0) return;
    if (plan.grouped) {
      // Group over the compacted survivors so the unboxed aggregate kernel still applies.
      const working =
        survivors < batch.length
          ? materializeSelection(batch, selection, survivors, memory)
          : batch;
      try {
        consumeBatch(plan, working, groups, output, memory, true);
      } finally {
        if (working !== batch) working.memory?.close();
      }
      return;
    }
    for (let index = 0; index < survivors; index += 1) {
      output.add(batch, selection[index] ?? 0);
      if (reachedEarlyLimit(plan, output.size)) return;
    }
    return;
  }
  if (plan.grouped && groups.consumeFast(batch)) return;
  if (!plan.grouped && output.tryAddBatch(batch)) return;
  for (let row = 0; row < batch.length; row += 1) {
    if (plan.grouped) {
      updateAggregates(plan, groups.stateFor(batch, row), batch, row, memory);
    } else {
      output.add(batch, row);
      if (reachedEarlyLimit(plan, output.size)) return;
    }
  }
}

/**
 * Shared accumulator arrays for groups in a plan with no aggregates — a `GROUP BY` that only
 * produces its keys, which is exactly the inner block `COUNT(DISTINCT x)` desugars into. That
 * block makes one group per distinct (key, x) pair, so allocating five per-group accumulators
 * that nothing can ever write costs five allocations per distinct value. Every writer is bounded
 * by the aggregate count, so with none there is nothing to write; the plain arrays are frozen so
 * a future writer that ignores that bound fails loudly instead of corrupting every group.
 */
const EMPTY_ACCUMULATOR = new Float64Array(0);
const EMPTY_VALUES = Object.freeze([]) as unknown as Array<QueryValue | undefined>;
const EMPTY_RESERVATIONS = Object.freeze([]) as unknown as Array<
  QueryMemoryReservation | undefined
>;

function createGroupState(
  groupValues: QueryValue[],
  plan: BoundPlan,
  memory: QueryMemoryContext,
): GroupState {
  let payloadBytes = QUERY_REFERENCE_BYTES;
  for (const value of groupValues) {
    payloadBytes = safeMemorySum(payloadBytes, queryValuePayloadBytes(value), "Group state");
  }
  payloadBytes = safeMemorySum(
    payloadBytes,
    safeMemoryProduct(
      plan.aggregates.length,
      AGGREGATE_ACCUMULATOR_BYTES,
      "Aggregate accumulator state",
    ),
    "Group state",
  );
  if (plan.hasListAggregate) {
    payloadBytes = safeMemorySum(
      payloadBytes,
      safeMemoryProduct(plan.aggregates.length, QUERY_REFERENCE_BYTES, "JSON aggregate list slots"),
      "Group state",
    );
  }
  // tally, not reserve: a group state lives until the context closes and is never released
  // on its own, so a per-group QueryMemoryReservation object — retained in the context's Set
  // for the whole query — is pure overhead at one per distinct group.
  memory.tally(payloadBytes, "Group state");
  if (plan.aggregates.length === 0) {
    return {
      groupValues,
      counts: EMPTY_ACCUMULATOR,
      sums: EMPTY_ACCUMULATOR,
      values: EMPTY_VALUES,
      valueReservations: EMPTY_RESERVATIONS,
      valueReservationBytes: EMPTY_ACCUMULATOR,
      distincts: undefined,
    };
  }
  // Only DISTINCT slots get a set, and only when the plan has one at all — an ordinary grouped
  // query allocates nothing extra for a feature it does not use.
  let distincts: Array<Set<unknown> | undefined> | undefined;
  for (let index = 0; index < plan.aggregates.length; index += 1) {
    if (plan.aggregates[index]?.distinct !== true) continue;
    distincts ??= new Array<Set<unknown> | undefined>(plan.aggregates.length);
    distincts[index] = new Set<unknown>();
  }
  return {
    groupValues,
    counts: new Float64Array(plan.aggregates.length),
    sums: new Float64Array(plan.aggregates.length),
    values: new Array<QueryValue | undefined>(plan.aggregates.length),
    ...(plan.hasListAggregate
      ? { lists: new Array<QueryValue[] | undefined>(plan.aggregates.length) }
      : {}),
    valueReservations: new Array<QueryMemoryReservation | undefined>(plan.aggregates.length),
    valueReservationBytes: new Float64Array(plan.aggregates.length),
    distincts,
  };
}

/**
 * A Set member standing for one aggregate input. Primitives are their own key — a Set separates
 * `1` from `"1"` on its own — while a Date has to become its instant, since two Dates for the
 * same moment are different objects. The NUL prefix keeps that instant from colliding with a
 * string a row genuinely holds.
 */
function distinctKey(value: unknown): unknown {
  return value instanceof Date ? `\0d${String(value.getTime())}` : value;
}

/**
 * Whether this value is the first of its kind for the slot, folding it into the set when it is.
 * Growth is tallied rather than reserved for the same reason group state is: the set lives until
 * the query's memory context closes, so a per-value reservation object would cost more than the
 * value it tracks.
 */
function firstOfItsKind(
  state: GroupState,
  index: number,
  value: unknown,
  memory: QueryMemoryContext,
): boolean {
  const seen = state.distincts?.[index];
  if (seen === undefined) return true;
  const key = distinctKey(value);
  if (seen.has(key)) return false;
  seen.add(key);
  memory.tally(
    safeMemorySum(
      QUERY_REFERENCE_BYTES,
      queryValuePayloadBytes(asQueryValue(key)),
      "Distinct aggregate value",
    ),
    "Distinct aggregate value",
  );
  return true;
}

function updateAggregates(
  plan: BoundPlan,
  state: GroupState,
  batch: BatchRows,
  row: number,
  memory: QueryMemoryContext,
): void {
  for (let index = 0; index < plan.aggregates.length; index += 1) {
    const spec = required(plan.aggregates[index], "Aggregate specification is missing");
    // MIN/MAX/COUNT over a bare datetime column track raw epoch milliseconds: boxing a Date per
    // row only to unbox it in the comparison would dominate the scan. The final read re-boxes
    // the single surviving value.
    if (spec.rawDatetime !== undefined) {
      const sourceRow = batch.rowsBySource[spec.rawDatetime.source]?.[row] ?? -1;
      applyAggregateValue(
        spec,
        state,
        index,
        rawFloat64Value(spec.rawDatetime.vector, sourceRow),
        memory,
      );
      continue;
    }
    // A bare number column reads its Float64Array slot directly and accumulates SUM/AVG into
    // the typed sums array, so the per-row value never crosses an interpreter dispatch and the
    // common accumulation path stays unboxed. MIN/MAX keep applyAggregateValue's comparison
    // semantics (including NaN ordering) through compareValues.
    // A DISTINCT slot always takes the generic path below: the unboxed branch writes straight
    // into the accumulators, with no place to ask whether this value has been seen before.
    if (spec.rawNumber !== undefined && spec.distinct !== true) {
      const sourceRow = batch.rowsBySource[spec.rawNumber.source]?.[row] ?? -1;
      const value = rawFloat64Value(spec.rawNumber.vector, sourceRow);
      if (value !== null) {
        state.counts[index] = (state.counts[index] ?? 0) + 1;
        if (spec.name === "SUM" || spec.name === "AVG") {
          state.sums[index] = (state.sums[index] ?? 0) + value;
        } else if (spec.name === "MIN") {
          const current = state.values[index];
          if (current === undefined || compareValues(value, current) < 0) {
            replaceAggregateValue(state, index, value, "MIN aggregate value", memory);
          }
        } else if (spec.name === "MAX") {
          const current = state.values[index];
          if (current === undefined || compareValues(value, current) > 0) {
            replaceAggregateValue(state, index, value, "MAX aggregate value", memory);
          }
        }
      }
      continue;
    }
    const value =
      spec.argument.kind === "wildcard"
        ? 1
        : evaluateBatchExpression(plan, spec.argument, batch, row);
    const delimiter =
      spec.delimiter === undefined
        ? undefined
        : evaluateBatchExpression(plan, spec.delimiter, batch, row);
    const orderValues = (spec.orderBy ?? []).map((order) =>
      asQueryValue(evaluateBatchExpression(plan, order.expression, batch, row)),
    );
    applyAggregateValue(spec, state, index, value, memory, delimiter, orderValues);
  }
}

/** Reads a float64 slot (number value or raw epoch milliseconds) unboxed, or null when invalid. */
function rawFloat64Value(vector: NumberVector | DateTimeVector, rowIndex: number): number | null {
  if (rowIndex < 0 || rowIndex >= vector.length) return null;
  const window = vector.window;
  let slot = rowIndex;
  if (window !== undefined) {
    slot = rowIndex - window.start;
    if (slot < 0 || slot >= window.length) {
      throw new RangeError("Streamed vector row is outside the resident window");
    }
  }
  if (!isValid(vector.validity, slot)) return null;
  return vector.values[slot] ?? 0;
}

function updateAggregatesFromValues(
  plan: BoundPlan,
  state: GroupState,
  values: readonly unknown[],
  memory: QueryMemoryContext,
): void {
  for (let index = 0; index < plan.aggregates.length; index += 1) {
    const spec = required(plan.aggregates[index], "Aggregate specification is missing");
    const value = values[index];
    if (spec.name === "STRING_AGG" && typeof value === "string") {
      const decoded: unknown = JSON.parse(value);
      if (!Array.isArray(decoded) || decoded.length !== 3) {
        throw new Error("Spilled STRING_AGG input is invalid");
      }
      const encodedOrder: unknown = decoded[2];
      if (!Array.isArray(encodedOrder)) throw new Error("Spilled STRING_AGG order is invalid");
      applyAggregateValue(
        spec,
        state,
        index,
        decoded[0],
        memory,
        decoded[1],
        encodedOrder.map(decodeSpillValue),
      );
    } else {
      applyAggregateValue(spec, state, index, value, memory);
    }
  }
}

function applyAggregateValue(
  spec: AggregateSpec,
  state: GroupState,
  index: number,
  value: unknown,
  memory: QueryMemoryContext,
  delimiter?: unknown,
  orderValues: readonly QueryValue[] = [],
): void {
  if (spec.name === "JSON_ARRAYAGG") {
    const member = asQueryValue(value ?? null);
    if (spec.distinct === true && !firstOfItsKind(state, index, member, memory)) return;
    state.counts[index] = (state.counts[index] ?? 0) + 1;
    const lists = required(state.lists, "JSON aggregate list state is missing");
    let list = lists[index];
    if (list === undefined) {
      list = [];
      lists[index] = list;
    }
    list.push(member);
    memory.tally(
      safeMemorySum(QUERY_REFERENCE_BYTES, queryValuePayloadBytes(member), "JSON_ARRAYAGG member"),
      "JSON_ARRAYAGG member",
    );
    return;
  }
  if (spec.name === "STRING_AGG") {
    if (value === null || value === undefined) return;
    value = externalSqlDomainValue(value);
    delimiter = externalSqlDomainValue(delimiter);
    if (typeof value !== "string") throw new TypeError("STRING_AGG value must be a string");
    if (delimiter !== null && delimiter !== undefined && typeof delimiter !== "string") {
      throw new TypeError("STRING_AGG delimiter must be a string");
    }
    const distinctMember = JSON.stringify([value, delimiter ?? ""]);
    if (spec.distinct === true && !firstOfItsKind(state, index, distinctMember, memory)) return;
    const member = JSON.stringify([value, delimiter ?? "", orderValues.map(encodeSpillValue)]);
    state.counts[index] = (state.counts[index] ?? 0) + 1;
    const lists = required(state.lists, "STRING_AGG list state is missing");
    let list = lists[index];
    if (list === undefined) {
      list = [];
      lists[index] = list;
    }
    list.push(member);
    memory.tally(
      safeMemorySum(QUERY_REFERENCE_BYTES, queryValuePayloadBytes(member), "STRING_AGG member"),
      "STRING_AGG member",
    );
    return;
  }
  if (value === null || value === undefined) return;
  // One gate for every path that accumulates: the generic per-row path, the raw datetime path,
  // and the re-accumulation of spilled rows.
  if (spec.distinct === true && !firstOfItsKind(state, index, value, memory)) return;
  state.counts[index] = (state.counts[index] ?? 0) + 1;
  if (spec.name === "SUM" || spec.name === "AVG") {
    if (isExactNumeric(value)) {
      const current = state.values[index];
      const sum = current === undefined ? value : exactNumericBinary("+", current, value);
      if (sum === null || sum === undefined) throw new Error("Exact NUMERIC sum disappeared");
      replaceAggregateValue(state, index, sum, `${spec.name} aggregate value`, memory);
    } else {
      state.sums[index] = (state.sums[index] ?? 0) + numeric(value);
    }
  } else if (
    spec.name === "MIN" &&
    (state.values[index] === undefined || compareValues(value, state.values[index]) < 0)
  ) {
    replaceAggregateValue(state, index, asQueryValue(value), "MIN aggregate value", memory);
  } else if (
    spec.name === "MAX" &&
    (state.values[index] === undefined || compareValues(value, state.values[index]) > 0)
  ) {
    replaceAggregateValue(state, index, asQueryValue(value), "MAX aggregate value", memory);
  }
}

/**
 * Installs a MIN/MAX replacement value. The retained reservation only changes when the payload
 * size does: monotone inputs replace the extreme on nearly every row, and fixed-width values
 * (numbers, datetimes) would otherwise churn a reserve/release pair per row for the same bytes.
 */
function replaceAggregateValue(
  state: GroupState,
  index: number,
  value: QueryValue,
  label: string,
  memory: QueryMemoryContext,
): void {
  const bytes = queryValuePayloadBytes(value);
  if (bytes !== state.valueReservationBytes[index]) {
    const replacement = memory.reserve(bytes, label);
    state.valueReservations[index]?.release();
    state.valueReservations[index] = replacement;
    state.valueReservationBytes[index] = bytes;
  }
  state.values[index] = value;
}

function finishGroups(
  plan: BoundPlan,
  groups: readonly GroupState[],
  memory: QueryMemoryContext,
): QueryRow[] {
  const rows: QueryRow[] = [];
  for (const group of groups) {
    if (
      !plan.having.every((predicate) =>
        predicateTruth(predicate, (nested) => evaluateFinalExpression(plan, nested, group)),
      )
    ) {
      continue;
    }
    const row: QueryRow = {};
    for (const item of plan.select) {
      row[item.alias] = asQueryValue(evaluateFinalExpression(plan, item.expression, group));
    }
    memory.tally(queryRowPayloadBytes(row), "Accumulated grouped result row");
    rows.push(row);
  }
  return rows;
}

function evaluateFinalExpression(
  plan: BoundPlan,
  expression: BoundExpression,
  group: GroupState,
): unknown {
  const groupIndex = plan.groupIndexBySignature.get(expression.signature);
  if (groupIndex !== undefined) return group.groupValues[groupIndex] ?? null;
  if (expression.kind === "literal") return expression.value;
  if (expression.kind === "wildcard") return 1;
  if (expression.kind === "list") throw new TypeError("Value lists are only supported with IN");
  if (
    expression.kind === "condition" ||
    expression.kind === "logical" ||
    expression.kind === "not"
  ) {
    return booleanTruth(expression, (nested) => evaluateFinalExpression(plan, nested, group));
  }
  if (expression.kind === "case") {
    for (const branch of expression.branches) {
      const matched = booleanTruth(branch.when, (nested) =>
        evaluateFinalExpression(plan, nested, group),
      );
      if (matched === true) return evaluateFinalExpression(plan, branch.then, group);
    }
    return expression.otherwise === undefined
      ? null
      : evaluateFinalExpression(plan, expression.otherwise, group);
  }
  if (expression.kind === "column") {
    throw new TypeError("Selected column must appear in GROUP BY");
  }
  if (expression.kind === "fts") {
    throw new TypeError("Selected full-text expression must appear in GROUP BY");
  }
  if (expression.kind === "binary") {
    return binaryValue(
      expression.operator,
      evaluateFinalExpression(plan, expression.left, group),
      evaluateFinalExpression(plan, expression.right, group),
    );
  }
  if (expression.name === "COALESCE") {
    for (const argument of expression.arguments) {
      const candidate = evaluateFinalExpression(plan, argument, group);
      if (candidate !== null && candidate !== undefined) return candidate;
    }
    return null;
  }
  if (isScalarFunctionName(expression.name)) {
    return scalarFunctionValue(
      expression.name,
      expression.arguments.map((argument) => evaluateFinalExpression(plan, argument, group)),
    );
  }
  const aggregateIndex = expression.aggregateIndex ?? -1;
  const count = group.counts[aggregateIndex];
  if (count === undefined) throw new Error("Aggregate state is missing");
  if (expression.name === "COUNT") return count;
  if (count === 0) return null;
  if (expression.name === "JSON_ARRAYAGG") {
    return JSON.stringify(
      (required(group.lists, "JSON aggregate list state is missing")[aggregateIndex] ?? []).map(
        jsonValueOf,
      ),
    );
  }
  if (expression.name === "STRING_AGG") {
    const members = required(group.lists, "STRING_AGG list state is missing")[aggregateIndex] ?? [];
    const decoded = members.map((encoded) => {
      if (typeof encoded !== "string") throw new Error("STRING_AGG member is invalid");
      const pair: unknown = JSON.parse(encoded);
      if (
        !Array.isArray(pair) ||
        typeof pair[0] !== "string" ||
        typeof pair[1] !== "string" ||
        !Array.isArray(pair[2])
      ) {
        throw new Error("STRING_AGG member is invalid");
      }
      return {
        value: pair[0],
        delimiter: pair[1],
        order: pair[2].map(decodeSpillValue),
      };
    });
    const orderBy = plan.aggregates[aggregateIndex]?.orderBy ?? [];
    if (orderBy.length > 0) {
      decoded.sort((left, right) => {
        for (const [index, order] of orderBy.entries()) {
          const a = left.order[index];
          const b = right.order[index];
          const placed = nullOrder(a, b, order.nulls, order.direction);
          if (placed !== undefined && placed !== 0) return placed;
          const compared = compareValues(a, b);
          if (compared !== 0) return order.direction === "desc" ? -compared : compared;
        }
        return 0;
      });
    }
    return protectedSqlTextValue(
      decoded
        .map((member, index) => (index === 0 ? member.value : member.delimiter + member.value))
        .join(""),
    );
  }
  const value = group.values[aggregateIndex] ?? null;
  if (expression.name === "SUM")
    return isExactNumeric(value) ? value : (group.sums[aggregateIndex] ?? 0);
  if (expression.name === "AVG") {
    if (isExactNumeric(value)) return exactNumericBinary("/", value, count);
    return (group.sums[aggregateIndex] ?? 0) / count;
  }
  // Raw-millisecond datetime extremes re-box into a Date only here, once per surviving group.
  if (typeof value === "number" && plan.aggregates[aggregateIndex]?.rawDatetime !== undefined) {
    return new Date(value);
  }
  return value;
}

/**
 * Builds one result row by direct property assignment in stable select order: every projected
 * row shares one hidden class and the per-row tuple/array churn of an entries-based build never
 * exists. This runs once per surviving row, so it is the hottest allocation site of a query.
 */
function projectBatchRow(plan: BoundPlan, batch: BatchRows, row: number): QueryRow {
  const result: QueryRow = {};
  if (!plan.wildcard) {
    for (const item of plan.select) {
      const value = asQueryValue(evaluateBatchExpression(plan, item.expression, batch, row));
      if (item.alias === "__proto__") defineSqlResultProperty(result, item.alias, value);
      else result[item.alias] = value;
    }
    return result;
  }
  const multiple = plan.sourceTables.length > 1;
  for (let source = 0; source < plan.sourceTables.length; source += 1) {
    const table = required(plan.sourceTables[source], "Wildcard source table is missing");
    const rowIndex = batch.rowsBySource[source]?.[row] ?? -1;
    const prefix = multiple ? `${plan.sourceAliases[source] ?? ""}.` : "";
    for (const [name, vector] of table.columns) {
      const outputName = multiple ? prefix + name : name;
      const value = vectorValue(vector, rowIndex);
      if (outputName === "__proto__") defineSqlResultProperty(result, outputName, value);
      else result[outputName] = value;
    }
  }
  return result;
}

function wildcardColumnNames(plan: BoundPlan): string[] {
  const multiple = plan.sourceTables.length > 1;
  return plan.sourceTables.flatMap((table, source) =>
    [...table.columns.keys()].map((name) =>
      multiple ? `${plan.sourceAliases[source] ?? ""}.${name}` : name,
    ),
  );
}

/** Detects `stringColumn = 'literal'` (or !=) so batches compare dictionary codes per row. */
function detectDictionaryEquality(predicate: BoundPredicate): DictionaryEquality | undefined {
  if (!["=", "!=", "<>"].includes(predicate.operator)) return undefined;
  const sides = [
    { column: predicate.left, literal: predicate.right },
    { column: predicate.right, literal: predicate.left },
  ];
  for (const { column, literal } of sides) {
    if (
      column.kind === "column" &&
      column.vector.kind === "string" &&
      literal.kind === "literal" &&
      typeof literal.value === "string"
    ) {
      return {
        source: column.source,
        vector: column.vector,
        value: literal.value,
        negated: predicate.operator !== "=",
        cache: { dictionary: undefined, code: -1 },
      };
    }
  }
  return undefined;
}

const primitiveOperators: ReadonlySet<string> = new Set(["=", "!=", "<>", ">", ">=", "<", "<="]);

/** Detects `column <op> literal` over numeric/datetime/boolean columns for unboxed evaluation. */
function detectPrimitiveComparison(predicate: BoundPredicate): PrimitiveComparison | undefined {
  if (!primitiveOperators.has(predicate.operator)) return undefined;
  const comparison = predicate.operator as ComparisonOperator;
  const sides = [
    { column: predicate.left, literal: predicate.right, operator: comparison },
    {
      column: predicate.right,
      literal: predicate.left,
      operator: reverseComparisonOperator(comparison),
    },
  ];
  for (const { column, literal, operator } of sides) {
    if (column.kind !== "column" || literal.kind !== "literal") continue;
    const vector = column.vector;
    const value = literal.value;
    if (vector.kind === "number" && typeof value === "number") {
      return { source: column.source, vector, operator, value };
    }
    if (vector.kind === "datetime" && value instanceof Date) {
      return { source: column.source, vector, operator, value: value.getTime() };
    }
    if (
      vector.kind === "boolean" &&
      typeof value === "boolean" &&
      (operator === "=" || operator === "!=" || operator === "<>")
    ) {
      return { source: column.source, vector, operator, value: value ? 1 : 0 };
    }
  }
  return undefined;
}

/**
 * Detects `column IN (literal, ...)` over a numeric/datetime column. NULL members are dropped:
 * a value can never equal NULL, so they add nothing to the membership test — but a list that
 * is entirely NULL is left to the general path, where `IN (NULL)` keeps its three-valued
 * answer instead of becoming a plain false.
 */
function detectPrimitiveInList(predicate: BoundPredicate): PrimitiveInList | undefined {
  const negated = predicate.operator === "NOT IN";
  if (predicate.operator !== "IN" && !negated) return undefined;
  if (predicate.left.kind !== "column" || predicate.right.kind !== "list") return undefined;
  const vector = predicate.left.vector;
  if (vector.kind !== "number" && vector.kind !== "datetime") return undefined;
  const members = new Set<number>();
  let minimum = Number.POSITIVE_INFINITY;
  let maximum = Number.NEGATIVE_INFINITY;
  for (const item of predicate.right.items) {
    if (item.kind !== "literal") return undefined;
    const value = item.value;
    if (value === null) {
      // A NULL member only ever turns a non-match into unknown, which the filter discards
      // exactly as it discards false -- so IN may ignore it. NOT IN cannot: with a NULL in
      // the list it is never true, and a kernel over the remaining members would keep rows.
      if (negated) return undefined;
      continue;
    }
    if (vector.kind === "number") {
      if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
      members.add(value);
    } else {
      if (!(value instanceof Date)) return undefined;
      members.add(value.getTime());
    }
  }
  if (members.size === 0) return undefined;
  for (const member of members) {
    if (member < minimum) minimum = member;
    if (member > maximum) maximum = member;
  }
  return { source: predicate.left.source, vector, members, minimum, maximum, negated };
}

function reverseComparisonOperator(operator: ComparisonOperator): ComparisonOperator {
  if (operator === ">") return "<";
  if (operator === ">=") return "<=";
  if (operator === "<") return ">";
  if (operator === "<=") return ">=";
  return operator;
}

/** Reads a numeric/datetime/boolean slot as an unboxed float, or null when invalid. */
function rawPrimitiveValue(
  vector: NumberVector | DateTimeVector | BooleanVector,
  rowIndex: number,
): number | null {
  if (rowIndex < 0 || rowIndex >= vector.length) return null;
  const window = vector.window;
  let slot = rowIndex;
  if (window !== undefined) {
    slot = rowIndex - window.start;
    if (slot < 0 || slot >= window.length) {
      throw new RangeError("Streamed vector row is outside the resident window");
    }
  }
  if (!isValid(vector.validity, slot)) return null;
  return vector.values[slot] ?? 0;
}

function primitiveComparisonHolds(primitive: PrimitiveComparison, value: number): boolean {
  switch (primitive.operator) {
    case "=":
      return value === primitive.value;
    case "!=":
    case "<>":
      return value !== primitive.value;
    case ">":
      return value > primitive.value;
    case ">=":
      return value >= primitive.value;
    case "<":
      return value < primitive.value;
    default:
      return value <= primitive.value;
  }
}

/** Detects `stringColumn LIKE 'literal'` (and NOT/ILIKE) for dictionary-level matching. */
function detectDictionaryLike(predicate: BoundPredicate): DictionaryLike | undefined {
  const operator = predicate.operator;
  if (
    operator !== "LIKE" &&
    operator !== "NOT LIKE" &&
    operator !== "ILIKE" &&
    operator !== "NOT ILIKE"
  ) {
    return undefined;
  }
  const column = predicate.left;
  const literal = predicate.right;
  if (
    column.kind !== "column" ||
    column.vector.kind !== "string" ||
    literal.kind !== "literal" ||
    typeof literal.value !== "string"
  ) {
    return undefined;
  }
  return {
    source: column.source,
    vector: column.vector,
    pattern: literal.value,
    caseInsensitive: operator === "ILIKE" || operator === "NOT ILIKE",
    ...(predicate.escape === undefined ? {} : { escape: predicate.escape }),
    negated: operator === "NOT LIKE" || operator === "NOT ILIKE",
    cache: { dictionary: undefined, matches: new Uint8Array(0) },
  };
}

/**
 * Match table for one (dictionary, pattern) pair, cached across queries on the dictionary's
 * identity: repeated LIKE scans over a cached columnar table pay the per-entry match once ever.
 */
const dictionaryLikeCache = new WeakMap<readonly string[], Map<string, Uint8Array>>();

function dictionaryLikeMatches(
  dictionary: readonly string[],
  pattern: string,
  caseInsensitive: boolean,
  escape: string | undefined,
): Uint8Array {
  const externalPattern = externalSqlDomainValue(pattern);
  if (typeof externalPattern !== "string") throw new TypeError("LIKE requires string operands");
  let patterns = dictionaryLikeCache.get(dictionary);
  if (patterns === undefined) {
    patterns = new Map();
    dictionaryLikeCache.set(dictionary, patterns);
  }
  const key = `${caseInsensitive ? "i" : "s"}${escape ?? ""} ${externalPattern}`;
  let matches = patterns.get(key);
  if (matches === undefined) {
    matches = new Uint8Array(dictionary.length);
    for (let index = 0; index < dictionary.length; index += 1) {
      const value = externalSqlDomainValue(dictionary[index] ?? "");
      matches[index] =
        typeof value === "string" && likeMatches(externalPattern, value, caseInsensitive, escape)
          ? 1
          : 0;
    }
    if (patterns.size >= 32) patterns.clear();
    patterns.set(key, matches);
  }
  return matches;
}

function stringCodeAt(vector: StringVector, rowIndex: number): number | undefined {
  if (rowIndex < 0 || rowIndex >= vector.length) return undefined;
  const window = vector.window;
  let slot = rowIndex;
  if (window !== undefined) {
    slot = rowIndex - window.start;
    if (slot < 0 || slot >= window.length) {
      throw new RangeError(
        `Streamed vector row ${String(rowIndex)} is outside the resident window ${String(window.start)}..${String(window.start + window.length)}`,
      );
    }
  }
  if (!isValid(vector.validity, slot)) return undefined;
  const code = vector.codes[slot] ?? NULL_STRING_CODE;
  return code === NULL_STRING_CODE ? undefined : code;
}

/**
 * SQL three-valued logic over bound boolean trees; mirrors evaluateBooleanExpression in query.ts
 * with bound leaves. Only the caller collapses unknown (null) to false.
 */
function booleanTruth(
  expression: BoundExpression,
  evaluateValue: (expression: BoundExpression) => unknown,
): boolean | null {
  if (expression.kind === "logical") {
    const left = booleanTruth(expression.left, evaluateValue);
    if (expression.operator === "and") {
      if (left === false) return false;
      const right = booleanTruth(expression.right, evaluateValue);
      if (right === false) return false;
      return left === null || right === null ? null : true;
    }
    if (left === true) return true;
    const right = booleanTruth(expression.right, evaluateValue);
    if (right === true) return true;
    return left === null || right === null ? null : false;
  }
  if (expression.kind === "not") {
    const value = booleanTruth(expression.operand, evaluateValue);
    return value === null ? null : !value;
  }
  if (expression.kind === "condition") {
    const operator = expression.operator;
    if (operator === "IS TRUE") return booleanTruth(expression.left, evaluateValue);
    if (operator === "IS NULL" || operator === "IS NOT NULL") {
      const value = evaluateValue(expression.left);
      const isNull = value === null || value === undefined;
      return operator === "IS NULL" ? isNull : !isNull;
    }
    if (operator === "IN" || operator === "NOT IN") {
      if (expression.right.kind !== "list") throw new TypeError("IN requires a value list");
      const probe = evaluateValue(expression.left);
      if (probe === null || probe === undefined) return null;
      const membership = cachedListMembership(expression.right, expression.right.items);
      if (membership !== null) {
        if (membership.set.has(comparable(probe))) return operator === "IN";
        if (membership.hasNull) return null;
        return operator === "NOT IN";
      }
      let sawNull = false;
      for (const item of expression.right.items) {
        const value = evaluateValue(item);
        if (value === null || value === undefined) {
          sawNull = true;
          continue;
        }
        if (comparable(value) === comparable(probe)) return operator === "IN";
      }
      if (sawNull) return null;
      return operator === "NOT IN";
    }
    {
      const quantified = parseQuantified(operator);
      if (quantified !== undefined) {
        if (expression.right.kind !== "list") {
          throw new TypeError("ANY/ALL subqueries must be resolved before evaluation");
        }
        return quantifiedComparison(
          quantified.comparison,
          quantified.quantifier,
          evaluateValue(expression.left),
          expression.right.items.map((item) => evaluateValue(item)),
        );
      }
    }
    if (
      operator === "LIKE" ||
      operator === "NOT LIKE" ||
      operator === "ILIKE" ||
      operator === "NOT ILIKE" ||
      operator === "SIMILAR TO" ||
      operator === "NOT SIMILAR TO"
    ) {
      const value = externalSqlDomainValue(evaluateValue(expression.left));
      const pattern = externalSqlDomainValue(evaluateValue(expression.right));
      if (value === null || value === undefined || pattern === null || pattern === undefined) {
        return null;
      }
      if (typeof value !== "string" || typeof pattern !== "string") {
        throw new TypeError("LIKE requires string operands");
      }
      const similar = operator === "SIMILAR TO" || operator === "NOT SIMILAR TO";
      const matched = similar
        ? compileSimilarPattern(pattern, expression.escape ?? "\\").test(value)
        : likeMatches(
            pattern,
            value,
            operator === "ILIKE" || operator === "NOT ILIKE",
            expression.escape,
          );
      return operator === "LIKE" || operator === "ILIKE" || operator === "SIMILAR TO"
        ? matched
        : !matched;
    }
    if (operator === "IS DISTINCT FROM" || operator === "IS NOT DISTINCT FROM") {
      const distinct = distinctFromComparison(
        evaluateValue(expression.left),
        evaluateValue(expression.right),
      );
      return operator === "IS DISTINCT FROM" ? distinct : !distinct;
    }
    const left = evaluateValue(expression.left);
    const right = evaluateValue(expression.right);
    if (left === null || left === undefined || right === null || right === undefined) return null;
    const a = comparable(left);
    const b = comparable(right);
    if (operator === "=") return compareValues(a, b) === 0;
    if (operator === "!=" || operator === "<>") return compareValues(a, b) !== 0;
    const comparison = compareValues(a, b);
    if (operator === ">") return comparison > 0;
    if (operator === ">=") return comparison >= 0;
    if (operator === "<") return comparison < 0;
    return comparison <= 0;
  }
  const value = evaluateValue(expression);
  if (value === null || value === undefined) return null;
  if (typeof value === "boolean") return value;
  throw new TypeError("Boolean conditions require boolean operands");
}

function predicateTruth(
  predicate: {
    left: BoundExpression;
    operator: PredicateOperator;
    right: BoundExpression;
    escape?: string;
  },
  evaluateValue: (expression: BoundExpression) => unknown,
): boolean {
  if (predicate.operator === "IS TRUE") {
    return booleanTruth(predicate.left, evaluateValue) === true;
  }
  return (
    booleanTruth(
      {
        kind: "condition",
        operator: predicate.operator,
        left: predicate.left,
        right: predicate.right,
        ...(predicate.escape === undefined ? {} : { escape: predicate.escape }),
        signature: "",
      },
      evaluateValue,
    ) === true
  );
}

function evaluateBatchPredicate(
  plan: BoundPlan,
  predicate: BoundPredicate,
  batch: BatchRows,
  row: number,
): boolean {
  const fast = predicate.dictionaryEquality;
  if (fast !== undefined) {
    const code = stringCodeAt(fast.vector, batch.rowsBySource[fast.source]?.[row] ?? -1);
    if (code === undefined) return false;
    if (fast.cache.dictionary !== fast.vector.dictionary) {
      fast.cache.dictionary = fast.vector.dictionary;
      fast.cache.code = fast.vector.dictionary.indexOf(fast.value);
    }
    const matches = fast.cache.code >= 0 && code === fast.cache.code;
    return fast.negated ? !matches : matches;
  }
  const primitive = predicate.primitive;
  if (primitive !== undefined) {
    const value = rawPrimitiveValue(
      primitive.vector,
      batch.rowsBySource[primitive.source]?.[row] ?? -1,
    );
    return value === null ? false : primitiveComparisonHolds(primitive, value);
  }
  const like = predicate.dictionaryLike;
  if (like !== undefined) {
    const code = stringCodeAt(like.vector, batch.rowsBySource[like.source]?.[row] ?? -1);
    if (code === undefined) return false;
    if (like.cache.dictionary !== like.vector.dictionary) {
      like.cache.dictionary = like.vector.dictionary;
      like.cache.matches = dictionaryLikeMatches(
        like.vector.dictionary,
        like.pattern,
        like.caseInsensitive,
        like.escape,
      );
    }
    const matched = like.cache.matches[code] === 1;
    return like.negated ? !matched : matched;
  }
  if (
    predicate.operator === "IS TRUE" ||
    predicate.operator === "LIKE" ||
    predicate.operator === "NOT LIKE" ||
    predicate.operator === "ILIKE" ||
    predicate.operator === "NOT ILIKE" ||
    parseQuantified(predicate.operator) !== undefined
  ) {
    return predicateTruth(predicate, (nested) => evaluateBatchExpression(plan, nested, batch, row));
  }
  if (predicate.operator === "IN" || predicate.operator === "NOT IN") {
    if (predicate.right.kind !== "list") throw new TypeError("IN requires a value list");
    const membership = cachedListMembership(predicate.right, predicate.right.items);
    if (membership !== null) {
      const value = evaluateBatchExpression(plan, predicate.left, batch, row);
      if (value === null || value === undefined) return false;
      if (membership.set.has(comparable(value))) return predicate.operator === "IN";
      return predicate.operator === "NOT IN" && !membership.hasNull;
    }
    return inListHolds(
      predicate.operator,
      evaluateBatchExpression(plan, predicate.left, batch, row),
      predicate.right.items.map((item) => evaluateBatchExpression(plan, item, batch, row)),
    );
  }
  return comparisonValue(
    predicate.operator,
    evaluateBatchExpression(plan, predicate.left, batch, row),
    evaluateBatchExpression(plan, predicate.right, batch, row),
  );
}

/**
 * SQL membership semantics: a NULL probe never matches, and NOT IN cannot be satisfied when the
 * list contains NULL because the comparison is unknown rather than false.
 */
function inListHolds(
  operator: "IN" | "NOT IN",
  value: unknown,
  items: readonly unknown[],
): boolean {
  if (value === null || value === undefined) return false;
  let hasNull = false;
  for (const item of items) {
    if (item === null || item === undefined) {
      hasNull = true;
      continue;
    }
    if (comparable(value) === comparable(item)) return operator === "IN";
  }
  return operator === "NOT IN" && !hasNull;
}

function evaluateBatchExpression(
  plan: BoundPlan,
  expression: BoundExpression,
  batch: BatchRows,
  row: number,
): unknown {
  if (expression.kind === "literal") return expression.value;
  if (expression.kind === "wildcard") return 1;
  if (expression.kind === "list") throw new TypeError("Value lists are only supported with IN");
  if (
    expression.kind === "condition" ||
    expression.kind === "logical" ||
    expression.kind === "not"
  ) {
    return booleanTruth(expression, (nested) => evaluateBatchExpression(plan, nested, batch, row));
  }
  if (expression.kind === "case") {
    for (const branch of expression.branches) {
      const matched = booleanTruth(branch.when, (nested) =>
        evaluateBatchExpression(plan, nested, batch, row),
      );
      if (matched === true) return evaluateBatchExpression(plan, branch.then, batch, row);
    }
    return expression.otherwise === undefined
      ? null
      : evaluateBatchExpression(plan, expression.otherwise, batch, row);
  }
  if (expression.kind === "column") {
    return vectorValue(expression.vector, batch.rowsBySource[expression.source]?.[row] ?? -1);
  }
  if (expression.kind === "fts") {
    return expression.op === "match"
      ? ftsBatchTruth(expression, batch, null, row)
      : ftsBm25BatchValue(expression, batch, null, row);
  }
  if (expression.kind === "binary") {
    return binaryValue(
      expression.operator,
      evaluateBatchExpression(plan, expression.left, batch, row),
      evaluateBatchExpression(plan, expression.right, batch, row),
    );
  }
  if (expression.name === "COALESCE") {
    for (const argument of expression.arguments) {
      const candidate = evaluateBatchExpression(plan, argument, batch, row);
      if (candidate !== null && candidate !== undefined) return candidate;
    }
    return null;
  }
  if (!isScalarFunctionName(expression.name))
    throw new TypeError(`${expression.name} requires grouped execution`);
  return scalarFunctionValue(
    expression.name,
    expression.arguments.map((argument) => evaluateBatchExpression(plan, argument, batch, row)),
  );
}

function evaluateExpression(expression: BoundExpression, rowsBySource: Int32Array): unknown {
  if (expression.kind === "literal") return expression.value;
  if (expression.kind === "wildcard") return 1;
  if (expression.kind === "list") throw new TypeError("Value lists are only supported with IN");
  if (
    expression.kind === "condition" ||
    expression.kind === "logical" ||
    expression.kind === "not"
  ) {
    return booleanTruth(expression, (nested) => evaluateExpression(nested, rowsBySource));
  }
  if (expression.kind === "case") {
    for (const branch of expression.branches) {
      const matched = booleanTruth(branch.when, (nested) =>
        evaluateExpression(nested, rowsBySource),
      );
      if (matched === true) return evaluateExpression(branch.then, rowsBySource);
    }
    return expression.otherwise === undefined
      ? null
      : evaluateExpression(expression.otherwise, rowsBySource);
  }
  if (expression.kind === "column") {
    return vectorValue(expression.vector, rowsBySource[expression.source] ?? -1);
  }
  if (expression.kind === "fts") {
    return expression.op === "match"
      ? ftsBatchTruth(expression, null, rowsBySource, 0)
      : ftsBm25BatchValue(expression, null, rowsBySource, 0);
  }
  if (expression.kind === "binary") {
    return binaryValue(
      expression.operator,
      evaluateExpression(expression.left, rowsBySource),
      evaluateExpression(expression.right, rowsBySource),
    );
  }
  if (expression.name === "COALESCE") {
    for (const argument of expression.arguments) {
      const candidate = evaluateExpression(argument, rowsBySource);
      if (candidate !== null && candidate !== undefined) return candidate;
    }
    return null;
  }
  if (!isScalarFunctionName(expression.name))
    throw new TypeError(`${expression.name} requires grouped execution`);
  return scalarFunctionValue(
    expression.name,
    expression.arguments.map((argument) => evaluateExpression(argument, rowsBySource)),
  );
}

function binaryValue(
  operator: BinaryOperator,
  left: unknown,
  right: unknown,
): number | string | null {
  if (left === null || left === undefined || right === null || right === undefined) return null;
  if (operator === "||") {
    if (typeof left !== "string" || typeof right !== "string") {
      throw new TypeError("|| requires string operands");
    }
    return protectedSqlTextValue(
      String(externalSqlDomainValue(left)) + String(externalSqlDomainValue(right)),
    );
  }
  const exact = exactNumericBinary(operator, left, right);
  if (exact !== undefined) return exact;
  const a = numeric(left);
  const b = numeric(right);
  if (operator === "+") return a + b;
  if (operator === "-") return a - b;
  if (operator === "*") return a * b;
  // Division and remainder by zero are NULL, matching SQLite, not Infinity/NaN.
  if (b === 0) return null;
  return operator === "%" ? a % b : a / b;
}

function comparisonValue(
  operator: PredicateOperator,
  leftValue: unknown,
  rightValue: unknown,
): boolean {
  if (operator === "IS NULL") return leftValue === null || leftValue === undefined;
  if (operator === "IS NOT NULL") return leftValue !== null && leftValue !== undefined;
  if (operator === "IN" || operator === "NOT IN") {
    throw new TypeError("IN is only supported in WHERE predicates");
  }
  if (operator === "IS DISTINCT FROM") return distinctFromComparison(leftValue, rightValue);
  if (operator === "IS NOT DISTINCT FROM") return !distinctFromComparison(leftValue, rightValue);
  if (
    operator === "LIKE" ||
    operator === "NOT LIKE" ||
    operator === "ILIKE" ||
    operator === "NOT ILIKE" ||
    operator === "SIMILAR TO" ||
    operator === "NOT SIMILAR TO"
  ) {
    leftValue = externalSqlDomainValue(leftValue);
    rightValue = externalSqlDomainValue(rightValue);
    if (
      leftValue === null ||
      leftValue === undefined ||
      rightValue === null ||
      rightValue === undefined
    ) {
      return false;
    }
    if (typeof leftValue !== "string" || typeof rightValue !== "string") {
      throw new TypeError("LIKE requires string operands");
    }
    const similar = operator === "SIMILAR TO" || operator === "NOT SIMILAR TO";
    const matched = similar
      ? compileSimilarPattern(rightValue).test(leftValue)
      : likeMatches(rightValue, leftValue, operator === "ILIKE" || operator === "NOT ILIKE");
    return operator === "LIKE" || operator === "ILIKE" || operator === "SIMILAR TO"
      ? matched
      : !matched;
  }
  if (
    leftValue === null ||
    leftValue === undefined ||
    rightValue === null ||
    rightValue === undefined
  ) {
    return false;
  }
  const left = comparable(leftValue);
  const right = comparable(rightValue);
  if (operator === "=") return compareValues(left, right) === 0;
  if (operator === "!=" || operator === "<>") return compareValues(left, right) !== 0;
  const comparison = compareValues(left, right);
  if (operator === ">") return comparison > 0;
  if (operator === ">=") return comparison >= 0;
  if (operator === "<") return comparison < 0;
  return comparison <= 0;
}

function comparable(value: unknown): unknown {
  return value instanceof Date ? value.getTime() : value;
}

function groupKey(value: unknown): GroupIndexKey {
  const comparableValue = comparable(value);
  if (typeof comparableValue === "number" && !Number.isFinite(comparableValue)) return null;
  if (
    comparableValue === null ||
    typeof comparableValue === "boolean" ||
    typeof comparableValue === "number" ||
    typeof comparableValue === "string"
  ) {
    return comparableValue;
  }
  throw new TypeError("Group keys must be SQL scalar values");
}

function stableSortRows(rows: QueryRow[], orderBy: BoundPlan["orderBy"]): void {
  // One prepared column per term, extracted once; see sort-keys.ts for the kernel and for why
  // the comparison rather than the merge is what a sort costs.
  const terms = orderBy.map((order) => ({
    column: buildSortKeyColumn(
      rows.length,
      (index) => required(rows[index], "Ordering row is missing")[order.outputName],
    ),
    descending: order.direction === "desc",
    nulls: order.nulls,
  }));
  const indexes = sortKeyIndexes(rows.length, terms);
  const visited = new Uint8Array(rows.length);
  for (let start = 0; start < rows.length; start += 1) {
    if (visited[start] === 1) continue;
    const first = required(rows[start], "Ordering row is missing");
    let output = start;
    let input = indexes[output] ?? start;
    while (input !== start) {
      visited[output] = 1;
      rows[output] = required(rows[input], "Ordering row is missing");
      output = input;
      input = indexes[output] ?? start;
    }
    visited[output] = 1;
    rows[output] = first;
  }
}

function compareValues(left: unknown, right: unknown): number {
  const collated = collatedDomainCompare(left, right);
  if (collated !== undefined) return collated;
  const enumOrder = enumDomainCompare(left, right);
  if (enumOrder !== undefined) return enumOrder;
  const exact = exactNumericCompare(left, right);
  if (exact !== undefined) return exact;
  const a = left instanceof Date ? left.getTime() : left;
  const b = right instanceof Date ? right.getTime() : right;
  if (a === b) return 0;
  if (a === null || a === undefined) return -1;
  if (b === null || b === undefined) return 1;
  if (typeof a === "number" && typeof b === "number") {
    if (Number.isNaN(a)) return Number.isNaN(b) ? 0 : 1;
    if (Number.isNaN(b)) return -1;
    return a - b;
  }
  if (typeof a === "string" && typeof b === "string") return compareSqlStrings(a, b);
  if (typeof a === "boolean" && typeof b === "boolean") return Number(a) - Number(b);
  throw new TypeError("Values must have comparable SQL types");
}

function numeric(value: unknown): number {
  if (typeof value !== "number")
    throw new TypeError("Arithmetic and numeric aggregates require numbers");
  return value;
}

function asQueryValue(value: unknown): QueryValue {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string" ||
    value instanceof Date
  ) {
    return value;
  }
  if (value === undefined) return null;
  throw new TypeError("Query produced an unsupported value");
}

function required<T>(value: T | undefined, message: string): T {
  if (value === undefined) throw new Error(message);
  return value;
}

function queryRowPayloadBytes(row: QueryRow): number {
  // for-in rather than Object.values: this runs once per result row, and Object.values
  // allocates a throwaway array of every value each time. Every term is non-negative, so one
  // range check at the end is equivalent to checking each addition.
  let total = QUERY_REFERENCE_BYTES;
  for (const key in row) total += queryValuePayloadBytes(row[key] ?? null);
  if (!Number.isSafeInteger(total) || total < 0) {
    throw new RangeError("Result row payload exceeds the safe integer range");
  }
  return total;
}

function queryValuePayloadBytes(value: QueryValue): number {
  if (value === null) return QUERY_VALUE_TAG_BYTES;
  if (typeof value === "boolean") return QUERY_VALUE_TAG_BYTES + 1;
  if (typeof value === "number" || value instanceof Date) {
    return QUERY_VALUE_TAG_BYTES + Float64Array.BYTES_PER_ELEMENT;
  }
  // Accounted as one byte per UTF-16 code unit: exact for the dominant Latin-1 case, O(1)
  // instead of a per-value UTF-8 encode that allocates a throwaway buffer per string.
  return safeMemorySum(QUERY_VALUE_TAG_BYTES, value.length, "String query value payload");
}

function safeMemorySum(left: number, right: number, label: string): number {
  const total = left + right;
  if (!Number.isSafeInteger(total) || total < 0) {
    throw new RangeError(`${label} exceeds the safe integer range`);
  }
  return total;
}

function safeMemoryProduct(left: number, right: number, label: string): number {
  const product = left * right;
  if (!Number.isSafeInteger(product) || product < 0) {
    throw new RangeError(`${label} exceeds the safe integer range`);
  }
  return product;
}
