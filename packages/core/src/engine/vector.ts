import type { DatabaseRow } from "./database.js";
import type {
  AggregateName,
  CompiledQuery,
  PredicateOperator,
  Expression,
  QueryResult,
  QueryRow,
  QueryValue,
  ScalarFunctionName,
  SelectItem,
} from "./query.js";
import { cachedListMembership, dateTruncValue, isScalarFunctionName, likeRegExp } from "./query.js";
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

const DEFAULT_BATCH_ROWS = 2_048;
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
}

export interface PreparedVectorQuery {
  readonly memoryUsage: QueryMemoryUsage;
  execute(): QueryResult;
  executeAsync(options?: AsyncQueryExecutionOptions): Promise<QueryResult>;
  close(): void;
}

export interface QuerySpillStore {
  putPage(ownerId: string, runId: string, pageIndex: number, bytes: Uint8Array): Promise<void>;
  getPage(ownerId: string, runId: string, pageIndex: number): Promise<Uint8Array | undefined>;
  removeRun(ownerId: string, runId: string): Promise<void>;
  removeOwner(ownerId: string): Promise<void>;
}

export interface AsyncQueryExecutionOptions {
  readonly spillStore?: QuerySpillStore;
  readonly spillPageRows?: number;
  /**
   * Makes the scan-source window [start, start + length) resident before each batch. Supplied by
   * a streaming preparation whose scan-source vectors hold only a sliding window; the executor
   * awaits it at the top of every scan batch in every asynchronous execution path.
   */
  readonly loadScanWindow?: (start: number, length: number) => Promise<void>;
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
  /** Dictionary-code rewrite for string equality against a literal; codes compare per row. */
  readonly dictionaryEquality?: DictionaryEquality;
}

interface DictionaryEquality {
  readonly source: number;
  readonly vector: StringVector;
  readonly value: string;
  readonly negated: boolean;
  readonly cache: { dictionary: readonly string[] | undefined; code: number };
}

interface BoundJoin {
  readonly kind: "inner" | "left";
  readonly buildSource: number;
  readonly probe: BoundExpression;
  readonly build: BoundExpression;
  readonly lookup: JoinLookup;
  /** Nested-loop fallback for non-equi or multi-key ON conditions; lookup is unused when set. */
  readonly loop?: { condition: BoundExpression; rowCount: number };
}

interface BoundSelectItem {
  readonly expression: BoundExpression;
  readonly alias: string;
}

interface AggregateSpec {
  readonly name: AggregateName;
  readonly argument: BoundExpression;
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
  readonly valueReservations: Array<QueryMemoryReservation | undefined>;
  readonly valueReservationBytes: Float64Array;
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
  readonly select: readonly BoundSelectItem[];
  readonly orderBy: ReadonlyArray<{ outputName: string; direction: "asc" | "desc" }>;
  readonly grouped: boolean;
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
    const values = rows.map((row) => row[columnName] ?? null);
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
        const canSpillSort = bound.orderBy.length > 0 && !bound.grouped;
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
          ? value.getTime()
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
  let total = 0;
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

function bindPlan(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, ColumnarTable>,
  memory: QueryMemoryContext,
  ftsStats?: ReadonlyMap<string, FtsStats>,
): BoundPlan {
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
  const predicates = plan.predicates.map((predicate) => {
    const bound: BoundPredicate = {
      left: bind(predicate.left),
      operator: predicate.operator,
      right: bind(predicate.right),
    };
    const dictionaryEquality = detectDictionaryEquality(bound);
    return dictionaryEquality === undefined ? bound : { ...bound, dictionaryEquality };
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

  const orderBy = plan.orderBy.map(({ expression, direction }) => {
    const outputName = orderOutputName(expression, plan.select);
    if (outputName === undefined)
      throw new TypeError("ORDER BY requires a selected column or output alias");
    return { outputName, direction };
  });
  const grouped = groupBy.length > 0 || aggregateSpecs.length > 0;
  // A single bare string-column GROUP BY can group on dictionary codes: identical values share a
  // code within one materialized vector, so a code-indexed slot table replaces per-row hashing.
  // Streamed vectors rebuild their dictionary per window, so a windowed vector never qualifies.
  const groupColumn = groupBy.length === 1 ? groupBy[0] : undefined;
  const codeGrouping =
    grouped &&
    groupColumn?.kind === "column" &&
    groupColumn.vector.kind === "string" &&
    groupColumn.vector.window === undefined
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
    select,
    orderBy,
    grouped,
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
        bindExpression(item, sources, aggregateSpecs, aggregateIndexes, memory, ftsBySignature, ftsStats),
      ),
      signature,
    };
  }
  if (expression.kind === "literal" || expression.kind === "wildcard") {
    return { ...expression, signature };
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
      left: bindExpression(expression.left, sources, aggregateSpecs, aggregateIndexes, memory, ftsBySignature, ftsStats),
      right: bindExpression(expression.right, sources, aggregateSpecs, aggregateIndexes, memory, ftsBySignature, ftsStats),
      signature,
    } as BoundExpression;
  }
  if (expression.kind === "logical") {
    return {
      kind: "logical",
      operator: expression.operator,
      left: bindExpression(expression.left, sources, aggregateSpecs, aggregateIndexes, memory, ftsBySignature, ftsStats),
      right: bindExpression(expression.right, sources, aggregateSpecs, aggregateIndexes, memory, ftsBySignature, ftsStats),
      signature,
    };
  }
  if (expression.kind === "not") {
    return {
      kind: "not",
      operand: bindExpression(expression.operand, sources, aggregateSpecs, aggregateIndexes, memory, ftsBySignature, ftsStats),
      signature,
    };
  }
  if (expression.kind === "case") {
    const otherwise =
      expression.otherwise === undefined
        ? undefined
        : bindExpression(expression.otherwise, sources, aggregateSpecs, aggregateIndexes, memory, ftsBySignature, ftsStats);
    return {
      kind: "case",
      branches: expression.branches.map((branch) => ({
        when: bindExpression(branch.when, sources, aggregateSpecs, aggregateIndexes, memory, ftsBySignature, ftsStats),
        then: bindExpression(branch.then, sources, aggregateSpecs, aggregateIndexes, memory, ftsBySignature, ftsStats),
      })),
      ...(otherwise === undefined ? {} : { otherwise }),
      signature,
    };
  }
  const arguments_ = expression.arguments.map((argument) =>
    bindExpression(argument, sources, aggregateSpecs, aggregateIndexes, memory, ftsBySignature, ftsStats),
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
      argument.kind === "column" && argument.vector.kind === "number"
        ? { source: argument.source, vector: argument.vector }
        : undefined;
    aggregateSpecs.push({
      name: expression.name,
      argument,
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
    const termTf = withScores
      ? new Uint32Array(vector.dictionary.length * termCount)
      : undefined;
    for (let code = 0; code < vector.dictionary.length; code += 1) {
      const tokens = tokenize(vector.dictionary[code] ?? "");
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
  kind: "inner" | "left",
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
    for (let index = 0; index < length; index += 1) scan[index] = start + index;
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
  for (let start = 0; start < scanRows; start += DEFAULT_BATCH_ROWS) {
    const length = Math.min(DEFAULT_BATCH_ROWS, scanRows - start);
    if (runScanBatch(plan, start, length, groups, output, memory)) break;
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
  for (let start = 0; start < scanRows; start += DEFAULT_BATCH_ROWS) {
    const length = Math.min(DEFAULT_BATCH_ROWS, scanRows - start);
    await options.loadScanWindow?.(start, length);
    if (runScanBatch(plan, start, length, groups, output, memory)) break;
  }
  const rows = plan.grouped ? finishGroups(plan, groups.values(), memory) : output.finish();
  return finishResult(plan, rows, memory);
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
  const pageRows = options.spillPageRows ?? DEFAULT_BATCH_ROWS;
  if (!Number.isSafeInteger(pageRows) || pageRows <= 0) {
    throw new RangeError("Query spill page rows must be a positive whole number");
  }
  const columns = plan.wildcard ? wildcardColumnNames(plan) : plan.select.map((item) => item.alias);
  const ownerId = createSpillOwnerId();
  const runs: SpillRun[] = [];
  let runSequence = 0;
  try {
    const scanRows = plan.sourceTables[plan.scanSource]?.rowCount ?? 0;
    const scanBatchRows = Math.min(DEFAULT_BATCH_ROWS, pageRows);
    for (let start = 0; start < scanRows; start += scanBatchRows) {
      const length = Math.min(scanBatchRows, scanRows - start);
      await options.loadScanWindow?.(start, length);
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
        if (scan === undefined) continue;
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
              await store.putPage(ownerId, runId, 0, encodeSpillRows(columns, rows));
              runs.push({ id: runId, pageCount: 1 });
            } finally {
              outputMemory.close();
            }
          },
        );
      } finally {
        batchMemory.close();
      }
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
  const pageRows = options.spillPageRows ?? DEFAULT_BATCH_ROWS;
  if (!Number.isSafeInteger(pageRows) || pageRows <= 0) {
    throw new RangeError("Query spill page rows must be a positive whole number");
  }
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
    for (let start = 0; start < scanRows; start += scanChunkRows) {
      const length = Math.min(scanChunkRows, scanRows - start);
      await options.loadScanWindow?.(start, length);
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
        if (scan === undefined) continue;
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
                spillRow[`a${String(index)}`] =
                  raw === null || raw === undefined ? null : asQueryValue(raw);
              }
              batchMemory.tally(queryRowPayloadBytes(spillRow), "Hash spill value row");
              const partition = hashQueryValues(groupValues) & (partitionCount - 1);
              const rows = partitionBuffers.get(partition) ?? [];
              rows.push(spillRow);
              partitionBuffers.set(partition, rows);
            }
          },
        );
        for (const [partition, rows] of partitionBuffers) {
          const pageIndex = partitionPages[partition] ?? 0;
          await store.putPage(
            ownerId,
            `partition-${String(partition)}`,
            pageIndex,
            encodeSpillRows(spillColumns, rows),
          );
          partitionPages[partition] = pageIndex + 1;
        }
      } finally {
        batchMemory.close();
      }
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
        let outputPage = 0;
        for (let start = 0; start < rows.length; start += pageRows) {
          await store.putPage(
            ownerId,
            runId,
            outputPage,
            encodeSpillRows(columns, rows.slice(start, start + pageRows)),
          );
          outputPage += 1;
        }
        runs.push({ id: runId, pageCount: outputPage });
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
    await store.putPage(ownerId, outputId, pageIndex, encodeSpillRows(columns, outputPage));
    outputPage = [];
    outputMemory.close();
    outputMemory = mergeMemory.createChild();
    pageIndex += 1;
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
    const comparison = compareValues(left[order.outputName], right[order.outputName]);
    if (comparison !== 0) return order.direction === "desc" ? -comparison : comparison;
  }
  return 0;
}

function encodeSpillRows(columns: readonly string[], rows: readonly QueryRow[]): Uint8Array {
  const encoded = rows.map((row) => columns.map((column) => encodeSpillValue(row[column] ?? null)));
  const payload = vectorTextEncoder.encode(JSON.stringify(encoded));
  const modeledBytes = rows.reduce(
    (total, row) => safeMemorySum(total, queryRowPayloadBytes(row), "Spill page rows"),
    0,
  );
  if (modeledBytes > 0xffffffff) throw new RangeError("Spill page modeled bytes exceed uint32");
  const bytes = new Uint8Array(SPILL_PAGE_HEADER_BYTES + payload.byteLength);
  const header = new DataView(bytes.buffer);
  header.setUint32(0, SPILL_PAGE_MAGIC, true);
  header.setUint32(4, modeledBytes, true);
  bytes.set(payload, SPILL_PAGE_HEADER_BYTES);
  return bytes;
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
  return [4, value.getTime()];
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
  if (plan.orderBy.length > 0) {
    const ordering = memory.reserve(
      safeMemoryProduct(
        rows.length,
        Uint32Array.BYTES_PER_ELEMENT * 2 +
          Uint8Array.BYTES_PER_ELEMENT +
          // One reference slot per extracted sort key (stableSortRows key columns).
          plan.orderBy.length * QUERY_REFERENCE_BYTES,
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

function consumeJoinedBatches(
  plan: BoundPlan,
  batch: BatchRows,
  joinIndex: number,
  groups: GroupAccumulator,
  output: ResultSink,
  memory: QueryMemoryContext,
): boolean {
  const join = plan.joins[joinIndex];
  if (join === undefined) {
    consumeBatch(plan, batch, groups, output, memory);
    return reachedEarlyLimit(plan, output.size);
  }
  for (const joined of joinBatches(plan, batch, join, memory)) {
    try {
      if (consumeJoinedBatches(plan, joined, joinIndex + 1, groups, output, memory)) return true;
    } finally {
      joined.memory?.close();
    }
  }
  return false;
}

function reachedEarlyLimit(plan: BoundPlan, outputRows: number): boolean {
  // Early termination must still produce the rows the trailing OFFSET will discard.
  return (
    !plan.grouped &&
    plan.orderBy.length === 0 &&
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
        if (join.kind === "left") {
          appendJoinedRow(ensureOutput(), outputLength, input, row, join.buildSource, -1);
          outputLength += 1;
        }
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
        appendJoinedRow(ensureOutput(), outputLength, input, row, join.buildSource, buildRow);
        outputLength += 1;
        if (outputLength === DEFAULT_BATCH_ROWS) yield* emit();
      }
      if (!matched && join.kind === "left") {
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
    for (let row = 0; row < input.length; row += 1) {
      const probeKey = evaluateBatchExpression(plan, join.probe, input, row);
      const buildRow = probeKey === null ? -1 : join.lookup.firstRow(probeKey);
      if (buildRow < 0 && join.kind === "inner") continue;
      selectedRows[outputLength] = row;
      buildRows[outputLength] = buildRow;
      outputLength += 1;
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

interface RetainedTopRow {
  row: QueryRow;
  keys: QueryValue[];
  seq: number;
  reservation: QueryMemoryReservation;
}

interface DeferredTopRow {
  keys: QueryValue[];
  seq: number;
  sourceRow: number;
  bytes: number;
}

const DEFERRED_SELECTION_BYTES = 2 * QUERY_REFERENCE_BYTES;

/**
 * Accumulates ungrouped result rows. With ORDER BY and LIMIT the sink keeps only the best
 * `limit + offset` rows instead of materializing every scanned row for one full sort, so memory
 * stays bounded by the limit rather than the table. Retention matches a stable sort exactly:
 * ties resolve by arrival order, so the retained set is precisely the slice a full stable sort
 * would have produced.
 *
 * Two bounded strategies:
 * - Deferred selection (single-source plans whose order keys resolve to select expressions and
 *   whose scan vectors outlive the scan): rows are tracked as (keys, arrival, source row) and
 *   compacted by sort once the selection buffer reaches twice the capacity; only the final
 *   survivors are projected. A losing row never allocates a result object, so even the
 *   adversarial ascending-input/descending-order case stays allocation-free per row.
 * - Eager heap (joins, wildcard selects, or windowed scans): each retained row is projected into
 *   a worst-at-root heap; a candidate that cannot beat the current worst is rejected before
 *   projection whenever the order keys are resolvable.
 */
class ResultSink {
  readonly #plan: BoundPlan;
  readonly #memory: QueryMemoryContext;
  readonly #capacity: number | undefined;
  readonly #keyExpressions: readonly BoundExpression[] | undefined;
  readonly #deferred: boolean;
  readonly #rows: QueryRow[] = [];
  readonly #heap: RetainedTopRow[] = [];
  readonly #selection: DeferredTopRow[] = [];
  readonly #keyScratch: QueryValue[] = [];
  #threshold: DeferredTopRow | undefined;
  #selectionBytes = 0;
  #selectionReservedBytes = 0;
  #seq = 0;

  constructor(plan: BoundPlan, memory: QueryMemoryContext, stableScan: boolean) {
    this.#plan = plan;
    this.#memory = memory;
    const bounded = !plan.grouped && plan.orderBy.length > 0 && plan.limit !== undefined;
    this.#capacity = bounded ? (plan.limit ?? 0) + (plan.offset ?? 0) : undefined;
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
  }

  /** Rows accepted so far; only meaningful for the unbounded early-limit check. */
  get size(): number {
    return this.#capacity === undefined ? this.#rows.length : 0;
  }

  add(batch: BatchRows, row: number): void {
    if (this.#capacity === undefined) {
      const resultRow = projectBatchRow(this.#plan, batch, row);
      this.#memory.tally(queryRowPayloadBytes(resultRow), "Accumulated result row");
      this.#rows.push(resultRow);
      return;
    }
    if (this.#capacity === 0) return;
    if (this.#deferred) {
      this.#addDeferred(batch, row);
      return;
    }
    this.#addEager(batch, row);
  }

  /** Returns accepted rows in arrival order, ready for the shared stable sort and trim. */
  finish(): QueryRow[] {
    if (this.#capacity === undefined) return this.#rows;
    if (!this.#deferred) {
      return this.#heap.sort((left, right) => left.seq - right.seq).map((entry) => entry.row);
    }
    this.#compactSelection();
    this.#selection.sort((left, right) => left.seq - right.seq);
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

  #addDeferred(batch: BatchRows, row: number): void {
    this.#evaluateKeys(batch, row);
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
    let bytes = DEFERRED_SELECTION_BYTES;
    for (let index = 0; index < this.#plan.orderBy.length; index += 1) {
      bytes = safeMemorySum(
        bytes,
        queryValuePayloadBytes(this.#keyScratch[index] ?? null),
        "Top-N selection entry",
      );
    }
    this.#selection.push({
      keys: this.#keyScratch.slice(0, this.#plan.orderBy.length),
      seq,
      sourceRow: batch.rowsBySource[this.#plan.scanSource]?.[row] ?? -1,
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

  /** Sorts the selection, keeps the best `capacity` entries, and advances the cut line. */
  #compactSelection(): void {
    const capacity = this.#capacity ?? 0;
    if (this.#selection.length <= capacity) return;
    this.#selection.sort((left, right) => {
      const comparison = this.#compareKeys(left.keys, right.keys);
      return comparison !== 0 ? comparison : left.seq - right.seq;
    });
    this.#selection.length = capacity;
    let retainedBytes = 0;
    for (const entry of this.#selection) retainedBytes += entry.bytes;
    this.#selectionBytes = retainedBytes;
    this.#threshold = this.#selection[capacity - 1];
  }

  #addEager(batch: BatchRows, row: number): void {
    let projected: QueryRow | undefined;
    if (this.#keyExpressions === undefined) {
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
    const capacity = this.#capacity ?? 0;
    if (this.#heap.length >= capacity) {
      const worst = required(this.#heap[0], "Top-N heap root is missing");
      // An order-key tie keeps the earlier-arriving retained row.
      if (this.#compareKeys(this.#keyScratch, worst.keys) >= 0) return;
      const resultRow = projected ?? projectBatchRow(this.#plan, batch, row);
      const reservation = this.#memory.reserve(
        this.#entryPayloadBytes(resultRow),
        "Top-N result row",
      );
      worst.reservation.release();
      this.#heap[0] = {
        row: resultRow,
        keys: this.#keyScratch.slice(0, this.#plan.orderBy.length),
        seq,
        reservation,
      };
      this.#siftDown(0);
      return;
    }
    const resultRow = projected ?? projectBatchRow(this.#plan, batch, row);
    const reservation = this.#memory.reserve(
      this.#entryPayloadBytes(resultRow),
      "Top-N result row",
    );
    this.#heap.push({
      row: resultRow,
      keys: this.#keyScratch.slice(0, this.#plan.orderBy.length),
      seq,
      reservation,
    });
    this.#siftUp(this.#heap.length - 1);
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

  #entryPayloadBytes(row: QueryRow): number {
    let bytes = queryRowPayloadBytes(row);
    for (let index = 0; index < this.#plan.orderBy.length; index += 1) {
      bytes = safeMemorySum(
        bytes,
        queryValuePayloadBytes(this.#keyScratch[index] ?? null),
        "Top-N order keys",
      );
    }
    return bytes;
  }

  #compareKeys(left: readonly QueryValue[], right: readonly QueryValue[]): number {
    const orderBy = this.#plan.orderBy;
    for (let index = 0; index < orderBy.length; index += 1) {
      const order = required(orderBy[index], "Order term is missing");
      const comparison = compareValues(left[index], right[index]);
      if (comparison !== 0) return order.direction === "desc" ? -comparison : comparison;
    }
    return 0;
  }

  /** Entry `left` loses to `right` when it sorts later under (keys, arrival). */
  #isWorse(left: RetainedTopRow, right: RetainedTopRow): boolean {
    const comparison = this.#compareKeys(left.keys, right.keys);
    if (comparison !== 0) return comparison > 0;
    return left.seq > right.seq;
  }

  #siftUp(index: number): void {
    let child = index;
    while (child > 0) {
      const parent = (child - 1) >> 1;
      const childEntry = required(this.#heap[child], "Heap entry is missing");
      const parentEntry = required(this.#heap[parent], "Heap entry is missing");
      if (!this.#isWorse(childEntry, parentEntry)) break;
      this.#heap[child] = parentEntry;
      this.#heap[parent] = childEntry;
      child = parent;
    }
  }

  #siftDown(index: number): void {
    let parent = index;
    for (;;) {
      let worst = parent;
      let worstEntry = required(this.#heap[worst], "Heap entry is missing");
      const left = parent * 2 + 1;
      const right = left + 1;
      const leftEntry = this.#heap[left];
      if (leftEntry !== undefined && this.#isWorse(leftEntry, worstEntry)) {
        worst = left;
        worstEntry = leftEntry;
      }
      const rightEntry = this.#heap[right];
      if (rightEntry !== undefined && this.#isWorse(rightEntry, worstEntry)) {
        worst = right;
        worstEntry = rightEntry;
      }
      if (worst === parent) return;
      const parentEntry = required(this.#heap[parent], "Heap entry is missing");
      this.#heap[parent] = worstEntry;
      this.#heap[worst] = parentEntry;
      parent = worst;
    }
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
  #ordered: GroupState[] | undefined;
  readonly #codeColumns: ReadonlyArray<{ source: number; vector: StringVector } | undefined>;
  #multiCodeColumns: ReadonlyArray<{ source: number; vector: StringVector }> | undefined;
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

  constructor(plan: BoundPlan, memory: QueryMemoryContext) {
    this.#plan = plan;
    this.#memory = memory;
    this.#index = new ByteGroupIndex<GroupState>(memory);
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
    // group lookup skips hashing entirely: the codes multiply into one direct array slot. Each
    // column contributes (dictionary size + 1) slots, the extra one for NULL. Falls back to the
    // byte index when the slot space is too large or its reservation exceeds the query budget.
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
      }
    }
  }

  /** Resolves the group state for one row, creating it on first touch. */
  stateFor(batch: BatchRows, row: number): GroupState {
    const plan = this.#plan;
    const codeGrouping = plan.codeGrouping;
    if (codeGrouping !== undefined && this.#codeStates !== undefined) {
      const sourceRow = batch.rowsBySource[codeGrouping.source]?.[row] ?? -1;
      const vector = codeGrouping.vector;
      let code = vector.dictionary.length;
      if (sourceRow >= 0 && sourceRow < vector.length && isValid(vector.validity, sourceRow)) {
        const rawCode = vector.codes[sourceRow] ?? NULL_STRING_CODE;
        if (rawCode !== NULL_STRING_CODE) code = rawCode;
      }
      let state = this.#codeStates[code];
      if (state === undefined) {
        const value = code === vector.dictionary.length ? null : (vector.dictionary[code] ?? null);
        state = createGroupState([value], plan, this.#memory);
        this.#codeStates[code] = state;
        this.#ordered?.push(state);
      }
      return state;
    }
    if (plan.groupBy.length === 0) {
      return required(this.#index.getEmpty(), "Grouped query state is missing");
    }
    const multiCode = this.#multiCodeColumns;
    if (multiCode !== undefined && this.#codeStates !== undefined) {
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
      let state = this.#codeStates[slot];
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
        this.#codeStates[slot] = state;
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
): void {
  for (let row = 0; row < batch.length; row += 1) {
    if (!passesPredicates(plan, batch, row)) continue;
    if (plan.grouped) {
      updateAggregates(plan, groups.stateFor(batch, row), batch, row, memory);
    } else {
      output.add(batch, row);
      if (reachedEarlyLimit(plan, output.size)) return;
    }
  }
}

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
  memory.reserve(payloadBytes, "Group state");
  return {
    groupValues,
    counts: new Float64Array(plan.aggregates.length),
    sums: new Float64Array(plan.aggregates.length),
    values: new Array<QueryValue | undefined>(plan.aggregates.length),
    valueReservations: new Array<QueryMemoryReservation | undefined>(plan.aggregates.length),
    valueReservationBytes: new Float64Array(plan.aggregates.length),
  };
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
    if (spec.rawNumber !== undefined) {
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
    applyAggregateValue(spec, state, index, value, memory);
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
    applyAggregateValue(spec, state, index, values[index], memory);
  }
}

function applyAggregateValue(
  spec: AggregateSpec,
  state: GroupState,
  index: number,
  value: unknown,
  memory: QueryMemoryContext,
): void {
  if (value === null || value === undefined) return;
  state.counts[index] = (state.counts[index] ?? 0) + 1;
  if (spec.name === "SUM" || spec.name === "AVG") {
    state.sums[index] = (state.sums[index] ?? 0) + numeric(value);
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
  if (expression.name === "ROUND") {
    return roundValue(
      evaluateFinalExpression(
        plan,
        required(expression.arguments[0], "ROUND argument is missing"),
        group,
      ),
      expression.arguments[1] === undefined
        ? 0
        : numeric(evaluateFinalExpression(plan, expression.arguments[1], group)),
    );
  }
  if (expression.name === "COALESCE") {
    for (const argument of expression.arguments) {
      const candidate = evaluateFinalExpression(plan, argument, group);
      if (candidate !== null && candidate !== undefined) return candidate;
    }
    return null;
  }
  if (expression.name === "DATE_TRUNC") {
    return dateTruncValue(
      evaluateFinalExpression(
        plan,
        required(expression.arguments[0], "DATE_TRUNC unit is missing"),
        group,
      ),
      evaluateFinalExpression(
        plan,
        required(expression.arguments[1], "DATE_TRUNC argument is missing"),
        group,
      ),
    );
  }
  const aggregateIndex = expression.aggregateIndex ?? -1;
  const count = group.counts[aggregateIndex];
  if (count === undefined) throw new Error("Aggregate state is missing");
  if (expression.name === "COUNT") return count;
  if (count === 0) return null;
  const sum = group.sums[aggregateIndex] ?? 0;
  if (expression.name === "SUM") return sum;
  if (expression.name === "AVG") return sum / count;
  const value = group.values[aggregateIndex] ?? null;
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
      result[item.alias] = asQueryValue(evaluateBatchExpression(plan, item.expression, batch, row));
    }
    return result;
  }
  const multiple = plan.sourceTables.length > 1;
  for (let source = 0; source < plan.sourceTables.length; source += 1) {
    const table = required(plan.sourceTables[source], "Wildcard source table is missing");
    const rowIndex = batch.rowsBySource[source]?.[row] ?? -1;
    const prefix = multiple ? `${plan.sourceAliases[source] ?? ""}.` : "";
    for (const [name, vector] of table.columns) {
      result[multiple ? prefix + name : name] = vectorValue(vector, rowIndex);
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
    if (operator === "LIKE" || operator === "NOT LIKE") {
      const value = evaluateValue(expression.left);
      const pattern = evaluateValue(expression.right);
      if (value === null || value === undefined || pattern === null || pattern === undefined) {
        return null;
      }
      if (typeof value !== "string" || typeof pattern !== "string") {
        throw new TypeError("LIKE requires string operands");
      }
      const matched = likeRegExp(pattern).test(value);
      return operator === "LIKE" ? matched : !matched;
    }
    const left = evaluateValue(expression.left);
    const right = evaluateValue(expression.right);
    if (left === null || left === undefined || right === null || right === undefined) return null;
    const a = comparable(left);
    const b = comparable(right);
    if (operator === "=") return a === b;
    if (operator === "!=" || operator === "<>") return a !== b;
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
  predicate: { left: BoundExpression; operator: PredicateOperator; right: BoundExpression },
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
  if (
    predicate.operator === "IS TRUE" ||
    predicate.operator === "LIKE" ||
    predicate.operator === "NOT LIKE"
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
  if (expression.name === "DATE_TRUNC") {
    return dateTruncValue(
      evaluateBatchExpression(
        plan,
        required(expression.arguments[0], "DATE_TRUNC unit is missing"),
        batch,
        row,
      ),
      evaluateBatchExpression(
        plan,
        required(expression.arguments[1], "DATE_TRUNC argument is missing"),
        batch,
        row,
      ),
    );
  }
  if (expression.name !== "ROUND")
    throw new TypeError(`${expression.name} requires grouped execution`);
  return roundValue(
    evaluateBatchExpression(
      plan,
      required(expression.arguments[0], "ROUND argument is missing"),
      batch,
      row,
    ),
    expression.arguments[1] === undefined
      ? 0
      : numeric(evaluateBatchExpression(plan, expression.arguments[1], batch, row)),
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
  if (expression.name === "DATE_TRUNC") {
    return dateTruncValue(
      evaluateExpression(
        required(expression.arguments[0], "DATE_TRUNC unit is missing"),
        rowsBySource,
      ),
      evaluateExpression(
        required(expression.arguments[1], "DATE_TRUNC argument is missing"),
        rowsBySource,
      ),
    );
  }
  if (expression.name !== "ROUND")
    throw new TypeError(`${expression.name} requires grouped execution`);
  return roundValue(
    evaluateExpression(
      required(expression.arguments[0], "ROUND argument is missing"),
      rowsBySource,
    ),
    expression.arguments[1] === undefined
      ? 0
      : numeric(evaluateExpression(expression.arguments[1], rowsBySource)),
  );
}

function binaryValue(
  operator: "+" | "-" | "*" | "/",
  left: unknown,
  right: unknown,
): number | null {
  if (left === null || left === undefined || right === null || right === undefined) return null;
  const a = numeric(left);
  const b = numeric(right);
  if (operator === "+") return a + b;
  if (operator === "-") return a - b;
  if (operator === "*") return a * b;
  return a / b;
}

function roundValue(value: unknown, digits: number): number | null {
  if (value === null || value === undefined) return null;
  const factor = 10 ** digits;
  return Math.round(numeric(value) * factor) / factor;
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
  if (operator === "=") return left === right;
  if (operator === "!=" || operator === "<>") return left !== right;
  const comparison = compareValues(left, right);
  if (operator === ">") return comparison > 0;
  if (operator === ">=") return comparison >= 0;
  if (operator === "<") return comparison < 0;
  return comparison <= 0;
}

function orderOutputName(
  expression: Expression,
  select: readonly SelectItem[],
): string | undefined {
  if (expression.kind !== "column") return undefined;
  // A wildcard select exposes source columns under their own names, so bare references order by
  // the matching output column directly.
  if (select[0]?.expression.kind === "wildcard") return expression.reference;
  const selected = select.find(
    (item) =>
      (item.expression.kind === "column" && item.expression.reference === expression.reference) ||
      (!expression.reference.includes(".") && item.alias === expression.reference),
  );
  return selected?.alias;
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

function stableSortRows(
  rows: QueryRow[],
  orderBy: ReadonlyArray<{ outputName: string; direction: "asc" | "desc" }>,
): void {
  if (rows.length > 0xffffffff) throw new RangeError("Too many rows to order");
  const indexes = new Uint32Array(rows.length);
  const scratch = new Uint32Array(rows.length);
  for (let index = 0; index < indexes.length; index += 1) indexes[index] = index;
  let source = indexes;
  let target = scratch;
  // Sort keys are extracted once per row: the merge performs O(n log n) comparisons, and
  // re-reading string-keyed row properties (plus Date unboxing) inside every comparison
  // dominates the sort for wide inputs. The keys land in one flat array in row-major order.
  const termCount = orderBy.length;
  const keys = new Array<unknown>(rows.length * termCount);
  for (let index = 0; index < rows.length; index += 1) {
    const row = required(rows[index], "Ordering row is missing");
    for (let term = 0; term < termCount; term += 1) {
      const order = required(orderBy[term], "Order term is missing");
      keys[index * termCount + term] = comparable(row[order.outputName]);
    }
  }
  const compareIndexes = (leftIndex: number, rightIndex: number): number => {
    for (let term = 0; term < termCount; term += 1) {
      const order = required(orderBy[term], "Order term is missing");
      const comparison = compareComparables(
        keys[leftIndex * termCount + term],
        keys[rightIndex * termCount + term],
      );
      if (comparison !== 0) return order.direction === "desc" ? -comparison : comparison;
    }
    return 0;
  };
  for (let width = 1; width < rows.length; width *= 2) {
    for (let start = 0; start < rows.length; start += width * 2) {
      const middle = Math.min(start + width, rows.length);
      const end = Math.min(start + width * 2, rows.length);
      let left = start;
      let right = middle;
      for (let output = start; output < end; output += 1) {
        if (
          right >= end ||
          (left < middle && compareIndexes(source[left] ?? 0, source[right] ?? 0) <= 0)
        ) {
          target[output] = source[left] ?? 0;
          left += 1;
        } else {
          target[output] = source[right] ?? 0;
          right += 1;
        }
      }
    }
    [source, target] = [target, source];
  }
  if (source !== indexes) indexes.set(source);

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

/**
 * Shares one default-locale collator across all string comparisons: `left.localeCompare(right)`
 * with no arguments is specified to order identically, but re-resolves locale data per call.
 */
const defaultCollator = new Intl.Collator();

function compareValues(left: unknown, right: unknown): number {
  return compareComparables(comparable(left), comparable(right));
}

function compareComparables(a: unknown, b: unknown): number {
  if (a === b) return 0;
  if (a === null || a === undefined) return -1;
  if (b === null || b === undefined) return 1;
  if (typeof a === "number" && typeof b === "number") {
    if (Number.isNaN(a)) return Number.isNaN(b) ? 0 : 1;
    if (Number.isNaN(b)) return -1;
    return a - b;
  }
  if (typeof a === "string" && typeof b === "string") return defaultCollator.compare(a, b);
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
  let total = QUERY_REFERENCE_BYTES;
  for (const value of Object.values(row)) {
    total = safeMemorySum(total, queryValuePayloadBytes(value), "Result row payload");
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
