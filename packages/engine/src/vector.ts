import type { DatabaseRow } from "./database.js";
import type {
  AggregateName,
  CompiledQuery,
  PredicateOperator,
  Expression,
  QueryResult,
  QueryRow,
  QueryValue,
  SelectItem,
} from "./query.js";
import { ByteGroupIndex, type GroupIndexKey } from "./group-index.js";
import { ByteJoinIndex } from "./join-index.js";
import {
  QueryMemoryContext,
  type QueryMemoryReservation,
  type QueryMemoryUsage,
} from "./memory.js";

const DEFAULT_BATCH_ROWS = 2_048;
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
      name: AggregateName | "ROUND";
      arguments: BoundExpression[];
      aggregateIndex?: number;
      signature: string;
    }
  | { kind: "list"; items: BoundExpression[]; signature: string };

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
}

interface BoundSelectItem {
  readonly expression: BoundExpression;
  readonly alias: string;
}

interface AggregateSpec {
  readonly name: AggregateName;
  readonly argument: BoundExpression;
}

interface GroupState {
  readonly groupValues: QueryValue[];
  readonly counts: Float64Array;
  readonly sums: Float64Array;
  readonly values: Array<QueryValue | undefined>;
  readonly valueReservations: Array<QueryMemoryReservation | undefined>;
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
  readonly wildcard: boolean;
  readonly limit?: number;
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
    const bound = bindPlan(plan, inputTables, retainedMemory);
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

function vectorValue(vector: ColumnVector, rowIndex: number): QueryValue {
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
      for (const value of vector.dictionary) {
        total = safeMemorySum(
          total,
          vectorTextEncoder.encode(value).byteLength,
          "String dictionary payload",
        );
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
  const bind = (expression: Expression): BoundExpression =>
    bindExpression(
      expression,
      sources.map((source, index) => ({ alias: source.alias, table: sourceTables[index] })),
      aggregateSpecs,
      aggregateIndexes,
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
    wildcard: plan.select[0]?.expression.kind === "wildcard",
    ...(plan.limit === undefined ? {} : { limit: plan.limit }),
  };
}

function bindExpression(
  expression: Expression,
  sources: ReadonlyArray<{ alias: string; table: ColumnarTable | undefined }>,
  aggregateSpecs: AggregateSpec[],
  aggregateIndexes: Map<string, number>,
): BoundExpression {
  const signature = JSON.stringify(expression);
  if (expression.kind === "subquery") {
    throw new TypeError("Subqueries are only supported in WHERE, HAVING, SELECT, and IN");
  }
  if (expression.kind === "window") {
    throw new TypeError("Window functions are only allowed in the select list");
  }
  if (expression.kind === "list") {
    return {
      kind: "list",
      items: expression.items.map((item) =>
        bindExpression(item, sources, aggregateSpecs, aggregateIndexes),
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
  if (expression.kind === "binary") {
    return {
      kind: "binary",
      operator: expression.operator,
      left: bindExpression(expression.left, sources, aggregateSpecs, aggregateIndexes),
      right: bindExpression(expression.right, sources, aggregateSpecs, aggregateIndexes),
      signature,
    };
  }
  const arguments_ = expression.arguments.map((argument) =>
    bindExpression(argument, sources, aggregateSpecs, aggregateIndexes),
  );
  if (expression.name === "ROUND") {
    return { kind: "call", name: expression.name, arguments: arguments_, signature };
  }
  let aggregateIndex = aggregateIndexes.get(signature);
  if (aggregateIndex === undefined) {
    aggregateIndex = aggregateSpecs.length;
    aggregateIndexes.set(signature, aggregateIndex);
    aggregateSpecs.push({
      name: expression.name,
      argument: required(arguments_[0], `${expression.name} argument is missing`),
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

function expressionSources(expression: BoundExpression): Set<number> {
  if (expression.kind === "column") return new Set([expression.source]);
  if (expression.kind === "binary") {
    return new Set([...expressionSources(expression.left), ...expressionSources(expression.right)]);
  }
  if (expression.kind === "call") {
    return new Set(expression.arguments.flatMap((argument) => [...expressionSources(argument)]));
  }
  return new Set();
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
  groups: ByteGroupIndex<GroupState>,
  output: QueryRow[],
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
  const groups = new ByteGroupIndex<GroupState>(memory);
  if (plan.grouped && plan.groupBy.length === 0) {
    groups.setEmpty(createGroupState([], plan, memory));
  }
  const output: QueryRow[] = [];
  const scanRows = plan.sourceTables[plan.scanSource]?.rowCount ?? 0;
  for (let start = 0; start < scanRows; start += DEFAULT_BATCH_ROWS) {
    const length = Math.min(DEFAULT_BATCH_ROWS, scanRows - start);
    if (runScanBatch(plan, start, length, groups, output, memory)) break;
  }
  const rows = plan.grouped ? finishGroups(plan, groups.values(), memory) : output;
  return finishResult(plan, rows, memory);
}

async function executeBoundPlanAsync(
  plan: BoundPlan,
  memory: QueryMemoryContext,
  options: AsyncQueryExecutionOptions,
): Promise<QueryResult> {
  const metadataCount = executeMetadataCount(plan, memory);
  if (metadataCount !== undefined) return metadataCount;
  const groups = new ByteGroupIndex<GroupState>(memory);
  if (plan.grouped && plan.groupBy.length === 0) {
    groups.setEmpty(createGroupState([], plan, memory));
  }
  const output: QueryRow[] = [];
  const scanRows = plan.sourceTables[plan.scanSource]?.rowCount ?? 0;
  for (let start = 0; start < scanRows; start += DEFAULT_BATCH_ROWS) {
    const length = Math.min(DEFAULT_BATCH_ROWS, scanRows - start);
    await options.loadScanWindow?.(start, length);
    if (runScanBatch(plan, start, length, groups, output, memory)) break;
  }
  const rows = plan.grouped ? finishGroups(plan, groups.values(), memory) : output;
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
    const limit = plan.limit ?? Number.MAX_SAFE_INTEGER;
    for (let pageIndex = 0; pageIndex < finalRun.pageCount && rows.length < limit; pageIndex += 1) {
      const bytes = await store.getPage(ownerId, finalRun.id, pageIndex);
      if (bytes === undefined) throw new Error("Query spill page is missing");
      for (const row of decodeSpillRows(columns, bytes)) {
        if (rows.length === limit) break;
        rows.push(row);
      }
    }
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
              batchMemory.reserve(queryRowPayloadBytes(spillRow), "Hash spill value row");
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
    return await readFinalSpillRun(store, ownerId, finalRun, columns, plan.limit);
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
    if (
      !plan.predicates.every((predicate) => evaluateBatchPredicate(plan, predicate, batch, row))
    ) {
      continue;
    }
    const resultRow = projectBatchRow(plan, batch, row);
    memory.reserve(queryRowPayloadBytes(resultRow), "Spill result row");
    output.push(resultRow);
  }
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
        outputMemory.reserve(queryRowPayloadBytes(row), "Spill merge output row");
        outputPage.push(row);
        leftRow = await leftReader.next();
      } else {
        outputMemory.reserve(queryRowPayloadBytes(rightRow), "Spill merge output row");
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
        Uint32Array.BYTES_PER_ELEMENT * 2 + Uint8Array.BYTES_PER_ELEMENT,
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
  groups: ByteGroupIndex<GroupState>,
  output: QueryRow[],
  memory: QueryMemoryContext,
): boolean {
  const join = plan.joins[joinIndex];
  if (join === undefined) {
    consumeBatch(plan, batch, groups, output, memory);
    return reachedEarlyLimit(plan, output.length);
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
  return (
    !plan.grouped &&
    plan.orderBy.length === 0 &&
    plan.limit !== undefined &&
    outputRows >= plan.limit
  );
}

function* joinBatches(
  plan: BoundPlan,
  input: BatchRows,
  join: BoundJoin,
  memory: QueryMemoryContext,
): Generator<BatchRows> {
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

function consumeBatch(
  plan: BoundPlan,
  batch: BatchRows,
  groups: ByteGroupIndex<GroupState>,
  output: QueryRow[],
  memory: QueryMemoryContext,
): void {
  for (let row = 0; row < batch.length; row += 1) {
    if (
      !plan.predicates.every((predicate) => evaluateBatchPredicate(plan, predicate, batch, row))
    ) {
      continue;
    }
    if (plan.grouped) {
      let state: GroupState | undefined;
      if (plan.groupBy.length === 0) state = groups.getEmpty();
      else if (plan.groupBy.length === 1) {
        const groupValue = asQueryValue(
          evaluateBatchExpression(
            plan,
            required(plan.groupBy[0], "Group expression is missing"),
            batch,
            row,
          ),
        );
        state = groups.getOrInsertOne(groupKey(groupValue), () =>
          createGroupState([groupValue], plan, memory),
        );
      } else {
        const groupValues = plan.groupBy.map((expression) =>
          asQueryValue(evaluateBatchExpression(plan, expression, batch, row)),
        );
        state = groups.getOrInsert(groupValues.map(groupKey), () =>
          createGroupState(groupValues, plan, memory),
        );
      }
      if (state === undefined) throw new Error("Grouped query state is missing");
      updateAggregates(plan, state, batch, row, memory);
    } else {
      const resultRow = projectBatchRow(plan, batch, row);
      memory.reserve(queryRowPayloadBytes(resultRow), "Accumulated result row");
      output.push(resultRow);
      if (plan.orderBy.length === 0 && plan.limit !== undefined && output.length >= plan.limit)
        return;
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
    const value =
      spec.argument.kind === "wildcard"
        ? 1
        : evaluateBatchExpression(plan, spec.argument, batch, row);
    applyAggregateValue(spec, state, index, value, memory);
  }
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
    const replacementValue = asQueryValue(value);
    const replacement = memory.reserve(
      queryValuePayloadBytes(replacementValue),
      "MIN aggregate value",
    );
    state.valueReservations[index]?.release();
    state.valueReservations[index] = replacement;
    state.values[index] = replacementValue;
  } else if (
    spec.name === "MAX" &&
    (state.values[index] === undefined || compareValues(value, state.values[index]) > 0)
  ) {
    const replacementValue = asQueryValue(value);
    const replacement = memory.reserve(
      queryValuePayloadBytes(replacementValue),
      "MAX aggregate value",
    );
    state.valueReservations[index]?.release();
    state.valueReservations[index] = replacement;
    state.values[index] = replacementValue;
  }
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
        comparisonValue(
          predicate.operator,
          evaluateFinalExpression(plan, predicate.left, group),
          evaluateFinalExpression(plan, predicate.right, group),
        ),
      )
    ) {
      continue;
    }
    const row = Object.fromEntries(
      plan.select.map((item) => [
        item.alias,
        asQueryValue(evaluateFinalExpression(plan, item.expression, group)),
      ]),
    );
    memory.reserve(queryRowPayloadBytes(row), "Accumulated grouped result row");
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
  if (expression.kind === "column") {
    throw new TypeError("Selected column must appear in GROUP BY");
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
  const aggregateIndex = expression.aggregateIndex ?? -1;
  const count = group.counts[aggregateIndex];
  if (count === undefined) throw new Error("Aggregate state is missing");
  if (expression.name === "COUNT") return count;
  if (count === 0) return null;
  const sum = group.sums[aggregateIndex] ?? 0;
  if (expression.name === "SUM") return sum;
  if (expression.name === "AVG") return sum / count;
  return group.values[aggregateIndex] ?? null;
}

function projectBatchRow(plan: BoundPlan, batch: BatchRows, row: number): QueryRow {
  if (!plan.wildcard) {
    return Object.fromEntries(
      plan.select.map((item) => [
        item.alias,
        asQueryValue(evaluateBatchExpression(plan, item.expression, batch, row)),
      ]),
    );
  }
  const values: Array<[string, QueryValue]> = [];
  const multiple = plan.sourceTables.length > 1;
  for (let source = 0; source < plan.sourceTables.length; source += 1) {
    const table = required(plan.sourceTables[source], "Wildcard source table is missing");
    const rowIndex = batch.rowsBySource[source]?.[row] ?? -1;
    for (const [name, vector] of table.columns) {
      values.push([
        multiple ? `${plan.sourceAliases[source] ?? ""}.${name}` : name,
        vectorValue(vector, rowIndex),
      ]);
    }
  }
  return Object.fromEntries(values);
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
  if (predicate.operator === "IN" || predicate.operator === "NOT IN") {
    if (predicate.right.kind !== "list") throw new TypeError("IN requires a value list");
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
  if (expression.kind === "column") {
    return vectorValue(expression.vector, batch.rowsBySource[expression.source]?.[row] ?? -1);
  }
  if (expression.kind === "binary") {
    return binaryValue(
      expression.operator,
      evaluateBatchExpression(plan, expression.left, batch, row),
      evaluateBatchExpression(plan, expression.right, batch, row),
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
  if (expression.kind === "column") {
    return vectorValue(expression.vector, rowsBySource[expression.source] ?? -1);
  }
  if (expression.kind === "binary") {
    return binaryValue(
      expression.operator,
      evaluateExpression(expression.left, rowsBySource),
      evaluateExpression(expression.right, rowsBySource),
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
  const compareIndexes = (leftIndex: number, rightIndex: number): number => {
    const left = required(rows[leftIndex], "Ordering row is missing");
    const right = required(rows[rightIndex], "Ordering row is missing");
    for (const order of orderBy) {
      const comparison = compareValues(left[order.outputName], right[order.outputName]);
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

function compareValues(left: unknown, right: unknown): number {
  const a = comparable(left);
  const b = comparable(right);
  if (a === b) return 0;
  if (a === null || a === undefined) return -1;
  if (b === null || b === undefined) return 1;
  if (typeof a === "number" && typeof b === "number") {
    if (Number.isNaN(a)) return Number.isNaN(b) ? 0 : 1;
    if (Number.isNaN(b)) return -1;
    return a - b;
  }
  if (typeof a === "string" && typeof b === "string") return a.localeCompare(b);
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
  return safeMemorySum(
    QUERY_VALUE_TAG_BYTES,
    vectorTextEncoder.encode(value).byteLength,
    "String query value payload",
  );
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
