import type { DatabaseRow } from "./database.js";
import type {
  AggregateName,
  CompiledQuery,
  ComparisonOperator,
  Expression,
  QueryResult,
  QueryRow,
  QueryValue,
  SelectItem,
} from "./query.js";
import { QueryMemoryContext, type QueryMemoryUsage } from "./memory.js";

const DEFAULT_BATCH_ROWS = 2_048;
const NULL_STRING_CODE = 0xffffffff;
const vectorTextEncoder = new TextEncoder();

export type VectorType = "boolean" | "number" | "string" | "datetime";

interface VectorBase {
  readonly validity: Uint8Array;
  readonly length: number;
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
  close(): void;
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
    };

interface BoundPredicate {
  readonly left: BoundExpression;
  readonly operator: ComparisonOperator;
  readonly right: BoundExpression;
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

interface AggregateAccumulator {
  count: number;
  value: QueryValue | undefined;
  sum: number;
}

interface GroupState {
  readonly groupValues: QueryValue[];
  readonly aggregates: AggregateAccumulator[];
}

interface BoundPlan {
  readonly sourceTables: readonly ColumnarTable[];
  readonly sourceAliases: readonly string[];
  readonly scanSource: number;
  readonly joins: readonly BoundJoin[];
  readonly predicates: readonly BoundPredicate[];
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
  uniqueRow(key: unknown): number;
  matches(key: unknown): readonly number[] | Int32Array;
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
  if (rowIndex < 0 || rowIndex >= vector.length || !isValid(vector.validity, rowIndex)) return null;
  if (vector.kind === "boolean") return vector.values[rowIndex] === 1;
  if (vector.kind === "number") return vector.values[rowIndex] ?? 0;
  if (vector.kind === "datetime") return new Date(vector.values[rowIndex] ?? 0);
  const code = vector.codes[rowIndex] ?? NULL_STRING_CODE;
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
  const predicates = plan.predicates.map((predicate) => ({
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
  memory.reserve(
    safeMemoryProduct(table.rowCount + 1, Int32Array.BYTES_PER_ELEMENT, "Hash join row indexes"),
    `Hash join ${table.name}`,
  );
  const firstRowByKey = new Map<unknown, number>();
  const duplicateRowsByKey = new Map<unknown, number[]>();
  const buildScratch = memory.reserve(
    safeMemoryProduct(source + 1, Int32Array.BYTES_PER_ELEMENT, "Hash join bind scratch"),
    `Hash join ${table.name} bind scratch`,
  );
  try {
    const rowBySource = new Int32Array(source + 1);
    rowBySource.fill(-1);
    for (let rowIndex = 0; rowIndex < table.rowCount; rowIndex += 1) {
      rowBySource[source] = rowIndex;
      const key = comparable(evaluateExpression(expression, rowBySource));
      if (key === null || key === undefined) continue;
      const firstRow = firstRowByKey.get(key);
      if (firstRow === undefined) firstRowByKey.set(key, rowIndex);
      else {
        const rows = duplicateRowsByKey.get(key) ?? [firstRow];
        rows.push(rowIndex);
        duplicateRowsByKey.set(key, rows);
      }
    }
  } finally {
    buildScratch.release();
  }
  const unique = duplicateRowsByKey.size === 0;
  const singleton = new Int32Array(1);
  return {
    unique,
    uniqueRow(key) {
      return firstRowByKey.get(comparable(key)) ?? -1;
    },
    matches(key) {
      const comparableKey = comparable(key);
      const duplicateRows = duplicateRowsByKey.get(comparableKey);
      if (duplicateRows !== undefined) return duplicateRows;
      const row = firstRowByKey.get(comparableKey);
      if (row === undefined) return [];
      singleton[0] = row;
      return singleton;
    },
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
    return { unique: true, uniqueRow: () => -1, matches: () => [] };
  }
  const range = maximum - minimum + 1;
  if (range > Math.max(1_024, vector.length * 4) || range > 10_000_000) return undefined;
  const reservation = memory.reserve(
    safeMemoryProduct(range + 1, Int32Array.BYTES_PER_ELEMENT, "Direct join lookup"),
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
  const singleton = new Int32Array(1);
  return {
    unique: true,
    uniqueRow(key) {
      if (typeof key !== "number" || !Number.isSafeInteger(key)) return -1;
      const slot = key - minimum;
      return slot < 0 || slot >= rowByKey.length ? -1 : (rowByKey[slot] ?? -1);
    },
    matches(key) {
      if (typeof key !== "number" || !Number.isSafeInteger(key)) return [];
      const slot = key - minimum;
      if (slot < 0 || slot >= rowByKey.length) return [];
      const row = rowByKey[slot] ?? -1;
      if (row < 0) return [];
      singleton[0] = row;
      return singleton;
    },
  };
}

function executeBoundPlan(plan: BoundPlan, memory: QueryMemoryContext): QueryResult {
  const metadataCount = executeMetadataCount(plan);
  if (metadataCount !== undefined) return metadataCount;
  const groups = new NestedGroupMap<GroupState>();
  if (plan.grouped && plan.groupBy.length === 0) groups.setEmpty(createGroupState([], plan));
  const output: QueryRow[] = [];
  const scanRows = plan.sourceTables[plan.scanSource]?.rowCount ?? 0;
  for (let start = 0; start < scanRows; start += DEFAULT_BATCH_ROWS) {
    const length = Math.min(DEFAULT_BATCH_ROWS, scanRows - start);
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
      const batch: BatchRows = { length, rowsBySource: sourceRows, memory: batchMemory };
      if (consumeJoinedBatches(plan, batch, 0, groups, output, memory)) break;
    } finally {
      batchMemory.close();
    }
  }
  const rows = plan.grouped ? finishGroups(plan, groups.values()) : output;
  return finishResult(plan, rows);
}

function executeMetadataCount(plan: BoundPlan): QueryResult | undefined {
  if (
    !plan.grouped ||
    plan.groupBy.length > 0 ||
    plan.joins.length > 0 ||
    plan.predicates.length > 0 ||
    plan.aggregates.length === 0 ||
    plan.aggregates.some(
      (aggregate) => aggregate.name !== "COUNT" || aggregate.argument.kind !== "wildcard",
    )
  ) {
    return undefined;
  }
  const state = createGroupState([], plan);
  const rowCount = plan.sourceTables[plan.scanSource]?.rowCount ?? 0;
  for (const accumulator of state.aggregates) accumulator.count = rowCount;
  return finishResult(plan, finishGroups(plan, [state]));
}

function finishResult(plan: BoundPlan, inputRows: QueryRow[]): QueryResult {
  let rows = inputRows;
  if (plan.orderBy.length > 0) {
    rows.sort((left, right) => {
      for (const order of plan.orderBy) {
        const comparison = compareValues(left[order.outputName], right[order.outputName]);
        if (comparison !== 0) return order.direction === "desc" ? -comparison : comparison;
      }
      return 0;
    });
  }
  if (plan.limit !== undefined) rows = rows.slice(0, plan.limit);
  const columns = plan.wildcard ? wildcardColumnNames(plan) : plan.select.map((item) => item.alias);
  return { columns, rows };
}

function consumeJoinedBatches(
  plan: BoundPlan,
  batch: BatchRows,
  joinIndex: number,
  groups: NestedGroupMap<GroupState>,
  output: QueryRow[],
  memory: QueryMemoryContext,
): boolean {
  const join = plan.joins[joinIndex];
  if (join === undefined) {
    consumeBatch(plan, batch, groups, output);
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
  const workspace = memory.reserve(
    safeMemoryProduct(
      safeMemoryProduct(
        plan.sourceTables.length,
        DEFAULT_BATCH_ROWS,
        "Join fan-out workspace slots",
      ),
      8,
      "Join fan-out workspace",
    ),
    "Join fan-out workspace",
  );
  try {
    let outputRows = plan.sourceTables.map(() => [] as number[]);
    for (let row = 0; row < input.length; row += 1) {
      const probeKey = evaluateBatchExpression(plan, join.probe, input, row);
      const matches = probeKey === null ? [] : join.lookup.matches(probeKey);
      if (matches.length === 0) {
        if (join.kind === "left") appendJoinedRow(outputRows, input, row, join.buildSource, -1);
      } else {
        for (const buildRow of matches) {
          appendJoinedRow(outputRows, input, row, join.buildSource, buildRow);
          if ((outputRows[0]?.length ?? 0) === DEFAULT_BATCH_ROWS) {
            yield materializeJoinedRows(outputRows, memory);
            outputRows = plan.sourceTables.map(() => [] as number[]);
          }
        }
      }
      if ((outputRows[0]?.length ?? 0) === DEFAULT_BATCH_ROWS) {
        yield materializeJoinedRows(outputRows, memory);
        outputRows = plan.sourceTables.map(() => [] as number[]);
      }
    }
    if ((outputRows[0]?.length ?? 0) > 0) yield materializeJoinedRows(outputRows, memory);
  } finally {
    workspace.release();
  }
}

function materializeJoinedRows(
  outputRows: readonly number[][],
  memory: QueryMemoryContext,
): BatchRows {
  const batchMemory = memory.createChild();
  try {
    const length = outputRows[0]?.length ?? 0;
    batchMemory.reserve(
      safeMemoryProduct(
        safeMemoryProduct(outputRows.length, length, "Joined batch row-index count"),
        Int32Array.BYTES_PER_ELEMENT,
        "Joined batch row indexes",
      ),
      "Joined batch row indexes",
    );
    return {
      length,
      rowsBySource: outputRows.map((rows) => Int32Array.from(rows)),
      memory: batchMemory,
    };
  } catch (error) {
    batchMemory.close();
    throw error;
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
      const buildRow = probeKey === null ? -1 : join.lookup.uniqueRow(probeKey);
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
  output: number[][],
  input: BatchRows,
  row: number,
  buildSource: number,
  buildRow: number,
): void {
  for (let source = 0; source < output.length; source += 1) {
    output[source]?.push(
      source === buildSource ? buildRow : (input.rowsBySource[source]?.[row] ?? -1),
    );
  }
}

function consumeBatch(
  plan: BoundPlan,
  batch: BatchRows,
  groups: NestedGroupMap<GroupState>,
  output: QueryRow[],
): void {
  for (let row = 0; row < batch.length; row += 1) {
    if (
      !plan.predicates.every((predicate) => evaluateBatchPredicate(plan, predicate, batch, row))
    ) {
      continue;
    }
    if (plan.grouped) {
      let state: GroupState | undefined;
      let groupKeys: unknown[] | undefined;
      if (plan.groupBy.length === 0) state = groups.getEmpty();
      else if (plan.groupBy.length === 1) {
        state = groups.getOne(
          groupKey(
            evaluateBatchExpression(
              plan,
              required(plan.groupBy[0], "Group expression is missing"),
              batch,
              row,
            ),
          ),
        );
      } else {
        groupKeys = plan.groupBy.map((expression) =>
          groupKey(evaluateBatchExpression(plan, expression, batch, row)),
        );
        state = groups.get(groupKeys);
      }
      if (state === undefined) {
        const groupValues = plan.groupBy.map((expression) =>
          asQueryValue(evaluateBatchExpression(plan, expression, batch, row)),
        );
        state = createGroupState(groupValues, plan);
        if (plan.groupBy.length === 1) groups.setOne(groupKey(groupValues[0]), state);
        else groups.set(groupKeys ?? [], state);
      }
      updateAggregates(plan, state, batch, row);
    } else {
      output.push(projectBatchRow(plan, batch, row));
      if (plan.orderBy.length === 0 && plan.limit !== undefined && output.length >= plan.limit)
        return;
    }
  }
}

function createGroupState(groupValues: QueryValue[], plan: BoundPlan): GroupState {
  return {
    groupValues,
    aggregates: plan.aggregates.map(() => ({ count: 0, value: undefined, sum: 0 })),
  };
}

function updateAggregates(plan: BoundPlan, state: GroupState, batch: BatchRows, row: number): void {
  for (let index = 0; index < plan.aggregates.length; index += 1) {
    const spec = required(plan.aggregates[index], "Aggregate specification is missing");
    const accumulator = required(state.aggregates[index], "Aggregate accumulator is missing");
    const value =
      spec.argument.kind === "wildcard"
        ? 1
        : evaluateBatchExpression(plan, spec.argument, batch, row);
    if (value === null || value === undefined) continue;
    accumulator.count += 1;
    if (spec.name === "SUM" || spec.name === "AVG") accumulator.sum += numeric(value);
    else if (
      spec.name === "MIN" &&
      (accumulator.value === undefined || compareValues(value, accumulator.value) < 0)
    ) {
      accumulator.value = asQueryValue(value);
    } else if (
      spec.name === "MAX" &&
      (accumulator.value === undefined || compareValues(value, accumulator.value) > 0)
    ) {
      accumulator.value = asQueryValue(value);
    }
  }
}

function finishGroups(plan: BoundPlan, groups: readonly GroupState[]): QueryRow[] {
  return groups.map((group) =>
    Object.fromEntries(
      plan.select.map((item) => [
        item.alias,
        asQueryValue(evaluateFinalExpression(plan, item.expression, group)),
      ]),
    ),
  );
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
  const accumulator = group.aggregates[expression.aggregateIndex ?? -1];
  if (accumulator === undefined) throw new Error("Aggregate state is missing");
  if (expression.name === "COUNT") return accumulator.count;
  if (accumulator.count === 0) return null;
  if (expression.name === "SUM") return accumulator.sum;
  if (expression.name === "AVG") return accumulator.sum / accumulator.count;
  return accumulator.value ?? null;
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

function evaluateBatchPredicate(
  plan: BoundPlan,
  predicate: BoundPredicate,
  batch: BatchRows,
  row: number,
): boolean {
  return comparisonValue(
    predicate.operator,
    evaluateBatchExpression(plan, predicate.left, batch, row),
    evaluateBatchExpression(plan, predicate.right, batch, row),
  );
}

function evaluateBatchExpression(
  plan: BoundPlan,
  expression: BoundExpression,
  batch: BatchRows,
  row: number,
): unknown {
  if (expression.kind === "literal") return expression.value;
  if (expression.kind === "wildcard") return 1;
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
  operator: ComparisonOperator,
  leftValue: unknown,
  rightValue: unknown,
): boolean {
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

function groupKey(value: unknown): unknown {
  return typeof value === "number" && !Number.isFinite(value) ? null : comparable(value);
}

function compareValues(left: unknown, right: unknown): number {
  const a = comparable(left);
  const b = comparable(right);
  if (a === b) return 0;
  if (a === null || a === undefined) return -1;
  if (b === null || b === undefined) return 1;
  if (typeof a === "number" && typeof b === "number") return a - b;
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

class NestedGroupMap<T> {
  readonly #root = new Map<unknown, unknown>();
  readonly #values: T[] = [];

  get(keys: readonly unknown[]): T | undefined {
    if (keys.length === 0) return this.#root.get(NestedGroupMap) as T | undefined;
    let node: unknown = this.#root;
    for (const key of keys) {
      if (!(node instanceof Map)) return undefined;
      node = node.get(key);
    }
    return node as T | undefined;
  }

  getEmpty(): T | undefined {
    return this.#root.get(NestedGroupMap) as T | undefined;
  }

  getOne(key: unknown): T | undefined {
    return this.#root.get(key) as T | undefined;
  }

  set(keys: readonly unknown[], value: T): void {
    if (keys.length === 0) {
      if (!this.#root.has(NestedGroupMap)) this.#values.push(value);
      this.#root.set(NestedGroupMap, value);
      return;
    }
    let node = this.#root;
    for (let index = 0; index < keys.length - 1; index += 1) {
      const key = keys[index];
      const existing = node.get(key);
      if (existing instanceof Map) node = existing as Map<unknown, unknown>;
      else {
        const child = new Map<unknown, unknown>();
        node.set(key, child);
        node = child;
      }
    }
    const finalKey = keys[keys.length - 1];
    if (!node.has(finalKey)) this.#values.push(value);
    node.set(finalKey, value);
  }

  setEmpty(value: T): void {
    if (!this.#root.has(NestedGroupMap)) this.#values.push(value);
    this.#root.set(NestedGroupMap, value);
  }

  setOne(key: unknown, value: T): void {
    if (!this.#root.has(key)) this.#values.push(value);
    this.#root.set(key, value);
  }

  values(): readonly T[] {
    return this.#values;
  }
}
