import type { QueryResult, QueryValue } from "../engine/index.js";

export type SqlLogicType = "I" | "R" | "T";
export type SqlLogicSortMode = "nosort" | "rowsort" | "valuesort";

export interface SqlLogicLocation {
  readonly file: string;
  readonly line: number;
}

export interface SqlLogicCondition {
  readonly kind: "onlyif" | "skipif";
  readonly engine: string;
  readonly location: SqlLogicLocation;
}

interface LocatedRecord {
  readonly location: SqlLogicLocation;
}

export interface SqlLogicStatement extends LocatedRecord {
  readonly kind: "statement";
  readonly expectation: "ok" | "error";
  readonly sql: string;
  readonly conditions: readonly SqlLogicCondition[];
}

export interface SqlLogicExpectedValues {
  readonly kind: "values";
  readonly values: readonly string[];
}

export interface SqlLogicExpectedHash {
  readonly kind: "hash";
  readonly valueCount: number;
  readonly hash: string;
}

export interface SqlLogicQuery extends LocatedRecord {
  readonly kind: "query";
  readonly types: readonly SqlLogicType[];
  readonly sortMode: SqlLogicSortMode;
  readonly label?: string;
  readonly sql: string;
  readonly expected: SqlLogicExpectedValues | SqlLogicExpectedHash;
  readonly conditions: readonly SqlLogicCondition[];
}

export interface SqlLogicHashThreshold extends LocatedRecord {
  readonly kind: "hash-threshold";
  readonly valueCount: number;
}

export interface SqlLogicHalt extends LocatedRecord {
  readonly kind: "halt";
}

export type SqlLogicRecord =
  SqlLogicStatement | SqlLogicQuery | SqlLogicHashThreshold | SqlLogicHalt;

export interface SqlLogicDatabase {
  readonly engineName: string;
  execute(sql: string): Promise<unknown>;
  query(sql: string): Promise<QueryResult>;
  close(): void | Promise<void>;
}

export interface SqlLogicRunStatistics {
  readonly files: number;
  readonly statements: number;
  readonly queries: number;
  readonly values: number;
  readonly skipped: number;
  readonly halted: boolean;
  readonly hashThreshold: number;
}

export interface SqlLogicRunOptions {
  /** Audit hook. Returning "continue" records the failure externally and runs the next record. */
  readonly onFailure?: (
    failure: SqlLogicFailure,
    record: SqlLogicStatement | SqlLogicQuery,
  ) => "continue" | undefined;
}

export class SqlLogicParseError extends Error {
  constructor(
    message: string,
    readonly location: SqlLogicLocation,
  ) {
    super(`${location.file}:${String(location.line)}: ${message}`);
    this.name = "SqlLogicParseError";
  }
}

export class SqlLogicFailure extends Error {
  constructor(
    message: string,
    readonly location: SqlLogicLocation,
    readonly sql?: string,
    options?: ErrorOptions,
  ) {
    super(
      `${location.file}:${String(location.line)}: ${message}${sql === undefined ? "" : `\n${sql}`}`,
      options,
    );
    this.name = "SqlLogicFailure";
  }
}

interface SourceLine {
  readonly text: string;
  readonly number: number;
}

interface RecordGroup {
  readonly lines: readonly SourceLine[];
}

/** Parses the original SQLite SQLLogicTest format, rejecting unknown extensions. */
export function parseSqlLogicTest(source: string, file = "<memory>"): SqlLogicRecord[] {
  const groups = splitRecordGroups(source);
  return groups.map((group) => parseRecord(group, file));
}

/**
 * Parses a line source one record at a time. The full upstream corpus contains very large files;
 * this form bounds parser memory to the largest individual record instead of the largest file.
 */
export async function* parseSqlLogicTestLines(
  lines: Iterable<string> | AsyncIterable<string>,
  file = "<stream>",
): AsyncGenerator<SqlLogicRecord> {
  let current: SourceLine[] = [];
  let lineNumber = 0;
  const finish = (): SqlLogicRecord | undefined => {
    if (current.length === 0) return undefined;
    const record = parseRecord({ lines: current }, file);
    current = [];
    return record;
  };

  for await (const original of lines) {
    lineNumber++;
    const text = original.endsWith("\r") ? original.slice(0, -1) : original;
    if (text.startsWith("#")) continue;
    if (text.trim().length === 0) {
      const record = finish();
      if (record !== undefined) yield record;
      continue;
    }
    current.push({ text, number: lineNumber });
  }
  const record = finish();
  if (record !== undefined) yield record;
}

function splitRecordGroups(source: string): RecordGroup[] {
  const lines = source.replaceAll("\r\n", "\n").replaceAll("\r", "\n").split("\n");
  const groups: RecordGroup[] = [];
  let current: SourceLine[] = [];

  const finish = (): void => {
    if (current.length > 0) groups.push({ lines: current });
    current = [];
  };

  for (const [index, text] of lines.entries()) {
    if (text.startsWith("#")) continue;
    if (text.trim().length === 0) {
      finish();
      continue;
    }
    current.push({ text, number: index + 1 });
  }
  finish();
  return groups;
}

function parseRecord(group: RecordGroup, file: string): SqlLogicRecord {
  let index = 0;
  const conditions: SqlLogicCondition[] = [];
  while (index < group.lines.length) {
    const line = requireLine(group, index, file);
    const tokens = tokenizeDirective(line.text);
    if (tokens[0] !== "skipif" && tokens[0] !== "onlyif") break;
    if (tokens.length !== 2) fail("conditional must name exactly one engine", file, line.number);
    conditions.push({
      kind: tokens[0],
      engine: tokens[1] ?? "",
      location: { file, line: line.number },
    });
    index++;
  }

  const header = requireLine(group, index, file);
  const tokens = tokenizeDirective(header.text);
  const location = { file, line: header.number };
  if (tokens[0] === "statement") {
    if (tokens.length !== 2 || (tokens[1] !== "ok" && tokens[1] !== "error")) {
      fail("statement must be 'statement ok' or 'statement error'", file, header.number);
    }
    const sql = sqlLines(group.lines.slice(index + 1));
    if (sql.length === 0) fail("statement has no SQL", file, header.number);
    return {
      kind: "statement",
      expectation: tokens[1],
      sql,
      conditions,
      location,
    };
  }

  if (tokens[0] === "query") {
    if (tokens.length < 2 || tokens.length > 4) {
      fail("query must be 'query <types> [sort-mode] [label]'", file, header.number);
    }
    const typeString = tokens[1] ?? "";
    const types: string[] = [];
    for (const type of typeString) types.push(type);
    if (types.length === 0 || types.some((type) => type !== "I" && type !== "R" && type !== "T")) {
      fail("query type string must contain only I, R, and T", file, header.number);
    }
    const sortMode = tokens[2] ?? "nosort";
    if (sortMode !== "nosort" && sortMode !== "rowsort" && sortMode !== "valuesort") {
      fail(`unknown query sort mode '${sortMode}'`, file, header.number);
    }
    const body = group.lines.slice(index + 1);
    const separator = body.findIndex((line) => line.text === "----");
    const queryLines = separator < 0 ? body : body.slice(0, separator);
    const sql = sqlLines(queryLines);
    if (sql.length === 0) fail("query has no SQL", file, header.number);
    const expectedLines = separator < 0 ? [] : body.slice(separator + 1).map((line) => line.text);
    return {
      kind: "query",
      types: types as SqlLogicType[],
      sortMode,
      ...(tokens[3] === undefined ? {} : { label: tokens[3] }),
      sql,
      expected: parseExpected(expectedLines, file, header.number),
      conditions,
      location,
    };
  }

  if (conditions.length > 0) {
    fail("conditionals may prefix only a statement or query", file, header.number);
  }
  if (tokens[0] === "hash-threshold") {
    if (tokens.length !== 2 || !isNonNegativeInteger(tokens[1])) {
      fail("hash-threshold must be a non-negative whole number", file, header.number);
    }
    return { kind: "hash-threshold", valueCount: Number(tokens[1]), location };
  }
  if (tokens[0] === "halt") {
    if (tokens.length !== 1 || group.lines.length !== 1) {
      fail("halt does not accept arguments", file, header.number);
    }
    return { kind: "halt", location };
  }
  fail(`unknown SQLLogicTest directive '${tokens[0] ?? ""}'`, file, header.number);
}

function parseExpected(
  lines: readonly string[],
  file: string,
  line: number,
): SqlLogicExpectedValues | SqlLogicExpectedHash {
  if (lines.length !== 1) return { kind: "values", values: lines };
  const match = /^(\d+) values hashing to ([0-9a-fA-F]{32})$/.exec(lines[0] ?? "");
  if (match === null) return { kind: "values", values: lines };
  const valueCount = Number(match[1]);
  if (!Number.isSafeInteger(valueCount)) fail("hashed value count is too large", file, line);
  return { kind: "hash", valueCount, hash: (match[2] ?? "").toLowerCase() };
}

function tokenizeDirective(line: string): string[] {
  return line.trim().split(/\s+/u);
}

function sqlLines(lines: readonly SourceLine[]): string {
  return lines.map((line) => line.text).join("\n");
}

function requireLine(group: RecordGroup, index: number, file: string): SourceLine {
  const line = group.lines[index];
  if (line === undefined) {
    const last = group.lines.at(-1)?.number ?? 1;
    fail("conditional is missing its statement or query", file, last);
  }
  return line;
}

function isNonNegativeInteger(value: string | undefined): value is string {
  return value !== undefined && /^\d+$/u.test(value) && Number.isSafeInteger(Number(value));
}

function fail(message: string, file: string, line: number): never {
  throw new SqlLogicParseError(message, { file, line });
}

/** Runs already-parsed records against one empty database connection. */
export async function runSqlLogicTest(
  records: Iterable<SqlLogicRecord> | AsyncIterable<SqlLogicRecord>,
  database: SqlLogicDatabase,
  options: SqlLogicRunOptions = {},
): Promise<SqlLogicRunStatistics> {
  let statements = 0;
  let queries = 0;
  let values = 0;
  let skipped = 0;
  let hashThreshold = 8;
  let halted = false;
  const labels = new Map<string, { hash: string; location: SqlLogicLocation }>();

  try {
    for await (const record of records) {
      if (record.kind === "hash-threshold") {
        hashThreshold = record.valueCount;
        continue;
      }
      if (record.kind === "halt") {
        halted = true;
        break;
      }
      if (shouldSkip(record.conditions, database.engineName)) {
        skipped++;
        if (record.kind === "query" && record.label !== undefined) {
          rememberExpectedLabel(record, labels);
        }
        continue;
      }
      try {
        if (record.kind === "statement") {
          statements++;
          await runStatement(record, database);
          continue;
        }
        queries++;
        const actual = await runQuery(record, database);
        values += actual.length;
        compareExpected(record, actual, hashThreshold);
        rememberActualLabel(record, actual, labels);
      } catch (error) {
        if (error instanceof SqlLogicFailure && options.onFailure?.(error, record) === "continue") {
          continue;
        }
        throw error;
      }
    }
  } finally {
    await database.close();
  }

  return { files: 1, statements, queries, values, skipped, halted, hashThreshold };
}

function shouldSkip(conditions: readonly SqlLogicCondition[], engineName: string): boolean {
  const engine = engineName.toLowerCase();
  return conditions.some((condition) => {
    const matches = condition.engine.toLowerCase() === engine;
    return condition.kind === "skipif" ? matches : !matches;
  });
}

async function runStatement(record: SqlLogicStatement, database: SqlLogicDatabase): Promise<void> {
  let failure: unknown;
  try {
    await database.execute(record.sql);
  } catch (error) {
    failure = error;
  }
  if (record.expectation === "error") {
    if (failure !== undefined) return;
    throw new SqlLogicFailure(
      "statement succeeded; expected an error",
      record.location,
      record.sql,
    );
  }
  if (failure !== undefined) {
    throw new SqlLogicFailure("statement failed; expected success", record.location, record.sql, {
      cause: failure,
    });
  }
}

async function runQuery(record: SqlLogicQuery, database: SqlLogicDatabase): Promise<string[]> {
  let result: QueryResult;
  try {
    result = await database.query(record.sql);
  } catch (cause) {
    throw new SqlLogicFailure("query failed", record.location, record.sql, { cause });
  }
  if (result.columns.length !== record.types.length) {
    throw new SqlLogicFailure(
      `wrong column count: expected ${String(record.types.length)}, received ${String(result.columns.length)}`,
      record.location,
      record.sql,
    );
  }

  const rows = result.rows.map((row) =>
    result.columns.map((column, index) =>
      renderSqlLogicValue(row[column] ?? null, record.types[index]),
    ),
  );
  if (record.sortMode === "rowsort") rows.sort(compareRows);
  const flattened = rows.flat();
  if (record.sortMode === "valuesort") flattened.sort(compareText);
  return flattened;
}

function compareRows(left: readonly string[], right: readonly string[]): number {
  for (let index = 0; index < left.length; index++) {
    const comparison = compareText(left[index] ?? "", right[index] ?? "");
    if (comparison !== 0) return comparison;
  }
  return 0;
}

function compareText(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

/** Renders one value with the original SQLite SQLLogicTest adapter rules. */
export function renderSqlLogicValue(value: QueryValue, type: SqlLogicType | undefined): string {
  if (value === null) return "NULL";
  if (type === "I") {
    const numeric = typeof value === "boolean" ? Number(value) : Number(value);
    if (!Number.isFinite(numeric)) return "0";
    return String(Math.trunc(numeric));
  }
  if (type === "R") {
    const numeric = typeof value === "boolean" ? Number(value) : Number(value);
    return Number.isFinite(numeric) ? numeric.toFixed(3) : "0.000";
  }
  const rendered = value instanceof Date ? value.toISOString() : String(value);
  if (rendered.length === 0) return "(empty)";
  return rendered.replaceAll(/[^ -~]/gu, "@");
}

function compareExpected(
  record: SqlLogicQuery,
  actual: readonly string[],
  hashThreshold: number,
): void {
  const hashed = hashThreshold > 0 && actual.length > hashThreshold;
  if (hashed) {
    if (record.expected.kind !== "hash") {
      throw new SqlLogicFailure(
        `result exceeds hash-threshold ${String(hashThreshold)} but the expected result is not hashed`,
        record.location,
        record.sql,
      );
    }
    const hash = md5Hex(withTrailingNewlines(actual));
    if (record.expected.valueCount !== actual.length || record.expected.hash !== hash) {
      throw new SqlLogicFailure(
        `wrong result hash: expected ${String(record.expected.valueCount)} values hashing to ${record.expected.hash}, received ${String(actual.length)} values hashing to ${hash}`,
        record.location,
        record.sql,
      );
    }
    return;
  }
  if (record.expected.kind === "hash") {
    throw new SqlLogicFailure(
      `expected result is hashed but ${String(actual.length)} values do not exceed hash-threshold ${String(hashThreshold)}`,
      record.location,
      record.sql,
    );
  }
  if (!equalValues(record.expected.values, actual)) {
    const expected = describeValues(record.expected.values, hashThreshold);
    const received = describeValues(actual, hashThreshold);
    throw new SqlLogicFailure(
      `wrong result:\nexpected ${expected}\nreceived ${received}`,
      record.location,
      record.sql,
    );
  }
}

function equalValues(expected: readonly string[], actual: readonly string[]): boolean {
  return (
    expected.length === actual.length && expected.every((value, index) => value === actual[index])
  );
}

function describeValues(values: readonly string[], hashThreshold: number): string {
  if (hashThreshold > 0 && values.length > hashThreshold) {
    return `${String(values.length)} values hashing to ${md5Hex(withTrailingNewlines(values))}`;
  }
  return JSON.stringify(values);
}

function rememberExpectedLabel(
  record: SqlLogicQuery,
  labels: Map<string, { hash: string; location: SqlLogicLocation }>,
): void {
  if (record.label === undefined) return;
  const hash =
    record.expected.kind === "hash"
      ? record.expected.hash
      : md5Hex(withTrailingNewlines(record.expected.values));
  rememberLabel(record.label, hash, record.location, labels);
}

function rememberActualLabel(
  record: SqlLogicQuery,
  values: readonly string[],
  labels: Map<string, { hash: string; location: SqlLogicLocation }>,
): void {
  if (record.label === undefined) return;
  rememberLabel(record.label, md5Hex(withTrailingNewlines(values)), record.location, labels);
}

function rememberLabel(
  label: string,
  hash: string,
  location: SqlLogicLocation,
  labels: Map<string, { hash: string; location: SqlLogicLocation }>,
): void {
  const previous = labels.get(label);
  if (previous !== undefined && previous.hash !== hash) {
    throw new SqlLogicFailure(
      `labeled result '${label}' differs from ${previous.location.file}:${String(previous.location.line)}`,
      location,
    );
  }
  labels.set(label, { hash, location });
}

function withTrailingNewlines(values: readonly string[]): string {
  return values.map((value) => `${value}\n`).join("");
}

/** Small browser-safe MD5 implementation for compatibility with the original corpus hashes. */
export function md5Hex(input: string): string {
  const bytes = new TextEncoder().encode(input);
  const paddedLength = Math.ceil((bytes.length + 9) / 64) * 64;
  const padded = new Uint8Array(paddedLength);
  padded.set(bytes);
  padded[bytes.length] = 0x80;
  const bitLength = BigInt(bytes.length) * 8n;
  const view = new DataView(padded.buffer);
  view.setUint32(paddedLength - 8, Number(bitLength & 0xffffffffn), true);
  view.setUint32(paddedLength - 4, Number((bitLength >> 32n) & 0xffffffffn), true);

  let a0 = 0x67452301;
  let b0 = 0xefcdab89;
  let c0 = 0x98badcfe;
  let d0 = 0x10325476;
  const words = new Uint32Array(16);

  for (let offset = 0; offset < paddedLength; offset += 64) {
    for (let index = 0; index < 16; index++)
      words[index] = view.getUint32(offset + index * 4, true);
    let a = a0;
    let b = b0;
    let c = c0;
    let d = d0;
    for (let index = 0; index < 64; index++) {
      const { mix, word } = md5Round(index, b, c, d);
      const rotated = rotateLeft(
        (a + mix + (MD5_CONSTANTS[index] ?? 0) + (words[word] ?? 0)) >>> 0,
        MD5_SHIFTS[index],
      );
      a = d;
      d = c;
      c = b;
      b = (b + rotated) >>> 0;
    }
    a0 = (a0 + a) >>> 0;
    b0 = (b0 + b) >>> 0;
    c0 = (c0 + c) >>> 0;
    d0 = (d0 + d) >>> 0;
  }
  return [a0, b0, c0, d0].map(littleEndianHex).join("");
}

function md5Round(index: number, b: number, c: number, d: number): { mix: number; word: number } {
  if (index < 16) return { mix: (b & c) | (~b & d), word: index };
  if (index < 32) return { mix: (d & b) | (~d & c), word: (5 * index + 1) % 16 };
  if (index < 48) return { mix: b ^ c ^ d, word: (3 * index + 5) % 16 };
  return { mix: c ^ (b | ~d), word: (7 * index) % 16 };
}

function rotateLeft(value: number, shift: number | undefined): number {
  const bits = shift ?? 0;
  return ((value << bits) | (value >>> (32 - bits))) >>> 0;
}

function littleEndianHex(value: number): string {
  let output = "";
  for (let index = 0; index < 4; index++) {
    output += ((value >>> (index * 8)) & 0xff).toString(16).padStart(2, "0");
  }
  return output;
}

const MD5_SHIFTS = [
  7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14,
  20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10, 15, 21, 6,
  10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
] as const;

const MD5_CONSTANTS = Array.from({ length: 64 }, (_, index) =>
  Math.floor(Math.abs(Math.sin(index + 1)) * 2 ** 32),
);
