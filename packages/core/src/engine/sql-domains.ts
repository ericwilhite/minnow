import type { SqlDomain } from "../storage/types.js";
import { assertWellFormedString } from "../block-format/unicode.js";
import { dateIsoString } from "../date-value.js";
import {
  MAX_SQL_SCALAR_RESULT_CHARACTERS,
  MAX_SQL_NUMERIC_DIGITS,
  MAX_SQL_STRUCTURED_VALUE_DEPTH,
  MAX_SQL_STRUCTURED_VALUE_ITEMS,
} from "./cache-limits.js";

/** Internal string encodings for PostgreSQL value domains carried by string vectors. */
const PREFIX = "\u0000minnow-domain:";
const TEXT_VALUE = `${PREFIX}text:`;
const NUMERIC = `${PREFIX}numeric:`;
const JSON_VALUE = `${PREFIX}json:`;
const JSONB_VALUE = `${PREFIX}jsonb:`;
const UUID_VALUE = `${PREFIX}uuid:`;
const DATE_VALUE = `${PREFIX}date:`;
const TIME_VALUE = `${PREFIX}time:`;
const INTERVAL_VALUE = `${PREFIX}interval:`;
const ARRAY_VALUE = `${PREFIX}array:`;
const COLLATION_VALUE = `${PREFIX}collation:`;
const ENUM_VALUE = `${PREFIX}enum:`;

/**
 * Protects an ordinary SQL string that happens to use the internal domain namespace. Physical
 * TEXT remains unchanged on disk; this wrapper is applied at execution boundaries so a user
 * value can never be mistaken for NUMERIC, INTERVAL, enum, or another tagged logical value.
 */
export function protectedSqlTextValue(value: string): string {
  return value.startsWith(PREFIX) ? TEXT_VALUE + value : value;
}

/** Removes only the ordinary-TEXT wrapper, leaving real domain values tagged. */
export function externalSqlTextValue(value: unknown): unknown {
  return typeof value === "string" && value.startsWith(TEXT_VALUE)
    ? value.slice(TEXT_VALUE.length)
    : value;
}

interface DecimalParts {
  coefficient: bigint;
  scale: number;
}

function pow10(exponent: number): bigint {
  if (!Number.isSafeInteger(exponent) || exponent < 0 || exponent > 100_000) {
    throw new RangeError(`Decimal scale is outside the supported range: ${String(exponent)}`);
  }
  // Retaining every intermediate power up to 100,000 consumes quadratic memory in the number
  // of decimal digits. Native exponentiation constructs only the requested result.
  return 10n ** BigInt(exponent);
}

function decimalParts(value: string | number): DecimalParts {
  const source = String(value).trim();
  assertBoundedDomainString(source, "NUMERIC value");
  const match = /^([+-]?)(?:(\d+)(?:\.(\d*))?|\.(\d+))(?:[eE]([+-]?\d+))?$/.exec(source);
  if (match === null) throw new TypeError(`Invalid NUMERIC value: ${String(value)}`);
  const fraction = match[3] ?? match[4] ?? "";
  const digits = `${match[2] ?? "0"}${fraction}`.replace(/^0+(?=\d)/, "");
  if (digits.length > MAX_SQL_NUMERIC_DIGITS) {
    throw new RangeError(
      `NUMERIC value exceeds ${String(MAX_SQL_NUMERIC_DIGITS)} significant digits`,
    );
  }
  const exponent = Number(match[5] ?? 0);
  if (!Number.isSafeInteger(exponent)) throw new RangeError(`Invalid NUMERIC exponent: ${source}`);
  let coefficient = BigInt(digits || "0") * (match[1] === "-" ? -1n : 1n);
  let scale = fraction.length - exponent;
  if (scale < 0) {
    coefficient *= pow10(-scale);
    scale = 0;
  }
  return { coefficient, scale };
}

function normalizeDecimal(parts: DecimalParts): DecimalParts {
  let { coefficient, scale } = parts;
  while (scale > 0 && coefficient % 10n === 0n) {
    coefficient /= 10n;
    scale -= 1;
  }
  return { coefficient, scale };
}

function rescaleDecimal(parts: DecimalParts, scale: number): DecimalParts {
  if (!Number.isSafeInteger(scale) || scale < 0 || scale > 100_000) {
    throw new RangeError(`NUMERIC scale is outside the supported range: ${String(scale)}`);
  }
  if (parts.scale <= scale) {
    return { coefficient: parts.coefficient * pow10(scale - parts.scale), scale };
  }
  const divisor = pow10(parts.scale - scale);
  let quotient = parts.coefficient / divisor;
  const remainder = parts.coefficient % divisor;
  if ((remainder < 0n ? -remainder : remainder) * 2n >= divisor) {
    quotient += parts.coefficient < 0n ? -1n : 1n;
  }
  return { coefficient: quotient, scale };
}

function formatDecimal(parts: DecimalParts): string {
  const negative = parts.coefficient < 0n;
  const digits = (negative ? -parts.coefficient : parts.coefficient).toString();
  if (parts.scale === 0) return `${negative ? "-" : ""}${digits}`;
  const padded = digits.padStart(parts.scale + 1, "0");
  const split = padded.length - parts.scale;
  return `${negative ? "-" : ""}${padded.slice(0, split)}.${padded.slice(split)}`;
}

function taggedDecimalParts(value: unknown): DecimalParts | undefined {
  if (typeof value !== "string" || !value.startsWith(NUMERIC)) return undefined;
  return decimalParts(value.slice(NUMERIC.length));
}

export function isExactNumeric(value: unknown): value is string {
  return taggedDecimalParts(value) !== undefined;
}

export function exactNumericValue(
  value: unknown,
  precision?: number,
  scale?: number,
): string | null {
  if (value === null || value === undefined) return null;
  const raw =
    taggedDecimalParts(value) ??
    (typeof value === "number" || typeof value === "string" ? decimalParts(value) : undefined);
  if (raw === undefined) throw new TypeError("NUMERIC accepts a number or decimal string");
  const adjusted = scale === undefined ? normalizeDecimal(raw) : rescaleDecimal(raw, scale);
  if (precision !== undefined) {
    if (!Number.isSafeInteger(precision) || precision < 1 || precision > 100_000) {
      throw new RangeError(
        `NUMERIC precision is outside the supported range: ${String(precision)}`,
      );
    }
    const declaredScale = scale ?? 0;
    if (declaredScale > precision) {
      throw new TypeError("NUMERIC scale cannot exceed its precision");
    }
    const integer = adjusted.coefficient < 0n ? -adjusted.coefficient : adjusted.coefficient;
    const integerDigits = (integer / pow10(adjusted.scale)).toString().length;
    if (integer !== 0n && integerDigits > precision - declaredScale) {
      throw new RangeError(`NUMERIC(${String(precision)}, ${String(declaredScale)}) overflow`);
    }
  }
  return boundedTaggedDomainValue(
    NUMERIC,
    formatDecimal(normalizeDecimal(adjusted)),
    "NUMERIC value",
  );
}

export function exactNumericBinary(
  operator: "+" | "-" | "*" | "/" | "%",
  left: unknown,
  right: unknown,
): string | null | undefined {
  const leftTagged = taggedDecimalParts(left);
  const rightTagged = taggedDecimalParts(right);
  if (leftTagged === undefined && rightTagged === undefined) return undefined;
  const a = leftTagged ?? decimalParts(typeof left === "number" ? left : String(left));
  const b = rightTagged ?? decimalParts(typeof right === "number" ? right : String(right));
  if ((operator === "/" || operator === "%") && b.coefficient === 0n) return null;
  let result: DecimalParts;
  if (operator === "+" || operator === "-") {
    const scale = Math.max(a.scale, b.scale);
    const ac = a.coefficient * pow10(scale - a.scale);
    const bc = b.coefficient * pow10(scale - b.scale);
    result = { coefficient: operator === "+" ? ac + bc : ac - bc, scale };
  } else if (operator === "*") {
    result = { coefficient: a.coefficient * b.coefficient, scale: a.scale + b.scale };
  } else if (operator === "%") {
    const scale = Math.max(a.scale, b.scale);
    result = {
      coefficient:
        (a.coefficient * pow10(scale - a.scale)) % (b.coefficient * pow10(scale - b.scale)),
      scale,
    };
  } else {
    // PostgreSQL NUMERIC division is arbitrary precision. Twenty fractional digits gives a
    // deterministic exact decimal rounding boundary without ever crossing binary Float64.
    const scale = Math.max(20, a.scale, b.scale);
    const numerator = a.coefficient * pow10(scale + b.scale);
    const denominator = b.coefficient * pow10(a.scale);
    let coefficient = numerator / denominator;
    const remainder = numerator % denominator;
    const magnitude = denominator < 0n ? -denominator : denominator;
    if ((remainder < 0n ? -remainder : remainder) * 2n >= magnitude) {
      coefficient += numerator < 0n !== denominator < 0n ? -1n : 1n;
    }
    result = { coefficient, scale };
  }
  return boundedTaggedDomainValue(
    NUMERIC,
    formatDecimal(normalizeDecimal(result)),
    "NUMERIC result",
  );
}

export function exactNumericCompare(left: unknown, right: unknown): number | undefined {
  const leftTagged = taggedDecimalParts(left);
  const rightTagged = taggedDecimalParts(right);
  if (leftTagged === undefined && rightTagged === undefined) return undefined;
  const a =
    leftTagged ??
    (typeof left === "number" || typeof left === "string" ? decimalParts(left) : undefined);
  const b =
    rightTagged ??
    (typeof right === "number" || typeof right === "string" ? decimalParts(right) : undefined);
  if (a === undefined || b === undefined) return undefined;
  const scale = Math.max(a.scale, b.scale);
  const ac = a.coefficient * pow10(scale - a.scale);
  const bc = b.coefficient * pow10(scale - b.scale);
  return ac === bc ? 0 : ac < bc ? -1 : 1;
}

interface JsonEncodingState {
  readonly active: Set<object>;
  readonly pieces: string[];
  items: number;
  length: number;
}

/**
 * Serializes a JSON value while bounding traversal, nesting, and output before concatenation.
 * Canonical mode sorts object names directly in the wire text (including integer-looking names,
 * which JavaScript object enumeration would otherwise silently reorder).
 */
export function boundedJsonText(value: unknown, canonical: boolean, label = "JSON value"): string {
  const state: JsonEncodingState = { active: new Set(), pieces: [], items: 0, length: 0 };
  encodeBoundedJson(value, canonical, label, 0, state);
  return state.pieces.join("");
}

function encodeBoundedJson(
  value: unknown,
  canonical: boolean,
  label: string,
  depth: number,
  state: JsonEncodingState,
): void {
  state.items += 1;
  if (state.items > MAX_SQL_STRUCTURED_VALUE_ITEMS) {
    throw new RangeError(
      `${label} cannot exceed ${String(MAX_SQL_STRUCTURED_VALUE_ITEMS)} values and names`,
    );
  }
  if (depth > MAX_SQL_STRUCTURED_VALUE_DEPTH) {
    throw new RangeError(`${label} cannot exceed ${String(MAX_SQL_STRUCTURED_VALUE_DEPTH)} levels`);
  }
  if (value === null) {
    appendJsonPiece(state, "null", label);
    return;
  }
  if (typeof value === "boolean") {
    appendJsonPiece(state, value ? "true" : "false", label);
    return;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("JSON numbers must be finite");
    appendJsonPiece(state, JSON.stringify(value), label);
    return;
  }
  if (typeof value === "string") {
    assertBoundedDomainString(value, label);
    appendJsonPiece(state, JSON.stringify(value), label);
    return;
  }
  if (value instanceof Date) {
    const timestamp = dateIsoString(value);
    appendJsonPiece(state, JSON.stringify(timestamp), label);
    return;
  }
  if (typeof value !== "object") throw new TypeError(`${label} cannot be represented as JSON`);
  const object = value;
  if (state.active.has(object)) throw new TypeError(`${label} cannot contain a cycle`);
  state.active.add(object);
  try {
    if (Array.isArray(value)) {
      appendJsonPiece(state, "[", label);
      for (let index = 0; index < value.length; index += 1) {
        if (index > 0) appendJsonPiece(state, ",", label);
        encodeBoundedJson(value[index], canonical, label, depth + 1, state);
      }
      appendJsonPiece(state, "]", label);
      return;
    }
    const prototype: unknown = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError(`${label} accepts only JSON objects, arrays, and scalar values`);
    }
    const record = value as Record<string, unknown>;
    // Object.keys() would allocate one array slot per property before the item cap can fire.
    // Plain JSON objects need no prototype walk, so collect at most the admitted count instead.
    const keys: string[] = [];
    for (const key in record) {
      if (!Object.hasOwn(record, key)) continue;
      if (keys.length >= MAX_SQL_STRUCTURED_VALUE_ITEMS - state.items) {
        throw new RangeError(
          `${label} cannot exceed ${String(MAX_SQL_STRUCTURED_VALUE_ITEMS)} values and names`,
        );
      }
      keys.push(key);
    }
    if (canonical) keys.sort();
    appendJsonPiece(state, "{", label);
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index] ?? "";
      state.items += 1;
      if (state.items > MAX_SQL_STRUCTURED_VALUE_ITEMS) {
        throw new RangeError(
          `${label} cannot exceed ${String(MAX_SQL_STRUCTURED_VALUE_ITEMS)} values and names`,
        );
      }
      assertBoundedDomainString(key, `${label} object name`);
      if (index > 0) appendJsonPiece(state, ",", label);
      appendJsonPiece(state, JSON.stringify(key), label);
      appendJsonPiece(state, ":", label);
      encodeBoundedJson(record[key], canonical, label, depth + 1, state);
    }
    appendJsonPiece(state, "}", label);
  } finally {
    state.active.delete(object);
  }
}

function appendJsonPiece(state: JsonEncodingState, piece: string, label: string): void {
  const next = state.length + piece.length;
  if (!Number.isSafeInteger(next) || next > MAX_SQL_SCALAR_RESULT_CHARACTERS) {
    throw new RangeError(`${label} exceeds ${String(MAX_SQL_SCALAR_RESULT_CHARACTERS)} characters`);
  }
  state.length = next;
  state.pieces.push(piece);
}

function assertBoundedDomainString(value: string, label: string): void {
  if (value.length > MAX_SQL_SCALAR_RESULT_CHARACTERS) {
    throw new RangeError(`${label} exceeds ${String(MAX_SQL_SCALAR_RESULT_CHARACTERS)} characters`);
  }
  assertWellFormedString(value, label);
}

export function jsonDomainValue(value: unknown, binary: boolean): string | null {
  if (value === null || value === undefined) return null;
  let parsed: unknown = value;
  if (typeof value === "string") {
    const prefix = value.startsWith(JSONB_VALUE)
      ? JSONB_VALUE
      : value.startsWith(JSON_VALUE)
        ? JSON_VALUE
        : undefined;
    const source = prefix === undefined ? value : value.slice(prefix.length);
    assertBoundedDomainString(source, "JSON value");
    try {
      parsed = JSON.parse(source);
    } catch {
      throw new TypeError("Invalid JSON value");
    }
  }
  const text = boundedJsonText(parsed, binary, binary ? "JSONB value" : "JSON value");
  return boundedTaggedDomainValue(
    binary ? JSONB_VALUE : JSON_VALUE,
    text,
    binary ? "JSONB value" : "JSON value",
  );
}

/** Returns the JSON document carried by an internal JSON/JSONB scalar, if any. */
export function jsonDomainDocument(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  if (value.startsWith(JSONB_VALUE)) return value.slice(JSONB_VALUE.length);
  if (value.startsWith(JSON_VALUE)) return value.slice(JSON_VALUE.length);
  return undefined;
}

/**
 * Tags already-constructed JSON without parsing and re-stringifying it. The validation parse
 * rejects malformed documents, while retaining duplicate object names and the constructor's
 * exact member order for embedding in an outer JSON value.
 */
export function preservedJsonDomainValue(document: string, binary = false): string {
  assertBoundedDomainString(document, binary ? "JSONB value" : "JSON value");
  try {
    JSON.parse(document);
  } catch {
    throw new TypeError("Invalid JSON value");
  }
  return boundedTaggedDomainValue(
    binary ? JSONB_VALUE : JSON_VALUE,
    document,
    binary ? "JSONB value" : "JSON value",
  );
}

export function uuidDomainValue(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") throw new TypeError("UUID accepts a string value");
  assertBoundedDomainString(value, "UUID value");
  const source = (
    value.startsWith(UUID_VALUE) ? value.slice(UUID_VALUE.length) : value
  ).toLowerCase();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(source)) {
    throw new TypeError(`Invalid UUID value: ${value}`);
  }
  return UUID_VALUE + source;
}

/** Canonical, zoneless SQL DATE value. No JavaScript time zone participates in validation. */
export function dateDomainValue(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  let source: string;
  if (value instanceof Date) {
    if (!Number.isFinite(value.getTime())) throw new TypeError("DATE accepts a valid Date value");
    source = dateIsoString(value).slice(0, 10);
  } else if (typeof value === "string") {
    assertBoundedDomainString(value, "DATE value");
    source = value.startsWith(DATE_VALUE) ? value.slice(DATE_VALUE.length) : value;
  } else {
    throw new TypeError("DATE accepts a YYYY-MM-DD string value");
  }
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(source);
  const year = Number(match?.[1]);
  const month = Number(match?.[2]);
  const day = Number(match?.[3]);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (
    match === null ||
    year < 1 ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > (days[month - 1] ?? 0)
  ) {
    throw new TypeError(`Invalid DATE value: ${String(value)}`);
  }
  return DATE_VALUE + source;
}

export function isDateDomainValue(value: unknown): value is string {
  return typeof value === "string" && value.startsWith(DATE_VALUE);
}

export function timeDomainValue(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") throw new TypeError("TIME accepts a string value");
  assertBoundedDomainString(value, "TIME value");
  const source = value.startsWith(TIME_VALUE) ? value.slice(TIME_VALUE.length) : value;
  const match = /^(\d{2}):(\d{2})(?::(\d{2})(\.\d{1,6})?)?$/.exec(source);
  const hours = match?.[1];
  const minutes = match?.[2];
  if (
    match === null ||
    hours === undefined ||
    minutes === undefined ||
    Number(hours) > 23 ||
    Number(minutes) > 59 ||
    Number(match[3] ?? 0) > 59
  ) {
    throw new TypeError(`Invalid TIME value: ${value}`);
  }
  return TIME_VALUE + `${hours}:${minutes}:${match[3] ?? "00"}${match[4] ?? ""}`;
}

export function intervalDomainValue(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") throw new TypeError("INTERVAL accepts a string value");
  assertBoundedDomainString(value, "INTERVAL value");
  if (value.startsWith(INTERVAL_VALUE)) {
    const source = value.slice(INTERVAL_VALUE.length);
    let decoded: unknown;
    try {
      decoded = JSON.parse(source) as unknown;
    } catch {
      throw new TypeError("Invalid tagged INTERVAL value");
    }
    if (
      !Array.isArray(decoded) ||
      decoded.length !== 3 ||
      !decoded.every((part) => Number.isSafeInteger(part)) ||
      JSON.stringify(decoded) !== source
    ) {
      throw new TypeError("Invalid tagged INTERVAL value");
    }
    return value;
  }
  const source = value.trim();
  let months = 0;
  let days = 0;
  let microseconds = 0;
  let matched = 0;
  const partPattern = /([+-]?\d+(?:\.\d+)?)\s*([a-zA-Z]+)/g;
  const units = new Map<string, (amount: number) => void>([
    ["year", (amount) => (months += amount * 12)],
    ["month", (amount) => (months += amount)],
    ["week", (amount) => (days += amount * 7)],
    ["day", (amount) => (days += amount)],
    ["hour", (amount) => (microseconds += amount * 3_600_000_000)],
    ["minute", (amount) => (microseconds += amount * 60_000_000)],
    ["second", (amount) => (microseconds += amount * 1_000_000)],
  ]);
  for (const part of source.matchAll(partPattern)) {
    const unit = (part[2] ?? "").toLowerCase().replace(/s$/, "");
    const apply = units.get(unit);
    if (apply === undefined) throw new TypeError(`Unknown INTERVAL unit: ${unit}`);
    const amount = Number(part[1]);
    if (
      (unit === "year" || unit === "month" || unit === "week" || unit === "day") &&
      !Number.isInteger(amount)
    ) {
      throw new TypeError(`INTERVAL ${unit} must be a whole number`);
    }
    apply(amount);
    matched += 1;
  }
  const residue = source.replace(partPattern, "").trim();
  if (
    matched === 0 ||
    residue !== "" ||
    !Number.isSafeInteger(months) ||
    !Number.isSafeInteger(days) ||
    !Number.isSafeInteger(microseconds)
  ) {
    throw new TypeError(`Invalid INTERVAL value: ${value}`);
  }
  return boundedTaggedDomainValue(
    INTERVAL_VALUE,
    JSON.stringify([months, days, microseconds]),
    "INTERVAL value",
  );
}

export function arrayDomainValue(values: readonly unknown[]): string {
  return boundedTaggedDomainValue(
    ARRAY_VALUE,
    boundedJsonText(values.map(externalSqlDomainValue), true, "ARRAY value"),
    "ARRAY value",
  );
}

export function enumDomainValue(
  value: unknown,
  name: string,
  values: readonly string[],
): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") throw new TypeError(`Enum ${name} accepts a string value`);
  assertBoundedDomainString(value, `Enum ${name} value`);
  let source = value;
  // Enum labels are arbitrary strings. Prefer an exact declared label before recognizing the
  // internal tag namespace, so a legal label can begin with that prefix just like ordinary TEXT.
  if (!values.includes(value) && value.startsWith(ENUM_VALUE)) {
    const encoded = value.slice(ENUM_VALUE.length);
    let decoded: unknown;
    try {
      decoded = JSON.parse(encoded) as unknown;
    } catch {
      throw new TypeError(`Invalid tagged enum ${name} value`);
    }
    if (
      !Array.isArray(decoded) ||
      decoded.length !== 3 ||
      decoded[0] !== name ||
      !Number.isSafeInteger(decoded[1]) ||
      typeof decoded[2] !== "string" ||
      JSON.stringify(decoded) !== encoded
    ) {
      throw new TypeError(`Invalid tagged enum ${name} value`);
    }
    source = decoded[2];
  }
  const index = values.indexOf(source);
  if (index === -1) throw new TypeError(`${source} is not a value of enum ${name}`);
  return boundedTaggedDomainValue(
    ENUM_VALUE,
    boundedJsonText([name, index, source], false, `Enum ${name} value`),
    `Enum ${name} value`,
  );
}

export function enumDomainCompare(left: unknown, right: unknown): number | undefined {
  if (
    typeof left !== "string" ||
    typeof right !== "string" ||
    !left.startsWith(ENUM_VALUE) ||
    !right.startsWith(ENUM_VALUE)
  ) {
    return undefined;
  }
  const a = JSON.parse(left.slice(ENUM_VALUE.length)) as [string, number, string];
  const b = JSON.parse(right.slice(ENUM_VALUE.length)) as [string, number, string];
  if (a[0] !== b[0]) throw new TypeError("Cannot compare values of different enum types");
  return a[1] - b[1];
}

export function normalizeSqlDomainValue(domain: SqlDomain, value: unknown): string | null {
  if (domain.kind === "numeric") {
    return exactNumericValue(value, domain.precision, domain.scale);
  }
  if (domain.kind === "json") return jsonDomainValue(value, false);
  if (domain.kind === "jsonb") return jsonDomainValue(value, true);
  if (domain.kind === "uuid") return uuidDomainValue(value);
  if (domain.kind === "date") return dateDomainValue(value);
  if (domain.kind === "time") return timeDomainValue(value);
  if (domain.kind === "interval") return intervalDomainValue(value);
  if (domain.kind === "enum") return enumDomainValue(value, domain.name, domain.values);
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") throw new TypeError("ARRAY columns accept JSON array text");
  const source = value.startsWith(ARRAY_VALUE) ? value.slice(ARRAY_VALUE.length) : value;
  assertBoundedDomainString(source, "ARRAY value");
  let parsed: unknown;
  try {
    parsed = JSON.parse(source);
  } catch {
    throw new TypeError("Invalid ARRAY value");
  }
  if (!Array.isArray(parsed)) throw new TypeError("ARRAY value must be an array");
  // JSON array text contains ordinary JSON strings. Do not interpret a string that happens to
  // begin with Minnow's internal domain prefix as an already-tagged SQL scalar.
  return boundedTaggedDomainValue(
    ARRAY_VALUE,
    boundedJsonText(parsed, true, "ARRAY value"),
    "ARRAY value",
  );
}

const collators = new Map<string, Intl.Collator>();
const MAX_COLLATOR_CACHE_ENTRIES = 64;

export function collatedDomainValue(value: unknown, collation: unknown): string | null {
  if (value === null || value === undefined) return null;
  value = externalSqlDomainValue(value);
  if (typeof value !== "string" || typeof collation !== "string") {
    throw new TypeError("COLLATE requires a string value and collation name");
  }
  assertBoundedDomainString(value, "COLLATE value");
  assertBoundedDomainString(collation, "COLLATE name");
  const locale = collation === "POSIX" ? "C" : collation;
  if (locale !== "C") collatorFor(locale, collation);
  return boundedTaggedDomainValue(
    COLLATION_VALUE,
    boundedJsonText([locale, value], false, "COLLATE value"),
    "COLLATE value",
  );
}

function boundedTaggedDomainValue(prefix: string, value: string, label: string): string {
  // The prefix is an internal physical tag and is removed at the JavaScript boundary. Limit the
  // user-visible payload, while the fixed tag adds only a few bounded bytes in storage.
  if (value.length > MAX_SQL_SCALAR_RESULT_CHARACTERS) {
    throw new RangeError(`${label} exceeds ${String(MAX_SQL_SCALAR_RESULT_CHARACTERS)} characters`);
  }
  return prefix + value;
}

export function collatedDomainCompare(left: unknown, right: unknown): number | undefined {
  const decode = (value: unknown): [string, string] | undefined =>
    typeof value === "string" && value.startsWith(COLLATION_VALUE)
      ? (JSON.parse(value.slice(COLLATION_VALUE.length)) as [string, string])
      : undefined;
  const a = decode(left);
  const b = decode(right);
  if (a === undefined && b === undefined) return undefined;
  if (a === undefined && typeof left !== "string") return undefined;
  if (b === undefined && typeof right !== "string") return undefined;
  const locale = a?.[0] ?? b?.[0] ?? "C";
  if ((a !== undefined && a[0] !== locale) || (b !== undefined && b[0] !== locale)) {
    throw new TypeError("Cannot compare values with different collations");
  }
  const leftValue = a?.[1] ?? (left as string);
  const rightValue = b?.[1] ?? (right as string);
  if (locale === "C") return leftValue === rightValue ? 0 : leftValue < rightValue ? -1 : 1;
  return collatorFor(locale, locale).compare(leftValue, rightValue);
}

function collatorFor(locale: string, displayName: string): Intl.Collator {
  const cached = collators.get(locale);
  if (cached !== undefined) {
    collators.delete(locale);
    collators.set(locale, cached);
    return cached;
  }
  let created: Intl.Collator;
  try {
    created = new Intl.Collator(locale, { usage: "sort", sensitivity: "variant" });
  } catch {
    throw new TypeError(`Unsupported collation: ${displayName}`);
  }
  if (collators.size >= MAX_COLLATOR_CACHE_ENTRIES) {
    const oldest = collators.keys().next().value;
    if (oldest !== undefined) collators.delete(oldest);
  }
  collators.set(locale, created);
  return created;
}

export function externalSqlDomainValue(value: unknown): unknown {
  if (typeof value !== "string") return value;
  if (value.startsWith(TEXT_VALUE)) return value.slice(TEXT_VALUE.length);
  for (const prefix of [NUMERIC, JSON_VALUE, JSONB_VALUE, UUID_VALUE, DATE_VALUE, TIME_VALUE]) {
    if (value.startsWith(prefix)) return value.slice(prefix.length);
  }
  if (value.startsWith(INTERVAL_VALUE)) {
    const decoded = JSON.parse(value.slice(INTERVAL_VALUE.length)) as [number, number, number];
    return `${String(decoded[0])} mons ${String(decoded[1])} days ${String(decoded[2])} usecs`;
  }
  if (value.startsWith(ARRAY_VALUE)) return value.slice(ARRAY_VALUE.length);
  if (value.startsWith(COLLATION_VALUE)) {
    return (JSON.parse(value.slice(COLLATION_VALUE.length)) as [string, string])[1];
  }
  if (value.startsWith(ENUM_VALUE)) {
    return (JSON.parse(value.slice(ENUM_VALUE.length)) as [string, number, string])[2];
  }
  return value;
}

export function isSqlDomainValue(value: unknown): value is string {
  return typeof value === "string" && value.startsWith(PREFIX);
}
