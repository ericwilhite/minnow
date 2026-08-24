import type { SqlDomain } from "../storage/types.js";

/** Internal string encodings for PostgreSQL value domains carried by string vectors. */
const PREFIX = "\u0000minnow-domain:";
const TEXT_VALUE = `${PREFIX}text:`;
const NUMERIC = `${PREFIX}numeric:`;
const JSON_VALUE = `${PREFIX}json:`;
const JSONB_VALUE = `${PREFIX}jsonb:`;
const UUID_VALUE = `${PREFIX}uuid:`;
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

const powersOfTen: bigint[] = [1n];
function pow10(exponent: number): bigint {
  if (!Number.isSafeInteger(exponent) || exponent < 0 || exponent > 100_000) {
    throw new RangeError(`Decimal scale is outside the supported range: ${String(exponent)}`);
  }
  for (let index = powersOfTen.length; index <= exponent; index += 1) {
    powersOfTen.push((powersOfTen[index - 1] ?? 1n) * 10n);
  }
  return powersOfTen[exponent] ?? 1n;
}

function decimalParts(value: string | number): DecimalParts {
  const source = String(value).trim();
  const match = /^([+-]?)(?:(\d+)(?:\.(\d*))?|\.(\d+))(?:[eE]([+-]?\d+))?$/.exec(source);
  if (match === null) throw new TypeError(`Invalid NUMERIC value: ${String(value)}`);
  const fraction = match[3] ?? match[4] ?? "";
  const digits = `${match[2] ?? "0"}${fraction}`.replace(/^0+(?=\d)/, "");
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
  return NUMERIC + formatDecimal(normalizeDecimal(adjusted));
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
  return NUMERIC + formatDecimal(normalizeDecimal(result));
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

function canonicalJson(value: unknown): string {
  const normalize = (item: unknown): unknown => {
    if (item === null || typeof item === "boolean" || typeof item === "string") return item;
    if (typeof item === "number") {
      if (!Number.isFinite(item)) throw new TypeError("JSON numbers must be finite");
      return item;
    }
    if (Array.isArray(item)) return item.map(normalize);
    if (typeof item === "object") {
      return Object.fromEntries(
        Object.keys(item as Record<string, unknown>)
          .sort()
          .map((key) => [key, normalize((item as Record<string, unknown>)[key])]),
      );
    }
    throw new TypeError("Value cannot be represented as JSON");
  };
  return JSON.stringify(normalize(value));
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
    try {
      parsed = JSON.parse(source);
    } catch {
      throw new TypeError("Invalid JSON value");
    }
  }
  const text = binary ? canonicalJson(parsed) : JSON.stringify(parsed);
  return (binary ? JSONB_VALUE : JSON_VALUE) + text;
}

export function uuidDomainValue(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") throw new TypeError("UUID accepts a string value");
  const source = (
    value.startsWith(UUID_VALUE) ? value.slice(UUID_VALUE.length) : value
  ).toLowerCase();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(source)) {
    throw new TypeError(`Invalid UUID value: ${value}`);
  }
  return UUID_VALUE + source;
}

export function timeDomainValue(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") throw new TypeError("TIME accepts a string value");
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
  if (typeof value === "string" && value.startsWith(INTERVAL_VALUE)) return value;
  if (typeof value !== "string") throw new TypeError("INTERVAL accepts a string value");
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
  return INTERVAL_VALUE + JSON.stringify([months, days, microseconds]);
}

export function arrayDomainValue(values: readonly unknown[]): string {
  return ARRAY_VALUE + canonicalJson(values.map(externalSqlDomainValue));
}

export function enumDomainValue(
  value: unknown,
  name: string,
  values: readonly string[],
): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") throw new TypeError(`Enum ${name} accepts a string value`);
  const source = value.startsWith(ENUM_VALUE)
    ? (JSON.parse(value.slice(ENUM_VALUE.length)) as [string, number, string])[2]
    : value;
  const index = values.indexOf(source);
  if (index === -1) throw new TypeError(`${source} is not a value of enum ${name}`);
  return ENUM_VALUE + JSON.stringify([name, index, source]);
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
  if (domain.kind === "time") return timeDomainValue(value);
  if (domain.kind === "interval") return intervalDomainValue(value);
  if (domain.kind === "enum") return enumDomainValue(value, domain.name, domain.values);
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") throw new TypeError("ARRAY columns accept JSON array text");
  const source = value.startsWith(ARRAY_VALUE) ? value.slice(ARRAY_VALUE.length) : value;
  let parsed: unknown;
  try {
    parsed = JSON.parse(source);
  } catch {
    throw new TypeError("Invalid ARRAY value");
  }
  if (!Array.isArray(parsed)) throw new TypeError("ARRAY value must be an array");
  return arrayDomainValue(parsed);
}

const collators = new Map<string, Intl.Collator>();

export function collatedDomainValue(value: unknown, collation: unknown): string | null {
  if (value === null || value === undefined) return null;
  value = externalSqlDomainValue(value);
  if (typeof value !== "string" || typeof collation !== "string") {
    throw new TypeError("COLLATE requires a string value and collation name");
  }
  const locale = collation === "POSIX" ? "C" : collation;
  if (locale !== "C" && !collators.has(locale)) {
    try {
      collators.set(locale, new Intl.Collator(locale, { usage: "sort", sensitivity: "variant" }));
    } catch {
      throw new TypeError(`Unsupported collation: ${collation}`);
    }
  }
  return COLLATION_VALUE + JSON.stringify([locale, value]);
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
  return (collators.get(locale) ?? new Intl.Collator(locale)).compare(leftValue, rightValue);
}

export function externalSqlDomainValue(value: unknown): unknown {
  if (typeof value !== "string") return value;
  if (value.startsWith(TEXT_VALUE)) return value.slice(TEXT_VALUE.length);
  for (const prefix of [NUMERIC, JSON_VALUE, JSONB_VALUE, UUID_VALUE, TIME_VALUE]) {
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
