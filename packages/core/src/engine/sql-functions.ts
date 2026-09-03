/**
 * Table-driven scalar functions: the PostgreSQL string, math, datetime, regex, and formatting
 * functions that need no special parsing. Each entry carries its arity, its result type, and a
 * pure evaluator over already-evaluated arguments, so the parser's arity check, schema
 * inference, constant folding, and both executors read one definition. Anything with its own
 * syntax (EXTRACT, CAST, TRIM ... FROM, JSON constructors) stays in the parser's own tables.
 */
import {
  dateIsoString,
  dateMilliseconds,
  dateUtcDate,
  dateUtcDay,
  dateUtcFullYear,
  dateUtcHours,
  dateUtcMinutes,
  dateUtcMonth,
  dateUtcSeconds,
} from "../date-value.js";
import { MAX_SQL_SCALAR_RESULT_CHARACTERS } from "./cache-limits.js";
import {
  dateDomainValue,
  exactNumericRounded,
  exactNumericUnary,
  externalSqlDomainValue,
  intervalDomainValue,
  isDateDomainValue,
  isExactNumeric,
  protectedSqlTextValue,
} from "./sql-domains.js";
import { compileRegexPattern, parseSqlTimestampText, stringArgument } from "./sql-semantics.js";

export type SimpleScalarResult =
  "string" | "number" | "boolean" | "datetime" | "date" | "interval" | "argument";

export interface SimpleScalarFunction {
  readonly minArgs: number;
  readonly maxArgs: number;
  /** The output type; "argument" carries the first argument's type through. */
  readonly returns: SimpleScalarResult;
  /** False for functions that read NULL arguments as data (CONCAT) instead of returning NULL. */
  readonly nullOnNull?: false;
  readonly evaluate: (values: readonly unknown[]) => unknown;
}

// --- Argument readers ----------------------------------------------------------------------

function text(name: string, value: unknown): string {
  const source = stringArgument(name, value);
  if (source.length > MAX_SQL_SCALAR_RESULT_CHARACTERS) {
    throw new RangeError(
      `${name} input exceeds ${String(MAX_SQL_SCALAR_RESULT_CHARACTERS)} characters`,
    );
  }
  return source;
}

function number(name: string, value: unknown): number {
  const external = externalSqlDomainValue(value);
  if (typeof external === "number") return external;
  if (
    typeof external === "string" &&
    /^\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$/.test(external)
  ) {
    return Number(external);
  }
  throw new TypeError(`${name} requires a numeric argument`);
}

function integer(name: string, value: unknown): number {
  const parsed = number(name, value);
  if (!Number.isInteger(parsed)) throw new TypeError(`${name} requires a whole number`);
  return parsed;
}

function datetime(name: string, value: unknown): Date {
  if (value instanceof Date) return value;
  if (isDateDomainValue(value)) {
    const external = externalSqlDomainValue(value);
    if (typeof external === "string") return new Date(`${external}T00:00:00.000Z`);
  }
  if (typeof value === "string") {
    const parsed = parseSqlTimestampText(value);
    if (parsed !== undefined) return parsed;
  }
  throw new TypeError(`${name} requires a datetime argument`);
}

function bounded(value: string, name: string): string {
  if (value.length > MAX_SQL_SCALAR_RESULT_CHARACTERS) {
    throw new RangeError(
      `${name} result exceeds ${String(MAX_SQL_SCALAR_RESULT_CHARACTERS)} characters`,
    );
  }
  return protectedSqlTextValue(value);
}

function characters(value: string): string[] {
  return Array.from(value);
}

/** PostgreSQL's text rendering of a value inside CONCAT and FORMAT. */
function rendered(value: unknown): string {
  const external = externalSqlDomainValue(value);
  if (typeof external === "string") return external;
  if (typeof external === "number") return String(external);
  if (typeof external === "boolean") return external ? "t" : "f";
  if (external instanceof Date) return dateIsoString(external);
  return String(external);
}

function finite(name: string, value: number): number {
  if (!Number.isFinite(value)) throw new TypeError(`${name} produced a non-finite number`);
  return value;
}

// --- Datetime formatting (TO_CHAR / TO_DATE / TO_TIMESTAMP) -------------------------------

const MONTHS = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];
const DAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

/** Template tokens, longest first, so `HH24` wins over `HH`, `YYYY` over `YY`. */
const DATE_TOKENS = [
  "HH24",
  "HH12",
  "YYYY",
  "MONTH",
  "Month",
  "month",
  "DDD",
  "DAY",
  "Day",
  "day",
  "MON",
  "Mon",
  "mon",
  "DY",
  "Dy",
  "dy",
  "IW",
  "MS",
  "US",
  "TZ",
  "HH",
  "MI",
  "SS",
  "YY",
  "MM",
  "DD",
  "AM",
  "PM",
  "am",
  "pm",
  "A.M.",
  "P.M.",
  "Q",
  "D",
  "J",
] as const;

interface TemplateItem {
  token?: string;
  literal?: string;
  fill: boolean;
}

/** Splits a TO_CHAR template into tokens and literal text; FM before a token disables padding. */
function dateTemplate(template: string): TemplateItem[] {
  const items: TemplateItem[] = [];
  let index = 0;
  let fill = false;
  while (index < template.length) {
    if (template[index] === '"') {
      const close = template.indexOf('"', index + 1);
      const end = close === -1 ? template.length : close;
      items.push({ literal: template.slice(index + 1, end), fill: false });
      index = end + 1;
      continue;
    }
    if (template.startsWith("FM", index)) {
      fill = true;
      index += 2;
      continue;
    }
    const token = DATE_TOKENS.find((candidate) => template.startsWith(candidate, index));
    if (token !== undefined) {
      items.push({ token, fill });
      fill = false;
      index += token.length;
      continue;
    }
    items.push({ literal: template[index] ?? "", fill: false });
    index += 1;
  }
  return items;
}

function pad(value: number, width: number, fill: boolean): string {
  return fill ? String(value) : String(value).padStart(width, "0");
}

function cased(name: string, token: string): string {
  if (token === token.toUpperCase()) return name.toUpperCase();
  if (token === token.toLowerCase()) return name.toLowerCase();
  return name;
}

function isoWeek(date: Date): number {
  const probe = new Date(Date.UTC(dateUtcFullYear(date), dateUtcMonth(date), dateUtcDate(date)));
  probe.setUTCDate(probe.getUTCDate() + 4 - (probe.getUTCDay() || 7));
  const yearStart = Date.UTC(probe.getUTCFullYear(), 0, 1);
  return Math.ceil(((probe.getTime() - yearStart) / 86_400_000 + 1) / 7);
}

function dayOfYear(date: Date): number {
  const start = Date.UTC(dateUtcFullYear(date), 0, 1);
  return (
    Math.floor(
      (Date.UTC(dateUtcFullYear(date), dateUtcMonth(date), dateUtcDate(date)) - start) / 86_400_000,
    ) + 1
  );
}

function formatDatetime(date: Date, template: string): string {
  const parts: string[] = [];
  const hours = dateUtcHours(date);
  for (const item of dateTemplate(template)) {
    if (item.literal !== undefined) {
      parts.push(item.literal);
      continue;
    }
    const token = item.token ?? "";
    const fill = item.fill;
    switch (token) {
      case "YYYY":
        parts.push(pad(dateUtcFullYear(date), 4, fill));
        break;
      case "YY":
        parts.push(pad(dateUtcFullYear(date) % 100, 2, fill));
        break;
      case "MM":
        parts.push(pad(dateUtcMonth(date) + 1, 2, fill));
        break;
      case "DD":
        parts.push(pad(dateUtcDate(date), 2, fill));
        break;
      case "DDD":
        parts.push(pad(dayOfYear(date), 3, fill));
        break;
      case "D":
        parts.push(String(dateUtcDay(date) + 1));
        break;
      case "Q":
        parts.push(String(Math.floor(dateUtcMonth(date) / 3) + 1));
        break;
      case "IW":
        parts.push(pad(isoWeek(date), 2, fill));
        break;
      case "J":
        parts.push(String(Math.floor(dateMilliseconds(date) / 86_400_000) + 2_440_588));
        break;
      case "HH24":
        parts.push(pad(hours, 2, fill));
        break;
      case "HH12":
      case "HH":
        parts.push(pad(hours % 12 === 0 ? 12 : hours % 12, 2, fill));
        break;
      case "MI":
        parts.push(pad(dateUtcMinutes(date), 2, fill));
        break;
      case "SS":
        parts.push(pad(dateUtcSeconds(date), 2, fill));
        break;
      case "MS":
        parts.push(String(date.getUTCMilliseconds()).padStart(3, "0"));
        break;
      case "US":
        parts.push(String(date.getUTCMilliseconds() * 1000).padStart(6, "0"));
        break;
      case "TZ":
        parts.push("UTC");
        break;
      case "AM":
      case "PM":
      case "am":
      case "pm":
        parts.push(cased(hours < 12 ? "AM" : "PM", token));
        break;
      case "A.M.":
      case "P.M.":
        parts.push(hours < 12 ? "A.M." : "P.M.");
        break;
      case "MONTH":
      case "Month":
      case "month": {
        const name = cased(MONTHS[dateUtcMonth(date)] ?? "", token);
        parts.push(fill ? name : name.padEnd(9, " "));
        break;
      }
      case "MON":
      case "Mon":
      case "mon":
        parts.push(cased((MONTHS[dateUtcMonth(date)] ?? "").slice(0, 3), token));
        break;
      case "DAY":
      case "Day":
      case "day": {
        const name = cased(DAYS[dateUtcDay(date)] ?? "", token);
        parts.push(fill ? name : name.padEnd(9, " "));
        break;
      }
      case "DY":
      case "Dy":
      case "dy":
        parts.push(cased((DAYS[dateUtcDay(date)] ?? "").slice(0, 3), token));
        break;
      default:
        parts.push(token);
    }
  }
  return parts.join("");
}

/** Reads datetime text against a TO_DATE / TO_TIMESTAMP template; fields not named default. */
function parseDatetime(name: string, input: string, template: string): Date {
  let year = 1970;
  let month = 1;
  let day = 1;
  let hours = 0;
  let minutes = 0;
  let seconds = 0;
  let milliseconds = 0;
  let pm: boolean | undefined;
  let cursor = 0;
  const digits = (width: number, label: string): number => {
    const match = /^\d+/.exec(input.slice(cursor, cursor + width));
    if (match === null)
      throw new TypeError(`${name} could not read ${label} at position ${String(cursor + 1)}`);
    cursor += match[0].length;
    return Number(match[0]);
  };
  const word = (options: readonly string[], label: string): number => {
    const rest = input.slice(cursor).toLowerCase();
    const found = options.findIndex((option) => rest.startsWith(option.toLowerCase()));
    if (found === -1)
      throw new TypeError(`${name} could not read ${label} at position ${String(cursor + 1)}`);
    cursor += options[found]?.length ?? 0;
    return found;
  };
  for (const item of dateTemplate(template)) {
    if (item.literal !== undefined) {
      // Separators in the template match one separator in the input, whatever character it is.
      if (cursor < input.length && !/\d/.test(input[cursor] ?? "")) cursor += item.literal.length;
      continue;
    }
    switch (item.token) {
      case "YYYY":
        year = digits(4, "the year");
        break;
      case "YY":
        year = 2000 + digits(2, "the year");
        break;
      case "MM":
        month = digits(2, "the month");
        break;
      case "MONTH":
      case "Month":
      case "month":
        month = word(MONTHS, "the month name") + 1;
        break;
      case "MON":
      case "Mon":
      case "mon":
        month =
          word(
            MONTHS.map((entry) => entry.slice(0, 3)),
            "the month name",
          ) + 1;
        break;
      case "DD":
        day = digits(2, "the day");
        break;
      case "DDD": {
        const ordinal = digits(3, "the day of year");
        const date = new Date(Date.UTC(year, 0, ordinal));
        month = date.getUTCMonth() + 1;
        day = date.getUTCDate();
        break;
      }
      case "HH24":
      case "HH12":
      case "HH":
        hours = digits(2, "the hour");
        break;
      case "MI":
        minutes = digits(2, "the minutes");
        break;
      case "SS":
        seconds = digits(2, "the seconds");
        break;
      case "MS":
        milliseconds = digits(3, "the milliseconds");
        break;
      case "US":
        milliseconds = Math.floor(digits(6, "the microseconds") / 1000);
        break;
      case "AM":
      case "PM":
      case "am":
      case "pm":
        pm = word(["am", "pm"], "the meridiem") === 1;
        break;
      case "A.M.":
      case "P.M.":
        pm = word(["a.m.", "p.m."], "the meridiem") === 1;
        break;
      case "DAY":
      case "Day":
      case "day":
        word(DAYS, "the day name");
        break;
      case "DY":
      case "Dy":
      case "dy":
        word(
          DAYS.map((entry) => entry.slice(0, 3)),
          "the day name",
        );
        break;
      case "TZ":
        cursor = input.length;
        break;
      default:
        throw new TypeError(`${name} does not read the ${item.token ?? ""} template field`);
    }
  }
  if (pm !== undefined) hours = pm ? (hours % 12) + 12 : hours % 12;
  const date = new Date(Date.UTC(year, month - 1, day, hours, minutes, seconds, milliseconds));
  if (
    !Number.isFinite(date.getTime()) ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day ||
    hours > 23 ||
    minutes > 59 ||
    seconds > 59
  ) {
    throw new TypeError(`${name} read an invalid date from ${input}`);
  }
  return date;
}

// --- Numeric formatting (TO_CHAR(number, template)) ------------------------------------------

/**
 * The digit templates in everyday use: 9 and 0 digit positions, a decimal point, group
 * separators, FM to drop padding, and S / MI for an explicit sign. Other pattern letters
 * (EEEE, RN, V, PL, L, TH) are refused rather than rendered wrongly.
 */
function formatNumber(value: number, template: string): string {
  const fill = template.includes("FM");
  const body = template.replace(/FM/g, "");
  const unsupported = /[^90.,SMI\s]/.exec(body);
  if (unsupported !== null) {
    throw new TypeError(`TO_CHAR does not support the ${unsupported[0]} numeric template element`);
  }
  const signStyle = body.includes("S") ? "S" : body.includes("MI") ? "MI" : "default";
  const pattern = body.replace(/S|MI/g, "");
  const [integerPattern = "", fractionPattern = ""] = pattern.split(".");
  const fractionDigits = (fractionPattern.match(/[90]/g) ?? []).length;
  const integerSlots = (integerPattern.match(/[90]/g) ?? []).length;
  // PostgreSQL formats the double's exact binary value and breaks an exact tie to even: 0.075
  // is a hair below the tie and renders as .07 under '9.99', while 77.25 is exact and renders
  // as 77.2 under '9999.9'. toFixed rounds by the exact value but breaks ties upward, so ties
  // are detected on a long exact expansion and settled here.
  const magnitude = Math.abs(value);
  const expansion = magnitude.toFixed(Math.min(fractionDigits + 30, 100));
  const cut = expansion.indexOf(".") + 1 + fractionDigits;
  const tie = /^50*$/.test(expansion.slice(cut));
  let rounded = magnitude.toFixed(fractionDigits);
  if (tie) {
    const kept = expansion.slice(0, cut).replace(/\.$/, "");
    const lastDigit = Number(kept.at(-1) ?? "0");
    rounded =
      lastDigit % 2 === 0
        ? Number(kept).toFixed(fractionDigits)
        : (Number(kept) + 10 ** -fractionDigits).toFixed(fractionDigits);
  }
  const [wholeText = "0", fractionText = ""] = rounded.split(".");
  if (wholeText.length > integerSlots && !(wholeText === "0" && integerSlots === 0)) {
    return "#".repeat(pattern.length + (signStyle === "default" ? 1 : 0));
  }
  // A zero integer part prints nothing when the template continues with a fraction (' .5'),
  // and a single 0 otherwise; every explicit 0 slot then forces its digit.
  const digits = wholeText === "0" && fractionDigits > 0 ? [] : characters(wholeText);
  const output: string[] = [];
  let index = digits.length - 1;
  let forced = false;
  for (const symbol of characters(integerPattern).reverse()) {
    if (symbol === "9" || symbol === "0") {
      if (index >= 0) {
        output.unshift(digits[index] ?? "0");
        index -= 1;
      } else if (symbol === "0" || forced) {
        output.unshift("0");
        forced = true;
      } else if (!fill) {
        output.unshift(" ");
      }
    } else if (symbol === ",") {
      const more = index >= 0 || forced;
      if (more) output.unshift(",");
      else if (!fill) output.unshift(" ");
    } else if (symbol !== " ") {
      output.unshift(symbol);
    }
  }
  // The 0 slots to the left of the highest forced slot are also forced.
  let text = output.join("");
  if (integerPattern.includes("0")) {
    const firstZero = characters(integerPattern).findIndex((symbol) => symbol === "0");
    const slotsFromFirstZero = (integerPattern.slice(firstZero).match(/[90]/g) ?? []).length;
    const rendered = text.replace(/ /g, "");
    if (rendered.replace(/,/g, "").length < slotsFromFirstZero) {
      const needed = slotsFromFirstZero - rendered.replace(/,/g, "").length;
      text =
        (fill ? "" : " ".repeat(Math.max(integerSlots - slotsFromFirstZero, 0))) +
        "0".repeat(needed) +
        rendered;
    }
  }
  if (fractionDigits > 0) text += `.${fractionText}`;
  // The sign is the value's, even when the digits round to zero: -0.001 and -0 render with '-'.
  const negative = value < 0 || Object.is(value, -0);
  if (signStyle === "MI") {
    const result = text + (negative ? "-" : fill ? "" : " ");
    return fill ? result.trim() : result;
  }
  // The sign sits directly before the first digit; padding stays to its left.
  const leading = text.length - text.trimStart().length;
  const sign = negative ? "-" : signStyle === "S" ? "+" : fill ? "" : " ";
  const result = " ".repeat(leading) + sign + text.trimStart();
  return fill ? result.trim() : result;
}

// --- FORMAT ----------------------------------------------------------------------------------

function formatText(template: string, values: readonly unknown[]): string {
  let next = 0;
  return template.replace(
    /%(?:(\d+)\$)?([sIL%])/g,
    (_, position: string | undefined, kind: string) => {
      if (kind === "%") return "%";
      const index = position === undefined ? next++ : Number(position) - 1;
      if (index >= values.length)
        throw new TypeError("FORMAT has too few arguments for its template");
      const value = values[index];
      if (kind === "s") return value === null || value === undefined ? "" : rendered(value);
      if (kind === "I") {
        if (value === null || value === undefined)
          throw new TypeError("FORMAT %I does not accept NULL");
        const name = rendered(value);
        return /^[a-z_][a-z0-9_]*$/.test(name) ? name : `"${name.replace(/"/g, '""')}"`;
      }
      if (value === null || value === undefined) return "NULL";
      return `'${rendered(value).replace(/'/g, "''")}'`;
    },
  );
}

// --- MD5 ---------------------------------------------------------------------------------------

/** RFC 1321, over the UTF-8 bytes of the input, rendered as 32 lowercase hex digits. */
function md5Hex(input: string): string {
  const bytes = new TextEncoder().encode(input);
  const words = new Uint32Array(((bytes.length + 8) >> 6) * 16 + 16);
  for (let index = 0; index < bytes.length; index += 1) {
    words[index >> 2] = (words[index >> 2] ?? 0) | ((bytes[index] ?? 0) << ((index % 4) * 8));
  }
  words[bytes.length >> 2] = (words[bytes.length >> 2] ?? 0) | (0x80 << ((bytes.length % 4) * 8));
  const bitLength = bytes.length * 8;
  words[words.length - 2] = bitLength >>> 0;
  words[words.length - 1] = Math.floor(bitLength / 0x1_0000_0000);
  const shifts = [7, 12, 17, 22, 5, 9, 14, 20, 4, 11, 16, 23, 6, 10, 15, 21];
  const constants = Array.from(
    { length: 64 },
    (_, index) => Math.floor(Math.abs(Math.sin(index + 1)) * 0x1_0000_0000) >>> 0,
  );
  let a0 = 0x67452301;
  let b0 = 0xefcdab89;
  let c0 = 0x98badcfe;
  let d0 = 0x10325476;
  for (let chunk = 0; chunk < words.length; chunk += 16) {
    let a = a0;
    let b = b0;
    let c = c0;
    let d = d0;
    for (let round = 0; round < 64; round += 1) {
      let f: number;
      let g: number;
      if (round < 16) {
        f = (b & c) | (~b & d);
        g = round;
      } else if (round < 32) {
        f = (d & b) | (~d & c);
        g = (5 * round + 1) % 16;
      } else if (round < 48) {
        f = b ^ c ^ d;
        g = (3 * round + 5) % 16;
      } else {
        f = c ^ (b | ~d);
        g = (7 * round) % 16;
      }
      const shift = shifts[(round >> 4) * 4 + (round % 4)] ?? 0;
      const sum = (a + f + (constants[round] ?? 0) + (words[chunk + g] ?? 0)) >>> 0;
      a = d;
      d = c;
      c = b;
      b = (b + ((sum << shift) | (sum >>> (32 - shift)))) >>> 0;
    }
    a0 = (a0 + a) >>> 0;
    b0 = (b0 + b) >>> 0;
    c0 = (c0 + c) >>> 0;
    d0 = (d0 + d) >>> 0;
  }
  return [a0, b0, c0, d0]
    .map((word) =>
      [0, 8, 16, 24]
        .map((offset) => ((word >>> offset) & 0xff).toString(16).padStart(2, "0"))
        .join(""),
    )
    .join("");
}

// --- Regular expressions -----------------------------------------------------------------------

function regexFlags(name: string, value: unknown): string {
  if (value === null || value === undefined) return "";
  const flags = text(name, value);
  for (const flag of flags) {
    if (flag !== "i" && flag !== "g" && flag !== "n" && flag !== "c") {
      throw new TypeError(`${name} does not support the ${flag} flag`);
    }
  }
  return flags;
}

/** PostgreSQL replacement text: \1 back-references and \& become JavaScript's $1 and $&. */
function replacementText(value: string): string {
  return value
    .replace(/\$/g, "$$$$")
    .replace(/\\(\d)/g, "$$$1")
    .replace(/\\&/g, "$$&")
    .replace(/\\\\/g, "\\");
}

/** AGE(later, earlier): the calendar difference PostgreSQL reports, in months, days, and time. */
function ageInterval(later: Date, earlier: Date): string | null {
  let sign = 1;
  let a = later;
  let b = earlier;
  if (dateMilliseconds(a) < dateMilliseconds(b)) {
    sign = -1;
    [a, b] = [b, a];
  }
  let months = (dateUtcFullYear(a) - dateUtcFullYear(b)) * 12 + (dateUtcMonth(a) - dateUtcMonth(b));
  let days = dateUtcDate(a) - dateUtcDate(b);
  let milliseconds =
    dateMilliseconds(a) -
    Date.UTC(dateUtcFullYear(a), dateUtcMonth(a), dateUtcDate(a)) -
    (dateMilliseconds(b) - Date.UTC(dateUtcFullYear(b), dateUtcMonth(b), dateUtcDate(b)));
  if (milliseconds < 0) {
    milliseconds += 86_400_000;
    days -= 1;
  }
  if (days < 0) {
    // Borrow the length of the earlier date's month, as PostgreSQL's timestamp_age does.
    const earlierMonthDays = new Date(
      Date.UTC(dateUtcFullYear(b), dateUtcMonth(b) + 1, 0),
    ).getUTCDate();
    days += earlierMonthDays;
    months -= 1;
  }
  return intervalDomainValue(
    `${String(sign * months)} months ${String(sign * days)} days ${String((sign * milliseconds) / 1000)} seconds`,
  );
}

// --- The registry --------------------------------------------------------------------------

function nullish(value: unknown): value is null | undefined {
  return value === null || value === undefined;
}

export const simpleScalarFunctions: ReadonlyMap<string, SimpleScalarFunction> = new Map<
  string,
  SimpleScalarFunction
>([
  // Strings
  [
    "CONCAT",
    {
      minArgs: 1,
      maxArgs: Number.POSITIVE_INFINITY,
      returns: "string",
      nullOnNull: false,
      evaluate: (values) =>
        bounded(
          values
            .filter((value) => !nullish(value))
            .map(rendered)
            .join(""),
          "CONCAT",
        ),
    },
  ],
  [
    "CONCAT_WS",
    {
      minArgs: 2,
      maxArgs: Number.POSITIVE_INFINITY,
      returns: "string",
      nullOnNull: false,
      evaluate: (values) =>
        nullish(values[0])
          ? null
          : bounded(
              values
                .slice(1)
                .filter((value) => !nullish(value))
                .map(rendered)
                .join(text("CONCAT_WS", values[0])),
              "CONCAT_WS",
            ),
    },
  ],
  [
    "LEFT",
    {
      minArgs: 2,
      maxArgs: 2,
      returns: "string",
      evaluate: (values) => {
        const source = characters(text("LEFT", values[0]));
        const count = integer("LEFT", values[1]);
        return bounded(
          (count >= 0
            ? source.slice(0, count)
            : source.slice(0, Math.max(source.length + count, 0))
          ).join(""),
          "LEFT",
        );
      },
    },
  ],
  [
    "RIGHT",
    {
      minArgs: 2,
      maxArgs: 2,
      returns: "string",
      evaluate: (values) => {
        const source = characters(text("RIGHT", values[0]));
        const count = integer("RIGHT", values[1]);
        const kept =
          count >= 0
            ? source.slice(Math.max(source.length - count, 0))
            : source.slice(Math.min(-count, source.length));
        return bounded(kept.join(""), "RIGHT");
      },
    },
  ],
  [
    "REVERSE",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "string",
      evaluate: (values) =>
        bounded(characters(text("REVERSE", values[0])).reverse().join(""), "REVERSE"),
    },
  ],
  [
    "REPEAT",
    {
      minArgs: 2,
      maxArgs: 2,
      returns: "string",
      evaluate: (values) => {
        const source = text("REPEAT", values[0]);
        const count = integer("REPEAT", values[1]);
        if (count <= 0) return protectedSqlTextValue("");
        if (source.length * count > MAX_SQL_SCALAR_RESULT_CHARACTERS) {
          throw new RangeError(
            `REPEAT result exceeds ${String(MAX_SQL_SCALAR_RESULT_CHARACTERS)} characters`,
          );
        }
        return protectedSqlTextValue(source.repeat(count));
      },
    },
  ],
  [
    "INITCAP",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "string",
      evaluate: (values) =>
        bounded(
          text("INITCAP", values[0])
            .toLowerCase()
            .replace(
              /(^|[^\p{L}\p{N}])(\p{L})/gu,
              (_, before: string, letter: string) => before + letter.toUpperCase(),
            ),
          "INITCAP",
        ),
    },
  ],
  [
    "SPLIT_PART",
    {
      minArgs: 3,
      maxArgs: 3,
      returns: "string",
      evaluate: (values) => {
        const source = text("SPLIT_PART", values[0]);
        const delimiter = text("SPLIT_PART", values[1]);
        const position = integer("SPLIT_PART", values[2]);
        if (position === 0) throw new TypeError("SPLIT_PART field position must not be zero");
        const fields = delimiter === "" ? [source] : source.split(delimiter);
        const index = position > 0 ? position - 1 : fields.length + position;
        return protectedSqlTextValue(fields[index] ?? "");
      },
    },
  ],
  [
    "STRPOS",
    {
      minArgs: 2,
      maxArgs: 2,
      returns: "number",
      evaluate: (values) => {
        const haystack = text("STRPOS", values[0]);
        const index = haystack.indexOf(text("STRPOS", values[1]));
        return index === -1 ? 0 : characters(haystack.slice(0, index)).length + 1;
      },
    },
  ],
  [
    "STARTS_WITH",
    {
      minArgs: 2,
      maxArgs: 2,
      returns: "boolean",
      evaluate: (values) =>
        text("STARTS_WITH", values[0]).startsWith(text("STARTS_WITH", values[1])),
    },
  ],
  [
    "TRANSLATE",
    {
      minArgs: 3,
      maxArgs: 3,
      returns: "string",
      evaluate: (values) => {
        const from = characters(text("TRANSLATE", values[1]));
        const to = characters(text("TRANSLATE", values[2]));
        const mapping = new Map(from.map((character, index) => [character, to[index] ?? ""]));
        return bounded(
          characters(text("TRANSLATE", values[0]))
            .map((character) => mapping.get(character) ?? character)
            .join(""),
          "TRANSLATE",
        );
      },
    },
  ],
  [
    "ASCII",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => text("ASCII", values[0]).codePointAt(0) ?? 0,
    },
  ],
  [
    "CHR",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "string",
      evaluate: (values) => {
        const code = integer("CHR", values[0]);
        if (code <= 0 || code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) {
          throw new TypeError(`CHR has no character for code ${String(code)}`);
        }
        return protectedSqlTextValue(String.fromCodePoint(code));
      },
    },
  ],
  [
    "BTRIM",
    {
      minArgs: 1,
      maxArgs: 2,
      returns: "string",
      evaluate: (values) => {
        // PostgreSQL's BTRIM removes any character of the set from both ends.
        const set = new Set(characters(nullish(values[1]) ? " " : text("BTRIM", values[1])));
        const source = characters(text("BTRIM", values[0]));
        let start = 0;
        let end = source.length;
        while (start < end && set.has(source[start] ?? "")) start += 1;
        while (end > start && set.has(source[end - 1] ?? "")) end -= 1;
        return protectedSqlTextValue(source.slice(start, end).join(""));
      },
    },
  ],
  [
    "MD5",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "string",
      evaluate: (values) => protectedSqlTextValue(md5Hex(text("MD5", values[0]))),
    },
  ],
  [
    "FORMAT",
    {
      minArgs: 1,
      maxArgs: Number.POSITIVE_INFINITY,
      returns: "string",
      nullOnNull: false,
      evaluate: (values) => {
        if (nullish(values[0])) return null;
        return bounded(formatText(text("FORMAT", values[0]), values.slice(1)), "FORMAT");
      },
    },
  ],
  [
    "REGEXP_REPLACE",
    {
      minArgs: 3,
      maxArgs: 4,
      returns: "string",
      evaluate: (values) => {
        const flags = regexFlags("REGEXP_REPLACE", values[3]);
        const expression = compileRegexPattern(text("REGEXP_REPLACE", values[1]), flags);
        const replacement = replacementText(text("REGEXP_REPLACE", values[2]));
        return bounded(
          text("REGEXP_REPLACE", values[0]).replace(expression, replacement),
          "REGEXP_REPLACE",
        );
      },
    },
  ],
  [
    "MINNOW_REGEX_MATCH",
    {
      minArgs: 3,
      maxArgs: 3,
      returns: "boolean",
      evaluate: (values) =>
        compileRegexPattern(text("~", values[1]), regexFlags("~", values[2])).test(
          text("~", values[0]),
        ),
    },
  ],
  // Math
  [
    "EXP",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => finite("EXP", Math.exp(number("EXP", values[0]))),
    },
  ],
  [
    "LN",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => {
        const operand = number("LN", values[0]);
        if (operand <= 0) throw new TypeError("LN requires a positive number");
        return Math.log(operand);
      },
    },
  ],
  [
    "LOG",
    {
      minArgs: 1,
      maxArgs: 2,
      returns: "number",
      evaluate: (values) => {
        // LOG(x) is base 10, LOG(b, x) an explicit base, as in PostgreSQL.
        const operand = number("LOG", values.length > 1 ? values[1] : values[0]);
        const base = values.length > 1 ? number("LOG", values[0]) : 10;
        if (operand <= 0 || base <= 0 || base === 1)
          throw new TypeError("LOG requires positive arguments");
        return Math.log(operand) / Math.log(base);
      },
    },
  ],
  [
    "LOG10",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => {
        const operand = number("LOG10", values[0]);
        if (operand <= 0) throw new TypeError("LOG10 requires a positive number");
        return Math.log10(operand);
      },
    },
  ],
  [
    "SIGN",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) =>
        isExactNumeric(values[0])
          ? exactNumericUnary("SIGN", values[0])
          : Math.sign(number("SIGN", values[0])),
    },
  ],
  [
    "TRUNC",
    {
      minArgs: 1,
      maxArgs: 2,
      returns: "number",
      evaluate: (values) => {
        const digits = values.length > 1 ? integer("TRUNC", values[1]) : 0;
        // An exact NUMERIC truncates exactly, to the requested scale, as PostgreSQL's numeric
        // TRUNC does; a double takes the float path.
        if (isExactNumeric(values[0])) return exactNumericRounded(values[0], digits, "trunc");
        const operand = number("TRUNC", values[0]);
        const scale = 10 ** digits;
        return Math.trunc(operand * scale) / scale;
      },
    },
  ],
  ["PI", { minArgs: 0, maxArgs: 0, returns: "number", evaluate: () => Math.PI }],
  [
    "CBRT",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => Math.cbrt(number("CBRT", values[0])),
    },
  ],
  [
    "DIV",
    {
      minArgs: 2,
      maxArgs: 2,
      returns: "number",
      evaluate: (values) => {
        const divisor = number("DIV", values[1]);
        if (divisor === 0) throw new TypeError("DIV by zero");
        return Math.trunc(number("DIV", values[0]) / divisor);
      },
    },
  ],
  [
    "WIDTH_BUCKET",
    {
      minArgs: 4,
      maxArgs: 4,
      returns: "number",
      evaluate: (values) => {
        const operand = number("WIDTH_BUCKET", values[0]);
        const low = number("WIDTH_BUCKET", values[1]);
        const high = number("WIDTH_BUCKET", values[2]);
        const count = integer("WIDTH_BUCKET", values[3]);
        if (count <= 0) throw new TypeError("WIDTH_BUCKET count must be positive");
        if (low === high) throw new TypeError("WIDTH_BUCKET bounds must differ");
        if (low < high) {
          if (operand < low) return 0;
          if (operand >= high) return count + 1;
          return Math.floor(((operand - low) / (high - low)) * count) + 1;
        }
        if (operand > low) return 0;
        if (operand <= high) return count + 1;
        return Math.floor(((low - operand) / (low - high)) * count) + 1;
      },
    },
  ],
  [
    "SIN",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => Math.sin(number("SIN", values[0])),
    },
  ],
  [
    "COS",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => Math.cos(number("COS", values[0])),
    },
  ],
  [
    "TAN",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => finite("TAN", Math.tan(number("TAN", values[0]))),
    },
  ],
  [
    "ASIN",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => finite("ASIN", Math.asin(number("ASIN", values[0]))),
    },
  ],
  [
    "ACOS",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => finite("ACOS", Math.acos(number("ACOS", values[0]))),
    },
  ],
  [
    "ATAN",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => Math.atan(number("ATAN", values[0])),
    },
  ],
  [
    "ATAN2",
    {
      minArgs: 2,
      maxArgs: 2,
      returns: "number",
      evaluate: (values) => Math.atan2(number("ATAN2", values[0]), number("ATAN2", values[1])),
    },
  ],
  [
    "DEGREES",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => (number("DEGREES", values[0]) * 180) / Math.PI,
    },
  ],
  [
    "RADIANS",
    {
      minArgs: 1,
      maxArgs: 1,
      returns: "number",
      evaluate: (values) => (number("RADIANS", values[0]) * Math.PI) / 180,
    },
  ],
  // Datetime
  [
    "TO_CHAR",
    {
      minArgs: 2,
      maxArgs: 2,
      returns: "string",
      evaluate: (values) => {
        const template = text("TO_CHAR", values[1]);
        const external = externalSqlDomainValue(values[0]);
        if (typeof external === "number")
          return bounded(formatNumber(external, template), "TO_CHAR");
        if (
          typeof external === "string" &&
          !isDateDomainValue(values[0]) &&
          /^\s*[-+]?\d/.test(external) &&
          !/[-:]/.test(external.slice(1))
        ) {
          return bounded(formatNumber(Number(external), template), "TO_CHAR");
        }
        return bounded(formatDatetime(datetime("TO_CHAR", values[0]), template), "TO_CHAR");
      },
    },
  ],
  [
    "TO_DATE",
    {
      minArgs: 2,
      maxArgs: 2,
      returns: "date",
      evaluate: (values) =>
        dateDomainValue(
          parseDatetime("TO_DATE", text("TO_DATE", values[0]), text("TO_DATE", values[1])),
        ),
    },
  ],
  [
    "TO_TIMESTAMP",
    {
      minArgs: 1,
      maxArgs: 2,
      returns: "datetime",
      evaluate: (values) => {
        if (values.length === 1) {
          // TO_TIMESTAMP(seconds since the epoch).
          const date = new Date(number("TO_TIMESTAMP", values[0]) * 1000);
          if (!Number.isFinite(date.getTime()))
            throw new TypeError("TO_TIMESTAMP epoch is out of range");
          return date;
        }
        return parseDatetime(
          "TO_TIMESTAMP",
          text("TO_TIMESTAMP", values[0]),
          text("TO_TIMESTAMP", values[1]),
        );
      },
    },
  ],
  [
    "MAKE_DATE",
    {
      minArgs: 3,
      maxArgs: 3,
      returns: "date",
      evaluate: (values) => {
        const year = integer("MAKE_DATE", values[0]);
        const month = integer("MAKE_DATE", values[1]);
        const day = integer("MAKE_DATE", values[2]);
        const iso = `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
        return dateDomainValue(iso);
      },
    },
  ],
  [
    "MAKE_TIMESTAMP",
    {
      minArgs: 6,
      maxArgs: 6,
      returns: "datetime",
      evaluate: (values) => {
        const [year, month, day, hour, minute] = values
          .slice(0, 5)
          .map((value) => integer("MAKE_TIMESTAMP", value));
        const seconds = number("MAKE_TIMESTAMP", values[5]);
        const date = new Date(
          Date.UTC(
            year ?? 0,
            (month ?? 1) - 1,
            day ?? 1,
            hour ?? 0,
            minute ?? 0,
            0,
            Math.round(seconds * 1000),
          ),
        );
        if (
          !Number.isFinite(date.getTime()) ||
          date.getUTCMonth() !== (month ?? 1) - 1 ||
          date.getUTCDate() !== (day ?? 1)
        ) {
          throw new TypeError("MAKE_TIMESTAMP fields do not form a valid timestamp");
        }
        return date;
      },
    },
  ],
  [
    "AGE",
    {
      minArgs: 2,
      maxArgs: 2,
      returns: "interval",
      evaluate: (values) => ageInterval(datetime("AGE", values[0]), datetime("AGE", values[1])),
    },
  ],
]);
