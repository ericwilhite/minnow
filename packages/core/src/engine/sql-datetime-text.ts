/**
 * PostgreSQL's text output for datetime values, shared by every path that turns one into SQL
 * text: CAST and `::text`, `||`, CONCAT and FORMAT, the JSON constructors, and the INTERVAL and
 * TIME values returned at the JavaScript boundary. PostgreSQL runs here with DateStyle ISO and
 * IntervalStyle postgres in a UTC session, because Minnow reads every datetime as an instant in
 * UTC:
 *
 * - `timestamp`: `2024-01-02 10:00:00`, fractional seconds only when nonzero and trimmed
 *   (`10:00:00.5`), a year below 1000 zero-padded to four digits, one after 9999 written in
 *   full, and a year at or before 1 BC as its BC year with a ` BC` suffix;
 * - `timestamp with time zone`: the same with the session offset, `+00`, before any ` BC`;
 * - JSON: ISO 8601 with a `T` and, for a zoned value, `+00:00`, as `to_json` writes it;
 * - `interval`: `1 year 2 mons 3 days 04:05:06.5`, `-00:00:01`, `-3 days +04:00:00`.
 *
 * Formatting reads the epoch milliseconds directly, with no Date allocation and no per-call
 * padding work: the two-digit fields and millisecond fractions come from tables built once.
 */
import { civilFromDays } from "../date-value.js";

const MS_PER_DAY = 86_400_000;
const MS_PER_HOUR = 3_600_000;
const MS_PER_MINUTE = 60_000;

/** "00" through "99". */
const TWO_DIGITS: readonly string[] = Array.from({ length: 100 }, (_, value) =>
  value < 10 ? `0${String(value)}` : String(value),
);

/** The trimmed fraction PostgreSQL prints for each millisecond count: "", ".001", …, ".5", …. */
let millisecondFractions: readonly string[] | undefined;
function millisecondFraction(milliseconds: number): string {
  if (milliseconds === 0) return "";
  millisecondFractions ??= Array.from({ length: 1000 }, (_, value) =>
    value === 0 ? "" : `.${String(value).padStart(3, "0").replace(/0+$/, "")}`,
  );
  return millisecondFractions[milliseconds] ?? "";
}

function twoDigits(value: number): string {
  return TWO_DIGITS[value] ?? String(value);
}

/**
 * One timestamp's text: `separator` between the date and the time, `zone` after the seconds.
 * Throws for an invalid Date's NaN, as `toISOString` does.
 */
function timestampText(milliseconds: number, separator: string, zone: string): string {
  if (!Number.isFinite(milliseconds)) throw new RangeError("Invalid time value");
  const days = Math.floor(milliseconds / MS_PER_DAY);
  let rest = milliseconds - days * MS_PER_DAY;
  const hours = Math.floor(rest / MS_PER_HOUR);
  rest -= hours * MS_PER_HOUR;
  const minutes = Math.floor(rest / MS_PER_MINUTE);
  rest -= minutes * MS_PER_MINUTE;
  const seconds = Math.floor(rest / 1000);
  const fraction = millisecondFraction(rest - seconds * 1000);
  const civil = civilFromDays(days);
  const astronomicalYear = civil[0];
  const month = civil[1] + 1;
  const day = civil[2];
  // PostgreSQL has no year zero: astronomical year 0 is 1 BC, -1 is 2 BC.
  const bc = astronomicalYear <= 0;
  const year = bc ? 1 - astronomicalYear : astronomicalYear;
  const yearText = year >= 1000 ? String(year) : String(year).padStart(4, "0");
  const text =
    yearText +
    "-" +
    twoDigits(month) +
    "-" +
    twoDigits(day) +
    separator +
    twoDigits(hours) +
    ":" +
    twoDigits(minutes) +
    ":" +
    twoDigits(seconds) +
    fraction +
    zone;
  return bc ? `${text} BC` : text;
}

/**
 * PostgreSQL's text output for a timestamp held as epoch milliseconds: `timestamp with time
 * zone` (`zoned`) carries the UTC session's `+00`, `timestamp` carries no zone.
 */
export function postgresTimestampText(milliseconds: number, zoned: boolean): string {
  return timestampText(milliseconds, " ", zoned ? "+00" : "");
}

/** The ISO 8601 text `to_json` writes for a timestamp: a `T`, and `+00:00` when zoned. */
export function postgresTimestampJsonText(milliseconds: number, zoned: boolean): string {
  return timestampText(milliseconds, "T", zoned ? "+00:00" : "");
}

/**
 * PostgreSQL's TIME output for a canonical `HH:MM:SS[.ffffff]`: the fraction without its
 * trailing zeros, and no point at all when nothing is left.
 */
export function postgresTimeText(canonical: string): string {
  const point = canonical.indexOf(".");
  if (point === -1) return canonical;
  let end = canonical.length;
  while (end > point + 1 && canonical.charCodeAt(end - 1) === 48) end -= 1;
  return end === point + 1 ? canonical.slice(0, point) : canonical.slice(0, end);
}

const MICROSECONDS_PER_HOUR = 3_600_000_000;
const MICROSECONDS_PER_MINUTE = 60_000_000;
const MICROSECONDS_PER_SECOND = 1_000_000;

/**
 * PostgreSQL's `interval` output in its default IntervalStyle, `postgres`: years and months
 * split from the month count, then days, then a signed `HH:MM:SS[.ffffff]` time part when any
 * time remains or the interval is zero. Each field carries its own sign; a positive field after
 * a negative one gets an explicit `+`, exactly as PostgreSQL's EncodeInterval writes it.
 */
export function postgresIntervalText(months: number, days: number, microseconds: number): string {
  const years = Math.trunc(months / 12);
  const remainingMonths = months - years * 12;
  let text = "";
  if (years !== 0) text = intervalPart("", false, years, years === 1 ? " year" : " years");
  if (remainingMonths !== 0) {
    text = intervalPart(text, years < 0, remainingMonths, remainingMonths === 1 ? " mon" : " mons");
  }
  if (days !== 0) {
    const before = remainingMonths !== 0 ? remainingMonths < 0 : years < 0;
    text = intervalPart(text, before, days, days === 1 ? " day" : " days");
  }
  const zero = text === "";
  // A positive field after a negative one is written with its sign; the last nonzero field
  // before the time part decides whether the time needs one.
  const before = days !== 0 ? days < 0 : remainingMonths !== 0 ? remainingMonths < 0 : years < 0;
  if (zero || microseconds !== 0) {
    const negative = microseconds < 0;
    const magnitude = Math.abs(microseconds);
    const hours = Math.floor(magnitude / MICROSECONDS_PER_HOUR);
    let rest = magnitude - hours * MICROSECONDS_PER_HOUR;
    const minutes = Math.floor(rest / MICROSECONDS_PER_MINUTE);
    rest -= minutes * MICROSECONDS_PER_MINUTE;
    const seconds = Math.floor(rest / MICROSECONDS_PER_SECOND);
    const fraction = rest - seconds * MICROSECONDS_PER_SECOND;
    text +=
      (zero ? "" : " ") +
      (negative ? "-" : before ? "+" : "") +
      (hours < 100 ? twoDigits(hours) : String(hours)) +
      ":" +
      twoDigits(minutes) +
      ":" +
      twoDigits(seconds) +
      (fraction === 0 ? "" : `.${String(fraction).padStart(6, "0").replace(/0+$/, "")}`);
  }
  return text;
}

/** One `N unit` field after `text`, signed the way PostgreSQL signs it after `before`. */
function intervalPart(text: string, before: boolean, value: number, unit: string): string {
  return (text === "" ? "" : `${text} `) + (before && value > 0 ? "+" : "") + String(value) + unit;
}
