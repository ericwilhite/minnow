/**
 * DATE, TIMESTAMP and TIMESTAMPTZ representation and text (DECISIONS.md D1, C1, C4).
 *
 * Stored values use PostgreSQL's epoch, 2000-01-01 00:00:00 UTC, so the whole PostgreSQL range
 * is exact in int32 days and int64 microseconds:
 * - DATE: int32 days since 2000-01-01; finite range 4714-11-24 BC … 5874897-12-31;
 *   `-infinity` / `infinity` are INT32_MIN / INT32_MAX (PostgreSQL's DATEVAL_NOBEGIN / NOEND).
 * - TIMESTAMP[TZ]: int64 µs since 2000-01-01 UTC; finite range [TIMESTAMP_MIN, TIMESTAMP_END);
 *   `-infinity` / `infinity` are INT64_MIN / INT64_MAX (DT_NOBEGIN / DT_NOEND).
 * - Infinities are never in the f64 fast path: a page or batch holding one uses BigInt64Array
 *   (`timestampSafeRange`). Kernels test the two sentinels before arithmetic.
 *
 * Years are astronomical in ISO text (0000 is 1 BC, −0001 is 2 BC) and PostgreSQL-style ("BC")
 * in DATE text. Calendar is proleptic Gregorian, as in PostgreSQL.
 */
import { INT32_MAX, INT32_MIN, INT64_MAX, INT64_MIN } from "./types.js";

/** Days from 1970-01-01 to 2000-01-01. */
export const PG_EPOCH_UNIX_DAYS = 10_957;
export const MICROS_PER_DAY_BIG = 86_400_000_000n;
/** µs from 1970-01-01 to 2000-01-01. */
export const PG_EPOCH_UNIX_MICROS = 946_684_800_000_000n;

export const DATE_NEG_INFINITY = INT32_MIN;
export const DATE_POS_INFINITY = INT32_MAX;
export const TIMESTAMP_NEG_INFINITY = INT64_MIN;
export const TIMESTAMP_POS_INFINITY = INT64_MAX;
/** First finite timestamp: 4714-11-24 00:00:00 BC (Julian day 0). */
export const TIMESTAMP_MIN = -211_813_488_000_000_000n;
/** One past the last finite timestamp: 294277-01-01 00:00:00. */
export const TIMESTAMP_END = 9_223_371_331_200_000_000n;
/** First and last finite dates (days since 2000-01-01). */
export const DATE_MIN = -2_451_545;
export const DATE_MAX = 2_145_031_948;

const MAX_SAFE_BIG = BigInt(Number.MAX_SAFE_INTEGER);

/**
 * Whether a page or batch with these exact bounds may use the Float64Array representation:
 * both finite (no sentinel) and within ±(2^53 − 1). Covers roughly 1715–2285.
 */
export function timestampSafeRange(min: bigint, max: bigint): boolean {
  return (
    min !== TIMESTAMP_NEG_INFINITY &&
    max !== TIMESTAMP_POS_INFINITY &&
    min >= -MAX_SAFE_BIG &&
    max <= MAX_SAFE_BIG
  );
}

/** Days since 2000-01-01 → [astronomical year, month, day] (H. Hinnant's civil_from_days). */
export function civilFromDays(days: number): readonly [number, number, number] {
  const z = days + PG_EPOCH_UNIX_DAYS + 719_468;
  const era = Math.floor(z / 146_097);
  const doe = z - era * 146_097;
  const yoe = Math.floor(
    (doe - Math.floor(doe / 1460) + Math.floor(doe / 36_524) - Math.floor(doe / 146_096)) / 365,
  );
  const doy = doe - (365 * yoe + Math.floor(yoe / 4) - Math.floor(yoe / 100));
  const mp = Math.floor((5 * doy + 2) / 153);
  const day = doy - Math.floor((153 * mp + 2) / 5) + 1;
  const month = mp < 10 ? mp + 3 : mp - 9;
  return [yoe + era * 400 + (month <= 2 ? 1 : 0), month, day];
}

/** [astronomical year, month, day] → days since 2000-01-01 (days_from_civil). */
export function daysFromCivil(year: number, month: number, day: number): number {
  const y = month <= 2 ? year - 1 : year;
  const era = Math.floor(y / 400);
  const yoe = y - era * 400;
  const doy = Math.floor((153 * (month + (month > 2 ? -3 : 9)) + 2) / 5) + day - 1;
  const doe = yoe * 365 + Math.floor(yoe / 4) - Math.floor(yoe / 100) + doy;
  return era * 146_097 + doe - 719_468 - PG_EPOCH_UNIX_DAYS;
}

const pad = (value: number, width: number): string => String(value).padStart(width, "0");

/** DATE text as PostgreSQL writes it: `2024-02-29`, `0044-03-15 BC`, `infinity`. */
export function formatDate(days: number): string {
  if (days === DATE_POS_INFINITY) return "infinity";
  if (days === DATE_NEG_INFINITY) return "-infinity";
  const [year, month, day] = civilFromDays(days);
  const bc = year <= 0;
  return `${pad(bc ? 1 - year : year, 4)}-${pad(month, 2)}-${pad(day, 2)}${bc ? " BC" : ""}`;
}

/** Parse canonical PostgreSQL DATE text, including BC and infinities; reject invalid dates. */
export function parseDate(value: string): number {
  if (/^\s*\+?infinity\s*$/iu.test(value)) return DATE_POS_INFINITY;
  if (/^\s*-infinity\s*$/iu.test(value)) return DATE_NEG_INFINITY;
  const m = /^(\d+)-(\d{2})-(\d{2})(?:\s+(BC))?$/iu.exec(value.trim());
  if (m === null) throw new RangeError("date text expected");
  const y = Number(m[1]);
  const year = m[4] === undefined ? y : 1 - y;
  const month = Number(m[2]),
    day = Number(m[3]);
  const days = daysFromCivil(year, month, day);
  const civil = civilFromDays(days);
  if (
    y === 0 ||
    !Number.isSafeInteger(y) ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    civil[0] !== year ||
    civil[1] !== month ||
    civil[2] !== day ||
    days < DATE_MIN ||
    days > DATE_MAX
  )
    throw new RangeError("date is out of range");
  return days;
}

function isoYear(year: number): string {
  if (year >= 0 && year <= 9999) return pad(year, 4);
  return `${year < 0 ? "-" : "+"}${pad(Math.abs(year), 6)}`;
}

/**
 * TIMESTAMP[TZ] as the public value (D1): ISO 8601 with exactly six fraction digits,
 * `2024-01-02T03:04:05.123456Z` for TIMESTAMPTZ (an instant, shown in UTC) and the same without
 * `Z` for TIMESTAMP (a wall-clock time with no zone). Years outside 0000–9999 use ISO's expanded
 * form (`+012345`, `-000044`); years are astronomical. Infinities are `infinity` / `-infinity`.
 */
export function formatTimestamp(micros: bigint, zoned: boolean): string {
  if (micros === TIMESTAMP_POS_INFINITY) return "infinity";
  if (micros === TIMESTAMP_NEG_INFINITY) return "-infinity";
  let days = micros / MICROS_PER_DAY_BIG;
  let rest = micros % MICROS_PER_DAY_BIG;
  if (rest < 0n) {
    rest += MICROS_PER_DAY_BIG;
    days -= 1n;
  }
  const [year, month, day] = civilFromDays(Number(days));
  const time = Number(rest);
  const fraction = time % 1_000_000;
  const seconds = Math.floor(time / 1_000_000);
  return (
    `${isoYear(year)}-${pad(month, 2)}-${pad(day, 2)}T` +
    `${pad(Math.floor(seconds / 3600), 2)}:${pad(Math.floor(seconds / 60) % 60, 2)}:` +
    `${pad(seconds % 60, 2)}.${pad(fraction, 6)}${zoned ? "Z" : ""}`
  );
}

const ISO =
  /^([+-]\d{6}|\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(Z|[+-]00(?::?00)?)?$/u;

/**
 * Parses the forms `formatTimestamp` writes, plus a space separator, 1–6 fraction digits, and
 * `+00` / `+00:00` for UTC. Returns null for anything else or an out-of-range value.
 */
export function parseTimestamp(text: string): bigint | null {
  if (text === "infinity") return TIMESTAMP_POS_INFINITY;
  if (text === "-infinity") return TIMESTAMP_NEG_INFINITY;
  const match = ISO.exec(text);
  if (match === null) return null;
  const [, y, mo, d, h, mi, s, f] = match;
  const year = Number(y);
  const month = Number(mo);
  const day = Number(d);
  const hour = Number(h);
  const minute = Number(mi);
  const second = Number(s);
  if (month < 1 || month > 12 || day < 1 || hour > 23 || minute > 59 || second > 59) return null;
  const days = daysFromCivil(year, month, day);
  if (civilFromDays(days)[2] !== day) return null;
  const fraction = BigInt((f ?? "").padEnd(6, "0"));
  const micros =
    BigInt(days) * MICROS_PER_DAY_BIG +
    BigInt((hour * 3600 + minute * 60 + second) * 1_000_000) +
    fraction;
  return micros >= TIMESTAMP_MIN && micros < TIMESTAMP_END ? micros : null;
}

/** A JS Date (ms since 1970) → stored µs. Exact to the millisecond. */
export function timestampFromDate(date: Date): bigint {
  const millis = date.getTime();
  if (!Number.isFinite(millis)) throw new RangeError("Invalid Date");
  return BigInt(millis) * 1000n - PG_EPOCH_UNIX_MICROS;
}

/** Stored µs → ms since 1970 (truncated toward −∞), e.g. for `new Date(…)` in helpers. */
export function unixMillisFromTimestamp(micros: bigint): number {
  const unix = micros + PG_EPOCH_UNIX_MICROS;
  const millis = unix / 1000n - (unix % 1000n < 0n ? 1n : 0n);
  return Number(millis);
}
