/**
 * PostgreSQL's date and timestamp arithmetic, shared by the row executor, the vector executor,
 * and type inference so the three cannot disagree:
 *
 * - `date - date` is the whole number of days between them;
 * - `date + integer`, `integer + date`, and `date - integer` shift a date by whole days;
 * - a timestamp (or a date, read as its midnight) minus another is an interval of days and
 *   microseconds, the hours folded into days the way PostgreSQL's justify_hours does;
 * - a timestamp or date plus or minus an interval value is a timestamp: months first, with the
 *   day clamped to the target month's last day, then days, then the time part.
 *
 * Every datetime is an instant in UTC, so a day is always 24 hours.
 */
import { civilFromDays, dateMilliseconds, daysFromCivil, epochDays } from "../date-value.js";
import {
  dateDomainValue,
  externalSqlDomainValue,
  intervalDomainFromParts,
  intervalDomainParts,
  isDateDomainValue,
} from "./sql-domains.js";

const DAY_MILLISECONDS = 86_400_000;
const DAY_MICROSECONDS = 86_400_000_000;
/** ECMAScript's Date range: 100,000,000 days either side of the epoch. */
const MAX_DATE_MILLISECONDS = 8.64e15;

/** The operand kinds datetime arithmetic distinguishes, as values or as inferred types. */
export type DatetimeOperandKind = "date" | "datetime" | "interval" | "number" | "null" | "other";

/**
 * The result kind of `left operator right` when PostgreSQL resolves it as datetime arithmetic,
 * or undefined when it does not (both sides numbers, or no datetime operand at all). An untyped
 * NULL takes the type that makes the operator exist, as PostgreSQL's unknown-literal rule does.
 */
export function datetimeArithmeticKind(
  operator: string,
  left: DatetimeOperandKind,
  right: DatetimeOperandKind,
): "date" | "datetime" | "interval" | "number" | undefined {
  if (operator !== "+" && operator !== "-") return undefined;
  const temporal = (kind: DatetimeOperandKind): boolean =>
    kind === "date" || kind === "datetime" || kind === "interval";
  if (!temporal(left) && !temporal(right)) return undefined;
  const instant = (kind: DatetimeOperandKind): boolean => kind === "date" || kind === "datetime";
  if (operator === "-") {
    if (left === "date" && (right === "date" || right === "null")) return "number";
    if (left === "null" && right === "date") return "number";
    if (left === "date" && right === "number") return "date";
    if (instant(left) && (instant(right) || right === "null")) return "interval";
    if (left === "null" && right === "datetime") return "interval";
    if (instant(left) && right === "interval") return "datetime";
    return undefined;
  }
  if ((left === "date" && right === "number") || (left === "number" && right === "date")) {
    return "date";
  }
  if (left === "date" && right === "null") return "date";
  if (left === "null" && right === "date") return "date";
  if (instant(left) && (right === "interval" || right === "null")) return "datetime";
  if ((left === "interval" || left === "null") && instant(right)) return "datetime";
  return undefined;
}

/** Days since 1970-01-01 for a tagged DATE value. */
function calendarDays(value: string): number {
  const text = String(externalSqlDomainValue(value));
  return daysFromCivil(
    Number(text.slice(0, 4)),
    Number(text.slice(5, 7)) - 1,
    Number(text.slice(8, 10)),
  );
}

function calendarDate(days: number): string {
  const [year, month, day] = civilFromDays(days);
  if (year < 1 || year > 9999) throw new RangeError("date out of range");
  const value = dateDomainValue(
    `${String(year).padStart(4, "0")}-${String(month + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`,
  );
  if (value === null) throw new RangeError("date out of range");
  return value;
}

function wholeDays(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new TypeError("Date arithmetic takes a whole number of days");
  }
  return value;
}

/** Epoch milliseconds of a timestamp, or of a date at its midnight; undefined otherwise. */
function instantMilliseconds(value: unknown): number | undefined {
  if (value instanceof Date) return dateMilliseconds(value);
  if (isDateDomainValue(value)) return calendarDays(value) * DAY_MILLISECONDS;
  return undefined;
}

/** An instant shifted by an interval: months (clamping the day), then days, then time. */
function shiftedInstant(
  milliseconds: number,
  interval: readonly [number, number, number],
  sign: 1 | -1,
): Date {
  let shifted = milliseconds;
  const months = sign * interval[0];
  if (months !== 0) {
    const days = epochDays(shifted);
    const timeOfDay = shifted - days * DAY_MILLISECONDS;
    const [year, month, day] = civilFromDays(days);
    const monthIndex = year * 12 + month + months;
    const targetYear = Math.floor(monthIndex / 12);
    const targetMonth = monthIndex - targetYear * 12;
    const monthLength =
      daysFromCivil(targetYear + (targetMonth === 11 ? 1 : 0), (targetMonth + 1) % 12, 1) -
      daysFromCivil(targetYear, targetMonth, 1);
    shifted =
      daysFromCivil(targetYear, targetMonth, Math.min(day, monthLength)) * DAY_MILLISECONDS +
      timeOfDay;
  }
  shifted += sign * (interval[1] * DAY_MILLISECONDS + interval[2] / 1000);
  if (!Number.isFinite(shifted) || Math.abs(shifted) > MAX_DATE_MILLISECONDS) {
    throw new RangeError("timestamp out of range");
  }
  return new Date(Math.trunc(shifted));
}

/** `later - earlier` as an interval: whole days, then the remaining microseconds. */
function differenceInterval(later: number, earlier: number): string {
  const microseconds = Math.round((later - earlier) * 1000);
  const days = Math.trunc(microseconds / DAY_MICROSECONDS);
  return intervalDomainFromParts(0, days, microseconds - days * DAY_MICROSECONDS);
}

/**
 * The value of `left operator right` when it is datetime arithmetic, or undefined when neither
 * operand is a date, timestamp, or interval (ordinary arithmetic then applies). Both operands
 * are non-NULL. A datetime operand in any other combination has no PostgreSQL operator.
 */
export function datetimeArithmeticValue(operator: string, left: unknown, right: unknown): unknown {
  const leftDate = isDateDomainValue(left);
  const rightDate = isDateDomainValue(right);
  const leftInterval = intervalDomainParts(left);
  const rightInterval = intervalDomainParts(right);
  if (
    !leftDate &&
    !rightDate &&
    !(left instanceof Date) &&
    !(right instanceof Date) &&
    leftInterval === undefined &&
    rightInterval === undefined
  ) {
    return undefined;
  }
  if (operator === "-") {
    if (leftDate && rightDate) return calendarDays(left) - calendarDays(right);
    if (leftDate && typeof right === "number") {
      return calendarDate(calendarDays(left) - wholeDays(right));
    }
    const leftInstant = instantMilliseconds(left);
    const rightInstant = instantMilliseconds(right);
    if (leftInstant !== undefined && rightInstant !== undefined) {
      return differenceInterval(leftInstant, rightInstant);
    }
    if (leftInstant !== undefined && rightInterval !== undefined) {
      return shiftedInstant(leftInstant, rightInterval, -1);
    }
  } else if (operator === "+") {
    if (leftDate && typeof right === "number") {
      return calendarDate(calendarDays(left) + wholeDays(right));
    }
    if (rightDate && typeof left === "number") {
      return calendarDate(calendarDays(right) + wholeDays(left));
    }
    const leftInstant = instantMilliseconds(left);
    if (leftInstant !== undefined && rightInterval !== undefined) {
      return shiftedInstant(leftInstant, rightInterval, 1);
    }
    const rightInstant = instantMilliseconds(right);
    if (rightInstant !== undefined && leftInterval !== undefined) {
      return shiftedInstant(rightInstant, leftInterval, 1);
    }
  }
  throw new TypeError(
    `No ${operator} operator for ${operandName(left)} and ${operandName(right)} values`,
  );
}

function operandName(value: unknown): string {
  if (value instanceof Date) return "timestamp";
  if (isDateDomainValue(value)) return "date";
  if (intervalDomainParts(value) !== undefined) return "interval";
  if (typeof value === "number") return "number";
  if (typeof value === "boolean") return "boolean";
  return "text";
}
