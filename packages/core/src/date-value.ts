// Capture the intrinsic once. A Date instance may shadow `getTime`; persistence, indexes, and
// comparisons must all observe its internal [[DateValue]], not caller-controlled method code.
// Capturing the intrinsic is the point: later Date.prototype mutations must not change values.
// eslint-disable-next-line @typescript-eslint/unbound-method
const intrinsicDateGetTime = Date.prototype.getTime;
// eslint-disable-next-line @typescript-eslint/unbound-method -- See the getTime capture above.
const intrinsicDateToISOString = Date.prototype.toISOString;
// eslint-disable-next-line @typescript-eslint/unbound-method -- Captured for the same reason.
const intrinsicDateGetUtcFullYear = Date.prototype.getUTCFullYear;
// eslint-disable-next-line @typescript-eslint/unbound-method -- Captured for the same reason.
const intrinsicDateGetUtcMonth = Date.prototype.getUTCMonth;
// eslint-disable-next-line @typescript-eslint/unbound-method -- Captured for the same reason.
const intrinsicDateGetUtcDate = Date.prototype.getUTCDate;
// eslint-disable-next-line @typescript-eslint/unbound-method -- Captured for the same reason.
const intrinsicDateGetUtcDay = Date.prototype.getUTCDay;
// eslint-disable-next-line @typescript-eslint/unbound-method -- Captured for the same reason.
const intrinsicDateGetUtcHours = Date.prototype.getUTCHours;
// eslint-disable-next-line @typescript-eslint/unbound-method -- Captured for the same reason.
const intrinsicDateGetUtcMinutes = Date.prototype.getUTCMinutes;
// eslint-disable-next-line @typescript-eslint/unbound-method -- Captured for the same reason.
const intrinsicDateGetUtcSeconds = Date.prototype.getUTCSeconds;
// eslint-disable-next-line @typescript-eslint/unbound-method -- Captured for the same reason.
const intrinsicDateSetUtcDate = Date.prototype.setUTCDate;
// eslint-disable-next-line @typescript-eslint/unbound-method -- Captured for the same reason.
const intrinsicDateSetUtcMonth = Date.prototype.setUTCMonth;

/** Returns a Date's internal millisecond value without invoking an instance override. */
export function dateMilliseconds(value: Date): number {
  return intrinsicDateGetTime.call(value);
}

/** Copies a Date by its internal value without invoking instance methods. */
export function copyDate(value: Date): Date {
  return new Date(dateMilliseconds(value));
}

/** Formats a Date's internal value without invoking an instance override. */
export function dateIsoString(value: Date): string {
  return intrinsicDateToISOString.call(value);
}

export function dateUtcFullYear(value: Date): number {
  return intrinsicDateGetUtcFullYear.call(value);
}

export function dateUtcMonth(value: Date): number {
  return intrinsicDateGetUtcMonth.call(value);
}

export function dateUtcDate(value: Date): number {
  return intrinsicDateGetUtcDate.call(value);
}

export function dateUtcDay(value: Date): number {
  return intrinsicDateGetUtcDay.call(value);
}

export function dateUtcHours(value: Date): number {
  return intrinsicDateGetUtcHours.call(value);
}

export function dateUtcMinutes(value: Date): number {
  return intrinsicDateGetUtcMinutes.call(value);
}

export function dateUtcSeconds(value: Date): number {
  return intrinsicDateGetUtcSeconds.call(value);
}

export function setDateUtcDate(value: Date, day: number): number {
  return intrinsicDateSetUtcDate.call(value, day);
}

export function setDateUtcMonth(value: Date, month: number): number {
  return intrinsicDateSetUtcMonth.call(value, month);
}

const MS_PER_DAY = 86_400_000;

/** Whole days since 1970-01-01 for an epoch-millisecond value (floored, so negatives work). */
export function epochDays(milliseconds: number): number {
  return Math.floor(milliseconds / MS_PER_DAY);
}

/**
 * Proleptic Gregorian civil date for a day count since 1970-01-01, without a Date allocation
 * (Howard Hinnant's days-to-civil algorithm). Returns [year, month (0-11), day (1-31)] in a
 * shared scratch array: callers read it before the next call.
 */
const civilScratch: [number, number, number] = [0, 0, 0];
export function civilFromDays(days: number): readonly [number, number, number] {
  const z = days + 719_468;
  const era = Math.floor(z / 146_097);
  const dayOfEra = z - era * 146_097;
  const yearOfEra =
    (dayOfEra -
      Math.floor(dayOfEra / 1460) +
      Math.floor(dayOfEra / 36_524) -
      Math.floor(dayOfEra / 146_096)) /
    365;
  const yoe = Math.floor(yearOfEra);
  const dayOfYear = dayOfEra - (365 * yoe + Math.floor(yoe / 4) - Math.floor(yoe / 100));
  const mp = Math.floor((5 * dayOfYear + 2) / 153);
  const day = dayOfYear - Math.floor((153 * mp + 2) / 5) + 1;
  const month = mp < 10 ? mp + 2 : mp - 10;
  const year = yoe + era * 400 + (month <= 1 ? 1 : 0);
  civilScratch[0] = year;
  civilScratch[1] = month;
  civilScratch[2] = day;
  return civilScratch;
}

/** Days since 1970-01-01 for a civil date; month is 0-11 as in Date.UTC. */
export function daysFromCivil(year: number, month: number, day: number): number {
  const y = month <= 1 ? year - 1 : year;
  const era = Math.floor(y / 400);
  const yoe = y - era * 400;
  const mp = month > 1 ? month - 2 : month + 10;
  const dayOfYear = Math.floor((153 * mp + 2) / 5) + day - 1;
  const dayOfEra = yoe * 365 + Math.floor(yoe / 4) - Math.floor(yoe / 100) + dayOfYear;
  return era * 146_097 + dayOfEra - 719_468;
}
