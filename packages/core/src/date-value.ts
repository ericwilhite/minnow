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
