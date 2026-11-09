/** Exact SQL TIMESTAMP wall-clock literals; Date inputs retain their own millisecond contract. */
import { MICROS_PER_DAY_BIG, daysFromCivil, TIMESTAMP_END, TIMESTAMP_MIN } from "../temporal.js";
import { PlanningError } from "./errors.js";

// The shared front end has validated this spelling, calendar, time and optional offset.
// TIMESTAMP WITHOUT TIME ZONE ignores a written offset in PG; fixed-UTC casts keep these µs.
const SQL_TIMESTAMP =
  /^(\d{4,6})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?)?(?: ?(?:[Zz]|[+-]\d{2}(?::?\d{2})?))?(?: ([Bb][Cc]))?$/u;
export function parseTimestampLiteral(text: string): bigint {
  const match = SQL_TIMESTAMP.exec(text.trim());
  if (match === null) throw new PlanningError("Invalid TIMESTAMP literal", "22007");
  const [
    ,
    year = "",
    month = "",
    day = "",
    hour = "0",
    minute = "0",
    second = "0",
    fraction = "",
    era,
  ] = match;
  const days = daysFromCivil(
    era === undefined ? Number(year) : 1 - Number(year),
    Number(month),
    Number(day),
  );
  const micros =
    BigInt(days) * MICROS_PER_DAY_BIG +
    BigInt((Number(hour) * 3600 + Number(minute) * 60 + Number(second)) * 1_000_000) +
    BigInt(fraction.padEnd(6, "0"));
  if (micros < TIMESTAMP_MIN || micros >= TIMESTAMP_END)
    throw new PlanningError("TIMESTAMP literal out of range", "22008");
  return micros;
}
