/** PostgreSQL's default SQL output styles, separate from D1's ISO timestamp row values.
 * Callers validate finite ranges and refuse interval infinities before formatting.
 */
import {
  formatDate,
  MICROS_PER_DAY_BIG,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
} from "./temporal.js";
const pad = (n: number, width = 2) => String(n).padStart(width, "0");
const fraction = (n: number) => (n === 0 ? "" : `.${pad(n, 6).replace(/0+$/u, "")}`);
export function formatSqlTimestamp(micros: bigint, zoned: boolean): string {
  if (micros === TIMESTAMP_POS_INFINITY) return "infinity";
  if (micros === TIMESTAMP_NEG_INFINITY) return "-infinity";
  let day = micros / MICROS_PER_DAY_BIG,
    rest = micros % MICROS_PER_DAY_BIG;
  if (rest < 0n) {
    day -= 1n;
    rest += MICROS_PER_DAY_BIG;
  }
  const date = formatDate(Number(day)),
    bc = date.endsWith(" BC");
  const time = Number(rest),
    seconds = Math.floor(time / 1000000);
  return `${bc ? date.slice(0, -3) : date} ${pad(Math.floor(seconds / 3600))}:${pad(Math.floor(seconds / 60) % 60)}:${pad(seconds % 60)}${fraction(time % 1000000)}${zoned ? "+00" : ""}${bc ? " BC" : ""}`;
}
/** Recognize PG's native sentinel tuples only to refuse unsupported interval infinities. */
export function isInfiniteInterval(months: number, days: number, micros: bigint): boolean {
  return (
    (months === 2147483647 && days === 2147483647 && micros === 9223372036854775807n) ||
    (months === -2147483648 && days === -2147483648 && micros === -9223372036854775808n)
  );
}
export function formatSqlInterval(months: number, days: number, micros: bigint): string {
  const parts: string[] = [];
  let before = false;
  for (const [value, unit] of [
    [Math.trunc(months / 12), "year"],
    [months % 12, "mon"],
    [days, "day"],
  ] as const) {
    if (value === 0) continue;
    parts.push(
      `${before && value > 0 ? "+" : ""}${String(value)} ${unit}${value === 1 ? "" : "s"}`,
    );
    before = value < 0;
  }
  if (micros !== 0n || parts.length === 0) {
    const abs = micros < 0n ? -micros : micros,
      seconds = abs / 1000000n;
    parts.push(
      `${micros < 0n ? "-" : before ? "+" : ""}${(seconds / 3600n).toString().padStart(2, "0")}:${pad(Number((seconds / 60n) % 60n))}:${pad(Number(seconds % 60n))}${fraction(Number(abs % 1000000n))}`,
    );
  }
  return parts.join(" ");
}
