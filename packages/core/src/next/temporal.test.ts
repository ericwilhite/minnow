import { Temporal as polyfillTemporal } from "@js-temporal/polyfill";
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { seedsFor } from "../testing/seeds.js";
import {
  DATE_MAX,
  DATE_MIN,
  DATE_NEG_INFINITY,
  DATE_POS_INFINITY,
  TIMESTAMP_END,
  TIMESTAMP_MIN,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
  civilFromDays,
  daysFromCivil,
  formatDate,
  formatTimestamp,
  parseTimestamp,
  timestampFromDate,
  timestampSafeRange,
  unixMillisFromTimestamp,
} from "./temporal.js";
import { INT64_MAX } from "./types.js";

/**
 * Temporal is an independent oracle for the proleptic Gregorian calendar. Node 26 ships it; the
 * CI runners' Node 22 and 24 do not, so they use the TC39 reference polyfill.
 */
interface TemporalOracle {
  PlainDate: { from(fields: { year: number; month: number; day: number }): { toString(): string } };
  Instant: {
    fromEpochNanoseconds(ns: bigint): { toString(options: { fractionalSecondDigits: 6 }): string };
  };
}
const Temporal =
  (globalThis as unknown as { Temporal?: TemporalOracle }).Temporal ?? polyfillTemporal;
const SEEDS = seedsFor("next-temporal", [0x7e3a]);
/** Temporal's own range is ±271821 years; within it every value is checked against it. */
const ORACLE_MIN = -98_000_000_000_000_000n;
const ORACLE_MAX = 8_000_000_000_000_000_000n;
const UNIX_EPOCH_NS_OFFSET = 946_684_800_000_000_000n;

describe("PostgreSQL-epoch dates and timestamps", () => {
  it("anchors the epoch and PostgreSQL's range limits", () => {
    expect(civilFromDays(0)).toEqual([2000, 1, 1]);
    expect(daysFromCivil(1970, 1, 1)).toBe(-10_957);
    expect(civilFromDays(DATE_MIN)).toEqual([-4713, 11, 24]);
    expect(civilFromDays(DATE_MAX)).toEqual([5_874_897, 12, 31]);
    expect(formatDate(DATE_MIN)).toBe("4714-11-24 BC");
    expect(formatDate(0)).toBe("2000-01-01");
    expect(formatDate(DATE_POS_INFINITY)).toBe("infinity");
    expect(formatDate(DATE_NEG_INFINITY)).toBe("-infinity");
    expect(formatTimestamp(TIMESTAMP_MIN, true)).toBe("-004713-11-24T00:00:00.000000Z");
    expect(formatTimestamp(TIMESTAMP_END - 1n, false)).toBe("+294276-12-31T23:59:59.999999");
    expect(TIMESTAMP_END - 1n < INT64_MAX).toBe(true);
    expect(formatTimestamp(TIMESTAMP_POS_INFINITY, true)).toBe("infinity");
    expect(formatTimestamp(TIMESTAMP_NEG_INFINITY, false)).toBe("-infinity");
    expect(formatTimestamp(-1n, true)).toBe("1999-12-31T23:59:59.999999Z");
  });

  it("keeps infinities and out-of-range values off the f64 fast path", () => {
    expect(timestampSafeRange(-(2n ** 53n) + 1n, 2n ** 53n - 1n)).toBe(true);
    expect(timestampSafeRange(0n, TIMESTAMP_POS_INFINITY)).toBe(false);
    expect(timestampSafeRange(TIMESTAMP_NEG_INFINITY, 0n)).toBe(false);
    expect(timestampSafeRange(0n, 2n ** 53n)).toBe(false);
  });

  it("converts to and from JS Date at millisecond precision", () => {
    const date = new Date("2024-01-02T03:04:05.123Z");
    const micros = timestampFromDate(date);
    expect(formatTimestamp(micros, true)).toBe("2024-01-02T03:04:05.123000Z");
    expect(unixMillisFromTimestamp(micros + 999n)).toBe(date.getTime());
    expect(unixMillisFromTimestamp(timestampFromDate(new Date(-1)))).toBe(-1);
  });

  it("refuses malformed and out-of-range text", () => {
    for (const text of ["2024-02-30T00:00:00Z", "2024-13-01T00:00:00", "2024-01-01T24:00:00", "x"])
      expect(parseTimestamp(text), text).toBeNull();
    expect(parseTimestamp("+294277-01-01T00:00:00")).toBeNull();
    expect(parseTimestamp("2024-01-02 03:04:05.5+00")).toBe(
      parseTimestamp("2024-01-02T03:04:05.500000Z"),
    );
  });

  it.each(SEEDS)("agrees with Temporal and round-trips its text (seed %i)", (seed) => {
    fc.assert(
      fc.property(fc.bigInt({ min: ORACLE_MIN, max: ORACLE_MAX }), (micros) => {
        const text = formatTimestamp(micros, true);
        const oracle = Temporal.Instant.fromEpochNanoseconds(
          micros * 1000n + UNIX_EPOCH_NS_OFFSET,
        ).toString({ fractionalSecondDigits: 6 });
        expect(text).toBe(oracle);
        expect(parseTimestamp(text)).toBe(micros);
        expect(parseTimestamp(formatTimestamp(micros, false))).toBe(micros);
      }),
      { seed, numRuns: 2000 },
    );
    fc.assert(
      fc.property(fc.integer({ min: DATE_MIN, max: DATE_MAX }), (days) => {
        const [year, month, day] = civilFromDays(days);
        expect(daysFromCivil(year, month, day)).toBe(days);
        if (Math.abs(year) < 271_000) {
          const oracle = Temporal.PlainDate.from({ year, month, day }).toString();
          expect(
            oracle.endsWith(`-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`),
          ).toBe(true);
        }
      }),
      { seed, numRuns: 2000 },
    );
  });
});
