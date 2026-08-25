import { describe, expect, it } from "vitest";
import {
  copyDate,
  dateIsoString,
  dateMilliseconds,
  dateUtcDate,
  dateUtcDay,
  dateUtcFullYear,
  dateUtcHours,
  dateUtcMinutes,
  dateUtcMonth,
  dateUtcSeconds,
  setDateUtcDate,
  setDateUtcMonth,
} from "./date-value.js";

describe("Date intrinsic access", () => {
  it("ignores caller-controlled instance methods", () => {
    const value = new Date("2025-01-02T03:04:05.678Z");
    Object.defineProperties(value, {
      getTime: {
        value: () => {
          throw new Error("instance getTime must not run");
        },
      },
      toISOString: {
        value: () => {
          throw new Error("instance toISOString must not run");
        },
      },
    });

    expect(dateMilliseconds(value)).toBe(1_735_787_045_678);
    expect(dateIsoString(value)).toBe("2025-01-02T03:04:05.678Z");
    expect(copyDate(value)).toEqual(new Date(1_735_787_045_678));
  });

  it("preserves invalid-Date behavior", () => {
    const value = new Date(Number.NaN);

    expect(Number.isNaN(dateMilliseconds(value))).toBe(true);
    expect(copyDate(value).toString()).toBe("Invalid Date");
    expect(() => dateIsoString(value)).toThrow(RangeError);
  });

  it("keeps UTC calendar access stable after prototype mutations", () => {
    const names = [
      "getUTCFullYear",
      "getUTCMonth",
      "getUTCDate",
      "getUTCDay",
      "getUTCHours",
      "getUTCMinutes",
      "getUTCSeconds",
      "setUTCDate",
      "setUTCMonth",
    ] as const;
    const originals = names.map(
      (name) => [name, Object.getOwnPropertyDescriptor(Date.prototype, name)] as const,
    );
    try {
      for (const [name, descriptor] of originals) {
        if (descriptor === undefined) throw new Error(`Missing Date.prototype.${name}`);
        Object.defineProperty(Date.prototype, name, {
          ...descriptor,
          value: () => {
            throw new Error(`caller-controlled ${name} must not run`);
          },
        });
      }

      const value = new Date("2025-01-02T03:04:05.678Z");
      expect([
        dateUtcFullYear(value),
        dateUtcMonth(value),
        dateUtcDate(value),
        dateUtcDay(value),
        dateUtcHours(value),
        dateUtcMinutes(value),
        dateUtcSeconds(value),
      ]).toEqual([2025, 0, 2, 4, 3, 4, 5]);
      setDateUtcDate(value, 3);
      setDateUtcMonth(value, 1);
      expect(dateIsoString(value)).toBe("2025-02-03T03:04:05.678Z");
    } finally {
      for (const [name, descriptor] of originals) {
        if (descriptor !== undefined) Object.defineProperty(Date.prototype, name, descriptor);
      }
    }
  });
});
