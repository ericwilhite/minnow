import { describe, expect, it, vi } from "vitest";
import { dateIsoString, dateMilliseconds } from "./date-value.js";

describe("Date value intrinsics", () => {
  it("ignores caller-controlled instance methods", () => {
    const value = new Date("2026-08-25T12:34:56.789Z");
    Object.defineProperties(value, {
      getTime: {
        value: () => {
          throw new Error("caller getTime must not run");
        },
      },
      toISOString: {
        value: () => {
          throw new Error("caller toISOString must not run");
        },
      },
    });

    expect(dateMilliseconds(value)).toBe(1_787_661_296_789);
    expect(dateIsoString(value)).toBe("2026-08-25T12:34:56.789Z");
  });

  it("keeps using the captured intrinsics after Date.prototype changes", () => {
    const value = new Date("2026-08-25T12:34:56.789Z");
    const getTime = vi.spyOn(Date.prototype, "getTime").mockImplementation(() => {
      throw new Error("mutated Date.prototype.getTime must not run");
    });
    const toISOString = vi.spyOn(Date.prototype, "toISOString").mockImplementation(() => {
      throw new Error("mutated Date.prototype.toISOString must not run");
    });
    try {
      expect(dateMilliseconds(value)).toBe(1_787_661_296_789);
      expect(dateIsoString(value)).toBe("2026-08-25T12:34:56.789Z");
    } finally {
      getTime.mockRestore();
      toISOString.mockRestore();
    }
  });
});
