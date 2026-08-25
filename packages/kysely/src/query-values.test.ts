import { describe, expect, it, vi } from "vitest";
import { kyselyQueryValues } from "./query-values.js";

describe("kyselyQueryValues", () => {
  it("copies a Date without invoking caller-controlled methods", () => {
    const input = new Date("2026-08-25T12:34:56.789Z");
    Object.defineProperties(input, {
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

    const [copied] = kyselyQueryValues([input]);
    expect(copied).toBeInstanceOf(Date);
    expect(copied).not.toBe(input);
    expect(Date.prototype.getTime.call(copied)).toBe(1_787_661_296_789);
  });

  it("rejects unsupported parameter values with their one-based position", () => {
    expect(() => kyselyQueryValues([1, undefined])).toThrow("Kysely parameter 2");

    const sparse = [1] as unknown[];
    sparse.length = 2;
    expect(() => kyselyQueryValues(sparse)).toThrow("Kysely parameter 2");
  });

  it("keeps using the captured intrinsic after Date.prototype changes", () => {
    const input = new Date("2026-08-25T12:34:56.789Z");
    const getTime = vi.spyOn(Date.prototype, "getTime").mockImplementation(() => {
      throw new Error("mutated Date.prototype.getTime must not run");
    });
    try {
      const [copied] = kyselyQueryValues([input]);
      expect(copied).toBeInstanceOf(Date);
      expect((copied as Date).valueOf()).toBe(1_787_661_296_789);
    } finally {
      getTime.mockRestore();
    }
  });
});
