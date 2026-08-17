import { afterEach, describe, expect, it, vi } from "vitest";
import { measureRepeated } from "./worker/support";

/**
 * The browser's clock is deliberately coarse: 100µs on an ordinary origin, 5µs on a cross-origin
 * isolated one. Timing one execution of anything quicker reports the clock rather than the
 * engine, which is how a table of point lookups came to read 0.20 ms in every row and 0.00 ms for
 * the fastest engine. These tests run the measurement against a simulated clock of each
 * resolution, with an operation whose real cost is known.
 */

/** A clock that only advances in `step` increments, like the real one. */
function quantizedClock(step: number): { advance(ms: number): void; install(): void } {
  let real = 0;
  return {
    advance(ms: number) {
      real += ms;
    },
    install() {
      vi.spyOn(performance, "now").mockImplementation(() => Math.floor(real / step) * step);
    },
  };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("measureRepeated", () => {
  for (const [name, step] of [
    ["an ordinary origin (100µs)", 0.1],
    ["a cross-origin isolated one (5µs)", 0.005],
  ] as const) {
    it(`recovers a 4µs operation through the clock of ${name}`, async () => {
      const clock = quantizedClock(step);
      clock.install();
      const costMs = 0.004;
      const result = await measureRepeated(() => {
        clock.advance(costMs);
        return Promise.resolve();
      }, 7);

      expect(result.batchSize).toBeGreaterThan(1);
      // Within a few percent of the truth, rather than rounded to 0 or to a tick.
      expect(result.medianMs).toBeGreaterThan(costMs * 0.9);
      expect(result.medianMs).toBeLessThan(costMs * 1.1);
    });
  }

  it("times a slow operation directly rather than repeating it", async () => {
    const clock = quantizedClock(0.1);
    clock.install();
    const result = await measureRepeated(() => {
      clock.advance(25);
      return Promise.resolve();
    }, 5);

    expect(result.batchSize).toBe(1);
    expect(result.medianMs).toBeGreaterThan(24);
    expect(result.medianMs).toBeLessThan(26);
  });

  it("does not stall on an operation the clock cannot see at all", async () => {
    const clock = quantizedClock(0.1);
    clock.install();
    let calls = 0;
    const result = await measureRepeated(() => {
      calls += 1;
      return Promise.resolve();
    }, 3);

    // A free operation reads as zero however many times it runs; the batch has to stop growing.
    expect(result.medianMs).toBe(0);
    expect(calls).toBeLessThan(200_000);
  });
});
