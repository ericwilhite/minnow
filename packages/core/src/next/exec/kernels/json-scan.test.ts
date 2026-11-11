/** The scanner consumes the shared task budget even when one document is a single wide token. */
import { expect, it } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, JSON_SCAN_BUDGET_REACH } from "../../../testing/reach.js";
import { Slice, runToEnd } from "../slices.js";
import { TaskClock } from "../pipeline.js";
import { JsonScan } from "./json-scan.js";
it.each(["string", "number", "space", "nesting"])(
  "yields inside a wide %s before starting a private task",
  (shape) => {
    const value =
      shape === "string"
        ? '"' + "a".repeat(16000) + '"'
        : shape === "number"
          ? "1".repeat(16000)
          : shape === "space"
            ? " ".repeat(16000) + "0"
            : "[".repeat(8000) + "0" + "]".repeat(8000);
    let now = 0;
    const clock = new TaskClock(() => now),
      slice = new Slice(4, clock);
    now = 8;
    const scan = new JsonScan(new TextEncoder().encode(value), slice),
      job = scan.depth();
    expect(job.next().done).toBe(false);
    expect(scan.at).toBeLessThan(1200);
    expect(clock.epoch).toBe(0);
    const depth = runToEnd(job, slice);
    expect(depth).toBe(shape === "nesting" ? 8001 : 1);
  },
);

it("checks the shared budget while closing a deep validation stack", () => {
  const depth = 12017,
    bytes = new TextEncoder().encode("[".repeat(depth) + "0" + "]".repeat(depth));
  const clock = new TaskClock(() => 0),
    slice = new Slice(4, clock),
    scan = new JsonScan(bytes, slice);
  clock.now = () => (scan.at > depth ? 8 : 0);
  const job = scan.validate(new Uint8Array(depth + 1));
  expect(job.next().done).toBe(false);
  expect(scan.at).toBeGreaterThan(depth);
  expect(scan.at).toBeLessThan(depth + 2048);
  job.return(0);
});
it.each([-1, 0])("checks the shared budget across empty string elements (index %i)", (index) => {
  const bytes = new TextEncoder().encode(
    "[" + Array.from({ length: 12017 }, () => '""').join(",") + "]",
  );
  let now = 0;
  const clock = new TaskClock(() => now),
    scan = new JsonScan(bytes, new Slice(4, clock));
  now = 8;
  const job = scan.field(index);
  expect(job.next().done).toBe(false);
  expect(scan.at).toBeLessThan(5000);
  expect(clock.epoch).toBe(0);
  job.return(null);
});

it.each(seedsFor("next-json-scan-budget", [0xb648, 0xb649]))(
  "keeps every scanner phase inside the task budget (seed %i)",
  (seed) => {
    const reach = createReach<string>("JSON scanner phases", JSON_SCAN_BUDGET_REACH);
    const examples: Array<[number, number]> = [
      [0, 12017],
      [1, 12017],
      [2, 12017],
    ];
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 2 }),
        fc.integer({ min: 2049, max: 20000 }),
        (shape, width) => {
          reach.beginHistory();
          const text =
            shape === 0
              ? "[".repeat(width) + "0" + "]".repeat(width)
              : "[" + Array.from({ length: width }, () => '""').join(",") + "]";
          let now = 0;
          const clock = new TaskClock(() => now),
            scan = new JsonScan(new TextEncoder().encode(text), new Slice(4, clock));
          if (shape === 0) {
            clock.now = () => (scan.at > width ? 8 : 0);
            const job = scan.validate(new Uint8Array(width + 1));
            expect(job.next().done).toBe(false);
            expect(scan.at).toBeGreaterThan(width);
            expect(scan.at).toBeLessThan(width + 2048);
            job.return(0);
            reach.reach("closing stack");
          } else {
            now = 8;
            const job = scan.field(shape === 1 ? -1 : 0);
            expect(job.next().done).toBe(false);
            expect(scan.at).toBeLessThan(5000);
            job.return(null);
            reach.reach(
              shape === 1 ? "empty-string negative index" : "empty-string positive index",
            );
          }
          expect(clock.epoch).toBe(0);
          reach.reach("shared clock");
        },
      ),
      { seed, numRuns: 40 + examples.length, examples },
    );
    reach.assertReached();
  },
);
