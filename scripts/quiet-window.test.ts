import { expect, it } from "vitest";
import { QuietWindow } from "./lib/quiet-window.mts";

it("requires ten unchanged confirmations and restarts on physical changes or active work", () => {
  const window = new QuietWindow();
  expect(() => window.timings(0, 0)).toThrow(/not been confirmed/);
  for (let i = 0; i < 10; i++) expect(window.observe("first", i * 50)).toBe(false);
  expect(window.observe("changed", 500)).toBe(false);
  for (let i = 1; i <= 9; i++) expect(window.observe("changed", 500 + i * 50)).toBe(false);
  expect(window.observe(undefined, 1000)).toBe(false);
  for (let i = 1; i <= 9; i++) expect(window.observe("changed", 1000 + i * 50)).toBe(false);
  expect(() => window.timings(0, 1450)).toThrow(/not been confirmed/);
  expect(window.observe("changed", 1500)).toBe(true);
  expect(window.timings(0, 1500)).toEqual({
    observedWorkMs: 1050,
    confirmationMs: 450,
    totalMs: 1500,
  });
});
