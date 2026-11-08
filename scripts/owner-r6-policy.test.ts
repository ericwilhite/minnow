import { expect, it } from "vitest";
import { QuietHost } from "./owner-r6-policy.js";
it("refuses absent, invalid or oversubscribed load evidence, without rounding", () => {
  for (const history of [
    [],
    [[]],
    [[NaN]],
    [[Infinity]],
    [[-1]],
    [[1.0000001]],
    [[0.2], [2], [0.1]],
    [[1], [1.1]],
  ]) {
    const host = new QuietHost();
    for (const sample of history) host.observe(sample);
    expect(() => host.assertQuiet()).toThrow("report refused");
  }
});
it("admits a measured quiet history including the exact boundary", () => {
  const host = new QuietHost();
  for (const load of [0, 0.2, 1, 0.9]) host.observe([load, 10, 20]);
  expect(() => host.assertQuiet()).not.toThrow();
  expect(host.samples).toHaveLength(4);
});
