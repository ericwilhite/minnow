/**
 * The new executor's aggregation shapes (the analytics harness's agg queries, next-agg-shapes.ts)
 * in a real browser: recorded nightly by the browser performance lane, never judged
 * (MINNOW_AGG_MICROBENCH=1; skipped otherwise). Prints one `AGG_MICROBENCH {json}` line for the
 * lane to collect, and attaches the same JSON.
 */
import { expect } from "@playwright/test";
import { test } from "./fixtures.js";

type Run = typeof import("../browser/next-agg-bench-run.js");
const RUN = "/packages/core/browser/next-agg-bench-run.ts";

test.describe("rebuild core aggregation microbench", () => {
  test.skip(process.env.MINNOW_AGG_MICROBENCH !== "1", "opt-in: MINNOW_AGG_MICROBENCH=1");
  test.describe.configure({ timeout: 1_800_000 });

  test("agg harness shapes through the aggregate operator", async ({ page }, info) => {
    await page.goto("/packages/core/browser/");
    const rows = Number(process.env.MINNOW_AGG_ROWS ?? 1_048_576);
    const only = (process.env.MINNOW_AGG_ONLY ?? "").split(",").filter((s) => s !== "");
    const results = await page.evaluate(
      async ([url, n, names]) => ((await import(url)) as Run).runAggBench(n, 5, names),
      [RUN, rows, only] as const,
    );
    const body = JSON.stringify({ browser: info.project.name, results });
    console.log(`AGG_MICROBENCH ${body}`);
    await info.attach("agg-microbench", { body, contentType: "application/json" });
    expect(results.length).toBeGreaterThan(0);
    if (only.length === 0)
      expect(results.map((result) => result.shape)).toContain("agg_count_distinct_dictionary");
  });
});
