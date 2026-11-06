/**
 * The executor's task yields in real browsers (all three engines): a query yielding every 4 ms
 * must let the worker's timers run while it works, whatever order the engine services task
 * sources in. Law: exec/pipeline.ts (`createYieldTask`) and ruling R23: long work yields so the
 * owner's other tasks, timers included, run within about a slice plus `TIMER_TURN_MS`.
 *
 * WebKit services message tasks ahead of timers: with message-channel yields only (the recorded
 * `messages` control) its timers did not run at all during the work (review B4-p2c-4). The
 * assertion is deliberately loose (a 100 ms interval must tick at least 5 times in 1.5 s; about
 * 14 is the norm) so a loaded host passes and starvation fails. The control is attached, not
 * asserted: whether it starves depends on the engine.
 */
import { expect } from "@playwright/test";
import { test } from "./fixtures.js";

type Run = typeof import("../browser/next-yield-run.js");
const RUN = "/packages/core/browser/next-yield-run.ts";

test.describe("executor task yields let timers run", () => {
  // Launching a persistent profile on a loaded host can take most of the default 30 s.
  test.describe.configure({ timeout: 120_000 });
  test.beforeEach(async ({ page }) => {
    await page.goto("/packages/core/browser/");
  });

  for (const kind of ["yieldTask", "drive"] as const)
    test(`a 1.5 s ${kind} loop in 4 ms slices services a 100 ms interval`, async ({
      page,
    }, info) => {
      const [result, control] = await page.evaluate(
        async ([url, which]) => {
          const run = (await import(url)) as Run;
          return [await run.runYield(which), await run.runYield("messages")];
        },
        [RUN, kind] as const,
      );
      await info.attach(`${kind}-and-messages-control`, {
        body: JSON.stringify({ result, control }, null, 2),
        contentType: "application/json",
      });
      expect(result.slices).toBeGreaterThan(50);
      expect(result.ticks.length).toBeGreaterThanOrEqual(5);
    });
});
