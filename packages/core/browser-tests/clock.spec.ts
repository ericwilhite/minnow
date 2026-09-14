import { expect, test } from "@playwright/test";

test("bounds worker keepalives when the wall clock moves backward", async ({ page }) => {
  const errors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  page.on("pageerror", (error) => errors.push(error.message));

  await page.goto("/packages/core/browser/");
  const result = await page.evaluate(async () => {
    const url = "/packages/core/browser/clock-run.ts";
    const clock = (await import(url)) as typeof import("../browser/clock-run.js");
    return clock.runBackwardClockDeadline();
  });

  expect(result.settled).toBe(true);
  expect(result.errorName).toBe("DatabaseWorkerTimeoutError");
  expect(result.elapsedMs).toBeGreaterThanOrEqual(500);
  expect(result.elapsedMs).toBeLessThan(3_000);
  expect(errors).toEqual([]);
});
