import { expect, test } from "@playwright/test";

// The docs site is the only place runnable examples live, so this smoke keeps them honest:
// the engine must boot in every browser and the printed output must match the snippet.
test("home page runs a showcase example against an in-memory store", async ({ page }) => {
  await page.goto("/");
  // Not pinned to an exact count: the showcase grows whenever the API does. What matters is
  // that the examples render and that one of them really runs.
  expect(await page.locator(".example").count()).toBeGreaterThanOrEqual(4);
  await page.locator('[data-run="sql"]').click();
  await expect(page.locator('[data-output="sql"]')).toContainText('"score": 30', {
    timeout: 20_000,
  });
  await expect(page.locator('[data-output="sql"]')).toContainText('"people": 2');
});
