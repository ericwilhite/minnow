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

test("benchmarks keep OLTP and OLAP reads and writes visible", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 720 });
  await page.goto("/benchmarks/");
  const pane = page.locator("section.bench-pane:visible");
  await expect(pane).toHaveCount(1);
  await expect(pane).toContainText("OLTP");
  await expect(pane).toContainText("OLAP");
  await expect(pane).toContainText("Every measured query");
  await expect(pane).toContainText(/bulk ingestion/i);
  await expect(pane.locator(".split-table th.engine-start")).toHaveCount(3);
  await expect(pane.locator(".bench-table th.engine-group")).toHaveCount(3);
  expect(
    await pane
      .locator(".engine-start")
      .first()
      .evaluate((cell) => getComputedStyle(cell).borderLeftWidth),
  ).toBe("2px");
  const stripeColors = await pane.locator(".split-table tbody tr").evaluateAll((rows) =>
    rows.slice(0, 2).map((row) => {
      const cell = row.querySelector("td");
      return cell === null ? "" : getComputedStyle(cell).backgroundColor;
    }),
  );
  expect(stripeColors[0]).not.toBe(stripeColors[1]);
  expect(
    await pane
      .locator(".read-summary .chart-card")
      .first()
      .evaluate((card) => card.getBoundingClientRect().width),
  ).toBeGreaterThan(520);
  await expect(page.locator("main")).not.toContainText("DuckDB");

  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  const tableOverflow = await pane.locator(".table-scroll").evaluate((scroller) => ({
    clientWidth: scroller.clientWidth,
    scrollWidth: scroller.scrollWidth,
  }));
  expect(tableOverflow.scrollWidth).toBeGreaterThan(tableOverflow.clientWidth);
});

// The toggles are CSS-only, so the rule revealing a pane has to name every published scale and
// browser. A hand-maintained list of those selectors once went stale against a new scale and
// hid every pane on the page, so this walks the whole matrix rather than trusting the default.
test("every published scale and browser reveals exactly one pane", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 720 });
  await page.goto("/benchmarks/");
  const scaleIds = await page
    .locator('input[name="bench-scale"]')
    .evaluateAll((inputs) => inputs.map((input) => input.id));
  const browserIds = await page
    .locator('input[name="bench-browser"]')
    .evaluateAll((inputs) => inputs.map((input) => input.id));
  expect(browserIds.length).toBeGreaterThan(0);

  for (const browserId of browserIds) {
    for (const scaleId of scaleIds.length > 0 ? scaleIds : [undefined]) {
      // The radios sit behind their labels, which is what a visitor clicks.
      if (scaleId !== undefined) await page.locator(`label[for="${scaleId}"]`).click();
      await page.locator(`label[for="${browserId}"]`).click();
      const visible = page.locator("section.bench-pane:visible");
      await expect(visible, `${scaleId ?? "single scale"} / ${browserId}`).toHaveCount(1);
      const paneId = await visible.evaluate((pane) => pane.id);
      if (scaleId !== undefined) expect(paneId).toContain(scaleId.replace("scale-", ""));
      expect(paneId).toContain(browserId.replace("toggle-", ""));
    }
  }
});

test("testing guide is the contributor runner reference", async ({ page }) => {
  await page.goto("/docs/testing/");
  await expect(page.getByRole("heading", { name: "Testing & benchmarks", level: 1 })).toBeVisible();
  await expect(page.locator("main")).toContainText("npm run test:browser:library");
  await expect(page.locator("main")).toContainText("npm run benchmark:capture");
});
