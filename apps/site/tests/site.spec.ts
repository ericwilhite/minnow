import { expect, test } from "@playwright/test";

/**
 * The site's claims are all things that either happen in the visitor's browser or do not happen
 * at all, so these tests exercise them rather than checking that the words are on the page.
 */

test("the home page builds a database in the browser and answers a query", async ({ page }) => {
  await page.goto("/");
  const panel = page.locator(".minnow-panel");
  await expect(panel).toBeVisible();

  // The dataset is generated and loaded into IndexedDB before the console appears.
  const run = panel.getByRole("button", { name: "Run", exact: true });
  await expect(run).toBeVisible({ timeout: 120_000 });

  await run.click();
  // The seeded query groups three years of orders by month.
  await expect(panel.locator(".statusbar").first()).toContainText(/\d+ rows/, {
    timeout: 60_000,
  });

  // Landing must not scroll the reader past the hero: the console does not take focus.
  expect(await page.evaluate(() => window.scrollY)).toBe(0);
});

test("the playground reopens an existing database instead of rebuilding it", async ({ page }) => {
  await page.goto("/playground/");
  await expect(page.locator(".minnow-panel .statusbar").first()).toBeVisible({
    timeout: 120_000,
  });

  await page.reload();
  await expect(page.getByText("this database was already on your machine")).toBeVisible({
    timeout: 60_000,
  });
});

test("the SQL docs render and the feature matrix comes from the checked-in fixture", async ({
  page,
}) => {
  await page.goto("/docs/sql/");
  await expect(page.locator("h1")).toHaveText("Running SQL");

  await page.goto("/docs/sql/feature-matrix/");
  await expect(page.locator("h1")).toHaveText("Feature matrix");
  // Counts are rendered from sql-feature-matrix.json, so they move when the engine's surface does.
  await expect(page.getByText(/\d+ forms, each executed through both executors/)).toBeVisible();
  await expect(page.getByText(/\d+ forms, each checked on every test run/)).toBeVisible();
  await expect(page.locator("body")).toContainText("UPDATE requires a table with a unique key");
});

test("the docs navigation covers every section", async ({ page }) => {
  await page.goto("/docs/");
  const sidebar = page.locator("#nd-sidebar");
  for (const section of ["SQL", "Client", "Storage", "Reference"]) {
    await expect(sidebar.getByText(section, { exact: true }).first()).toBeVisible();
  }
});

test("the benchmarks page runs a suite in the browser and verifies it", async ({ page }) => {
  await page.goto("/benchmarks/");

  // Minnow alone at the smallest scale: enough to prove the whole chain without asking a test
  // runner to download two WebAssembly builds.
  await page.getByRole("checkbox", { name: /SQLite/ }).uncheck();
  await page.getByRole("button", { name: "0.1×" }).click();
  await page.getByRole("button", { name: "Run", exact: true }).click();

  const failure = page.locator(".text-red-500");
  await expect
    .poll(
      async () =>
        (await page.getByRole("heading", { name: "Reads" }).count()) > 0
          ? "done"
          : (await failure.count()) > 0
            ? await failure.innerText()
            : "running",
      { timeout: 240_000, intervals: [2_000] },
    )
    .toBe("done");
  await expect(page.getByText("agreed with the independent oracle")).toBeVisible();
  // OLTP and OLAP stay split; a blended score would hide the trade-off.
  await expect(page.getByRole("heading", { name: "OLTP" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "OLAP" })).toBeVisible();
});

test("the benchmarks route is cross-origin isolated", async ({ page }) => {
  await page.goto("/benchmarks/");
  expect(await page.evaluate(() => window.crossOriginIsolated)).toBe(true);
  // And the rest of the site is not, so ordinary pages keep their cross-origin subresources.
  await page.goto("/docs/");
  expect(await page.evaluate(() => window.crossOriginIsolated)).toBe(false);
});
