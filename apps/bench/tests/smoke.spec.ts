import { expect, test, type Page } from "@playwright/test";

const pages = [
  { path: "/", title: "Minnow bench", nav: "Overview" },
  { path: "/datasets.html", title: "Datasets — Minnow bench", nav: "Datasets" },
  { path: "/query.html", title: "Query — Minnow bench", nav: "Query" },
  { path: "/suites.html", title: "Benchmarks — Minnow bench", nav: "Benchmarks" },
  { path: "/sql.html", title: "SQL support — Minnow bench", nav: "SQL support" },
];

function collectPageErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  return errors;
}

test("every page loads with the injected shell and no uncaught errors", async ({ page }) => {
  const errors = collectPageErrors(page);
  for (const entry of pages) {
    await page.goto(entry.path);
    await expect(page).toHaveTitle(entry.title);
    await expect(page.locator(".site-header .brand")).toHaveText("Minnow bench");
    await expect(page.locator('.site-header a[aria-current="page"]')).toHaveText(entry.nav);
    await expect(page.locator(".site-footer")).toContainText("Minnow bench");
  }
  expect(errors).toEqual([]);
});

test("sql page renders the live feature matrix", async ({ page }) => {
  await page.goto("/sql.html");
  // Counts are deliberately not pinned: the matrix moves whenever the SQL surface grows, and
  // the engine's own conformance test keeps each entry honest. This asserts the page is wired
  // to real data, not that the surface is frozen.
  expect(await page.locator("#sql-supported .sql-feature").count()).toBeGreaterThan(35);
  expect(await page.locator("#sql-unsupported .sql-feature").count()).toBeGreaterThan(0);
  await expect(page.locator("#sql-supported")).toContainText("select.projection");
  await expect(page.locator("#sql-summary")).toContainText("supported");
  await expect(page.locator("#sql-supported pre.shiki").first()).toBeVisible();
});

test("stays usable at a phone viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  for (const path of ["/", "/query.html", "/suites.html"]) {
    await page.goto(path);
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(
      390,
    );
  }
});

test("generates, queries, verifies, and deletes a dataset on all three engines", async ({
  page,
  browserName,
}) => {
  test.setTimeout(420_000);
  test.skip(browserName !== "chromium", "The full Wasm engine lifecycle runs once in Chromium");

  await page.goto("/datasets.html");
  await page.locator("#scale").selectOption("0.1");
  await page.locator("#generate").click();
  await expect(page.locator("#gen-status")).toContainText("Dataset ready", { timeout: 240_000 });
  const row = page.locator("#dataset-rows tr").first();
  await expect(row.locator(".badge.ok")).toHaveCount(3);

  // The registry, not this page's memory, is the source of truth: reload and re-list.
  await page.reload();
  await expect(page.locator("#dataset-rows tr")).toHaveCount(1);
  await expect(page.locator("#dataset-rows .badge.ok")).toHaveCount(3);

  // Run the default query against all three engines and require checksum agreement.
  await page.goto("/query.html");
  await expect(page.locator("#dataset-select option")).toHaveCount(1);
  await expect(page.locator("#engine-checks input:checked")).toHaveCount(3);
  await page.locator("#run").click();
  await expect(page.locator("#status")).toContainText("3 of 3 engines", { timeout: 120_000 });
  await expect(page.locator("#agreement .badge.ok")).toContainText("checksums match");
  await expect(page.locator("#engine-metrics tr")).toHaveCount(3);
  await expect(page.locator("#plan-details")).toBeAttached();
  expect(await page.locator("#result-rows tr").count()).toBeGreaterThan(0);

  // Reference suite: 15 queries, every supported engine execution must agree with its oracle.
  await page.goto("/suites.html");
  await expect(page.locator("#ref-dataset option")).toHaveCount(1);
  await expect(page.locator("#ref-engines input:checked")).toHaveCount(3);
  await page.locator("#ref-run").click();
  await expect(page.locator("#ref-status")).toContainText("agreed with its oracle", {
    timeout: 240_000,
  });
  await expect(page.locator("#ref-results tbody tr")).toHaveCount(15);
  await expect(page.locator("#ref-results .badge.fail")).toHaveCount(0);
  // Every query now compiles on every engine: COALESCE and DATE_TRUNC joined the
  // MinnowDatabase surface, so no cell reports "not supported".
  await expect(page.locator("#ref-results .badge.gap")).toHaveCount(0);
  await expect(page.locator("#ref-results")).toContainText("15/15 supported");

  // Delete the dataset from every engine's storage.
  await page.goto("/datasets.html");
  page.once("dialog", (dialog) => void dialog.accept());
  await page.locator("[data-delete]").first().click();
  await expect(page.locator("#dataset-empty")).toBeVisible({ timeout: 60_000 });
  await expect(page.locator("#dataset-rows tr")).toHaveCount(0);
});

test("storage-configuration benchmark passes every check", async ({ page, browserName }) => {
  test.setTimeout(240_000);
  test.skip(browserName !== "chromium", "The storage benchmark smoke runs once in Chromium");
  await page.goto("/suites.html");
  await page.locator("#storage-scale").selectOption("0.1");
  await page.locator("#storage-run").click();
  await expect(page.locator("#storage-status")).toContainText(
    "Storage, transactions, writes, and reference queries pass",
    { timeout: 180_000 },
  );
  await expect(page.locator("#storage-results")).toContainText("Transaction checks · 8/8 passed");
  await expect(page.locator("#storage-results")).toContainText("Write API checks · 14/14 passed");
  await expect(page.locator("#storage-results")).toContainText(
    "Dataset integrity checks · 4/4 passed",
  );
  await expect(page.locator("#storage-results")).toContainText("15/15 on SQL");
});

test("sql conformance run reports zero drift against the shipped matrix", async ({
  page,
  browserName,
}) => {
  test.setTimeout(300_000);
  test.skip(browserName !== "chromium", "The conformance smoke runs once in Chromium");
  await page.goto("/suites.html");
  await page.locator("#fm-run").click();
  await expect(page.locator("#fm-status")).toContainText("No drift", { timeout: 240_000 });
  expect(await page.locator("#fm-results tbody tr").count()).toBeGreaterThan(40);
  await expect(page.locator("#fm-results td .badge.fail")).toHaveCount(0);
});
