import { expect, test } from "@playwright/test";

test("renders the relational benchmark controls", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveTitle("BrowserDatabase Storage Lab");
  await expect(page.getByText("BrowserDatabase", { exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Relational workload lab" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "27 related tables" })).toBeVisible();
  await expect(page.locator("#dataset-plan-tables span")).toHaveCount(27);
  await expect(page.locator("#dataset-plan-tables")).toContainText("payment_transactions");
  await expect(page.getByRole("button", { name: "Run one test" })).toBeEnabled();
  await expect(page.getByRole("button", { name: "Compare all 15 settings" })).toBeEnabled();
  await expect(page.getByRole("button", { name: "Compare database engines" })).toBeEnabled();
  await expect(page.locator("#run-ad-hoc")).toBeDisabled();
});

test("keeps the configuration and comparison summary usable on mobile", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Relational workload lab" })).toBeVisible();
  await expect(page.getByLabel(/Dataset scale/)).toBeVisible();
  await expect(page.getByRole("button", { name: "Compare database engines" })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
});

test("compares all database public APIs with matching SQL results", async ({
  page,
  browserName,
}) => {
  test.setTimeout(120_000);
  test.skip(browserName !== "chromium", "The full Wasm adapter smoke test runs once in Chromium");
  await page.goto("/");
  await page.locator("#scale").selectOption("0.1");
  await page.getByRole("button", { name: "Compare database engines" }).click();

  await expect(page.locator("#engine-comparison-state")).toHaveText("All verified", {
    timeout: 120_000,
  });
  await expect(page.locator(".engine-result")).toHaveCount(4);
  await expect(page.locator(".engine-result.pass")).toHaveCount(4);
  await expect(page.locator(".engine-comparison-matrix")).toHaveCount(1);
  await expect(page.locator(".engine-comparison-matrix thead th")).toHaveCount(5);
  await expect(page.locator(".engine-comparison-matrix tbody tr")).toHaveCount(7);
  await expect(page.locator(".engine-sql-matrix thead th")).toHaveCount(5);
  await expect(page.locator(".engine-sql-matrix tbody tr")).toHaveCount(6);
  await expect(page.locator(".engine-query-table")).toHaveCount(4);
  await expect(page.locator(".engine-query-table tbody tr")).toHaveCount(24);
  await expect(page.locator(".engine-result").filter({ hasText: "Reopen passed" })).toHaveCount(3);
  await expect(page.locator(".engine-result").filter({ hasText: "Memory only" })).toHaveCount(1);
  await expect(page.locator(".engine-result").filter({ hasText: "Total DB size" })).toHaveCount(4);
  await expect(page.locator(".engine-result").filter({ hasText: "SQLite Wasm" })).toContainText(
    "persistent SQLite file",
  );
  await expect(page.locator("#engine-comparison-note")).toContainText(
    "Checksums must agree across all four engines",
  );
  await expect(page.locator("#progress-value")).toHaveText("100%");
});

test("shows exact parameters and derives every row count from scale", async ({ page }) => {
  await page.goto("/");

  await expect(page.locator("#scale option:checked")).toHaveText("1× — standard");
  await expect(page.locator("#scale-description")).toContainText("1,000 orders");
  await expect(page.locator("#block-size option:checked")).toHaveText("512 KiB — 524,288 bytes");
  await expect(page.locator("#run-summary-copy")).toContainText("encode it with gzip");
  await expect(page.locator("#run-summary-copy")).toContainText("27 connected tables");
  await expect(page.locator("#run-summary-copy")).toContainText("IndexedDB durability “relaxed”");

  await page.locator("#scale").selectOption("0.25");
  await page.locator("#compression").selectOption("raw");
  await page.locator("#block-size").selectOption("1048576");
  await expect(page.locator("#scale-description")).toContainText("250 orders");
  await expect(page.locator("#run-summary-copy")).toContainText("0.25× relational commerce");
  await expect(page.locator("#run-summary-copy")).toContainText("encode it with raw");
  await expect(page.locator("#run-summary-copy")).toContainText(
    "targeting 1 MiB — 1,048,576 bytes",
  );
});

test("runs the 27-table suite and executes a measured ad-hoc query", async ({ page }) => {
  await page.goto("/");
  await page.locator("#scale").selectOption("0.1");
  await page.locator("#compression").selectOption("gzip");
  await page.locator("#block-size").selectOption("262144");
  await page.getByRole("button", { name: "Run one test" }).click();

  await expect(page.locator("#verification-badge")).toContainText("reference queries pass", {
    timeout: 60_000,
  });
  await expect(page.locator("#entity-rows tr")).toHaveCount(27);
  await expect(page.locator("#entity-rows")).toContainText("order_taxes");
  await expect(page.locator("#block-rows")).toContainText("orders.total");
  await expect(page.locator("#history-rows")).toContainText("Relational commerce");
  await expect(page.locator("#progress-value")).toHaveText("100%");
  await expect(page.locator("#transaction-summary")).toContainText("transaction journal persisted");
  await expect(page.locator("#transaction-summary")).toContainText("status is “committed”");
  await expect(page.locator("#transaction-checks .transaction-check")).toHaveCount(8);
  await expect(page.locator("#transaction-checks")).toContainText("Two writers, one atomic winner");
  await expect(page.locator("#transaction-checks")).toContainText(
    "Lost commit response is reconciled",
  );
  await expect(page.locator("#library-summary")).toContainText(
    "inserted through BrowserDatabase across 27 persistent tables",
  );
  await expect(page.locator("#library-table-rows tr")).toHaveCount(27);
  await expect(page.locator("#library-checks .transaction-check")).toHaveCount(14);
  await expect(page.locator("#library-checks")).toContainText(
    "Append compaction preserves snapshots",
  );
  await expect(page.locator("#library-checks")).toContainText(
    "Competing upserts recheck keys after conflicts",
  );
  await expect(page.locator("#query-note")).toContainText(
    "These are not query-engine operator timings",
  );
  await expect(page.locator("#query-dataset .dataset-card")).toHaveCount(27);
  await expect(page.locator("#query-dataset")).toContainText(
    "tax lines many → 1 orders/items/tax rates",
  );
  await expect(page.locator("#query-checks .transaction-check")).toHaveCount(4);
  await expect(page.locator("#query-checks")).toContainText("Foreign-key graph");
  await expect(page.locator("#query-checks")).toContainText("Transaction and ledger coverage");
  await expect(page.locator("#query-results .query-card")).toHaveCount(15);
  await expect(page.locator("#query-results .query-card.passed")).toHaveCount(15);
  await expect(page.locator("#query-results")).toContainText("Tax collected by jurisdiction");
  await expect(page.locator("#query-results")).toContainText("Supplier inventory ledger");
  await expect(page.locator("#query-results")).toContainText("oracle match");

  await expect(page.locator("#run-ad-hoc")).toBeEnabled();
  await page.locator("#run-ad-hoc").click();
  await expect(page.locator("#ad-hoc-status")).toContainText("customers → orders");
  await expect(page.locator("#ad-hoc-metrics")).toContainText("Median");
  await expect(page.locator("#ad-hoc-result-rows tr")).toHaveCount(3);
  await expect(page.locator("#ad-hoc-history-rows tr")).toHaveCount(1);
});

test("compares every compression and block-size setting", async ({ page, browserName }) => {
  const matrixScale = process.env.BROWSERDATABASE_MATRIX_SCALE ?? "0.1";
  const matrixRepetitions = process.env.BROWSERDATABASE_MATRIX_REPETITIONS ?? "1";
  const outputDirectory = process.env.BROWSERDATABASE_MATRIX_OUTPUT_DIRECTORY;
  const timeoutMs = Number(process.env.BROWSERDATABASE_MATRIX_TIMEOUT_MS ?? "120000");
  const expectedRuns = 15 * Number(matrixRepetitions);
  test.setTimeout(timeoutMs);
  await page.goto("/");
  await page.locator("#scale").selectOption(matrixScale);
  await page.locator("#repetitions").selectOption(matrixRepetitions);
  await page.getByRole("button", { name: "Compare all 15 settings" }).click();

  await expect(page.locator("#history-rows tr")).toHaveCount(expectedRuns, {
    timeout: Math.max(90_000, timeoutMs - 30_000),
  });
  await expect(page.locator("#history-rows .check.pass")).toHaveCount(expectedRuns);
  await expect(page.locator("#progress-label")).toContainText(
    `${String(expectedRuns)} tests complete`,
  );
  await expect(page.locator("#history-rows")).toContainText("Relational commerce");
  await expect(page.locator("#history-rows")).toContainText("4.00 MB");
  await expect(page.locator("#comparison-summary")).toContainText("Fastest");
  await expect(page.locator("#comparison-summary")).toContainText("Smallest saved data");
  await expect(page.locator("#comparison-summary")).toContainText("Fastest insertBatch");
  await expect(page.locator("#comparison-summary")).toContainText("Fastest bounded upsert");
  await expect(page.locator("#comparison-summary")).toContainText("Fastest reference suite");

  if (outputDirectory !== undefined) {
    const downloadPromise = page.waitForEvent("download");
    await page.locator("#export").click();
    const download = await downloadPromise;
    await download.saveAs(
      `${outputDirectory}/browserdatabase-matrix-${matrixScale.replace(".", "_")}x-${browserName}.json`,
    );
  }
});
