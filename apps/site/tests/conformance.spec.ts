import { expect, test } from "@playwright/test";

test("the SQL conformance strategy publishes executable proof", async ({ page }) => {
  await page.goto("/docs/conformance/");
  await expect(page.locator("h1")).toHaveText("SQL conformance & testing");
  await expect(page.getByText("6,744 upstream queries")).toBeVisible();
  await expect(page.getByText("There is no wildcard skip list.")).toBeVisible();

  const sidebar = page.locator("#nd-sidebar");
  await expect(
    sidebar.getByText("SQL conformance & testing", { exact: true }).first(),
  ).toBeVisible();
});

test("the home page features SQL conformance and its proof", async ({ page }) => {
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "SQL support that is tested, not asserted" }),
  ).toBeVisible();
  await expect(page.getByText("6,744", { exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "See the testing strategy" })).toHaveAttribute(
    "href",
    "/docs/conformance/",
  );
  await expect(page.getByRole("link", { name: "Explore SQL support" })).toHaveAttribute(
    "href",
    "/docs/sql/feature-matrix/",
  );
});
