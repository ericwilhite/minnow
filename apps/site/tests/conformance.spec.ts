import { expect, test } from "@playwright/test";

test("the SQL compatibility page explains its checks", async ({ page }) => {
  await page.goto("/docs/conformance/");
  await expect(page.locator("h1")).toHaveText("SQL compatibility checks");
  await expect(
    page.getByText("Every supported example runs through both query executors."),
  ).toBeVisible();
  await expect(
    page.getByText(/SQLLogicTest adds thousands of database-neutral queries/),
  ).toBeVisible();

  const sidebar = page.locator("#nd-sidebar");
  await expect(
    sidebar.getByText("SQL compatibility checks", { exact: true }).first(),
  ).toBeVisible();
});

test("the home page links to PostgreSQL compatibility and its checks", async ({ page }) => {
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Know which PostgreSQL features work" }),
  ).toBeVisible();
  await expect(page.getByRole("link", { name: "See PostgreSQL compatibility" })).toHaveAttribute(
    "href",
    "/docs/sql/feature-matrix/",
  );
  await expect(page.getByRole("link", { name: "See how it is tested" })).toHaveAttribute(
    "href",
    "/docs/conformance/",
  );
});
