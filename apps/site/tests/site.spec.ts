import { expect, test } from "@playwright/test";

/**
 * The site's claims are all things that either happen in the visitor's browser or do not happen
 * at all, so these tests exercise them rather than checking that the words are on the page.
 */

test("the home page builds a database in the browser and answers a query", async ({ page }) => {
  await page.goto("/");
  // The devtools mark their own host element, whatever the page wraps it in.
  const panel = page.locator("[data-minnow-devtools]");

  // The dataset is generated and loaded into IndexedDB before the console appears.
  const run = panel.getByRole("button", { name: "Run", exact: true });
  await expect(run).toBeVisible({ timeout: 120_000 });

  // Landing must leave the reader at the top of the hero: the console does not take focus while
  // it builds. Checked before anything is clicked, since clicking scrolls the button into view.
  expect(await page.evaluate(() => window.scrollY)).toBe(0);

  await run.click();
  // The seeded query groups three years of orders by month.
  await expect(panel.locator(".statusbar").first()).toContainText(/\d+ rows/, {
    timeout: 60_000,
  });
});

test("the playground reopens an existing database instead of rebuilding it", async ({ page }) => {
  await page.goto("/playground/");
  await expect(page.locator("[data-minnow-devtools] .statusbar").first()).toBeVisible({
    timeout: 120_000,
  });

  // The inline panel fills the box the page gives it rather than standing at a height of its own.
  expect(
    await page.evaluate(() => {
      const host = document.querySelector("[data-minnow-devtools]");
      const box = host?.parentElement;
      const inline = host?.shadowRoot?.querySelector(".panel");
      if (box === null || box === undefined || inline === null || inline === undefined) return null;
      return [box.getBoundingClientRect().height, inline.getBoundingClientRect().height];
    }),
  ).toEqual([720, 720]);

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
  for (const section of ["SQL", "Schema", "Typed client", "Engine", "Storage", "Reference"]) {
    await expect(sidebar.getByText(section, { exact: true }).first()).toBeVisible();
  }
});

test("the benchmarks page runs a suite in the browser and verifies it", async ({ page }) => {
  await page.goto("/benchmarks/");

  // Minnow alone at the smallest scale: enough to prove the whole chain without asking a test
  // runner to download two WebAssembly builds. Reads alone, so the assertions below name one
  // section's tables rather than matching the write suite's too.
  await page.getByRole("checkbox", { name: /SQLite/ }).uncheck();
  await page.getByRole("checkbox", { name: /Writes/ }).uncheck();
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

  // Timings are measured by the batch, so a lookup faster than the clock's tick reports its own
  // cost. Before that, every sub-millisecond case on this page read as a multiple of 0.1 ms.
  const oltpValues = await page.locator("table").first().locator("td.tabular-nums").allInnerTexts();
  expect(oltpValues.length).toBeGreaterThan(3);
  expect(oltpValues.every((value) => /^0\.00 ms$|^0\.\d0 ms$/.test(value))).toBe(false);

  // Storage is reported per engine from each engine's own accounting.
  await expect(page.getByRole("heading", { name: "Storage" })).toBeVisible();
  await expect(page.getByText("as each engine stored them just now")).toBeVisible();
});

test("the benchmarks route is cross-origin isolated", async ({ page }) => {
  await page.goto("/benchmarks/");
  expect(await page.evaluate(() => window.crossOriginIsolated)).toBe(true);
  // And the rest of the site is not, so ordinary pages keep their cross-origin subresources.
  await page.goto("/docs/");
  expect(await page.evaluate(() => window.crossOriginIsolated)).toBe(false);
});

test("every docs page is published as markdown, with indexes above it", async ({
  page,
  request,
}) => {
  // llms.txt indexes the set; each entry links the markdown rather than the page, and links are
  // site-relative so a local build reads as correctly as the deployed one.
  const index = await request.get("/llms.txt");
  expect(index.ok()).toBe(true);
  const listed = await index.text();
  expect(listed).toContain("](/docs/sql/select.md)");
  expect(listed).not.toContain("](https://minnowdb.com");

  // And the markdown it points at is the page's own prose, with its components expanded: the
  // feature matrix is a table of the same fixture the engine is tested against.
  const markdown = await request.get("/docs/sql/feature-matrix.md");
  expect(markdown.ok()).toBe(true);
  const text = await markdown.text();
  expect(text).toContain("# Feature matrix");
  expect(text).toContain("| `select.projection` | E051 |");
  expect(text).not.toMatch(/<[A-Z]/);

  // The rules file an agent is told to fetch is generated from the page that documents it, so
  // the two cannot drift apart.
  const rules = await request.get("/agent-rules.md");
  expect(rules.ok()).toBe(true);
  expect(await rules.text()).toContain(
    "`UPDATE` and `DELETE` require a table with a `PRIMARY KEY`",
  );

  // And the whole set is one link away from every page.
  await page.goto("/");
  await expect(page.getByRole("link", { name: "AI & LLMs" }).first()).toHaveAttribute(
    "href",
    "/docs/reference/agents/",
  );

  // A reader that prefers markdown is told where it is without knowing the rule.
  await page.goto("/docs/sql/select/");
  const alternate = page.locator('link[rel="alternate"][type="text/markdown"]');
  await expect(alternate).toHaveAttribute("href", "https://minnowdb.com/docs/sql/select.md");
});

test("the docs sidebar offers the version it is showing", async ({ page }) => {
  await page.goto("/docs/");
  const picker = page.getByLabel("Documentation version");
  await expect(picker).toBeVisible();
  // The unprefixed build is the current release, and the picker says so.
  await expect(picker).toHaveValue("/");
  await expect(picker.locator("option")).toHaveText([/\(latest\)/]);
});

test("search reads the index the build wrote", async ({ page }) => {
  await page.goto("/docs/");
  await page
    .getByRole("button", { name: /search/i })
    .first()
    .click();
  await page.getByPlaceholder("Search").fill("window functions");
  // A result that could only come from the static index, not from the page already open. The
  // index is named explicitly in components/search.tsx: the client's own default is a route a
  // static export never emits, and searching then finds nothing at all.
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText("Window functions").first()).toBeVisible({ timeout: 15_000 });
  await expect(dialog.getByText("Reading data").first()).toBeVisible();
});

test("the embedded console scrolls under the site header", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("button", { name: "Run", exact: true })).toBeVisible({
    timeout: 120_000,
  });
  await page.evaluate(() => {
    window.scrollTo(0, 400);
  });

  // An inline panel is part of the document, not an overlay: whatever the devtools stack inside
  // themselves, the page's own header stays on top of them.
  const headerOnTop = await page.evaluate(() => {
    const nav = document.querySelector("#nd-nav");
    if (nav === null) return false;
    const box = nav.getBoundingClientRect();
    const painted = document.elementFromPoint(box.left + box.width / 2, box.top + box.height / 2);
    return painted !== null && nav.contains(painted);
  });
  expect(headerOnTop).toBe(true);
});

test("the devtools page opens the floating panel over itself", async ({ page }) => {
  await page.goto("/docs/devtools/");
  await page.getByRole("button", { name: "Open the floating panel" }).click();

  // The panel lives in a shadow root, which Playwright's selector engine sees through.
  const panel = page.getByRole("region", { name: "Minnow devtools" });
  await expect(panel).toBeVisible({ timeout: 120_000 });
  // This one runs in the page rather than a worker, which is what the badge reports.
  await expect(panel.getByText("main thread")).toBeVisible();

  await page.getByRole("button", { name: "Run", exact: true }).click();
  // The status bar and the history entry both report it; the first is the status bar.
  await expect(panel.getByText(/\d+ rows · \d+ms/).first()).toBeVisible({ timeout: 60_000 });

  // Closing leaves the launcher behind, the way it does in an application.
  await page.getByRole("button", { name: "Close devtools" }).click();
  await expect(panel).toBeHidden();

  // Removing it takes the panel and the database behind it away with it.
  await page.getByRole("button", { name: "Remove it" }).click();
  await expect(panel).toHaveCount(0);
});

test("a path that misses lands in the pond", async ({ page }) => {
  const response = await page.goto("/pond/where-did-it-go/");
  expect(response?.status()).toBe(404);
  await expect(page.getByRole("heading", { name: "This one got away" })).toBeVisible();
  // One 404.html serves every path that misses, so the path is read in the browser rather than
  // rendered into the page.
  await expect(page.getByText("'/pond/where-did-it-go/'")).toBeVisible();
  // And the pond is really the pond, canvas and all.
  await expect(page.locator("canvas")).toBeVisible();
});

test("a link to a file the site serves opens the file", async ({ page }) => {
  await page.goto("/docs/reference/agents/");
  await page.getByRole("link", { name: "/llms.txt", exact: true }).first().click();

  // Handed to the router instead, this resolves as an app route: the extension is dropped and
  // the reader lands on the 404 for a URL they never clicked.
  await expect(page).toHaveURL(/\/llms\.txt$/);
  await expect(page.locator("body")).toContainText("# Minnow");

  // A link to a page still navigates on the client.
  await page.goBack();
  await page.getByRole("link", { name: "Feature matrix" }).first().click();
  await expect(page).toHaveURL(/\/docs\/sql\/feature-matrix\/$/);
});
