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

test("the console reopens an existing database instead of rebuilding it", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator("[data-minnow-devtools] .statusbar").first()).toBeVisible({
    timeout: 120_000,
  });

  // The inline panel fills the box the page gives it rather than standing at a height of its own.
  // The box is the host element itself: `mountMinnowDevtools` adopts the container it is handed.
  expect(
    await page.evaluate(() => {
      const host = document.querySelector("[data-minnow-devtools]");
      const inline = host?.shadowRoot?.querySelector(".panel");
      if (host === null || inline === null || inline === undefined) return null;
      return [host.getBoundingClientRect().height, inline.getBoundingClientRect().height];
    }),
  ).toEqual([620, 620]);

  await page.reload();
  await expect(page.getByText("this database was already on your machine")).toBeVisible({
    timeout: 60_000,
  });
});

test("the TypeScript tab checks a snippet against the schema and runs it", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("button", { name: "Run", exact: true })).toBeVisible({
    timeout: 120_000,
  });

  // Nothing of the editor is fetched until the tab is picked, which is the whole reason it is a
  // tab rather than a second panel.
  await page.getByRole("tab", { name: "TypeScript" }).click();
  const console_ = page.locator('[data-minnow-console="typescript"]');
  await expect(console_.locator(".monaco-editor").first()).toBeVisible({ timeout: 120_000 });

  // The seeded snippet is a grouped aggregate through the builder. Rows come back as a table.
  await console_.getByRole("button", { name: "Run", exact: true }).click();
  await expect(console_.locator("table")).toBeVisible({ timeout: 60_000 });
  await expect(console_.getByRole("columnheader", { name: "revenue" })).toBeVisible();

  // And the type checking is real: a column the schema does not have never reaches the database.
  // Clicking the rendered lines rather than the editor's own hidden textarea, and inserting the
  // text rather than typing it: typed keystrokes would meet the editor's auto-closing brackets.
  await console_.locator(".monaco-editor .view-lines").first().click();
  await page.keyboard.press("ControlOrMeta+a");
  await page.keyboard.insertText(
    'const rows = await db.selectFrom("orders").select(["nmae"]).execute();',
  );
  await console_.getByRole("button", { name: "Run", exact: true }).click();
  await expect(console_.getByText("the compiler refused it")).toBeVisible({ timeout: 60_000 });
  await expect(console_.getByText(/nmae/)).toBeVisible();
});

/**
 * The editor's own IntelliSense, which is a separate thing from the type checking above: Run
 * compiles through the worker directly, so it passes perfectly well while the editor has no
 * completion, hover, or squiggles registered at all. That is not hypothetical — it is what
 * shipped the first time. Everything here is what a reader actually sees.
 */
test("the TypeScript editor completes and hovers from the playground's schema", async ({
  page,
}) => {
  await page.goto("/");
  await expect(page.getByRole("button", { name: "Run", exact: true })).toBeVisible({
    timeout: 120_000,
  });
  await page.getByRole("tab", { name: "TypeScript" }).click();
  const console_ = page.locator('[data-minnow-console="typescript"]');
  await expect(console_.locator(".monaco-editor").first()).toBeVisible({ timeout: 120_000 });

  const suggestions = page.locator(".suggest-widget");
  const type = async (text: string): Promise<void> => {
    await page.keyboard.press("Escape");
    await console_.locator(".monaco-editor .view-lines").first().click();
    await page.keyboard.press("ControlOrMeta+a");
    // Inserted rather than typed, so the editor's auto-closing brackets leave it alone; the one
    // character that has to be genuinely typed is the quote, below.
    await page.keyboard.insertText(text);
  };

  // Nothing is pressed to ask for any of this. A reader who does not know the API or the schema
  // is the reader this tab is for, so the list has to arrive on its own.
  await type("db");
  await page.keyboard.type(".");
  await expect(suggestions).toBeVisible({ timeout: 60_000 });
  await expect(suggestions).toContainText("selectFrom");

  // Opening a quote offers the tables. The language service's only trigger character is `.`, so
  // without the console's own handler this is where a reader is left guessing.
  await type("db.selectFrom()");
  await page.keyboard.press("ArrowLeft");
  await page.keyboard.type('"');
  await expect(suggestions).toBeVisible({ timeout: 60_000 });
  // Every table, and only tables: the list is this schema's, not a word list from the buffer.
  for (const table of ["customers", "order_items", "orders", "products", "returns", "stores"]) {
    await expect(suggestions).toContainText(table);
  }

  // And one table deeper, the columns of that table. The list is virtualized, so these two are
  // named because they are inside the first screen of it, not because they are the only ones.
  await type('db.selectFrom("orders").select([])');
  await page.keyboard.press("ArrowLeft");
  await page.keyboard.press("ArrowLeft");
  await page.keyboard.type('"');
  await expect(suggestions).toBeVisible({ timeout: 60_000 });
  await expect(suggestions).toContainText("discount");
  await expect(suggestions).toContainText("item_count");

  // The list is taller than the room above the line that summoned it, and the console is a short
  // box with `overflow: hidden`. It stays inside that box in the DOM and escapes it by being
  // positioned against the viewport instead — so both halves are checked: the positioning that
  // does the escaping, and that the top of the list is really painted where it claims to be.
  expect(
    await page.evaluate(() => {
      const widget = document.querySelector(".suggest-widget");
      if (widget === null) return "no list";
      if (getComputedStyle(widget).position !== "fixed") return "clipped by the console";
      const box = widget.getBoundingClientRect();
      if (box.height < 100) return "list too short to judge";
      const painted = document.elementFromPoint(box.left + 20, box.top + 8);
      return painted !== null && widget.contains(painted) ? "painted" : "covered";
    }),
  ).toBe("painted");

  // The selected row has to be readable. Overriding its background without its foreground leaves
  // the base theme's white text on a near-white row, which is what this number catches.
  expect(
    await page.evaluate(() => {
      const row = document.querySelector(".suggest-widget .monaco-list-row.focused");
      if (row === null) return 0;
      const style = getComputedStyle(row);
      const luminance = (color: string): number => {
        const [r = 0, g = 0, b = 0] = (/(\d+), (\d+), (\d+)/.exec(color) ?? [])
          .slice(1)
          .map((part) => {
            const channel = Number(part) / 255;
            return channel <= 0.03928 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4;
          });
        return 0.2126 * r + 0.7152 * g + 0.0722 * b;
      };
      const [dark, light] = [luminance(style.color), luminance(style.backgroundColor)].sort(
        (a, b) => a - b,
      ) as [number, number];
      return (light + 0.05) / (dark + 0.05);
    }),
  ).toBeGreaterThan(4.5);

  // And hovering reports the type the select list built — three keys, not the table's fourteen
  // columns, which is exactly what the seeded snippet's comment tells the reader to look for.
  await page.keyboard.press("Escape");
  await console_.getByRole("button", { name: "Revenue by month" }).click();
  await console_.locator(".view-line span", { hasText: "rows" }).last().hover();
  const hover = page.locator(".hover-contents").first();
  await expect(hover).toContainText("const rows", { timeout: 60_000 });
  await expect(hover).toContainText("revenue: number");
});

/**
 * A bare `d` toggles light and dark. Fumadocs decides the reader was not typing by looking at the
 * tag name of the event's target, and neither console presents one: CodeMirror sits in a shadow
 * root, so the event retargets to the plain host `<div>`, and Monaco writes through an
 * `EditContext` `<div>` that is not `contentEditable`. Every `d` in `discount`, `id`, or
 * `product_id` flipped the whole page mid-word.
 */
test("typing in either console does not trip the site's theme shortcut", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("button", { name: "Run", exact: true })).toBeVisible({
    timeout: 120_000,
  });
  const theme = (): Promise<string> => page.evaluate(() => document.documentElement.className);
  const before = await theme();

  await page.locator("[data-minnow-devtools] .cm-content").first().click();
  await page.keyboard.type("select discount from orders");
  expect(await theme()).toBe(before);

  await page.getByRole("tab", { name: "TypeScript" }).click();
  const console_ = page.locator('[data-minnow-console="typescript"]');
  await expect(console_.locator(".monaco-editor").first()).toBeVisible({ timeout: 120_000 });
  await console_.locator(".monaco-editor .view-lines").first().click();
  await page.keyboard.press("ControlOrMeta+a");
  await page.keyboard.type("const discount = 1;");
  expect(await theme()).toBe(before);

  // And the shortcut still does its job everywhere it was meant to.
  await page.getByRole("heading", { level: 1 }).click();
  await page.keyboard.press("d");
  await expect.poll(theme).not.toBe(before);
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
