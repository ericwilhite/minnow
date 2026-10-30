import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

/**
 * A newer build of the app opening the database in a second tab, in a real browser: the older
 * tab writes the rows its buffered writer accepted, lets go of the database, and reloads itself
 * — no prompt, no stuck second tab. The newer build is stood in for by what it does to the
 * store: an OPFS upgrade request on the store's channel, or an IndexedDB open one version up.
 */
for (const kind of ["opfs", "indexeddb"] as const) {
  test(`an older tab steps aside for a newer build and reloads (${kind})`, async ({
    page,
    context,
  }) => {
    await page.goto("/packages/core/browser/");
    if (kind === "opfs") await requireWorkerOpfs(page);
    const name = `supersede-${kind}-${String(Date.now())}`;
    const url = "/packages/core/browser/supersede-run.ts";
    type Run = typeof import("../browser/supersede-run.js");
    await page.evaluate(
      async ({ url, kind, name }) => ((await import(url)) as Run).openOlderTab(kind, name),
      { url, kind, name },
    );

    const reloaded = page.waitForEvent("load", { timeout: 20_000 });
    const newer = await context.newPage();
    await newer.goto("/packages/core/browser/");
    const result = await newer.evaluate(
      async ({ url, kind, name }) => ((await import(url)) as Run).actAsNewerBuild(kind, name),
      { url, kind, name },
    );
    await reloaded;
    expect(result).toEqual({ upgraded: true });

    const after = await page.evaluate(
      async ({ url, kind, name }) => ((await import(url)) as Run).afterReload(kind, name),
      { url, kind, name },
    );
    expect(after.event).toBe(JSON.stringify({ reloading: true }));
    expect(after.guarded).toBe(true);
    if (kind === "opfs") expect(after.rows).toBe(25);
    await newer.close();
  });
}
