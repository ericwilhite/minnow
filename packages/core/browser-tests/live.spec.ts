import { requireWorkerOpfs } from "./fixtures.js";
import { chromium, firefox, webkit, test as base, type BrowserContext } from "@playwright/test";

const test = base.extend<{ context: BrowserContext }>({
  context: async ({ browserName }, use, info) => {
    const launcher =
      browserName === "webkit" ? webkit : browserName === "firefox" ? firefox : chromium;
    const context = await launcher.launchPersistentContext(info.outputPath("profile"), {
      baseURL: info.project.use.baseURL ?? "",
    });
    await use(context);
    await context.close();
  },
});

for (const store of ["indexeddb", "opfs"] as const) {
  test(`maintains exact live patches through a real ${store} worker`, async ({ page }) => {
    const errors: string[] = [];
    page.on("pageerror", (error) => errors.push(error.message));
    await page.goto("/packages/core/browser/live/");
    await page.locator("#ready").filter({ hasText: "Live-query benchmark ready" }).waitFor();
    if (store === "opfs") await requireWorkerOpfs(page);
    const result = await page.evaluate(async (storeKind) => {
      const target = window as typeof window & {
        runLiveCorrectness(
          kind: "indexeddb" | "opfs",
        ): Promise<{ checked: number; patchRows: number[] }>;
      };
      return target.runLiveCorrectness(storeKind);
    }, store);
    test.expect(result.checked).toBe(5);
    test.expect(result.patchRows.length).toBe(10);
    test.expect(result.patchRows.every((count) => count <= 1)).toBe(true);
    test.expect(errors).toEqual([]);
  });
}
