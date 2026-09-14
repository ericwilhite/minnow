import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

for (const mode of ["prefix", "replay"] as const) {
  test(`native OPFS preserves manifest chains during ${mode} cleanup`, async ({ page }) => {
    await page.goto("/packages/core/browser/");
    await requireWorkerOpfs(page);
    const result = await page.evaluate(async (mode) => {
      const url = "/packages/core/browser/manifest-prefix-run.ts";
      return (
        (await import(url)) as typeof import("../browser/manifest-prefix-run.js")
      ).runManifestPrefix(mode);
    }, mode);
    expect(result.prepared.firstRemoval).toBe(0);
    expect(result.reopened.versions).toEqual(result.prepared.versions);
    expect(result.prepared.secondRemoval).toBe(2);
    expect(result.prepared.versions).toEqual([2, 3, 4, 5, 6, 7]);
    expect(result.reopened.current).toBe(7);
    expect(result.reopened.integrity).toBe(true);
    if (mode === "prefix") expect(result.prepared.walBytes).toBe(0);
    else expect(result.prepared.walBytes).toBeGreaterThan(0);
  });
}
