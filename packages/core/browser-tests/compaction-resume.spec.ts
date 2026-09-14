import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

for (const store of ["indexeddb", "opfs"] as const) {
  test(`${store}: compaction reconciles publication during transaction resume`, async ({
    page,
  }) => {
    await page.goto("/packages/core/browser/");
    if (store === "opfs") await requireWorkerOpfs(page);
    const result = await page.evaluate(async (kind) => {
      const url = "/packages/core/browser/compaction-resume-run.ts";
      return (
        (await import(url)) as typeof import("../browser/compaction-resume-run.js")
      ).runCompactionResume(kind);
    }, store);
    expect(result.published).toMatchObject({
      state: "published",
      result: { compacted: true, rowCount: 4 },
    });
    expect(result.resumed).toEqual(result.published);
    expect(result.rows).toEqual([{ value: 1 }, { value: 2 }, { value: 3 }, { value: 4 }]);
    expect(result.integrity).toBe(true);
  });
}
