import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";
for (const kind of ["indexeddb", "opfs"] as const) {
  test(`${kind}: generated semantic values, domains and refusals agree across native RPC`, async ({
    page,
  }) => {
    await page.goto("/packages/core/browser/");
    if (kind === "opfs") await requireWorkerOpfs(page);
    const result = await page.evaluate(async (kind) => {
      const url = "/packages/core/browser/semantic-run.ts";
      const module = (await import(url)) as typeof import("../browser/semantic-run.js");
      return module.runSemanticEquivalence(kind);
    }, kind);
    expect(result.compared).toBe(292);
    expect(result.failures).toBe(0);
  });
}
