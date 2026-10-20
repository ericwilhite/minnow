import { expect } from "@playwright/test";
import { test, requireWorkerOpfs } from "./fixtures.js";

for (const kind of ["indexeddb", "opfs"] as const) {
  test(`offline identities migrate, merge, notify, batch and reopen over ${kind}`, async ({
    page,
  }) => {
    await page.goto("/packages/core/browser/");
    if (kind === "opfs") await requireWorkerOpfs(page);
    const result = await page.evaluate(async (store) => {
      const url = "/packages/core/browser/offline-identity-run.ts";
      const runner = (await import(url)) as typeof import("../browser/offline-identity-run.js");
      return runner.runOfflineIdentity(store);
    }, kind);
    expect(result.changes).toEqual([
      { table: "orders", from: -1, to: 900 },
      { table: "items", from: -7, to: 500 },
    ]);
    expect(result.mergeOrder).toEqual(["keys", "rows"]);
    expect(result.resolved).toEqual([900, 900, 900, 123]);
    expect(result.remainingSteps).toBe(0);
    expect(result.orders).toEqual([{ id: 900, customer: 1, note: "server" }]);
    expect(result.items).toEqual([{ id: 500, orderId: 900 }]);
    expect(result.reopenedKeys).toEqual([900, 900]);
  });
}
