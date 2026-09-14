import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

for (const store of ["indexeddb", "opfs"] as const) {
  test(`${store}: every storage fault preserves whole statements and acknowledged writes`, async ({
    storageContext,
  }, info) => {
    test.setTimeout(180_000);
    const page = await storageContext.newPage();
    const errors: string[] = [];
    page.on("pageerror", (error) => errors.push(error.message));
    page.on("console", (message) => {
      if (message.type() === "error") errors.push(message.text());
    });
    await page.goto("/packages/core/browser/");
    if (store === "opfs") await requireWorkerOpfs(page);
    const result = await page.evaluate(
      (store) =>
        new Promise<{
          injections: number;
          counts: Record<string, number>;
          outcomes: number[];
        }>((resolve, reject) => {
          const worker = new Worker("/packages/core/browser/fault-sweep-worker.ts", {
            type: "module",
          });
          worker.onerror = (event) => {
            worker.terminate();
            reject(new Error(event.message));
          };
          worker.onmessage = (
            event: MessageEvent<{
              result: { injections: number; counts: Record<string, number>; outcomes: number[] };
              error?: string;
            }>,
          ) => {
            worker.terminate();
            if (event.data.error !== undefined) reject(new Error(event.data.error));
            else resolve(event.data.result);
          };
          worker.postMessage(store);
        }),
      store,
    );
    await info.attach(`${store}-fault-sweep.json`, {
      contentType: "application/json",
      body: JSON.stringify(result, undefined, 2),
    });
    expect(result.injections).toBe(Object.values(result.counts).reduce((a, b) => a + b, 0));
    expect(result.injections).toBeGreaterThan(20);
    expect(result.outcomes.length).toBeGreaterThan(1);
    expect(errors).toEqual([]);
  });
}
