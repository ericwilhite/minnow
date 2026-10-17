import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

/**
 * A measurement, gated behind MINNOW_OLTP_BENCH=1 so the ordinary run never depends on wall
 * time: single-row read and write latency over real IndexedDB (engine on the page and through
 * the published worker) and OPFS (worker). The only assertion is that every configuration ends
 * with the same rows; the timings are printed. They are not published benchmark numbers.
 */
test.describe("single-row latency", () => {
  test.skip(process.env.MINNOW_OLTP_BENCH !== "1", "Set MINNOW_OLTP_BENCH=1 to measure");
  test("indexeddb and opfs", async ({ page }, info) => {
    test.setTimeout(600_000);
    await page.goto("/packages/core/browser/");
    const url = "/packages/core/browser/oltp-latency-run.ts";
    type Run = typeof import("../browser/oltp-latency-run.js");
    const raw = await page.evaluate(
      async (path) => ((await import(path)) as Run).measureRawIndexedDb(),
      url,
    );
    console.log(`OLTP_RAW ${info.project.name} ${JSON.stringify(raw)}`);
    if (process.env.MINNOW_OLTP_RAW_ONLY === "1") return;
    const direct = await page.evaluate(
      async (path) => ((await import(path)) as Run).measureDirectIndexedDb(),
      url,
    );
    const worker = await page.evaluate(
      async (path) => ((await import(path)) as Run).measureWorker("indexeddb"),
      url,
    );
    const results: Record<string, unknown> = { direct, worker };
    expect(worker.checksum).toBe(direct.checksum);
    if (info.project.name !== "firefox") {
      await requireWorkerOpfs(page);
      const opfs = await page.evaluate(
        async (path) => ((await import(path)) as Run).measureWorker("opfs"),
        url,
      );
      expect(opfs.checksum).toBe(direct.checksum);
      results.opfs = opfs;
    }
    console.log(`OLTP_BENCH ${info.project.name} ${JSON.stringify(results)}`);
  });
});
