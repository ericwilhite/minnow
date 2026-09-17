import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

/**
 * A measurement, gated behind MINNOW_WRITE_BENCH=1 so the ordinary run never depends on wall
 * time: the fw-ui sync shape at scope concurrency 1, 6, and 12 over real IndexedDB and OPFS.
 * Every level persists identical rows; the only assertion is that equality, and the timings
 * are printed for the handoff. They are not published benchmark numbers.
 */
test.describe("write scope throughput", () => {
  test.skip(process.env.MINNOW_WRITE_BENCH !== "1", "Set MINNOW_WRITE_BENCH=1 to measure");
  for (const store of ["indexeddb", "opfs"] as const) {
    test(`${store}: fw-ui shape at concurrency 1, 6 and 12`, async ({ page }, info) => {
      test.setTimeout(600_000);
      await page.goto("/packages/core/browser/");
      if (store === "opfs") await requireWorkerOpfs(page);
      const tables = 45;
      const rowsPerTable = 867;
      const results = [];
      for (const concurrency of [1, 6, 12]) {
        const result = await page.evaluate(
          async (options) => {
            const url = "/packages/core/browser/write-scope-admission-run.ts";
            return (
              (await import(url)) as typeof import("../browser/write-scope-admission-run.js")
            ).benchmarkBurst(options);
          },
          { kind: store, concurrency, tables, rowsPerTable },
        );
        expect(result.conflicts).toBe(0);
        expect(result.callbacks).toBe(tables);
        expect(result.rows).toBe(tables * rowsPerTable);
        results.push({ concurrency, ...result });
      }
      const summary = { browser: info.project.name, store, tables, rowsPerTable, results };
      console.log(`WRITE_BENCH ${JSON.stringify(summary)}`);
      await info.attach("write-bench", {
        body: Buffer.from(JSON.stringify(summary, null, 2)),
        contentType: "application/json",
      });
    });
  }
});
