// eslint-disable-next-line no-restricted-imports -- Node-only Playwright evidence writer; never shipped.
import { writeFile } from "node:fs/promises";
import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

// Run alone with `npm run test:browser:pos`; time thresholds are regression guards for a
// controlled machine, not product SLAs. Correctness assertions apply to every measured sale.
const enabled = process.env.MINNOW_POS_ACCEPTANCE === "1";
const count = Number(process.env.MINNOW_POS_TRANSACTIONS ?? 1000);
const writeP99Budget = Number(process.env.MINNOW_POS_WRITE_P99_MS ?? 1000);
const readP99Budget = Number(process.env.MINNOW_POS_READ_P99_MS ?? 250);

for (const kind of ["indexeddb", "opfs"] as const) {
  test(`strict POS ${kind}: sustained checkouts and reads during maintenance`, async ({
    storageContext,
  }, info) => {
    test.skip(!enabled, "Opt-in sustained latency acceptance run");
    test.setTimeout(600_000);
    expect(Number.isFinite(writeP99Budget) && writeP99Budget > 0).toBe(true);
    expect(Number.isFinite(readP99Budget) && readP99Budget > 0).toBe(true);
    const page = await storageContext.newPage();
    page.on("console", (message) => {
      if (message.text().startsWith("POS_PROGRESS"))
        console.log(`${info.project.name}: ${message.text()}`);
    });
    await page.goto("/packages/core/browser/");
    if (kind === "opfs") await requireWorkerOpfs(page);
    const result = await page
      .evaluate(
        async ({ kind, count }) => {
          const url = "/packages/core/browser/pos.ts";
          return ((await import(url)) as typeof import("../browser/pos.js")).run(
            kind,
            count,
            (completed) =>
              console.log(`POS_PROGRESS ${kind} ${String(completed)}/${String(count)}`),
          );
        },
        { kind, count },
      )
      .catch(async (error: unknown) => {
        if (kind === "opfs") {
          const evidence = await page.evaluate(async () => {
            const url = "/packages/core/browser/pos.ts";
            return ((await import(url)) as typeof import("../browser/pos.js")).captureOpfsFiles();
          });
          const path = info.outputPath("opfs-recovery-evidence.json");
          await writeFile(path, JSON.stringify(evidence));
          await info.attach("opfs-recovery-evidence", { path, contentType: "application/json" });
        }
        throw error;
      });
    await info.attach("pos-results", {
      body: JSON.stringify(result, null, 2),
      contentType: "application/json",
    });
    console.log(JSON.stringify({ browser: info.project.name, ...result }));
    expect(result.totals).toEqual({
      sales: [{ n: count, total: count * 1000 }],
      lines: [{ n: count, qty: count }],
      stock: [{ qty: 10_000_000 - count }],
    });
    expect(result.maintenance.lastError).toBeNull();
    expect(result.writes.p99).toBeLessThan(writeP99Budget);
    expect(result.reads.p99).toBeLessThan(readP99Budget);
  });
}
