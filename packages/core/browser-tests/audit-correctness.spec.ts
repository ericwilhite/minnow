import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";
for (const kind of ["indexeddb", "opfs"] as const) {
  test(`${kind}: audit regressions survive the real worker and reopening`, async ({ page }) => {
    await page.goto("/packages/core/browser/");
    if (kind === "opfs") await requireWorkerOpfs(page);
    const result = await page.evaluate(async (kind) => {
      const url = "/packages/core/browser/audit-run.ts";
      const module = (await import(url)) as typeof import("../browser/audit-run.js");
      return module.runAuditCorrectness(kind);
    }, kind);
    expect(result.rows).toEqual([
      {
        id: 1,
        extracted: "9007199254740993.0001",
        quotient: "9007199254740993",
        formatted: "9007199254740993.0001",
        s: "1",
      },
      { id: 2, extracted: "9007199254740992.0001", quotient: "-1", formatted: "-1.2500", s: "2" },
      {
        included: false,
        excluded: true,
        longest: "ab",
        classes: true,
        positional: "bc",
        identifier: '"select"',
        year: "0001-01-01",
      },
      { intact: 2 },
    ]);
    expect(result.arrayValue).toBe('["9007199254740993"]');
    expect(result.regexBounded).toBe(true);
    expect(result.refusals).toBe(5);
    expect(result.diagnostics).toEqual([]);
  });
}
test("opfs: zeroed acknowledged WAL is corruption across a real worker crash", async ({ page }) => {
  await page.goto("/packages/core/browser/");
  await requireWorkerOpfs(page);
  const error = await page.evaluate(async () => {
    const url = "/packages/core/browser/audit-run.ts";
    const module = (await import(url)) as typeof import("../browser/audit-run.js");
    return module.runAcknowledgedWalDamage();
  });
  expect(error).toBe("StorageCorruptionError");
});

for (const kind of ["indexeddb", "opfs"] as const) {
  test(`${kind}: prototype-named columns survive native RPC, rollback, compaction and reopening`, async ({
    page,
  }) => {
    await page.goto("/packages/core/browser/");
    if (kind === "opfs") await requireWorkerOpfs(page);
    const result = await page.evaluate(async (kind) => {
      const url = "/packages/core/browser/audit-run.ts";
      const module = (await import(url)) as typeof import("../browser/audit-run.js");
      return module.runAuditNamedColumns(kind);
    }, kind);
    expect(result).toEqual({
      values: [
        [1, 10, 20],
        [2, 19, 38],
      ],
      ownProperties: true,
      plainRows: true,
      checkRefused: true,
      diagnostics: [],
    });
  });
}
