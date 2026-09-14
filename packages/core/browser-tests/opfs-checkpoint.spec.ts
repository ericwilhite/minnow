import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

for (const mode of ["renew", "append", "mirror"] as const) {
  test(`native OPFS recovers an interrupted ${mode} checkpoint`, async ({ page }) => {
    await page.goto("/packages/core/browser/");
    await requireWorkerOpfs(page);
    const result = await page.evaluate(async (mode) => {
      const url = "/packages/core/browser/opfs-checkpoint-run.ts";
      return (
        (await import(url)) as typeof import("../browser/opfs-checkpoint-run.js")
      ).runCheckpointRecovery(mode);
    }, mode);
    expect(result.tables).toEqual(["durable"]);
    expect(result.integrity).toBe(true);
    expect(result.checkpointPair).toHaveLength(2);
    const [first, second] = result.checkpointPair;
    if (first === undefined || second === undefined)
      throw new Error("Expected two checkpoint mirrors");
    expect(first.lastSeq).toBe(second.lastSeq);
    if (mode === "mirror") {
      expect(Math.abs(first.generation - second.generation)).toBe(1);
    } else {
      expect(result.rowIds).toEqual([["3"], mode === "append" ? ["7"] : []]);
    }
  });
}
