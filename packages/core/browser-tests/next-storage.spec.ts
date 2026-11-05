/**
 * The rebuild core's storage (packages/core/src/next/storage) in real browsers, over real OPFS sync
 * access handles in dedicated workers, on a persistent profile on disk (fixtures.ts):
 * store conformance, kill and tab close during publish, the handle fence across workers, the spill
 * scratch file, and (opt-in, MINNOW_NEXT_STORAGE_BENCH=1) the page format's builder pattern.
 */
import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

type Run = typeof import("../browser/next-storage-run.js");
const RUN = "/packages/core/browser/next-storage-run.ts";

test.describe("rebuild core storage on OPFS", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/packages/core/browser/");
    await requireWorkerOpfs(page);
  });

  test("passes the store conformance cases in a dedicated worker", async ({ page }) => {
    const results = await page.evaluate(async (url) => {
      return ((await import(url)) as Run).runConformance();
    }, RUN);
    expect(results.length).toBeGreaterThan(5);
    expect(results.filter((result) => result.error !== null)).toEqual([]);
  });

  test("survives a worker killed in every phase of publishing, again and again", async ({
    page,
  }, info) => {
    test.setTimeout(180_000);
    const result = await page.evaluate(async (url) => {
      return ((await import(url)) as Run).runKillDuringPublish(12, 0x6b11);
    }, RUN);
    await info.attach("kill-during-publish", {
      body: JSON.stringify(result, null, 2),
      contentType: "application/json",
    });
    expect(result.iterations).toHaveLength(12);
    // Every phase of a round was the moment of a kill, several times (review B1-p2b finding 5).
    for (const phase of ["writing", "publishing", "published"] as const)
      expect(result.iterations.filter((iteration) => iteration.target === phase)).toHaveLength(4);
    for (const iteration of result.iterations) {
      expect(iteration.killedRound).toBeGreaterThan(1);
      expect(iteration.round).toBeGreaterThanOrEqual(iteration.acked);
      if (iteration.target === "published")
        expect(iteration.round).toBeGreaterThanOrEqual(iteration.killedRound);
      if (iteration.round > 1) expect(iteration.previous).toBe(iteration.round - 1);
    }
  });

  test("survives the owner's tab closing during publish", async ({ context, page }) => {
    test.setTimeout(120_000);
    for (let attempt = 0; attempt < 3; attempt++) {
      const owner = attempt === 0 ? page : await context.newPage();
      if (attempt > 0) await owner.goto("/packages/core/browser/");
      const dir = await owner.evaluate(async (url) => {
        return ((await import(url)) as Run).startPublishLoop();
      }, RUN);
      await owner.close({ runBeforeUnload: false });
      const checker = await context.newPage();
      await checker.goto("/packages/core/browser/");
      const verified = await checker.evaluate(
        async ([url, directory]) => {
          return ((await import(url)) as Run).verifyDirectory(directory);
        },
        [RUN, dir] as const,
      );
      expect(verified.round).toBeGreaterThanOrEqual(3);
      expect(verified.previous).toBe(verified.round - 1);
      await checker.close();
    }
  });

  test("fences a second owner across workers until it steals; the loser's writes are refused", async ({
    page,
  }) => {
    const result = await page.evaluate(async (url) => {
      return ((await import(url)) as Run).runSteal();
    }, RUN);
    expect(result.waited).toMatchObject({ ok: false, error: "OwnerUnresponsiveError" });
    expect(result.stolen.ok).toBe(true);
    expect(result.loser).toEqual({ lost: true, error: "OwnershipLostError" });
  });

  test("spills to the OPFS scratch file and sweeps it", async ({ page }) => {
    const result = await page.evaluate(async (url) => {
      return ((await import(url)) as Run).runSpill();
    }, RUN);
    expect(result.pages).toBe(64);
    expect(result.inUse).toBeGreaterThan(64 * 30_000 - 1);
    expect(result.after).toBe(0);
  });

  test("builder pattern: many columns page by page, shared slab vs own buffers vs one by one (bench)", async ({
    page,
  }, info) => {
    test.skip(process.env.MINNOW_NEXT_STORAGE_BENCH !== "1", "opt-in bench");
    test.setTimeout(300_000);
    const result = await page.evaluate(async (url) => {
      const run = (await import(url)) as Run;
      const samples = [];
      for (let repeat = 0; repeat < 3; repeat++)
        for (const [alternating, buffered] of [
          [true, false],
          [true, true],
          [false, false],
        ] as const)
          samples.push({
            alternating,
            buffered,
            ...(await run.runBuilderBench(alternating, buffered, 200, 8, 32_768)),
          });
      return samples;
    }, RUN);
    await info.attach("builder-bench", {
      body: JSON.stringify(result, null, 2),
      contentType: "application/json",
    });
    console.log(JSON.stringify(result));
  });
});
