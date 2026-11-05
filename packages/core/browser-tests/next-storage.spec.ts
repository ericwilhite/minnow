/**
 * The rebuild core's storage (packages/core/src/next/storage) in real browsers, over real OPFS sync
 * access handles in dedicated workers, on a persistent profile on disk (fixtures.ts):
 * store conformance, kill and tab close during publish, the handle fence across workers, the spill
 * scratch file, and (opt-in, MINNOW_NEXT_STORAGE_BENCH=1) the page format's builder pattern.
 * The same scenarios run on the IndexedDB backend, on the persistent profile and where OPFS is
 * absent (Firefox private browsing, a WebKit ephemeral session as in Safari private browsing).
 */
import { chromium, expect, firefox, webkit, type Page } from "@playwright/test";
import { EXPECTED_IDB_OUTCOMES } from "../src/next/storage/idb-scenarios.js";
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

/** Conformance, kills in every phase, the fence and spill, on the IndexedDB backend of `page`. */
async function indexedDbScenarios(page: Page, iterations: number) {
  return page.evaluate(
    async ([url, count]) => {
      const run = (await import(url)) as Run;
      run.useStore("indexeddb");
      return {
        conformance: await run.runConformance(),
        kill: await run.runKillDuringPublish(count, 0x1dbb),
        steal: await run.runSteal(),
        spill: await run.runSpill(),
      };
    },
    [RUN, iterations] as const,
  );
}

function expectIndexedDbScenarios(result: Awaited<ReturnType<typeof indexedDbScenarios>>) {
  expect(result.conformance.filter((entry) => entry.error !== null)).toEqual([]);
  for (const phase of ["writing", "publishing", "published"] as const)
    expect(result.kill.iterations.some((iteration) => iteration.target === phase)).toBe(true);
  for (const iteration of result.kill.iterations) {
    expect(iteration.round).toBeGreaterThanOrEqual(iteration.acked);
    if (iteration.round > 1) expect(iteration.previous).toBe(iteration.round - 1);
  }
  expect(result.steal.waited).toMatchObject({ ok: false, error: "OwnerUnresponsiveError" });
  expect(result.steal.stolen.ok).toBe(true);
  expect(result.steal.loser).toEqual({ lost: true, error: "OwnershipLostError" });
  expect(result.spill).toMatchObject({ pages: 64, after: 0 });
}

test.describe("rebuild core storage on IndexedDB", () => {
  test("persistent profile: conformance, kills in every phase, the epoch fence and spill", async ({
    page,
  }) => {
    test.setTimeout(240_000);
    await page.goto("/packages/core/browser/");
    expectIndexedDbScenarios(await indexedDbScenarios(page, 9));
  });

  test("where OPFS is absent, the owner gets IndexedDB and it works", async ({
    browserName,
  }, info) => {
    test.setTimeout(240_000);
    test.skip(browserName === "chromium", "Chromium incognito keeps OPFS (its own test)");
    // Firefox: real private browsing (OPFS is SecurityError). WebKit: an ephemeral session, Safari
    // private browsing's store (getDirectory is UnknownError).
    const browser =
      browserName === "firefox"
        ? await firefox.launch({ firefoxUserPrefs: { "browser.privatebrowsing.autostart": true } })
        : await webkit.launch();
    try {
      const context = await browser.newContext({ baseURL: info.project.use.baseURL ?? "" });
      const page = await context.newPage();
      await page.goto("/packages/core/browser/");
      const chosen = await page.evaluate(async (url) => {
        return ((await import(url)) as Run).runChoose();
      }, RUN);
      expect(chosen).toEqual({
        kind: "indexeddb",
        opfsRefused: browserName === "firefox" ? "SecurityError" : "UnknownError",
        error: null,
      });
      expectIndexedDbScenarios(await indexedDbScenarios(page, 6));
    } finally {
      await browser.close();
    }
  });

  test("the IndexedDB abstraction: the simulator answers exactly as the platform", async ({
    page,
  }) => {
    await page.goto("/packages/core/browser/");
    const result = await page.evaluate(async (url) => {
      return ((await import(url)) as Run).runIdbDifferential();
    }, RUN);
    expect(result.real).toEqual(EXPECTED_IDB_OUTCOMES);
    expect(result.simulated).toEqual(result.real);
  });

  test("refuses the pre-reset engine's data, in OPFS and IndexedDB", async ({ page }) => {
    test.setTimeout(120_000);
    await page.goto("/packages/core/browser/");
    await requireWorkerOpfs(page);
    for (const kind of ["opfs", "indexeddb"] as const) {
      const result = await page.evaluate(
        async ([url, store]) => {
          return ((await import(url)) as Run).runLegacyDetection(store);
        },
        [RUN, kind] as const,
      );
      expect(result.kind, kind).toBeNull();
      expect(result.error, kind).toMatchObject({ name: "LegacyDataError", store: kind });
    }
  });

  test("IndexedDB batching: bulk writes and small commits (bench)", async ({ page }, info) => {
    test.skip(process.env.MINNOW_NEXT_STORAGE_BENCH !== "1", "opt-in bench");
    test.setTimeout(300_000);
    await page.goto("/packages/core/browser/");
    const result = await page.evaluate(async (url) => {
      const run = (await import(url)) as Run;
      const samples = [];
      for (let repeat = 0; repeat < 3; repeat++)
        for (const batching of [true, false])
          samples.push({ batching, ...(await run.runIdbBench(batching)) });
      return samples;
    }, RUN);
    await info.attach("idb-bench", {
      body: JSON.stringify(result, null, 2),
      contentType: "application/json",
    });
    console.log(JSON.stringify(result));
  });

  test("Chromium incognito keeps OPFS", async ({ browserName }, info) => {
    test.skip(browserName !== "chromium", "Chromium only");
    const browser = await chromium.launch();
    try {
      const context = await browser.newContext({ baseURL: info.project.use.baseURL ?? "" });
      const page = await context.newPage();
      await page.goto("/packages/core/browser/");
      const chosen = await page.evaluate(async (url) => {
        return ((await import(url)) as Run).runChoose();
      }, RUN);
      expect(chosen).toEqual({ kind: "opfs", opfsRefused: null, error: null });
    } finally {
      await browser.close();
    }
  });
});

test.describe("rebuild core storage at scale (heavy, opt-in)", () => {
  test("multi-GB OPFS: fill, reopen, reclaim, then quota pressure and clean recovery", async ({
    page,
    browserName,
  }, info) => {
    test.skip(
      browserName !== "chromium" || process.env.MINNOW_NEXT_STORAGE_HEAVY !== "1",
      "opt-in, Chromium only (CDP quota override)",
    );
    test.setTimeout(3_600_000);
    const gib = Number(process.env.MINNOW_NEXT_STORAGE_GIB ?? "4");
    await page.goto("/packages/core/browser/");
    await requireWorkerOpfs(page);
    const dir = `heavy-${String(Date.now())}`;
    const step = (request: Record<string, unknown>) =>
      page.evaluate(async ([url, body]) => ((await import(url)) as Run).heavyStep(body as never), [
        RUN,
        request,
      ] as const) as Promise<Record<string, unknown>>;
    const restart = () =>
      page.evaluate(async (url) => {
        ((await import(url)) as Run).heavyRestart();
      }, RUN);
    const report: Record<string, unknown> = { gib };

    expect(await step({ op: "heavy-open", dir })).toMatchObject({ blobs: 0 });
    report.fill = await step({ op: "heavy-fill", mib: gib * 1024, publishEveryMib: 256 });
    expect(report.fill).toMatchObject({ blobs: gib * 1024 });

    // Reopen as a new process: the walk, 64 verified samples and an exact audit.
    await step({ op: "heavy-close" });
    await restart();
    report.reopen = await step({ op: "heavy-open", dir });
    expect(report.reopen).toMatchObject({ blobs: gib * 1024 });

    // Reclaim: half retired; as much again lands in the freed space, so the space does not grow.
    report.reclaim = await step({ op: "heavy-reclaim", refillMib: gib * 512 });
    // (A retired blob at the end shortens the space first; the refill may take that tail again.)
    const reclaim = report.reclaim as { filled: number; after: number };
    expect(reclaim.after).toBeLessThanOrEqual(reclaim.filled);

    // Quota pressure: 32 MiB of headroom. Writes fail with StoreQuotaError; the space stays up.
    const cdp = await page.context().newCDPSession(page);
    const origin = new URL(page.url()).origin;
    const { usage } = await page.evaluate(
      async (url) => ((await import(url)) as Run).storageEstimate(),
      RUN,
    );
    await cdp.send("Storage.overrideQuotaForOrigin", { origin, quotaSize: usage + 32 * 2 ** 20 });
    try {
      report.quotaWrite = await step({ op: "heavy-quota-write", mib: 256 });
      expect(report.quotaWrite).toEqual({ error: "StoreQuotaError", spaceFailed: null });
      // A root too large for what is left fails inside publish: the space fails (indeterminate).
      report.quotaPublish = await step({ op: "heavy-quota-publish", fillerMib: 64 });
      expect(report.quotaPublish).toMatchObject({
        error: "StoreQuotaError",
        spaceFailed: "StoreQuotaError",
      });
    } finally {
      await cdp.send("Storage.overrideQuotaForOrigin", { origin });
    }

    // Recovery: a new process opens to an intact root, audits exactly and keeps writing.
    await step({ op: "heavy-close" });
    await restart();
    report.recovered = await step({ op: "heavy-open", dir });
    report.after = await step({ op: "heavy-fill", mib: 64, publishEveryMib: 64 });
    await step({ op: "heavy-close" });
    await restart();
    await info.attach("multi-gb-opfs", {
      body: JSON.stringify(report, null, 2),
      contentType: "application/json",
    });
    console.log(`HEAVY ${JSON.stringify(report)}`);
    await page.evaluate(
      async ([url, directory]) => {
        const run = (await import(url)) as Run;
        await run.heavyStep({ op: "remove", dir: directory });
        run.heavyRestart();
      },
      [RUN, dir] as const,
    );
  });
});

test.describe("rebuild core storage microbench (recorded nightly)", () => {
  test("storage microbench: cache hits, cold slab, publish and commit latency", async ({
    page,
    browserName,
  }, info) => {
    test.skip(process.env.MINNOW_STORAGE_MICROBENCH !== "1", "opt-in: MINNOW_STORAGE_MICROBENCH=1");
    test.setTimeout(600_000);
    await page.goto("/packages/core/browser/");
    await requireWorkerOpfs(page);
    const metrics = await page.evaluate(async (url) => {
      return ((await import(url)) as Run).runMicrobench();
    }, RUN);
    const result = {
      recordedAt: new Date().toISOString(),
      browser: browserName,
      version: page.context().browser()?.version() ?? null,
      metrics,
    };
    expect(metrics.opfs?.coldSlabPlatformReads).toBe(1);
    await info.attach("storage-microbench", {
      body: JSON.stringify(result, null, 2),
      contentType: "application/json",
    });
    // One tagged line: the nightly workflow extracts it into the lane's results.
    console.log(`STORAGE_MICROBENCH ${JSON.stringify(result)}`);
  });
});
