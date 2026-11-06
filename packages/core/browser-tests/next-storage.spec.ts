/**
 * The rebuild core's storage (packages/core/src/next/storage) in real browsers, over real OPFS sync
 * access handles in dedicated workers, on a persistent profile on disk (fixtures.ts):
 * store conformance, kill and tab close during publish, the handle fence across workers, the spill
 * scratch file, and (opt-in, MINNOW_NEXT_STORAGE_BENCH=1) the page format's builder pattern.
 * The same scenarios run on the IndexedDB backend, on the persistent profile and where OPFS is
 * absent (Firefox private browsing, a WebKit ephemeral session as in Safari private browsing).
 * The kill campaign (Gate A, next-storage-chaos.ts) kills a worker running a random storage
 * workload at seeded moments and checks every reopen against what was acknowledged
 * (MINNOW_NEXT_STORAGE_KILLS=1000 for the nightly count).
 */
import { chromium, expect, firefox, webkit, type Page } from "@playwright/test";
import { EXPECTED_IDB_OUTCOMES } from "../src/next/storage/idb-scenarios.js";
import {
  createReach,
  STORAGE_CONCURRENT_KILL_REACH,
  STORAGE_KILL_EDGE_REACH,
  STORAGE_KILL_REACH,
} from "../src/testing/reach.js";
import { mulberry32, seedsFor } from "../src/testing/seeds.js";
import type { ChaosExpect, ChaosStep } from "../browser/next-storage-chaos.js";
import type { KillPlan } from "../browser/next-storage-run.js";
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

    // Quota pressure: 32 MiB of headroom by `estimate()`. Chromium refuses even the first 1 MiB
    // of growth under it (`writtenMib` records where the refusal came), so the space must survive
    // a refusal at once. Writers fail with StoreQuotaError; the space stays up.
    const cdp = await page.context().newCDPSession(page);
    const origin = new URL(page.url()).origin;
    const lowerQuota = async () => {
      const { usage } = await page.evaluate(
        async (url) => ((await import(url)) as Run).storageEstimate(),
        RUN,
      );
      await cdp.send("Storage.overrideQuotaForOrigin", { origin, quotaSize: usage + 32 * 2 ** 20 });
    };
    let lastRoot: { round: number; identity: string };
    await lowerQuota();
    try {
      // The writer fails; the same space then reads retained blobs and audits exactly.
      report.quotaWrite = await step({ op: "heavy-quota-write", mib: 256 });
      expect(report.quotaWrite).toMatchObject({
        error: "StoreQuotaError",
        spaceFailed: null,
        afterAudit: null,
      });
      // With room again, the same space writes and publishes 4 MiB.
      await cdp.send("Storage.overrideQuotaForOrigin", { origin });
      report.continued = await step({ op: "heavy-continue", mib: 4 });
      expect(report.continued).toMatchObject({ published: 1, spaceFailed: null });
      // A root too large for what is left fails inside publish: the space fails (indeterminate).
      await lowerQuota();
      report.quotaPublish = await step({ op: "heavy-quota-publish", fillerMib: 64 });
      expect(report.quotaPublish).toMatchObject({
        error: "StoreQuotaError",
        spaceFailed: "StoreQuotaError",
      });
      lastRoot = (report.quotaPublish as { lastRoot: typeof lastRoot }).lastRoot;
    } finally {
      await cdp.send("Storage.overrideQuotaForOrigin", { origin });
    }

    // Recovery: a new process opens to exactly the last successful root (round and every blob's
    // identity; no root at all is refused), audits exactly and keeps writing.
    await step({ op: "heavy-close" });
    await restart();
    report.recovered = await step({ op: "heavy-open", dir, expect: lastRoot });
    expect(report.recovered).toMatchObject(lastRoot);
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
    // One coalesced store read on both backends; on OPFS that is one file read.
    expect(metrics.opfs?.coldSlabStoreReads).toBe(1);
    expect(metrics.indexeddb?.coldSlabStoreReads).toBe(1);
    expect(metrics.opfs?.coldSlabFileReads).toBe(1);
    expect(metrics.indexeddb?.coldSlabIdbGets).toBeGreaterThan(0);
    await info.attach("storage-microbench", {
      body: JSON.stringify(result, null, 2),
      contentType: "application/json",
    });
    // One tagged line: the nightly workflow extracts it into the lane's results.
    console.log(`STORAGE_MICROBENCH ${JSON.stringify(result)}`);
  });
});

/** The steps a kill aims at (a reset is rare in the workload, so it is aimed at rarely). */
const KILL_TARGETS: readonly ChaosStep[] = [
  "write",
  "publish",
  "reclaim",
  "log-append",
  "log-flush",
  "truncate",
  "spill",
  "write",
  "publish",
  "reclaim",
  "log-flush",
  "truncate",
  "spill",
  "spill",
  "reset",
];

test.describe("rebuild core storage kill campaign (Gate A)", () => {
  const nightly = Number(process.env.MINNOW_NEXT_STORAGE_KILLS ?? "0");
  const kills = nightly > 0 ? nightly : 50;
  for (const kind of ["opfs", "indexeddb"] as const)
    for (const seed of seedsFor(`next-storage-kills-${kind}`, [kind === "opfs" ? 0x6b1a : 0x1db6]))
      test(`${kind}: the store opens after every kill, exactly as acknowledged (seed ${String(seed)})`, async ({
        page: first,
        context,
        browserName,
      }, info) => {
        test.skip(
          nightly > 0 && browserName !== "chromium",
          "the nightly campaign runs in Chromium",
        );
        test.setTimeout(Math.max(600_000, kills * 5_000));
        let page: Page = first;
        await page.goto("/packages/core/browser/");
        if (kind === "opfs") await requireWorkerOpfs(page);
        const random = mulberry32(seed);
        const dir = `kills-${kind}-${String(seed)}-${Date.now().toString(36)}`;
        // IndexedDB's work is asynchronous: kills must land inside each phase's storage work.
        // OPFS's is synchronous, so kills land at its edges (STORAGE_KILL_EDGE_REACH).
        const reach = createReach(
          `storage kills ${kind} ${browserName}`,
          kind === "indexeddb" ? STORAGE_KILL_REACH : STORAGE_KILL_EDGE_REACH,
          { minimum: Math.max(1, Math.floor(kills / 50)) },
        );
        const table = new Map<string, number>();
        const count = (key: string) => table.set(key, (table.get(key) ?? 0) + 1);
        const begin = (expected: ChaosExpect | null, iteration: number) =>
          page.evaluate(
            async ([url, store, directory, workerSeed, expectation]) =>
              ((await import(url)) as Run).chaosBegin(store, directory, workerSeed, expectation),
            [RUN, kind, dir, (seed + iteration * 0x9e3779b9) >>> 0, expected] as const,
          );
        const started = Date.now();
        let expected: ChaosExpect | null = null;
        let order: ChaosStep[] = [...KILL_TARGETS];
        let pageKills = 0;
        let workerStartRetries = 0;
        let verifiedLogBytes = 0;
        let lockSteals = 0;
        let previous = "none";
        for (let iteration = 0; iteration <= kills; iteration++) {
          let recovery;
          try {
            recovery = await begin(expected, iteration);
          } catch (error) {
            throw new Error(
              `${kind}/${browserName}: recovery after kill ${String(iteration)} failed (replay: MINNOW_SEED=${String(seed)}; ${previous}): ${String(error)}`,
              { cause: error },
            );
          }
          verifiedLogBytes += recovery.verifiedLogBytes;
          if (recovery.stolenLock) lockSteals += 1;
          if (iteration === kills) break;
          // Each pass over the targets is a seeded shuffle, so every phase is aimed at in every
          // pass (reach), in a seeded order.
          if (iteration % KILL_TARGETS.length === 0) {
            order = [...KILL_TARGETS];
            for (let at = order.length - 1; at > 0; at--) {
              const other = Math.floor(random() * (at + 1));
              [order[at], order[other]] = [order[other] ?? "write", order[at] ?? "write"];
            }
          }
          const target = order[iteration % order.length] ?? "write";
          const plan: KillPlan = {
            target,
            occurrence: 1 + Math.floor(random() * 3),
            delayMs: random() < 0.4 ? 0 : Math.floor(random() * 8),
            mode: random() < 0.1 ? "page" : "terminate",
          };
          let landing;
          try {
            landing = await page.evaluate(
              async ([url, killPlan]) => {
                const run = (await import(url)) as Run;
                const terminated = await run.chaosKill(killPlan);
                if (terminated !== null) return { ...terminated, retries: 0 };
                // A page kill: the step runs on; the boundary is taken just before the close.
                await new Promise<void>((resolve) => {
                  setTimeout(resolve, killPlan.delayMs);
                });
                return { ...run.chaosPageBoundary(), retries: run.chaosStartRetries() };
              },
              [RUN, plan] as const,
            );
          } catch (error) {
            throw new Error(
              `${kind}/${browserName}: kill ${String(iteration)} (${JSON.stringify(plan)}) observed a workload failure (replay: MINNOW_SEED=${String(seed)}; ${previous}): ${String(error)}`,
              { cause: error },
            );
          }
          expected = landing.expect;
          previous = `kill ${String(iteration)}: ${JSON.stringify(plan)} landed ${landing.landed}, expecting ${JSON.stringify(landing.expect)}`;
          if (plan.mode === "page") {
            workerStartRetries += landing.retries;
            // A page close (CDP's Page.crash takes the whole persistent context down under the
            // headless shell Playwright launches, so it is not used). The page processed no
            // message after its boundary.
            pageKills += 1;
            await page.close({ runBeforeUnload: false });
            count(`${landing.landed} (page close)`);
            page = await context.newPage();
            await page.goto("/packages/core/browser/");
          } else {
            reach.recordHistory([
              kind === "indexeddb"
                ? landing.landed
                : landing.landed.replace(/^(before|in|after)-/u, "at-"),
            ]);
            count(landing.landed);
          }
        }
        await page.evaluate(
          async ([url, store, directory]) =>
            ((await import(url)) as Run).chaosEnd(store, directory),
          [RUN, kind, dir] as const,
        );
        // Workers that failed to load their module (counted per page; none ran storage code).
        workerStartRetries += await page.evaluate(
          async (url) => ((await import(url)) as Run).chaosStartRetries(),
          RUN,
        );
        const report = {
          browser: browserName,
          version: page.context().browser()?.version() ?? null,
          store: kind,
          seed,
          kills,
          pageKills,
          workerStartRetries,
          corruptions: 0,
          verifiedLogBytes,
          lockSteals,
          seconds: Math.round((Date.now() - started) / 1000),
          landed: Object.fromEntries([...table].sort(([a], [b]) => a.localeCompare(b))),
          reach: Object.fromEntries(reach.counts()),
        };
        await info.attach(`storage-kills-${kind}`, {
          body: JSON.stringify(report, null, 2),
          contentType: "application/json",
        });
        console.log(`STORAGE_KILLS ${JSON.stringify(report)}`);
        reach.assertReached();
      });
});

test.describe("rebuild core storage kill campaign: its own failure paths (review B1-p2f)", () => {
  test("a workload failure after a kill is armed fails the kill, in both kill modes", async ({
    page,
  }) => {
    test.setTimeout(180_000);
    await page.goto("/packages/core/browser/");
    for (const mode of ["terminate", "page"] as const) {
      const dir = `inject-${mode}-${Date.now().toString(36)}`;
      const outcome = await page.evaluate(
        async ([url, directory, killMode]) => {
          const run = (await import(url)) as Run;
          // The first spill step reports a mismatch that a later sweep could never see.
          await run.chaosBegin("indexeddb", directory, 7, null, "spill-mismatch");
          try {
            const terminated = await run.chaosKill({
              target: "spill",
              occurrence: 1,
              delayMs: 300,
              mode: killMode,
            });
            if (terminated === null) {
              await new Promise<void>((resolve) => {
                setTimeout(resolve, 300);
              });
              run.chaosPageBoundary();
            }
            return "no failure";
          } catch (error) {
            return String(error);
          } finally {
            await run.chaosEnd("indexeddb", directory);
          }
        },
        [RUN, dir, mode] as const,
      );
      expect(outcome, mode).toContain("a spill page read back wrong (injected)");
    }
  });

  test("a kill after go but before storage began is counted before the step, not in it", async ({
    page,
  }) => {
    test.setTimeout(120_000);
    await page.goto("/packages/core/browser/");
    const dir = `stall-${Date.now().toString(36)}`;
    const landing = await page.evaluate(
      async ([url, directory]) => {
        const run = (await import(url)) as Run;
        await run.chaosBegin("indexeddb", directory, 5, null, "stall-publish-after-go");
        const killed = await run.chaosKill({
          target: "publish",
          occurrence: 1,
          delayMs: 200,
          mode: "terminate",
        });
        await run.chaosEnd("indexeddb", directory);
        return killed;
      },
      [RUN, dir] as const,
    );
    expect(landing?.landed).toBe("before-publish");
  });

  test("a page-close boundary includes the acknowledgements received after the kill was armed", async ({
    page,
  }) => {
    test.setTimeout(120_000);
    await page.goto("/packages/core/browser/");
    const dir = `boundary-${Date.now().toString(36)}`;
    const result = await page.evaluate(
      async ([url, directory]) => {
        const run = (await import(url)) as Run;
        const recovered = await run.chaosBegin("indexeddb", directory, 11, null);
        await run.chaosKill({ target: "publish", occurrence: 1, delayMs: 0, mode: "page" });
        // The armed publish runs on and is acknowledged before the boundary is taken.
        await new Promise<void>((resolve) => {
          setTimeout(resolve, 1_000);
        });
        const boundary = run.chaosPageBoundary();
        await run.chaosEnd("indexeddb", directory);
        return { before: recovered.state.round, boundary };
      },
      [RUN, dir] as const,
    );
    expect(result.boundary.landed).toBe("between");
    expect(result.boundary.expect.inFlight).toEqual([]);
    expect(result.boundary.expect.state.round).toBe(result.before + 1);
  });
});

test.describe("rebuild core storage concurrent kill campaign (Gate A)", () => {
  const nightly = Number(process.env.MINNOW_NEXT_STORAGE_KILLS ?? "0");
  const kills = nightly > 0 ? nightly : 50;
  for (const kind of ["opfs", "indexeddb"] as const)
    for (const seed of seedsFor(`next-storage-concurrent-kills-${kind}`, [
      kind === "opfs" ? 0xc0c1 : 0xc0c2,
    ]))
      test(`${kind}: kills at random moments of a concurrent workload reopen to a legal state (seed ${String(seed)})`, async ({
        page: first,
        browserName,
      }, info) => {
        test.skip(
          nightly > 0 && browserName !== "chromium",
          "the nightly campaign runs in Chromium",
        );
        // WebKit: a worker terminated with several IndexedDB transactions in flight leaves the
        // database blocked for the rest of the browser session: the next owner's fence
        // transaction never runs, even with the lock stolen and from a fresh page (seen at kill 5
        // and kill 12 with 5 operations in flight; the serialized campaign, one transaction in
        // flight, never wedges). A platform finding for the ownership track, not a corruption.
        test.fixme(
          browserName === "webkit" &&
            kind === "indexeddb" &&
            process.env.MINNOW_WEBKIT_WEDGE !== "1",
          "WebKit wedges IndexedDB after a worker is terminated mid-transactions",
        );
        test.setTimeout(Math.max(600_000, kills * 5_000));
        const page: Page = first;
        await page.goto("/packages/core/browser/");
        if (kind === "opfs") await requireWorkerOpfs(page);
        const random = mulberry32(seed);
        const dir = `concurrent-${kind}-${String(seed)}-${Date.now().toString(36)}`;
        // IndexedDB's storage work is asynchronous: kills land inside interleaved operations,
        // and every interleaving is required. OPFS's calls are synchronous: each operation starts
        // in its own task (after its go) and its storage calls finish within it, so a kill never
        // finds two OPFS operations inside storage; there the interleavings are the simulated
        // power-loss sweep's (spill/chaos-crash.test.ts, STORAGE_CONCURRENT_CRASH_REACH).
        const required: ReadonlyArray<(typeof STORAGE_CONCURRENT_KILL_REACH)[number]> =
          kind === "indexeddb" ? STORAGE_CONCURRENT_KILL_REACH : [];
        const reach = createReach(`concurrent storage kills ${kind} ${browserName}`, required, {
          minimum: Math.max(1, Math.floor(kills / 50)),
        });
        const inFlightCounts = new Map<number, number>();
        const pairs = new Map<string, number>();
        const started = Date.now();
        let expected: ChaosExpect | null = null;
        let previous = "none";
        let verifiedLogBytes = 0;
        let lockSteals = 0;
        // Diagnosis only (MINNOW_CHAOS_LANES=writer,log,...): run a subset of the lanes.
        const lanes = process.env.MINNOW_CHAOS_LANES?.split(",") ?? null;
        for (let iteration = 0; iteration <= kills; iteration++) {
          let recovery;
          const begin = () =>
            page.evaluate(
              async ([url, store, directory, workerSeed, expectation, only]) =>
                ((await import(url)) as Run).chaosBegin(
                  store,
                  directory,
                  workerSeed,
                  expectation,
                  null,
                  "concurrent",
                  only,
                ),
              [RUN, kind, dir, (seed + iteration * 0x9e3779b9) >>> 0, expected, lanes] as const,
            );
          try {
            recovery = await begin();
          } catch (error) {
            const diagnosis =
              kind === "indexeddb"
                ? await page
                    .evaluate(
                      async ([url, store, directory]) =>
                        ((await import(url)) as Run).chaosDiagnose(store, directory),
                      [RUN, kind, dir] as const,
                    )
                    .catch((failure: unknown) => String(failure))
                : null;
            console.log(`WEDGE ${JSON.stringify(diagnosis)}`);
            throw new Error(
              `${kind}/${browserName}: recovery after concurrent kill ${String(iteration)} failed (replay: MINNOW_SEED=${String(seed)}; ${previous}): ${String(error)}`,
              { cause: error },
            );
          }
          verifiedLogBytes += recovery.verifiedLogBytes;
          if (recovery.stolenLock) lockSteals += 1;
          if (iteration === kills) break;
          const delayMs = 20 + Math.floor(random() * 500);
          let landing;
          try {
            landing = await page.evaluate(
              async ([url, delay]) => ((await import(url)) as Run).chaosKillAfter(delay),
              [RUN, delayMs] as const,
            );
          } catch (error) {
            throw new Error(
              `${kind}/${browserName}: concurrent kill ${String(iteration)} observed a workload failure (replay: MINNOW_SEED=${String(seed)}; ${previous}): ${String(error)}`,
              { cause: error },
            );
          }
          expected = landing.expect;
          previous = `kill ${String(iteration)} after ${String(delayMs)} ms, in storage ${landing.inOperation.join("+") || "nothing"}, in flight ${landing.inFlight.join("+") || "nothing"}, expecting ${JSON.stringify(landing.expect)}`;
          // Reach counts operations inside their storage work at the kill.
          const steps = landing.inOperation;
          inFlightCounts.set(steps.length, (inFlightCounts.get(steps.length) ?? 0) + 1);
          for (const step of new Set(steps)) pairs.set(step, (pairs.get(step) ?? 0) + 1);
          const has = (step: ChaosStep) => steps.includes(step);
          const entered: string[] = [];
          if (steps.length >= 2) entered.push("two-or-more-in-operation");
          if ((has("publish") || has("reclaim")) && has("log-flush"))
            entered.push("publish-with-log-flush");
          if (has("reclaim") && has("read")) entered.push("reclaim-with-read");
          if (steps.filter((step) => step === "write").length >= 2)
            entered.push("write-with-write");
          if (has("spill") && steps.length >= 2) entered.push("spill-with-another");
          reach.recordHistory(entered);
        }
        await page.evaluate(
          async ([url, store, directory]) =>
            ((await import(url)) as Run).chaosEnd(store, directory),
          [RUN, kind, dir] as const,
        );
        const report = {
          mode: "concurrent",
          browser: browserName,
          version: page.context().browser()?.version() ?? null,
          store: kind,
          seed,
          kills,
          corruptions: 0,
          verifiedLogBytes,
          lockSteals,
          seconds: Math.round((Date.now() - started) / 1000),
          inOperationAtKill: Object.fromEntries([...inFlightCounts].sort(([a], [b]) => a - b)),
          stepInOperationAtKill: Object.fromEntries(
            [...pairs].sort(([a], [b]) => a.localeCompare(b)),
          ),
          reach: Object.fromEntries(reach.counts()),
          reachBackedBy: required.length === 0 ? "spill/chaos-crash.test.ts" : null,
        };
        await info.attach(`storage-concurrent-kills-${kind}`, {
          body: JSON.stringify(report, null, 2),
          contentType: "application/json",
        });
        console.log(`STORAGE_KILLS ${JSON.stringify(report)}`);
        reach.assertReached();
      });
});

test.describe("rebuild core storage reopen at a million blobs (Gate A, opt-in)", () => {
  const blobs = Number(process.env.MINNOW_NEXT_STORAGE_REOPEN_BLOBS ?? "1000000");
  for (const [kind, layout] of [
    ["opfs", "packed"],
    ["indexeddb", "packed"],
    ["indexeddb", "scattered"],
  ] as const)
    test(`${kind}, ${layout}: reopen time and its parts`, async ({ page, browserName }, info) => {
      test.skip(
        browserName !== "chromium" || process.env.MINNOW_NEXT_STORAGE_REOPEN !== "1",
        "opt-in: MINNOW_NEXT_STORAGE_REOPEN=1, Chromium",
      );
      test.setTimeout(3_600_000);
      await page.goto("/packages/core/browser/");
      if (kind === "opfs") await requireWorkerOpfs(page);
      const result = await page.evaluate(
        async ([url, store, shape, count]) =>
          ((await import(url)) as Run).runReopenBench(store, shape, count, 3),
        [RUN, kind, layout, blobs] as const,
      );
      const report = { browser: browserName, store: kind, ...result };
      await info.attach(`storage-reopen-${kind}-${layout}`, {
        body: JSON.stringify(report, null, 2),
        contentType: "application/json",
      });
      console.log(`STORAGE_REOPEN ${JSON.stringify(report)}`);
      expect(result.reopens).toHaveLength(3);
    });
});

test.describe("rebuild core storage graceful step-down (the WebKit IndexedDB wedge)", () => {
  test("stop, drain, close, release, then terminate: the next owner always recovers", async ({
    page,
    browserName,
  }, info) => {
    test.setTimeout(600_000);
    await page.goto("/packages/core/browser/");
    const cycles = Number(process.env.MINNOW_STEP_DOWN_CYCLES ?? "40");
    const random = mulberry32(0x57e9);
    const dir = `step-down-${Date.now().toString(36)}`;
    let expected: ChaosExpect | null = null;
    const timings: Array<{ quiesceMs: number; sessionMs: number; releaseMs: number }> = [];
    for (let cycle = 0; cycle <= cycles; cycle++) {
      await page.evaluate(
        async ([url, directory, workerSeed, expectation]) =>
          ((await import(url)) as Run).chaosBegin(
            "indexeddb",
            directory,
            workerSeed,
            expectation,
            null,
            "concurrent",
          ),
        [RUN, dir, 0x57e9 + cycle, expected] as const,
      );
      if (cycle === cycles) break;
      const stepDown = await page.evaluate(
        async ([url, delay]) => ((await import(url)) as Run).chaosStepDown(delay),
        [RUN, 20 + Math.floor(random() * 500)] as const,
      );
      expect(stepDown.leftInFlight).toBe(0);
      expected = stepDown.expect;
      timings.push(stepDown);
    }
    await page.evaluate(
      async ([url, directory]) => ((await import(url)) as Run).chaosEnd("indexeddb", directory),
      [RUN, dir] as const,
    );
    const sorted = (key: "quiesceMs" | "sessionMs" | "releaseMs") =>
      timings.map((timing) => timing[key]).sort((a, b) => a - b);
    const summary = (key: "quiesceMs" | "sessionMs" | "releaseMs") => {
      const values = sorted(key);
      return {
        median: Math.round(values[Math.floor(values.length / 2)] ?? 0),
        p95: Math.round(values[Math.floor(values.length * 0.95)] ?? 0),
        max: Math.round(values.at(-1) ?? 0),
      };
    };
    const report = {
      browser: browserName,
      cycles,
      quiesceMs: summary("quiesceMs"),
      sessionMs: summary("sessionMs"),
      releaseMs: summary("releaseMs"),
    };
    await info.attach("storage-step-down", {
      body: JSON.stringify(report, null, 2),
      contentType: "application/json",
    });
    console.log(`STORAGE_STEP_DOWN ${JSON.stringify(report)}`);
  });
});
