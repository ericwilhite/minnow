/**
 * The capture itself. A persistent context with a real on-disk profile is non-negotiable:
 * Playwright's default ephemeral context backs IndexedDB with memory, which benchmarks
 * nothing. Progress is forwarded through an exposed binding, because in-page console.log
 * is invisible and a stalled run must be distinguishable from a slow one.
 */
import { test, chromium, firefox } from "@playwright/test";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const BROWSERS = { chromium, firefox };
const engineIds = ["minnow", "sqlite", "pglite"];
const requestedEngines = (process.env.CAPTURE_ENGINES ?? engineIds.join(","))
  .split(",")
  .map((engine) => engine.trim())
  .filter(Boolean);
const invalidEngines = requestedEngines.filter((engine) => !engineIds.includes(engine));
if (requestedEngines.length === 0 || invalidEngines.length > 0) {
  throw new Error(
    `CAPTURE_ENGINES must contain one or more of ${engineIds.join(", ")}; invalid: ${invalidEngines.join(", ") || "empty list"}`,
  );
}
const engines = [...new Set(requestedEngines)];
const scale = Number(process.env.CAPTURE_SCALE ?? 100);
if (!Number.isFinite(scale) || scale <= 0) {
  throw new Error("CAPTURE_SCALE must be a positive number");
}
const outDir = path.resolve(repoRoot, process.env.CAPTURE_OUT ?? ".captures");

// Playwright requires the destructuring pattern; this test drives its own persistent context.
// eslint-disable-next-line no-empty-pattern
test("capture engine comparison", async ({}, testInfo) => {
  test.setTimeout(5_400_000);
  const browserType = BROWSERS[testInfo.project.name];
  const profileDir = mkdtempSync(path.join(os.tmpdir(), `minnow-bench-${testInfo.project.name}-`));
  const context = await browserType.launchPersistentContext(profileDir, { headless: true });
  try {
    const page = context.pages()[0] ?? (await context.newPage());
    page.on("console", (message) => {
      console.log(`[page:${message.type()}]`, message.text().slice(0, 300));
    });
    page.on("pageerror", (error) => {
      console.log("[pageerror]", String(error).slice(0, 500));
    });
    await page.exposeFunction("reportProgress", (line) => {
      console.log(`[progress ${new Date().toISOString().slice(11, 19)}]`, line);
    });
    await page.goto("http://127.0.0.1:4176/");
    const config = {
      scale,
      compression: "gzip",
      targetBlockBytes: 1_048_576,
      durability: "relaxed",
      engines,
    };
    const started = Date.now();
    const payload = await page.evaluate(async (config) => {
      const { BenchWorker } = await import("/src/ui/worker-client.ts");
      const worker = new BenchWorker();
      let progressCount = 0;
      let lastReported = 0;
      const report = (stage) => (progress) => {
        progressCount += 1;
        if (progressCount <= 10 || Date.now() - lastReported > 5_000) {
          lastReported = Date.now();
          void window.reportProgress(
            `${stage} ${progress.phase ?? ""} ${progress.message ?? ""} ${progress.completed}/${progress.total}`,
          );
        }
      };
      const dataset = await worker.request("datasetCreate", config, report("load"));
      const reads = await worker.request(
        "suiteReference",
        { datasetId: dataset.id, engines: config.engines },
        report("suite"),
      );
      // Writes run after the read suite: they add tables to each engine's copy of the
      // dataset, and the read numbers must be measured against the dataset as loaded.
      const writes = await worker.request(
        "suiteWrite",
        { datasetId: dataset.id, engines: config.engines },
        report("writes"),
      );
      return {
        dataset,
        reads,
        writes,
        progressCount,
        browserMeta: {
          userAgent: navigator.userAgent,
          hardwareConcurrency: navigator.hardwareConcurrency,
          crossOriginIsolated: globalThis.crossOriginIsolated === true,
        },
      };
    }, config);
    const failedMaterializations = engines.filter(
      (engine) => payload.dataset.engines[engine]?.status !== "ready",
    );
    const incompleteReads = payload.reads.queries.filter((query) =>
      engines.some((engine) => {
        const run = query.engines.find((candidate) => candidate.engine === engine);
        return run?.supported !== true || run.verified !== true;
      }),
    );
    const incompleteWrites = payload.writes.cases.filter((entry) =>
      engines.some((engine) => {
        const run = entry.engines.find((candidate) => candidate.engine === engine);
        return run?.supported !== true || run.verified !== true;
      }),
    );
    if (
      failedMaterializations.length > 0 ||
      !payload.reads.passed ||
      !payload.writes.passed ||
      incompleteReads.length > 0 ||
      incompleteWrites.length > 0
    ) {
      throw new Error(
        [
          `Capture failed correctness checks for ${testInfo.project.name}.`,
          failedMaterializations.length > 0
            ? `materializations: ${failedMaterializations.join(", ")}`
            : undefined,
          incompleteReads.length > 0
            ? `reads: ${incompleteReads.map(({ id }) => id).join(", ")}`
            : undefined,
          incompleteWrites.length > 0
            ? `writes: ${incompleteWrites.map(({ id }) => id).join(", ")}`
            : undefined,
        ]
          .filter(Boolean)
          .join(" "),
      );
    }
    const bundle = {
      schemaVersion: 1,
      kind: "engine-comparison",
      capturedAt: new Date().toISOString(),
      browser: {
        project: testInfo.project.name,
        version: context.browser()?.version() ?? "unknown",
        userAgent: payload.browserMeta.userAgent,
        hardwareConcurrency: payload.browserMeta.hardwareConcurrency,
        crossOriginIsolated: payload.browserMeta.crossOriginIsolated,
        storage: "persistent profile (disk-backed)",
      },
      wallClock: { totalMs: Date.now() - started },
      progressCount: payload.progressCount,
      config,
      dataset: payload.dataset,
      reads: payload.reads,
      writes: payload.writes,
      host: {
        platform: os.platform(),
        release: os.release(),
        arch: os.arch(),
        cpus: os.cpus().length,
        model: os.cpus()[0]?.model ?? "unknown",
      },
    };
    mkdirSync(outDir, { recursive: true });
    const stamp = bundle.capturedAt.slice(0, 10);
    // The scale is part of the identity: captures at different scales publish side by side.
    const file = path.join(
      outDir,
      `${stamp}-engine-comparison-scale${scale}-${testInfo.project.name}.json`,
    );
    writeFileSync(file, JSON.stringify(bundle, null, 1) + "\n");
    console.log(
      `[done:${testInfo.project.name}] readsPassed=${payload.reads.passed} writesPassed=${payload.writes.passed} -> ${file} totals=${JSON.stringify(payload.reads.totalMsByEngine)}`,
    );
  } finally {
    await context.close();
  }
});
