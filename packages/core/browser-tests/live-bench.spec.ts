import { chromium, firefox, webkit, test as base, type BrowserContext } from "@playwright/test";

/**
 * The live-query benchmark on real storage. Opt in with `MINNOW_LIVE_BENCH=1`; it prints one
 * table per browser and store and asserts only that every notification arrived. Persistent
 * contexts throughout: Playwright's default context keeps IndexedDB in memory and gives WebKit
 * no OPFS at all, and a benchmark of browser storage has to hit the browser's storage.
 *
 *   MINNOW_LIVE_BENCH=1 npx playwright test --config playwright.library.config.ts live-bench
 *   MINNOW_LIVE_BENCH=1 MINNOW_LIVE_BENCH_ROWS=500000 … --project chromium
 */
const enabled = process.env.MINNOW_LIVE_BENCH !== undefined;
const rowsList = (process.env.MINNOW_LIVE_BENCH_ROWS ?? "10000,100000")
  .split(",")
  .map(Number)
  .filter((rows) => Number.isFinite(rows) && rows > 0);
const subscriptions = Number(process.env.MINNOW_LIVE_BENCH_SUBSCRIPTIONS ?? "100");
const samples = Number(process.env.MINNOW_LIVE_BENCH_SAMPLES ?? "5");

interface Timing {
  medianMs: number;
  p95Ms: number;
  samples: number;
}
interface LiveBenchReport {
  store: string;
  rows: number;
  subscriptions: number;
  seedMs: number;
  writeFloor: Timing;
  subscribeMs: number;
  typedSubscribeMs: number;
  insertVisible: Timing;
  updateVisible: Timing;
  updateHidden: Timing;
  deleteVisible: Timing;
  unrelated: Timing;
  burst50Ms: number;
  stats: Record<string, number>;
  pageHeapMiB: { before: number; after: number } | null;
  typedEmitsPerVisibleChange: number;
  typedRowIdentityPreserved: boolean;
}

const test = base.extend<{ context: BrowserContext }>({
  context: async ({ browserName }, use, testInfo) => {
    const launcher =
      browserName === "webkit" ? webkit : browserName === "firefox" ? firefox : chromium;
    const context = await launcher.launchPersistentContext(testInfo.outputPath("profile"), {
      baseURL: testInfo.project.use.baseURL ?? "",
    });
    await use(context);
    await context.close();
  },
});

function row(label: string, value: string): string {
  return `${label.padEnd(28)} ${value}`;
}

function ms(value: number): string {
  return `${value.toFixed(1)} ms`;
}

function timing(value: Timing): string {
  return `${ms(value.medianMs)} median, ${ms(value.p95Ms)} p95`;
}

for (const store of ["indexeddb", "opfs"] as const) {
  for (const rows of rowsList) {
    test(`live queries over ${store} with ${String(rows)} rows`, async ({
      context,
      browserName,
    }) => {
      test.skip(!enabled, "set MINNOW_LIVE_BENCH=1 to run the live-query benchmark");
      test.setTimeout(8 * 60_000);
      const page = await context.newPage();
      const errors: string[] = [];
      const phases: string[] = [];
      page.on("pageerror", (error) => errors.push(error.message));
      page.on("crash", () => errors.push("the page crashed"));
      page.on("framenavigated", (frame) => {
        if (frame === page.mainFrame()) phases.push(`navigated to ${frame.url()}`);
      });
      page.on("close", () => phases.push("page closed"));
      page.on("console", (message) => {
        const text = message.text();
        if (text.startsWith("live-bench phase:")) phases.push(text.slice(17).trim());
        else if (message.type() === "error") errors.push(`console: ${text}`);
      });
      await page.goto("/packages/core/browser/live/");
      await page.locator("#ready").waitFor();
      if (store === "opfs") {
        const opfsInWorkers = await page.evaluate(
          () =>
            new Promise<boolean>((resolve) => {
              const code =
                "self.postMessage(typeof navigator?.storage?.getDirectory === 'function');";
              const worker = new Worker(
                URL.createObjectURL(new Blob([code], { type: "text/javascript" })),
              );
              worker.addEventListener("message", (event) => {
                resolve(event.data === true);
              });
              worker.addEventListener("error", () => {
                resolve(false);
              });
            }),
        );
        test.skip(!opfsInWorkers, "this browser exposes no OPFS in workers");
      }
      let report: LiveBenchReport;
      try {
        report = await page.evaluate(
          (options) =>
            (
              window as typeof window & {
                runLiveBench(input: typeof options): Promise<LiveBenchReport>;
              }
            ).runLiveBench(options),
          { store, rows, subscriptions, samples },
        );
      } catch (error) {
        console.log(`phases reached: ${phases.join(" → ")}\npage errors:\n${errors.join("\n")}`);
        throw error;
      }
      const lines = [
        `${browserName} · ${store} · ${String(rows)} rows · ${String(subscriptions)} subscriptions`,
        row("seed", ms(report.seedMs)),
        row("one insert, nothing live", timing(report.writeFloor)),
        row("subscribe (low-level)", ms(report.subscribeMs)),
        row("subscribe (typed)", ms(report.typedSubscribeMs)),
        row("insert visible → all", timing(report.insertVisible)),
        row("update visible → all", timing(report.updateVisible)),
        row("update hidden (sweep)", timing(report.updateHidden)),
        row("delete visible → all", timing(report.deleteVisible)),
        row("unrelated commit (sweep)", timing(report.unrelated)),
        row("burst of 50 inserts", ms(report.burst50Ms)),
        row("typed emits / change", report.typedEmitsPerVisibleChange.toFixed(2)),
        row("typed row identity kept", String(report.typedRowIdentityPreserved)),
        row(
          "maintained / reruns",
          `${String(report.stats.maintained)} / ${String(report.stats.reruns)}`,
        ),
        row(
          "sweeps / groups visited",
          `${String(report.stats.sweeps)} / ${String(report.stats.groupsVisited)}`,
        ),
        row("rows retained (worker)", String(report.stats.retainedRows)),
        row(
          "page heap before → after",
          report.pageHeapMiB === null
            ? "n/a"
            : `${report.pageHeapMiB.before.toFixed(1)} → ${report.pageHeapMiB.after.toFixed(1)} MiB`,
        ),
      ];
      console.log(`\n${lines.join("\n")}\n`);
      void test.info().attach(`${browserName}-${store}-${String(rows)}`, {
        body: JSON.stringify(report, null, 2),
        contentType: "application/json",
      });
      test.expect(errors).toEqual([]);
      test.expect(report.typedEmitsPerVisibleChange).toBeLessThanOrEqual(1);
    });
  }
}
