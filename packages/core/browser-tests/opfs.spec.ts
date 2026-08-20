import { expect, test as base, webkit, type Page } from "@playwright/test";

/**
 * The OPFS block store on real browser storage. The Node suites prove the store's behaviour
 * over a shim that mimics OPFS locking; this proves the mimicry per engine — Chromium, Firefox,
 * and WebKit each implement the exclusive sync-access-handle lock the command log builds on,
 * and each is the only place that implementation is exercised.
 *
 * WebKit gets a persistent browser context: Playwright's default ephemeral context is
 * private-browsing storage, and Safari's private browsing has no OPFS at all —
 * `navigator.storage.getDirectory()` fails outright. (The same fact is why the docs keep
 * IndexedDB as the fallback story for private windows.)
 */
const test = base.extend<{ page: Page }>({
  page: async ({ browserName, page }, use, testInfo) => {
    if (browserName !== "webkit") {
      await use(page);
      return;
    }
    const context = await webkit.launchPersistentContext(testInfo.outputPath("webkit-profile"), {
      baseURL: testInfo.project.use.baseURL ?? "",
    });
    const persistentPage = await context.newPage();
    await use(persistentPage);
    await context.close();
  },
});

test("runs the OPFS store through real workers on real storage", async ({ page }) => {
  // Firefox occasionally needs well over the default 30s for the full sequence of real
  // flushed writes; the assertions below are about correctness, not latency.
  test.setTimeout(120_000);
  const consoleErrors: string[] = [];
  page.on("console", (message) => {
    if (message.type() !== "error") return;
    // Terminating a worker mid-session makes WebKit log a failed blob resource load for the
    // dying worker's module graph. That is the browser narrating the termination, not an
    // application error.
    if (message.text().includes("Failed to load resource")) return;
    const where = message.location();
    consoleErrors.push(`${message.text()} @ ${where.url}:${String(where.lineNumber)}`);
  });
  page.on("pageerror", (error) => consoleErrors.push(error.message));

  await page.goto("/packages/core/browser/opfs/");
  await expect(page.locator("#ready")).toHaveText("OPFS store tests ready");

  // Some WebKit builds (notably Playwright's Linux port) expose no OPFS inside dedicated
  // workers at all — `navigator.storage` is undefined there even in a persistent context.
  // That is the store's documented unavailable case, not a bug this suite can catch, so probe
  // from a real worker and skip honestly rather than fail on an API the browser does not have.
  const opfsInWorkers = await page.evaluate(
    () =>
      new Promise<boolean>((resolve) => {
        const code = "self.postMessage(typeof navigator?.storage?.getDirectory === 'function');";
        const worker = new Worker(
          URL.createObjectURL(new Blob([code], { type: "text/javascript" })),
        );
        worker.addEventListener("message", (event) => {
          resolve(event.data === true);
          worker.terminate();
        });
        setTimeout(() => {
          resolve(false);
        }, 5_000);
      }),
  );
  test.skip(!opfsInWorkers, "this WebKit build exposes no OPFS in workers");

  const result = await page.evaluate(async () => {
    const target = window as typeof window & {
      runOpfsStoreTest(): Promise<{
        roundTrip: { rows: number; firstRegion: string | null };
        competingCommits: {
          acknowledged: number;
          rejectedCleanly: number;
          unexpected: string[];
          firstCount: number;
          secondCount: number;
        };
        survivesTermination: { rowsAfterRestart: number };
        checkpointCrossing: { rowsAfterReopen: number };
      }>;
    };
    return target.runOpfsStoreTest();
  });

  // Sync access handles resolved inside the published worker and the store answered.
  expect(result.roundTrip.rows).toBe(500);
  expect(result.roundTrip.firstRegion).toBe("west");

  // Two independent workers raced on one directory. Every write was either acknowledged or
  // rejected as a conflict — nothing failed for any other reason — and both workers converge
  // on one database containing exactly the acknowledged writes.
  expect(result.competingCommits.unexpected).toEqual([]);
  expect(result.competingCommits.acknowledged + result.competingCommits.rejectedCleanly).toBe(24);
  expect(result.competingCommits.acknowledged).toBeGreaterThan(0);
  expect(result.competingCommits.firstCount).toBe(result.competingCommits.acknowledged);
  expect(result.competingCommits.secondCount).toBe(result.competingCommits.acknowledged);

  // Terminating a worker released its locks (or the fresh worker could not have written at
  // all) and lost nothing acknowledged.
  expect(result.survivesTermination.rowsAfterRestart).toBe(result.competingCommits.acknowledged);

  // Forty batches cross the checkpoint interval several times; the cold reopen replayed
  // checkpoint plus tail back to the full row count.
  expect(result.checkpointCrossing.rowsAfterReopen).toBe(40);

  expect(consoleErrors).toEqual([]);
});
