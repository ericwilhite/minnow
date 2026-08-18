import { expect, test } from "@playwright/test";

/**
 * The worker boundary, with a real worker on the other side of it.
 *
 * The in-process stand-in in client.test.ts covers the protocol thoroughly and cheaply. What it
 * cannot cover is the part that only exists when there are genuinely two threads: whether the
 * published entry resolves and boots as a module worker, whether transferred buffers survive the
 * trip, whether a worker's IndexedDB database is the same one a second worker later opens, and
 * whether terminating one loses anything.
 */
test("drives a database through a real module worker", async ({ page }) => {
  const consoleErrors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => consoleErrors.push(error.message));

  await page.goto("/packages/core/browser/worker/");
  await expect(page.locator("#ready")).toHaveText("Worker boundary tests ready");

  // The page's own declaration lives in worker-run.ts, which is a different TypeScript project;
  // naming the shape here is what transactions.spec.ts does for the same reason.
  const result = await page.evaluate(async () => {
    const target = window as typeof window & {
      runWorkerBoundaryTest(): Promise<{
        roundTrip: { rows: number; firstRegion: string | null };
        largeTransfer: { rows: number; checksum: number };
        workerError: { rejected: boolean; message: string };
        survivesTermination: { rowsAfterRestart: number; reopenedSameDatabase: boolean };
        concurrentWrites: { acknowledged: number; rowsPersisted: number; reasons: string[] };
      }>;
    };
    return target.runWorkerBoundaryTest();
  });

  // The worker booted and answered at all -- which is the two lines in worker.ts that nothing
  // else in the suite executes.
  expect(result.roundTrip.rows).toBe(1);
  expect(result.roundTrip.firstRegion).toBe("west");

  // Every row came back across the boundary intact. The checksum is recomputed in-page from the
  // rows the worker sent, so a detached or mis-sliced transferred buffer changes it rather than
  // throwing somewhere convenient.
  expect(result.largeTransfer.rows).toBe(4_000);
  let expected = 0;
  for (let index = 0; index < 4_000; index += 1) {
    expected = (expected + (index + 1) * 2 - index) % 1_000_003;
  }
  expect(result.largeTransfer.checksum).toBe(expected);

  // A failure raised inside the worker arrives on the page as a rejection carrying its own
  // message, not as a generic transport error or a hang.
  expect(result.workerError.rejected).toBe(true);
  expect(result.workerError.message).toMatch(/nope/);

  // Sixteen writes issued without awaiting each one. Not all of them land: concurrent commits
  // contend for the manifest, and the retry budget caps how many can win -- write-contention
  // test.ts pins that ceiling and explains it. What this asserts is the part the real boundary
  // could break and the in-process stand-in could not: exactly the acknowledged writes are
  // present, so nothing was lost in flight or applied twice by the postMessage queue.
  expect(result.concurrentWrites.acknowledged).toBeGreaterThan(0);
  expect(result.concurrentWrites.rowsPersisted).toBe(result.concurrentWrites.acknowledged);
  expect(result.concurrentWrites.reasons.join(" | ")).toMatch(/^$|Manifest changed/);

  // Terminating the worker leaves a database a second worker opens and reads as its own.
  expect(result.survivesTermination.reopenedSameDatabase).toBe(true);
  expect(result.survivesTermination.rowsAfterRestart).toBe(
    4_000 + result.concurrentWrites.acknowledged,
  );

  // A worker that logs an error while appearing to succeed is still a failure.
  expect(consoleErrors).toEqual([]);
});
