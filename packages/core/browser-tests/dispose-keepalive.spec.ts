import { expect, test, type Page } from "@playwright/test";

function collectPageErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  page.on("pageerror", (error) => errors.push(error.message));
  return errors;
}

test("a reporting worker completes graceful disposal past the base deadline", async ({ page }) => {
  const errors = collectPageErrors(page);

  await page.goto("/packages/core/browser/");
  const result = await page.evaluate(async () => {
    const url = "/packages/core/browser/dispose-keepalive-run.ts";
    return (
      (await import(url)) as typeof import("../browser/dispose-keepalive-run.js")
    ).runDisposeKeepalive();
  });

  expect(result.elapsedMs).toBeGreaterThanOrEqual(5_250);
  expect(result.elapsedMs).toBeLessThan(10_000);
  expect(errors).toEqual([]);
});

test("a reporting worker cannot extend disposal past the absolute cap", async ({ page }) => {
  const errors = collectPageErrors(page);
  await page.goto("/packages/core/browser/");
  const result = await page.evaluate(async () => {
    const url = "/packages/core/browser/dispose-keepalive-run.ts";
    return (
      (await import(url)) as typeof import("../browser/dispose-keepalive-run.js")
    ).runDisposeAbsoluteCap();
  });

  expect(result.elapsedMs).toBeGreaterThanOrEqual(2_990);
  expect(result.elapsedMs).toBeLessThan(4_500);
  expect(result.error).toMatchObject({
    name: "DatabaseWorkerOutcomeUnknownError",
    props: { method: "dispose" },
    cause: {
      name: "DatabaseWorkerTimeoutError",
      props: { method: "dispose", timeoutMs: 300 },
    },
  });
  expect(result.disposeCalls).toBe(1);
  expect(result.terminations).toBe(1);
  expect(errors).toEqual([]);
});

test("close cleans up a real worker after its initialization failed", async ({ page }) => {
  const errors = collectPageErrors(page);
  await page.goto("/packages/core/browser/");
  const result = await page.evaluate(async () => {
    const url = "/packages/core/browser/dispose-keepalive-run.ts";
    return (
      (await import(url)) as typeof import("../browser/dispose-keepalive-run.js")
    ).runFailedInitializationClose();
  });

  expect(result.readyError).toMatchObject({
    name: "StorageUnresponsiveError",
    props: { backend: "indexeddb", databaseName: "failed-initialization", waitedMs: 30_000 },
  });
  expect(result.disposeCalls).toBe(0);
  expect(result.removed).toEqual(["error", "message", "messageerror"]);
  expect(result.terminations).toBe(1);
  expect(errors).toEqual([]);
});
