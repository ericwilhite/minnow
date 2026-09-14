import { expect } from "@playwright/test";
import { requireWorkerOpfs, test as storageTest } from "./fixtures.js";

/**
 * The recorded SQLLogicTest corpus in real browsers, over real storage.
 *
 * Node runs this corpus over MemoryBlockStore on V8 (`npm run test:sql:standard`). Here the same
 * files run through the published module worker against IndexedDB and OPFS in Chromium, Firefox,
 * and WebKit, so a browser JavaScript engine's Date, regex, or number-formatting difference, a
 * result that does not survive structured cloning across the worker boundary, or a durable
 * store's commit path under a thousand statements with automatic maintenance enabled, is caught
 * against the corpus's recorded values rather than against Node's answers.
 *
 * The per-file timings are attached to the report so a real-browser slowdown has a number.
 */
const conformance = process.env.MINNOW_BROWSER_CONFORMANCE === "1";
const files = conformance
  ? [
      "standard-select1.test",
      "standard-select2.test",
      "standard-select3.test",
      "full-select4.test",
      "full-select5.test",
    ]
  : [
      "standard-select1.test",
      "standard-select2.test",
      "standard-select3.test",
      "standard-select4.test",
      "standard-select5.test",
    ];

// Independently counted statement/query headers in the committed corpus, not parser output.
// A parser that drops records or invents skips must not make a shortened run green.
const recordCounts: Record<string, { statements: number; queries: number }> = {
  "standard-select1.test": { statements: 31, queries: 1000 },
  "standard-select2.test": { statements: 31, queries: 1000 },
  "standard-select3.test": { statements: 31, queries: 3320 },
  "standard-select4.test": { statements: 1025, queries: 1022 },
  "standard-select5.test": { statements: 704, queries: 3 },
  "full-select4.test": { statements: 1025, queries: 2832 },
  "full-select5.test": { statements: 704, queries: 732 },
};

type StoreKind = "indexeddb" | "opfs";

interface BrowserRunResult {
  statistics: { statements: number; queries: number; values: number; skipped: number };
  failures: Array<{
    message: string;
    file: string;
    line: number;
    sql: string | undefined;
    cause: string | undefined;
  }>;
  elapsedMs: number;
  statementMs: number;
  queryMs: number;
  pageErrors: string[];
}

for (const store of ["indexeddb", "opfs"] as const satisfies readonly StoreKind[]) {
  for (const file of files) {
    storageTest(
      `${store}: ${file} matches the recorded corpus`,
      async ({ storageContext }, info) => {
        storageTest.setTimeout(600_000);
        const page = await storageContext.newPage();
        // Errors the worker reports through the client's sink land on the page console. They are
        // part of the verdict: a corpus that passes while maintenance fails in the background is
        // not a pass.
        const consoleErrors: string[] = [];
        page.on("console", (message) => {
          if (message.type() === "error") consoleErrors.push(message.text());
        });
        page.on("pageerror", (error) => consoleErrors.push(error.message));
        await page.goto("/packages/core/browser/sqllogictest/");
        await expect(page.locator("#ready")).toHaveText("SQLLogicTest runner ready");
        if (store === "opfs") await requireWorkerOpfs(page);

        const result = await page.evaluate(
          async (request) => {
            const target = window as typeof window & {
              runSqlLogicTestInBrowser(input: typeof request): Promise<BrowserRunResult>;
            };
            return target.runSqlLogicTestInBrowser(request);
          },
          { file, store },
        );

        await info.attach(`${store}-${file}-timing.json`, {
          contentType: "application/json",
          body: JSON.stringify(
            {
              browser: info.project.name,
              store,
              file,
              ...result.statistics,
              elapsedMs: Math.round(result.elapsedMs),
              statementMs: Math.round(result.statementMs),
              queryMs: Math.round(result.queryMs),
            },
            undefined,
            2,
          ),
        });

        expect(result.pageErrors).toEqual([]);
        expect(consoleErrors).toEqual([]);
        expect(result.failures).toEqual([]);
        expect(recordCounts[file]).toBeDefined();
        expect(result.statistics).toMatchObject({ ...recordCounts[file], skipped: 0 });
        await page.close();
      },
    );
  }
}
