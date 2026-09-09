import { test as base, webkit, type Page, type BrowserContext } from "@playwright/test";

// Safari needs a persistent (non-private) context for OPFS worker storage.
export const test = base.extend<{ storageContext: BrowserContext }>({
  storageContext: async ({ browserName, context }, use, info) => {
    if (browserName !== "webkit") return use(context);
    const persistent = await webkit.launchPersistentContext(info.outputPath("storage-profile"), {
      baseURL: info.project.use.baseURL ?? "",
    });
    try {
      await use(persistent);
    } finally {
      await persistent.close();
    }
  },
});

/** Probe the worker realm: Linux WebKit may expose OPFS in a page but not in its workers. */
export async function requireWorkerOpfs(page: Page): Promise<void> {
  const supported = await page.evaluate(
    () =>
      new Promise<boolean>((resolve, reject) => {
        const url = URL.createObjectURL(
          new Blob(['self.postMessage(typeof navigator.storage?.getDirectory === "function");'], {
            type: "text/javascript",
          }),
        );
        const worker = new Worker(url);
        const cleanup = () => {
          clearTimeout(timer);
          worker.terminate();
          URL.revokeObjectURL(url);
        };
        const timer = setTimeout(() => {
          cleanup();
          reject(new Error("OPFS capability worker did not answer"));
        }, 5_000);
        worker.addEventListener(
          "message",
          (event: MessageEvent<unknown>) => {
            cleanup();
            resolve(event.data === true);
          },
          { once: true },
        );
        worker.addEventListener(
          "error",
          (event) => {
            cleanup();
            reject(new Error(event.message));
          },
          { once: true },
        );
      }),
  );
  test.skip(!supported, "This browser build exposes no OPFS in dedicated workers");
}
